use std::{env, path::Path, sync::Arc, time::Duration};
use telegram_bulk_delivery::{
    auth::{EncryptedSecret, KeyRing, Purpose},
    config::OperatorConfig,
    domain::BotId,
    http::{api_router, resolve_api_url, router, ApiState, HealthState, Readiness},
    observe::{cgroup_memory_current_bytes, checkpoint_mode, rss_bytes},
    scheduler::{
        dispatcher::{snapshot_policy_and_base, snapshot_target_rate, Dispatcher, LeasedCall},
        fairness::Wdrr,
        limiters::{Limiters, Scope},
        RECONCILE_INTERVAL_MS,
    },
    store::{DispatchItem, Store, WriterCmd},
    webhook::{WebhookDeliverer, WebhookSsrPolicy},
};
use tokio::sync::{mpsc, Semaphore};
use uuid::Uuid;
use zeroize::Zeroize;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
/// Live size of the `-wal` sibling of the SQLite database.
fn wal_bytes(db: &Path) -> u64 {
    std::fs::metadata(format!("{}-wal", db.display()))
        .map(|m| m.len())
        .unwrap_or(0)
}
/// Best-effort removal of blobs/orphans left over from a crash: at boot no
/// ingest is in flight, so any file under `files/` not referenced by `job_files`
/// (or still a `*.part` under `tmp/`) is garbage and safe to delete. Accepting
/// jobs left behind by a crash (their tmp dirs were just wiped) are aborted so
/// they cannot leak forever.
fn sweep_orphan_files(store: &Store, data_root: &Path) {
    let Ok(blob_paths) = store.readers.file_blob_paths() else {
        return;
    };
    let referenced: std::collections::HashSet<String> = blob_paths.into_iter().collect();
    if let Ok(walk) = std::fs::read_dir(data_root.join("files")) {
        for entry in walk {
            let Ok(entry) = entry else { continue };
            let path = entry.path();
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                if let Ok(sub) = std::fs::read_dir(&path) {
                    for file in sub {
                        let Ok(file) = file else { continue };
                        let p = file.path();
                        if !referenced.contains(&p.to_string_lossy().into_owned()) {
                            let _ = std::fs::remove_file(&p);
                        }
                    }
                }
                let _ = std::fs::remove_dir(&path);
            } else if !referenced.contains(&path.to_string_lossy().into_owned()) {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    let _ = std::fs::remove_dir_all(data_root.join("tmp"));
    // Abort accepting jobs orphaned by the crash (their tmp dirs are gone, so
    // they can never be promoted). No ingest can be in flight before we bind.
    if let Ok(ids) = store.readers.accepting_job_ids() {
        for job in ids {
            let _ = store
                .writer
                .execute(WriterCmd::AbortAcceptJob { job_id: job });
        }
    }
}

fn merge_params(
    shared: &str,
    patch: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, ()> {
    let mut merged = serde_json::from_str::<serde_json::Value>(shared)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .ok_or(())?;
    let patch = serde_json::from_str::<serde_json::Value>(patch)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .ok_or(())?;
    merged.extend(patch);
    Ok(merged)
}

async fn send_telegram(
    keys: &KeyRing,
    global_base: &str,
    per_bot_base: Option<&str>,
    client: &reqwest::Client,
    bot_id: BotId,
    item: DispatchItem,
) -> Result<(u16, serde_json::Value), ()> {
    let nonce: [u8; 12] = item.token_nonce.try_into().map_err(|_| ())?;
    let encrypted = EncryptedSecret {
        key_id: item.token_kid,
        nonce,
        ciphertext: item.token_ciphertext,
    };
    let token = keys
        .decrypt(bot_id, Purpose::BotToken, &encrypted)
        .map_err(|_| ())?;
    let token = std::str::from_utf8(&token).map_err(|_| ())?;
    let params = merge_params(&item.shared_params_json, &item.patch_json)?;
    let spec = telegram_api_meta::method(&item.method).ok_or(())?;
    let url = resolve_api_url(global_base, per_bot_base, token, spec.name);
    let response = if item.files.is_empty() {
        client
            .post(url)
            .timeout(Duration::from_secs(u64::from(
                telegram_api_meta::method_timeout_secs(spec),
            )))
            .json(&serde_json::Value::Object(params))
            .send()
            .await
            .map_err(|_| ())?
    } else {
        let mut form = reqwest::multipart::Form::new();
        for (name, value) in params {
            if value.is_null() {
                continue;
            }
            let text = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            form = form.text(name, text);
        }
        for file in item.files {
            let mut bytes = tokio::fs::read(&file.blob_path).await.map_err(|_| ())?;
            // `Part::bytes` takes ownership. Move the original buffer (not a
            // second clone); reqwest owns it until the request is sent.
            let owned = std::mem::take(&mut bytes);
            bytes.zeroize();
            let part = reqwest::multipart::Part::bytes(owned).file_name(
                file.original_filename
                    .unwrap_or_else(|| file.field_name.clone()),
            );
            let part = match file.content_type {
                Some(content_type) => part.mime_str(&content_type).map_err(|_| ())?,
                None => part,
            };
            let field = file.attach_name.unwrap_or(file.field_name);
            form = form.part(field, part);
        }
        client
            .post(url)
            .timeout(Duration::from_secs(u64::from(
                telegram_api_meta::method_timeout_secs(spec),
            )))
            .multipart(form)
            .send()
            .await
            .map_err(|_| ())?
    };
    let status = response.status().as_u16();
    let body = response.json().await.map_err(|_| ())?;
    Ok((status, body))
}

/// Outcome of one spawned Telegram send, reported back to the single dispatch
/// task that owns the WDRR ring and the hierarchical limiters. Scope credits
/// are moved into the sender and back so `Limiters` state is only ever touched
/// by that one task.
struct SendDone {
    bot: BotId,
    scopes: Vec<Scope>,
    /// Telegram 429 `retry_after` seconds when the attempt was a flood answer.
    retry_after: Option<u32>,
    now_unix: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config/operator.defaults.toml".into());
    let (config, master) = OperatorConfig::load(config_path)?;
    let keys = Arc::new(KeyRing::derive(&master, config.key_id.clone())?);
    let store = Store::open(
        config.database_path.clone(),
        config.writer_queue_capacity,
        config.reader_queue_capacity,
    )?;
    let data_root = config
        .database_path
        .parent()
        .unwrap_or(std::path::Path::new("data"))
        .to_path_buf();
    let store = Arc::new(store);
    // Crash cleanup before serving: unreferenced blobs, leftover tmp dirs, and
    // accepting jobs orphaned by an unclean exit (multipart durability is still
    // guaranteed by the temp->fsync->rename protocol for live jobs).
    sweep_orphan_files(&store, &data_root);
    let telegram_base =
        env::var("TELEGRAM_API_BASE").unwrap_or_else(|_| "https://api.telegram.org".into());
    // Optional operator API key: when set, every bot endpoint requires it. The
    // dispatch client never follows redirects (SSRF hardening, matching the
    // webhook deliverer) — a redirect could otherwise send a token-bearing
    // request to an attacker-chosen host.
    let api_key: Option<Arc<str>> = env::var("BULK_API_KEY").ok().map(Arc::from);
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(4)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let readiness = Arc::new(Readiness::ready());
    let app = router(HealthState {
        readiness: readiness.clone(),
        readers: store.readers.clone(),
    })
    .merge(api_router(ApiState {
        store: store.clone(),
        keys: keys.clone(),
        telegram_base: telegram_base.clone(),
        client: client.clone(),
        telegram: Arc::new(Semaphore::new(config.max_telegram_inflight)),
        large_body: Arc::new(Semaphore::new(2)),
        large_ingest: Arc::new(Semaphore::new(1)),
        body_budget: Arc::new(Semaphore::new(64 * 1024 * 1024)),
        max_body: config.request_body_bytes,
        max_multipart_body: config.multipart_body_bytes,
        data_root,
        max_recipients: config.max_recipients_per_job,
        shared_limit: config.shared_parameters_bytes,
        patch_limit: config.recipient_patch_bytes,
        negative_401: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        database_path: config.database_path.clone(),
        http_concurrency: Arc::new(Semaphore::new(config.max_concurrent_http)),
        free_disk_reserve_bytes: config.free_disk_reserve_bytes,
        storage_high_watermark_bytes: config.storage_high_watermark_bytes,
        nonterminal_cap: u32::try_from(config.global_nonterminal_recipients).unwrap_or(u32::MAX),
        api_key,
        allow_private_targets: config.allow_private_targets,
    }));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(config.max_blocking_threads)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let process_epoch = Uuid::now_v7().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        store.writer.execute(WriterCmd::RecoverLeases {
            process_epoch: process_epoch.clone(),
            now_unix: now,
        })?;

        let (shutdown_tx, shutdown_rx1) = tokio::sync::watch::channel(false);
        let shutdown_rx2 = shutdown_rx1.clone();
        let shutdown_rx3 = shutdown_rx1.clone();
        let maintenance_store = store.clone();
        let maintenance_readiness = readiness.clone();
        let wal_truncate_bytes = config.wal_truncate_bytes;
        let retention_sweep_secs = config.retention_sweep_secs;
        let retention_batch = u16::try_from(config.retention_batch).unwrap_or(500);
        let maintenance_epoch = process_epoch.clone();
        let maintenance = tokio::spawn(async move {
            // Writer-owned checkpoint cadence (US-005): PASSIVE every second;
            // forced TRUNCATE at the configured WAL threshold even while grants
            // are paused (this task is independent of dispatch). Retention runs
            // on the same bounded writer queue, so it yields to dispatch I/O.
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut rx = shutdown_rx1;
            let mut last_retention: i64 = i64::MIN;
            let mut tick: u8 = 0;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let now_unix = unix_now();
                        if let Err(e) = maintenance_store.writer.execute(WriterCmd::ExpireDeadline { now_unix }) {
                            maintenance_readiness.set_writer_alive(false);
                            eprintln!("[maintenance] ExpireDeadline failed: {e}");
                        }
                        let wal = wal_bytes(&maintenance_store.path);
                        if wal >= 32 * 1024 * 1024 && wal < wal_truncate_bytes {
                            eprintln!("[maintenance] WAL {wal} bytes >= 32 MiB alarm; TRUNCATE threshold {wal_truncate_bytes}");
                        }
                        let mode = checkpoint_mode(wal, wal_truncate_bytes);
                        if let Err(e) = maintenance_store.writer.execute(WriterCmd::Checkpoint { mode }) {
                            maintenance_readiness.set_writer_alive(false);
                            eprintln!("[maintenance] Checkpoint failed: {e}");
                        }
                        if now_unix.saturating_sub(last_retention) >= retention_sweep_secs as i64 {
                            last_retention = now_unix;
                            if let Err(e) = maintenance_store.writer.execute(WriterCmd::RetentionTick { now_unix, batch: retention_batch }) {
                                maintenance_readiness.set_writer_alive(false);
                                eprintln!("[maintenance] RetentionTick failed: {e}");
                            }
                        }
                        // Requeue recipients whose lease expired without a
                        // completion (abandoned in-flight send): the writer
                        // requeues pre-wire rows and policy-handles the rest.
                        tick = tick.wrapping_add(1);
                        if tick % 5 == 0 {
                            if let Err(e) = maintenance_store.writer.execute(WriterCmd::RequeueExpiredLeases {
                                process_epoch: maintenance_epoch.clone(),
                                now_unix: unix_now(),
                            }) {
                                eprintln!("[maintenance] RequeueExpiredLeases failed: {e}");
                            }
                            let _ = maintenance_store.refresh_metrics();
                            maintenance_store.metrics.set_rss_bytes(rss_bytes());
                            maintenance_store.metrics.set_cgroup_memory_current_bytes(cgroup_memory_current_bytes());
                        }
                    }
                    changed = rx.changed() => if changed.is_err() || *rx.borrow() { break },
                }
            }
        });

        // Persistent dispatch task. A single task owns the WDRR ring and the
        // hierarchical limiters; each granted lease is handed to a bounded
        // spawned sender so a slow upstream never serializes the whole engine.
        // Sends never live inside a `select!` branch future, so a reconcile
        // tick can no longer cancel an in-flight request mid-send.
        let dispatch_store = store.clone();
        let dispatch_epoch = process_epoch.clone();
        let dispatch_keys = keys.clone();
        let max_dispatch_inflight = config.max_telegram_inflight;
        let dispatch = tokio::spawn(async move {
            let mut wdrr = Wdrr::new();
            let monotonic_start = tokio::time::Instant::now();
            let mut limiters = Limiters::new(0);
            let mut reconcile_ticker = tokio::time::interval(Duration::from_millis(RECONCILE_INTERVAL_MS));
            let mut worker_counter: u16 = 0;
            let mut rx = shutdown_rx2;
            let mut shutdown_deadline = None;
            // Conservative pre-lease TTL bound used while the true job method
            // is read (the writer re-derives the exact TTL from it). sendPhoto
            // metadata is committed at build time, so absence is a hard boot
            // failure -- never a silent drop of a granted bot mid-run.
            let lease_spec = telegram_api_meta::method("sendPhoto")
                .expect("sendPhoto is committed Bot API metadata");
            let (done_tx, mut done_rx) =
                mpsc::channel::<SendDone>(max_dispatch_inflight * 2 + 8);
            let mut in_flight: usize = 0;
            // Reload durable Telegram flood pauses (limiter_pauses) persisted
            // by a previous process so a restarted bot stays paused until its
            // 429 retry-after elapses instead of resuming at full rate.
            if let Ok(pauses) = dispatch_store.readers.active_pauses(unix_now()) {
                let boot_mono = monotonic_start.elapsed().as_millis() as u64;
                let boot_unix = unix_now();
                for (scope_key, until_unix) in pauses {
                    if let Ok(scope) = scope_key.parse::<Scope>() {
                        let remain_secs = until_unix.saturating_sub(boot_unix).max(0) as u64;
                        let until_mono = boot_mono.saturating_add(remain_secs.saturating_mul(1000));
                        if until_mono > boot_mono {
                            limiters.pause(&scope, until_mono, boot_mono);
                        }
                    }
                }
            }
            loop {
                if shutdown_deadline.is_some_and(|deadline| monotonic_start.elapsed() >= deadline)
                    || (shutdown_deadline.is_some() && in_flight == 0)
                {
                    break;
                }
                tokio::select! {
                    _ = reconcile_ticker.tick() => {
                        let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                        let now_millis = monotonic_start.elapsed().as_millis() as u64;
                        let now_unix = wall.as_secs() as i64;
                        limiters.evict_idle(now_millis, Limiters::IDLE_BUCKET_TTL_MS);
                        // Reconcile durable ready work into the in-memory WDRR;
                        // this is what makes accepted jobs dispatchable after
                        // both normal operation and same-disk reboot.
                        if let Ok(ready) = dispatch_store.readers.ready_bots(now_unix) {
                            wdrr.reconcile(ready);
                        }
                        // Wake due sleep entries before durable reconciliation;
                        // sleeping bots are deliberately ignored by upsert_ready.
                        let woken: Vec<BotId> = wdrr.wake_due(now_millis);
                        for bot in woken {
                            // readmit preserves the bot's configured fairness
                            // weight (upsert_ready(bot, 1) would reset it).
                            wdrr.readmit(bot);
                        }
                        dispatch_store.metrics.set_ready_bots(wdrr.ready_len() as u64);
                    }
                    done = done_rx.recv() => {
                        if let Some(done) = done {
                            in_flight = in_flight.saturating_sub(1);
                            // Release the scope credits acquired for this send;
                            // this task is the sole owner of the limiters.
                            limiters.release(&done.scopes);
                            match done.retry_after {
                                Some(seconds) => {
                                    dispatch_store.metrics.record_flood_wait();
                                    // Recompute the monotonic clock: the send
                                    // may have taken time, and WDRR wake
                                    // deadlines are monotonic-ms.
                                    let now_ms = monotonic_start.elapsed().as_millis() as u64;
                                    let until_ms = now_ms
                                        .saturating_add(u64::from(seconds) * 1000);
                                    let bot_scope = Scope::Bot(done.bot);
                                    limiters.pause(&bot_scope, until_ms, now_ms);
                                    let _ = dispatch_store.writer.execute(WriterCmd::PauseScope {
                                        scope_key: format!("bot:{}", hex::encode(done.bot.0)),
                                        until_unix: done.now_unix + i64::from(seconds),
                                        retry_after: Some(seconds),
                                        now_unix: done.now_unix,
                                    });
                                    wdrr.complete(done.bot);
                                    wdrr.sleep_until(done.bot, until_ms);
                                }
                                None => {
                                    wdrr.complete(done.bot);
                                }
                            }
                        }
                    }
                    _ = async {
                        if shutdown_deadline.is_some() {
                            // Stop granting new work while draining existing
                            // senders; the completion arm remains active.
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            return;
                        }
                        if in_flight >= max_dispatch_inflight {
                            // All sender slots are busy; park until a completion
                            // frees one (the done arm wakes this loop).
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            return;
                        }
                        let Some(bot_id) = wdrr.grant() else {
                            dispatch_store.metrics.set_ready_bots(wdrr.ready_len() as u64);
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            return;
                        };
                        worker_counter = worker_counter.wrapping_add(1);
                        let now_unix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                        // Lease one recipient for this bot. Lease TTL is
                        // selected after reading the true job method below;
                        // use the conservative multipart (70 s) TTL here so
                        // no file upload can be recovered prematurely.
                        let dispatcher = Dispatcher::new(dispatch_store.clone(), dispatch_epoch.clone());
                        let Ok(Some((job_id, idx, lease_token))) =
                            dispatcher.lease(bot_id, lease_spec, worker_counter, now_unix)
                        else {
                            // Repay the grant() inflight increment before ejecting:
                            // no send was spawned, so nothing will ever call
                            // `complete` for it. Leaking it would drift the bot's
                            // WDRR inflight up by one per drained burst until it
                            // reaches PER_BOT_CONCURRENCY and the bot is never
                            // re-admitted (silent starvation of later jobs).
                            wdrr.complete(bot_id);
                            wdrr.remove(bot_id);
                            return;
                        };
                        let item = dispatch_store.readers.dispatch_item(job_id.clone(), idx).ok().flatten();
                        // Submission validates against committed metadata, so fallback is
                        // only for a corrupt/manual row; keep the scheduler alive rather
                        // than terminating the entire dispatch task.
                        let actual_spec = item
                            .as_ref()
                            .and_then(|i| telegram_api_meta::method(&i.method))
                            .unwrap_or(lease_spec);
                        let chat_id = item.as_ref().and_then(|i| i.chat_id.clone());
                        let group = item
                            .as_ref()
                            .and_then(|i| i.chat_kind.as_deref())
                            .is_some_and(|k| matches!(k, "group" | "supergroup"));
                        let media = telegram_api_meta::is_multipart(actual_spec);
                        let now_millis = monotonic_start.elapsed().as_millis() as u64;
                        let target_rate = item
                            .as_ref()
                            .map(snapshot_target_rate)
                            .unwrap_or(20.0);
                        match limiters.check_and_acquire(bot_id, chat_id.as_deref(), group, actual_spec.name, media, target_rate, now_millis) {
                            Ok(scopes) => {
                                let Some(item) = item else {
                                    // The lease could not be materialized (row vanished or a
                                    // transient read failure). Give the lease back so it is not
                                    // stranded in `leased` until the next boot.
                                    let _ = dispatch_store.writer.execute(WriterCmd::ReleaseLease {
                                        job_id,
                                        idx,
                                        lease_token,
                                        not_before_unix: now_unix,
                                        reason: "dispatch_item_unavailable".into(),
                                    });
                                    limiters.release(&scopes);
                                    wdrr.complete(bot_id);
                                    return;
                                };
                                let (policy, per_bot_base_opt) = snapshot_policy_and_base(&item);
                                in_flight += 1;
                                let keys2 = dispatch_keys.clone();
                                let global_base = telegram_base.clone();
                                let client2 = client.clone();
                                let tx2 = done_tx.clone();
                                let method_spec = actual_spec;
                                // Bounded concurrent sender: mark wire, POST to
                                // Telegram, classify, then report back to the
                                // select loop, which alone re-admits the bot and
                                // releases the limiter credits.
                                let release_store = dispatch_store.clone();
                                tokio::spawn(async move {
                                    let call = LeasedCall {
                                        job_id,
                                        idx,
                                        lease_token,
                                        bot_id,
                                        method: method_spec,
                                        chat_id,
                                        policy,
                                        migrated: false,
                                    };
                                    if let Err(e) = dispatcher.mark_wire_started(&call) {
                                        // The writer could not flag this attempt
                                        // as on-the-wire, so persisting its
                                        // outcome would also fail and recovery
                                        // would treat the row as pre-wire and
                                        // re-send. Do not touch Telegram: give
                                        // the lease back for a clean retry.
                                        eprintln!("mark_wire_started failed: {e}");
                                        let now_ws = unix_now();
                                        let _ = release_store.writer.execute(WriterCmd::ReleaseLease {
                                            job_id: call.job_id,
                                            idx: call.idx,
                                            lease_token: call.lease_token,
                                            not_before_unix: now_ws,
                                            reason: "wire_start_failed".into(),
                                        });
                                        let _ = tx2
                                            .send(SendDone {
                                                bot: bot_id,
                                                scopes,
                                                retry_after: None,
                                                now_unix: now_ws,
                                            })
                                            .await;
                                        return;
                                    }
                                    // Belt-and-braces outer timeout: even if reqwest's own
                                    // deadline is not covering some internal wait, this sender
                                    // is never held hostage past method_timeout+10s and the
                                    // attempt is finished as transient for a bounded retry.
                                    let send_budget = Duration::from_secs(
                                        u64::from(telegram_api_meta::method_timeout_secs(method_spec)) + 10,
                                    );
                                    let attempt = tokio::time::timeout(
                                        send_budget,
                                        send_telegram(&keys2, &global_base, per_bot_base_opt.as_deref(), &client2, bot_id, item),
                                    )
                                    .await;
                                    let result = match attempt {
                                        Ok(r) => r,
                                        Err(_) => Err(()),
                                    };
                                    let now2 = unix_now();
                                    let retry_after = match result {
                                        Ok((status, body)) => dispatcher
                                            .finish(&call, status, Some(&body), false, now2)
                                            .ok()
                                            .flatten(),
                                        Err(()) => {
                                            let _ = dispatcher.finish(&call, 0, None, true, now2);
                                            None
                                        }
                                    };
                                    let _ = tx2
                                        .send(SendDone { bot: bot_id, scopes, retry_after, now_unix: now2 })
                                        .await;
                                });
                            }
                            Err((scope, until)) => {
                                dispatch_store.metrics.record_flood_wait();
                                let ms_until = until.saturating_sub(now_millis);
                                limiters.pause(&scope, until, now_millis);
                                let not_before_unix = unix_now() + i64::try_from(ms_until.div_ceil(1000)).unwrap_or(i64::MAX);
                                let _ = dispatch_store.writer.execute(WriterCmd::ReleaseLease {
                                    job_id,
                                    idx,
                                    lease_token,
                                    not_before_unix,
                                    reason: "local_rate_limit".into(),
                                });
                                wdrr.complete(bot_id);
                                // Sleep the whole bot only when the denied
                                // scope is bot/global -- every recipient needs
                                // those scopes, so pausing the bot is the only
                                // way to stop the dispatch loop from spinning
                                // through the same denial. A chat/group/method
                                // denial is scoped: the recipient is re-leased
                                // at not_before_unix and other chats must keep
                                // flowing while it waits. Sleeping the bot here
                                // would serialize every chat behind the slowest
                                // one.
                                let bot_wide = matches!(scope, Scope::Bot(_) | Scope::Global);
                                if ms_until > 0 && bot_wide {
                                    // `until` is an absolute monotonic-millis deadline, the same
                                    // clock `wake_due` compares against.
                                    wdrr.sleep_until(bot_id, until);
                                } else {
                                    // Re-admit without touching the stored weight.
                                    wdrr.readmit(bot_id);
                                }
                            }
                        }
                    } => {}
                    changed = rx.changed() => {
                        if changed.is_err() || *rx.borrow() {
                            // Stop admitting work and allow current Telegram
                            // requests to report SendDone. Bound the drain so
                            // a hung upstream cannot hold shutdown forever.
                            shutdown_deadline = Some(monotonic_start.elapsed() + Duration::from_secs(30));
                        }
                    },
                }
            }
        });

        // Durable completion webhook deliverer (1 s poll, SSRF-safe).
        let deliverer = WebhookDeliverer::new(
            store.clone(),
            keys.clone(),
            config.max_webhook_inflight,
            WebhookSsrPolicy::default(),
        )?;
        let webhook_task = tokio::spawn(async move { deliverer.run(shutdown_rx3).await });

        let listener = tokio::net::TcpListener::bind(config.bind).await?;
        axum::serve(listener, app).with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; }).await?;
        let _ = shutdown_tx.send(true);
        let _ = maintenance.await;
        let _ = dispatch.await;
        let _ = webhook_task.await;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
