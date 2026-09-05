//! US-005 acceptance tests: incremental retention (terminal only, cascade of
//! terminal webhook events, on-disk blob cleanup), writer-owned checkpoints
//! (PASSIVE cadence + forced TRUNCATE) with live readers and paused grants,
//! disk-full recovery and busy-timeout behaviour against the real SQLite
//! writer, and the Prometheus `/metrics` contract after a completed job.
use axum::response::IntoResponse;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use telegram_bulk_delivery::{
    domain::BotId,
    http::{api_router, ApiState},
    store::{
        open_reader, open_writer, CheckpointMode, CompletionOutcome, FileRow, RecipientInsert,
        Store, StoreError, WebhookMark, WebhookTerminal, WriterCmd, WriterResult,
    },
};
use tokio::sync::Semaphore;
use tower::ServiceExt;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn bot_id(seed: u8) -> BotId {
    let mut b = [0u8; 32];
    b[0] = seed;
    BotId(b)
}

/// Minimal config object matching what the writer's `UpsertConfig` reads.
fn config_json(webhook_url: Option<&str>) -> serde_json::Value {
    json!({
        "target_msgs_per_sec": 5.0,
        "retry_max_attempts": 8,
        "retry_base_ms": 500,
        "retry_max_ms": 60000,
        "retryable_classes_json": "[\"flood\",\"transient\"]",
        "ambiguity_policy": "at_least_once",
        "fairness_weight": 1,
        "job_deadline_secs": 86400,
        "completion_webhook_url": webhook_url,
        "webhook_max_attempts": 10,
        "webhook_retry_base_ms": 1000,
        "webhook_retry_max_ms": 300000
    })
}

fn upsert_bot(store: &Store, bot: BotId) {
    let _ = store
        .writer
        .execute(WriterCmd::UpsertBot {
            bot_id: bot,
            token_nonce: [0u8; 12],
            token_ciphertext: vec![1u8; 16],
            token_kid: "test".into(),
            telegram_user_id: None,
            now_unix: unix_now(),
        })
        .expect("upsert bot");
}

fn set_webhook(store: &Store, bot: BotId, url: Option<&str>) {
    let _ = store
        .writer
        .execute(WriterCmd::UpsertConfig {
            bot_id: bot,
            config_json: config_json(url),
            webhook_secret: None,
            updated_at_unix: unix_now(),
        })
        .expect("upsert config");
}

/// Run the writer DML to durably accept and *complete* a job of `total`
/// recipients. `accepted_at` and the final `sent_at` are caller-controlled so
/// tests can backdate completion/expiry without sleeping.
#[allow(clippy::too_many_arguments)]
fn complete_job(
    store: &Store,
    data_root: &std::path::Path,
    job: &str,
    bot: BotId,
    total: u32,
    accepted_at: i64,
    sent_at: i64,
    with_blob: bool,
) -> PathBuf {
    let _ = store
        .writer
        .execute(WriterCmd::BeginAcceptJob {
            job_id: job.into(),
            bot_id: bot,
            method: "sendMessage".into(),
            policy_snapshot_json: config_json(None).to_string(),
            config_version: 1,
            deadline_unix: Some(accepted_at + 3600),
        })
        .expect("begin accept");
    for chunk in (0..total).collect::<Vec<_>>().chunks(1000) {
        let rows: Vec<RecipientInsert> = chunk
            .iter()
            .map(|&i| RecipientInsert {
                idx: i,
                bot_id: bot,
                patch_json: format!("{{\"text\":\"m{i}\"}}"),
                chat_id: Some(i.to_string()),
                chat_kind: Some("private".into()),
            })
            .collect();
        let _ = store
            .writer
            .execute(WriterCmd::InsertRecipientBatch {
                job_id: job.into(),
                rows,
            })
            .expect("insert recipients");
    }
    let mut file_rows = Vec::new();
    let mut blob: Option<PathBuf> = None;
    if with_blob {
        let p = data_root.join("files").join(job).join("attach");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, b"durable-bytes").unwrap();
        file_rows.push(FileRow {
            file_id: "f1".into(),
            field_name: "document".into(),
            attach_name: Some("document".into()),
            content_type: Some("application/octet-stream".into()),
            original_filename: Some("f.bin".into()),
            sha256: [0u8; 32],
            size_bytes: 13,
            blob_path: p.to_string_lossy().into_owned(),
            retain_until_unix: accepted_at + 604800,
        });
        blob = Some(p);
    }
    let _ = store
        .writer
        .execute(WriterCmd::PromoteJob {
            job_id: job.into(),
            total,
            shared_params_json: "{\"text\":\"shared\"}".into(),
            accepted_at_unix: accepted_at,
            file_rows,
            nonterminal_cap: None,
        })
        .expect("promote");
    for i in 0..total {
        let leased = store
            .writer
            .execute(WriterCmd::LeaseBatch {
                bot_id: bot,
                owner: "us005:0".into(),
                now_unix: accepted_at,
                lease_secs: 30,
                max: 1,
            })
            .expect("lease");
        let (lj, lidx, ltok) = match leased {
            WriterResult::Lease(Some(v)) => v,
            _ => panic!("expected a leaseable recipient"),
        };
        assert_eq!((lj.as_str(), lidx), (job, i));
        let _ = store
            .writer
            .execute(WriterCmd::MarkWireStarted {
                job_id: lj,
                idx: lidx,
                lease_token: ltok,
            })
            .expect("wire started");
        let _ = store
            .writer
            .execute(WriterCmd::CompleteAttempt {
                job_id: job.into(),
                idx: lidx,
                lease_token: ltok,
                outcome: CompletionOutcome::Succeeded {
                    message_id: Some(i64::from(i) + 1),
                    sent_at_unix: sent_at,
                },
            })
            .expect("complete");
    }
    blob.unwrap_or_default()
}

#[test]
fn retention_deletes_expired_terminal_job_with_event_and_blob() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db.sqlite"), 128, 64).unwrap();
    let bot = bot_id(1);
    upsert_bot(&store, bot);
    set_webhook(&store, bot, Some("https://hooks.test/cb"));
    let job = "retained-delivered-job";
    let accepted = unix_now() - 8 * 86400;
    let sent = accepted + 5;
    let blob = complete_job(&store, dir.path(), job, bot, 2, accepted, sent, true);
    assert!(blob.exists(), "blob should exist before retention");

    // Deliver the single completion page so the event/job go terminal.
    let event = format!("{job}-event");
    let _ = store
        .writer
        .execute(WriterCmd::MarkWebhook {
            event_id: event,
            mark: WebhookMark::Attempt {
                page: 0,
                attempt: 1,
                at_unix: sent + 1,
                http_status: Some(200),
                error: None,
                next_attempt_unix: None,
                terminal: Some(WebhookTerminal::Delivered),
            },
        })
        .expect("deliver webhook");
    assert_eq!(
        store
            .readers
            .job(bot, job.into())
            .unwrap()
            .unwrap()
            .webhook_state,
        "delivered"
    );

    let result = store
        .writer
        .execute(WriterCmd::RetentionTick {
            now_unix: unix_now(),
            batch: 500,
        })
        .expect("retention tick");
    match result {
        WriterResult::Retention { rows, files } => {
            assert_eq!(rows, 1, "exactly one terminal job is retained away");
            assert_eq!(files.len(), 1);
        }
        other => panic!("unexpected retention result {other:?}"),
    }
    assert!(store.readers.job(bot, job.into()).unwrap().is_none());
    assert!(!blob.exists(), "retention must delete the on-disk blob");
    store.writer.execute(WriterCmd::Shutdown).unwrap();
}

#[test]
fn retention_skips_jobs_with_pending_webhooks() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db.sqlite"), 128, 64).unwrap();
    let bot = bot_id(2);
    upsert_bot(&store, bot);
    set_webhook(&store, bot, Some("https://hooks.test/cb"));
    let job = "retained-pending-job";
    let accepted = unix_now() - 8 * 86400;
    let sent = accepted + 5;
    let blob = complete_job(&store, dir.path(), job, bot, 1, accepted, sent, true);
    assert!(blob.exists());

    // Webhook event exists and is pending (not delivered) -> must be skipped.
    let result = store
        .writer
        .execute(WriterCmd::RetentionTick {
            now_unix: unix_now(),
            batch: 500,
        })
        .expect("retention tick");
    assert_eq!(
        result,
        WriterResult::Retention {
            rows: 0,
            files: vec![]
        }
    );
    assert!(
        store.readers.job(bot, job.into()).unwrap().is_some(),
        "pending-webhook job must survive retention"
    );
    assert!(blob.exists());
    store.writer.execute(WriterCmd::Shutdown).unwrap();
}

#[test]
fn writer_owned_checkpoints_survive_readers_and_paused_grants() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("db.sqlite"), 128, 64).unwrap());
    let bot = bot_id(3);
    upsert_bot(&store, bot);
    // Simulate "paused grants": recipients delayed into the future, none leased.
    let job = "paused-grants-job";
    let accepted = unix_now() - 3600;
    complete_job(
        &store,
        dir.path(),
        job,
        bot,
        3,
        accepted,
        accepted + 10,
        false,
    );
    // Park the recipients as delayed (grants paused) like a flood pause.
    for i in 0..3u32 {
        let _ = store
            .writer
            .execute(WriterCmd::DelayRecipient {
                job_id: job.into(),
                idx: i,
                not_before_unix: unix_now() + 3600,
                reason: "flood".into(),
            })
            .expect("delay recipient");
    }

    // Readers hammer the pool while writer-owned checkpoints run.
    let reader_store = store.clone();
    let reader_thread = std::thread::spawn(move || {
        let mut pings = 0;
        while pings < 40 {
            if reader_store.readers.ping().is_ok() {
                pings += 1;
            }
        }
    });
    for i in 0..5 {
        let mode = if i == 4 {
            CheckpointMode::Truncate
        } else {
            CheckpointMode::Passive
        };
        let res = store
            .writer
            .execute(WriterCmd::Checkpoint { mode })
            .expect("checkpoint must not fail with readers/delays present");
        assert!(matches!(res, WriterResult::Checkpoint { .. }));
    }
    reader_thread.join().unwrap();
    assert_eq!(store.readers.thread_count(), 4);
    store.writer.execute(WriterCmd::Shutdown).unwrap();
}

#[test]
fn writer_recovers_after_disk_full_and_integrity_holds() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db.sqlite");
    let store = Store::open(db.clone(), 128, 64).unwrap();
    // Cap the database size on the actual writer connection so growth beyond
    // a few pages reports SQLITE_FULL. This exercises the same recovery path
    // as a genuinely full disk without touching the real filesystem.
    store
        .writer
        .execute(WriterCmd::SetMaxPageCount { pages: 96 })
        .unwrap();
    let bot = bot_id(4);
    upsert_bot(&store, bot);

    let job = "disk-full-job";
    let accepted = unix_now() - 60;
    let _ = store
        .writer
        .execute(WriterCmd::BeginAcceptJob {
            job_id: job.into(),
            bot_id: bot,
            method: "sendMessage".into(),
            policy_snapshot_json: config_json(None).to_string(),
            config_version: 1,
            deadline_unix: Some(accepted + 3600),
        })
        .expect("begin accept");
    // Insert until the capped writer reports SQLITE_FULL. A failed batch is
    // rolled back by the writer's transaction helper; the writer thread must
    // remain alive so the operator can free space and recover.
    let mut full_error = None;
    for start in (0u32..40_000).step_by(1000) {
        let rows: Vec<RecipientInsert> = (start..start + 1000)
            .map(|i| RecipientInsert {
                idx: i,
                bot_id: bot,
                patch_json: r#"{"text":"x"}"#.into(),
                chat_id: Some(i.to_string()),
                chat_kind: Some("private".into()),
            })
            .collect();
        if let Err(error) = store.writer.execute(WriterCmd::InsertRecipientBatch {
            job_id: job.into(),
            rows,
        }) {
            full_error = Some(error);
            break;
        }
    }
    let full = full_error.expect("page cap should report disk full");
    assert!(
        full.to_string().contains("full") || full.to_string().contains("disk"),
        "unexpected error: {full}"
    );

    // Recover: lift the cap on the same writer connection, checkpoint, then
    // run integrity_check from a query-only reader.
    store
        .writer
        .execute(WriterCmd::SetMaxPageCount { pages: -1 })
        .expect("lift max-page cap");
    let recovered = store
        .writer
        .execute(WriterCmd::Checkpoint {
            mode: CheckpointMode::Truncate,
        })
        .expect("after lifting the cap the writer must recover");
    assert!(matches!(recovered, WriterResult::Checkpoint { .. }));

    let reader = open_reader(&db).expect("read-only conn");
    let integrity: String = reader
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("integrity_check runs");
    assert_eq!(integrity, "ok");
    store.writer.execute(WriterCmd::Shutdown).unwrap();
}

#[test]
fn writer_busy_timeout_waits_then_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("db.sqlite"), 128, 64).unwrap());
    let bot = bot_id(5);
    upsert_bot(&store, bot);

    // A second writer-capable connection holds RESERVED (BEGIN IMMEDIATE) for
    // ~1.5 s. Writer commands honour the 5 s busy_timeout and must block, then
    // succeed once the lock is released.
    let lock = open_writer(&dir.path().join("db.sqlite")).expect("lock conn");
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let worker_store = store.clone();
    let holder = std::thread::spawn(move || {
        let start = Instant::now();
        let res = worker_store
            .writer
            .execute(WriterCmd::UpsertConfig {
                bot_id: bot,
                config_json: config_json(None),
                webhook_secret: None,
                updated_at_unix: unix_now(),
            })
            .map(|_| start.elapsed());
        tx.send(res).unwrap();
    });
    std::thread::sleep(Duration::from_millis(1500));
    lock.execute_batch("ROLLBACK").unwrap();
    drop(lock);
    let elapsed = rx
        .recv()
        .unwrap()
        .expect("writer command succeeds after lock release");
    holder.join().unwrap();
    assert!(
        elapsed >= Duration::from_millis(900),
        "writer should have blocked on busy_timeout, waited {elapsed:?}"
    );
    store.writer.execute(WriterCmd::Shutdown).unwrap();
}

#[test]
fn thousand_bot_capped_sanity_proves_bounded_fair_progress() {
    use telegram_bulk_delivery::scheduler::fairness::Wdrr;

    // Deterministic short sanity pass used in normal CI. The real 30-minute
    // cgroup run is `scripts/loadtest-1vcpu-soak.sh`; this test verifies the
    // same scheduler invariant without consuming shared-host resources.
    let baseline_rss = telegram_bulk_delivery::observe::rss_bytes();
    let mut wdrr = Wdrr::new();
    for n in 0u32..1000 {
        let mut raw = [0u8; 32];
        raw[..4].copy_from_slice(&n.to_be_bytes());
        wdrr.upsert_ready(BotId(raw), 1);
    }
    let mut grants = vec![0u16; 1000];
    for _ in 0..1000 {
        let bot = wdrr.grant().expect("all bots are runnable");
        let n = u32::from_be_bytes(bot.0[..4].try_into().unwrap()) as usize;
        grants[n] += 1;
        wdrr.complete(bot);
    }
    assert!(
        grants.iter().all(|n| *n == 1),
        "all 1000 bots must progress after 1000 grants"
    );
    assert_eq!(wdrr.ready_len(), 1000);
    let final_rss = telegram_bulk_delivery::observe::rss_bytes();
    assert!(final_rss.saturating_sub(baseline_rss) < 64 * 1024 * 1024);
}

#[test]
fn accepted_json_and_multipart_state_survive_same_disk_reopen() {
    use sha2::{Digest, Sha256};
    use telegram_bulk_delivery::store::files;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("reboot.sqlite");
    let bot = bot_id(7);
    let json_job = "a-json-reboot";
    let multipart_job = "z-multipart-reboot";
    let blob: PathBuf;

    {
        let store = Store::open(db.clone(), 128, 64).unwrap();
        upsert_bot(&store, bot);
        let accepted = unix_now();
        let _ = store
            .writer
            .execute(WriterCmd::BeginAcceptJob {
                job_id: json_job.into(),
                bot_id: bot,
                method: "sendMessage".into(),
                policy_snapshot_json: config_json(None).to_string(),
                config_version: 1,
                deadline_unix: Some(accepted + 3600),
            })
            .unwrap();
        let _ = store
            .writer
            .execute(WriterCmd::InsertRecipientBatch {
                job_id: json_job.into(),
                rows: vec![RecipientInsert {
                    idx: 0,
                    bot_id: bot,
                    patch_json: r#"{"chat_id":1}"#.into(),
                    chat_id: Some("1".into()),
                    chat_kind: Some("private".into()),
                }],
            })
            .unwrap();
        let _ = store
            .writer
            .execute(WriterCmd::PromoteJob {
                job_id: json_job.into(),
                total: 1,
                shared_params_json: r#"{"text":"json"}"#.into(),
                accepted_at_unix: accepted,
                file_rows: vec![],

                nonterminal_cap: None,
            })
            .unwrap();

        let bytes = b"multipart survives process restart";
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let durable = files::persist(dir.path(), multipart_job, "document", bytes).unwrap();
        assert_eq!(durable.sha256, digest);
        blob = durable.path.clone();
        let _ = store
            .writer
            .execute(WriterCmd::BeginAcceptJob {
                job_id: multipart_job.into(),
                bot_id: bot,
                method: "sendDocument".into(),
                policy_snapshot_json: config_json(None).to_string(),
                config_version: 1,
                deadline_unix: Some(accepted + 3600),
            })
            .unwrap();
        let _ = store
            .writer
            .execute(WriterCmd::InsertRecipientBatch {
                job_id: multipart_job.into(),
                rows: vec![RecipientInsert {
                    idx: 0,
                    bot_id: bot,
                    patch_json: r#"{"chat_id":2}"#.into(),
                    chat_id: Some("2".into()),
                    chat_kind: Some("private".into()),
                }],
            })
            .unwrap();
        let _ = store
            .writer
            .execute(WriterCmd::PromoteJob {
                job_id: multipart_job.into(),
                total: 1,
                shared_params_json: r#"{}"#.into(),
                accepted_at_unix: accepted,
                file_rows: vec![FileRow {
                    file_id: "document".into(),
                    field_name: "document".into(),
                    attach_name: Some("document".into()),
                    content_type: Some("application/octet-stream".into()),
                    original_filename: Some("document.bin".into()),
                    sha256: digest,
                    size_bytes: bytes.len() as u64,
                    blob_path: blob.to_string_lossy().into_owned(),
                    retain_until_unix: accepted + 604800,
                }],

                nonterminal_cap: None,
            })
            .unwrap();
    }

    let reopened = Store::open(db.clone(), 128, 64).unwrap();
    reopened
        .writer
        .execute(WriterCmd::RecoverLeases {
            process_epoch: "new-process-epoch".into(),
            now_unix: unix_now(),
        })
        .unwrap();
    let ready = reopened.readers.ready_bots(unix_now()).unwrap();
    assert_eq!(ready.len(), 1, "both jobs share one ready bot");
    assert!(reopened
        .readers
        .job(bot, json_job.into())
        .unwrap()
        .is_some());
    assert!(reopened
        .readers
        .job(bot, multipart_job.into())
        .unwrap()
        .is_some());
    assert_eq!(
        std::fs::read(&blob).unwrap(),
        b"multipart survives process restart"
    );

    let first = reopened
        .writer
        .execute(WriterCmd::LeaseBatch {
            bot_id: bot,
            owner: "new-process-epoch:1".into(),
            now_unix: unix_now(),
            lease_secs: 70,
            max: 1,
        })
        .unwrap();
    let (first_job, first_idx, first_token) = match first {
        WriterResult::Lease(Some(v)) => v,
        _ => panic!("expected a leaseable recipient after reboot"),
    };
    // Complete the json recipient only if THIS lease holds it (a completion
    // must carry the matching attempt token); the second lease below then
    // materializes whichever recipient remains eligible.
    if first_job == json_job && first_idx == 0 {
        let _ = reopened
            .writer
            .execute(WriterCmd::CompleteAttempt {
                job_id: first_job,
                idx: first_idx,
                lease_token: first_token,
                outcome: CompletionOutcome::Succeeded {
                    message_id: Some(1),
                    sent_at_unix: unix_now(),
                },
            })
            .unwrap();
    }
    let _ = reopened
        .writer
        .execute(WriterCmd::LeaseBatch {
            bot_id: bot,
            owner: "new-process-epoch:2".into(),
            now_unix: unix_now(),
            lease_secs: 70,
            max: 1,
        })
        .unwrap();
    let item = reopened
        .readers
        .dispatch_item(multipart_job.into(), 0)
        .unwrap()
        .expect("multipart is lease-materialized after reboot");
    assert_eq!(item.files.len(), 1);
    assert_eq!(item.files[0].blob_path, blob.to_string_lossy());

    let conn = open_reader(&db).unwrap();
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    reopened.writer.execute(WriterCmd::Shutdown).unwrap();
}

// ---------------------------------------------------------------------------
// /metrics contract
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_expose_required_series_after_a_job() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(temp.path().join("queue.db"), 128, 64).unwrap());

    // Token-validation failure through the real HTTP path (bot "bad" -> 401).
    let upstream_calls = Arc::new(std::sync::Mutex::new(0usize));
    let c = upstream_calls.clone();
    let upstream = axum::Router::new().route(
        "/bot{token}/getMe",
        axum::routing::get(
            move |axum::extract::Path(token): axum::extract::Path<String>| {
                let c = c.clone();
                async move {
                    *c.lock().unwrap() += 1;
                    if token == "bad" {
                        (
                            axum::http::StatusCode::UNAUTHORIZED,
                            axum::Json(json!({"ok": false})),
                        )
                            .into_response()
                    } else {
                        axum::Json(json!({"ok": true, "result": {"id": 1}})).into_response()
                    }
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

    let keys = Arc::new(telegram_bulk_delivery::auth::KeyRing::derive(&[7; 32], "test").unwrap());
    let state = ApiState {
        store: store.clone(),
        keys,
        telegram_base: format!("http://{address}"),
        client: reqwest::Client::new(),
        telegram: Arc::new(Semaphore::new(1)),
        large_body: Arc::new(Semaphore::new(2)),
        large_ingest: Arc::new(Semaphore::new(1)),
        body_budget: Arc::new(Semaphore::new(64 * 1024 * 1024)),
        max_body: 32 * 1024 * 1024,
        max_multipart_body: 64 * 1024 * 1024,
        data_root: temp.path().to_path_buf(),
        max_recipients: 100_000,
        shared_limit: 65536,
        patch_limit: 8192,
        negative_401: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        database_path: temp.path().join("queue.db"),
        http_concurrency: Arc::new(Semaphore::new(64)),
        free_disk_reserve_bytes: 0,
        storage_high_watermark_bytes: u64::MAX,
        nonterminal_cap: 1_000_000,
        api_key: None,
        allow_private_targets: true,
    };
    let app = api_router(state);
    let bad = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/botbad/getBulkDeliveryConfig")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 401);

    // Complete a job through the writer so job_wallclock has a real sample.
    let bot = bot_id(6);
    upsert_bot(&store, bot);
    let now = unix_now();
    complete_job(
        &store,
        temp.path(),
        "metrics-job",
        bot,
        1,
        now - 20,
        now - 15,
        false,
    );
    store.refresh_metrics().unwrap();
    store.metrics.set_rss_bytes(1234);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();

    for expected in [
        "token_validation_failures_total 1",
        "ingest_accepting_jobs 0",
        "job_wallclock_seconds_count 1",
        "job_wallclock_seconds_bucket{le=\"10\"} 1",
        "oldest_nonterminal_recipient_age_seconds 0",
        "cgroup_memory_current_bytes 0",
        "process_rss_bytes 1234",
        "telegram_outcomes_total{class=\"succeeded\"} 1",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
    }
    // No token/high-cardinality leakage.
    for leak in ["bad", "metrics-job", "shared", "chat_id"] {
        assert!(!text.contains(leak), "unexpected leak {leak:?} in:\n{text}");
    }
    store.writer.execute(WriterCmd::Shutdown).unwrap();
}

#[test]
fn promote_enforces_global_nonterminal_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db.sqlite"), 128, 64).unwrap();
    let bot = bot_id(9);
    upsert_bot(&store, bot);
    let begin = |job: &str| {
        let _ = store
            .writer
            .execute(WriterCmd::BeginAcceptJob {
                job_id: job.into(),
                bot_id: bot,
                method: "sendMessage".into(),
                policy_snapshot_json: config_json(None).to_string(),
                config_version: 1,
                deadline_unix: Some(unix_now() + 3600),
            })
            .expect("begin accept");
    };
    let insert = |job: &str, total: u32| {
        let rows: Vec<RecipientInsert> = (0..total)
            .map(|i| RecipientInsert {
                idx: i,
                bot_id: bot,
                patch_json: "{\"text\":\"m\"}".into(),
                chat_id: Some(i.to_string()),
                chat_kind: Some("private".into()),
            })
            .collect();
        let _ = store
            .writer
            .execute(WriterCmd::InsertRecipientBatch {
                job_id: job.into(),
                rows,
            })
            .expect("insert");
    };
    // 4 staged recipients against a ceiling of 3 must be refused atomically and
    // leave the job accepting (the abort path then cascades the rows away).
    begin("cap-job");
    insert("cap-job", 4);
    let err = store
        .writer
        .execute(WriterCmd::PromoteJob {
            job_id: "cap-job".into(),
            total: 4,
            shared_params_json: "{\"text\":\"shared\"}".into(),
            accepted_at_unix: unix_now(),
            file_rows: vec![],
            nonterminal_cap: Some(3),
        })
        .expect_err("promote must refuse above the ceiling");
    assert!(matches!(err, StoreError::CapacityExceeded(_)));
    let _ = store
        .writer
        .execute(WriterCmd::AbortAcceptJob {
            job_id: "cap-job".into(),
        })
        .expect("abort");
    // A job that fits the budget promotes normally.
    begin("ok-job");
    insert("ok-job", 2);
    let _ = store
        .writer
        .execute(WriterCmd::PromoteJob {
            job_id: "ok-job".into(),
            total: 2,
            shared_params_json: "{\"text\":\"shared\"}".into(),
            accepted_at_unix: unix_now(),
            file_rows: vec![],
            nonterminal_cap: Some(3),
        })
        .expect("promote within ceiling");
    let _ = store
        .writer
        .execute(WriterCmd::AbortAcceptJob {
            job_id: "ok-job".into(),
        })
        .expect("abort ok job");
}
