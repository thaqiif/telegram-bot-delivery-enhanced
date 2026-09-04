use super::{migrate, open_writer, ReadPool, StoreError};
use crate::{
    domain::BotId,
    observe::{Metrics, Outcome},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread::{self, JoinHandle},
};

#[derive(Debug, Clone)]
pub struct RecipientInsert {
    pub idx: u32,
    pub bot_id: BotId,
    pub patch_json: String,
    pub chat_id: Option<String>,
    pub chat_kind: Option<String>,
}
#[derive(Debug, Clone)]
pub struct FileRow {
    pub file_id: String,
    pub field_name: String,
    pub attach_name: Option<String>,
    pub content_type: Option<String>,
    pub original_filename: Option<String>,
    pub sha256: [u8; 32],
    pub size_bytes: u64,
    pub blob_path: String,
    pub retain_until_unix: i64,
}
#[derive(Debug, Clone)]
pub enum CompletionOutcome {
    Succeeded {
        message_id: Option<i64>,
        sent_at_unix: i64,
    },
    Failed {
        error_norm: String,
    },
    Ambiguous,
    Delayed {
        not_before_unix: i64,
        error_norm: String,
        consume_retry: bool,
    },
}
#[derive(Debug, Clone, Copy)]
pub enum CheckpointMode {
    Passive,
    Truncate,
}
#[derive(Debug, Clone)]
pub enum WebhookTerminal {
    Delivered,
    Exhausted { error: String },
}

/// Record one completed HTTP attempt for one webhook page.
/// `attempt` is 1-based. `next_attempt_unix: Some(t)` schedules a retry;
/// `terminal` marks the page (and possibly event/job) terminal.
#[derive(Debug, Clone)]
pub enum WebhookMark {
    Attempt {
        page: u16,
        attempt: u16,
        at_unix: i64,
        http_status: Option<u16>,
        error: Option<String>,
        next_attempt_unix: Option<i64>,
        terminal: Option<WebhookTerminal>,
    },
}

#[derive(Debug, Clone)]
pub enum WriterCmd {
    BeginAcceptJob {
        job_id: String,
        bot_id: BotId,
        method: String,
        policy_snapshot_json: String,
        config_version: u64,
        deadline_unix: Option<i64>,
    },
    InsertRecipientBatch {
        job_id: String,
        rows: Vec<RecipientInsert>,
    },
    PromoteJob {
        job_id: String,
        total: u32,
        shared_params_json: String,
        accepted_at_unix: i64,
        file_rows: Vec<FileRow>,
        /// Global ceiling on non-terminal recipients (queued/delayed/leased),
        /// enforced atomically inside the promote transaction. `None` disables
        /// the check (used by direct-writer tests).
        nonterminal_cap: Option<u32>,
    },
    AbortAcceptJob {
        job_id: String,
    },
    LeaseBatch {
        bot_id: BotId,
        owner: String,
        now_unix: i64,
        /// Conservative upper bound supplied by the scheduler; the writer
        /// derives the exact 30 s JSON / 70 s multipart TTL from the selected
        /// job's method metadata before committing the lease.
        lease_secs: u16,
        max: u8,
    },
    DelayRecipient {
        job_id: String,
        idx: u32,
        not_before_unix: i64,
        reason: String,
    },
    /// Release an unwritten lease back to delayed state when an in-memory
    /// limiter denies it. This keeps persisted job counters and recipient state
    /// consistent; no Telegram bytes have been written yet.
    ReleaseLease {
        job_id: String,
        idx: u32,
        not_before_unix: i64,
        reason: String,
        lease_token: i64,
    },
    MarkWireStarted {
        job_id: String,
        idx: u32,
        lease_token: i64,
    },
    MigrateRecipient {
        job_id: String,
        idx: u32,
        old_chat_id: String,
        new_chat_id: String,
        lease_token: i64,
    },
    CompleteAttempt {
        job_id: String,
        idx: u32,
        outcome: CompletionOutcome,
        lease_token: i64,
    },
    RecoverLeases {
        process_epoch: String,
        now_unix: i64,
    },
    /// Periodic equivalent of `RecoverLeases` for the live process: requeues
    /// recipients whose lease expired without a completion, so an abandoned
    /// in-flight send (task preempted/cancelled mid-request) is retried instead
    /// of waiting for the next restart. Rows that never reached the wire
    /// (`wire_started=0`) go straight back to queued; rows that may have
    /// reached Telegram follow the job's ambiguity policy.
    RequeueExpiredLeases {
        process_epoch: String,
        now_unix: i64,
    },
    ExpireDeadline {
        now_unix: i64,
    },
    Checkpoint {
        mode: CheckpointMode,
    },
    UpsertConfig {
        bot_id: BotId,
        config_json: Value,
        webhook_secret: Option<([u8; 12], Vec<u8>, String)>,
        updated_at_unix: i64,
    },
    UpsertBot {
        bot_id: BotId,
        token_nonce: [u8; 12],
        token_ciphertext: Vec<u8>,
        token_kid: String,
        telegram_user_id: Option<i64>,
        now_unix: i64,
    },
    FailBotUnauthorized {
        bot_id: BotId,
        now_unix: i64,
    },
    PauseScope {
        scope_key: String,
        until_unix: i64,
        retry_after: Option<u32>,
        now_unix: i64,
    },
    MarkWebhook {
        event_id: String,
        mark: WebhookMark,
    },
    RetentionTick {
        now_unix: i64,
        batch: u16,
    },
    /// SQLite `PRAGMA max_page_count` on the writer's own connection. This is
    /// the writer-owned way to bound main-db growth (used by the disk-full
    /// drill; `-1` restores the unlimited default). All post-migration SQLite
    /// pragma mutations belong on the writer thread.
    SetMaxPageCount {
        pages: i64,
    },
    Backup {
        destination: PathBuf,
    },
    PersistScheduler {
        bot_id: BotId,
        deficit: i64,
        weight: u8,
        last_grant_unix: i64,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriterResult {
    Done,
    Count(usize),
    Lease(Option<(String, u32, i64)>),
    Checkpoint {
        busy: i64,
        log: i64,
        checkpointed: i64,
    },
    /// Terminal jobs deleted by incremental retention, plus the on-disk blob
    /// paths removed after commit (best-effort; leftovers are swept at boot).
    Retention {
        rows: usize,
        files: Vec<String>,
    },
}
struct Request {
    command: WriterCmd,
    reply: mpsc::Sender<Result<WriterResult, StoreError>>,
}

pub struct WriterHandle {
    sender: mpsc::SyncSender<Request>,
    join: Option<JoinHandle<()>>,
    /// Number of commands enqueued but not yet answered (queue-depth gauge).
    depth: Arc<AtomicUsize>,
    /// OS thread name captured from the writer thread at bootstrap.
    name: String,
}
impl WriterHandle {
    pub fn execute(&self, command: WriterCmd) -> Result<WriterResult, StoreError> {
        self.depth.fetch_add(1, Ordering::SeqCst);
        let result = (|| {
            let (reply, response) = mpsc::channel();
            self.sender
                .send(Request { command, reply })
                .map_err(|_| StoreError::WorkerUnavailable)?;
            response.recv().map_err(|_| StoreError::ResponseClosed)?
        })();
        self.depth.fetch_sub(1, Ordering::SeqCst);
        result
    }
    pub fn depth(&self) -> u64 {
        self.depth.load(Ordering::Relaxed) as u64
    }
    /// OS thread name of the writer thread (captured at bootstrap).
    pub fn thread_name(&self) -> &str {
        &self.name
    }
}
impl Drop for WriterHandle {
    fn drop(&mut self) {
        let _ = self.execute(WriterCmd::Shutdown);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

pub struct Store {
    pub writer: WriterHandle,
    pub readers: Arc<ReadPool>,
    pub path: PathBuf,
    pub metrics: Arc<Metrics>,
}
impl Store {
    pub fn open(
        path: PathBuf,
        writer_capacity: usize,
        reader_capacity: usize,
    ) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|_| StoreError::WorkerUnavailable)?;
        }
        let mut bootstrap = open_writer(&path)?;
        if super::current_version(&bootstrap)? < super::LATEST_MIGRATION_VERSION {
            migrate(&mut bootstrap)?;
        }
        drop(bootstrap);
        let (sender, receiver) = mpsc::sync_channel::<Request>(writer_capacity);
        let metrics = Arc::new(Metrics::new());
        let writer_metrics = metrics.clone();
        let depth = Arc::new(AtomicUsize::new(0));
        let writer_path = path.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel::<(Result<(), StoreError>, String)>(1);
        let join = thread::Builder::new()
            .name("sqlite-writer".into())
            .spawn(move || writer_loop(writer_path, receiver, ready_tx, writer_metrics))
            .map_err(|_| StoreError::WorkerUnavailable)?;
        let (writer_ready, writer_name) =
            ready_rx.recv().map_err(|_| StoreError::WorkerUnavailable)?;
        writer_ready?;
        let readers = ReadPool::start(path.clone(), reader_capacity)?;
        Ok(Self {
            writer: WriterHandle {
                sender,
                join: Some(join),
                depth: depth.clone(),
                name: writer_name,
            },
            readers,
            path,
            metrics,
        })
    }

    /// One maintenance snapshot: apply DB-derived gauges, absorb newly
    /// completed jobs into the wall-clock histogram, and refresh the cheap
    /// process-local gauges (WAL bytes, writer queue depth).
    pub fn refresh_metrics(&self) -> Result<(), StoreError> {
        let db = self.readers.snapshot_gauges()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.metrics.apply_db(&db, now);
        let (second, job) = self.metrics.wallclock_cursor();
        let rows = self.readers.completed_jobs(second, job)?;
        if !rows.is_empty() {
            self.metrics.record_completed(&rows);
        }
        let wal_bytes = std::fs::metadata(format!("{}-wal", self.path.display()))
            .map(|m| m.len())
            .unwrap_or(0);
        self.metrics.set_wal_bytes(wal_bytes);
        self.metrics.set_writer_queue_depth(self.writer.depth());
        Ok(())
    }
}
fn writer_loop(
    path: PathBuf,
    receiver: mpsc::Receiver<Request>,
    ready: mpsc::SyncSender<(Result<(), StoreError>, String)>,
    metrics: Arc<Metrics>,
) {
    let thread_name = std::thread::current()
        .name()
        .unwrap_or("sqlite-writer")
        .to_owned();
    let mut conn = match open_writer(&path) {
        Ok(conn) => {
            let _ = ready.send((Ok(()), thread_name));
            conn
        }
        Err(error) => {
            let _ = ready.send((Err(error), thread_name));
            return;
        }
    };
    while let Ok(request) = receiver.recv() {
        let shutdown = matches!(request.command, WriterCmd::Shutdown);
        // Observe the outcome of the send this command records (Delayed = a
        // retry has just been scheduled for that recipient).
        let observed = match &request.command {
            WriterCmd::CompleteAttempt { outcome, .. } => Some(match outcome {
                CompletionOutcome::Succeeded { .. } => Outcome::Succeeded,
                CompletionOutcome::Failed { .. } => Outcome::Failed,
                CompletionOutcome::Ambiguous => Outcome::Ambiguous,
                CompletionOutcome::Delayed { .. } => Outcome::Delayed,
            }),
            _ => None,
        };
        let result = execute(&mut conn, request.command);
        if let Ok(WriterResult::Retention { rows, .. }) = &result {
            metrics.record_retention_rows(*rows as u64);
        }
        if let Some(outcome) = observed {
            if result.is_ok() {
                metrics.record_outcome(outcome);
                if outcome == Outcome::Delayed {
                    metrics.record_retry_scheduled();
                }
            }
        }
        let _ = request.reply.send(result);
        if shutdown {
            break;
        }
    }
}
fn transaction<T>(
    conn: &mut Connection,
    operation: impl FnOnce(&Connection) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    match operation(conn) {
        Ok(value) => match conn.execute_batch("COMMIT") {
            Ok(()) => Ok(value),
            Err(commit_error) => {
                // A COMMIT that fails may leave the transaction open; roll it
                // back so the writer connection is not wedged for every later
                // BEGIN IMMEDIATE.
                let _ = conn.execute_batch("ROLLBACK");
                Err(commit_error.into())
            }
        },
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}
fn execute(conn: &mut Connection, command: WriterCmd) -> Result<WriterResult, StoreError> {
    match command {
        WriterCmd::BeginAcceptJob {
            job_id,
            bot_id,
            method,
            policy_snapshot_json,
            config_version,
            deadline_unix,
        } => transaction(conn, |c| {
            c.execute("INSERT INTO jobs(job_id,bot_id,method,method_canonical,shared_params_json,state,total,queued,inflight,delayed,succeeded,failed,ambiguous,policy_snapshot_json,config_version,deadline_unix,webhook_state) VALUES(?1,?2,?3,lower(?3),'{}','accepting',0,0,0,0,0,0,0,?4,?5,?6,'not_configured')", params![job_id,bot_id.0.as_slice(),method,policy_snapshot_json,i64::try_from(config_version).map_err(|_| StoreError::InvalidCommand("config version exceeds sqlite integer"))?,deadline_unix])?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::InsertRecipientBatch { job_id, rows } => {
            if rows.len() > 1000 {
                return Err(StoreError::InvalidCommand("recipient batch exceeds 1000"));
            }
            transaction(conn, |c| {
                let mut stmt=c.prepare("INSERT INTO recipients(job_id,idx,bot_id,patch_json,chat_id,chat_kind,state) VALUES(?1,?2,?3,?4,?5,?6,'queued')")?;
                for row in &rows {
                    stmt.execute(params![
                        job_id,
                        row.idx,
                        row.bot_id.0.as_slice(),
                        row.patch_json,
                        row.chat_id,
                        row.chat_kind
                    ])?;
                }
                Ok(WriterResult::Count(rows.len()))
            })
        }
        WriterCmd::PromoteJob {
            job_id,
            total,
            shared_params_json,
            accepted_at_unix,
            file_rows,
            nonterminal_cap,
        } => transaction(conn, |c| {
            // Operator ceiling: refuse to promote when the store would exceed
            // the configured global non-terminal recipient budget. Recipients
            // are inserted as `queued` while their job is still `accepting`, so
            // this count already covers every staged row in the system.
            if let Some(cap) = nonterminal_cap {
                let held: i64 = c.query_row(
                    "SELECT COUNT(*) FROM recipients WHERE state IN ('queued','delayed','leased')",
                    [],
                    |r| r.get(0),
                )?;
                if held > i64::from(cap) {
                    return Err(StoreError::CapacityExceeded(
                        "global_nonterminal_recipients",
                    ));
                }
            }
            let changed=c.execute("UPDATE jobs SET state='queued',total=?2,queued=?2,shared_params_json=?3,accepted_at_unix=?4,webhook_state=CASE WHEN EXISTS(SELECT 1 FROM bot_configs WHERE bot_id=jobs.bot_id AND webhook_url IS NOT NULL) THEN 'pending' ELSE 'not_configured' END WHERE job_id=?1 AND state='accepting'",params![job_id,total,shared_params_json,accepted_at_unix])?;
            if changed != 1 {
                return Err(StoreError::InvalidCommand("job is not accepting"));
            }
            let mut stmt=c.prepare("INSERT INTO job_files(file_id,job_id,field_name,attach_name,content_type,original_filename,sha256,size_bytes,blob_path,retain_until_unix) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)")?;
            for f in &file_rows {
                stmt.execute(params![
                    f.file_id,
                    job_id,
                    f.field_name,
                    f.attach_name,
                    f.content_type,
                    f.original_filename,
                    f.sha256.as_slice(),
                    i64::try_from(f.size_bytes).map_err(|_| StoreError::InvalidCommand(
                        "file size exceeds sqlite integer"
                    ))?,
                    f.blob_path,
                    f.retain_until_unix
                ])?;
            }
            Ok(WriterResult::Done)
        }),
        WriterCmd::AbortAcceptJob { job_id } => transaction(conn, |c| {
            Ok(WriterResult::Count(c.execute(
                "DELETE FROM jobs WHERE job_id=?1 AND state='accepting'",
                [job_id],
            )?))
        }),
        WriterCmd::LeaseBatch {
            bot_id,
            owner,
            now_unix,
            lease_secs,
            max,
        } => {
            if max != 1 {
                return Err(StoreError::InvalidCommand(
                    "v1 leases exactly one recipient",
                ));
            }
            transaction(conn, |c| {
                // The row may currently be `queued` (fresh) or `delayed` (a
                // limiter backoff that has come due): move exactly one unit out
                // of whichever ledger column holds it, so job counters never go
                // negative or double-count across deny/release cycles.
                let candidate:Option<(String,u32,String,String)>=c.query_row("SELECT r.job_id,r.idx,j.method,r.state FROM recipients r JOIN jobs j ON j.job_id=r.job_id WHERE r.bot_id=?1 AND r.state IN ('queued','delayed') AND r.not_before_unix<=?2 AND j.state IN ('queued','running') ORDER BY r.not_before_unix,r.job_id,r.idx LIMIT 1",params![bot_id.0.as_slice(),now_unix],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
                // Every lease issues a fresh, strictly increasing per-recipient
                // token. Attempt mutations carry it, so a completion from a
                // sender whose lease was reclaimed and re-leased can never land
                // on the newer attempt (its guard requires this lease's token).
                let lease: Option<(String, u32, i64)> = if let Some((job, idx, method, state)) =
                    &candidate
                {
                    let exact_ttl = telegram_api_meta::method(method)
                        .map(telegram_api_meta::lease_ttl_secs)
                        .unwrap_or(lease_secs);
                    c.execute("UPDATE recipients SET state='leased',lease_owner=?3,lease_until_unix=?4,lease_token=lease_token+1 WHERE job_id=?1 AND idx=?2",params![job,idx,owner,now_unix+i64::from(exact_ttl)])?;
                    c.execute("UPDATE jobs SET state='running',inflight=inflight+1,queued=queued-CASE WHEN ?3='queued' THEN 1 ELSE 0 END,delayed=delayed-CASE WHEN ?3='delayed' THEN 1 ELSE 0 END,started_at_unix=coalesce(started_at_unix,?2) WHERE job_id=?1",params![job,now_unix,state])?;
                    let token: i64 = c.query_row(
                        "SELECT lease_token FROM recipients WHERE job_id=?1 AND idx=?2",
                        params![job, idx],
                        |r| r.get(0),
                    )?;
                    Some((job.clone(), *idx, token))
                } else {
                    None
                };
                Ok(WriterResult::Lease(lease))
            })
        }
        WriterCmd::DelayRecipient {
            job_id,
            idx,
            not_before_unix,
            reason,
        } => transaction(conn, |c| {
            c.execute("UPDATE recipients SET state='delayed',not_before_unix=?3,limiter_scope_hint=?4 WHERE job_id=?1 AND idx=?2 AND state IN ('queued','delayed')",params![job_id,idx,not_before_unix,reason])?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::ReleaseLease {
            job_id,
            idx,
            not_before_unix,
            reason,
            lease_token,
        } => {
            transaction(conn, |c| {
                let n = c.execute("UPDATE recipients SET state='delayed',not_before_unix=?3,limiter_scope_hint=?4,lease_owner=NULL,lease_until_unix=NULL WHERE job_id=?1 AND idx=?2 AND state='leased' AND wire_started=0 AND lease_token=?5",params![job_id,idx,not_before_unix,reason,lease_token])?;
                if n == 1 {
                    c.execute("UPDATE jobs SET inflight=max(inflight-1,0),delayed=delayed+1 WHERE job_id=?1", [&job_id])?;
                }
                Ok(WriterResult::Count(n))
            })
        }
        WriterCmd::MarkWireStarted {
            job_id,
            idx,
            lease_token,
        } => transaction(conn, |c| {
            c.execute("UPDATE recipients SET wire_started=1 WHERE job_id=?1 AND idx=?2 AND state='leased' AND lease_token=?3",params![job_id,idx,lease_token])?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::MigrateRecipient {
            job_id,
            idx,
            old_chat_id,
            new_chat_id,
            lease_token,
        } => transaction(conn, |c| {
            c.execute("UPDATE recipients SET chat_id=?4,chat_kind='supergroup',limiter_scope_hint='migrated_from:'||?3,wire_started=0 WHERE job_id=?1 AND idx=?2 AND state='leased' AND chat_id=?3 AND lease_token=?5", params![job_id,idx,old_chat_id,new_chat_id,lease_token])?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::CompleteAttempt {
            job_id,
            idx,
            outcome,
            lease_token,
        } => transaction(conn, |c| {
            let (state, error, not_before, consume, msg, sent) = match outcome {
                CompletionOutcome::Succeeded {
                    message_id,
                    sent_at_unix,
                } => (
                    "succeeded",
                    None,
                    None,
                    true,
                    message_id,
                    Some(sent_at_unix),
                ),
                CompletionOutcome::Failed { error_norm } => {
                    ("failed", Some(error_norm), None, true, None, None)
                }
                CompletionOutcome::Ambiguous => (
                    "ambiguous",
                    Some("ambiguous".into()),
                    None,
                    true,
                    None,
                    None,
                ),
                CompletionOutcome::Delayed {
                    not_before_unix,
                    error_norm,
                    consume_retry,
                } => (
                    "delayed",
                    Some(error_norm),
                    Some(not_before_unix),
                    consume_retry,
                    None,
                    None,
                ),
            };
            let moved = c.execute("UPDATE recipients SET state=?3,attempt_count=attempt_count+1,retry_consumed=retry_consumed+?4,error_norm=?5,not_before_unix=coalesce(?6,not_before_unix),telegram_message_id=?7,time_send_unix=?8,lease_owner=NULL,lease_until_unix=NULL,wire_started=0 WHERE job_id=?1 AND idx=?2 AND state='leased' AND lease_token=?9",params![job_id,idx,state,i64::from(consume),error,not_before,msg,sent,lease_token])?;
            if moved == 0 {
                // Stale completion for a recipient that is no longer leased
                // (e.g. recovered by an earlier expiry). Nothing to move.
                return Ok(WriterResult::Done);
            }
            // Wall-clock now when Telegram did not return a send timestamp, so a
            // job that reaches terminal on a failed final attempt is stamped with
            // a real completion/expiry time instead of unix 0 (which would let
            // retention delete it immediately rather than after seven days).
            let finished_at = sent.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64
            });
            // Incremental O(1) job-ledger update for this single recipient
            // instead of a full per-send rescan of the job's recipients (which
            // would be O(n^2) across a large job). Lease/release transitions keep
            // queued/inflight/delayed exact, so only this attempt's state moves.
            c.execute("UPDATE jobs SET inflight=max(inflight-1,0),succeeded=succeeded+CASE WHEN ?2='succeeded' THEN 1 ELSE 0 END,failed=failed+CASE WHEN ?2='failed' THEN 1 ELSE 0 END,ambiguous=ambiguous+CASE WHEN ?2='ambiguous' THEN 1 ELSE 0 END,delayed=delayed+CASE WHEN ?2='delayed' THEN 1 ELSE 0 END WHERE job_id=?1 AND state IN('queued','running')",params![job_id,state])?;
            if state != "delayed" {
                // Delayed is a scheduled retry, not a completed send: it must
                // not inflate the rate sample that drives the ETA.
                c.execute("INSERT INTO rate_samples(job_id,window_start_unix,sent) VALUES(?1,?2,1) ON CONFLICT(job_id,window_start_unix) DO UPDATE SET sent=sent+1",params![job_id,finished_at])?;
                let (total, terminal): (i64, i64) = c.query_row(
                    "SELECT total, succeeded+failed+ambiguous FROM jobs WHERE job_id=?1",
                    [&job_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                if terminal == total && total > 0 {
                    c.execute("UPDATE jobs SET state='completed',completed_at_unix=coalesce(completed_at_unix,?2),expire_at_unix=coalesce(expire_at_unix,?2+604800) WHERE job_id=?1 AND state IN('queued','running')",params![job_id,finished_at])?;
                    finalize_completed_job(c, &job_id, finished_at)?;
                }
            }
            Ok(WriterResult::Done)
        }),
        WriterCmd::RecoverLeases {
            process_epoch: _,
            now_unix,
        } => transaction(conn, |c| {
            recover_expired(c, now_unix).map(WriterResult::Count)
        }),
        WriterCmd::RequeueExpiredLeases {
            process_epoch: _,
            now_unix,
        } => transaction(conn, |c| {
            recover_expired(c, now_unix).map(WriterResult::Count)
        }),
        WriterCmd::ExpireDeadline { now_unix } => transaction(conn, |c| {
            let mut jobs=c.prepare("SELECT job_id FROM jobs WHERE deadline_unix IS NOT NULL AND deadline_unix<=?1 AND state IN('queued','running')")?.query_map([now_unix],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
            let n=c.execute("UPDATE recipients SET state='failed',error_norm='deadline_exceeded' WHERE state IN ('queued','delayed') AND job_id IN(SELECT job_id FROM jobs WHERE deadline_unix IS NOT NULL AND deadline_unix<=?1 AND state IN('queued','running'))",[now_unix])?;
            for job in jobs.drain(..) {
                reconcile_job(c, &job, now_unix)?;
            }
            Ok(WriterResult::Count(n))
        }),
        WriterCmd::Checkpoint { mode } => {
            let pragma = match mode {
                CheckpointMode::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
                CheckpointMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
            };
            let (busy, log, checkpointed) =
                conn.query_row(pragma, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            Ok(WriterResult::Checkpoint {
                busy,
                log,
                checkpointed,
            })
        }
        WriterCmd::UpsertConfig {
            bot_id,
            config_json,
            webhook_secret,
            updated_at_unix,
        } => transaction(conn, |c| {
            let number = |key: &str| {
                config_json
                    .get(key)
                    .and_then(Value::as_i64)
                    .ok_or(StoreError::InvalidCommand("config integer missing"))
            };
            let text = |key: &str| {
                config_json
                    .get(key)
                    .and_then(Value::as_str)
                    .ok_or(StoreError::InvalidCommand("config string missing"))
            };
            let target = config_json
                .get("target_msgs_per_sec")
                .and_then(Value::as_f64)
                .ok_or(StoreError::InvalidCommand("config rate missing"))?;
            let webhook_url = config_json
                .get("completion_webhook_url")
                .and_then(Value::as_str);
            let (secret_nonce, secret_ciphertext, secret_kid) = webhook_secret
                .map(|(n, c, k)| (Some(n), Some(c), Some(k)))
                .unwrap_or((None, None, None));
            c.execute("INSERT INTO bot_configs VALUES(?1,1,?2,?3,?4,?5,'full',?6,?7,?8,?9,?10,1,?11,?12,?13,?14,?15,?16,?17) ON CONFLICT(bot_id) DO UPDATE SET config_version=config_version+1,target_msgs_per_sec=excluded.target_msgs_per_sec,retry_max_attempts=excluded.retry_max_attempts,retry_base_ms=excluded.retry_base_ms,retry_max_ms=excluded.retry_max_ms,retryable_classes_json=excluded.retryable_classes_json,ambiguity_policy=excluded.ambiguity_policy,job_deadline_secs=excluded.job_deadline_secs,fairness_weight=excluded.fairness_weight,webhook_url=excluded.webhook_url,webhook_secret_nonce=CASE WHEN excluded.webhook_url IS NULL THEN NULL ELSE coalesce(excluded.webhook_secret_nonce,webhook_secret_nonce) END,webhook_secret_ciphertext=CASE WHEN excluded.webhook_url IS NULL THEN NULL ELSE coalesce(excluded.webhook_secret_ciphertext,webhook_secret_ciphertext) END,webhook_secret_kid=CASE WHEN excluded.webhook_url IS NULL THEN NULL ELSE coalesce(excluded.webhook_secret_kid,webhook_secret_kid) END,webhook_max_attempts=excluded.webhook_max_attempts,webhook_retry_base_ms=excluded.webhook_retry_base_ms,webhook_retry_max_ms=excluded.webhook_retry_max_ms,updated_at_unix=excluded.updated_at_unix",params![bot_id.0.as_slice(),target,number("retry_max_attempts")?,number("retry_base_ms")?,number("retry_max_ms")?,text("retryable_classes_json")?,text("ambiguity_policy")?,number("job_deadline_secs")?,number("fairness_weight")?,webhook_url,secret_nonce.map(|n|n.to_vec()),secret_ciphertext,secret_kid,number("webhook_max_attempts")?,number("webhook_retry_base_ms")?,number("webhook_retry_max_ms")?,updated_at_unix])?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::UpsertBot {
            bot_id,
            token_nonce,
            token_ciphertext,
            token_kid,
            telegram_user_id,
            now_unix,
        } => transaction(conn, |c| {
            c.execute(
                "INSERT INTO bots VALUES(?1,?2,?3,?4,?5,?6,?6) ON CONFLICT(bot_id) DO NOTHING",
                params![
                    bot_id.0.as_slice(),
                    token_nonce.as_slice(),
                    token_ciphertext,
                    token_kid,
                    telegram_user_id,
                    now_unix
                ],
            )?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::FailBotUnauthorized { bot_id, now_unix } => transaction(conn, |c| {
            let jobs = c
                .prepare(
                    "SELECT job_id FROM jobs WHERE bot_id=?1 AND state IN('queued','running')",
                )?
                .query_map([bot_id.0.as_slice()], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let n=c.execute("UPDATE recipients SET state='failed',error_norm='bot_unauthorized',lease_owner=NULL,lease_until_unix=NULL WHERE bot_id=?1 AND state IN('queued','delayed','leased')",[bot_id.0.as_slice()])?;
            for job in jobs {
                reconcile_job(c, &job, now_unix)?;
                c.execute(
                    "UPDATE jobs SET error_summary='bot_unauthorized' WHERE job_id=?1",
                    [job],
                )?;
            }
            Ok(WriterResult::Count(n))
        }),
        WriterCmd::PauseScope {
            scope_key,
            until_unix,
            retry_after,
            now_unix,
        } => transaction(conn, |c| {
            c.execute("INSERT INTO limiter_pauses VALUES(?1,?2,?3,?4) ON CONFLICT(scope_key) DO UPDATE SET paused_until_unix=max(paused_until_unix,excluded.paused_until_unix),last_retry_after=excluded.last_retry_after,updated_at_unix=excluded.updated_at_unix",params![scope_key,until_unix,retry_after,now_unix])?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::MarkWebhook { event_id, mark } => {
            transaction(conn, |c| {
                match mark {
                    WebhookMark::Attempt {
                        page,
                        attempt,
                        at_unix,
                        http_status,
                        error,
                        next_attempt_unix,
                        terminal,
                    } => {
                        let err = error.clone();
                        c.execute(
                            "INSERT OR IGNORE INTO webhook_deliveries(event_id,page_index,attempt,at_unix,http_status,error) VALUES(?1,?2,?3,?4,?5,?6)",
                            params![event_id, page, attempt, at_unix, http_status, error],
                        )?;
                        c.execute(
                            "UPDATE webhook_pages SET attempt_count=?2,last_http_status=?3,last_error=?4 WHERE event_id=?1 AND page_index=?5",
                            params![event_id, attempt, http_status, err, page],
                        )?;
                        // Cm7: event-level attempt_count tracks page-0 attempts only.
                        if page == 0 {
                            c.execute(
                                "UPDATE webhook_events SET attempt_count=?2 WHERE event_id=?1 AND attempt_count<?2",
                                params![event_id, attempt],
                            )?;
                        }
                        match terminal {
                            Some(WebhookTerminal::Delivered) => {
                                c.execute("UPDATE webhook_pages SET delivered=1,next_attempt_unix=NULL WHERE event_id=?1 AND page_index=?2", params![event_id, page])?;
                                let all: bool = c.query_row("SELECT NOT EXISTS(SELECT 1 FROM webhook_pages WHERE event_id=?1 AND delivered=0)", [&event_id], |r| r.get(0))?;
                                if all {
                                    c.execute("UPDATE webhook_events SET state='delivered',delivered_at_unix=?2,next_attempt_unix=?2,lease_until_unix=NULL WHERE event_id=?1", params![event_id, at_unix])?;
                                    c.execute("UPDATE jobs SET webhook_state='delivered' WHERE webhook_event_id=?1", [&event_id])?;
                                } else {
                                    c.execute("UPDATE webhook_events SET state='pending',next_attempt_unix=?2,lease_until_unix=NULL WHERE event_id=?1", params![event_id, at_unix])?;
                                    c.execute("UPDATE jobs SET webhook_state='pending' WHERE webhook_event_id=?1", [&event_id])?;
                                }
                            }
                            Some(WebhookTerminal::Exhausted { error }) => {
                                c.execute("UPDATE webhook_events SET state='exhausted',last_error=?2,next_attempt_unix=?3,lease_until_unix=NULL WHERE event_id=?1", params![event_id, error, at_unix])?;
                                c.execute("UPDATE jobs SET webhook_state='exhausted' WHERE webhook_event_id=?1", [&event_id])?;
                            }
                            None => {
                                let next = next_attempt_unix.ok_or(StoreError::InvalidCommand(
                                    "retry attempt requires next_attempt_unix",
                                ))?;
                                c.execute("UPDATE webhook_pages SET next_attempt_unix=?2 WHERE event_id=?1 AND page_index=?3", params![event_id, next, page])?;
                                if page == 0 {
                                    c.execute("UPDATE webhook_events SET state='pending',next_attempt_unix=?2,last_error=?3,lease_until_unix=NULL WHERE event_id=?1", params![event_id, next, err])?;
                                } else {
                                    c.execute("UPDATE webhook_events SET state='pending',next_attempt_unix=?2,lease_until_unix=NULL,last_error=?3 WHERE event_id=?1", params![event_id, next, err])?;
                                }
                                c.execute("UPDATE jobs SET webhook_state='pending' WHERE webhook_event_id=?1", [&event_id])?;
                            }
                        }
                    }
                }
                Ok(WriterResult::Done)
            })
        }
        WriterCmd::RetentionTick { now_unix, batch } => {
            // Incremental, writer-ordered retention. Only terminal jobs whose
            // completion webhook is itself terminal (delivered/exhausted/never
            // configured) are eligible, so no in-flight webhook is ever removed
            // underneath the deliverer. `webhook_events` has no ON DELETE
            // CASCADE from `jobs`, so the (terminal) event row and its pages
            // and deliveries must be removed first. On-disk blobs are deleted
            // after the transaction commits (best-effort); a crash in between
            // leaves at most an orphan file, which the boot sweep removes.
            let removed = transaction(conn, |c| {
                let ids = {
                    let mut s = c.prepare(
                        "SELECT job_id FROM jobs WHERE expire_at_unix<?1 AND webhook_state NOT IN('pending','delivering') AND state IN('completed','failed_internal') ORDER BY expire_at_unix,job_id LIMIT ?2",
                    )?;
                    let ids = s
                        .query_map(params![now_unix, i64::from(batch)], |r| {
                            r.get::<_, String>(0)
                        })?
                        .collect::<Result<Vec<_>, _>>()?;
                    ids
                };
                if ids.is_empty() {
                    return Ok((0usize, Vec::<String>::new()));
                }
                let files = {
                    let mut s = c.prepare("SELECT blob_path FROM job_files WHERE job_id=?1")?;
                    let mut out = Vec::new();
                    for id in &ids {
                        let mut q = s.query([id])?;
                        while let Some(row) = q.next()? {
                            out.push(row.get::<_, String>(0)?);
                        }
                    }
                    out
                };
                let mut del_event = c.prepare("DELETE FROM webhook_events WHERE job_id=?1")?;
                let mut del_job = c.prepare("DELETE FROM jobs WHERE job_id=?1")?;
                for id in &ids {
                    let _ = del_event.execute([id])?;
                    let _ = del_job.execute([id])?;
                }
                Ok((ids.len(), files))
            })?;
            let (rows, files) = removed;
            for path in &files {
                let _ = std::fs::remove_file(path);
            }
            Ok(WriterResult::Retention { rows, files })
        }
        WriterCmd::SetMaxPageCount { pages } => {
            if pages == -1 {
                let _: i64 =
                    conn.pragma_update_and_check(None, "max_page_count", 2_147_483_646_i64, |r| {
                        r.get(0)
                    })?;
            } else if pages > 0 {
                let _: i64 =
                    conn.pragma_update_and_check(None, "max_page_count", pages, |r| r.get(0))?;
            } else {
                return Err(StoreError::InvalidCommand(
                    "max_page_count must be positive or -1",
                ));
            }
            Ok(WriterResult::Done)
        }
        WriterCmd::Backup { destination } => {
            let escaped = destination.to_string_lossy().replace('\'', "''");
            conn.execute_batch(&format!("VACUUM INTO '{escaped}'"))?;
            Ok(WriterResult::Done)
        }
        WriterCmd::PersistScheduler {
            bot_id,
            deficit,
            weight,
            last_grant_unix,
        } => transaction(conn, |c| {
            c.execute("INSERT INTO fairness_deficits VALUES(?1,?2,?3,?4) ON CONFLICT(bot_id) DO UPDATE SET deficit=excluded.deficit,weight=excluded.weight,last_grant_unix=excluded.last_grant_unix",params![bot_id.0.as_slice(),deficit,weight,last_grant_unix])?;
            Ok(WriterResult::Done)
        }),
        WriterCmd::Shutdown => Ok(WriterResult::Done),
    }
}

/// Requeue recipients whose lease expired without a completion. Rows that
/// never reached the wire in this process epoch (`wire_started=0`) go straight
/// back to `queued`; rows that may already have reached Telegram (a crash or
/// preempted owner after `wire_started=1`) follow the job's ambiguity policy so
/// at-most-once work is never blindly double-sent.
fn recover_expired(c: &Connection, now_unix: i64) -> Result<usize, StoreError> {
    let mut stmt = c.prepare(
        "SELECT r.job_id,r.idx,r.lease_owner,r.wire_started,r.retry_consumed,j.policy_snapshot_json          FROM recipients r JOIN jobs j ON j.job_id=r.job_id          WHERE r.state='leased' AND r.lease_until_unix<=?1",
    )?;
    let rows = stmt
        .query_map([now_unix], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, u32>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, u16>(4)?,
                r.get::<_, String>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    let mut jobs = std::collections::HashSet::new();
    for (job, idx, _owner, wire, retry, policy_json) in &rows {
        jobs.insert(job.clone());
        // MarkWireStarted is committed before the network request. A leased
        // row with wire_started=0 therefore never reached Telegram, regardless
        // of which process epoch owns the expired lease, and is safe to retry.
        if *wire == 0 {
            c.execute("UPDATE recipients SET state='queued',error_norm='transient',wire_started=0,lease_owner=NULL,lease_until_unix=NULL WHERE job_id=?1 AND idx=?2 AND state='leased'",params![job,idx])?;
            continue;
        }
        let policy: crate::domain::PolicySnapshot =
            serde_json::from_str(policy_json).unwrap_or(crate::domain::PolicySnapshot {
                retry_max_attempts: 0,
                ambiguity_policy: crate::domain::AmbiguityPolicy::AtMostOnce,
                fairness_weight: 1,
                job_deadline_secs: 0,
            });
        let can_retry = matches!(
            policy.ambiguity_policy,
            crate::domain::AmbiguityPolicy::AtLeastOnce
        ) && retry.saturating_add(1) < policy.retry_max_attempts;
        c.execute("UPDATE recipients SET state=?3,error_norm='ambiguous',retry_consumed=retry_consumed+1,not_before_unix=CASE WHEN ?3='delayed' THEN ?4 ELSE not_before_unix END,lease_owner=NULL,lease_until_unix=NULL WHERE job_id=?1 AND idx=?2 AND state='leased'",params![job,idx,if can_retry{"delayed"}else{"ambiguous"},now_unix])?;
    }
    for job in jobs {
        reconcile_job(c, &job, now_unix)?;
    }
    Ok(rows.len())
}

/// When a job's recipients are all terminal, mark the job completed and, if a
/// completion webhook is configured, materialize its (possibly paged) event and
/// pages. Shared by the authoritative `reconcile_job` and the incremental
/// per-attempt completion path.
fn finalize_completed_job(c: &Connection, job_id: &str, now_unix: i64) -> Result<(), StoreError> {
    let webhook: Option<(Vec<u8>, String)> = c
        .query_row(
            "SELECT bot_id,webhook_state FROM jobs WHERE job_id=?1 AND state='completed'",
            [job_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((bot_id, state)) = webhook else {
        return Ok(());
    };
    if state != "pending" {
        return Ok(());
    }
    let event_id = format!("{job_id}-event");
    c.execute(
        "UPDATE jobs SET webhook_event_id=?2 WHERE job_id=?1",
        params![job_id, event_id],
    )?;
    let (total, succeeded, failed, ambiguous): (i64, i64, i64, i64) = c.query_row(
        "SELECT total,succeeded,failed,ambiguous FROM jobs WHERE job_id=?1",
        [job_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    let mut rows_stmt = c.prepare(
        "SELECT chat_id,telegram_message_id,time_send_unix FROM recipients WHERE job_id=?1 AND state='succeeded' ORDER BY idx",
    )?;
    let rows = rows_stmt
        .query_map([job_id], |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let bodies = crate::webhook::page_bodies(
        job_id,
        &event_id,
        "completed",
        total as u32,
        succeeded as u32,
        failed as u32,
        ambiguous as u32,
        rows,
        crate::webhook::MAX_WEBHOOK_PAGE_BYTES,
    );
    let page_count = bodies.len() as i64;
    let inserted = c.execute(
        "INSERT OR IGNORE INTO webhook_events(event_id,job_id,bot_id,state,next_attempt_unix,page_count,created_at_unix) VALUES(?1,?2,?3,'pending',?4,?5,?4)",
        params![event_id, job_id, bot_id, now_unix, page_count],
    )?;
    if inserted == 1 {
        let mut ins = c.prepare(
            "INSERT INTO webhook_pages(event_id,page_index,delivered,body,body_sha256,attempt_count,next_attempt_unix) VALUES(?1,?2,0,?3,?4,0,CASE WHEN ?2=0 THEN ?5 ELSE NULL END)",
        )?;
        for (i, body) in bodies.iter().enumerate() {
            let digest: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(body).into();
            ins.execute(params![
                event_id,
                i as i64,
                body,
                digest.as_slice(),
                now_unix
            ])?;
        }
    }
    Ok(())
}

/// Authoritative ledger recompute for a job, used on the non-hot paths that move
/// many recipients at once (lease recovery, deadline expiry, bot revocation).
/// Recomputes the job counters from its recipients, marks the job completed
/// when every recipient is terminal, and materializes any pending webhook.
fn reconcile_job(c: &Connection, job_id: &str, now_unix: i64) -> Result<(), StoreError> {
    let (queued,inflight,delayed,succeeded,failed,ambiguous):(i64,i64,i64,i64,i64,i64)=c.query_row("SELECT sum(state='queued'),sum(state='leased'),sum(state='delayed'),sum(state='succeeded'),sum(state='failed'),sum(state='ambiguous') FROM recipients WHERE job_id=?1",[job_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
    let terminal = succeeded + failed + ambiguous;
    c.execute("UPDATE jobs SET queued=?2,inflight=?3,delayed=?4,succeeded=?5,failed=?6,ambiguous=?7,state=CASE WHEN ?8=total THEN 'completed' ELSE state END,completed_at_unix=CASE WHEN ?8=total THEN coalesce(completed_at_unix,?9) ELSE completed_at_unix END,expire_at_unix=CASE WHEN ?8=total THEN coalesce(expire_at_unix,?9+604800) ELSE expire_at_unix END WHERE job_id=?1",params![job_id,queued,inflight,delayed,succeeded,failed,ambiguous,terminal,now_unix])?;
    c.execute("INSERT INTO rate_samples(job_id,window_start_unix,sent) VALUES(?1,?2,1) ON CONFLICT(job_id,window_start_unix) DO UPDATE SET sent=sent+1",params![job_id,now_unix])?;
    finalize_completed_job(c, job_id, now_unix)
}
