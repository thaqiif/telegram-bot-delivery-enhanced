use super::{open_reader, StoreError};
use crate::{domain::BotId, observe::DbGauges};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread::{self, JoinHandle},
};
pub const READER_THREADS: usize = 4;
#[derive(Debug, Clone, PartialEq)]
pub enum ReadResult {
    Pong { thread_name: String },
    SQLiteVersion(String),
    Integer(i64),
    Text(String),
    TextList(Vec<String>),
    Bot(Option<BotRecord>),
    Config(Option<String>),
    Job(Option<JobStatus>),
    Results(Vec<ResultItem>),
    ReadyBots(Vec<(BotId, u8)>),
    WebhookPages(Vec<WebhookPageRow>),
    DbGauges(DbGauges),
    CompletedJobs(Vec<(String, i64, i64)>),
    DispatchItem(Option<DispatchItem>),
    ActivePauses(Vec<(String, i64)>),
    BlobBytes(i64),
}
#[derive(Debug, Clone, PartialEq)]
pub struct BotRecord {
    pub telegram_user_id: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobStatus {
    pub job_id: String,
    pub method: String,
    pub state: String,
    pub accepted_at_unix: Option<i64>,
    pub started_at_unix: Option<i64>,
    pub completed_at_unix: Option<i64>,
    pub expire_at_unix: Option<i64>,
    pub deadline_unix: Option<i64>,
    pub total: u32,
    pub queued: u32,
    pub inflight: u32,
    pub delayed: u32,
    pub succeeded: u32,
    pub failed: u32,
    pub ambiguous: u32,
    pub webhook_state: String,
    pub rate_5s: f64,
    pub rate_30s: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultItem {
    pub idx: u32,
    pub chat_id: Option<String>,
    pub status: String,
    pub attempt_count: u16,
    pub telegram_message_id: Option<i64>,
    pub time_send_unix: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DispatchFile {
    pub field_name: String,
    pub attach_name: Option<String>,
    pub content_type: Option<String>,
    pub original_filename: Option<String>,
    pub blob_path: String,
    pub size_bytes: u64,
}

/// Everything needed to send one already-leased recipient. Credentials stay
/// encrypted in SQLite and are decrypted only immediately before the request.
#[derive(Debug, Clone, PartialEq)]
pub struct DispatchItem {
    pub method: String,
    pub shared_params_json: String,
    pub patch_json: String,
    pub chat_id: Option<String>,
    pub chat_kind: Option<String>,
    pub retry_consumed: u16,
    pub policy_snapshot_json: String,
    pub token_nonce: Vec<u8>,
    pub token_ciphertext: Vec<u8>,
    pub token_kid: String,
    pub files: Vec<DispatchFile>,
}

type DispatchBase = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    u16,
    String,
    Vec<u8>,
    Vec<u8>,
    String,
);

#[derive(Debug, Clone, PartialEq)]
pub struct WebhookPageRow {
    pub event_id: String,
    pub job_id: String,
    pub bot_id: Vec<u8>,
    pub page_index: i64,
    pub page_attempt_count: i64,
    pub body: Option<Vec<u8>>,
    pub body_sha256: Option<Vec<u8>>,
    pub event_state: String,
    pub page_count: i64,
    pub webhook_url: Option<String>,
    pub webhook_max_attempts: Option<i64>,
    pub webhook_retry_base_ms: Option<i64>,
    pub webhook_retry_max_ms: Option<i64>,
    pub webhook_secret_nonce: Option<Vec<u8>>,
    pub webhook_secret_ciphertext: Option<Vec<u8>>,
    pub webhook_secret_kid: Option<String>,
}
enum ReadQuery {
    Ping,
    SQLiteVersion,
    PragmaInt(&'static str),
    PragmaText(&'static str),
    TableNames,
    Bot(BotId),
    Config(BotId),
    Job(BotId, String),
    Results(BotId, String, u32, u16),
    ReadyBots(i64),
    PendingWebhookPages(i64),
    DbGauges,
    CompletedJobs {
        after_second: i64,
        after_job: String,
    },
    DispatchItem {
        job_id: String,
        idx: u32,
    },
    FileBlobPaths,
    AcceptingJobIds,
    /// limiter_pauses rows still in force (`paused_until_unix > now`), so the
    /// dispatcher can honor durable Telegram flood pauses across a restart.
    ActivePauses(i64),
    /// Sum of durable blob bytes (`job_files.size_bytes`) for the operator
    /// storage high-watermark admission check.
    BlobBytes,
}
struct Request {
    query: ReadQuery,
    reply: mpsc::Sender<Result<ReadResult, StoreError>>,
}
pub struct ReadPool {
    senders: Vec<mpsc::SyncSender<Request>>,
    next: AtomicUsize,
    joins: Vec<JoinHandle<()>>,
}
impl ReadPool {
    pub fn start(path: PathBuf, cap: usize) -> Result<Arc<Self>, StoreError> {
        let (mut senders, mut joins) = (vec![], vec![]);
        for i in 0..READER_THREADS {
            let (tx, rx) = mpsc::sync_channel(cap);
            let p = path.clone();
            let (rt, rr) = mpsc::sync_channel(1);
            let j = thread::Builder::new()
                .name(format!("sqlite-reader-{i}"))
                .spawn(move || reader_loop(p, rx, rt))
                .map_err(|_| StoreError::WorkerUnavailable)?;
            rr.recv().map_err(|_| StoreError::WorkerUnavailable)??;
            senders.push(tx);
            joins.push(j)
        }
        Ok(Arc::new(Self {
            senders,
            next: AtomicUsize::new(0),
            joins,
        }))
    }
    fn ask(&self, q: ReadQuery) -> Result<ReadResult, StoreError> {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % READER_THREADS;
        let (tx, rx) = mpsc::channel();
        self.senders[i]
            .send(Request {
                query: q,
                reply: tx,
            })
            .map_err(|_| StoreError::WorkerUnavailable)?;
        rx.recv().map_err(|_| StoreError::ResponseClosed)?
    }
    pub fn ping(&self) -> Result<ReadResult, StoreError> {
        self.ask(ReadQuery::Ping)
    }
    pub fn sqlite_version(&self) -> Result<ReadResult, StoreError> {
        self.ask(ReadQuery::SQLiteVersion)
    }
    pub fn pragma_int(&self, n: &'static str) -> Result<ReadResult, StoreError> {
        self.ask(ReadQuery::PragmaInt(n))
    }
    pub fn pragma_text(&self, n: &'static str) -> Result<ReadResult, StoreError> {
        self.ask(ReadQuery::PragmaText(n))
    }
    pub fn table_names(&self) -> Result<ReadResult, StoreError> {
        self.ask(ReadQuery::TableNames)
    }
    pub fn bot(&self, id: BotId) -> Result<Option<BotRecord>, StoreError> {
        match self.ask(ReadQuery::Bot(id))? {
            ReadResult::Bot(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    pub fn config(&self, id: BotId) -> Result<Option<String>, StoreError> {
        match self.ask(ReadQuery::Config(id))? {
            ReadResult::Config(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    pub fn job(&self, id: BotId, j: String) -> Result<Option<JobStatus>, StoreError> {
        match self.ask(ReadQuery::Job(id, j))? {
            ReadResult::Job(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    pub fn results(
        &self,
        id: BotId,
        j: String,
        c: u32,
        l: u16,
    ) -> Result<Vec<ResultItem>, StoreError> {
        match self.ask(ReadQuery::Results(id, j, c, l))? {
            ReadResult::Results(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    pub fn ready_bots(&self, now_unix: i64) -> Result<Vec<(BotId, u8)>, StoreError> {
        match self.ask(ReadQuery::ReadyBots(now_unix))? {
            ReadResult::ReadyBots(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    pub fn pending_webhook_pages(&self, now_unix: i64) -> Result<Vec<WebhookPageRow>, StoreError> {
        match self.ask(ReadQuery::PendingWebhookPages(now_unix))? {
            ReadResult::WebhookPages(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    /// DB-derived observability gauges for the maintenance snapshot.
    pub fn snapshot_gauges(&self) -> Result<DbGauges, StoreError> {
        match self.ask(ReadQuery::DbGauges)? {
            ReadResult::DbGauges(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    /// Completed jobs after a `(completed_at_unix, job_id)` keyset cursor,
    /// ordered ascending, bounded to a maintenance batch. Each row is
    /// `(job_id, accepted_at_unix, completed_at_unix)`.
    pub fn completed_jobs(
        &self,
        after_second: i64,
        after_job: String,
    ) -> Result<Vec<(String, i64, i64)>, StoreError> {
        match self.ask(ReadQuery::CompletedJobs {
            after_second,
            after_job,
        })? {
            ReadResult::CompletedJobs(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    /// Materialize the payload/file metadata for a recipient already leased by
    /// the writer. The query is single-shot on a dedicated read thread.
    pub fn dispatch_item(
        &self,
        job_id: String,
        idx: u32,
    ) -> Result<Option<DispatchItem>, StoreError> {
        match self.ask(ReadQuery::DispatchItem { job_id, idx })? {
            ReadResult::DispatchItem(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    /// All `job_files.blob_path` values (for the boot-time orphan sweep).
    pub fn file_blob_paths(&self) -> Result<Vec<String>, StoreError> {
        match self.ask(ReadQuery::FileBlobPaths)? {
            ReadResult::TextList(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    /// Job ids still `accepting` (orphaned by a crash; safe to abort at boot).
    pub fn accepting_job_ids(&self) -> Result<Vec<String>, StoreError> {
        match self.ask(ReadQuery::AcceptingJobIds)? {
            ReadResult::TextList(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    /// limiter_pauses rows still in force at `now_unix` (durable flood pauses
    /// from a previous process). Returns `(scope_key, paused_until_unix)`.
    pub fn active_pauses(&self, now_unix: i64) -> Result<Vec<(String, i64)>, StoreError> {
        match self.ask(ReadQuery::ActivePauses(now_unix))? {
            ReadResult::ActivePauses(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    /// Sum of durable multipart blob bytes (operator storage high-watermark).
    pub fn blob_bytes(&self) -> Result<i64, StoreError> {
        match self.ask(ReadQuery::BlobBytes)? {
            ReadResult::BlobBytes(v) => Ok(v),
            _ => Err(StoreError::ResponseClosed),
        }
    }
    pub fn thread_count(&self) -> usize {
        self.joins.len()
    }
}
fn reader_loop(
    path: PathBuf,
    rx: mpsc::Receiver<Request>,
    ready: mpsc::SyncSender<Result<(), StoreError>>,
) {
    let conn = match open_reader(&path) {
        Ok(c) => {
            let _ = ready.send(Ok(()));
            c
        }
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    while let Ok(r) = rx.recv() {
        let result = (|| -> Result<ReadResult, StoreError> {
            Ok(match r.query {
                ReadQuery::Ping => ReadResult::Pong { thread_name: thread::current().name().unwrap_or("unnamed").into() },
                ReadQuery::SQLiteVersion => ReadResult::SQLiteVersion(conn.query_row("SELECT sqlite_version()", [], |r| r.get(0))?),
                ReadQuery::PragmaInt(n) => ReadResult::Integer(conn.pragma_query_value(None, n, |r| r.get(0))?),
                ReadQuery::PragmaText(n) => ReadResult::Text(conn.pragma_query_value(None, n, |r| r.get(0))?),
                ReadQuery::TableNames => {
                    let mut s = conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")?;
                    let rows = s.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
                    ReadResult::TextList(rows)
                }
                ReadQuery::Bot(id) => ReadResult::Bot(conn.query_row(
                    "SELECT telegram_user_id FROM bots WHERE bot_id=?1",
                    [id.0.as_slice()],
                    |r| Ok(BotRecord { telegram_user_id: r.get(0)? }),
                ).optional()?),
                ReadQuery::Config(id) => ReadResult::Config(conn.query_row(
                    "SELECT json_object('config_version',config_version,'target_msgs_per_sec',target_msgs_per_sec,'retry_max_attempts',retry_max_attempts,'retry_base_ms',retry_base_ms,'retry_max_ms',retry_max_ms,'retry_jitter',retry_jitter,'retryable_classes',json(retryable_classes_json),'ambiguity_policy',ambiguity_policy,'job_deadline_secs',job_deadline_secs,'fairness_weight',fairness_weight,'completion_webhook_url',webhook_url,'webhook_secret_configured',json(CASE WHEN webhook_secret_ciphertext IS NOT NULL THEN 'true' ELSE 'false' END),'webhook_max_attempts',webhook_max_attempts,'webhook_retry_base_ms',webhook_retry_base_ms,'webhook_retry_max_ms',webhook_retry_max_ms) FROM bot_configs WHERE bot_id=?1",
                    [id.0.as_slice()],
                    |r| r.get(0)
                ).optional()?),
                ReadQuery::Job(id, j) => ReadResult::Job(conn.query_row(
                    "SELECT job_id,method,state,accepted_at_unix,started_at_unix,completed_at_unix,expire_at_unix,deadline_unix,total,queued,inflight,delayed,succeeded,failed,ambiguous,webhook_state,(SELECT coalesce(sum(sent),0)/5.0 FROM rate_samples WHERE job_id=jobs.job_id AND window_start_unix>=unixepoch()-5),(SELECT coalesce(sum(sent),0)/30.0 FROM rate_samples WHERE job_id=jobs.job_id AND window_start_unix>=unixepoch()-30) FROM jobs WHERE bot_id=?1 AND job_id=?2 AND state!='accepting'",
                    rusqlite::params![id.0.as_slice(), j],
                    |r| Ok(JobStatus {
                        job_id: r.get(0)?,
                        method: r.get(1)?,
                        state: r.get(2)?,
                        accepted_at_unix: r.get(3)?,
                        started_at_unix: r.get(4)?,
                        completed_at_unix: r.get(5)?,
                        expire_at_unix: r.get(6)?,
                        deadline_unix: r.get(7)?,
                        total: r.get(8)?,
                        queued: r.get(9)?,
                        inflight: r.get(10)?,
                        delayed: r.get(11)?,
                        succeeded: r.get(12)?,
                        failed: r.get(13)?,
                        ambiguous: r.get(14)?,
                        webhook_state: r.get(15)?,
                        rate_5s: r.get(16)?,
                        rate_30s: r.get(17)?,
                    })
                ).optional()?),
                ReadQuery::Results(id, j, c, l) => {
                    let mut s = conn.prepare(
                        "SELECT r.idx,r.chat_id,r.state,r.attempt_count,r.telegram_message_id,r.time_send_unix,r.error_norm FROM recipients r JOIN jobs j ON j.job_id=r.job_id WHERE j.bot_id=?1 AND j.job_id=?2 AND j.state!='accepting' AND r.idx>=?3 ORDER BY r.idx LIMIT ?4"
                    )?;
                    let rows = s.query_map(rusqlite::params![id.0.as_slice(), j, c, l], |r| Ok(ResultItem {
                        idx: r.get(0)?,
                        chat_id: r.get(1)?,
                        status: r.get(2)?,
                        attempt_count: r.get(3)?,
                        telegram_message_id: r.get(4)?,
                        time_send_unix: r.get(5)?,
                        error: r.get(6)?,
                    }))?.collect::<Result<_, _>>()?;
                    ReadResult::Results(rows)
                }
                ReadQuery::ReadyBots(now_unix) => {
                    let mut s = conn.prepare(
                        "SELECT DISTINCT r.bot_id, coalesce(c.fairness_weight,1) FROM recipients r JOIN jobs j ON j.job_id=r.job_id LEFT JOIN bot_configs c ON c.bot_id=r.bot_id WHERE r.state IN ('queued','delayed') AND r.not_before_unix<=?1 AND j.state IN ('queued','running')"
                    )?;
                    let rows = s.query_map([now_unix], |r| {
                        let raw: Vec<u8> = r.get(0)?;
                        let id: [u8; 32] = raw.try_into().map_err(|got: Vec<u8>| {
                            rusqlite::Error::FromSqlConversionFailure(
                                got.len(),
                                rusqlite::types::Type::Blob,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "bot_id blob must be exactly 32 bytes",
                                )),
                            )
                        })?;
                        Ok((BotId(id), r.get::<_, i64>(1).unwrap_or(1) as u8))
                    })?
                    .collect::<Result<_, _>>()?;
                    ReadResult::ReadyBots(rows)
                }
                ReadQuery::PendingWebhookPages(now_unix) => {
                    let mut s = conn.prepare(
                        "SELECT e.event_id, e.job_id, e.bot_id, p.page_index, p.attempt_count, p.body, p.body_sha256, e.state, e.page_count, c.webhook_url, c.webhook_max_attempts, c.webhook_retry_base_ms, c.webhook_retry_max_ms, c.webhook_secret_nonce, c.webhook_secret_ciphertext, c.webhook_secret_kid FROM webhook_pages p JOIN webhook_events e ON e.event_id = p.event_id LEFT JOIN bot_configs c ON c.bot_id = e.bot_id WHERE p.delivered = 0 AND e.state IN ('pending','delivering') AND (p.next_attempt_unix IS NULL OR p.next_attempt_unix <= ?1) AND NOT EXISTS (SELECT 1 FROM webhook_pages q WHERE q.event_id = p.event_id AND q.delivered = 0 AND q.page_index < p.page_index) ORDER BY p.next_attempt_unix, e.event_id, p.page_index LIMIT 16"
                    )?;
                    let rows = s.query_map([now_unix], |r| Ok(WebhookPageRow {
                        event_id: r.get(0)?,
                        job_id: r.get(1)?,
                        bot_id: r.get(2)?,
                        page_index: r.get(3)?,
                        page_attempt_count: r.get(4)?,
                        body: r.get(5)?,
                        body_sha256: r.get(6)?,
                        event_state: r.get(7)?,
                        page_count: r.get(8)?,
                        webhook_url: r.get(9)?,
                        webhook_max_attempts: r.get(10)?,
                        webhook_retry_base_ms: r.get(11)?,
                        webhook_retry_max_ms: r.get(12)?,
                        webhook_secret_nonce: r.get(13)?,
                        webhook_secret_ciphertext: r.get(14)?,
                        webhook_secret_kid: r.get(15)?,
                    }))?.collect::<Result<_, _>>()?;
                    ReadResult::WebhookPages(rows)
                }
                ReadQuery::DispatchItem { job_id, idx } => {
                    let base: Option<DispatchBase> = conn.query_row(
                        "SELECT j.method,j.shared_params_json,r.patch_json,r.chat_id,r.chat_kind,r.retry_consumed,j.policy_snapshot_json,b.token_nonce,b.token_ciphertext,b.token_kid FROM recipients r JOIN jobs j ON j.job_id=r.job_id JOIN bots b ON b.bot_id=r.bot_id WHERE r.job_id=?1 AND r.idx=?2 AND r.state='leased'",
                        rusqlite::params![job_id, idx],
                        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?)),
                    ).optional()?;
                    if let Some((method, shared_params_json, patch_json, chat_id, chat_kind, retry_consumed, policy_snapshot_json, token_nonce, token_ciphertext, token_kid)) = base {
                        let mut s = conn.prepare("SELECT field_name,attach_name,content_type,original_filename,blob_path,size_bytes FROM job_files WHERE job_id=?1 ORDER BY file_id")?;
                        let files = s.query_map([&job_id], |r| Ok(DispatchFile {
                            field_name: r.get(0)?,
                            attach_name: r.get(1)?,
                            content_type: r.get(2)?,
                            original_filename: r.get(3)?,
                            blob_path: r.get(4)?,
                            size_bytes: u64::try_from(r.get::<_, i64>(5)?.max(0)).unwrap_or(0),
                        }))?.collect::<Result<Vec<_>, _>>()?;
                        ReadResult::DispatchItem(Some(DispatchItem { method, shared_params_json, patch_json, chat_id, chat_kind, retry_consumed, policy_snapshot_json, token_nonce, token_ciphertext, token_kid, files }))
                    } else {
                        ReadResult::DispatchItem(None)
                    }
                }
                ReadQuery::FileBlobPaths => {
                    let mut s = conn.prepare("SELECT blob_path FROM job_files")?;
                    let rows = s
                        .query_map([], |r| r.get(0))?
                        .collect::<Result<_, _>>()?;
                    ReadResult::TextList(rows)
                }
                ReadQuery::AcceptingJobIds => {
                    let mut s = conn.prepare("SELECT job_id FROM jobs WHERE state='accepting'")?;
                    let rows = s
                        .query_map([], |r| r.get(0))?
                        .collect::<Result<_, _>>()?;
                    ReadResult::TextList(rows)
                }

                ReadQuery::ActivePauses(now_unix) => {
                    let mut s = conn.prepare(
                        "SELECT scope_key, paused_until_unix FROM limiter_pauses WHERE paused_until_unix > ?1"
                    )?;
                    let rows = s.query_map([now_unix], |r| Ok((r.get(0)?, r.get(1)?)))?
                        .collect::<Result<_, _>>()?;
                    ReadResult::ActivePauses(rows)
                }
                ReadQuery::BlobBytes => ReadResult::BlobBytes(conn.query_row(
                    "SELECT COALESCE(SUM(size_bytes),0) FROM job_files",
                    [],
                    |r| r.get(0),
                )?),
ReadQuery::DbGauges => ReadResult::DbGauges(conn.query_row(
                    "SELECT
                      (SELECT count(*) FROM jobs WHERE state='accepting'),
                      (SELECT count(*) FROM jobs WHERE state IN ('queued','running')),
                      (SELECT coalesce(sum(queued),0) FROM jobs),
                      (SELECT coalesce(sum(delayed),0) FROM jobs),
                      (SELECT coalesce(sum(inflight),0) FROM jobs),
                      (SELECT coalesce(sum(succeeded),0) FROM jobs),
                      (SELECT coalesce(sum(failed),0) FROM jobs),
                      (SELECT coalesce(sum(ambiguous),0) FROM jobs),
                      (SELECT count(*) FROM webhook_events WHERE state='pending'),
                      (SELECT count(*) FROM webhook_events WHERE state='delivering'),
                      (SELECT count(*) FROM webhook_events WHERE state='delivered'),
                      (SELECT count(*) FROM webhook_events WHERE state='exhausted'),
                      (SELECT min(coalesce(started_at_unix,accepted_at_unix)) FROM jobs WHERE state IN ('accepting','queued','running'))",
                    [],
                    |r| {
                        let u = |i: usize| -> rusqlite::Result<u64> {
                            let v: i64 = r.get(i)?;
                            Ok(u64::try_from(v.max(0)).unwrap_or(0))
                        };
                        Ok(DbGauges {
                            jobs_accepting: u(0)?,
                            jobs_active: u(1)?,
                            recipients_queued: u(2)?,
                            recipients_delayed: u(3)?,
                            recipients_leased: u(4)?,
                            recipients_succeeded: u(5)?,
                            recipients_failed: u(6)?,
                            recipients_ambiguous: u(7)?,
                            webhook_pending: u(8)?,
                            webhook_delivering: u(9)?,
                            webhook_delivered: u(10)?,
                            webhook_exhausted: u(11)?,
                            oldest_active_start_unix: r.get(12)?,
                        })
                    },
                )?),
                ReadQuery::CompletedJobs {
                    after_second,
                    after_job,
                } => {
                    let mut s = conn.prepare(
                        "SELECT job_id, accepted_at_unix, completed_at_unix FROM jobs WHERE state='completed' AND completed_at_unix IS NOT NULL AND accepted_at_unix IS NOT NULL AND (completed_at_unix>?1 OR (completed_at_unix=?1 AND job_id>?2)) ORDER BY completed_at_unix, job_id LIMIT 5000",
                    )?;
                    let rows = s
                        .query_map(rusqlite::params![after_second, after_job], |r| {
                            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                        })?
                        .collect::<Result<_, _>>()?;
                    ReadResult::CompletedJobs(rows)
                }
            })
        })();
        let _ = r.reply.send(result);
    }
}
