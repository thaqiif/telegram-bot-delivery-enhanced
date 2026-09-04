//! Operator observability: a dependency-free Prometheus text exposition
//! registry plus process-level gauges.
//!
//! Design notes (see `.omc/plans/telegram-bulk-delivery-engine-v1.md` §metrics):
//! - Counters that fire on hot paths are plain atomics: writers and the writer
//!   thread update them lock-free.
//! - DB-derived gauges (recipient/job/webhook counts, queue age) are refreshed
//!   by a maintenance snapshot and applied atomically under one short mutex, so
//!   `/metrics` renders one consistent picture without ever scanning the large
//!   `recipients` table (aggregates come from the `jobs` ledger columns, which
//!   reconcile keeps exact and which are far smaller).
//! - Nothing here ever records tokens, chat ids, webhook bodies, or any other
//!   high-cardinality identifier.

use crate::store::CheckpointMode;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

/// Writer checkpoint policy required by US-005. This decision is deliberately
/// independent of dispatcher/grant state: a paused bot cannot suppress the
/// WAL-size backstop.
pub fn checkpoint_mode(wal_bytes: u64, truncate_threshold: u64) -> CheckpointMode {
    if wal_bytes >= truncate_threshold {
        CheckpointMode::Truncate
    } else {
        CheckpointMode::Passive
    }
}

/// Upper bounds (inclusive) for the job wall-clock histogram. Last bucket is +Inf.
const WALLCLOCK_BUCKETS_SECS: [f64; 16] = [
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    25.0,
    60.0,
    120.0,
    300.0,
    600.0,
    1800.0,
    3600.0,
    f64::INFINITY,
];
const OUTCOME_CLASSES: [&str; 4] = ["succeeded", "failed", "ambiguous", "delayed"];

/// Per-writer outcome classes (mirrors `CompletionOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Succeeded,
    Failed,
    Ambiguous,
    Delayed,
}
impl Outcome {
    pub fn class(self) -> &'static str {
        match self {
            Outcome::Succeeded => "succeeded",
            Outcome::Failed => "failed",
            Outcome::Ambiguous => "ambiguous",
            Outcome::Delayed => "delayed",
        }
    }
    fn index(self) -> usize {
        OUTCOME_CLASSES
            .iter()
            .position(|c| *c == self.class())
            .unwrap()
    }
}

/// DB-derived gauges refreshed by the maintenance snapshot task.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DbGauges {
    pub jobs_accepting: u64,
    pub jobs_active: u64,
    pub recipients_queued: u64,
    pub recipients_delayed: u64,
    pub recipients_leased: u64,
    pub recipients_succeeded: u64,
    pub recipients_failed: u64,
    pub recipients_ambiguous: u64,
    pub webhook_pending: u64,
    pub webhook_delivering: u64,
    pub webhook_delivered: u64,
    pub webhook_exhausted: u64,
    /// `min(coalesce(started_at_unix, accepted_at_unix))` over non-terminal jobs.
    pub oldest_active_start_unix: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Gauges {
    jobs_accepting: u64,
    jobs_active: u64,
    recipients_queued: u64,
    recipients_delayed: u64,
    recipients_leased: u64,
    recipients_succeeded: u64,
    recipients_failed: u64,
    recipients_ambiguous: u64,
    webhook_pending: u64,
    webhook_delivering: u64,
    webhook_delivered: u64,
    webhook_exhausted: u64,
    oldest_nonterminal_age_seconds: u64,
    writer_queue_depth: u64,
    ready_bots: u64,
    sqlite_wal_bytes: u64,
    process_rss_bytes: u64,
    cgroup_memory_current_bytes: u64,
}

/// Shared metric registry. Cheap to clone through `Arc`; every method is
/// interior-mutable and safe to call from the writer thread and any handler.
pub struct Metrics {
    ingest_recipients_accepted: AtomicU64,
    token_validation_failures: AtomicU64,
    flood_waits: AtomicU64,
    retries_scheduled: AtomicU64,
    retention_deleted_rows: AtomicU64,
    outcomes: [AtomicU64; 4],
    wallclock_buckets: [AtomicU64; 16],
    wallclock_count: AtomicU64,
    wallclock_sum_ms: AtomicU64,
    /// Keyset cursor over `(completed_at_unix, job_id)` for the wall-clock scan.
    wallclock_cursor: Mutex<(i64, String)>,
    gauges: Mutex<Gauges>,
}

fn zeroed_4() -> [AtomicU64; 4] {
    [const { AtomicU64::new(0) }; 4]
}
fn zeroed_16() -> [AtomicU64; 16] {
    [const { AtomicU64::new(0) }; 16]
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            ingest_recipients_accepted: AtomicU64::new(0),
            token_validation_failures: AtomicU64::new(0),
            flood_waits: AtomicU64::new(0),
            retries_scheduled: AtomicU64::new(0),
            retention_deleted_rows: AtomicU64::new(0),
            outcomes: zeroed_4(),
            wallclock_buckets: zeroed_16(),
            wallclock_count: AtomicU64::new(0),
            wallclock_sum_ms: AtomicU64::new(0),
            wallclock_cursor: Mutex::new((0, String::new())),
            gauges: Mutex::new(Gauges::default()),
        }
    }

    // ---- runtime counters -------------------------------------------------

    pub fn record_ingest_accepted(&self, recipients: u64) {
        self.ingest_recipients_accepted
            .fetch_add(recipients, Ordering::Relaxed);
    }
    pub fn record_token_validation_failure(&self) {
        self.token_validation_failures
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_flood_wait(&self) {
        self.flood_waits.fetch_add(1, Ordering::Relaxed);
    }
    /// A send was delayed and a retry has been scheduled for it.
    pub fn record_retry_scheduled(&self) {
        self.retries_scheduled.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_outcome(&self, outcome: Outcome) {
        self.outcomes[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_retention_rows(&self, rows: u64) {
        self.retention_deleted_rows
            .fetch_add(rows, Ordering::Relaxed);
    }

    // ---- wall-clock histogram (maintenance cursor) ------------------------

    /// Next `(completed_at_unix, job_id)` watermark to scan from.
    pub fn wallclock_cursor(&self) -> (i64, String) {
        self.wallclock_cursor.lock().unwrap().clone()
    }
    /// Advance the cursor to the last row of `rows` and record each wall-clock
    /// sample (rows must be ordered by `(completed_at_unix, job_id)` ascending).
    pub fn record_completed(&self, rows: &[(String, i64, i64)]) {
        if let Some((job, _accepted, completed)) = rows.last() {
            *self.wallclock_cursor.lock().unwrap() = (*completed, job.clone());
        }
        for (_job, accepted, completed) in rows {
            let secs = completed.saturating_sub(*accepted) as f64;
            let ms = (secs * 1000.0).round() as u64;
            self.wallclock_count.fetch_add(1, Ordering::Relaxed);
            self.wallclock_sum_ms.fetch_add(ms, Ordering::Relaxed);
            let idx = WALLCLOCK_BUCKETS_SECS
                .iter()
                .position(|le| secs <= *le)
                .unwrap_or(WALLCLOCK_BUCKETS_SECS.len() - 1);
            self.wallclock_buckets[idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    // ---- gauges -----------------------------------------------------------

    pub fn apply_db(&self, db: &DbGauges, now_unix: i64) {
        let age = db
            .oldest_active_start_unix
            .map(|s| now_unix.saturating_sub(s).max(0) as u64)
            .unwrap_or(0);
        let mut g = self.gauges.lock().unwrap();
        g.jobs_accepting = db.jobs_accepting;
        g.jobs_active = db.jobs_active;
        g.recipients_queued = db.recipients_queued;
        g.recipients_delayed = db.recipients_delayed;
        g.recipients_leased = db.recipients_leased;
        g.recipients_succeeded = db.recipients_succeeded;
        g.recipients_failed = db.recipients_failed;
        g.recipients_ambiguous = db.recipients_ambiguous;
        g.webhook_pending = db.webhook_pending;
        g.webhook_delivering = db.webhook_delivering;
        g.webhook_delivered = db.webhook_delivered;
        g.webhook_exhausted = db.webhook_exhausted;
        g.oldest_nonterminal_age_seconds = age;
    }
    pub fn set_writer_queue_depth(&self, v: u64) {
        self.gauges.lock().unwrap().writer_queue_depth = v;
    }
    pub fn set_ready_bots(&self, v: u64) {
        self.gauges.lock().unwrap().ready_bots = v;
    }
    pub fn set_wal_bytes(&self, v: u64) {
        self.gauges.lock().unwrap().sqlite_wal_bytes = v;
    }
    pub fn set_rss_bytes(&self, v: u64) {
        self.gauges.lock().unwrap().process_rss_bytes = v;
    }
    pub fn set_cgroup_memory_current_bytes(&self, v: u64) {
        self.gauges.lock().unwrap().cgroup_memory_current_bytes = v;
    }

    // ---- Prometheus text exposition ---------------------------------------

    pub fn render(&self) -> String {
        let g = self.gauges.lock().unwrap();
        let mut out = String::with_capacity(4096);
        macro_rules! counter {
            ($name:literal, $help:literal, $v:expr) => {{
                out.push_str("# HELP ");
                out.push_str($name);
                out.push_str(" ");
                out.push_str($help);
                out.push('\n');
                out.push_str("# TYPE ");
                out.push_str($name);
                out.push_str(" counter\n");
                out.push_str($name);
                out.push(' ');
                out.push_str(&$v.to_string());
                out.push('\n');
            }};
        }
        macro_rules! gauge {
            ($name:literal, $help:literal, $v:expr) => {{
                out.push_str("# HELP ");
                out.push_str($name);
                out.push_str(" ");
                out.push_str($help);
                out.push('\n');
                out.push_str("# TYPE ");
                out.push_str($name);
                out.push_str(" gauge\n");
                out.push_str($name);
                out.push(' ');
                out.push_str(&$v.to_string());
                out.push('\n');
            }};
        }

        counter!(
            "ingest_recipients_accepted_total",
            "Recipients durably accepted (promoted) across all bulk jobs.",
            self.ingest_recipients_accepted.load(Ordering::Relaxed)
        );
        counter!(
            "token_validation_failures_total",
            "Bot tokens rejected by Telegram getMe (401) since boot.",
            self.token_validation_failures.load(Ordering::Relaxed)
        );
        counter!(
            "telegram_flood_waits_total",
            "Flood/backoff pauses applied to limiter scopes since boot.",
            self.flood_waits.load(Ordering::Relaxed)
        );
        counter!(
            "telegram_retries_total",
            "Retries scheduled (delayed sends) since boot.",
            self.retries_scheduled.load(Ordering::Relaxed)
        );
        counter!(
            "retention_deleted_rows_total",
            "Terminal jobs deleted by incremental retention since boot.",
            self.retention_deleted_rows.load(Ordering::Relaxed)
        );

        out.push_str("# TYPE telegram_outcomes_total counter\n");
        for (i, class) in OUTCOME_CLASSES.iter().enumerate() {
            out.push_str(&format!(
                "telegram_outcomes_total{{class=\"{class}\"}} {}\n",
                self.outcomes[i].load(Ordering::Relaxed)
            ));
        }

        gauge!(
            "ingest_accepting_jobs",
            "Bulk jobs currently accepting.",
            g.jobs_accepting
        );
        gauge!(
            "jobs_active",
            "Jobs queued or running (dispatched).",
            g.jobs_active
        );
        gauge!(
            "recipients_queued",
            "Recipients waiting to be sent.",
            g.recipients_queued
        );
        gauge!(
            "recipients_delayed",
            "Recipients in rate-limit backoff.",
            g.recipients_delayed
        );
        gauge!(
            "recipients_inflight",
            "Recipients leased and on the wire.",
            g.recipients_leased
        );

        out.push_str("# TYPE recipients_terminal_total gauge\n");
        for (class, v) in [
            ("succeeded", g.recipients_succeeded),
            ("failed", g.recipients_failed),
            ("ambiguous", g.recipients_ambiguous),
        ] {
            out.push_str(&format!(
                "recipients_terminal_total{{class=\"{class}\"}} {v}\n"
            ));
        }

        gauge!(
            "oldest_nonterminal_recipient_age_seconds",
            "Age of the oldest non-terminal job (queue age proxy).",
            g.oldest_nonterminal_age_seconds
        );

        out.push_str("# TYPE webhook_events gauge\n");
        for (state, v) in [
            ("pending", g.webhook_pending),
            ("delivering", g.webhook_delivering),
            ("delivered", g.webhook_delivered),
            ("exhausted", g.webhook_exhausted),
        ] {
            out.push_str(&format!("webhook_events{{state=\"{state}\"}} {v}\n"));
        }

        gauge!(
            "writer_queue_depth",
            "Writer commands awaiting the SQLite writer thread.",
            g.writer_queue_depth
        );
        gauge!(
            "ready_bots",
            "Bots with runnable work in the WDRR scheduler.",
            g.ready_bots
        );
        gauge!(
            "sqlite_wal_bytes",
            "Current SQLite WAL file size in bytes.",
            g.sqlite_wal_bytes
        );
        gauge!(
            "process_rss_bytes",
            "Process resident set size in bytes.",
            g.process_rss_bytes
        );
        gauge!(
            "cgroup_memory_current_bytes",
            "cgroup v2 memory.current in bytes (0 when unavailable).",
            g.cgroup_memory_current_bytes
        );

        // Job wall-clock histogram.
        let count = self.wallclock_count.load(Ordering::Relaxed);
        out.push_str("# TYPE job_wallclock_seconds histogram\n");
        for (i, le) in WALLCLOCK_BUCKETS_SECS.iter().enumerate() {
            let cumulative: u64 = self.wallclock_buckets[..=i]
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .sum();
            // Prometheus spells the implicit final bucket `+Inf`; Rust would
            // render f64::INFINITY as `inf`, a non-canonical label.
            let le = if le.is_infinite() {
                "+Inf".to_string()
            } else {
                format!("{le}")
            };
            out.push_str(&format!(
                "job_wallclock_seconds_bucket{{le=\"{le}\"}} {cumulative}\n"
            ));
        }
        out.push_str(&format!(
            "job_wallclock_seconds_count {count}\njob_wallclock_seconds_sum {:.3}\n",
            self.wallclock_sum_ms.load(Ordering::Relaxed) as f64 / 1000.0
        ));
        out
    }
}

/// Resident set size from `/proc/self/statm` (Linux). 0 when unreadable.
pub fn rss_bytes() -> u64 {
    let statm = match std::fs::read_to_string("/proc/self/statm") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    resident_pages.saturating_mul(page_size())
}

fn page_size() -> u64 {
    // 4096 is universal on x86-64/aarch64 Linux; a sysconf() here would need libc.
    4096
}

/// cgroup v2 memory.current (the admission signal required by the plan).
/// Returns 0 when unavailable (non-cgroup environments and tests).
pub fn cgroup_memory_current_bytes() -> u64 {
    let v2 = std::fs::read_to_string("/sys/fs/cgroup/memory.current");
    let path = match v2 {
        Ok(v) => {
            let n: u64 = v.trim().parse().unwrap_or(0);
            return n;
        }
        Err(_) => "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// Admission memory signal. A 1 GiB production cgroup reports `memory.current`
/// well above 700 MiB once page cache and SQLite buffers accumulate, which is
/// exactly when new ingests must shed. The check only fires inside a bounded
/// cgroup at or below the 1.5 GiB reference: on an unbounded or much larger
/// shared host, neighbouring tenants' usage is not this process's signal, so we
/// stay quiet and rely on RSS admission instead.
pub fn admission_memory_pressure() -> bool {
    let current = cgroup_memory_current_bytes();
    if current == 0 {
        return false;
    }
    let max = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    if max > 1536 * 1024 * 1024 {
        return false;
    }
    current > 700 * 1024 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_exposes_all_required_metric_names() {
        let m = Metrics::new();
        m.record_token_validation_failure();
        m.record_outcome(Outcome::Succeeded);
        m.record_retention_rows(3);
        m.record_completed(&[("j1".into(), 1000, 1010), ("j2".into(), 1000, 1030)]);
        let text = m.render();
        for name in [
            "token_validation_failures_total",
            "ingest_accepting_jobs",
            "job_wallclock_seconds_bucket",
            "job_wallclock_seconds_count",
            "oldest_nonterminal_recipient_age_seconds",
            "cgroup_memory_current_bytes",
            "telegram_outcomes_total{class=\"succeeded\"} 1",
            "retention_deleted_rows_total 3",
        ] {
            assert!(text.contains(name), "missing {name:?} in:\n{text}");
        }
    }

    #[test]
    fn histogram_cumulates_into_expected_bucket() {
        let m = Metrics::new();
        m.record_completed(&[("j".into(), 1000, 1000 + 5)]); // 5 s -> bucket le=10 not le=2.5
        let text = m.render();
        assert!(text.contains("job_wallclock_seconds_bucket{le=\"2.5\"} 0"));
        assert!(text.contains("job_wallclock_seconds_bucket{le=\"10\"} 1"));
        assert!(text.contains("job_wallclock_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(text.contains("job_wallclock_seconds_sum 5.000"));
    }

    #[test]
    fn threshold_forces_truncate_independent_of_dispatch_state() {
        assert!(matches!(
            checkpoint_mode(63 * 1024 * 1024, 64 * 1024 * 1024),
            CheckpointMode::Passive
        ));
        assert!(matches!(
            checkpoint_mode(64 * 1024 * 1024, 64 * 1024 * 1024),
            CheckpointMode::Truncate
        ));
        assert!(matches!(
            checkpoint_mode(256 * 1024 * 1024, 64 * 1024 * 1024),
            CheckpointMode::Truncate
        ));
    }
}
