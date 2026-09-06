pub mod dispatcher;
pub mod fairness;
pub mod limiters;
pub use fairness::Wdrr;
pub use limiters::{Limiters, Scope};
/// How often the scheduler reconciles durable ready work, wakes sleeping
/// bots, and evicts idle limiter buckets. This granularity bounds how long a
/// rate-limited (future-dated) recipient waits before its bot is readmitted:
/// a 2s tick quantized throughput to the bot bucket burst (~10 per 2s ≈ 5/s).
/// ~250ms keeps steady-state pace near the configured limiter target (e.g.
/// 25/s) instead of the wake cadence.
pub const RECONCILE_INTERVAL_MS: u64 = 250;
pub const PER_BOT_CONCURRENCY: u8 = 4;
