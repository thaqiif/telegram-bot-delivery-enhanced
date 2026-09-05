use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorConfig {
    pub database_path: PathBuf,
    pub bind: SocketAddr,
    pub key_id: String,
    pub writer_queue_capacity: usize,
    pub reader_queue_capacity: usize,
    pub max_concurrent_http: usize,
    pub max_telegram_inflight: usize,
    pub max_webhook_inflight: usize,
    pub max_blocking_threads: usize,
    pub max_recipients_per_job: usize,
    pub global_nonterminal_recipients: usize,
    pub request_body_bytes: usize,
    pub multipart_body_bytes: usize,
    pub shared_parameters_bytes: usize,
    pub recipient_patch_bytes: usize,
    pub free_disk_reserve_bytes: u64,
    pub storage_high_watermark_bytes: u64,
    /// SQLite WAL size (bytes) at which the maintenance task forces a
    /// writer-owned `wal_checkpoint(TRUNCATE)`, even while grants are paused.
    pub wal_truncate_bytes: u64,
    /// How often the maintenance task sends a `RetentionTick` (seconds). The
    /// story bound is "seven days plus at most fifteen minutes".
    pub retention_sweep_secs: u64,
    /// Maximum terminal jobs deleted per `RetentionTick`.
    pub retention_batch: u32,
    /// SSRF default-safe switch. When false, a per-bot `telegram_api_base` that
    /// targets a non-public (loopback/private/link-local/metadata) host is
    /// refused. Set true to allow pointing bots at a private/local bot server.
    pub allow_private_targets: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read operator config: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid operator config: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("BULK_MASTER_KEY is required")]
    MissingMasterKey,
    #[error("BULK_MASTER_KEY must be base64 encoding of exactly 32 bytes")]
    InvalidMasterKey,
    #[error("invalid operator limit: {0}")]
    InvalidLimit(&'static str),
}

impl OperatorConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<(Self, Zeroizing<[u8; 32]>), ConfigError> {
        let config: Self = toml::from_str(&fs::read_to_string(path)?)?;
        config.validate()?;
        let mut encoded = env::var("BULK_MASTER_KEY").map_err(|_| ConfigError::MissingMasterKey)?;
        let decoded = STANDARD
            .decode(encoded.as_bytes())
            .map_err(|_| ConfigError::InvalidMasterKey)?;
        encoded.zeroize();
        let key: [u8; 32] = decoded
            .try_into()
            .map_err(|_| ConfigError::InvalidMasterKey)?;
        Ok((config, Zeroizing::new(key)))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let checks = [
            (
                self.writer_queue_capacity > 0 && self.writer_queue_capacity <= 128,
                "writer_queue_capacity",
            ),
            (self.reader_queue_capacity > 0, "reader_queue_capacity"),
            (
                self.max_concurrent_http > 0 && self.max_concurrent_http <= 64,
                "max_concurrent_http",
            ),
            (
                self.max_telegram_inflight > 0 && self.max_telegram_inflight <= 32,
                "max_telegram_inflight",
            ),
            (
                self.max_webhook_inflight > 0 && self.max_webhook_inflight <= 4,
                "max_webhook_inflight",
            ),
            (
                self.max_blocking_threads > 0 && self.max_blocking_threads <= 8,
                "max_blocking_threads",
            ),
            (
                self.max_recipients_per_job > 0 && self.max_recipients_per_job <= 100_000,
                "max_recipients_per_job",
            ),
            (
                self.global_nonterminal_recipients > 0
                    && self.global_nonterminal_recipients <= 500_000,
                "global_nonterminal_recipients",
            ),
            (
                self.request_body_bytes > 0 && self.request_body_bytes <= 32 * 1024 * 1024,
                "request_body_bytes",
            ),
            (
                self.multipart_body_bytes > 0 && self.multipart_body_bytes <= 64 * 1024 * 1024,
                "multipart_body_bytes",
            ),
            (
                self.shared_parameters_bytes > 0 && self.shared_parameters_bytes <= 256 * 1024,
                "shared_parameters_bytes",
            ),
            (
                self.recipient_patch_bytes > 0 && self.recipient_patch_bytes <= 16 * 1024,
                "recipient_patch_bytes",
            ),
            (
                (1..=512 * 1024 * 1024).contains(&self.wal_truncate_bytes),
                "wal_truncate_bytes",
            ),
            (
                (1..=900).contains(&self.retention_sweep_secs),
                "retention_sweep_secs",
            ),
            (
                (1..=2000).contains(&self.retention_batch),
                "retention_batch",
            ),
        ];
        for (valid, name) in checks {
            if !valid {
                return Err(ConfigError::InvalidLimit(name));
            }
        }
        Ok(())
    }
}
