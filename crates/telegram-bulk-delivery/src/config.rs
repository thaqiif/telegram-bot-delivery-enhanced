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
    /// Completion-webhook SSRF opt-in (default off). When true, plain `http`
    /// webhooks are allowed — but ONLY to hosts listed in
    /// `webhook_trusted_hosts`. Everything else stays HTTPS-to-public-only.
    /// Intended for a receiver on the same trusted host/network (staging).
    #[serde(default)]
    pub webhook_allow_insecure_http: bool,
    /// IP literals the webhook deliverer trusts. A trusted host is exempt from
    /// ALL SSRF address checks (private/loopback, denied names, DNS pinning) and
    /// may use http when allowed above — hence IP literals only (validated),
    /// never link-local/metadata. Empty by default.
    #[serde(default)]
    pub webhook_trusted_hosts: Vec<String>,
    /// Process-wide send rate across ALL bots (msg/s). Each bot keeps its own
    /// ≤25/s limit and Telegram limits per bot, so raise this when one
    /// instance serves several busy bots. Default 25 (the historic hard-coded value).
    #[serde(default = "default_global_msgs_per_sec")]
    pub global_msgs_per_sec: f64,
}

fn default_global_msgs_per_sec() -> f64 {
    25.0
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
        // Trusted webhook hosts skip DNS resolution/pinning and address checks,
        // so a trusted HOSTNAME would let whoever controls its DNS aim webhooks
        // at metadata/loopback. Only IP literals are accepted, and never
        // link-local/metadata ranges.
        for h in &self.webhook_trusted_hosts {
            let ip = crate::webhook::parse_ip_literal(h.trim()).ok_or(ConfigError::InvalidLimit(
                "webhook_trusted_hosts must be IP literals (no hostnames, no IPv6 zone ids)",
            ))?;
            if crate::webhook::never_trustable(ip) {
                return Err(ConfigError::InvalidLimit(
                    "webhook_trusted_hosts must not contain metadata, link-local, unspecified or multicast addresses",
                ));
            }
        }
        if !(self.global_msgs_per_sec >= 1.0 && self.global_msgs_per_sec <= 1000.0) {
            return Err(ConfigError::InvalidLimit("global_msgs_per_sec must be between 1 and 1000"));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn with(extra: &str) -> Result<(), ConfigError> {
        let base = include_str!("../../../config/operator.defaults.toml");
        let cfg: OperatorConfig = toml::from_str(&format!("{base}\n{extra}")).expect("parses");
        cfg.validate()
    }

    #[test]
    fn webhook_trust_defaults_off_and_existing_configs_still_load() {
        let base = include_str!("../../../config/operator.defaults.toml");
        let cfg: OperatorConfig = toml::from_str(base).expect("parses without the new keys");
        assert!(!cfg.webhook_allow_insecure_http);
        assert!(cfg.webhook_trusted_hosts.is_empty());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn global_rate_defaults_to_25_and_is_validated() {
        let base = include_str!("../../../config/operator.defaults.toml");
        let cfg: OperatorConfig = toml::from_str(base).expect("parses");
        assert_eq!(cfg.global_msgs_per_sec, 25.0);
        assert!(with("global_msgs_per_sec = 75.0").is_ok());
        assert!(with("global_msgs_per_sec = 0.5").is_err());
        assert!(with("global_msgs_per_sec = 5000.0").is_err());
    }

    #[test]
    fn webhook_trusted_hosts_accepts_ip_literals_only() {
        assert!(with(r#"webhook_trusted_hosts = ["127.0.0.1", "10.0.0.5", "::1", "[fd12::5]"]"#).is_ok());
        assert!(with(r#"webhook_trusted_hosts = ["0:0:0:0:0:0:0:1", "FD12:0::5", "192.168.1.10"]"#).is_ok());
        for bad in [
            "hooks.example.com", "localhost", "169.254.169.254", "0.0.0.0", "0.1.2.3", "fe80::1", "fe80::1%eth0",
            "fd00:ec2::254", "255.255.255.255", "224.0.0.1", "ff02::1", "fec0::1",
            "::ffff:169.254.169.254", "::ffff:a9fe:a9fe", "::ffff:0.0.0.0", "100.100.100.200", "168.63.129.16",
        ] {
            assert!(
                with(&format!(r#"webhook_trusted_hosts = ["{bad}"]"#)).is_err(),
                "{bad} must be rejected"
            );
        }
    }
}
