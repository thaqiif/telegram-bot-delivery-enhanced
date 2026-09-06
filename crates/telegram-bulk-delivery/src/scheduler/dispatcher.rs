use crate::{
    domain::{AmbiguityPolicy, BotId},
    store::{CompletionOutcome, DispatchItem, Store, WriterCmd},
    telegram::classify::{classify, retries_ambiguous, Classification},
};
use serde_json::Value;
use std::sync::Arc;
use telegram_api_meta::{can_auto_migrate, lease_ttl_secs, MethodSpec};
#[derive(Debug, Clone)]
pub struct AttemptPolicy {
    pub retry_max: u16,
    pub retry_consumed: u16,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub ambiguity: AmbiguityPolicy,
}

/// Parse a job's `policy_snapshot_json` **once** and return the retry policy
/// together with the optional per-bot `telegram_api_base` extracted from the
/// same snapshot.  The snapshot is the source of truth both for routing (which
/// upstream to POST to) and for retry behaviour, so sharing the single parse
/// keeps the two in lock-step and avoids a second `serde_json` allocation on
/// the dispatch hot path.
pub fn snapshot_policy_and_base(item: &DispatchItem) -> (AttemptPolicy, Option<String>) {
    let config: serde_json::Value =
        serde_json::from_str(&item.policy_snapshot_json).unwrap_or_else(|_| serde_json::json!({}));
    let ambiguity = match config
        .get("ambiguity_policy")
        .and_then(serde_json::Value::as_str)
    {
        Some("at_most_once") => AmbiguityPolicy::AtMostOnce,
        Some("method_aware") => AmbiguityPolicy::MethodAware,
        _ => AmbiguityPolicy::AtLeastOnce,
    };
    let policy = AttemptPolicy {
        retry_max: config
            .get("retry_max_attempts")
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| u16::try_from(n).ok())
            .unwrap_or(8),
        retry_consumed: item.retry_consumed,
        retry_base_ms: config
            .get("retry_base_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(500),
        retry_max_ms: config
            .get("retry_max_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(60_000),
        ambiguity,
    };
    let per_bot_base = config
        .get("telegram_api_base")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter(|s| s.starts_with("http://") || s.starts_with("https://"))
        .map(str::to_owned);
    (policy, per_bot_base)
}

/// The bot's configured `target_msgs_per_sec` from the same policy snapshot,
/// defaulting to 20.0 and hard-clamped to the 25/s ceiling enforced at submit
/// time (http::api). The per-bot limiter bucket is created at this rate, so
/// the configured target is actually honored by the scheduler rather than
/// being decorative.
pub fn snapshot_target_rate(item: &DispatchItem) -> f64 {
    let config: serde_json::Value =
        serde_json::from_str(&item.policy_snapshot_json).unwrap_or_else(|_| serde_json::json!({}));
    config
        .get("target_msgs_per_sec")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(20.0)
        .clamp(0.1, 25.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::resolve_api_url;
    use crate::store::DispatchItem;

    fn item(base: Option<&str>) -> DispatchItem {
        let snap = match base {
            Some(b) => serde_json::json!({ "telegram_api_base": b }),
            None => serde_json::json!({}),
        };
        DispatchItem {
            method: "sendMessage".into(),
            shared_params_json: "{}".into(),
            patch_json: "{}".into(),
            chat_id: Some("1".into()),
            chat_kind: Some("private".into()),
            retry_consumed: 0,
            policy_snapshot_json: snap.to_string(),
            token_nonce: vec![0u8; 12],
            token_ciphertext: vec![1u8; 16],
            token_kid: "test".into(),
            files: vec![],
        }
    }

    // I-6: dispatch routing derives the per-bot URL from the job snapshot only.
    #[test]
    fn snapshot_routes_per_bot_and_global_distinctly() {
        let token = "t0ken";
        let global = "https://api.telegram.org";

        // Bot A: carries a per-bot base in its snapshot.
        let (_, per_bot_a) = snapshot_policy_and_base(&item(Some("http://127.0.0.1:9123")));
        assert_eq!(
            resolve_api_url(global, per_bot_a.as_deref(), token, "sendMessage"),
            "http://127.0.0.1:9123/bott0ken/sendMessage"
        );

        // Bot A with a {token} template layout.
        let (_, per_bot_t) =
            snapshot_policy_and_base(&item(Some("http://127.0.0.1:9123/bot{token}/test")));
        assert_eq!(
            resolve_api_url(global, per_bot_t.as_deref(), token, "sendMessage"),
            "http://127.0.0.1:9123/bott0ken/test/sendMessage"
        );

        // Bot B: snapshot has no base → None → resolves against the global host.
        let (_, per_bot_b) = snapshot_policy_and_base(&item(None));
        assert!(
            per_bot_b.is_none(),
            "absent base must be None, not a string"
        );
        assert_eq!(
            resolve_api_url(global, per_bot_b.as_deref(), token, "sendMessage"),
            "https://api.telegram.org/bott0ken/sendMessage"
        );

        // An empty-string base is clamped/normalized to None (global fallback).
        let (_, per_bot_empty) = snapshot_policy_and_base(&item(Some("   ")));
        assert!(
            per_bot_empty.is_none(),
            "whitespace-only base falls back to None"
        );
        assert_eq!(
            resolve_api_url(global, per_bot_empty.as_deref(), token, "sendMessage"),
            "https://api.telegram.org/bott0ken/sendMessage"
        );
    }
}

#[derive(Debug, Clone)]
pub struct LeasedCall {
    pub job_id: String,
    pub idx: u32,
    /// Per-attempt token issued by `LeaseBatch`; guards every attempt mutation
    /// so a stale sender can never overwrite a newer lease's outcome.
    pub lease_token: i64,
    pub bot_id: BotId,
    pub method: &'static MethodSpec,
    pub chat_id: Option<String>,
    pub policy: AttemptPolicy,
    pub migrated: bool,
}
#[derive(Clone)]
pub struct Dispatcher {
    store: Arc<Store>,
    epoch: String,
}
impl Dispatcher {
    pub fn new(store: Arc<Store>, epoch: impl Into<String>) -> Self {
        Self {
            store,
            epoch: epoch.into(),
        }
    }
    pub fn lease(
        &self,
        bot_id: BotId,
        spec: &MethodSpec,
        worker: u16,
        now: i64,
    ) -> Result<Option<(String, u32, i64)>, crate::store::StoreError> {
        match self.store.writer.execute(WriterCmd::LeaseBatch {
            bot_id,
            owner: format!("{}:{worker}", self.epoch),
            now_unix: now,
            lease_secs: lease_ttl_secs(spec),
            max: 1,
        })? {
            crate::store::WriterResult::Lease(v) => Ok(v),
            _ => Err(crate::store::StoreError::ResponseClosed),
        }
    }
    pub fn mark_wire_started(&self, call: &LeasedCall) -> Result<(), crate::store::StoreError> {
        self.store.writer.execute(WriterCmd::MarkWireStarted {
            job_id: call.job_id.clone(),
            idx: call.idx,
            lease_token: call.lease_token,
        })?;
        Ok(())
    }
    /// Persist the classification of one Telegram attempt. Returns a flood
    /// retry-after duration so the scheduler can durably pause the observed
    /// bot/chat scopes in addition to delaying this recipient.
    pub fn finish(
        &self,
        call: &LeasedCall,
        status: u16,
        body: Option<&Value>,
        transport_after_wire: bool,
        now: i64,
    ) -> Result<Option<u32>, crate::store::StoreError> {
        let c = classify(
            status,
            body,
            transport_after_wire,
            now,
            call.method,
            call.chat_id.as_deref(),
        );
        let flood_retry_after = match c {
            Classification::Flood { retry_after } => Some(retry_after),
            _ => None,
        };
        let outcome = match c {
            Classification::Success(s) => CompletionOutcome::Succeeded {
                message_id: s.message_id,
                sent_at_unix: s.sent_at_unix,
            },
            Classification::Flood { retry_after } => CompletionOutcome::Delayed {
                not_before_unix: now + i64::from(retry_after),
                error_norm: "flood".into(),
                consume_retry: false,
            },
            Classification::Transient => retry_or_fail(call, now, "transient"),
            Classification::Ambiguous => {
                if retries_ambiguous(call.policy.ambiguity, call.method) {
                    retry_or_fail(call, now, "ambiguous")
                } else {
                    CompletionOutcome::Ambiguous
                }
            }
            Classification::Permanent { norm } => CompletionOutcome::Failed {
                error_norm: norm.into(),
            },
            Classification::Unauthorized => {
                self.store.writer.execute(WriterCmd::FailBotUnauthorized {
                    bot_id: call.bot_id,
                    now_unix: now,
                })?;
                return Ok(None);
            }
            Classification::Migrate { chat_id } => {
                if !call.migrated && can_auto_migrate(call.method) {
                    if let Some(old) = &call.chat_id {
                        self.store.writer.execute(WriterCmd::MigrateRecipient {
                            job_id: call.job_id.clone(),
                            idx: call.idx,
                            old_chat_id: old.clone(),
                            new_chat_id: chat_id.to_string(),
                            lease_token: call.lease_token,
                        })?;
                        return Ok(None);
                    }
                }
                CompletionOutcome::Failed {
                    error_norm: "migrated".into(),
                }
            }
        };
        self.store.writer.execute(WriterCmd::CompleteAttempt {
            job_id: call.job_id.clone(),
            idx: call.idx,
            outcome,
            lease_token: call.lease_token,
        })?;
        Ok(flood_retry_after)
    }
}
fn retry_or_fail(call: &LeasedCall, now: i64, norm: &str) -> CompletionOutcome {
    if call.policy.retry_consumed.saturating_add(1) >= call.policy.retry_max {
        if norm == "ambiguous" {
            CompletionOutcome::Ambiguous
        } else {
            CompletionOutcome::Failed {
                error_norm: norm.into(),
            }
        }
    } else {
        let exp = call
            .policy
            .retry_base_ms
            .saturating_mul(
                1u64.checked_shl(u32::from(call.policy.retry_consumed))
                    .unwrap_or(u64::MAX),
            )
            .min(call.policy.retry_max_ms);
        CompletionOutcome::Delayed {
            not_before_unix: now + (exp.div_ceil(1000)) as i64,
            error_norm: norm.into(),
            consume_retry: true,
        }
    }
}
