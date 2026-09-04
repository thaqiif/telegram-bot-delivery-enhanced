use crate::{
    domain::{AmbiguityPolicy, BotId},
    store::{CompletionOutcome, Store, WriterCmd},
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
