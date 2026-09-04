use crate::domain::AmbiguityPolicy;
use rand::Rng;
use serde_json::Value;
use telegram_api_meta::{method_aware_retries, MethodSpec, SuccessProjection};
#[derive(Debug, Clone, PartialEq)]
pub struct Success {
    pub message_id: Option<i64>,
    pub sent_at_unix: i64,
    pub chat_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Classification {
    Success(Success),
    Flood { retry_after: u32 },
    Transient,
    Permanent { norm: &'static str },
    Unauthorized,
    Migrate { chat_id: i64 },
    Ambiguous,
}
pub fn classify(
    status: u16,
    body: Option<&Value>,
    transport_after_wire: bool,
    now: i64,
    spec: &MethodSpec,
    resolved_chat: Option<&str>,
) -> Classification {
    if transport_after_wire {
        return Classification::Ambiguous;
    }
    let Some(v) = body else {
        return if status >= 500 {
            Classification::Transient
        } else {
            Classification::Permanent { norm: "unknown" }
        };
    };
    if let Some(n) = v.pointer("/parameters/retry_after").and_then(Value::as_u64) {
        return Classification::Flood {
            retry_after: n.min(u64::from(u32::MAX)) as u32,
        };
    }
    // A proxy or edge response can preserve HTTP 429 while omitting the Bot
    // API retry_after field. Keep the recipient retryable rather than turning
    // an unknown rate-limit response into a permanent recipient failure.
    if status == 429 {
        return Classification::Flood { retry_after: 1 };
    }
    if let Some(n) = v
        .pointer("/parameters/migrate_to_chat_id")
        .and_then(Value::as_i64)
    {
        return Classification::Migrate { chat_id: n };
    }
    if status == 401 {
        return Classification::Unauthorized;
    }
    if status >= 500 {
        return Classification::Transient;
    }
    if !v.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        return Classification::Permanent {
            norm: if status == 403 {
                "forbidden"
            } else if status == 404 {
                "not_found"
            } else {
                "invalid_recipient"
            },
        };
    }
    let result = &v["result"];
    let first = if result.is_array() {
        result.as_array().and_then(|a| a.first()).unwrap_or(result)
    } else {
        result
    };
    let message_id = match spec.projection {
        SuccessProjection::None => None,
        _ => first.get("message_id").and_then(Value::as_i64),
    };
    let sent_at_unix = first.get("date").and_then(Value::as_i64).unwrap_or(now);
    let chat_id = resolved_chat.map(str::to_owned).or_else(|| {
        first.pointer("/chat/id").map(|x| {
            x.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| x.to_string())
        })
    });
    Classification::Success(Success {
        message_id,
        sent_at_unix,
        chat_id,
    })
}
pub fn retries_ambiguous(policy: AmbiguityPolicy, spec: &MethodSpec) -> bool {
    match policy {
        AmbiguityPolicy::AtLeastOnce => true,
        AmbiguityPolicy::AtMostOnce => false,
        AmbiguityPolicy::MethodAware => method_aware_retries(spec),
    }
}
pub fn full_jitter_ms(base: u64, max: u64, retry_consumed: u16, rng: &mut impl Rng) -> u64 {
    let cap = base
        .saturating_mul(
            1u64.checked_shl(u32::from(retry_consumed))
                .unwrap_or(u64::MAX),
        )
        .min(max);
    if cap == 0 {
        0
    } else {
        rng.gen_range(0..=cap)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use telegram_api_meta::method;
    #[test]
    fn all_classes() {
        let m = method("sendMessage").unwrap();
        assert!(matches!(
            classify(
                429,
                Some(&serde_json::json!({"ok":false,"parameters":{"retry_after":7}})),
                false,
                0,
                m,
                None
            ),
            Classification::Flood { retry_after: 7 }
        ));
        assert!(matches!(
            classify(
                429,
                Some(&serde_json::json!({"ok":false})),
                false,
                0,
                m,
                None
            ),
            Classification::Flood { retry_after: 1 }
        ));
        assert!(matches!(
            classify(503, None, false, 0, m, None),
            Classification::Transient
        ));
        assert!(matches!(
            classify(
                401,
                Some(&serde_json::json!({"ok":false})),
                false,
                0,
                m,
                None
            ),
            Classification::Unauthorized
        ));
        assert!(matches!(
            classify(200, None, true, 0, m, None),
            Classification::Ambiguous
        ));
    }
}
