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
    Flood {
        retry_after: u32,
    },
    Transient,
    /// `http_status`/`desc` are diagnostic only (sanitized Telegram
    /// `description`); `norm` stays the stable client-facing class.
    Permanent {
        norm: &'static str,
        http_status: u16,
        desc: Option<String>,
    },
    Unauthorized,
    Migrate {
        chat_id: i64,
    },
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
            Classification::Permanent {
                norm: "unknown",
                http_status: status,
                desc: None,
            }
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
            http_status: status,
            desc: v
                .get("description")
                .and_then(Value::as_str)
                .map(sanitize_desc),
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
/// Truncate to 200 chars and mask runs of 5+ digits (chat/user ids) as `<n>`.
pub fn sanitize_desc(raw: &str) -> String {
    let mut out = String::new();
    let mut digits = String::new();
    let flush = |out: &mut String, digits: &mut String| {
        if digits.len() >= 5 {
            out.push_str("<n>");
        } else {
            out.push_str(digits);
        }
        digits.clear();
    };
    for ch in raw.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            flush(&mut out, &mut digits);
            out.push(ch);
        }
    }
    flush(&mut out, &mut digits);
    out.chars().take(200).collect()
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
    #[test]
    fn permanent_captures_status_and_sanitized_description() {
        let m = method("deleteMessage").unwrap();
        let c = classify(
            400,
            Some(
                &serde_json::json!({"ok":false,"error_code":400,"description":"Bad Request: message to delete not found in chat 123456789 (id 42)"}),
            ),
            false,
            0,
            m,
            None,
        );
        assert_eq!(
            c,
            Classification::Permanent {
                norm: "invalid_recipient",
                http_status: 400,
                desc: Some("Bad Request: message to delete not found in chat <n> (id 42)".into()),
            }
        );
        assert_eq!(
            classify(
                403,
                Some(&serde_json::json!({"ok":false})),
                false,
                0,
                m,
                None
            ),
            Classification::Permanent {
                norm: "forbidden",
                http_status: 403,
                desc: None,
            }
        );
    }
    #[test]
    fn sanitize_truncates_and_masks_ids() {
        assert_eq!(
            sanitize_desc("user -1001234567890 and 1234"),
            "user -<n> and 1234"
        );
        assert_eq!(sanitize_desc("12345"), "<n>");
        assert_eq!(sanitize_desc(&"é".repeat(300)).chars().count(), 200);
    }
}
