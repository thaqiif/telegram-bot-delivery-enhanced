/// Resolve the outbound Telegram Bot API URL for one call.
///
/// - `global_base`: the process default (from `TELEGRAM_API_BASE`, default `https://api.telegram.org`).
/// - `per_bot_base`: the bot's `telegram_api_base` (`None`/null → use global; `Some(s)` validated in `clamp`).
/// - `token`: opaque Bot API token (only substituted when the template contains `{token}`).
/// - `method`: Telegram method name (`getMe`, `sendMessage`, `sendPhoto`, ... from `telegram_api_meta`).
///
/// Rules:
/// - Trim trailing `/` from the base.
/// - If base contains the literal substring `{token}` → replace **every** occurrence with `token`,
///   then join `/{method}` (ensuring exactly one `/` between prefix and method; if the substituted
///   base already ends with `/`, don't double).
/// - Else → `"{base}/bot{token}/{method}"` (the historical shape).
/// - The same function builds both getMe and send URLs — the only difference is `method`.
///
/// The per-bot base is validated for shape (`http(s)://` prefix, in `clamp`) but **never
/// reachability-checked**. Global fallback applies ONLY when the base is absent/null/empty.
/// If a per-bot base is set but the host is down/unreachable, the send fails through the
/// normal retry path — the builder does NOT silently substitute the global host.
pub fn resolve_api_url(
    global_base: &str,
    per_bot_base: Option<&str>,
    token: &str,
    method: &str,
) -> String {
    let base = per_bot_base
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(global_base);
    let base = base.trim_end_matches('/');

    if base.contains("{token}") {
        // {token} template: substitute all occurrences, then append /{method}
        let replaced = base.replace("{token}", token);
        // Ensure exactly one slash between the substituted base and the method
        format!("{}/{}", replaced.trim_end_matches('/'), method)
    } else {
        // Plain prefix: standard shape
        format!("{}/bot{}/{}", base, token, method)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_plain_prefix() {
        // U-1
        let url = resolve_api_url(
            "https://api.telegram.org",
            Some("http://127.0.0.1:9123"),
            "t0ken",
            "sendMessage",
        );
        assert_eq!(url, "http://127.0.0.1:9123/bott0ken/sendMessage");
    }

    #[test]
    fn resolve_global_fallback() {
        // U-2
        let url = resolve_api_url("https://api.telegram.org", None, "t0ken", "sendMessage");
        assert_eq!(url, "https://api.telegram.org/bott0ken/sendMessage");
    }

    #[test]
    fn resolve_token_template() {
        // U-3
        let url = resolve_api_url(
            "https://api.telegram.org",
            Some("https://api.telegram.org/bot{token}/test"),
            "t0ken",
            "sendMessage",
        );
        assert_eq!(url, "https://api.telegram.org/bott0ken/test/sendMessage");

        // getMe variant
        let url_get = resolve_api_url(
            "https://api.telegram.org",
            Some("https://api.telegram.org/bot{token}/test"),
            "t0ken",
            "getMe",
        );
        assert_eq!(url_get, "https://api.telegram.org/bott0ken/test/getMe");
    }

    #[test]
    fn resolve_trailing_slash() {
        // U-4: both plain and template bases with trailing /
        let url1 = resolve_api_url(
            "https://api.telegram.org",
            Some("http://127.0.0.1:9123/"),
            "t0ken",
            "sendMessage",
        );
        assert_eq!(url1, "http://127.0.0.1:9123/bott0ken/sendMessage");

        let url2 = resolve_api_url(
            "https://api.telegram.org",
            Some("https://api.telegram.org/bot{token}/test/"),
            "t0ken",
            "sendMessage",
        );
        assert_eq!(url2, "https://api.telegram.org/bott0ken/test/sendMessage");
    }

    #[test]
    fn resolve_token_case_not_substituted() {
        // U-5: {Token} (capital T) NOT substituted — only exact {token}.
        // A base without the exact `{token}` substring is treated as a plain
        // prefix (historical `/bot{token}/{method}` shape), so the literal
        // `{Token}` is preserved verbatim rather than substituted.
        let url = resolve_api_url(
            "https://api.telegram.org",
            Some("http://host/bot{Token}/x"),
            "t0ken",
            "sendMessage",
        );
        assert_eq!(url, "http://host/bot{Token}/x/bott0ken/sendMessage");
    }

    #[test]
    fn resolve_multiple_token_occurrences() {
        // U-6: multiple {token} occurrences all replaced
        let url = resolve_api_url(
            "https://api.telegram.org",
            Some("http://{token}.host/bot{token}/x"),
            "t0ken",
            "y",
        );
        assert_eq!(url, "http://t0ken.host/bott0ken/x/y");
    }

    #[test]
    fn resolve_no_unreachable_fallback() {
        // U-7: a present-but-down base NEVER auto-falls back to global
        // Fallback scope is "absent/null/empty" only — never "unreachable"
        let url = resolve_api_url(
            "https://api.telegram.org",
            Some("http://127.0.0.1:9123"),
            "t0ken",
            "sendMessage",
        );
        // Returns the per-bot URL even though the host would not respond
        assert_eq!(url, "http://127.0.0.1:9123/bott0ken/sendMessage");
    }
}
