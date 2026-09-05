-- Per-bot Telegram Bot API endpoint override: nullable string, http(s)://
-- prefix or a {token} template. NULL/absent -> global TELEGRAM_API_BASE
-- fallback at dispatch/authenticate. STRICT ADD COLUMN appends at end; no
-- default, existing rows read NULL (backward compatible).
ALTER TABLE bot_configs ADD COLUMN telegram_api_base TEXT;
