-- Per-page delivery state for paged completion webhooks (US-004).
ALTER TABLE webhook_pages ADD COLUMN body BLOB;
ALTER TABLE webhook_pages ADD COLUMN body_sha256 BLOB;
ALTER TABLE webhook_pages ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE webhook_pages ADD COLUMN next_attempt_unix INTEGER;
ALTER TABLE webhook_pages ADD COLUMN last_http_status INTEGER;
ALTER TABLE webhook_pages ADD COLUMN last_error TEXT;
CREATE INDEX webhook_pages_ready ON webhook_pages(event_id, page_index)
  WHERE delivered = 0;
