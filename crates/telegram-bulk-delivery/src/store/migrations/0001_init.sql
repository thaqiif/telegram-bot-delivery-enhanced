CREATE TABLE _migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at_unix INTEGER NOT NULL
) STRICT;

CREATE TABLE bots (
  bot_id BLOB PRIMARY KEY CHECK(length(bot_id) = 32),
  token_nonce BLOB NOT NULL CHECK(length(token_nonce) = 12),
  token_ciphertext BLOB NOT NULL,
  token_kid TEXT NOT NULL,
  telegram_user_id INTEGER,
  created_at_unix INTEGER NOT NULL,
  last_seen_unix INTEGER NOT NULL
) STRICT;
CREATE TABLE bot_configs (
  bot_id BLOB PRIMARY KEY REFERENCES bots(bot_id), config_version INTEGER NOT NULL,
  target_msgs_per_sec REAL NOT NULL, retry_max_attempts INTEGER NOT NULL,
  retry_base_ms INTEGER NOT NULL, retry_max_ms INTEGER NOT NULL, retry_jitter TEXT NOT NULL,
  retryable_classes_json TEXT NOT NULL, ambiguity_policy TEXT NOT NULL,
  job_deadline_secs INTEGER NOT NULL, fairness_weight INTEGER NOT NULL,
  webhook_url TEXT, webhook_https_only INTEGER NOT NULL,
  webhook_secret_nonce BLOB, webhook_secret_ciphertext BLOB, webhook_secret_kid TEXT,
  webhook_max_attempts INTEGER NOT NULL, webhook_retry_base_ms INTEGER NOT NULL,
  webhook_retry_max_ms INTEGER NOT NULL, updated_at_unix INTEGER NOT NULL
) STRICT;
CREATE TABLE jobs (
  job_id TEXT PRIMARY KEY, bot_id BLOB NOT NULL REFERENCES bots(bot_id), method TEXT NOT NULL,
  method_canonical TEXT NOT NULL, shared_params_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('accepting','queued','running','completed','failed_internal')),
  total INTEGER NOT NULL, queued INTEGER NOT NULL, inflight INTEGER NOT NULL,
  delayed INTEGER NOT NULL, succeeded INTEGER NOT NULL, failed INTEGER NOT NULL,
  ambiguous INTEGER NOT NULL, policy_snapshot_json TEXT NOT NULL, config_version INTEGER NOT NULL,
  accepted_at_unix INTEGER, started_at_unix INTEGER, completed_at_unix INTEGER,
  expire_at_unix INTEGER, deadline_unix INTEGER, webhook_event_id TEXT,
  webhook_state TEXT NOT NULL, error_summary TEXT
) STRICT;
CREATE INDEX jobs_bot_state ON jobs(bot_id,state);
CREATE INDEX jobs_expire ON jobs(expire_at_unix) WHERE expire_at_unix IS NOT NULL;
CREATE INDEX jobs_active_bot ON jobs(bot_id) WHERE state IN ('queued','running');
CREATE INDEX jobs_accepting ON jobs(state) WHERE state='accepting';
CREATE TABLE job_files (
  file_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  field_name TEXT NOT NULL, attach_name TEXT, content_type TEXT, original_filename TEXT,
  sha256 BLOB NOT NULL, size_bytes INTEGER NOT NULL, blob_path TEXT NOT NULL,
  retain_until_unix INTEGER NOT NULL
) STRICT;
CREATE INDEX job_files_job ON job_files(job_id);
CREATE TABLE recipients (
  job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE, idx INTEGER NOT NULL,
  bot_id BLOB NOT NULL CHECK(length(bot_id)=32), patch_json TEXT NOT NULL,
  chat_id TEXT, chat_kind TEXT, state TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0, retry_consumed INTEGER NOT NULL DEFAULT 0,
  wire_started INTEGER NOT NULL DEFAULT 0 CHECK(wire_started IN (0,1)),
  lease_owner TEXT, lease_until_unix INTEGER, not_before_unix INTEGER NOT NULL DEFAULT 0,
  limiter_scope_hint TEXT, terminal_class TEXT, telegram_message_id INTEGER,
  time_send_unix INTEGER, error_code INTEGER, error_norm TEXT, last_http_status INTEGER,
  last_error_desc TEXT, PRIMARY KEY(job_id,idx)
) STRICT;
CREATE INDEX recipients_ready ON recipients(bot_id,not_before_unix,job_id,idx) WHERE state IN ('queued','delayed');
CREATE INDEX recipients_lease ON recipients(lease_until_unix) WHERE state='leased';
CREATE INDEX recipients_job_state ON recipients(job_id,state);
CREATE INDEX recipients_success ON recipients(job_id,idx) WHERE state='succeeded' AND telegram_message_id IS NOT NULL;
CREATE TABLE limiter_pauses (
  scope_key TEXT PRIMARY KEY, paused_until_unix INTEGER NOT NULL,
  last_retry_after INTEGER, updated_at_unix INTEGER NOT NULL
) STRICT;
CREATE TABLE webhook_events (
  event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
  bot_id BLOB NOT NULL, state TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_unix INTEGER NOT NULL, lease_until_unix INTEGER, page_count INTEGER NOT NULL,
  body_sha256 BLOB, last_error TEXT, delivered_at_unix INTEGER, created_at_unix INTEGER NOT NULL
) STRICT;
CREATE INDEX webhook_ready ON webhook_events(next_attempt_unix) WHERE state IN ('pending','delivering');
CREATE TABLE webhook_pages (
  event_id TEXT NOT NULL REFERENCES webhook_events(event_id) ON DELETE CASCADE,
  page_index INTEGER NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(event_id,page_index)
) STRICT;
CREATE TABLE webhook_deliveries (
  event_id TEXT NOT NULL REFERENCES webhook_events(event_id) ON DELETE CASCADE,
  page_index INTEGER NOT NULL, attempt INTEGER NOT NULL, at_unix INTEGER NOT NULL,
  http_status INTEGER, error TEXT, PRIMARY KEY(event_id,page_index,attempt)
) STRICT;
CREATE TABLE fairness_deficits (
  bot_id BLOB PRIMARY KEY REFERENCES bots(bot_id), deficit INTEGER NOT NULL,
  weight INTEGER NOT NULL, last_grant_unix INTEGER NOT NULL
) STRICT;
CREATE TABLE rate_samples (
  job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  window_start_unix INTEGER NOT NULL, sent INTEGER NOT NULL,
  PRIMARY KEY(job_id,window_start_unix)
) STRICT;
