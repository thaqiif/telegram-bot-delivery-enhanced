# Telegram Bulk Delivery

A production-oriented, API-only Rust service for bulk outbound Telegram Bot API delivery. A client submits **one bulk request** with shared method parameters plus per-recipient patches; the service persists it to local SQLite, delivers it asynchronously under Telegram rate limits, exposes progress/results polling, and optionally sends signed completion webhooks.

## Quick start

Requirements: Rust 1.89+, local persistent storage, and SQLite 3.51.3+ (the bundled `rusqlite` build is pinned and checked at compile time and boot).

```sh
cp config/operator.defaults.toml config/operator.toml
export BULK_MASTER_KEY="$(openssl rand -base64 32)"
cargo run -p telegram-bulk-delivery -- config/operator.toml
```

Default listeners/endpoints:

- `GET /healthz` — process liveness
- `GET /readyz` — durable-acceptance readiness
- `GET /metrics` — aggregate Prometheus text exposition; bind locally or protect at the proxy
- `POST /bot<TOKEN>/<outbound-method>` — submit one bulk job
- `GET /bot<TOKEN>/bulk/jobs/<JOB_ID>` — status/progress/rate/ETA
- `GET /bot<TOKEN>/bulk/jobs/<JOB_ID>/results` — stable paginated results
- `GET|POST /bot<TOKEN>/{get,set,reset}BulkDeliveryConfig` — per-bot policy

`<TOKEN>` is a Telegram bot token and is therefore a secret. The service never logs it; the reverse proxy must also strip token-bearing paths from access logs. See [`docs/SECURITY.md`](docs/SECURITY.md).

## Submission contract

For any committed outbound method, submit an order-independent JSON envelope:

```json
{
  "parameters": {
    "text": "Shared text",
    "disable_notification": true
  },
  "recipients": [
    {"chat_id": 10001},
    {"chat_id": 10002, "text": "Recipient override"}
  ]
}
```

- `parameters` contains official Telegram fields shared by every recipient.
- Each `recipients` object is shallow-merged over `parameters`; recipient values win.
- Recipient order and duplicates are preserved. No recipient deduplication is performed.
- Envelope keys may appear in either order but may not be duplicated.
- Maximum recipients/job, JSON body, multipart body, shared parameters, and recipient patch sizes are hard operator ceilings in `config/operator.toml`.
- Multipart uses a required `payload_json` part plus file parts. Files are persisted by temp write → `fdatasync` → directory `fsync` → atomic rename → destination directory `fsync` before the job is promoted.

A successful submit returns HTTP 200 only after the job is durable:

```json
{"ok":true,"result":{"job_id":"...","state":"queued","total":2,"accepted_at_unix":...}}
```

## Polling contract

`GET /bot<TOKEN>/bulk/jobs/<JOB_ID>` returns exact counters (`queued`, `in_flight`, `delayed`, `succeeded`, `failed`, `ambiguous`), `percent_complete`, observed 5-second and 30-second rates, nullable ETA with a reason, retention expiry, and webhook state. The bot token and job ID are jointly scoped: a different bot receives 404, not information about another bot's job.

`GET .../results?cursor=<idx>&limit=<1..1000>` returns rows in stable recipient-index order and a `next_cursor`. Terminal results retain `chat_id`, attempt count, Telegram message ID/send timestamp when available, and normalized error class.

## Delivery semantics

The scheduler is weighted deficit round-robin across ready bots. Every dispatch is gated by the hierarchical limiters (operator/global, per-bot, per-chat, group, method, and media scopes) before any bytes reach Telegram: a recipient that cannot acquire every scope is returned immediately without touching the wire. Telegram flood responses dynamically pause relevant scopes and do **not** consume retry budget.

The service does **not** promise 30 messages/second. Telegram's free aggregate ceiling is roughly 30/s, but per-chat (~1/s), group (~20/min), media/method limits, flood adaptation, bot weights, disk latency, and the operator target can make actual throughput lower. The default target is deliberately 20/s and the hard configured maximum is 25/s.

On transport failure after bytes may have crossed the wire, Telegram acknowledgement is ambiguous. The default `at_least_once` policy may retry and therefore may duplicate a message. `at_most_once` avoids that retry at the cost of possible message loss; `method_aware` chooses by method metadata. There is no submit-idempotency key in v1, so a client retry of a timed-out bulk submit can create another fan-out.

## Completion webhooks

When configured, terminalization atomically creates one logical event split into deterministic pages no larger than 256 KiB. Every page body is one JSON envelope carrying per-page metadata plus a `results` array of outcome tuples in recipient order:

```json
{"job_id":"j_abc","event_id":"ev_1","state":"completed","total":2,"succeeded":2,"failed":0,"ambiguous":0,"page":0,"page_count":1,"results":[["10001",321,1770000000],["10002",322,1770000002]]}
```

Each tuple is a JSON array `[chat_id, telegram_message_id, time_send_unix]`: `chat_id` is a string and `telegram_message_id` is an integer when Telegram acknowledged the send — either may be `null` for a recipient without an acknowledged Telegram result.

Each physical POST includes exact-body HMAC-SHA256 signing plus event/job/page/attempt/timestamp/idempotency headers. Pages are delivered in order with bounded full-jitter retries. Delivery is **at least once**: if the receiver accepts a page but the acknowledgement is lost, that page can be repeated. Receivers must persist/dedupe the idempotency key before side effects. `delivered` and `exhausted` are visible in job polling.

Webhook URLs are HTTPS-only by default, redirect following is disabled, and both hostname and every resolved IP are checked against the SSRF policy. See [`docs/SECURITY.md`](docs/SECURITY.md).

## Operations and compatibility

SQLite and all durable files must remain on the same local persistent disk. The durability boundary is that disk: this is not replicated storage. See [`docs/OPERATIONS.md`](docs/OPERATIONS.md) for deploy sizing, safe backup, WAL/checkpoint/retention behavior, cgroup scripts, and kill/reboot drills.

Bot API method metadata is committed in `crates/telegram-api-meta`. See [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) for the update and review workflow.
