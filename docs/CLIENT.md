# Client contract

How a **consumer application** talks to an already-running `telegram-bulk-delivery` process. This document is not about deploying or operating the service.

Agents: the user-level skill `telegram-bulk-delivery` (`~/.claude/skills/telegram-bulk-delivery/SKILL.md`) is the short form of this file. Keep them in sync.

## What this service is

An HTTP fan-out front for the Telegram Bot API. The client submits **one** bulk job (shared method parameters + per-recipient patches). The service persists it, delivers asynchronously under Telegram's rate limits, and exposes progress/results polling (optional signed completion webhooks).

It is **not** a transparent proxy. Same `/bot<TOKEN>/<method>` path shape as Telegram; different JSON body; HTTP 200 means the job is durable, not that Telegram has delivered.

## Endpoints

Unauthenticated:

- `GET /healthz` — process liveness
- `GET /readyz` — durable-acceptance readiness
- `GET /metrics` — Prometheus; protect at the proxy

Gated by `BULK_API_KEY` when that env is set (`Authorization: Bearer <key>` or `X-TGBulk-Key: <key>`):

- `POST /bot<TOKEN>/<method>` — submit one bulk job
- `GET /bot<TOKEN>/bulk/jobs/<JOB_ID>` — status / progress / ETA
- `GET /bot<TOKEN>/bulk/jobs/<JOB_ID>/results?limit=100` — stable paginated results (`next_cursor`)
- `GET|POST /bot<TOKEN>/{get,set,reset}BulkDeliveryConfig` — per-bot policy

Never log token-bearing paths.

## Submit envelope

```json
{
  "parameters": { "text": "Hello from bulk delivery!", "parse_mode": "HTML" },
  "recipients": [
    { "chat_id": -1008001228039 },
    { "chat_id": 1008001967399, "text": "Personal override" }
  ]
}
```

- `parameters` — shared Telegram fields for `{method}`. Do not put `chat_id` here.
- `recipients` — non-empty array. Each object is shallow-merged over `parameters` (recipient wins) and must include `chat_id`. Order and duplicates are preserved; no dedup.
- `{method}` — any committed outbound Telegram method. JSON by default; multipart for media.
- Default hard cap: 100_000 recipients per job.
- A user chat ID only works if that user has started the bot (Telegram rule).

Success (HTTP 200 **after** durable accept):

```json
{
  "ok": true,
  "result": {
    "job_id": "01a06cbb-…",
    "state": "queued",
    "total": 3,
    "status_url": "/bot<token>/bulk/jobs/01a06cbb-…",
    "accepted_at_unix": 1788530612
  }
}
```

`result` is not a Telegram `Message`. There is no `message_id` at submit time. There is no submit-idempotency key: retrying a timed-out POST can create a second fan-out. Prefer one POST, then poll.

## Poll and results

`GET /bot<TOKEN>/bulk/jobs/<JOB_ID>` returns exact counters (`queued` / `in_flight` / `delayed` / `succeeded` / `failed` / `ambiguous`), percent complete, observed rates, ETA, webhook state.

`GET .../results` returns rows in stable recipient order with `telegram_message_id` and normalized error classes. Page with `next_cursor`.

If the consumer must persist Telegram `message_id`s, wait until the job is terminal, then page results. Back off between polls.

## Per-bot policy

`setBulkDeliveryConfig` fields that clients actually touch:

| Field | Notes |
| --- | --- |
| `target_msgs_per_sec` | Operator target; Telegram per-chat / group flood still bind |
| `ambiguity_policy` | `at_least_once` (default, may duplicate on ambiguous ack), `at_most_once` (may drop), `method_aware` |
| `job_deadline_secs` | |
| `completion_webhook_url` | HTTPS to public hosts only by default; HMAC-signed pages, at-least-once; persist idempotency key before side effects. **The first call that sets it returns a server-generated `webhook_secret`, once** (see below) |
| `telegram_api_base` | Per-bot upstream override. Private/loopback hosts 400 unless the operator set `allow_private_targets = true` |

Snapshot semantics: `telegram_api_base` is snapshotted per job. Changing it does not rewrite already-queued recipients.

### Verifying completion webhooks

- The service **generates** the secret. A `webhook_secret` sent in the request is ignored.
- The **first** `setBulkDeliveryConfig` that sets `completion_webhook_url` returns `webhook_secret`
  in its result: base64url (no padding) of 32 random bytes. It is never returned again, so store it.
- Each page is POSTed with `X-Bulk-Signature: v1=<hex>`, where `<hex>` is
  **HMAC-SHA256 keyed with the 32 raw bytes** (base64url-decode the secret first; the string itself
  is not the key) over the **exact raw body bytes**.
- Also sent: `X-Bulk-Event-Id` (the idempotency key; persist it before side effects),
  `X-Bulk-Job-Id`, `X-Bulk-Page`, `X-Bulk-Page-Count`, `X-Bulk-Attempt`.
- The TypeScript client (`clients/typescript`) implements this as `verifyWebhookSignature()`.

Operator options (config TOML) for a receiver on a trusted local network, which is otherwise
refused by the SSRF policy:

```toml
webhook_trusted_hosts = ["10.0.0.5"]   # exempt from private-address checks
webhook_allow_insecure_http = true     # http allowed ONLY to the trusted hosts
```

## TypeScript client

`clients/typescript` (`@tgbulk/client`) is zero-dependency and runs in Workers, Bun and Node:
`submit` (never auto-retried; a network failure raises `SubmitUnknownError`), `job`,
`waitForTerminal` (with backoff), `results` (async iterator over cursor pages), `ready`,
`getConfig`/`setConfig`, and `verifyWebhookSignature`. Its contract test
(`bun test` in that folder) runs against the real binary and `scripts/faketg`.

## Semantics

- Throughput is not 30/s. Telegram per-chat (~1/s) and group (~20/min) flood, plus the operator target (default 20/s, hard max 25/s), bind real delivery. Do not add a second client-side rate limiter on top of a bulk submit.
- Flood responses pause the relevant scopes durably and do not consume retry budget.
- Empty `recipients` is 400.
- Dual-sending the same fan-out to both this API and `api.telegram.org` duplicates the blast. Only fall back to Telegram directly if `/readyz` is not ready.

## Consumer integration sketch (TypeScript)

```ts
const res = await fetch(`${TGBULK_BASE}/bot${token}/sendMessage`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Authorization: `Bearer ${BULK_API_KEY}`,
  },
  body: JSON.stringify({
    parameters: { text: sharedText, parse_mode: "HTML" },
    recipients: chats.map((c) => ({ chat_id: c.id, text: c.textOverride })),
  }),
});
const { result } = await res.json(); // { job_id, state, total, status_url }
// poll GET `${TGBULK_BASE}${result.status_url}` until terminal
// then page GET `.../results?limit=100`
```

Keep the project's own Telegram client for 1:1 replies, edits, deletes, and inbound webhook handling.

## Curl

```sh
curl -s -X POST "$TGBULK_BASE/bot${TOKEN}/sendMessage" \
  -H "Authorization: Bearer ${BULK_API_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"parameters":{"text":"hi","parse_mode":"HTML"},"recipients":[{"chat_id":123456}]}'
```
