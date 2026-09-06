# Telegram Bulk Delivery

A production-oriented, API-only Rust service for bulk outbound Telegram Bot API delivery. A client submits **one bulk request** with shared method parameters plus per-recipient patches; the service persists it to local SQLite, delivers it asynchronously under Telegram rate limits, exposes progress/results polling, and optionally sends signed completion webhooks.

One binary. One config file. One env var. SQLite and TLS roots are compiled in — no runtime packages beyond a base Debian system.

---

## Deploy

The release artifact is a glibc-linked binary built for your architecture. It needs nothing but base Debian 13 (no `libsqlite3`, no `ca-certificates`, no OpenSSL).

### Quickstart (simplest path)

On a Debian 13 server (x86_64 or arm64), ~1 minute:

```sh
curl -fsSL https://raw.githubusercontent.com/thaqiif/telegram-bot-delivery-enhanced/main/deploy/install.sh | sudo bash
curl -s http://127.0.0.1:8080/healthz   # -> ok
```

Non-root without sudo: the plain `| bash` one-liner re-runs itself via sudo
automatically. The installer fetches the **latest release** for your
architecture, verifies its sha256, installs the binary + a hardened systemd
unit, and generates the master key. Pin an exact version with:

```sh
TGBULK_VERSION=v0.2.3 curl -fsSL https://raw.githubusercontent.com/thaqiif/telegram-bot-delivery-enhanced/main/deploy/install.sh | sudo bash
```

No sudo handy? Prefer GitHub's CLI (also works for any authenticated user,
e.g. if the repo is ever set back to private):

```sh
gh release download -R thaqiif/telegram-bot-delivery-enhanced \
  -p "telegram-bulk-delivery-*-linux-$(uname -m).tar.gz" -O /tmp/tgbulk.tar.gz
tar -xzf /tmp/tgbulk.tar.gz && cd telegram-bulk-delivery-*-linux-*/
sudo ./install.sh && sudo systemctl start telegram-bulk-delivery
curl -s http://127.0.0.1:8080/healthz   # -> ok   (localhost OK once install.sh runs)
```

`install.sh` is idempotent and does everything — installs the binary, creates a
dedicated `tgbulk` user, generates the master key, writes config + env, and
installs a hardened systemd unit (enabled at boot, auto-restart). Re-running it
upgrades the binary and restarts the service only when the binary changed;
existing config, master key, and env file are never overwritten.

**Only two settings to set up** (edit `/etc/telegram-bulk-delivery/env`, then `sudo systemctl restart telegram-bulk-delivery`):

```ini
BULK_MASTER_KEY=<install.sh already filled this in — back it up>
BULK_API_KEY=your-long-random-key   # OPTIONAL but recommended: gates every /bot endpoint
```

Optional toggle in `/etc/telegram-bulk-delivery/config.toml` (defaults are already safe):
`allow_private_targets = false` — leave it `false` unless a per-bot `telegram_api_base`
must point at a **private/local** bot server (e.g. a local mock for testing).

That's it. A call once the service is up (note the `BULK_API_KEY` header):

```sh
curl -s -X POST "http://127.0.0.1:8080/bot<TOKEN>/sendMessage" \
  -H "Authorization: Bearer <BULK_API_KEY>" -H 'Content-Type: application/json' \
  -d '{"parameters":{"text":"hi"},"recipients":[{"chat_id":123456}]}'
```

For a **public** deployment also put a TLS reverse proxy in front (see [Expose it](#expose-it) below) —
bot tokens travel in the URL path.

### Download a release

The repo is private, so download with the GitHub CLI (`gh auth login` once):

```sh
gh release download -R thaqiif/telegram-bot-delivery-enhanced \
  -p 'telegram-bulk-delivery-*-linux-*.tar.gz' -O /tmp/tgbulk.tar.gz
```

Or from a machine with `curl` + a token — then transfer to the server.

### Install

```sh
tar -xzf /tmp/tgbulk.tar.gz
cd telegram-bulk-delivery-*-linux-*/
sudo ./install.sh
sudo systemctl start telegram-bulk-delivery
curl -s http://127.0.0.1:8080/healthz   # -> ok
curl -s http://127.0.0.1:8080/readyz   # -> ready
```

`install.sh` is idempotent (re-running never overwrites config or keys) and sets up:

| Path | Purpose |
|------|---------|
| `/usr/local/bin/telegram-bulk-delivery` | the binary |
| `/etc/telegram-bulk-delivery/config.toml` | operator config |
| `/etc/telegram-bulk-delivery/env` | `BULK_MASTER_KEY` (+ optional `TELEGRAM_API_BASE`, `BULK_API_KEY`) |
| `/etc/telegram-bulk-delivery/master.key` | **back this up** — encrypts bot tokens at rest; losing it makes all stored tokens unrecoverable |
| `/var/lib/telegram-bulk-delivery/` | SQLite DB + multipart file blobs |
| `systemd` unit `telegram-bulk-delivery.service` | hardened service, enabled at boot, auto-restart |

Logs: `journalctl -u telegram-bulk-delivery -f`

### Expose it

The service binds `0.0.0.0:8080` by default. Put a TLS proxy in front for public exposure:

```nginx
# /etc/nginx/sites-available/tgbulk
server {
    listen 443 ssl;
    server_name bulk.example.com;
    ssl_certificate     /etc/letsencrypt/live/bulk.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/bulk.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_read_timeout 300s;   # large streaming submits
        client_max_body_size 64m;
    }
}
```

Bot tokens travel in the URL path (`/bot<TOKEN>/...`): keep the proxy private, use TLS, and strip token-bearing paths from access logs. See [`docs/SECURITY.md`](docs/SECURITY.md).

### Using a Telegram test-environment bot token

Test tokens only work on `https://api.telegram.org/bot<token>/test/<method>`. The service always calls `<base>/bot<token>/<method>`, so add a local rewrite proxy:

```nginx
# /etc/nginx/sites-available/tgtest  (listens locally only)
server {
    listen 127.0.0.1:8443;
    location ~ ^/bot[^/]+/[A-Za-z]+$ {
        rewrite ^(/bot[^/]+)/([A-Za-z]+)$ $1/test/$2 break;
        proxy_pass https://api.telegram.org;
        proxy_ssl_server_name on;
        proxy_set_header Host api.telegram.org;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }
}
```

Then uncomment `TELEGRAM_API_BASE=http://127.0.0.1:8443` in `/etc/telegram-bulk-delivery/env` and restart. For a **production** bot token, leave the default — the service talks straight to `https://api.telegram.org`.

> A test-environment token can also be used **without** a rewrite proxy — point the per-bot `telegram_api_base` at
> `https://api.telegram.org/bot{token}/test` (see [Per-bot API base](#per-bot-api-base-telegram-test-env--local-bot-servers)).

---

## Using the API

### Submit one bulk job

```sh
TOKEN="123456:ABC-your-bot-token"
curl -s -X POST "http://127.0.0.1:8080/bot${TOKEN}/sendMessage" \
  -H 'Content-Type: application/json' \
  -d '{
    "parameters": {"text": "Hello from bulk delivery!"},
    "recipients": [
      {"chat_id": -1008001228039},
      {"chat_id": -1008003100137},
      {"chat_id": 1008001967399, "text": "Personal override"}
    ]
  }'
```

`parameters` holds shared Telegram fields; each recipient object is shallow-merged over it (recipient wins). Order and duplicates are preserved — no dedup. A user chat ID only works if that user has started your bot (Telegram rule, not a service limitation).

Response (HTTP 200 only after the job is durable):

```json
{"ok":true,"result":{"job_id":"01a06cbb-…","state":"queued","total":3,
 "status_url":"/bot<token>/bulk/jobs/01a06cbb-…","accepted_at_unix":1788530612}}
```

### Poll progress

```sh
curl -s "http://127.0.0.1:8080/bot${TOKEN}/bulk/jobs/<JOB_ID>"
```

Returns exact counters (`queued/in_flight/delayed/succeeded/failed/ambiguous`), percent complete, observed rates, ETA, and webhook state.

### Read results

```sh
curl -s "http://127.0.0.1:8080/bot${TOKEN}/bulk/jobs/<JOB_ID>/results?limit=100"
```

Rows in stable recipient order with `telegram_message_id` and normalized error classes; `next_cursor` paginates.

### Per-bot policy

```sh
curl -s -X POST "http://127.0.0.1:8080/bot${TOKEN}/setBulkDeliveryConfig" \
  -H 'Content-Type: application/json' \
  -d '{"target_msgs_per_sec":5,"ambiguity_policy":"at_least_once",
       "retryable_classes":["transient","flood"],"job_deadline_secs":3600}'
curl -s "http://127.0.0.1:8080/bot${TOKEN}/getBulkDeliveryConfig"
curl -s -X POST "http://127.0.0.1:8080/bot${TOKEN}/resetBulkDeliveryConfig"
```

### Per-bot API base (Telegram test env / local bot servers)

A bot can override the upstream endpoint instead of using the process default
(`TELEGRAM_API_BASE`, by default `https://api.telegram.org`). Set
`telegram_api_base` in its config:

```sh
curl -s -X POST "http://127.0.0.1:8080/bot${TOKEN}/setBulkDeliveryConfig" \
  -H 'Content-Type: application/json' \
  -d '{"telegram_api_base":"http://127.0.0.1:9123"}'
```

Two layouts are supported by the URL builder; both accept any `http://` or
`https://` host (plain prefix or a `{token}` template). Non-`http(s)` values are
rejected and stored as absent (the bot then uses the global base):

* **Plain prefix** — `http://127.0.0.1:9123` resolves to
  `http://127.0.0.1:9123/bot<TOKEN>/<method>`.
* **Template** — `https://api.telegram.org/bot{token}/test` resolves to
  `https://api.telegram.org/bot<TOKEN>/test/<method>` (every `{token}`
  occurrence is substituted). This is the shape Telegram's test environment
  expects, so a test-environment token works with **no local rewrite proxy**:
  point the bot at `https://api.telegram.org/bot{token}/test`.

The base is validated by a default-safe SSRF gate **at set time**: unless the operator enables `allow_private_targets`, a base whose host is a private/loopback/link-local/metadata address (or that fails to resolve publicly) is rejected with 400 instead of being stored. Production configs leave `allow_private_targets` false.

Snapshot semantics: `telegram_api_base` is snapshotted per job — changing it
applies only to jobs submitted after the change. Already-queued recipients keep
the old base until they are re-submitted. An absent/empty/NULL base falls back
to the global host; a *set but unreachable* base is not silently replaced
(it fails through the normal retry path).

> A brand-new token claimed with an `http(s)://` `telegram_api_base` registers
> without a `getMe` round-trip (see [`docs/SECURITY.md`](docs/SECURITY.md) for
> the token-only trust boundary). For a token without a base, the service still
> performs a `getMe` against the configured base to authenticate the token.

### Endpoints

- `GET /healthz` — process liveness
- `GET /readyz` — durable-acceptance readiness
- `GET /metrics` — Prometheus exposition; bound aggregate labels only, but protect at the proxy (operationally sensitive)
- `POST /bot<TOKEN>/<method>` — submit one bulk job (any committed outbound method; multipart for media)
- `GET /bot<TOKEN>/bulk/jobs/<JOB_ID>` — status/progress/ETA
- `GET /bot<TOKEN>/bulk/jobs/<JOB_ID>/results` — stable paginated results
- `GET|POST /bot<TOKEN>/{get,set,reset}BulkDeliveryConfig` — per-bot policy

### Smoke test

`scripts/smoke.sh` exercises a live deployment end-to-end: the API-key gate
(no key 401 / Bearer + `X-TGBulk-Key` 200 / bad key 401, with `/healthz`
`/readyz` `/metrics` open), a public per-bot base claim, the default-safe SSRF
gate (a loopback base is rejected with 400), then a real bulk submit, poll to
completion, and result read:

```sh
BASE=http://HOST:8080 TOKEN=<bot-token> API_KEY=<key> \
  ./scripts/smoke.sh -c -1008001228039 -c -1008003100137
```

Add `--no-send` to run only the gate/SSRF checks, or `-b <base>` to override
the per-bot API base.

---

## Configuration reference

`config/operator.defaults.toml` documents every knob; the important ones:

| Key | Default | Meaning |
|-----|---------|---------|
| `database_path` | `data/…db` | SQLite database (keep on local persistent disk) |
| `bind` | `0.0.0.0:8080` | HTTP listen address |
| `max_recipients_per_job` | `100000` | hard per-job ceiling |
| `global_nonterminal_recipients` | `500000` | global in-flight ceiling across all jobs |
| `request_body_bytes` / `multipart_body_bytes` | 32 MiB / 64 MiB | submit body caps |
| `free_disk_reserve_bytes` | 1 GiB | submits are refused below this |
| `wal_truncate_bytes` | 64 MiB | forced WAL truncate threshold |
| `retention_sweep_secs` / `retention_batch` | 30 / 500 | terminal-job GC cadence |
| `allow_private_targets` | `false` | when true, a per-bot API base may point at a private/loopback host (local/testing bot servers); leave false in production |

`BULK_MASTER_KEY` (env, required) is base64 of exactly 32 bytes; it derives per-bot AEAD keys that encrypt tokens at rest. Rotate by re-registering bots.

`BULK_API_KEY` (env, optional) is a shared key that, when set, gates **every** `/bot<TOKEN>/...` endpoint: a caller must present `Authorization: Bearer <key>` or `X-TGBulk-Key: <key>`. `/healthz`, `/readyz`, and `/metrics` stay unauthenticated. Base64-encode the key used at the proxy, e.g. `export BULK_API_KEY="$(openssl rand -base64 32)"`.

---

## Delivery semantics

The scheduler is weighted deficit round-robin across ready bots. Every dispatch is gated by hierarchical limiters (global, per-bot, per-chat, group, method, media) before any bytes reach Telegram: a recipient that cannot acquire every scope is returned without touching the wire. Telegram flood responses pause relevant scopes durably and do **not** consume retry budget.

The service does **not** promise 30 messages/second. Telegram's free aggregate ceiling is ~30/s, but per-chat (~1/s), group (~20/min), flood adaptation, disk latency, and the operator target can make actual throughput lower. The default target is deliberately 20/s with a hard maximum of 25/s.

On transport failure after bytes may have crossed the wire, acknowledgement is ambiguous. The default `at_least_once` policy may retry and therefore may duplicate a message; `at_most_once` avoids that retry at the cost of possible message loss; `method_aware` chooses by method metadata. There is no submit-idempotency key in v1, so a client retry of a timed-out bulk submit can create another fan-out.

## Completion webhooks

When configured, terminalization atomically creates one logical event split into deterministic pages ≤256 KiB. Each physical POST carries exact-body HMAC-SHA256 signing plus event/job/page/attempt/idempotency headers. Pages are delivered in order with bounded full-jitter retries, **at least once** — receivers must persist the idempotency key before side effects. Webhook URLs are HTTPS-only by default, redirects are disabled, and hostname plus every resolved IP are checked against the SSRF policy ([`docs/SECURITY.md`](docs/SECURITY.md)).

```json
{"job_id":"j_abc","event_id":"ev_1","state":"completed","total":2,"succeeded":2,
 "page":0,"page_count":1,
 "results":[["10001",321,1770000000],["10002",322,1770000002]]}
```

---

## Build from source

Requirements: Rust 1.89+ (rustup), a C toolchain (`build-essential`), ~1 GiB free RAM.

```sh
git clone https://github.com/thaqiif/telegram-bot-delivery-enhanced.git
cd telegram-bot-delivery-enhanced
export BULK_MASTER_KEY="$(openssl rand -base64 32)"   # encrypts bot tokens at rest
cargo run -p telegram-bulk-delivery -- config/operator.defaults.toml
```

> Multipart jobs (photos, documents, …): submit multipart with a required
> `payload_json` part (the same envelope as above) plus file parts. Files are
> persisted by temp write → `fdatasync` → dir `fsync` → atomic rename → dir
> `fsync` **before** the job is promoted, so a crash mid-upload never
> half-delivers.

On small (1 CPU / 1 GiB) machines cap the build: `CARGO_BUILD_JOBS=1 cargo build --release` and run tests with `-- --test-threads=1`.

---

## Releases & CI

Release artifacts are **built and published entirely by CI on a GitHub-hosted
runner** — no self-hosted runner needed. The [`release`](.github/workflows/release.yml)
workflow builds **both** `x86_64` (natively) and `aarch64` (cross-compiled via
`gcc-aarch64-linux-gnu`) on a single `ubuntu-24.04` runner, runs the capped test
suite, and publishes a GitHub Release.

### Cut a release

**Always cut releases from `main`** — a `v*` tag must point at the tip of the
merged `main` branch (or a commit reachable from it), never at a feature
branch. Quickest: `git checkout main && git pull --ff-only`, then:

```sh
git tag -a v0.2.2 -m "v0.2.2"
git push origin v0.2.2
```

The workflow packages `telegram-bulk-delivery-vX.Y.Z-linux-{x86_64,aarch64}.tar.gz`
(binary + example config + systemd unit + `install.sh`) with a `.sha256` each,
and attaches all four as release assets. The cross-built binary links against
Ubuntu 24.04's glibc (2.39), which runs on the Debian 13 deployment target
(glibc 2.41) — glibc is backward compatible, so one CI build serves both.

---

## Security & operations notes

- **Back up the master key and the data directory together.** The durability boundary is one local disk; this is not replicated storage ([`docs/OPERATIONS.md`](docs/OPERATIONS.md) covers backup, WAL/checkpoint/retention, kill/reboot drills).
- Bot tokens are secrets: TLS at the proxy, never log token-bearing paths.
- SQLite, the DB, and file blobs must stay on the same filesystem.
- Bot API method metadata is committed in `crates/telegram-api-meta`; see [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) for the review/update workflow.

---

## Upgrade note: telegram_api_base migration

This release adds the `telegram_api_base` column via migration `0004`. The
`bot_configs` table is `STRICT` with an explicit 21-column `INSERT`; the prior
release wrote a 20-value implicit-column insert, so **downgrade is backup-restore
only** — restoring a pre-migration snapshot into the new release works, but
rolling the new DB backward into the previous release's binary fails on the
column count. Snapshot semantics: switching a bot's base affects only jobs
submitted after the change (each job snapshots the config at accept time).
