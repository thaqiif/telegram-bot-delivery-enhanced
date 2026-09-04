# Security

## Credential and tenant isolation

Bot tokens and webhook secrets are encrypted at rest with AES-256-GCM. Keys are derived from `BULK_MASTER_KEY` with HKDF-SHA256 and purpose-bound AAD, so bot-token ciphertext cannot be replayed as a webhook secret. Bot lookup identifiers are keyed BLAKE3 values, not hashes an attacker can precompute from leaked SQLite data. Decrypted values are zeroized on drop and used only at the outbound request boundary.

Every status/results/config read is scoped by the authenticated bot ID. A valid token requesting another bot's job receives 404. Config values are clamped to operator ceilings: tenants cannot raise request/body/file limits, global concurrency, or the 25/s service ceiling.

Plaintext tokens, webhook secrets, bodies, chat IDs, and job IDs must never be placed in logs, metrics labels, tracing fields, panic text, or error responses. `/metrics` intentionally exposes only bounded aggregate labels (`class`, `state`). Keep it localhost-only or protect it with operator authentication because queue/resource volume is operationally sensitive.

## Reverse-proxy token log stripping

The Bot API-compatible URL contains the credential: `/bot<TOKEN>/...`. The service's redaction helper strips it, but the reverse proxy sees the URL first and must not log `$request_uri`.

Nginx example:

```nginx
map $uri $safe_uri {
    ~^/bot[^/]+/(.*)$ /bot<redacted>/$1;
    default $uri;
}
log_format safe '$remote_addr $request_method $safe_uri $status $body_bytes_sent';
access_log /var/log/nginx/access.log safe;

location = /metrics { allow 127.0.0.1; deny all; proxy_pass http://127.0.0.1:8080; }
location / { proxy_pass http://127.0.0.1:8080; }
```

Also scrub query strings and APM/error-reporting breadcrumbs. Test redaction with a canary token before production.

## Request, file, and storage boundaries

The service validates and hard-caps JSON body (32 MiB), multipart body (64 MiB), shared parameters, recipient patches, 100,000 recipients/job, total non-terminal recipients, and all bounded channels. Known-large requests acquire large-body/ingest permits before admission; unknown chunked requests must acquire at recipient 10,000 or fail 503. Store uploaded files only on trusted local storage; never serve them back through HTTP.

Multipart durability is temp write → `fdatasync(file)` → `fsync(temp dir)` → atomic rename into `data/files/` → `fsync(destination dir)` → `job_files` insert/promote commit. A job ID is returned only after promotion. On boot, abandoned temp data and unreferenced blobs are swept; referenced accepted-job blobs survive process kill/reboot.

SQLite WAL with `synchronous=FULL` is the durability boundary. This protects acknowledged state across process/VPS restart on the same healthy disk; it does **not** provide replication, Byzantine storage protection, region failover, or immunity to lost/corrupt media. Follow `OPERATIONS.md` for safe backup/restore and integrity checks.

## Telegram-control compliance

Rate limiting is safety/compliance behavior, not an obstacle to bypass. Global, bot, chat, group, method, and media limits are checked before lease. Telegram `retry_after` pauses the relevant scope durably and does not consume retry budget. Do not shard one bot across instances to evade Telegram limits, rotate tokens to bypass flood control, weaken limits after 429, or remove backoff to chase throughput. The system deliberately makes no guaranteed 30 msg/s claim.

A transport error after `MarkWireStarted` is ambiguous. Default `at_least_once` may duplicate sends; `at_most_once` may lose sends; `method_aware` follows metadata. `retry_max_attempts` and job deadlines bound non-flood retry behavior. Operators should alert on `recipients_terminal_total{class="ambiguous"}`.

## Completion webhook integrity and SSRF

Webhook pages are immutable stored bytes, at most 256 KiB, signed with HMAC-SHA256 over those **exact bytes**. Headers include event/job/page/attempt/timestamp and an idempotency key. Receivers must verify signature and freshness, then atomically dedupe the idempotency key before side effects. Delivery is at least once; repeated valid pages are expected after acknowledgement loss.

Outbound webhook policy:

- HTTPS only by default.
- Deny localhost and metadata names before DNS resolution.
- Resolve once, reject if **any** address is loopback/private/link-local/CGNAT/ULA/IPv4-mapped/private/NAT64-mapped.
- Pin the validated address set on the request client.
- Disable all redirects (`Policy::none`), including HTTPS-to-HTTPS redirects.
- Re-validate on every delivery attempt so DNS changes cannot inherit prior trust.

Never add an “allow private IP” tenant option. If an operator explicitly needs internal callbacks, isolate that deployment and provide a narrow operator-owned allowlist rather than tenant-controlled SSRF bypass.

## Review checklist

Before release verify:

- no secrets in logs, metrics, panic/error output, fixtures, or repository history;
- cross-bot status/results/config return 404 isolation;
- config/body/recipient/concurrency ceilings cannot be raised by a bot;
- durable file protocol and same-disk multipart reboot drill pass;
- HMAC exact-body/idempotency/retry tests pass;
- SSRF name/IP/mixed-answer/rebinding/redirect matrix passes;
- `/metrics` is operationally restricted;
- limiter/flood behavior remains enabled and no control-circumvention path was added.
