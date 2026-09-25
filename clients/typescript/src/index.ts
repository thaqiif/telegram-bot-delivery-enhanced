// ---------------------------------------------------------------------------
// @tgbulk/client — typed client for telegram-bulk-delivery (see docs/CLIENT.md).
//
// Zero dependencies; Web APIs only (fetch, crypto.subtle), so it runs in
// Cloudflare Workers, Bun and Node ≥ 20. Semantics it encodes:
//   • submit() is NEVER retried automatically: tgbulk has no idempotency key,
//     and a retried POST after a timeout can create a second fan-out. A
//     network error on submit surfaces as SubmitUnknownError so the caller can
//     reconcile (e.g. by marking the outbox rows "unknown" and checking later).
//   • HTTP 200 on submit means the job is durable — not delivered.
//   • Only fall back to calling Telegram directly when ready() is false.
// ---------------------------------------------------------------------------

/** The subset of fetch the client needs (Workers / Bun / Node / test doubles). */
export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export interface TgBulkOptions {
  /** e.g. "https://tgbulk.example.com" (no trailing slash). */
  baseUrl: string;
  /** BULK_API_KEY, when the service gates /bot… endpoints. */
  apiKey?: string;
  fetch?: FetchLike;
  /** Per-request timeout, ms (submit of a large job needs more). */
  timeoutMs?: number;
}

export type RecipientPatch = { chat_id: number | string } & Record<string, unknown>;

export interface SubmitResult {
  job_id: string;
  state: "queued";
  total: number;
  status_url: string;
  accepted_at_unix: number;
}

export type JobState = "accepting" | "queued" | "running" | "completed" | "failed_internal";

export interface JobStatus {
  job_id: string;
  method: string;
  state: JobState;
  total: number;
  queued: number;
  in_flight: number;
  delayed: number;
  succeeded: number;
  failed: number;
  ambiguous: number;
  percent_complete: number;
  accepted_at_unix: number;
  started_at_unix: number | null;
  completed_at_unix: number | null;
  eta: { seconds: number | null; reason: string | null };
  webhook_state: string;
}

export interface RecipientResult {
  idx: number;
  chat_id: string;
  status: "queued" | "leased" | "delayed" | "succeeded" | "failed" | "ambiguous";
  /** Normalized class for failures, e.g. "forbidden" (blocked the bot), "invalid_recipient". */
  error: string | null;
  telegram_message_id: number | null;
  attempt_count: number;
  time_send_unix: number | null;
}

export class TgBulkError extends Error {
  constructor(message: string, readonly status: number, readonly body: unknown) {
    super(message);
    this.name = "TgBulkError";
  }
}

/** A 2xx whose body could not be read/parsed: the outcome is unknown. */
class IndeterminateResponseError extends Error {
  constructor(readonly status: number) {
    super(`unreadable HTTP ${status} response`);
  }
}

/** The submit may or may not have been accepted (network error / timeout). Do NOT blindly retry. */
export class SubmitUnknownError extends Error {
  constructor(readonly cause: unknown) {
    super(`tgbulk submit outcome unknown: ${cause instanceof Error ? cause.message : String(cause)}`);
    this.name = "SubmitUnknownError";
  }
}

export const TERMINAL: ReadonlySet<JobState> = new Set(["completed", "failed_internal"]);

export class TgBulkClient {
  private readonly base: string;
  private readonly f: FetchLike;

  constructor(private readonly o: TgBulkOptions) {
    this.base = o.baseUrl.replace(/\/+$/, "");
    // Never store the global fetch as a method: calling it with `this` = client
    // throws "Illegal invocation" on Cloudflare Workers.
    this.f = o.fetch ?? ((input, init) => fetch(input, init));
  }

  private headers(json: boolean): Record<string, string> {
    return {
      ...(json ? { "content-type": "application/json" } : {}),
      ...(this.o.apiKey ? { authorization: `Bearer ${this.o.apiKey}` } : {}),
    };
  }

  private async call<T>(path: string, init: RequestInit, timeoutMs = this.o.timeoutMs ?? 30_000): Promise<T> {
    const res = await this.f(`${this.base}${path}`, { ...init, signal: AbortSignal.timeout(timeoutMs) });
    let body: unknown = null;
    let bodyReadFailed = false;
    try {
      body = await res.json();
    } catch {
      bodyReadFailed = true; // non-JSON, or the read was cut off
    }
    const b = body as { ok?: boolean; result?: T; description?: string; error?: string; error_code?: number } | null;
    if (res.ok && (bodyReadFailed || !b?.ok)) {
      // A 2xx we could not read proves nothing either way.
      throw new IndeterminateResponseError(res.status);
    }
    if (!res.ok || !b?.ok) {
      // Never put the token-bearing path in the message.
      throw new TgBulkError(`tgbulk ${init.method ?? "GET"} failed: HTTP ${res.status} ${b?.description ?? b?.error ?? ""}`.trim(), res.status, body);
    }
    return b.result as T;
  }

  /** Durable-acceptance readiness. Falls back to false on any error. */
  async ready(): Promise<boolean> {
    try {
      const r = await this.f(`${this.base}/readyz`, { signal: AbortSignal.timeout(5_000) });
      return r.ok;
    } catch {
      return false;
    }
  }

  /**
   * Submit ONE bulk job. `parameters` are shared Telegram fields (no chat_id);
   * each recipient is shallow-merged over them and must carry chat_id.
   */
  async submit(botToken: string, method: string, parameters: Record<string, unknown>, recipients: RecipientPatch[], timeoutMs = 120_000): Promise<SubmitResult> {
    if (recipients.length === 0) throw new TgBulkError("recipients must not be empty", 400, null);
    if ("chat_id" in parameters) throw new TgBulkError("chat_id belongs in recipients, not parameters", 400, null);
    try {
      return await this.call<SubmitResult>(
        `/bot${botToken}/${method}`,
        { method: "POST", headers: this.headers(true), body: JSON.stringify({ parameters, recipients }) },
        timeoutMs,
      );
    } catch (err) {
      // Only a readable error envelope FROM tgbulk is a definite "not accepted".
      // A gateway 502/503/504 without it (proxy/tunnel in front) may sit after a
      // durable accept, as may an unreadable 2xx or a network failure.
      const gatewayish = err instanceof TgBulkError && [502, 503, 504].includes(err.status) && !(err.body as { ok?: unknown } | null)?.hasOwnProperty?.("ok");
      if (err instanceof TgBulkError && !gatewayish) throw err;
      throw new SubmitUnknownError(err);
    }
  }

  job(botToken: string, jobId: string): Promise<JobStatus> {
    return this.call<JobStatus>(`/bot${botToken}/bulk/jobs/${encodeURIComponent(jobId)}`, { headers: this.headers(false) });
  }

  /** Poll until terminal with backoff (1 s → 15 s). */
  async waitForTerminal(botToken: string, jobId: string, opts: { timeoutMs?: number; onProgress?: (s: JobStatus) => void } = {}): Promise<JobStatus> {
    const end = Date.now() + (opts.timeoutMs ?? 3_600_000);
    let delay = 1_000;
    for (;;) {
      const s = await this.job(botToken, jobId);
      opts.onProgress?.(s);
      if (TERMINAL.has(s.state)) return s;
      if (Date.now() > end) throw new TgBulkError(`job ${jobId} not terminal before timeout (state ${s.state})`, 0, s);
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(15_000, Math.round(delay * 1.5));
    }
  }

  /** All results in stable recipient order (cursor pagination). */
  async *results(botToken: string, jobId: string, pageSize = 1000): AsyncGenerator<RecipientResult> {
    // The server clamps to 1..1000 and ends a listing only with a short page,
    // so the end test must compare against the EFFECTIVE limit.
    const limit = Math.min(1000, Math.max(1, Math.floor(pageSize)));
    pageSize = limit;
    let cursor = 0;
    for (;;) {
      const page = await this.call<{ items: RecipientResult[]; next_cursor: number | null }>(
        `/bot${botToken}/bulk/jobs/${encodeURIComponent(jobId)}/results?limit=${pageSize}&cursor=${cursor}`,
        { headers: this.headers(false) },
      );
      yield* page.items;
      if (page.items.length < pageSize || page.next_cursor == null) return;
      cursor = page.next_cursor;
    }
  }

  getConfig(botToken: string): Promise<Record<string, unknown>> {
    return this.call(`/bot${botToken}/getBulkDeliveryConfig`, { headers: this.headers(false) });
  }

  /**
   * Update per-bot policy. The FIRST call that sets `completion_webhook_url`
   * returns a server-generated `webhook_secret` — store it; it is never shown
   * again (use it with verifyWebhookSignature).
   */
  setConfig(botToken: string, patch: Record<string, unknown>): Promise<Record<string, unknown> & { webhook_secret?: string }> {
    return this.call(`/bot${botToken}/setBulkDeliveryConfig`, { method: "POST", headers: this.headers(true), body: JSON.stringify(patch) });
  }
}

/** base64url (no padding) → bytes. */
function b64urlDecode(s: string): Uint8Array<ArrayBuffer> {
  const b64 = s.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (s.length % 4)) % 4);
  const bin = atob(b64);
  return Uint8Array.from(bin, (c) => c.charCodeAt(0));
}

/**
 * Verify a completion webhook: `X-Bulk-Signature: v1=<hex HMAC-SHA256(key, raw body)>`.
 *
 * `secret` is the `webhook_secret` string returned ONCE by the setConfig call
 * that first sets `completion_webhook_url` (the server generates it; a secret
 * sent by the client is ignored). It is base64url of 32 random bytes, and the
 * HMAC key is those RAW bytes — not the string. Pass the RAW body bytes
 * exactly as received. Constant-time comparison.
 */
export async function verifyWebhookSignature(secret: string, rawBody: ArrayBuffer | Uint8Array, header: string | null): Promise<boolean> {
  if (!header?.startsWith("v1=")) return false;
  let key: CryptoKey;
  try {
    const keyBytes = b64urlDecode(secret);
    if (keyBytes.length !== 32) return false; // the service always issues 32 bytes
    key = await crypto.subtle.importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  } catch {
    return false;
  }
  const data = rawBody instanceof Uint8Array ? new Uint8Array(rawBody) : new Uint8Array(rawBody); // own ArrayBuffer
  const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, data));
  const want = Array.from(mac, (b) => b.toString(16).padStart(2, "0")).join("");
  const got = header.slice(3).toLowerCase();
  if (got.length !== want.length) return false;
  let diff = 0;
  for (let i = 0; i < want.length; i++) diff |= want.charCodeAt(i) ^ got.charCodeAt(i);
  return diff === 0;
}
