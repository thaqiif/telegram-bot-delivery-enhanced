// Contract test: the client against the REAL tgbulk binary + faketg (no mocks).
// Requires `cargo build --release` in the repo root (target/release/telegram-bulk-delivery).
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import type { Subprocess } from "bun";
import { SubmitUnknownError, TgBulkClient, TgBulkError, verifyWebhookSignature, type RecipientResult } from "../src/index.ts";

const ROOT = path.resolve(import.meta.dir, "../../..");
const BIN = path.join(ROOT, "target/release/telegram-bulk-delivery");
// Free ports per run (parallel CI / leftovers must not collide).
const freePort = () => {
  const srv = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const p = srv.port!;
  srv.stop(true);
  return p;
};
const TG_PORT = freePort();
const BULK_PORT = freePort();
const HOOK_PORT = freePort();
const API_KEY = crypto.randomUUID();
const TOKEN = "7100000001:CONTRACT_bot";

let dir: string;
let faketg: Subprocess;
let tgbulk: Subprocess;
const client = new TgBulkClient({ baseUrl: `http://127.0.0.1:${BULK_PORT}`, apiKey: API_KEY });

async function waitHttp(url: string, ms = 15_000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      /* not up yet */
    }
    await Bun.sleep(100);
  }
  throw new Error(`${url} not up`);
}

beforeAll(async () => {
  if (!existsSync(BIN)) throw new Error(`build first: cargo build --release (${BIN})`);
  dir = mkdtempSync(path.join(tmpdir(), "tgbulk-contract-"));
  const defaults = readFileSync(path.join(ROOT, "config/operator.defaults.toml"), "utf8");
  writeFileSync(
    path.join(dir, "cfg.toml"),
    defaults
      .replace(/^database_path = .*/m, `database_path = "${dir}/db.sqlite"`)
      .replace(/^bind = .*/m, `bind = "127.0.0.1:${BULK_PORT}"`)
      .replace(/^allow_private_targets = .*/m, "allow_private_targets = true")
      .replace(/^free_disk_reserve_bytes = .*/m, "free_disk_reserve_bytes = 1048576")
      // Top-level keys: must precede any [table] header the defaults may grow.
      .replace(/^(database_path = .*)$/m, '$1\nwebhook_trusted_hosts = ["127.0.0.1"]\nwebhook_allow_insecure_http = true'),
  );
  faketg = Bun.spawn(["bun", "run", path.join(ROOT, "scripts/faketg/faketg.ts"), "--port", String(TG_PORT)], { stdout: "ignore", stderr: "inherit" });
  tgbulk = Bun.spawn([BIN, path.join(dir, "cfg.toml")], {
    cwd: dir,
    env: {
      ...process.env,
      BULK_MASTER_KEY: Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64"),
      BULK_API_KEY: API_KEY,
      TELEGRAM_API_BASE: `http://127.0.0.1:${TG_PORT}`,
    },
    stdout: "ignore",
    stderr: "ignore",
  });
  await waitHttp(`http://127.0.0.1:${TG_PORT}/_stats`);
  await waitHttp(`http://127.0.0.1:${BULK_PORT}/readyz`); // ready, not just alive
}, 30_000);

afterAll(() => {
  tgbulk?.kill();
  faketg?.kill();
  rmSync(dir, { recursive: true, force: true });
});

describe("TgBulkClient against real tgbulk", () => {
  test("ready()", async () => {
    expect(await client.ready()).toBe(true);
    expect(await new TgBulkClient({ baseUrl: "http://127.0.0.1:1" }).ready()).toBe(false);
  });

  test("submit → waitForTerminal → results: counts, classes and message ids", async () => {
    const recipients = [
      ...Array.from({ length: 25 }, (_, i) => ({ chat_id: 200000 + i * 100 + 1 })),
      { chat_id: 300013 }, // blocked the bot
      { chat_id: 300014 }, // chat not found
      { chat_id: 200099, text: "personal override" },
    ];
    const sub = await client.submit(TOKEN, "sendMessage", { text: "contract", parse_mode: "HTML" }, recipients);
    expect(sub).toMatchObject({ state: "queued", total: recipients.length });
    expect(sub.status_url).toContain(sub.job_id);

    const progress: number[] = [];
    const done = await client.waitForTerminal(TOKEN, sub.job_id, { timeoutMs: 60_000, onProgress: (s) => progress.push(s.percent_complete) });
    expect(done).toMatchObject({ state: "completed", total: 28, succeeded: 26, failed: 2, ambiguous: 0 });
    expect(progress.length).toBeGreaterThan(0);

    const rows: RecipientResult[] = [];
    for await (const r of client.results(TOKEN, sub.job_id, 10)) rows.push(r); // forces 3 pages
    expect(rows.map((r) => r.idx)).toEqual(Array.from({ length: 28 }, (_, i) => i));
    expect(rows.filter((r) => r.status === "succeeded").every((r) => typeof r.telegram_message_id === "number")).toBe(true);
    const errs = Object.fromEntries(rows.filter((r) => r.status === "failed").map((r) => [r.chat_id, r.error]));
    expect(errs).toEqual({ "300013": "forbidden", "300014": "invalid_recipient" });
  }, 90_000);

  test("auth gate and client-side validation", async () => {
    const bad = new TgBulkClient({ baseUrl: `http://127.0.0.1:${BULK_PORT}`, apiKey: "wrong" });
    const err = await bad.submit(TOKEN, "sendMessage", { text: "x" }, [{ chat_id: 1 }]).catch((e) => e);
    expect(err).toBeInstanceOf(TgBulkError);
    expect((err as TgBulkError).status).toBe(401);
    expect((err as TgBulkError).message).not.toContain(TOKEN); // never leak the token path
    await expect(client.submit(TOKEN, "sendMessage", { text: "x" }, [])).rejects.toThrow(/empty/);
    await expect(client.submit(TOKEN, "sendMessage", { text: "x", chat_id: 1 }, [{ chat_id: 1 }])).rejects.toThrow(/chat_id/);
  });

  test("pages larger than the server's 1000 cap still return every row", async () => {
    const recipients = Array.from({ length: 1205 }, (_, i) => ({ chat_id: 600000 + i * 100 + 1 }));
    const sub = await client.submit("7100000003:CONTRACT_page_bot", "sendMessage", { text: "p" }, recipients);
    let n = 0;
    for await (const _ of client.results("7100000003:CONTRACT_page_bot", sub.job_id, 5000)) n++;
    expect(n).toBe(1205); // results exist (queued) before delivery; all rows listed
  }, 60_000);

  test("unreadable 2xx and gateway 5xx on submit are 'unknown', not 'failed'", async () => {
    for (const fake of [
      () => new Response("<html>proxy</html>", { status: 200 }),
      () => new Response("Bad Gateway", { status: 502 }),
      () => new Response("Gateway Timeout", { status: 504 }),
    ]) {
      const c = new TgBulkClient({ baseUrl: "http://x", fetch: async () => fake() });
      expect(await c.submit(TOKEN, "sendMessage", { text: "x" }, [{ chat_id: 1 }]).catch((e) => e)).toBeInstanceOf(SubmitUnknownError);
    }
    // A readable tgbulk envelope IS definite.
    const c = new TgBulkClient({ baseUrl: "http://x", fetch: async () => Response.json({ ok: false, description: "overloaded" }, { status: 503 }) });
    expect(await c.submit(TOKEN, "sendMessage", { text: "x" }, [{ chat_id: 1 }]).catch((e) => e)).toBeInstanceOf(TgBulkError);
  });

  test("global fetch is not invoked as a method (Workers 'Illegal invocation')", async () => {
    const orig = globalThis.fetch;
    let sawThis: unknown = "unset";
    globalThis.fetch = function (this: unknown, ...a: Parameters<typeof fetch>) {
      sawThis = this;
      return orig(...a);
    } as typeof fetch;
    try {
      const c = new TgBulkClient({ baseUrl: `http://127.0.0.1:${BULK_PORT}`, apiKey: API_KEY });
      expect(await c.ready()).toBe(true);
      expect(sawThis).not.toBeInstanceOf(TgBulkClient);
    } finally {
      globalThis.fetch = orig;
    }
  });

  test("unreachable service on submit → SubmitUnknownError (never auto-retried)", async () => {
    const down = new TgBulkClient({ baseUrl: "http://127.0.0.1:1", apiKey: API_KEY });
    const err = await down.submit(TOKEN, "sendMessage", { text: "x" }, [{ chat_id: 1 }]).catch((e) => e);
    expect(err).toBeInstanceOf(SubmitUnknownError);
  });

  test("real signed completion webhook verifies; tampered body does not", async () => {
    const hooks: { raw: Uint8Array; sig: string | null }[] = [];
    const recv = Bun.serve({
      port: HOOK_PORT,
      hostname: "127.0.0.1",
      async fetch(req) {
        hooks.push({ raw: new Uint8Array(await req.arrayBuffer()), sig: req.headers.get("x-bulk-signature") });
        return new Response("ok");
      },
    });
    try {
      const bot = "7100000002:CONTRACT_hook_bot";
      const cfg = await client.setConfig(bot, { completion_webhook_url: `http://127.0.0.1:${HOOK_PORT}/hook` });
      const secret = cfg.webhook_secret!; // server-generated, returned once
      expect(secret).toMatch(/^[A-Za-z0-9_-]{43}$/); // base64url of 32 bytes
      const again = await client.setConfig(bot, { completion_webhook_url: `http://127.0.0.1:${HOOK_PORT}/hook` });
      expect(again.webhook_secret).toBeUndefined(); // never shown again
      const sub = await client.submit(bot, "sendMessage", { text: "hook" }, [{ chat_id: 400001 }, { chat_id: 400101 }]);
      await client.waitForTerminal(bot, sub.job_id, { timeoutMs: 60_000 });
      for (let i = 0; i < 100 && hooks.length === 0; i++) await Bun.sleep(200);
      expect(hooks.length).toBeGreaterThan(0);
      const h = hooks[0]!;
      expect(await verifyWebhookSignature(secret, h.raw, h.sig)).toBe(true);
      const tampered = new Uint8Array(h.raw);
      tampered[5] = tampered[5]! ^ 1;
      expect(await verifyWebhookSignature(secret, tampered, h.sig)).toBe(false);
      expect(await verifyWebhookSignature(secret.slice(0, -2) + "AA", h.raw, h.sig)).toBe(false);
      expect(await verifyWebhookSignature(secret, h.raw, null)).toBe(false);
      expect(await verifyWebhookSignature("", h.raw, h.sig)).toBe(false); // resolves false, never throws
      const body = JSON.parse(new TextDecoder().decode(h.raw));
      expect(JSON.stringify(body)).toContain(sub.job_id);
    } finally {
      recv.stop(true);
    }
  }, 90_000);
});
