// ---------------------------------------------------------------------------
// M-04 end-to-end scenarios for tgbulk against faketg (run INSIDE server A).
//   bun run m04.ts subuh [n=10000]     one bot, n private chats, one submit
//   bun run m04.ts multibot [n=3000]   3 bots (MWS private/group/channel) at once
//   bun run m04.ts groups              group fan-out under the 20/min rule
// Reads BULK_API_KEY from /etc/tgbulk.env (never printed). Writes the verdict
// to /var/log/m04-<scenario>.json and prints it.
// ---------------------------------------------------------------------------

import { readFileSync, writeFileSync } from "node:fs";

const BASE = "http://127.0.0.1:8080";
const FAKE = "http://127.0.0.1:8081";
const KEY = /BULK_API_KEY=(.+)/.exec(readFileSync("/etc/tgbulk.env", "utf8"))![1]!.trim();
const H = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
const [scenario = "subuh", nArg] = process.argv.slice(2);

type Job = { job_id: string; state: string; total: number; succeeded: number; failed: number; ambiguous: number; queued: number; delayed: number; in_flight: number; percent_complete: number; started_at_unix: number | null; completed_at_unix: number | null };

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const j = async (r: Response) => {
  const t = await r.text();
  if (!r.ok) throw new Error(`${r.status} ${t.slice(0, 300)}`);
  return JSON.parse(t);
};

function cgroup(name: string): number {
  try {
    return Number(readFileSync(`/sys/fs/cgroup/tgbulk/${name}`, "utf8").trim().split(/\s/)[0]);
  } catch {
    return -1;
  }
}
async function metric(name: string): Promise<number> {
  const m = await (await fetch(`${BASE}/metrics`)).text();
  const line = m.split("\n").find((l) => l.startsWith(`${name} `) || l.startsWith(`${name}{`));
  return line ? Number(line.split(" ").pop()) : -1;
}

/** Private chat ids avoiding the fault tails, plus an explicit fault mix. */
function privateChats(n: number, faults: Record<number, number>): number[] {
  const out: number[] = [];
  for (const [tail, count] of Object.entries(faults)) for (let i = 0; i < count; i++) out.push(500000 + i * 100 + Number(tail));
  for (let id = 100000; out.length < n; id++) if (id % 100 < 13 || id % 100 > 18) out.push(id);
  return out;
}

async function submit(token: string, chats: number[], text: string) {
  const t0 = performance.now();
  const r = await j(
    await fetch(`${BASE}/bot${token}/sendMessage`, {
      method: "POST",
      headers: H,
      body: JSON.stringify({ parameters: { text, parse_mode: "HTML" }, recipients: chats.map((chat_id) => ({ chat_id })) }),
    }),
  );
  return { ...r.result, submitMs: Math.round(performance.now() - t0) };
}

async function waitDone(token: string, jobId: string, onTick?: (j: Job) => void, timeoutS = 3600): Promise<Job> {
  const end = Date.now() + timeoutS * 1000;
  let peakMem = 0;
  let peakWal = 0;
  for (;;) {
    const job = (await j(await fetch(`${BASE}/bot${token}/bulk/jobs/${jobId}`, { headers: H }))).result as Job;
    peakMem = Math.max(peakMem, cgroup("memory.current"));
    peakWal = Math.max(peakWal, await metric("sqlite_wal_bytes"));
    onTick?.(job);
    if (job.state === "completed" || job.state === "failed_internal") return Object.assign(job, { peakMem, peakWal });
    if (Date.now() > end) throw new Error(`timeout; last ${JSON.stringify(job)}`);
    await sleep(2000);
  }
}

async function allResults(token: string, jobId: string) {
  const items: Record<string, unknown>[] = [];
  let cursor = 0;
  for (;;) {
    const r = (await j(await fetch(`${BASE}/bot${token}/bulk/jobs/${jobId}/results?limit=1000&cursor=${cursor}`, { headers: H }))).result;
    items.push(...r.items);
    if (!r.items.length || r.next_cursor == null || r.items.length < 1000) break;
    cursor = r.next_cursor;
  }
  return items;
}

function summarize(items: Record<string, unknown>[]) {
  const by: Record<string, number> = {};
  for (const it of items) {
    const k = `${it.status}${it.error ? `:${String(it.error).slice(0, 40)}` : ""}`;
    by[k] = (by[k] ?? 0) + 1;
  }
  return by;
}

const verdict: Record<string, unknown> = { scenario, startedAt: new Date().toISOString() };
await fetch(`${FAKE}/_reset`);

if (scenario === "subuh") {
  const n = Number(nArg ?? 10000);
  const token = "7000000001:SUBUH_private_bot";
  const chats = privateChats(n, { 13: 20, 14: 20, 15: 20, 17: 5, 18: 3 });
  const sub = await submit(token, chats, "🕌 Waktu Subuh telah masuk — Kuala Lumpur 05:58");
  verdict.submit = { total: sub.total, submitMs: sub.submitMs };
  const t0 = Date.now();
  let lastLog = 0;
  const job = await waitDone(token, sub.job_id, (jb) => {
    if (Date.now() - lastLog > 30000) {
      lastLog = Date.now();
      console.log(`  ${Math.round((Date.now() - t0) / 1000)}s: ${jb.succeeded} ok, ${jb.failed} failed, ${jb.ambiguous} ambiguous, ${jb.queued + jb.delayed + jb.in_flight} pending`);
    }
  });
  const secs = (Date.now() - t0) / 1000;
  const items = await allResults(token, sub.job_id);
  const fake = await (await fetch(`${FAKE}/_stats`)).json();
  Object.assign(verdict, {
    job: { state: job.state, succeeded: job.succeeded, failed: job.failed, ambiguous: job.ambiguous },
    wallSeconds: Math.round(secs),
    throughputPerSec: +(job.succeeded / secs).toFixed(2),
    results: { rows: items.length, byOutcome: summarize(items), sample: items.slice(0, 1) },
    telegramSaw: { accepted: fake.accepted, duplicates: fake.duplicates, peakPerSec: fake.peakPerSec[token], flood429: fake.flood429 },
    resources: { peakCgroupMemMiB: Math.round((job as unknown as { peakMem: number }).peakMem / 1048576), cgroupMemPeakMiB: Math.round(cgroup("memory.peak") / 1048576), peakWalMiB: +((job as unknown as { peakWal: number }).peakWal / 1048576).toFixed(1) },
  });
}

if (scenario === "multibot") {
  const n = Number(nArg ?? 3000);
  const bots = ["7000000011:MWS_private", "7000000012:MWS_group", "7000000013:MWS_channel"];
  const t0 = Date.now();
  // Bot 0 → private chats; bots 1 and 2 → distinct groups/channels (negative
  // ids), one message each, so the 20/min per-group rule applies per target.
  const chatsFor = (i: number) => (i === 0 ? privateChats(n, {}) : privateChats(n, {}).map((c) => -(c + i * 1_000_000)));
  const subs = await Promise.all(bots.map((b, i) => submit(b, chatsFor(i), `Zohor ${i}`)));
  const jobs = await Promise.all(subs.map((s, i) => waitDone(bots[i]!, s.job_id)));
  const secs = (Date.now() - t0) / 1000;
  const fake = await (await fetch(`${FAKE}/_stats`)).json();
  const ok = jobs.reduce((a, jb) => a + jb.succeeded, 0);
  Object.assign(verdict, {
    bots: bots.length,
    perBot: n,
    wallSeconds: Math.round(secs),
    aggregatePerSec: +(ok / secs).toFixed(2),
    succeeded: ok,
    telegramSaw: { duplicates: fake.duplicates, peakPerSec: fake.peakPerSec, flood429: fake.flood429 },
    cgroupMemPeakMiB: Math.round(cgroup("memory.peak") / 1048576),
  });
}

if (scenario === "groups") {
  // One group chat, 45 messages: Telegram allows 20/min → must take ≥ 2 min, never fail.
  const token = "7000000021:MWS_group_bot";
  const t0 = Date.now();
  const r = await j(
    await fetch(`${BASE}/bot${token}/sendMessage`, {
      method: "POST",
      headers: H,
      body: JSON.stringify({ parameters: { text: "x" }, recipients: Array.from({ length: 45 }, (_, i) => ({ chat_id: -1001234567890, text: `group msg ${i}` })) }),
    }),
  );
  const job = await waitDone(token, r.result.job_id);
  const fake = await (await fetch(`${FAKE}/_stats`)).json();
  Object.assign(verdict, { job: { state: job.state, succeeded: job.succeeded, failed: job.failed }, wallSeconds: Math.round((Date.now() - t0) / 1000), telegramSaw: { flood429: fake.flood429, duplicates: fake.duplicates } });
}

if (scenario === "kill") {
  // OPERATIONS.md drill: SIGKILL mid-job; boot.sh's supervisor restarts tgbulk
  // on the same data dir + master key. Every recipient must still complete.
  const token = "7000000031:KILL_drill_bot";
  const chats = privateChats(Number(nArg ?? 3000), {});
  const sub = await submit(token, chats, "Asar reminder");
  let killed = false;
  let killedAt = 0;
  const t0 = Date.now();
  const job = await waitDone(token, sub.job_id, (jb) => {
    if (!killed && jb.succeeded > chats.length * 0.3) {
      killed = true;
      killedAt = jb.succeeded;
      const pid = Bun.spawnSync(["pgrep", "-f", "^/opt/tgbulk/telegram-bulk-delivery "]).stdout.toString().trim().split("\n")[0];
      Bun.spawnSync(["kill", "-9", pid!]);
      console.log(`  SIGKILL pid ${pid} at ${jb.succeeded}/${chats.length} succeeded`);
    }
  }).catch(async (e) => {
    throw e;
  });
  const fake = await (await fetch(`${FAKE}/_stats`)).json();
  const restarts = Bun.spawnSync(["sh", "-c", "grep -c starting /var/log/tgbulk-supervisor.log 2>/dev/null || echo 0"]).stdout.toString().trim();
  Object.assign(verdict, {
    killedAtSucceeded: killedAt,
    job: { state: job.state, succeeded: job.succeeded, failed: job.failed, ambiguous: job.ambiguous, total: job.total },
    wallSeconds: Math.round((Date.now() - t0) / 1000),
    telegramSaw: { accepted: fake.accepted, duplicates: fake.duplicates, flood429: fake.flood429 },
    supervisorRestarts: Number(restarts),
    integrity: Bun.spawnSync(["sh", "-c", "sqlite3 -readonly /var/lib/tgbulk/telegram-bulk-delivery.db 'PRAGMA integrity_check'"]).stdout.toString().trim(),
  });
}

if (scenario === "webhook") {
  const token = "7000000041:WEBHOOK_bot";
  let key = new Uint8Array();
  const got: { ok: boolean; body: Record<string, unknown> }[] = [];
  const recv = Bun.serve({
    port: 8090,
    hostname: "127.0.0.1",
    async fetch(req) {
      const raw = new Uint8Array(await req.arrayBuffer());
      const sig = req.headers.get("x-bulk-signature") ?? "";
      // HMAC key = the 32 RAW bytes behind the base64url secret (docs/CLIENT.md).
      const expect = new Bun.CryptoHasher("sha256", key).update(raw).digest("hex");
      got.push({ ok: sig === `v1=${expect}`, body: JSON.parse(new TextDecoder().decode(raw)) });
      return new Response("ok");
    },
  });
  const cfg = (await j(await fetch(`${BASE}/bot${token}/setBulkDeliveryConfig`, { method: "POST", headers: H, body: JSON.stringify({ completion_webhook_url: "http://127.0.0.1:8090/hook" }) }))).result;
  key = new Uint8Array(Buffer.from(String(cfg.webhook_secret), "base64url"));
  const sub = await submit(token, privateChats(50, { 13: 2 }), "Isyak");
  const job = await waitDone(token, sub.job_id);
  for (let i = 0; i < 30 && got.length === 0; i++) await sleep(1000);
  const status = (await j(await fetch(`${BASE}/bot${token}/bulk/jobs/${sub.job_id}`, { headers: H }))).result;
  recv.stop(true);
  Object.assign(verdict, {
    job: { state: job.state, succeeded: job.succeeded, failed: job.failed },
    webhooks: got.length,
    allSignaturesValid: got.length > 0 && got.every((g) => g.ok),
    firstBodyKeys: got[0] ? Object.keys(got[0].body) : [],
    webhookState: status.webhook_state,
  });
}

verdict.finishedAt = new Date().toISOString();
writeFileSync(`/var/log/m04-${scenario}.json`, JSON.stringify(verdict, null, 2));
console.log(JSON.stringify(verdict, null, 2));
