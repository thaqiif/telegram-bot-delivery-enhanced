// ---------------------------------------------------------------------------
// Soak driver for crates/telegram-bulk-delivery/scripts/loadtest-1vcpu-soak.sh.
// 1 000 bots each keep one job in flight (70 private chats per job, unique per
// bot) for DURATION_SECS, so the dispatcher is always saturated and has to
// share the global grant budget fairly. At the end it writes FAIRNESS_FILE as
// "<bot token> <accepted sends>" lines from faketg's per-token counters.
//   DURATION_SECS=1800 FAIRNESS_FILE=/var/log/fairness.txt bun run soak-driver.ts
// ---------------------------------------------------------------------------
import { readFileSync, writeFileSync } from "node:fs";

const BASE = "http://127.0.0.1:8080";
const FAKE = "http://127.0.0.1:8081";
const KEY = /BULK_API_KEY=(.+)/.exec(readFileSync("/etc/tgbulk.env", "utf8"))![1]!.trim();
const H = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
const BOTS = Number(process.env.BOTS ?? 1000);
const PER_JOB = Number(process.env.PER_JOB ?? 70);
const DURATION = Number(process.env.DURATION_SECS ?? 1800) * 1000;
const OUT = process.env.FAIRNESS_FILE ?? "/var/log/fairness.txt";
const token = (i: number) => `${8000000000 + i}:SOAK_bot_${i}`;

await fetch(`${FAKE}/_reset`);
const jobs = new Map<number, string>();
let round = 0;
let submitted = 0;
let submitErrors = 0;

async function submit(i: number) {
  const base = 1_000_000 + i * 10_000 + (round % 100) * 100;
  // Offsets 0..99 minus faketg's fault tails 13..18, unless FAULTS=1 asks for them.
  const offs = Array.from({ length: 100 }, (_, k) => k).filter((k) => process.env.FAULTS === "1" || k < 13 || k > 18);
  const recipients = offs.slice(0, PER_JOB).map((k) => ({ chat_id: base + k, text: `soak r${round} b${i} #${k}` }));
  const r = await fetch(`${BASE}/bot${token(i)}/sendMessage`, { method: "POST", headers: H, body: JSON.stringify({ parameters: { text: "soak" }, recipients }) });
  if (!r.ok) {
    submitErrors++;
    return;
  }
  jobs.set(i, (await r.json()).result.job_id);
  submitted += PER_JOB;
}

const t0 = Date.now();
// Initial wave, 25 concurrent submits at a time.
for (let i = 0; i < BOTS; i += 25) await Promise.all(Array.from({ length: Math.min(25, BOTS - i) }, (_, k) => submit(i + k)));
console.log(`initial wave: ${BOTS} bots, ${submitted} recipients queued in ${Math.round((Date.now() - t0) / 1000)}s`);

// Keep every bot busy: re-submit when its job finishes.
while (Date.now() - t0 < DURATION) {
  await new Promise((r) => setTimeout(r, 30_000));
  round++;
  let refilled = 0;
  for (let i = 0; i < BOTS && Date.now() - t0 < DURATION; i++) {
    const id = jobs.get(i);
    if (!id) continue;
    const s = await fetch(`${BASE}/bot${token(i)}/bulk/jobs/${id}`, { headers: H });
    if (!s.ok) continue;
    const st = (await s.json()).result.state;
    if (st === "completed" || st === "failed_internal") {
      await submit(i);
      refilled++;
    }
  }
  const stats = await (await fetch(`${FAKE}/_stats`)).json();
  console.log(`t=${Math.round((Date.now() - t0) / 1000)}s accepted=${stats.accepted} dups=${stats.duplicates} 429=${stats.flood429} refilled=${refilled} submitErrors=${submitErrors}`);
}

const stats = await (await fetch(`${FAKE}/_stats`)).json();
const lines = Array.from({ length: BOTS }, (_, i) => `${token(i)} ${stats.acceptedByToken[token(i)] ?? 0}`);
writeFileSync(OUT, lines.join("\n") + "\n");
const progressed = lines.filter((l) => Number(l.split(" ")[1]) >= 1).length;
console.log(JSON.stringify({ bots: BOTS, progressed, accepted: stats.accepted, duplicates: stats.duplicates, flood429: stats.flood429, submitted, submitErrors }));
