// ---------------------------------------------------------------------------
// faketg — a fake Telegram Bot API for end-to-end tests of tgbulk.
//
// Enforces Telegram's real flood rules and answers like Telegram, so a
// delivery service is tested against the behaviour it will meet in prod:
//   • per bot token: ~30 msg/s (sliding 1 s window)          → 429 retry_after
//   • per private chat (chat_id > 0): 1 msg/s                → 429 retry_after
//   • per group/channel (chat_id < 0): 20 msg/min            → 429 retry_after
// Deterministic faults by chat id (last two digits):
//   13 → 403 "bot was blocked by the user"
//   14 → 400 "chat not found"
//   15 → 500 on the first attempt, then OK            (transient)
//   16 → 400 migrate_to_chat_id (groups)              (migration)
//   17 → 2.5 s latency                                 (slow upstream)
//   18 → first attempt: delivered, but the connection is dropped before the
//        response (the "ambiguous" case: did it send?)
// Every accepted send is recorded; GET /_stats reports duplicates and the peak
// accepted rate per token. faketg ENFORCES the per-chat/group rules (answering
// 429), so a client can't "violate" them — it pays in 429s (flood429).
// Duplicates/attempts are keyed by token|chat|text: call GET /_reset before
// each scenario, and give distinct recipients distinct text if the same chat
// may legitimately receive several messages.
// Real Telegram's private 1/s is a soft burst limit; faketg is stricter.
//
//   bun run faketg.ts --port 8081
// ---------------------------------------------------------------------------

const port = Number(process.argv[process.argv.indexOf("--port") + 1] || 8081);
const GLOBAL_PER_SEC = Number(process.env.FAKETG_GLOBAL_PER_SEC ?? 30);

type Json = Record<string, unknown>;
const now = () => Date.now();

const tokenWindow = new Map<string, number[]>(); // accepted send times, last 1 s
const chatLast = new Map<string, number>(); // token|chat → last accepted ms (private)
const groupWindow = new Map<string, number[]>(); // token|chat → accepted times, last 60 s
const attempts = new Map<string, number>(); // token|chat|text → attempts
const delivered = new Map<string, number>(); // token|chat|text → times delivered
let nextMessageId = 1;

const stats = {
  requests: 0,
  accepted: 0,
  duplicates: 0,
  flood429: 0,
  blocked403: 0,
  notFound400: 0,
  transient500: 0,
  migrated: 0,
  droppedAcks: 0,
  peakPerSec: {} as Record<string, number>,
  firstAcceptAt: 0,
  lastAcceptAt: 0,
};

const tg = (status: number, body: Json) => Response.json(body, { status });
const flood = (retryAfter: number) => {
  stats.flood429++;
  return tg(429, { ok: false, error_code: 429, description: `Too Many Requests: retry after ${retryAfter}`, parameters: { retry_after: retryAfter } });
};

function prune(list: number[], windowMs: number, t: number): number[] {
  while (list.length && list[0]! <= t - windowMs) list.shift();
  return list;
}

async function params(req: Request): Promise<Json> {
  const ct = req.headers.get("content-type") ?? "";
  if (ct.includes("application/json")) return (await req.json()) as Json;
  if (ct.includes("form")) return Object.fromEntries((await req.formData()).entries()) as Json;
  const u = new URL(req.url);
  return Object.fromEntries(u.searchParams.entries());
}

Bun.serve({
  port,
  hostname: "127.0.0.1",
  idleTimeout: 30,
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/_stats") return Response.json(stats);
    if (u.pathname === "/_reset") {
      tokenWindow.clear(); chatLast.clear(); groupWindow.clear(); attempts.clear(); delivered.clear();
      Object.assign(stats, { requests: 0, accepted: 0, duplicates: 0, flood429: 0, blocked403: 0, notFound400: 0, transient500: 0, migrated: 0, droppedAcks: 0, peakPerSec: {}, firstAcceptAt: 0, lastAcceptAt: 0 });
      return Response.json({ ok: true });
    }
    const m = /^\/bot([^/]+)\/([A-Za-z]+)$/.exec(u.pathname);
    if (!m) return tg(404, { ok: false, error_code: 404, description: "Not Found" });
    const [, token, method] = m as unknown as [string, string, string];
    stats.requests++;
    if (method === "getMe") return tg(200, { ok: true, result: { id: Number(token.split(":")[0]) || 1, is_bot: true, first_name: "FakeBot", username: "fake_bot" } });

    const p = await params(req);
    const chat = String(p.chat_id ?? "");
    if (!chat) return tg(400, { ok: false, error_code: 400, description: "Bad Request: chat_id is empty" });
    const tail = Math.abs(Number(chat)) % 100;
    const key = `${token}|${chat}|${String(p.text ?? p.caption ?? "")}`;
    const n = (attempts.get(key) ?? 0) + 1;
    attempts.set(key, n);
    const t = now();

    // Deterministic faults first (Telegram answers these before flood accounting).
    if (tail === 13) return (stats.blocked403++, tg(403, { ok: false, error_code: 403, description: "Forbidden: bot was blocked by the user" }));
    if (tail === 14) return (stats.notFound400++, tg(400, { ok: false, error_code: 400, description: "Bad Request: chat not found" }));
    if (tail === 15 && n === 1) return (stats.transient500++, tg(500, { ok: false, error_code: 500, description: "Internal Server Error" }));
    if (tail === 16 && Number(chat) < 0 && Number(chat) > -1e12) {
      stats.migrated++;
      return tg(400, { ok: false, error_code: 400, description: "Bad Request: group chat was upgraded to a supergroup chat", parameters: { migrate_to_chat_id: -1000000000000 - Math.abs(Number(chat)) } });
    }

    // Flood rules.
    const tw = prune(tokenWindow.get(token) ?? [], 1000, t);
    if (tw.length >= GLOBAL_PER_SEC) return flood(1);
    if (Number(chat) > 0) {
      const last = chatLast.get(`${token}|${chat}`);
      if (last !== undefined && t - last < 1000) return flood(1);
    } else {
      const gw = prune(groupWindow.get(`${token}|${chat}`) ?? [], 60_000, t);
      if (gw.length >= 20) return flood(Math.ceil((gw[0]! + 60_000 - t) / 1000));
      gw.push(t);
      groupWindow.set(`${token}|${chat}`, gw);
    }

    // Accept.
    tw.push(t);
    tokenWindow.set(token, tw);
    if (Number(chat) > 0) chatLast.set(`${token}|${chat}`, t);
    stats.peakPerSec[token] = Math.max(stats.peakPerSec[token] ?? 0, tw.length);
    stats.accepted++;
    stats.firstAcceptAt ||= t;
    stats.lastAcceptAt = t;
    const times = (delivered.get(key) ?? 0) + 1;
    delivered.set(key, times);
    if (times > 1) stats.duplicates++;
    const result = { message_id: nextMessageId++, date: Math.floor(t / 1000), chat: { id: Number(chat), type: Number(chat) > 0 ? "private" : "supergroup" }, text: p.text };

    if (tail === 17) await Bun.sleep(2500);
    if (tail === 18 && n === 1) {
      // Delivered, then the ack is lost: stall past any sane client timeout.
      stats.droppedAcks++;
      await Bun.sleep(120_000);
    }
    return tg(200, { ok: true, result });
  },
});
console.log(`faketg listening on 127.0.0.1:${port} (global ${GLOBAL_PER_SEC}/s per token)`);
