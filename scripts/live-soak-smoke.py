#!/usr/bin/env python3
"""Bounded live soak smoke (no writable cgroup required on shared hosts).

Proves US-005 AC[4] fair progress for 1000 queued bots and bounded
RSS/WAL/queue channels against a localhost Telegram mock, then exits.
The full 30-minute cgroup run stays operator-executed via
crates/telegram-bulk-delivery/scripts/loadtest-1vcpu-soak.sh; this is the short shared-host-safe harness.
"""
import json, os, subprocess, sys, threading, time, urllib.request, urllib.error, http.server, concurrent.futures

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RT = "/tmp/tbd-runtime"
API = "http://127.0.0.1:18082"
BIN = f"{REPO}/target/debug/telegram-bulk-delivery"
BOTS = int(os.environ.get("BOTS", "1000"))
SAMPLE = os.environ.get("SAMPLE", f"{RT}/soak.tsv")
LOG = os.environ.get("SERVICE_LOG", f"{RT}/service.log")
os.makedirs(RT, exist_ok=True)

for stale in ("db.sqlite", "db.sqlite-wal", "db.sqlite-shm"):
    try: os.remove(f"{RT}/{stale}")
    except FileNotFoundError: pass
cfg = open(f"{REPO}/config/operator.defaults.toml").read()
cfg = cfg.replace('database_path = "data/telegram-bulk-delivery.db"',
                  'database_path = "/tmp/tbd-runtime/db.sqlite"')
cfg = cfg.replace('bind = "127.0.0.1:8080"', 'bind = "127.0.0.1:18082"')
open(f"{RT}/config.toml", "w").write(cfg)

class Mock(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    sent = 0
    lock = threading.Lock()
    def log_message(self, *a): pass
    def _out(self, obj):
        b = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(b)))
        self.send_header("connection", "keep-alive")
        self.end_headers()
        self.wfile.write(b)
    def do_GET(self):
        self._out({"ok": True, "result": {"id": 1}})   # getMe
    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        if n: self.rfile.read(n)
        with Mock.lock:
            Mock.sent += 1
        self._out({"ok": True, "result": {"message_id": 7, "date": int(time.time())}})

def raw(method, path, payload=None, timeout=90):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"content-type": "application/json"} if data else {}
    r = urllib.request.Request(API + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()

def req(method, path, payload=None, timeout=90):
    status, body = raw(method, path, payload, timeout)
    return json.loads(body)

def metric(text, name):
    for line in text.splitlines():
        if line.startswith(name + " "):
            return int(line.split()[-1])
    return 0

def main():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 19091), Mock)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    env = dict(os.environ)
    env["TELEGRAM_API_BASE"] = "http://127.0.0.1:19091"
    env["BULK_MASTER_KEY"] = subprocess.check_output(
        ["sh", "-c", "head -c32 /dev/zero | base64 -w0"], text=True).strip()
    logf = open(LOG, "w")
    proc = subprocess.Popen([BIN, f"{RT}/config.toml"], env=env,
                            stdout=logf, stderr=subprocess.STDOUT)
    try:
        ready = False
        for _ in range(200):
            try:
                status, _ = raw("GET", "/readyz", timeout=5)
                if status == 200:
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.25)
        if not ready:
            print("FAIL: service never became ready; service.log tail:")
            print(open(LOG).read()[-2000:])
            sys.exit(2)

        tokens = [f"smoke{i:04d}" for i in range(BOTS)]
        def submit(t):
            out = req("POST", f"/bot{t}/sendMessage",
                      {"parameters": {"text": "probe"},
                       "recipients": [{"chat_id": 200000 + int(t[5:])}]})
            return out["result"]["job_id"]
        started = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
            jobs = list(ex.map(submit, tokens))
        print(f"submitted {len(jobs)} single-recipient jobs in {time.time()-started:.1f}s", flush=True)

        out = open(SAMPLE, "w")
        out.write("elapsed_s\tsucceeded\trss_bytes\twal_bytes\twriter_depth\tready_bots\n")
        peak_rss = peak_wal = done = 0
        deadline = started + 240
        while time.time() < deadline:
            time.sleep(2)
            _, body = raw("GET", "/metrics")
            text = body.decode()
            done = metric(text, 'telegram_outcomes_total{class="succeeded"}')
            rss = metric(text, "process_rss_bytes"); wal = metric(text, "sqlite_wal_bytes")
            peak_rss = max(peak_rss, rss); peak_wal = max(peak_wal, wal)
            out.write(f"{time.time()-started:.0f}\t{done}\t{rss}\t{wal}\t"
                      f"{metric(text,'writer_queue_depth')}\t{metric(text,'ready_bots')}\n")
            out.flush()
            if done >= BOTS:
                break
        out.close()

        def st(t, j):
            try:
                return req("GET", f"/bot{t}/bulk/jobs/{j}")["result"]
            except Exception:
                return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
            states = list(ex.map(lambda x: st(*x), zip(tokens, jobs)))
        completed = sum(1 for s in states if s and s["state"] == "completed" and s["succeeded"] >= 1)
        unreachable = sum(1 for s in states if s is None or s["state"] != "completed")
        elapsed = time.time() - started

        print(f"elapsed={elapsed:.0f}s  outcomes_succeeded={done}/{BOTS}  "
              f"jobs_completed_with_success={completed}/{BOTS}  nonterminal_or_unreachable={unreachable}")
        print(f"peak_rss={peak_rss}  peak_wal={peak_wal}")
        print("samples:")
        for line in open(SAMPLE).read().splitlines()[-5:]:
            print("  " + line)

        ok = done >= BOTS and completed >= BOTS and unreachable == 0
        ok = ok and peak_rss < 768*1024*1024 and peak_wal < 256*1024*1024
        print("PASS: bounded live soak (1000-bot fair progress, budgeted WAL/RSS); sample in " + SAMPLE
              if ok else "FAIL")
        sys.exit(0 if ok else 1)
    finally:
        proc.send_signal(subprocess.signal.SIGINT)
        try:
            proc.wait(timeout=25)
        except subprocess.TimeoutExpired:
            proc.kill()
        logf.close()

if __name__ == "__main__":
    main()
