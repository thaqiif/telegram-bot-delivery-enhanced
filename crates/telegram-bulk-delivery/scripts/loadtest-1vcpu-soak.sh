#!/usr/bin/env bash
# 30-minute constrained soak verifier. This script does not create a cgroup: run
# it inside systemd MemoryMax=1G/CPUQuota=100%, Docker --cpus=1 --memory=1g, or an
# equivalent cgroup v2 scope. Refuses to claim proof when limits are absent.
set -euo pipefail

DURATION_SECS=${DURATION_SECS:-1800}
BASE_URL=${BASE_URL:-http://127.0.0.1:8080}
PID=${PID:-}
OUT=${OUT:-loadtest-1vcpu-soak.tsv}
MAX_MEMORY=$((768 * 1024 * 1024))
MAX_WAL=$((256 * 1024 * 1024))

[[ -r /sys/fs/cgroup/memory.max && -r /sys/fs/cgroup/cpu.max ]] || {
  echo "FAIL: cgroup v2 cpu.max and memory.max are required" >&2; exit 2;
}
mem_max=$(cat /sys/fs/cgroup/memory.max)
read -r quota period < /sys/fs/cgroup/cpu.max
[[ "$mem_max" != max && "$mem_max" -le 1073741824 ]] || {
  echo "FAIL: memory.max must be <= 1 GiB (got $mem_max)" >&2; exit 2;
}
[[ "$quota" != max && "$quota" -le "$period" ]] || {
  echo "FAIL: cpu.max must constrain to <= 1 CPU (got $quota $period)" >&2; exit 2;
}

printf 'unix\tmemory_current\trss\twal\twriter_queue\tready_bots\n' > "$OUT"
deadline=$((SECONDS + DURATION_SECS))
while (( SECONDS < deadline )); do
  metrics=$(curl -fsS --max-time 5 "$BASE_URL/metrics")
  memory=$(cat /sys/fs/cgroup/memory.current)
  rss=0
  if [[ -n "$PID" && -r "/proc/$PID/statm" ]]; then
    rss=$(( $(awk '{print $2}' "/proc/$PID/statm") * $(getconf PAGESIZE) ))
  else
    rss=$(awk '$1=="process_rss_bytes" {print int($2)}' <<<"$metrics")
  fi
  wal=$(awk '$1=="sqlite_wal_bytes" {print int($2)}' <<<"$metrics")
  writer=$(awk '$1=="writer_queue_depth" {print int($2)}' <<<"$metrics")
  ready=$(awk '$1=="ready_bots" {print int($2)}' <<<"$metrics")
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "$memory" "$rss" "$wal" "$writer" "$ready" >> "$OUT"
  (( memory < MAX_MEMORY && rss < MAX_MEMORY )) || { echo "FAIL: memory >= 768 MiB" >&2; exit 1; }
  (( wal < MAX_WAL )) || { echo "FAIL: WAL >= 256 MiB" >&2; exit 1; }
  (( writer < 128 )) || { echo "FAIL: writer queue reached capacity" >&2; exit 1; }
  sleep 5
done

# The fixture/driver must expose its grant audit alongside this script. We do
# not infer fairness from aggregate ready_bots: pass only with an explicit
# newline-delimited bot_id/grant_count file where every ready bot has >=1 grant.
FAIRNESS_FILE=${FAIRNESS_FILE:-}
[[ -n "$FAIRNESS_FILE" && -s "$FAIRNESS_FILE" ]] || {
  echo "FAIL: FAIRNESS_FILE is required for 1,000-bot progress proof" >&2; exit 2;
}
count=$(awk '$2>=1 {n++} END {print n+0}' "$FAIRNESS_FILE")
[[ "$count" -ge 1000 ]] || { echo "FAIL: only $count/1000 bots made progress" >&2; exit 1; }
echo "PASS: ${DURATION_SECS}s constrained soak; samples=$OUT; fair bots=$count"
