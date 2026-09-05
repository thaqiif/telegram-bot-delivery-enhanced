#!/usr/bin/env bash
# Smoke test for a telegram-bulk-delivery deployment with the API-key gate and
# default-safe SSRF gate enabled (the v0.2.0 feature set).
#
# Exercises live over HTTP:
#   1. API-key gate: no key -> 401, Bearer / X-TGBulk-Key -> 200, bad key -> 401,
#      and /healthz /readyz /metrics stay open unauthenticated.
#   2. Claim with a public test-env base (getMe skipped) -> 200.
#   3. SSRF gate default-safe: a private loopback base is rejected with 400.
#   4. Real bulk submit to N chats, poll to completion, read results.
#
# Usage:
#   BASE=http://HOST:8080 TOKEN=123:ABC API_KEY=sekret ./scripts/smoke.sh \
#       -c -1008001228039 -c -1008003100137 -c -5017329825 -c -1008001967399
#
# Env vars: BASE (default 127.0.0.1:8080), TOKEN (required), API_KEY.
# Options:
#   -c CHAT_ID   recipient chat id (repeatable; default: none -> gate-only mode)
#   -b BASE_URL  per-bot api base (default https://api.telegram.org/bot{token}/test)
#   --no-send    skip the final submit (gate + SSRF checks only)
#   --text S     message text (default a timestamped smoke marker)
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8080}"
TOKEN="${TOKEN:-}"
API_KEY="${API_KEY:-}"
PER_BOT_BASE="https://api.telegram.org/bot{token}/test"
CHATS=()
SEND=1
TEXT="[tgbulk] smoke $(date -u +%Y-%m-%dT%H:%M:%SZ)"

while [ $# -gt 0 ]; do
  case "$1" in
    -c) CHATS+=("$2"); shift 2;;
    -b) PER_BOT_BASE="$2"; shift 2;;
    --no-send) SEND=0; shift;;
    --text) TEXT="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 64;;
  esac
done

[ -n "$TOKEN" ] || { echo "error: TOKEN is required (env or arg)" >&2; exit 64; }
command -v curl >/dev/null || { echo "error: curl required" >&2; exit 64; }
command -v jq   >/dev/null || { echo "error: jq required" >&2; exit 64; }

PASS=0; FAIL=0
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
check() { # name expected actual
  if [ "$2" = "$3" ]; then printf '  ok   %-38s (HTTP %s)\n' "$1" "$3"; PASS=$((PASS+1));
  else printf '  FAIL %-38s expected=%s got=%s\n' "$1" "$2" "$3"; FAIL=$((FAIL+1)); fi
}
# code METHOD URI [DATA]
code() {
  local method="$1" uri="$2" data="${3:-}"
  local args=(-s -o /dev/null -w '%{http_code}' -m 30 -X "$method" "$BASE$uri")
  [ -n "$API_KEY" ] && args+=(-H "Authorization: Bearer $API_KEY")
  [ -n "$data" ] && args+=(-H 'Content-Type: application/json' -d "$data")
  curl "${args[@]}"
}
# reply METHOD URI [DATA]  -> prints JSON body
reply() {
  local method="$1" uri="$2" data="${3:-}"
  local args=(-s -m 30 -X "$method" "$BASE$uri")
  [ -n "$API_KEY" ] && args+=(-H "Authorization: Bearer $API_KEY")
  [ -n "$data" ] && args+=(-H 'Content-Type: application/json' -d "$data")
  curl "${args[@]}"
}

say "target $BASE | token ${TOKEN%%:*}:... | api-key ${API_KEY:+set}${API_KEY:-none}"

say "1. API-key gate"
check "no key -> 401" 401 "$(curl -s -o /dev/null -w '%{http_code}' -m 30 "$BASE/bot$TOKEN/getBulkDeliveryConfig")"
if [ -n "$API_KEY" ]; then
  check "Authorization: Bearer -> 200" 200 "$(code GET /bot$TOKEN/getBulkDeliveryConfig)"
  check "X-TGBulk-Key: -> 200" 200 "$(curl -s -o /dev/null -w '%{http_code}' -m 30 -H "X-TGBulk-Key: $API_KEY" "$BASE/bot$TOKEN/getBulkDeliveryConfig")"
  check "bad key -> 401" 401 "$(curl -s -o /dev/null -w '%{http_code}' -m 30 -H 'Authorization: Bearer wrong' "$BASE/bot$TOKEN/getBulkDeliveryConfig")"
fi
check "/healthz no key -> 200" 200 "$(curl -s -o /dev/null -w '%{http_code}' -m 30 "$BASE/healthz")"
check "/readyz  no key -> 200" 200 "$(curl -s -o /dev/null -w '%{http_code}' -m 30 "$BASE/readyz")"
check "/metrics no key -> 200" 200 "$(curl -s -o /dev/null -w '%{http_code}' -m 30 "$BASE/metrics")"

say "2. Claim with public per-bot base (getMe skipped) -> 200"
CLAIM=$(reply POST "/bot$TOKEN/setBulkDeliveryConfig" "{\"telegram_api_base\":\"$PER_BOT_BASE\",\"target_msgs_per_sec\":5}")
CLAIM_ST=$(echo "$CLAIM" | jq -r '.ok' 2>/dev/null || echo false)
check "claim .ok == true" true "$CLAIM_ST"

say "3. SSRF gate default-safe: private loopback base -> 400"
check "private base -> 400" 400 "$(code POST "/bot$TOKEN/setBulkDeliveryConfig" '{"telegram_api_base":"http://127.0.0.1:9123"}')"

if [ "$SEND" = "1" ] && [ "${#CHATS[@]}" -gt 0 ]; then
  say "4. Bulk submit to ${#CHATS[@]} chat(s)"
  RECIPS="$(printf '{"chat_id":%s},' "${CHATS[@]}" | sed 's/,$//')"
  BODY="{\"parameters\":{\"text\":$(printf '%s' "$TEXT" | jq -Rs .)},\"recipients\":[$RECIPS]}"
  RESP=$(reply POST "/bot$TOKEN/sendMessage" "$BODY")
  JOB=$(echo "$RESP" | jq -r '.result.job_id // empty' 2>/dev/null)
  TOTAL=$(echo "$RESP" | jq -r '.result.total // empty' 2>/dev/null)
  if [ -n "$JOB" ]; then
    echo "  queued job=$JOB total=$TOTAL"
  else
    echo "  submit had no job_id; response:"; echo "$RESP" | jq .; FAIL=$((FAIL+1))
    JOB=""
  fi
  if [ -n "$JOB" ]; then
    say "5. Poll job to completion (up to 90s)"
    STATE="queued"; I=0
    while [ "$STATE" != "completed" ] && [ "$STATE" != "failed" ] && [ $I -lt 30 ]; do
      J=$(reply GET "/bot$TOKEN/bulk/jobs/$JOB")
      STATE=$(echo "$J" | jq -r '.result.state // "unknown"' 2>/dev/null)
      I=$((I+1)); [ "$STATE" = "completed" ] || [ "$STATE" = "failed" ] || sleep 3
    done
    check "job terminal state" "completed" "$STATE"
    SUCC=$(echo "$J" | jq -r '.result.succeeded // 0' 2>/dev/null)
    FAILED=$(echo "$J" | jq -r '.result.failed // 0' 2>/dev/null)
    check "all $TOTAL succeeded" "$TOTAL" "$SUCC"
    check "zero failed" "0" "$FAILED"
    say "6. Results"
    reply GET "/bot$TOKEN/bulk/jobs/$JOB/results?limit=100" | jq '.result.items' 2>/dev/null || true
  fi
else
  echo; echo "  (skipped submit: --no-send or no -c chats given)"
fi

echo
echo "==============================================="
echo "smoke: $PASS ok, $FAIL failed"
echo "==============================================="
[ "$FAIL" -eq 0 ]
