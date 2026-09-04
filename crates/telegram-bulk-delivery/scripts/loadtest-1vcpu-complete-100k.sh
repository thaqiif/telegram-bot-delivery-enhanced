#!/usr/bin/env bash
# Separate long completion verifier. At 25 sends/s, 100k needs 4000s minimum;
# default budget is 4500s (75 min). This script polls an already-submitted job.
set -euo pipefail
BASE_URL=${BASE_URL:-http://127.0.0.1:8080}
BOT_TOKEN=${BOT_TOKEN:?set BOT_TOKEN}
JOB_ID=${JOB_ID:?set JOB_ID for a 100000-recipient job}
TIMEOUT_SECS=${TIMEOUT_SECS:-4500}
[[ "$TIMEOUT_SECS" -ge 4200 ]] || { echo "FAIL: budget must be >=70min" >&2; exit 2; }

deadline=$((SECONDS + TIMEOUT_SECS))
while (( SECONDS < deadline )); do
  status=$(curl -fsS --max-time 10 "$BASE_URL/bot$BOT_TOKEN/bulk/jobs/$JOB_ID")
  total=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["total"])' <<<"$status")
  state=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["state"])' <<<"$status")
  [[ "$total" == 100000 ]] || { echo "FAIL: job total is $total, expected 100000" >&2; exit 2; }
  if [[ "$state" == completed ]]; then
    echo "PASS: 100000-recipient job completed in $SECONDS seconds"
    exit 0
  fi
  sleep 10
done
echo "FAIL: job did not complete within ${TIMEOUT_SECS}s" >&2
exit 1
