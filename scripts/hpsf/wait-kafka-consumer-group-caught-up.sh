#!/usr/bin/env bash
set -euo pipefail

GROUP_ID="${1:?consumer group id is required}"
LOG_FILE="${2:-artifacts/logs/${GROUP_ID}.catchup.log}"
ATTEMPTS="${HPSF_REPLAY_GROUP_CATCHUP_ATTEMPTS:-240}"
SLEEP_SECONDS="${HPSF_REPLAY_GROUP_CATCHUP_SLEEP_SECONDS:-2}"

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"

mkdir -p "$(dirname "$LOG_FILE")"

for attempt in $(seq 1 "$ATTEMPTS"); do
  set +e
  kafka-consumer-groups \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --describe \
    --group "$GROUP_ID" > "${LOG_FILE}.tmp" 2>&1
  status=$?
  set -e
  cp "${LOG_FILE}.tmp" "$LOG_FILE"

  if [ "$status" -eq 0 ]; then
    if python3 - "$LOG_FILE" <<'PY'
import sys
from pathlib import Path

rows = 0
total_lag = 0
empty_rows = 0
for raw in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    parts = raw.split()
    if len(parts) < 6 or parts[0] == "GROUP":
        continue
    current_offset = parts[3]
    log_end_offset = parts[4]
    lag_text = parts[5]
    if current_offset == "-" and log_end_offset == "0" and lag_text == "-":
        empty_rows += 1
        continue
    if not lag_text.isdigit():
        continue
    rows += 1
    total_lag += int(lag_text)

print(f"rows={rows} emptyRows={empty_rows} totalLag={total_lag}")
if rows == 0 and empty_rows == 0:
    raise SystemExit(2)
raise SystemExit(0 if total_lag == 0 else 1)
PY
    then
      echo "Consumer group $GROUP_ID caught up on attempt $attempt/$ATTEMPTS" | tee -a "$LOG_FILE"
      exit 0
    fi
  fi

  echo "Consumer group $GROUP_ID not caught up on attempt $attempt/$ATTEMPTS" | tee -a "$LOG_FILE"
  if [ "$attempt" != "$ATTEMPTS" ]; then
    sleep "$SLEEP_SECONDS"
  fi
done

echo "Consumer group $GROUP_ID did not catch up after $ATTEMPTS attempts" >&2
exit 1
