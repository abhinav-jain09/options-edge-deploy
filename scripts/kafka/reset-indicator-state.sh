#!/usr/bin/env bash
# reset-indicator-state.sh — the state rotation of the indicator-service Avro cutover (INDICATOR-AVRO-CONVERSION-PLAN.md
# section 1, "State rotation at deploy"), run while indicator-service is scaled to 0 and BEFORE its topics are recreated:
#
#   1. the Streams application's consumer group must be EMPTY (no member): a running instance would re-commit
#      offsets behind this script's back;
#   2. its committed offsets on every retained input partition are reset to 0 and read back — the retained input
#      is the re-warm source, and a resumed committed offset could never rebuild the destroyed state;
#   3. the changelog <app id>-indicator-state-changelog is DELETED (Streams recreates it at the declared shape on
#      the first start; ensure-indicator-topics.sh accepts its absence) — a purge to the high-watermark would keep
#      a JSON-era compacted tail reachable through a restore, deletion does not.
#
# Inputs: KAFKA_BOOTSTRAP_SERVERS, INDICATOR_APP_ID (e.g. indicator-service-dev), INDICATOR_INPUT_TOPIC
# (e.g. underlying.spx.price, or es.underlying.es.trades on es4), DEPLOY_DRY_RUN.
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset}"
: "${INDICATOR_APP_ID:?INDICATOR_APP_ID unset — the Streams application id names the group and the changelog}"
: "${INDICATOR_INPUT_TOPIC:?INDICATOR_INPUT_TOPIC unset — the retained input whose offsets reset to 0}"
DRY="${DEPLOY_DRY_RUN:-false}"
KCG="${KAFKA_CONSUMER_GROUPS_CMD:-kafka-consumer-groups}"
KT="${KAFKA_TOPICS_CMD:-kafka-topics}"
GROUP="$INDICATOR_APP_ID"
CHANGELOG="${INDICATOR_APP_ID}-indicator-state-changelog"
echo "=== reset-indicator-state: group=$GROUP input=$INDICATOR_INPUT_TOPIC changelog=$CHANGELOG on $KAFKA_BOOTSTRAP_SERVERS (dry_run=$DRY) ==="

# 1. the group must be ABSENT or Empty — an explicit allow-list; any other state (Stable, a rebalance, Dead,
#    an unparseable answer, a failed describe) refuses, because a member could commit behind the reset.
DRC=0; DESC=$("$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --group "$GROUP" --state 2>&1) || DRC=$?
if printf '%s' "$DESC" | grep -qiE "Consumer group '$GROUP' does not exist"; then
  STATE=absent
elif [ "$DRC" -ne 0 ]; then
  STATE="describe-failed(rc=$DRC)"
else
  STATE=$(printf '%s\n' "$DESC" | awk -v g="$GROUP" '$1 == g {print $(NF-1)}' | head -1)
fi
case "$STATE" in
  absent|Empty) echo "group $GROUP: $STATE" ;;
  *) echo "FAIL: consumer group $GROUP is '${STATE:-unknown}' — only an absent or Empty group may be reset (scale indicator-service to 0 first):"; printf '%s\n' "$DESC"; exit 1 ;;
esac
# the retained input's partitions, from broker metadata: the read-back below must name every one of them
PARTS=$("$KT" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$INDICATOR_INPUT_TOPIC" 2>/dev/null \
  | awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}')
case "$PARTS" in ''|*[!0-9]*|0) echo "FAIL: cannot read the partition count of $INDICATOR_INPUT_TOPIC (got '$PARTS')"; exit 1 ;; esac
echo "input $INDICATOR_INPUT_TOPIC: $PARTS partition(s)"

if [ "$DRY" = true ]; then
  echo "DEPLOY_DRY_RUN=true — would reset $GROUP to offset 0 on $INDICATOR_INPUT_TOPIC and delete $CHANGELOG"
  exit 0
fi

# 2. offsets to 0 on every partition of the retained input, then read back
"$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --group "$GROUP" --topic "$INDICATOR_INPUT_TOPIC" \
  --reset-offsets --to-earliest --execute
AFTER=$("$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --group "$GROUP" --offsets 2>&1) \
  || { echo "FAIL: could not read the offsets of $GROUP back after the reset:"; printf '%s\n' "$AFTER"; exit 1; }
# exactly one committed row per partition 0..PARTS-1, each at offset 0 — a missing or extra row fails
ROWS=$(printf '%s\n' "$AFTER" | awk -v t="$INDICATOR_INPUT_TOPIC" '$2 == t {print $3"="$4}' | sort -t= -k1,1n | tr '\n' ' ')
WANT=$(for ((p = 0; p < PARTS; p++)); do printf '%s=0 ' "$p"; done)
if [ "$ROWS" != "$WANT" ]; then echo "FAIL: offsets of $GROUP on $INDICATOR_INPUT_TOPIC after the reset are [$ROWS], expected [$WANT]"; exit 1; fi
echo "offsets of $GROUP on $INDICATOR_INPUT_TOPIC: 0 on every one of the $PARTS partitions"

# 3. the changelog goes away entirely
if "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list 2>/dev/null | grep -qxF "$CHANGELOG"; then
  "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$CHANGELOG"
  for _ in $(seq 1 "${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"); do
    "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list 2>/dev/null | grep -qxF "$CHANGELOG" || break
    sleep 1
  done
  if "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list 2>/dev/null | grep -qxF "$CHANGELOG"; then
    echo "FAIL: $CHANGELOG still present after delete"; exit 1
  fi
  echo "changelog $CHANGELOG deleted"
else
  echo "changelog $CHANGELOG absent"
fi
echo "=== reset-indicator-state: OK ==="
