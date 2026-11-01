#!/usr/bin/env bash
# strike-invasion-start-offsets.sh — the START-OFFSET GATE of the strike-invasion Avro cutover (docs/STRIKE-INVASION-AVRO-CUTOVER.md).
#
# reset-strike-invasion-state.sh deletes the Streams group, so the restarted service would begin at auto.offset.reset=latest — a
# position nothing has proven. This script removes the race and makes the position a verified fact:
#   pin    (service at 0, group absent/Empty, right after the reset script): capture the END offset of every partition of the
#          service's source topics, COMMIT exactly those offsets for the Streams group (kafka-consumer-groups --reset-offsets
#          --from-file ... --execute) and write them to $STATE_FILE.
#   verify (immediately BEFORE the service is enabled): the group must still be Empty, and for EVERY captured topic:partition the
#          committed offset must EQUAL the captured end offset. Anything else — a missing partition, a non-numeric offset, a
#          different number, a group that is not Empty — prints ABORT and exits 1: do NOT enable the service; re-run pin.
# Records the sources append between pin and the service start are consumed by the service (seconds of live evidence, which its
# 120 s freshness bound would admit anyway); nothing older than the pin can be replayed.
#
# Inputs: KAFKA_BOOTSTRAP_SERVERS, STRIKE_INVASION_APP_ID (…-dev | …-prod), STRIKE_INVASION_SOURCE_TOPICS (space separated; default
# the unprefixed service defaults — set it explicitly where TOPIC_PREFIX or *_INPUT_*_TOPIC overrides apply), STATE_FILE
# (default ~/.strike-invasion-start-offsets/$APP.offsets), DEPLOY_DRY_RUN, KAFKA_CONSUMER_GROUPS_CMD, KAFKA_GET_OFFSETS_CMD
# (default kafka-get-offsets; prints topic:partition:offset).
set -euo pipefail
MODE="${1:-}"
case "$MODE" in pin|verify) ;; *) echo "usage: $0 pin|verify" >&2; exit 2 ;; esac
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset}"
: "${STRIKE_INVASION_APP_ID:?STRIKE_INVASION_APP_ID unset}"
APP="$STRIKE_INVASION_APP_ID"
case "$APP" in options-edge-strike-invasion-service-dev|options-edge-strike-invasion-service-prod) ;;
  *) echo "FAIL: STRIKE_INVASION_APP_ID '$APP' is not options-edge-strike-invasion-service-dev|-prod"; exit 1 ;; esac
SOURCES="${STRIKE_INVASION_SOURCE_TOPICS:-underlying.spx.price options.spx.strike-sr.current options.databento.gex.strike delta-flow-by-strike option-price-behavior-by-strike options.databento.pace.mission strike-liquidity-heatmap-dashboard}"
STATE_FILE="${STATE_FILE:-$HOME/.strike-invasion-start-offsets/$APP.offsets}"
KCG="${KAFKA_CONSUMER_GROUPS_CMD:-kafka-consumer-groups}"
GO="${KAFKA_GET_OFFSETS_CMD:-kafka-get-offsets}"
DRY="${DEPLOY_DRY_RUN:-false}"

abort() { echo "ABORT: $* — do NOT enable strike-invasion; fix and re-run '$0 pin'."; exit 1; }

group_must_be_idle() {
  local rc=0 out
  out=$("$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --group "$APP" --state 2>&1) || rc=$?
  if printf '%s' "$out" | grep -qiE "Consumer group '$APP' does not exist"; then GSTATE=absent
  elif [ "$rc" -ne 0 ]; then abort "cannot describe group $APP (rc=$rc)"
  else GSTATE=$(printf '%s\n' "$out" | awk -v g="$APP" '$1 == g {print $(NF-1)}' | head -1); fi
  case "$GSTATE" in absent|Empty) ;; *) abort "group $APP is '${GSTATE:-unknown}', only absent/Empty may be pinned or verified" ;; esac
}

if [ "$MODE" = pin ]; then
  group_must_be_idle
  CSV=$(mktemp); : > "$STATE_FILE.new" 2>/dev/null || { mkdir -p "$(dirname "$STATE_FILE")"; : > "$STATE_FILE.new"; }
  for t in $SOURCES; do
    rc=0; out=$("$GO" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic "$t" --time -1 2>&1) || rc=$?
    [ "$rc" -eq 0 ] || abort "cannot read end offsets of $t (rc=$rc): $out"
    n=0
    while IFS=: read -r topic part off; do
      [ "$topic" = "$t" ] || continue
      case "$part$off" in ''|*[!0-9]*) abort "non-numeric end offset row '$topic:$part:$off'" ;; esac
      printf '%s,%s,%s\n' "$topic" "$part" "$off" >> "$CSV"; printf '%s:%s:%s\n' "$topic" "$part" "$off" >> "$STATE_FILE.new"; n=$((n + 1))
    done <<< "$out"
    [ "$n" -gt 0 ] || abort "no partitions returned for $t"
  done
  if [ "$DRY" = true ]; then echo "DEPLOY_DRY_RUN=true — would commit:"; cat "$STATE_FILE.new"; rm -f "$STATE_FILE.new" "$CSV"; exit 0; fi
  "$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --group "$APP" --reset-offsets --from-file "$CSV" --execute \
    || abort "committing the captured end offsets failed"
  mv "$STATE_FILE.new" "$STATE_FILE"; rm -f "$CSV"
  echo "pinned $(wc -l < "$STATE_FILE" | tr -d ' ') partitions for $APP -> $STATE_FILE; now run: $0 verify"
  exit 0
fi

# verify
[ -s "$STATE_FILE" ] || abort "no pinned offsets at $STATE_FILE"
group_must_be_idle
[ "$GSTATE" = Empty ] || abort "group $APP is absent: the pin did not take effect (a deleted group restarts at 'latest')"
rc=0; DESC=$("$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --group "$APP" --offsets 2>&1) || rc=$?
[ "$rc" -eq 0 ] || abort "cannot describe offsets of $APP (rc=$rc)"
BAD=0
while IFS=: read -r topic part want; do
  got=$(printf '%s\n' "$DESC" | awk -v g="$APP" -v t="$topic" -v p="$part" '$1 == g && $2 == t && $3 == p {print $4; exit}')
  case "$got" in ''|*[!0-9]*) echo "MISMATCH $topic:$part committed='${got:-missing}' pinned=$want"; BAD=$((BAD + 1)); continue ;; esac
  [ "$got" = "$want" ] || { echo "MISMATCH $topic:$part committed=$got pinned=$want"; BAD=$((BAD + 1)); }
done < "$STATE_FILE"
[ "$BAD" -eq 0 ] || abort "$BAD partition(s) not at the pinned end offset"
echo "=== start-offset gate OK: every one of $(wc -l < "$STATE_FILE" | tr -d ' ') source partitions is committed at its pinned end offset ==="
