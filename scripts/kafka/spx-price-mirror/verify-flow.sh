#!/usr/bin/env bash
# verify-flow.sh <src bootstrap> <src registry> <src topic> <tgt bootstrap> <tgt registry> <tgt topic> <group> <confluent bin>
# Proves the MIRROR moves data, not merely that the target offset advanced (another writer would advance it too):
#   * the mirror's consumer group holds committed offsets on the SOURCE topic and its lag is within MAX_LAG (default 1000);
#   * while the source advanced over FLOW_WAIT_S (default 30) the group's committed offsets advanced too;
#   * the source record just BEFORE the group's committed position (the last one the mirror flushed) appears on the target
#     with the same key, the same timestamp and the same decoded value (searched in the newest TAIL=500 records of every target partition).
# Exit 0 FLOWING, 3 INCONCLUSIVE (source did not advance: market closed), 1 FAIL.
set -euo pipefail
SB="${1:?}"; SR="${2:?}"; ST="${3:?}"; TB="${4:?}"; TR="${5:?}"; TT="${6:?}"; GROUP="${7:?}"; KBIN="${8:?}"
MAX_LAG="${MAX_LAG:-1000}"; WAIT="${FLOW_WAIT_S:-30}"; TAIL="${TAIL:-500}"
GO="${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}"; CG="${KAFKA_CONSUMER_GROUPS_CMD:-$KBIN/kafka-consumer-groups}"
AC="${KAFKA_AVRO_CONSOLE_CONSUMER_CMD:-$KBIN/kafka-avro-console-consumer}"
tmp=$(mktemp); trap 'rm -f "$tmp" "$tmp.t"' EXIT
ends() { "$GO" --bootstrap-server "$1" --topic "$2" 2>/dev/null | awk -F: -v t="$2" '$1 == t && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ { s += $3; n++ } END { if (n == 0) exit 3; print s }'; }
# prints "<sum committed> <sum lag> <partition:committed ...>"
group() {
  out=$("$CG" --bootstrap-server "$SB" --describe --group "$GROUP" --offsets 2>/dev/null) || { echo "FAIL: cannot describe group $GROUP" >&2; return 1; }
  printf '%s\n' "$out" | awk -v g="$GROUP" -v t="$ST" '$1 == g && $2 == t && $4 ~ /^[0-9]+$/ { c += $4; l += ($6 ~ /^[0-9]+$/ ? $6 : 0); n++; m = m " " $3 ":" $4 } END { if (n == 0) exit 3; print c, l, m }'
}
G0=$(group) || G0="-1 0"   # a brand-new group on a flat source has committed nothing yet
C0=${G0%% *}
E0=$(ends "$SB" "$ST") || { echo "FAIL: cannot read end offsets of $ST"; exit 1; }
sleep "$WAIT"
E1=$(ends "$SB" "$ST") || { echo "FAIL: cannot read end offsets of $ST"; exit 1; }
if ! G1=$(group); then
  [ "$E1" -gt "$E0" ] && { echo "FAIL: the source advanced but group $GROUP holds no committed offset: the mirror is not consuming"; exit 1; }
  echo "  INCONCLUSIVE: group $GROUP has committed nothing and the source did not advance (market closed)"; exit 3
fi
read -r C1 LAG PARTS <<< "$G1"
echo "  source end $E0 -> $E1; group $GROUP committed $C0 -> $C1, lag $LAG"
[ "$LAG" -le "$MAX_LAG" ] || { echo "FAIL: group $GROUP lag $LAG exceeds $MAX_LAG: the mirror is not keeping up"; exit 1; }
moving=0
if [ "$E1" -gt "$E0" ]; then
  [ "$C1" -gt "$C0" ] || { echo "FAIL: the source advanced but group $GROUP did not: the mirror is not consuming"; exit 1; }
  moving=1
fi
# the last record the mirror flushed: committed - 1 on a partition where the group has consumed something
found=0; tried=0
for pc in $PARTS; do
  p=${pc%%:*}; c=${pc##*:}
  [ "$c" -ge 1 ] || continue
  tried=$((tried + 1))
  "$AC" --bootstrap-server "$SB" --topic "$ST" --partition "$p" --offset $((c - 1)) --max-messages 1 --timeout-ms 20000 \
    --property schema.registry.url="$SR" --property print.key=true --property print.timestamp=true > "$tmp" 2>/dev/null || true
  line=$(head -n 1 "$tmp")
  [ -n "$line" ] || { echo "FAIL: cannot read source $ST-$p@$((c - 1)) (compacted away or unreadable)"; exit 1; }
  : > "$tmp.t"
  while IFS=: read -r t tp end; do
    [ "$t" = "$TT" ] || continue
    from=$((end - TAIL)); [ "$from" -ge 0 ] || from=0
    "$AC" --bootstrap-server "$TB" --topic "$TT" --partition "$tp" --offset "$from" --max-messages "$TAIL" --timeout-ms 15000 \
      --property schema.registry.url="$TR" --property print.key=true --property print.timestamp=true >> "$tmp.t" 2>/dev/null || true
  done <<< "$("$GO" --bootstrap-server "$TB" --topic "$TT" 2>/dev/null)"
  if grep -qxF -- "$line" "$tmp.t"; then found=$((found + 1)); echo "  $ST-$p@$((c - 1)) found on the target with the same key, timestamp and decoded value"
  else echo "FAIL: the mirror's last flushed record ($ST-$p@$((c - 1))) is not on the target with the same key, timestamp and value"; exit 1; fi
done
[ "$tried" -ge 1 ] || { echo "FAIL: the group has consumed nothing yet (committed 0 everywhere)"; exit 1; }
[ "$moving" -eq 1 ] && { echo "  MIRROR FLOWING"; exit 0; }
echo "  INCONCLUSIVE: the source did not advance in ${WAIT}s (market closed); earlier records are on the target but live movement is not proven"; exit 3
