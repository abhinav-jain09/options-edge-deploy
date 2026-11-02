#!/usr/bin/env bash
# assert-timestamp-type.sh <bootstrap> <topic> <confluent bin>
# The mirror preserves the SOURCE record timestamp, which the broker keeps only on a CreateTime topic; on LogAppendTime it is overwritten
# silently. Fail unless the target topic's effective message.timestamp.type is CreateTime (an unreadable answer is a failure).
set -euo pipefail
BS="${1:?bootstrap}"; TOPIC="${2:?topic}"; KBIN="${3:?confluent bin}"
out=$("${KAFKA_CONFIGS_CMD:-$KBIN/kafka-configs}" --bootstrap-server "$BS" --entity-type topics --entity-name "$TOPIC" --describe --all 2>/dev/null) \
  || { echo "FAIL: kafka-configs --describe failed for $TOPIC on $BS"; exit 1; }
v=$(printf '%s\n' "$out" | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^message\.timestamp\.type=/) { sub(/^message\.timestamp\.type=/, "", $i); print $i; exit } }')
[ "$v" = CreateTime ] || { echo "FAIL: $TOPIC on $BS has message.timestamp.type='${v:-unreadable}', not CreateTime: the source timestamp would not be preserved"; exit 1; }
echo "  $TOPIC on $BS: message.timestamp.type=CreateTime"
