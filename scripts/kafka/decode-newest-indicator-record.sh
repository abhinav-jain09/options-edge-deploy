#!/usr/bin/env bash
# decode-newest-indicator-record.sh <bootstrap> <registry url> <topic> <confluent bin>
# The mirror's decode gate (INDICATOR-AVRO-CONVERSION-PLAN.md section 6 step 8): on every partition of the topic
# the newest COMMITTED record is Confluent-framed Avro whose schema id (2001-2007) resolves on the given registry
# and whose value deserializes under that registry's schema for the id.
# Framing is checked from the bytes themselves (magic byte 0, big-endian id), never from a formatter's text.
#
# 2026-10-18: two defects found on the first live topic (es4 snapshot.current, build #45 era) —
#   (1) `--formatter-property` is not an option of Confluent 7.3.1's console consumer (Kafka 3.3; it arrived in
#       3.7), so the consumer exited with a usage error, the frame read empty and the gate failed every
#       non-empty partition while passing every empty one;
#   (2) `end - 1` is a transaction commit MARKER on a topic written by an exactly-once producer (indicator-service
#       on es4), never a record, so a consumer parked there waits for the next record instead of reading one.
# Now: the consumer starts at max(earliest, end - 64) with read_committed (markers are never returned) and takes
# the first committed record it meets — the newest within the last 64 offsets of a 1-record-per-transaction
# topic, a recent record on a batched one — and fails loud when it meets none within the timeout.
set -euo pipefail
BS="${1:?bootstrap}"; REG="${2:?registry}"; TOPIC="${3:?topic}"; KBIN="${4:?confluent bin}"
fail=0
# the partition count from broker metadata, and one end-offset row for every partition 0..N-1 exactly once —
# an empty, short or malformed kafka-get-offsets answer is a failure, never "nothing to decode"
PARTS=$("${KAFKA_TOPICS_CMD:-$KBIN/kafka-topics}" --bootstrap-server "$BS" --describe --topic "$TOPIC" 2>/dev/null \
  | awk '!done { for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); done = 1 } }')
case "$PARTS" in ''|*[!0-9]*|0) echo "FAIL: cannot read the partition count of $TOPIC on $BS (got '$PARTS')"; exit 1 ;; esac
OFFSETS=$("${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$BS" --topic "$TOPIC" 2>/dev/null) \
  || { echo "FAIL: kafka-get-offsets failed for $TOPIC on $BS"; exit 1; }
BEGINS=$("${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$BS" --topic "$TOPIC" --time -2 2>/dev/null) \
  || { echo "FAIL: kafka-get-offsets --time -2 failed for $TOPIC on $BS"; exit 1; }
ROWS=$(printf '%s\n' "$OFFSETS" | awk -F: -v t="$TOPIC" '$1 == t && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ {print $2}' | sort -n | tr '\n' ' ')
WANT=$(for ((p = 0; p < PARTS; p++)); do printf '%s ' "$p"; done)
[ "$ROWS" = "$WANT" ] || { echo "FAIL: $TOPIC has $PARTS partition(s) but kafka-get-offsets answered for [${ROWS% }] — every partition must be gated"; exit 1; }
echo "  $TOPIC: $PARTS partition(s), one end offset each"
while IFS=: read -r t p end; do
  [ "$t" = "$TOPIC" ] || continue
  begin=$(printf '%s\n' "$BEGINS" | awk -F: -v t="$TOPIC" -v p="$p" '$1 == t && $2 == p {print $3}')
  case "$begin" in ''|*[!0-9]*) echo "FAIL: cannot read the earliest offset of $TOPIC-$p"; fail=1; continue ;; esac
  [ "$end" -gt "$begin" ] || { echo "  $TOPIC-$p: empty"; continue; }
  from=$((end - 64)); [ "$from" -ge "$begin" ] || from=$begin
  # the first committed record at or after `from`: raw value bytes, no formatter decoration; the hex of the
  # first five bytes is the Confluent frame. read_committed: transaction markers are never handed out, and
  # auto.offset.reset=none makes an earliest offset the broker no longer holds an error, not a silent reset.
  hex=$(timeout 60 "$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$TOPIC" --partition "$p" --offset "$from" --max-messages 1 \
          --isolation-level read_committed --consumer-property auto.offset.reset=none \
          --property print.key=false --property print.timestamp=false --timeout-ms 20000 2>/dev/null \
          | head -c 5 | xxd -p | tr -d '\n')
  [ ${#hex} -eq 10 ] || { echo "FAIL: $TOPIC-$p from offset $from: no committed record read within the timeout (frame '$hex')"; fail=1; continue; }
  magic=${hex:0:2}; id=$((16#${hex:2:8}))
  if [ "$magic" != "00" ]; then echo "FAIL: $TOPIC-$p from offset $from: no Confluent magic byte (frame $hex)"; fail=1; continue; fi
  case "$id" in 2001|2002|2003|2004|2005|2006|2007) ;; *) echo "FAIL: $TOPIC-$p from offset $from: schema id $id is not one of the family's fixed ids"; fail=1; continue ;; esac
  code=$(curl -sS -m 10 -o /dev/null -w '%{http_code}' "${REG%/}/schemas/ids/$id")
  [ "$code" = 200 ] || { echo "FAIL: $TOPIC-$p from offset $from: schema id $id does not resolve on $REG (HTTP $code)"; fail=1; continue; }
  # and the value DESERIALIZES under the target registry's schema for that id: the Confluent Avro console
  # consumer prints the decoded record, or prints nothing and exits non-zero on a deserialization error
  if ! decoded=$(timeout 90 "${KAFKA_AVRO_CONSOLE_CONSUMER_CMD:-$KBIN/kafka-avro-console-consumer}" --bootstrap-server "$BS" --topic "$TOPIC" \
        --partition "$p" --offset "$from" --max-messages 1 --isolation-level read_committed --consumer-property auto.offset.reset=none \
        --timeout-ms 30000 --property schema.registry.url="$REG" 2>/dev/null) \
     || [ -z "$decoded" ]; then
    echo "FAIL: $TOPIC-$p from offset $from: the value does not deserialize under $REG (schema id $id)"; fail=1; continue
  fi
  echo "  $TOPIC-$p from offset $from: Avro, schema id $id resolves on $REG and the value deserializes"
done <<< "$OFFSETS"
[ "$fail" -eq 0 ] || exit 1
