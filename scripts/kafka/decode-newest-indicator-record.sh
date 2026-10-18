#!/usr/bin/env bash
# decode-newest-indicator-record.sh <bootstrap> <registry url> <topic> <confluent bin>
# The mirror's decode gate (INDICATOR-AVRO-CONVERSION-PLAN.md section 6 step 8): on every partition of the topic
# the NEWEST COMMITTED record is Confluent-framed Avro whose schema id (2001-2007) resolves on the given registry
# and whose value deserializes under that registry's schema for the id.
# Framing is checked from the bytes themselves (magic byte 0, big-endian id), never from a formatter's text.
#
# 2026-10-18: two defects found on the first live topic (es4 snapshot.current, build #45 era) —
#   (1) `--formatter-property` is not an option of Confluent 7.3.1's console consumer (Kafka 3.3; it arrived in
#       3.7), so the consumer exited with a usage error, the frame read empty and the gate failed every
#       non-empty partition while passing every empty one;
#   (2) `end - 1` is a transaction commit MARKER on a topic written by an exactly-once producer (indicator-service
#       on es4), never a record, so a consumer parked there waits for the next record instead of reading one.
# Now, per partition: (a) a read_committed consumer lists the offsets of the committed records in the window
# [max(earliest, end - 64), end) — markers are never returned — and the LAST listed offset is the newest committed
# record; (b) that one record is read raw for its frame and (c) deserialized by the Avro console consumer.
# An empty partition is a FAIL unless DECODE_EMPTY_OK (space-separated partition numbers) names it: the caller
# (the install's parity gate) names exactly the partitions whose SOURCE holds zero committed records, so an
# empty target partition passes only when there was nothing to copy. auto.offset.reset=none turns an earliest
# offset the broker no longer holds into an error instead of a silent reset to the live edge.
set -euo pipefail
BS="${1:?bootstrap}"; REG="${2:?registry}"; TOPIC="${3:?topic}"; KBIN="${4:?confluent bin}"
EMPTY_OK=" ${DECODE_EMPTY_OK:-} "
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
CC=(--isolation-level read_committed --consumer-property auto.offset.reset=none)
while IFS=: read -r t p end; do
  [ "$t" = "$TOPIC" ] || continue
  begin=$(printf '%s\n' "$BEGINS" | awk -F: -v t="$TOPIC" -v p="$p" '$1 == t && $2 == p {print $3}')
  case "$begin" in ''|*[!0-9]*) echo "FAIL: cannot read the earliest offset of $TOPIC-$p"; fail=1; continue ;; esac
  if [ "$end" -le "$begin" ]; then
    case "$EMPTY_OK" in *" $p "*) echo "  $TOPIC-$p: empty, and its source holds no committed record (allowed by the caller)"; continue ;; esac
    echo "FAIL: $TOPIC-$p is empty but the caller did not declare its source empty (DECODE_EMPTY_OK='${DECODE_EMPTY_OK:-}')"; fail=1; continue
  fi
  from=$((end - 64)); [ "$from" -ge "$begin" ] || from=$begin
  # (a) the committed records of the window, offsets only (a text line per record, never value bytes)
  # On a live topic the window holds fewer than 64 committed records (markers take the other offsets) and the
  # consumer never idles, so the 60 s bound ends the listing; the newest offset listed by then is the answer
  # and the bound itself is not a failure (`|| true` keeps pipefail from turning the kill into an abort).
  last=$({ timeout 60 "$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$TOPIC" --partition "$p" --offset "$from" --max-messages 64 \
          "${CC[@]}" --property print.offset=true --property print.key=false --property print.value=false --timeout-ms 20000 2>/dev/null || true; } \
          | awk -F'[:\t]' '$1 == "Offset" && $2 ~ /^[0-9]+$/ {o = $2} END {print o}')
  case "$last" in ''|*[!0-9]*) echo "FAIL: $TOPIC-$p: no committed record read in [$from, $end) within the timeout"; fail=1; continue ;; esac
  # (b) the newest committed record, raw value bytes: the hex of the first five bytes is the Confluent frame
  hex=$(timeout 60 "$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$TOPIC" --partition "$p" --offset "$last" --max-messages 1 \
          "${CC[@]}" --property print.key=false --property print.timestamp=false --timeout-ms 20000 2>/dev/null \
          | head -c 5 | xxd -p | tr -d '\n')
  [ ${#hex} -eq 10 ] || { echo "FAIL: $TOPIC-$p offset $last: the newest committed record could not be read (frame '$hex')"; fail=1; continue; }
  magic=${hex:0:2}; id=$((16#${hex:2:8}))
  if [ "$magic" != "00" ]; then echo "FAIL: $TOPIC-$p offset $last: no Confluent magic byte (frame $hex)"; fail=1; continue; fi
  case "$id" in 2001|2002|2003|2004|2005|2006|2007) ;; *) echo "FAIL: $TOPIC-$p offset $last: schema id $id is not one of the family's fixed ids"; fail=1; continue ;; esac
  code=$(curl -sS -m 10 -o /dev/null -w '%{http_code}' "${REG%/}/schemas/ids/$id")
  [ "$code" = 200 ] || { echo "FAIL: $TOPIC-$p offset $last: schema id $id does not resolve on $REG (HTTP $code)"; fail=1; continue; }
  # (c) and the value DESERIALIZES under the target registry's schema for that id: the Confluent Avro console
  # consumer prints the decoded record, or prints nothing and exits non-zero on a deserialization error
  if ! decoded=$(timeout 90 "${KAFKA_AVRO_CONSOLE_CONSUMER_CMD:-$KBIN/kafka-avro-console-consumer}" --bootstrap-server "$BS" --topic "$TOPIC" \
        --partition "$p" --offset "$last" --max-messages 1 "${CC[@]}" --timeout-ms 30000 --property schema.registry.url="$REG" 2>/dev/null) \
     || [ -z "$decoded" ]; then
    echo "FAIL: $TOPIC-$p offset $last: the value does not deserialize under $REG (schema id $id)"; fail=1; continue
  fi
  echo "  $TOPIC-$p offset $last (newest committed): Avro, schema id $id resolves on $REG and the value deserializes"
done <<< "$OFFSETS"
[ "$fail" -eq 0 ] || exit 1
