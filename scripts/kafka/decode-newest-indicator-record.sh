#!/usr/bin/env bash
# decode-newest-indicator-record.sh <bootstrap> <registry url> <topic> <confluent bin>
# The mirror's decode gate (INDICATOR-AVRO-CONVERSION-PLAN.md section 6 step 8): the newest record on every
# partition of the topic is Confluent-framed Avro whose schema id (2001-2007) resolves on the given registry.
# Framing is checked from the bytes themselves (magic byte 0, big-endian id), never from a formatter's text.
set -euo pipefail
BS="${1:?bootstrap}"; REG="${2:?registry}"; TOPIC="${3:?topic}"; KBIN="${4:?confluent bin}"
fail=0
while IFS=: read -r t p end; do
  [ "$t" = "$TOPIC" ] || continue
  [ "$end" -gt 0 ] || { echo "  $TOPIC-$p: empty"; continue; }
  from=$((end - 1))
  # one record, raw value bytes, no formatter decoration: hex of the first five bytes is the frame
  hex=$(timeout 60 "$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$TOPIC" --partition "$p" --offset "$from" --max-messages 1 \
          --formatter-property print.key=false --formatter-property print.timestamp=false --timeout-ms 20000 2>/dev/null \
          | head -c 5 | xxd -p | tr -d '\n')
  magic=${hex:0:2}; id=$((16#${hex:2:8}))
  if [ "$magic" != "00" ]; then echo "FAIL: $TOPIC-$p offset $from: no Confluent magic byte (frame $hex)"; fail=1; continue; fi
  case "$id" in 2001|2002|2003|2004|2005|2006|2007) ;; *) echo "FAIL: $TOPIC-$p offset $from: schema id $id is not one of the family's fixed ids"; fail=1; continue ;; esac
  code=$(curl -sS -m 10 -o /dev/null -w '%{http_code}' "${REG%/}/schemas/ids/$id")
  [ "$code" = 200 ] || { echo "FAIL: $TOPIC-$p offset $from: schema id $id does not resolve on $REG (HTTP $code)"; fail=1; continue; }
  # and the value DESERIALIZES under the target registry's schema for that id: the Confluent Avro console
  # consumer prints the decoded record, or prints nothing and exits non-zero on a deserialization error
  if ! decoded=$(timeout 90 "${KAFKA_AVRO_CONSOLE_CONSUMER_CMD:-$KBIN/kafka-avro-console-consumer}" --bootstrap-server "$BS" --topic "$TOPIC" \
        --partition "$p" --offset "$from" --max-messages 1 --timeout-ms 30000 --property schema.registry.url="$REG" 2>/dev/null) \
     || [ -z "$decoded" ]; then
    echo "FAIL: $TOPIC-$p offset $from: the value does not deserialize under $REG (schema id $id)"; fail=1; continue
  fi
  echo "  $TOPIC-$p offset $from: Avro, schema id $id resolves on $REG"
done < <("$KBIN/kafka-get-offsets" --bootstrap-server "$BS" --topic "$TOPIC" 2>/dev/null)
[ "$fail" -eq 0 ] || exit 1
