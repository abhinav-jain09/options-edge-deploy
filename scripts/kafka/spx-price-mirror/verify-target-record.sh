#!/usr/bin/env bash
# verify-target-record.sh <bootstrap> <registry url> <topic> <confluent bin>
# The spx-price mirror's decode gate: on every non-empty partition of the TARGET topic the newest record is Confluent-framed Avro
# whose schema id resolves ON THE TARGET REGISTRY to the subject com.optionsedge.contracts.hpsf.UnderlyingPrice and whose value
# deserializes under that registry. This is the proof a byte copy cannot give: a copied frame carries the SOURCE registry's id.
# The mirror's producer is not transactional, so end - 1 is a record (no commit marker). Framing is read from the bytes, never
# from a formatter's text; kafka-console-consumer exits 0 on every error, so an empty read is a FAIL, not "nothing there".
# An empty topic is a FAIL unless ALLOW_EMPTY=true (the caller says the source holds nothing either).
# CHECK_FIRST=true applies the SAME full check (frame, id resolves to the subject, decodes) to the EARLIEST record too: the install uses it
# to prove a topic holds no retained JSON (a JSON record followed by Avro would hide behind a newest-record check). Records in between are
# not read here; a mid-log schema from an older compatible version is caught by the mirror's own fail-closed stop.
set -euo pipefail
BS="${1:?bootstrap}"; REG="${2:?registry}"; TOPIC="${3:?topic}"; KBIN="${4:?confluent bin}"
SUBJECT="com.optionsedge.contracts.hpsf.UnderlyingPrice"
OFFSETS=$("${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$BS" --topic "$TOPIC" 2>/dev/null) \
  || { echo "FAIL: kafka-get-offsets failed for $TOPIC on $BS"; exit 1; }
BEGINS=$("${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$BS" --topic "$TOPIC" --time -2 2>/dev/null) \
  || { echo "FAIL: kafka-get-offsets --time -2 failed for $TOPIC on $BS"; exit 1; }
tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT
checked=0; fail=0
while IFS=: read -r t p end; do
  [ "$t" = "$TOPIC" ] || continue
  case "$p$end" in ''|*[!0-9]*) echo "FAIL: unparseable offset row for $TOPIC"; fail=1; continue ;; esac
  begin=$(printf '%s\n' "$BEGINS" | awk -F: -v t="$TOPIC" -v p="$p" '$1 == t && $2 == p {print $3}')
  case "$begin" in ''|*[!0-9]*) echo "FAIL: no earliest offset for $TOPIC-$p"; fail=1; continue ;; esac
  [ "$end" -gt "$begin" ] || continue
  checked=$((checked + 1))
  last=$((end - 1))
  offs="$last"
  [ "${CHECK_FIRST:-false}" != true ] || offs="$begin $last"
  for off in $offs; do
    "${KAFKA_CONSOLE_CONSUMER_CMD:-$KBIN/kafka-console-consumer}" --bootstrap-server "$BS" --topic "$TOPIC" --partition "$p" --offset "$off" \
      --max-messages 1 --timeout-ms 20000 > "$tmp" 2>/dev/null || true
    # a tombstone prints as the text "null": this topic declares none, so it is a failure like any non-Avro value
    [ -s "$tmp" ] || { echo "FAIL: $TOPIC-$p: nothing read at offset $off"; fail=1; continue; }
    hex=$(head -c 5 "$tmp" | xxd -p)
    case "$hex" in
      00????????*) ;;
      *) echo "FAIL: $TOPIC-$p@$off: not Confluent-framed Avro (first bytes ${hex:-none}); a JSON writer, a tombstone or a byte copy"; fail=1; continue ;;
    esac
    [ "${#hex}" -eq 10 ] || { echo "FAIL: $TOPIC-$p@$off: frame shorter than 5 bytes"; fail=1; continue; }
    id=$((16#${hex:2:8}))
    body=$(curl -sS -m 20 "${REG%/}/schemas/ids/$id/versions") || { echo "FAIL: registry $REG unreachable"; fail=1; continue; }
    body=${body//[[:space:]]/}
    case "$body" in
      *"\"subject\":\"$SUBJECT\""*) ;;
      *) echo "FAIL: $TOPIC-$p@$off: schema id $id does not resolve to $SUBJECT on $REG (a source-registry id copied byte for byte?)"; fail=1; continue ;;
    esac
    dec=$("${KAFKA_AVRO_CONSOLE_CONSUMER_CMD:-$KBIN/kafka-avro-console-consumer}" --bootstrap-server "$BS" --topic "$TOPIC" --partition "$p" --offset "$off" \
      --max-messages 1 --timeout-ms 20000 --property schema.registry.url="$REG" 2>/dev/null) || dec=""
    case "$dec" in
      *symbol*) echo "  $TOPIC-$p@$off: Avro id $id resolves to $SUBJECT on $REG and decodes" ;;
      *) echo "FAIL: $TOPIC-$p@$off: the Avro consumer could not decode the record under $REG"; fail=1 ;;
    esac
  done
done <<< "$OFFSETS"
if [ "$checked" -eq 0 ]; then
  [ "${ALLOW_EMPTY:-false}" = true ] && { echo "  $TOPIC is empty (allowed by the caller)"; exit "$fail"; }
  echo "FAIL: $TOPIC holds no record on $BS — nothing was decoded"; exit 1
fi
exit "$fail"
