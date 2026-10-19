#!/usr/bin/env bash
# assert-subject-parity.sh <source registry url> <target registry url> [<source bootstrap> <source topic> <confluent bin>]
# The spx-price mirror decodes with the SOURCE registry and serializes with the TARGET registry under the same subject, and it never
# registers. So BOTH registries must already hold com.optionsedge.contracts.hpsf.UnderlyingPrice, and the LATEST version on each must be the
# same shape (docs ignored): otherwise the target would refuse the record, or worse, a record would be re-encoded under a different shape.
# With the optional source arguments it also reads the EARLIEST and the NEWEST retained record of every non-empty source partition and requires that the
# schema id in each frame names a schema whose canonical text equals one registered under the subject on the TARGET registry. A record from the
# MIDDLE of the log written under another compatible version is not sampled here; the mirror's own fail-closed stop (target serializer lookup) catches it.
# The ids may differ; that is exactly why the mirror re-encodes. Register with scripts/kafka/ensure-underlying-price-schema.sh.
set -euo pipefail
SRC="${1:?source registry url}"; TGT="${2:?target registry url}"; SBS="${3:-}"; STOPIC="${4:-}"; KBIN="${5:-}"
SUBJECT="com.optionsedge.contracts.hpsf.UnderlyingPrice"
a=$(curl -sS -m 20 -w $'\n%{http_code}' "${SRC%/}/subjects/$SUBJECT/versions/latest") || { echo "FAIL: $SRC unreachable"; exit 1; }
b=$(curl -sS -m 20 -w $'\n%{http_code}' "${TGT%/}/subjects/$SUBJECT/versions/latest") || { echo "FAIL: $TGT unreachable"; exit 1; }
[ "${a##*$'\n'}" = 200 ] || { echo "FAIL: subject $SUBJECT absent on source registry $SRC"; exit 1; }
[ "${b##*$'\n'}" = 200 ] || { echo "FAIL: subject $SUBJECT absent on target registry $TGT (the mirror never registers; run ensure-underlying-price-schema.sh there)"; exit 1; }
for reg in "$SRC" "$TGT"; do
  c=$(curl -sS -m 20 -w $'\n%{http_code}' "${reg%/}/config/$SUBJECT") || { echo "FAIL: $reg unreachable"; exit 1; }
  [ "${c##*$'\n'}" = 200 ] && printf '%s' "${c%$'\n'*}" | grep -q '"compatibilityLevel" *: *"FULL"' \
    || { echo "FAIL: $SUBJECT on $reg does not have FULL compatibility configured on the subject itself (GET /config/$SUBJECT); run ensure-underlying-price-schema.sh"; exit 1; }
  echo "  $reg: $SUBJECT compatibility FULL"
done
python3 - "${a%$'\n'*}" "${b%$'\n'*}" <<'PY'
import json, sys
def structural(x):
    if isinstance(x, dict):
        return {k: structural(v) for k, v in x.items() if k != "doc"}
    if isinstance(x, list):
        return [structural(v) for v in x]
    return x
a, b = json.loads(sys.argv[1]), json.loads(sys.argv[2])
if structural(json.loads(a["schema"])) != structural(json.loads(b["schema"])):
    print("FAIL: the latest UnderlyingPrice schema differs between the source and the target registry")
    sys.exit(1)
print(f"  subject parity OK: source id {a['id']} v{a['version']}, target id {b['id']} v{b['version']} (ids differ per registry; shape identical)")
PY
[ -n "$SBS" ] || exit 0
OFFS=$("${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$SBS" --topic "$STOPIC" 2>/dev/null) || { echo "FAIL: kafka-get-offsets failed for $STOPIC on $SBS"; exit 1; }
BEG=$("${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$SBS" --topic "$STOPIC" --time -2 2>/dev/null) || { echo "FAIL: kafka-get-offsets --time -2 failed for $STOPIC on $SBS"; exit 1; }
tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT
ids=""
while IFS=: read -r t p end; do
  [ "$t" = "$STOPIC" ] || continue
  b=$(printf '%s\n' "$BEG" | awk -F: -v t="$STOPIC" -v p="$p" '$1 == t && $2 == p {print $3}')
  case "$p$end$b" in ''|*[!0-9]*) echo "FAIL: unparseable offsets for $STOPIC"; exit 1 ;; esac
  [ "$end" -gt "$b" ] || continue
  for off in "$b" $((end - 1)); do
    "${KAFKA_CONSOLE_CONSUMER_CMD:-$KBIN/kafka-console-consumer}" --bootstrap-server "$SBS" --topic "$STOPIC" --partition "$p" --offset "$off" --max-messages 1 --timeout-ms 20000 > "$tmp" 2>/dev/null || true
    hex=$(head -c 5 "$tmp" | xxd -p)
    case "$hex" in 00????????*) ;; *) echo "FAIL: $STOPIC-$p@$off is not Confluent-framed Avro (JSON, a tombstone or nothing)"; exit 1 ;; esac
    ids="$ids $((16#${hex:2:8}))"
  done
done <<< "$OFFS"
[ -n "$ids" ] || { echo "  source $STOPIC is empty: no retained record to check"; exit 0; }
python3 - "$SRC" "$TGT" "$SUBJECT" $ids <<'PY'
import json, sys, urllib.request
src, tgt, subject, ids = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/"), sys.argv[3], sorted(set(sys.argv[4:]))
get = lambda u: json.load(urllib.request.urlopen(u, timeout=20))
canon = lambda s: json.dumps(json.loads(s), sort_keys=True, separators=(",", ":"))
target = {canon(get(f"{tgt}/subjects/{subject}/versions/{v}")["schema"]) for v in get(f"{tgt}/subjects/{subject}/versions")}
bad = 0
for i in ids:
    try:
        s = canon(get(f"{src}/schemas/ids/{i}")["schema"])
    except Exception as e:
        print(f"FAIL: retained source record names schema id {i}, unknown to the source registry: {e}"); bad = 1; continue
    if s in target:
        print(f"  retained source record schema id {i}: present under {subject} on the target registry")
    else:
        print(f"FAIL: retained source record schema id {i} has no byte-identical schema under {subject} on the target registry"); bad = 1
sys.exit(bad)
PY
