#!/usr/bin/env bash
# assert-subject-parity.sh <source registry url> <target registry url>
# The spx-price mirror decodes with the SOURCE registry and serializes with the TARGET registry under the same subject, and it never
# registers. So BOTH registries must already hold com.optionsedge.contracts.hpsf.UnderlyingPrice, and the LATEST version on each must be the
# same shape (docs ignored): otherwise the target would refuse the record, or worse, a record would be re-encoded under a different shape.
# The ids may differ; that is exactly why the mirror re-encodes. Register with scripts/kafka/ensure-underlying-price-schema.sh.
set -euo pipefail
SRC="${1:?source registry url}"; TGT="${2:?target registry url}"
SUBJECT="com.optionsedge.contracts.hpsf.UnderlyingPrice"
a=$(curl -sS -m 20 -w $'\n%{http_code}' "${SRC%/}/subjects/$SUBJECT/versions/latest") || { echo "FAIL: $SRC unreachable"; exit 1; }
b=$(curl -sS -m 20 -w $'\n%{http_code}' "${TGT%/}/subjects/$SUBJECT/versions/latest") || { echo "FAIL: $TGT unreachable"; exit 1; }
[ "${a##*$'\n'}" = 200 ] || { echo "FAIL: subject $SUBJECT absent on source registry $SRC"; exit 1; }
[ "${b##*$'\n'}" = 200 ] || { echo "FAIL: subject $SUBJECT absent on target registry $TGT (the mirror never registers; run ensure-underlying-price-schema.sh there)"; exit 1; }
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
