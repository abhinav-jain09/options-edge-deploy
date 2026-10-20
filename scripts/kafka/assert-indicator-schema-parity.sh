#!/usr/bin/env bash
# assert-indicator-schema-parity.sh <source registry url> <target registry url>
# The indicator family's fixed ids 2001-2010 resolve to the SAME schema (by structure) and the same subject
# on both registries. The es4 → dev/prod mirrors copy bytes, and a framed value names its schema by id: a
# differing id on the target would decode a record under a foreign schema. Run before every mirror install
# (INDICATOR-AVRO-CONVERSION-PLAN.md section 4).
set -euo pipefail
SRC="${1:?source registry url}"; TGT="${2:?target registry url}"
fail=0
for id in 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010; do
  a=$(curl -sS -m 20 -w $'\n%{http_code}' "${SRC%/}/schemas/ids/$id") || { echo "FAIL: $SRC unreachable"; exit 1; }
  b=$(curl -sS -m 20 -w $'\n%{http_code}' "${TGT%/}/schemas/ids/$id") || { echo "FAIL: $TGT unreachable"; exit 1; }
  [ "${a##*$'\n'}" = 200 ] || { echo "FAIL: id $id absent on source $SRC"; fail=1; continue; }
  [ "${b##*$'\n'}" = 200 ] || { echo "FAIL: id $id absent on target $TGT"; fail=1; continue; }
  if ! python3 - "${a%$'\n'*}" "${b%$'\n'*}" "$id" <<'PY'
import json, sys
def structural(x):
    # the parsing form: docs and the Java-only string property are not the contract
    if isinstance(x, dict):
        return {k: structural(v) for k, v in x.items() if k not in ("doc", "avro.java.string", "oe.decimal")}
    if isinstance(x, list):
        return [structural(v) for v in x]
    return x
a = structural(json.loads(json.loads(sys.argv[1])["schema"]))
b = structural(json.loads(json.loads(sys.argv[2])["schema"]))
if a != b:
    print(f"FAIL: id {sys.argv[3]} differs between source and target: {a.get('name')} vs {b.get('name')}")
    sys.exit(1)
print(f"  id {sys.argv[3]}: {a.get('namespace','')}.{a.get('name')} identical")
PY
  then fail=1; fi
done
[ "$fail" -eq 0 ] || { echo "=== indicator schema parity: FAILED ==="; exit 1; }
echo "=== indicator schema parity: OK (ids 2001-2010 identical on $SRC and $TGT) ==="
