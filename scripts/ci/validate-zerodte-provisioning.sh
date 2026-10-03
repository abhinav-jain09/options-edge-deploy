#!/usr/bin/env bash
# Every 0DTE provisioning declaration (deploy/zerodte/provisioning/<env>.yaml) passes the Job's own rules (ProvisioningFile, increment 7a:
# exact keys, exact types, every domain and topology rule, the output CONTRACT) and names a lineage the attestation knows; every confirmed
# receipt (deploy/zerodte/provisioned/<env>.yaml) has the shape increment 8's render will read. A file that only LOADS is not a declaration
# that was reviewed: a refusal here is a refusal the Job would have given, found before any cluster is touched.
set -euo pipefail
cd "$(dirname "$0")/../.."
fail=0
ATT="deploy/zerodte/virgin-attestation.yaml"
LINEAGES="$(python3 - "$ATT" <<'PY'
import sys; sys.path.insert(0, "scripts/ci")
import zerodte_attestation as z
a = z.parse_attestation(open(sys.argv[1], encoding="utf-8").read())
print("\n".join(l["id"] for l in a["lineages"]))
PY
)" || { echo "FAIL: $ATT does not verify"; exit 1; }
for f in deploy/zerodte/provisioning/*.yaml; do
  [ -e "$f" ] || { echo "FAIL: no provisioning declaration under deploy/zerodte/provisioning/"; exit 1; }
  env="$(basename "$f" .yaml)"
  case "$env" in dev|production|experiment) : ;; *) echo "FAIL: $f is not named after an environment (dev|production|experiment)"; fail=1; continue ;; esac
  if out="$(python3 scripts/ci/zerodte_attestation.py provisioning "$f")"; then
    lineage="$(printf '%s' "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin)["environmentLineageId"])')"
    if printf '%s\n' "$LINEAGES" | grep -qxF "$lineage"; then
      echo "ok   $f ($out)"
    else
      echo "FAIL: $f names lineage $lineage, which $ATT does not define"; fail=1
    fi
  else
    echo "FAIL: $f — $out"; fail=1
  fi
done
for r in deploy/zerodte/provisioned/*.yaml; do
  [ -e "$r" ] || continue
  env="$(basename "$r" .yaml)"
  if python3 - "$r" "$env" <<'PY'
import re, sys
sys.path.insert(0, "scripts/ci")
import zerodte_attestation as z
root = z.load(open(sys.argv[1], encoding="utf-8").read())
keys = ["environment", "symbol", "generation", "eraId", "ledgerTopicId", "clusterId", "provisionedDigest", "ledgerOffset"]
z._exact_keys(root, keys, "the receipt")
assert z._text(root["environment"], "environment") == sys.argv[2], "environment names the file"
z._text(root["symbol"], "symbol", z.SYMBOL)
for k in ("generation", "eraId", "ledgerOffset"):
    v = root[k]
    assert isinstance(v, z.Scalar) and not v.quoted and re.match(r"^[0-9]+$", v.text), k + " is a non-negative integer"
z._text(root["ledgerTopicId"], "ledgerTopicId", z.HEX32, quoted=True)
z._text(root["clusterId"], "clusterId", z.TEXT, quoted=True)
z._text(root["provisionedDigest"], "provisionedDigest", z.HEX64, quoted=True)
PY
  then echo "ok   $r"; else echo "FAIL: $r is not a confirmed receipt of the documented shape"; fail=1; fi
done
[ "$fail" -eq 0 ] && { echo "=== validate-zerodte-provisioning: OK ==="; exit 0; }
echo "=== validate-zerodte-provisioning: FAILED ==="; exit 1
