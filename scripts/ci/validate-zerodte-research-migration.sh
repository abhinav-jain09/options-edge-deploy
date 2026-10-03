#!/usr/bin/env bash
# Every 0DTE research-migration declaration (deploy/zerodte/research-migration/<env>.yaml) is read with the SAME strict YAML subset the Job's
# loader and the attestation port share (scripts/ci/zerodte_attestation.py) and held to its exact schema: the keys, their types, their domains
# (fromVersion 6, toVersion 7, a 64-hex calendarVersion, expectedSessionsAhead 0..400, an operator id). A declaration is a reviewed statement
# of what a migration run may do; a file that only loads is not one.
set -euo pipefail
cd "$(dirname "$0")/../.."
fail=0
# the legacy-writer declaration (the closed world of scripts/ops/zerodte-quiescence.py): exact keys, lower-case DNS names, the repository
# the migration Job runs; read with yq by the wrapper, so judged here with yq too
W="deploy/zerodte/research-migration/legacy-writers.yaml"
if [ -f "$W" ]; then
  if out="$(python3 - "$W" <<'PY' 2>&1
import re, subprocess, sys, json
f = sys.argv[1]
doc = json.loads(subprocess.run(["yq", "-o=json", ".", f], capture_output=True, text=True, check=True).stdout)
if not isinstance(doc, dict) or sorted(doc.keys()) != ["exemptJobLabels", "imageRepository", "writerDeployments"]:
    raise SystemExit("keys must be exactly imageRepository, writerDeployments, exemptJobLabels; got %s" % (sorted(doc.keys()) if isinstance(doc, dict) else type(doc).__name__))
if doc["imageRepository"] != "options-edge-vix-option-inteligence":
    raise SystemExit("imageRepository must be options-edge-vix-option-inteligence (the image the migration Job runs), got %r" % (doc["imageRepository"],))
name = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")
for key, at_least in (("writerDeployments", 1), ("exemptJobLabels", 0)):
    v = doc[key]
    if not isinstance(v, list) or len(v) < at_least or any(not isinstance(x, str) or not name.match(x) for x in v) or len(set(v)) != len(v):
        raise SystemExit("%s must be a list of %s unique lower-case DNS names" % (key, "at least one" if at_least else "zero or more"))
print("imageRepository=%s writerDeployments=%s exemptJobLabels=%s" % (doc["imageRepository"], ",".join(doc["writerDeployments"]), ",".join(doc["exemptJobLabels"]) or "-"))
PY
)"; then echo "ok   $W ($out)"; else echo "FAIL: $W — $out"; fail=1; fi
else
  echo "FAIL: no legacy-writer declaration at $W (the quiescence proof's closed world)"; fail=1
fi
for f in deploy/zerodte/research-migration/*.yaml; do
  [ -e "$f" ] || { echo "FAIL: no migration declaration under deploy/zerodte/research-migration/"; exit 1; }
  [ "$f" = "$W" ] && continue
  env="$(basename "$f" .yaml)"
  case "$env" in dev|production) : ;; *) echo "FAIL: $f is not named after an environment (dev|production)"; fail=1; continue ;; esac
  if out="$(python3 scripts/ci/zerodte_attestation.py migration "$f" 2>&1)"; then
    echo "ok   $f ($out)"
  else
    echo "FAIL: $f — $out"; fail=1
  fi
done
[ "$fail" -eq 0 ] && { echo "=== validate-zerodte-research-migration: OK ==="; exit 0; }
echo "=== validate-zerodte-research-migration: FAILED ==="; exit 1
