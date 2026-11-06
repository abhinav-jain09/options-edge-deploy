#!/usr/bin/env bash
# schema-sync-scope.sh <service> <environment> - decides whether the schema gate applies to a service in an environment.
# Prints one line: "GATE" (run schema-sync.sh) or "NOT_GATED_UNCOVERED" (the manifest names the service nowhere: today's coverage ratchet)
# or "EXEMPT: <reason>" (the service is bound in other environments only, and an exemption line names this one).
# Fail closed: any error exits non-zero and prints nothing on stdout; a caller must treat that as a refusal.
set -euo pipefail
SERVICE="${1:?service}"; ENVIRONMENT="${2:?environment}"
cd "$(dirname "$0")/../.."
MANIFEST="${SCHEMA_MANIFEST:-schema-manifest/manifest.json}"
EXEMPTIONS="${SCHEMA_EXEMPTIONS:-scripts/ci/schema-sync-environment-exemptions.txt}"
case "$ENVIRONMENT" in dev) ENVN=dev ;; production) ENVN=prod ;; es4) ENVN=es4 ;; *) echo "FAIL: unknown ENVIRONMENT '$ENVIRONMENT'" >&2; exit 1 ;; esac
[ -f "$MANIFEST" ] && [ -f "$EXEMPTIONS" ] || { echo "FAIL: manifest or exemption file missing" >&2; exit 1; }
python3 -I - "$MANIFEST" "$EXEMPTIONS" "$SERVICE" "$ENVIRONMENT" "$ENVN" <<'PY'
import json, sys
manifest, exemptions, service, environment, envn = sys.argv[1:6]
bindings = [b for e in json.load(open(manifest))["entries"] for b in (e.get("producers") or []) + (e.get("consumers") or []) if b.get("service") == service]
if not bindings:
    print("NOT_GATED_UNCOVERED"); sys.exit(0)
if any(envn in (b.get("environments") or []) for b in bindings):
    print("GATE"); sys.exit(0)
reason = None
for line in open(exemptions).read().splitlines():
    if not line.strip() or line.lstrip().startswith("#"):
        continue
    parts = line.split("|", 2)
    if len(parts) != 3 or not all(p.strip() for p in parts):
        sys.stderr.write("FAIL: malformed exemption line: " + line + "\n"); sys.exit(1)
    if parts[0] == service and parts[1] == environment:
        reason = parts[2]
print("EXEMPT: " + reason if reason else "GATE")
PY
