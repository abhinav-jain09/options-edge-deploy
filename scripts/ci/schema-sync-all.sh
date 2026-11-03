#!/usr/bin/env bash
# schema-sync-all.sh <environment> - run the Schema sync gate for EVERY service the manifest binds to that environment (docs/schema-governance R11).
# For deploy paths that roll many services in one go (scripts/deploy/apply.sh). It has no per-service image digest to read, so each service must
# be declaredByManifest (R5e transition) - schema-sync.sh fails closed on any that is not. Registries come from schema-manifest/registries.json.
# Env: DEPLOY_DRY_RUN (passed through), PLAN_SHA256 (passed through; prod write needs it per schema-sync.sh), SCHEMA_GOV_JAR (default: pinned jar).
set -euo pipefail
cd "$(dirname "$0")/../.."
ENVIRONMENT="${1:?usage: schema-sync-all.sh <dev|production|es4>}"
case "$ENVIRONMENT" in dev) ENVN=dev ;; production) ENVN=prod ;; es4) ENVN=es4 ;; *) echo "FATAL: unknown environment '$ENVIRONMENT'" >&2; exit 1 ;; esac
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"
PIN="$(sed -n 's/^sha256=//p' tools/schema-governance/PIN)"
[ -f "$JAR" ] && [ "$(shasum -a 256 "$JAR" | cut -d' ' -f1)" = "$PIN" ] \
  || { echo "FATAL: $JAR is missing or does not match tools/schema-governance/PIN - refusing to run an unpinned gate" >&2; exit 1; }
URL="$(python3 -c 'import json,sys; print(json.load(open("schema-manifest/registries.json"))[sys.argv[1]])' "$ENVN")" \
  || { echo "FATAL: no registry URL for $ENVN in schema-manifest/registries.json" >&2; exit 1; }
SERVICES="$(python3 - "$ENVN" <<'PY'
import json, sys
env = sys.argv[1]; out = set()
for e in json.load(open("schema-manifest/manifest.json"))["entries"]:
    for p in (e.get("producers") or []) + (e.get("consumers") or []):
        if env in p.get("environments", []): out.add(p["service"])
print(" ".join(sorted(out)))
PY
)" || { echo "FATAL: cannot read schema-manifest/manifest.json" >&2; exit 1; }
if [ -z "$SERVICES" ]; then echo "schema-sync-all: no service is bound to $ENVN in the manifest - nothing to gate"; exit 0; fi
echo "schema-sync-all: $ENVN -> $SERVICES"
for svc in $SERVICES; do
  SERVICE="$svc" ENVIRONMENT="$ENVIRONMENT" SCHEMA_REGISTRY_URL="$URL" SCHEMA_GOV_JAR="$JAR" bash scripts/ci/schema-sync.sh \
    || { echo "FATAL: schema sync refused $svc in $ENVIRONMENT" >&2; exit 1; }
done
