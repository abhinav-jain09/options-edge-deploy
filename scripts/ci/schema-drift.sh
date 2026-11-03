#!/usr/bin/env bash
# schema-drift.sh <env>... - read-only drift check of live Schema Registries against schema-manifest/manifest.json (docs/schema-governance R14/R15).
# Every finding except ABSENT_OK is a failure; an unreachable registry is a failure (never "0 differences"). Exits non-zero if ANY env fails,
# after checking all of them. SCHEMA_GOV_JAR defaults to the pinned jar. SCHEMA_DRIFT_ANYTIME=true skips the market-hours guard.
set -uo pipefail
cd "$(dirname "$0")/../.."
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"
PIN="$(sed -n 's/^sha256=//p' tools/schema-governance/PIN)"
[ "$(shasum -a 256 "$JAR" | cut -d' ' -f1)" = "$PIN" ] || { echo "FATAL: $JAR does not match tools/schema-governance/PIN"; exit 2; }
[ "$#" -gt 0 ] || { echo "usage: schema-drift.sh <dev|prod|es4>..."; exit 2; }
# R14(b): no registry reads in market hours 09:00-16:15 ET on weekdays unless explicitly overridden
if [ "${SCHEMA_DRIFT_ANYTIME:-false}" != "true" ]; then
  hm="$(TZ=America/New_York date +%H%M)"; dow="$(TZ=America/New_York date +%u)"
  if [ "$dow" -le 5 ] && [ "$hm" -ge 0900 ] && [ "$hm" -lt 1615 ]; then echo "schema-drift: market hours ($hm ET) - skipped (SCHEMA_DRIFT_ANYTIME=true overrides)"; exit 0; fi
fi
rc=0
for env in "$@"; do
  url="$(python3 -c 'import json,sys; print(json.load(open("schema-manifest/registries.json"))[sys.argv[1]])' "$env")" || { echo "FATAL: no registry URL for $env"; rc=1; continue; }
  echo "=== schema-drift $env ($url) ==="
  java -jar "$JAR" validate --manifest schema-manifest/manifest.json --snapshots schema-manifest/schemas 2>&1 | grep -v '^SLF4J' || rc=1
  java -jar "$JAR" drift --manifest schema-manifest/manifest.json --env "$env" --registry "$url" 2> >(grep -v '^SLF4J' >&2) | grep -v -E '^(PRESENT|ABSENT_OK) ' || true
  # the pipe hides the tool's exit code; ask again for the verdict, quietly
  java -jar "$JAR" drift --manifest schema-manifest/manifest.json --env "$env" --registry "$url" >/dev/null 2>&1 && echo "schema-drift $env: OK" || { echo "schema-drift $env: DRIFT (or unreachable)"; rc=1; }
done
exit $rc
