#!/usr/bin/env bash
# schema-code-verify.sh <processing-checkout> - the deploy repo's half of docs/schema-governance R5b/R5e: for every processing module that carries
# src/main/resources/schema-requirements.json (the file that becomes the image label),
#   (1) the file must agree with the manifest's bindings for that service in every environment the manifest binds it to (tool `plan --label`), and
#   (2) the module's own tests must pass AGAINST THE MANIFEST UNDER REVIEW (-Dschemagov.manifest), so a producer whose runtime schema differs from the manifest
#       fails here, before any image is built.
# Module -> deploy service: the module directory minus a trailing "-service" (early-auction-verdict-service -> early-auction-verdict).
# Needs Maven (offline-capable) and the pinned tool jar. Exits non-zero on the first disagreement; finding NO requirements file at all is an error.
set -euo pipefail
cd "$(dirname "$0")/../.."
P="${1:?usage: schema-code-verify.sh <options-edge-processing checkout> [r6-backlog-file: tests only; the Jenkinsfile never passes it]}"
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"
PIN="$(sed -n 's/^sha256=//p' tools/schema-governance/PIN)"
[ -f "$JAR" ] && [ "$(shasum -a 256 "$JAR" | cut -d' ' -f1)" = "$PIN" ] || { echo "FATAL: $JAR is missing or does not match tools/schema-governance/PIN" >&2; exit 1; }
MANIFEST="${SCHEMA_MANIFEST:-$PWD/schema-manifest/manifest.json}"
mapfile -t FILES < <(cd "$P" && ls -1 */src/main/resources/schema-requirements.json 2>/dev/null || true)
[ "${#FILES[@]}" -gt 0 ] || { echo "FATAL: no schema-requirements.json found under $P - refusing to report 'verified' on nothing" >&2; exit 1; }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
rc=0
for f in "${FILES[@]}"; do
  module="${f%%/*}"; svc="${module%-service}"
  echo "=== $module (service $svc) ==="
  bound=0
  for env in dev prod es4; do
    if python3 - "$MANIFEST" "$svc" "$env" <<'PY'
import json,sys
m=json.load(open(sys.argv[1])); svc,env=sys.argv[2:4]
sys.exit(0 if any(env in p.get("environments",[]) for e in m["entries"] for p in (e.get("producers") or [])+(e.get("consumers") or []) if p["service"]==svc) else 1)
PY
    then bound=1
      # capture the tool's status explicitly: with `pipefail` a failing tool inside a pipeline silently took the "match" branch
      prc=0; pout="$(java -jar "$JAR" plan --manifest "$MANIFEST" --env "$env" --service "$svc" --label "$P/$f" --out "$WORK/$module-$env" 2>&1)" || prc=$?
      if [ "$prc" -ne 0 ]; then printf '%s\n' "$pout" | grep -v '^SLF4J'; echo "$module: requirements disagree with the manifest in $env"; rc=1; else echo "  $env: requirements match the manifest bindings"; fi
    fi
  done
  [ "$bound" = 1 ] || { echo "FAIL: $svc is bound to no environment in the manifest, yet $module declares requirements"; rc=1; }
  (cd "$P" && mvn -B -o -q -pl "$module" -am test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -DargLine="-Dschemagov.manifest=$MANIFEST" -Dtest='*SchemaRequirements*') \
    && echo "  module tests passed against the manifest" || { echo "FAIL: $module's schema-requirements tests failed against the manifest"; rc=1; }
done
# R6 ratchet: every Avro-using module without a requirements file must be on the committed backlog, and the backlog may only shrink.
python3 scripts/ci/schema-r6-backlog.py "$P" --backlog "${2:-schema-manifest/r6-backlog.txt}" || rc=1
exit $rc
