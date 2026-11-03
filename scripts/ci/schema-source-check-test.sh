#!/usr/bin/env bash
# Negative controls for schema-source-check.sh (docs/schema-governance R8a): a contracts edit without a manifest change, a missing file, a new ungoverned file,
# and a stale baseline line must each fail; an untouched checkout must pass. Fake contracts checkout, real jar.
set -euo pipefail
cd "$(dirname "$0")/../.."
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"; JAR="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")"; REPO="$PWD"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
S1='{"type":"record","name":"A","fields":[{"name":"a","type":"int","default":1}]}'
setup() { rm -rf "$T/r" "$T/c"; mkdir -p "$T/r/scripts/ci" "$T/r/tools/schema-governance" "$T/r/schema-manifest" "$T/c/schemas/x" "$T/c/schemas-registry"
  cp "$REPO/scripts/ci/schema-source-check.sh" "$T/r/scripts/ci/"; cp "$REPO/tools/schema-governance/PIN" "$JAR" "$T/r/tools/schema-governance/"
  printf '%s' "$S1" > "$T/c/schemas/x/A.avsc"; printf '%s' "$S1" | sed 's/"A"/"B"/' > "$T/c/schemas/x/B.avsc"
  H="$(java -jar "$JAR" hash "$T/c/schemas/x/A.avsc" 2>/dev/null)"
  python3 - "$T/r/schema-manifest/manifest.json" "$H" <<'PY'
import json,sys
json.dump({"entries":[{"kind":"MIRRORED","subject":"a-value","version":1,"id":2100,"schemaHash":sys.argv[2],"source":{"kind":"FILE","repo":"contracts","path":"schemas/x/A.avsc"}}]},open(sys.argv[1],"w"))
PY
  printf 'schemas/x/B.avsc\n' > "$T/r/schema-manifest/ungoverned-contracts-files.txt"; }
fail=0
expect() { local name="$1" want="$2" out rc=0; out=$(cd "$T/r" && bash scripts/ci/schema-source-check.sh "$T/c" 2>&1) || rc=$?
  if [ "$want" = ok ]; then [ "$rc" = 0 ] && echo "ok     $name" || { echo "NOT-OK $name: must pass"; echo "$out" | tail -3; fail=1; }
  else [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "$want" && echo "ok     $name" || { echo "NOT-OK $name: expected failure '$want' (rc=$rc)"; echo "$out" | tail -3; fail=1; }; fi; }
setup; expect "an untouched checkout passes" ok
setup; printf '%s' "$S1" | sed 's/"a"/"aa"/' > "$T/c/schemas/x/A.avsc"; expect "a contracts edit with no manifest change is SOURCE_DRIFT" "SOURCE_DRIFT a-value v1"
setup; rm "$T/c/schemas/x/A.avsc"; expect "a governed file removed from contracts" "is missing from the contracts checkout"
setup; printf '%s' "$S1" | sed 's/"A"/"C"/' > "$T/c/schemas/x/C.avsc"; expect "a NEW unnamed contracts schema" "UNGOVERNED_NEW schemas/x/C.avsc"
setup; rm "$T/c/schemas/x/B.avsc"; expect "a baseline file that is gone is stale" "STALE_BASELINE schemas/x/B.avsc"
setup; printf 'schemas/x/B.avsc\nschemas/x/A.avsc\n' > "$T/r/schema-manifest/ungoverned-contracts-files.txt"; expect "a baseline file that is now named is stale" "STALE_BASELINE schemas/x/A.avsc"
setup; printf 'not avro' > "$T/c/schemas/x/A.avsc"; expect "a governed file that is no longer valid Avro" "not valid Avro"
pend() { python3 - "$T/r/schema-manifest/manifest.json" "$1" "$2" <<'PY'
import json,sys
p,path,h=sys.argv[1:4]; m=json.load(open(p))
m["entries"].append({"kind":"MIRRORED","subject":"p-value","version":1,"id":2101,"schemaHash":h,"pending":True,"source":{"kind":"FILE","repo":"contracts","path":path}})
json.dump(m,open(p,"w"))
PY
}
setup; pend schemas/x/Future.avsc 0000000000000000000000000000000000000000000000000000000000000000; expect "a pending entry whose file has not landed is tolerated" ok
setup; pend schemas/x/B.avsc 0000000000000000000000000000000000000000000000000000000000000000; expect "a pending entry that names an existing file with another hash is refused (it could hide a change)" "PENDING_SHADOWS_FILE"
setup; HB="$(java -jar "$JAR" hash "$T/c/schemas/x/B.avsc" 2>/dev/null)"; pend schemas/x/B.avsc "$HB"; expect "a pending entry whose file has landed must drop pending" "PENDING_LANDED"
exit $fail
