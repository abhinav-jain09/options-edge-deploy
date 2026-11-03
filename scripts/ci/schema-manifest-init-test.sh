#!/usr/bin/env bash
# Negative controls for schema-manifest-init.py: a stale schema-text directory and shared ids must not produce a manifest (docs/schema-governance R3a, R4).
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"; JAR="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")"; REPO="$PWD"
S1='{"type":"record","name":"A","fields":[{"name":"a","type":"int","default":1}]}'
S2='{"type":"record","name":"B","fields":[{"name":"b","type":"int","default":2}]}'
h() { printf '%s' "$1" > "$T/x.avsc"; java -jar "$JAR" hash "$T/x.avsc" 2>/dev/null; }
H1="$(h "$S1")"; H2="$(h "$S2")"
inv() { # inv <file> <id> <hash> [<id2> <hash2>]  - subject s-value with version 1 (and version 2)
  python3 - "$@" <<'PY'
import json,sys
f,i1,h1=sys.argv[1:4]; es=[{"subject":"s-value","version":1,"id":int(i1),"schemaHash":h1,"schemaFile":None}]
if len(sys.argv)>4: es.append({"subject":"s-value","version":2,"id":int(sys.argv[4]),"schemaHash":sys.argv[5],"schemaFile":None})
json.dump({"entries":es},open(f,"w"))
PY
}
setup() { rm -rf "$T/w"; mkdir -p "$T/w/contracts/schemas" "$T/w/schema-manifest/schemas" "$T/w/text/dev/s-value" "$T/w/text/prod/s-value" "$T/w/text/es4/s-value"
  echo '{"manifestVersion":1,"familyBlocks":{},"entries":[]}' > "$T/w/schema-manifest/manifest.json"
  for e in dev prod es4; do printf '%s' "$S1" > "$T/w/text/$e/s-value/v1.avsc"; printf '%s' "$S2" > "$T/w/text/$e/s-value/v2.avsc"; done
  for e in dev prod es4; do echo '{"entries":[]}' > "$T/w/inv-$e.json"; done; }
run() { ( cd "$T/w" && python3 "$REPO/scripts/ci/schema-manifest-init.py" --jar "$JAR" --dev inv-dev.json --prod inv-prod.json --es4 inv-es4.json --schema-text-dir text --contracts-root contracts 2>&1 ); }
fail=0
ok() { echo "ok     $1"; }; bad() { echo "NOT-OK $1"; fail=1; }

setup; inv "$T/w/inv-dev.json" 2100 "$H1"; out=$(run) && ok "a consistent inventory generates a MIRRORED baseline" || { bad "consistent inventory must generate: $out"; }
grep -q '"MIRRORED"' "$T/w/schema-manifest/manifest.json" && ok "the manifest holds the MIRRORED entry" || bad "no MIRRORED entry written"

setup; inv "$T/w/inv-dev.json" 2100 "$H2"; if out=$(run); then bad "a schema text that does not hash to the inventory's hash was accepted"; else printf '%s' "$out" | grep -q "refusing to write anything" && ok "stale schema text is refused" || bad "refused for the wrong reason: $out"; fi
[ ! -e "$T/w/schema-manifest/schemas/s-value" ] && ok "nothing was written when the pre-flight failed" || bad "snapshots were written despite the failure"

setup; inv "$T/w/inv-dev.json" 2100 "$H1" 2100 "$H2"; out=$(run) || true
grep -q "two versions of this subject share id 2100" "$T/w/schema-manifest/manifest.json" && ok "two versions of one subject sharing an id become ENV_LOCAL (R3a)" || bad "same-subject duplicate id slipped through"
setup; python3 - "$T/w/inv-dev.json" "$H1" <<'PY'
import json,sys; json.dump({"entries":[{"subject":"s-value","version":1,"id":2100,"schemaHash":sys.argv[2],"schemaFile":"cvd-related/Gone.avsc"}]},open(sys.argv[1],"w"))
PY
if out=$(run); then bad "a FILE match with no such file in the contracts checkout was accepted"; else printf '%s' "$out" | grep -q "no file under" && ok "a FILE source that is not really in the contracts checkout is refused" || bad "refused for the wrong reason: $out"; fi
exit $fail
