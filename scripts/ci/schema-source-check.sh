#!/usr/bin/env bash
# schema-source-check.sh <options-edge-contracts checkout> - the reverse-drift half of docs/schema-governance R8a / R14b (contracts repo files):
#  (1) SOURCE_DRIFT: every FILE entry's file in the contracts checkout must still hash to the entry's schemaHash (a contracts change without a manifest change fails);
#  (2) a RATCHET on files nobody governs yet: every .avsc under schemas/ and schemas-registry/ that no manifest entry names must be listed in
#      schema-manifest/ungoverned-contracts-files.txt (the baseline of 2026-11-03). A NEW unnamed file fails ("register this schema in the manifest first"),
#      and a listed file that is gone or is now named fails as stale, so the list can only shrink.
# `pending: true` entries (dev-only, manifest-first ordering) are exempt from (1) until the file lands.
set -euo pipefail
cd "$(dirname "$0")/../.."
C="${1:?usage: schema-source-check.sh <options-edge-contracts checkout>}"
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"
PIN="$(sed -n 's/^sha256=//p' tools/schema-governance/PIN)"
[ -f "$JAR" ] && [ "$(shasum -a 256 "$JAR" | cut -d' ' -f1)" = "$PIN" ] || { echo "FATAL: $JAR is missing or does not match tools/schema-governance/PIN" >&2; exit 1; }
[ -d "$C/schemas" ] || [ -d "$C/schemas-registry" ] || { echo "FATAL: $C does not look like options-edge-contracts" >&2; exit 1; }
LIST=schema-manifest/ungoverned-contracts-files.txt
rc=0
# (1) FILE entries -> hash check
while IFS='|' read -r subject version path hash; do
  [ -f "$C/$path" ] || { echo "SOURCE_DRIFT $subject v$version: $path is missing from the contracts checkout"; rc=1; continue; }
  have="$(java -jar "$JAR" hash "$C/$path" 2>/dev/null)" || { echo "SOURCE_DRIFT $subject v$version: $path is not valid Avro"; rc=1; continue; }
  [ "$have" = "$hash" ] || { echo "SOURCE_DRIFT $subject v$version: $path hashes to $have but the manifest registers $hash"; rc=1; }
done < <(python3 - <<'PY'
import json
for e in json.load(open("schema-manifest/manifest.json"))["entries"]:
    if e.get("kind") == "MIRRORED" and e["source"]["kind"] == "FILE" and e["source"]["repo"] == "contracts" and not e.get("pending"):
        print("|".join([e["subject"], str(e["version"]), e["source"]["path"], e["schemaHash"]]))
PY
)
# (2) ratchet on unnamed files
python3 - "$C" "$LIST" <<'PY' || rc=1
import json, os, sys
c, lst = sys.argv[1:3]
named = {e["source"]["path"] for e in json.load(open("schema-manifest/manifest.json"))["entries"] if e.get("kind") == "MIRRORED" and e["source"]["kind"] == "FILE" and e["source"]["repo"] == "contracts"}
files = set()
for d in ("schemas", "schemas-registry"):
    for dp, _, fs in os.walk(os.path.join(c, d)):
        for f in fs:
            if f.endswith(".avsc"): files.add(os.path.relpath(os.path.join(dp, f), c))
unnamed = files - named
listed = {l.strip() for l in open(lst) if l.strip() and not l.startswith("#")}
bad = 0
for f in sorted(unnamed - listed): print("UNGOVERNED_NEW %s: a contracts schema no manifest entry names and not in the baseline - register it in the manifest first" % f); bad = 1
for f in sorted(listed - files): print("STALE_BASELINE %s: listed as ungoverned but no longer in the contracts checkout - remove it from %s" % (f, lst)); bad = 1
for f in sorted(listed & named): print("STALE_BASELINE %s: now named by a manifest entry - remove it from %s" % (f, lst)); bad = 1
print("schema-source-check: %d governed, %d baseline-ungoverned, %d new" % (len(named & files), len(unnamed & listed), len(unnamed - listed)))
sys.exit(bad)
PY
exit $rc
