#!/usr/bin/env bash
# schema-bundle.sh <env> <outdir> - build the schema bundle a host that cannot see this repo registers from (docs/schema-governance R11b).
# <outdir>/plan.map             one subject|id|file|version row per MIRRORED manifest entry listed for <env>, at the R3e EFFECTIVE id
# <outdir>/schemas/<file>       the snapshot of every row (the exact registered text)
# <outdir>/manifest.commit      the deploy git commit the bundle was built from (one line)
# <outdir>/SHA256SUMS           sha256 of plan.map, manifest.commit and every snapshot, for `sha256sum -c` on the host
# Runs on the Jenkins agent (needs Java and the pinned tool jar); the host needs only bash, curl and sha256sum.
set -euo pipefail
cd "$(dirname "$0")/../.."
ENVN="${1:?usage: schema-bundle.sh <dev|prod|es4> <outdir>}"; OUT="${2:?outdir}"
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"
PIN="$(sed -n 's/^sha256=//p' tools/schema-governance/PIN)"
[ -f "$JAR" ] && [ "$(shasum -a 256 "$JAR" | cut -d' ' -f1)" = "$PIN" ] || { echo "FATAL: $JAR is missing or does not match tools/schema-governance/PIN" >&2; exit 1; }
HEAD_SHA="$(git rev-parse HEAD 2>/dev/null)" && [ -n "$HEAD_SHA" ] || { echo "FATAL: not a git checkout - a bundle must be bound to a deploy commit" >&2; exit 1; }
[ -z "$(git status --porcelain -- schema-manifest scripts/ci/schema-bundle.sh)" ] || { echo "FATAL: the manifest or snapshots have uncommitted changes - refusing to bind a bundle to a commit that does not describe them" >&2; exit 1; }
rm -rf "$OUT"; mkdir -p "$OUT/schemas"
java -jar "$JAR" validate --manifest schema-manifest/manifest.json --snapshots schema-manifest/schemas 2>&1 | grep -v '^SLF4J' >&2 || true
java -jar "$JAR" validate --manifest schema-manifest/manifest.json --snapshots schema-manifest/schemas >/dev/null 2>&1 || { echo "FATAL: the manifest does not validate" >&2; exit 1; }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
java -jar "$JAR" plan --manifest schema-manifest/manifest.json --env "$ENVN" --all --out "$WORK" >/dev/null 2>"$WORK/err" || { grep -v '^SLF4J' "$WORK/err" >&2; echo "FATAL: cannot plan the whole $ENVN set" >&2; exit 1; }
cp "$WORK/plan.map" "$OUT/plan.map"
while IFS='|' read -r subject id file version; do
  mkdir -p "$OUT/schemas/$(dirname "$file")"; cp "schema-manifest/schemas/$file" "$OUT/schemas/$file"
done < "$OUT/plan.map"
printf '%s\n' "$HEAD_SHA" > "$OUT/manifest.commit"
( cd "$OUT" && { shasum -a 256 plan.map manifest.commit; find schemas -type f | sort | xargs shasum -a 256; } > SHA256SUMS )
echo "schema-bundle: $ENVN commit=$HEAD_SHA rows=$(wc -l < "$OUT/plan.map" | tr -d ' ') -> $OUT"
