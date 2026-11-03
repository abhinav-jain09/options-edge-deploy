#!/usr/bin/env bash
# schema-sync-es4-host.sh <verify|apply> <bundle-dir> [expected-commit] - runs ON the es4 host from a shipped bundle (docs/schema-governance R11b).
# verify: the bundle is present, was built from <expected-commit>, and every file matches SHA256SUMS. Changes nothing; used in the
#         clean-reset PREFLIGHT, before the wipe, so a bad bundle can never leave an empty registry.
# apply:  verify (no commit check unless one is given), then register plan.map on SCHEMA_REGISTRY_URL (default http://localhost:8081)
#         through the sanctioned engine ensure-fixed-id-schemas.sh. Honors DEPLOY_DRY_RUN.
# No Java, no network access to any repository.
set -euo pipefail
MODE="${1:?usage: schema-sync-es4-host.sh <verify|apply> <bundle-dir> [expected-commit]}"; B="${2:?bundle dir}"; WANT="${3:-}"
HERE="$(cd "$(dirname "$0")" && pwd)"
sum() { if command -v sha256sum >/dev/null 2>&1; then sha256sum -c --quiet "$@"; else shasum -a 256 -c --quiet "$@"; fi; }
[ -d "$B" ] && [ -f "$B/plan.map" ] && [ -f "$B/SHA256SUMS" ] && [ -f "$B/manifest.commit" ] || { echo "FAIL: schema bundle $B is missing or incomplete" >&2; exit 1; }
[ -s "$B/plan.map" ] || { echo "FAIL: schema bundle $B has an empty plan.map - refusing to treat it as 'nothing to register'" >&2; exit 1; }
HAVE="$(tr -d '[:space:]' < "$B/manifest.commit")"
[ -z "$WANT" ] || [ "$HAVE" = "$WANT" ] || { echo "FAIL: schema bundle was built from $HAVE but this run expects $WANT (stale bundle)" >&2; exit 1; }
( cd "$B" && sum SHA256SUMS ) || { echo "FAIL: a file in schema bundle $B does not match its SHA256SUMS" >&2; exit 1; }
while IFS='|' read -r subject id file version; do
  [ -f "$B/schemas/$file" ] || { echo "FAIL: plan row for $subject v$version names $file, which the bundle does not contain" >&2; exit 1; }
done < "$B/plan.map"
echo "schema-bundle verified: commit=$HAVE rows=$(wc -l < "$B/plan.map" | tr -d ' ')"
[ "$MODE" = apply ] || exit 0
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}" SCHEMAS_DIR="$B/schemas" SCHEMA_ID_MAP="$B/plan.map" DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-false}" \
  bash "$HERE/../kafka/ensure-fixed-id-schemas.sh"
