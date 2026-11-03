#!/usr/bin/env bash
# schema-sync-es4-host.sh <verify|apply> <bundle-dir> [expected-commit] [expected-digest] - runs ON the es4 host from a shipped bundle (docs/schema-governance R11b).
# verify: the bundle is present, was built from <expected-commit>, and every file matches SHA256SUMS. Changes nothing; used in the
#         clean-reset PREFLIGHT, before the wipe, so a bad bundle can never leave an empty registry.
# apply:  verify (no commit check unless one is given), then register plan.map on SCHEMA_REGISTRY_URL (default http://localhost:8081)
#         through the sanctioned engine ensure-fixed-id-schemas.sh. Honors DEPLOY_DRY_RUN.
# No Java, no network access to any repository.
set -euo pipefail
MODE="${1:?usage: schema-sync-es4-host.sh <verify|apply> <bundle-dir> [expected-commit] [expected-digest]}"; B="${2:?bundle dir}"; WANT="${3:-}"; WANT_DIGEST="${4:-}"
case "$MODE" in verify|apply) ;; *) echo "FAIL: unknown mode '$MODE' (verify or apply)" >&2; exit 2 ;; esac
HERE="$(cd "$(dirname "$0")" && pwd)"
sum() { if command -v sha256sum >/dev/null 2>&1; then sha256sum -c --quiet "$@"; else shasum -a 256 -c --quiet "$@"; fi; }
[ -d "$B" ] && [ -f "$B/plan.map" ] && [ -f "$B/SHA256SUMS" ] && [ -f "$B/manifest.commit" ] || { echo "FAIL: schema bundle $B is missing or incomplete" >&2; exit 1; }
[ -s "$B/plan.map" ] || { echo "FAIL: schema bundle $B has an empty plan.map - refusing to treat it as 'nothing to register'" >&2; exit 1; }
HAVE="$(tr -d '[:space:]' < "$B/manifest.commit")"
[ -z "$WANT" ] || [ "$HAVE" = "$WANT" ] || { echo "FAIL: schema bundle was built from $HAVE but this run expects $WANT (stale bundle)" >&2; exit 1; }
# The checksum list is only as trustworthy as its channel: Jenkins computes the sha256 of ITS OWN SHA256SUMS and hands it over on the ssh command
# line (not inside the shipped directory), so a bundle whose list was edited or regenerated no longer matches. Outside the pre-wipe path a digest
# may be omitted (tests, manual use); cleanup-es4.sh always supplies it.
if [ -n "$WANT_DIGEST" ]; then
  if command -v sha256sum >/dev/null 2>&1; then HAVE_DIGEST="$(sha256sum "$B/SHA256SUMS" | cut -d' ' -f1)"; else HAVE_DIGEST="$(shasum -a 256 "$B/SHA256SUMS" | cut -d' ' -f1)"; fi
  [ "$HAVE_DIGEST" = "$WANT_DIGEST" ] || { echo "FAIL: schema bundle SHA256SUMS digest $HAVE_DIGEST is not the $WANT_DIGEST Jenkins computed (the checksum list was altered or regenerated)" >&2; exit 1; }
fi
# COMPLETE coverage: every file in the bundle (except the list itself) is listed, and every listed file exists - nothing unchecked, nothing extra.
LISTED="$(awk '{print $2}' "$B/SHA256SUMS" | sort)"; PRESENT="$(cd "$B" && find . -type f ! -name SHA256SUMS | sed 's#^\./##' | sort)"
[ "$LISTED" = "$PRESENT" ] || { echo "FAIL: schema bundle $B and its SHA256SUMS disagree about which files exist (an unlisted or missing file)" >&2; exit 1; }
( cd "$B" && sum SHA256SUMS ) || { echo "FAIL: a file in schema bundle $B does not match its SHA256SUMS" >&2; exit 1; }
while IFS='|' read -r subject id file version; do
  [ -f "$B/schemas/$file" ] || { echo "FAIL: plan row for $subject v$version names $file, which the bundle does not contain" >&2; exit 1; }
done < "$B/plan.map"
echo "schema-bundle verified: commit=$HAVE rows=$(wc -l < "$B/plan.map" | tr -d ' ')"
[ "$MODE" = apply ] || exit 0
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}" SCHEMAS_DIR="$B/schemas" SCHEMA_ID_MAP="$B/plan.map" DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-false}" \
  bash "$HERE/../kafka/ensure-fixed-id-schemas.sh"
