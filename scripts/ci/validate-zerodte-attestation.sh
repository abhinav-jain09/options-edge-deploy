#!/usr/bin/env bash
# The 0DTE VIRGIN attestation (deploy/zerodte/virgin-attestation.yaml; design §5.1, increment 7 Q5) is APPEND-ONLY and hash-chained:
# this is the `attestation-append-only` check the design names. It runs the Job's own rules (scripts/ci/zerodte_attestation.py, held
# to the Java implementation by golden vectors) and then compares the file with its BASE version: every lineage and every entry the base
# held must still be there, unchanged and in order — a removed or altered row, or a broken prevEntryHash chain, refuses the change.
#
# Usage: scripts/ci/validate-zerodte-attestation.sh [--base <git ref>]     default base: origin/main (the merge target); a ref that does
#        scripts/ci/validate-zerodte-attestation.sh --vectors               not hold the file (a new file) compares against nothing.
# The lineage approvals (approvedBy) are the OWNER's: this check judges shape and chain, not who approved — the review does.
set -euo pipefail
cd "$(dirname "$0")/../.."
FILE="deploy/zerodte/virgin-attestation.yaml"
BASE="origin/main"
case "${1:-}" in
  --vectors) exec python3 scripts/ci/zerodte_attestation.py --vectors ;;
  --base) BASE="${2:?--base needs a ref}" ;;
  '') : ;;
  *) echo "usage: $0 [--base <ref>] | --vectors" >&2; exit 2 ;;
esac
[ -f "$FILE" ] || { echo "FAIL: $FILE is missing"; exit 1; }
python3 scripts/ci/zerodte_attestation.py --vectors >/dev/null || { echo "FAIL: the attestation port disagrees with the Java golden vectors — nothing it says about the chain can be trusted"; exit 1; }
if git rev-parse --verify -q "$BASE" >/dev/null 2>&1; then
  python3 scripts/ci/zerodte_attestation.py verify "$FILE" --base "$BASE"
else
  echo "base $BASE is not a known ref here (a fresh clone without the remote?): judging the file alone, NOT append-only" >&2
  python3 scripts/ci/zerodte_attestation.py verify "$FILE"
fi
echo "=== validate-zerodte-attestation: OK ==="
