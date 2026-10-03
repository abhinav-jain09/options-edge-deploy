#!/usr/bin/env bash
# The 0DTE VIRGIN attestation (deploy/zerodte/virgin-attestation.yaml; design §5.1, increment 7 Q5) is APPEND-ONLY and hash-chained:
# this is the `attestation-append-only` check the design names. It runs the Job's own rules (scripts/ci/zerodte_attestation.py, held
# to the Java implementation by golden vectors) and then compares the file with its BASE version: every lineage and every entry the base
# held must still be there, unchanged and in order — a removed or altered row, or a broken prevEntryHash chain, refuses the change.
#
# Usage: scripts/ci/validate-zerodte-attestation.sh [--base <git ref>]     default base: origin/main (the merge target). A base this
#        scripts/ci/validate-zerodte-attestation.sh --vectors               checkout does not hold is a REFUSAL (fail closed), never a
#        scripts/ci/validate-zerodte-attestation.sh --corpus                shape-only pass; a base that does not hold the FILE (a new file)
#                                                                           compares against nothing.
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
python3 scripts/ci/zerodte_attestation.py --corpus >/dev/null || { echo "FAIL: the attestation port disagrees with the shared YAML-subset corpus — the Job would judge the file differently"; exit 1; }
# FAIL CLOSED: the append-only property can only be judged against the base; a base this clone does not hold is a refusal, never a shape-only pass.
git rev-parse --verify -q "$BASE" >/dev/null 2>&1 \
  || { echo "FAIL: base $BASE is not a known ref in this checkout — fetch it (git fetch origin main) before judging the attestation; append-only cannot be judged without it"; exit 1; }
python3 scripts/ci/zerodte_attestation.py verify "$FILE" --base "$BASE"
echo "=== validate-zerodte-attestation: OK ==="
