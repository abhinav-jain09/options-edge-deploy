#!/usr/bin/env bash
# prod-start-prereqs.sh -- fail-closed pre-start prerequisites for production services that depend on a registry
# subject registered under a FIXED id. Prints, space separated on stdout, the deployments that must NOT be started
# now (empty = nothing blocked); the reasons go to stderr. Never mutates anything.
#
# Rule: a service whose subject is missing (or sits under another id), or whose registry cannot be asked, stays at 0.
# Today: reversal-confirmation-service needs com.optionsedge.contracts.reversalcandidate.ReversalTopCandidate under id 2901
# (REVERSAL_TOP_CANDIDATE_ENABLED=true makes it a canonical producer that refuses to start without it).
# Register it (registry up, nothing else started):
#   SCHEMA_REGISTRY_URL=http://192.168.100.252:8082 SCHEMAS_DIR=<contracts>/schemas/reversal-candidate \
#   SCHEMA_ID_MAP=scripts/kafka/schema-id-maps/reversal-top-candidate.map bash scripts/kafka/ensure-fixed-id-schemas.sh
# Input: PREREQ_REGISTRY (default http://127.0.0.1:8082, the registry on the prod host).
set -uo pipefail
REG="${PREREQ_REGISTRY:-http://127.0.0.1:8082}"
HOLD=""
check() { # deployment subject expected-id
  local dep="$1" subj="$2" want="$3" out got
  if ! out=$(curl -sf -m 8 "${REG%/}/subjects/${subj}/versions/latest" 2>/dev/null); then
    echo "prod-start-prereqs: HOLD $dep -- subject $subj not readable on $REG (registry down or subject absent)" >&2
    HOLD="$HOLD $dep"; return
  fi
  got=$(printf '%s' "$out" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("id",""))' 2>/dev/null || true)
  if [ "$got" != "$want" ]; then
    echo "prod-start-prereqs: HOLD $dep -- subject $subj has id '$got', expected $want" >&2
    HOLD="$HOLD $dep"
  fi
}
check reversal-confirmation-service com.optionsedge.contracts.reversalcandidate.ReversalTopCandidate 2901
printf '%s\n' "${HOLD# }"
