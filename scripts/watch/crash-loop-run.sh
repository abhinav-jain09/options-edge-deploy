#!/usr/bin/env bash
# Fetch the pods and run crash-loop.py, as ONE command the deliverer can wrap.
#
# WHY A WRAPPER. If the Jenkinsfile ran `kubectl get` itself and exited 2 on failure, that exit would
# never reach fleet-alert-deliver.sh and the most important state - "the check could not run" - would be
# the one state that sent NO Discord message (R1e says exit 2 is always delivered). Doing the fetch inside
# the wrapped command makes an unreachable cluster an ordinary exit 2, carrying kubectl's own words.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${FLEET_NAMESPACE:-options-edge}"

pods="$(mktemp)"
err="$(mktemp)"
trap 'rm -f "$pods" "$err"' EXIT

if ! kubectl -n "$NS" get pods -o json > "$pods" 2> "$err"; then
  # The ERROR: prefix is what fleet-alert.py reads for the "could not check" reason.
  echo "ERROR: kubectl -n $NS get pods failed: $(tr '\n' ' ' < "$err" | cut -c1-300)"
  exit 2
fi
exec python3 "$HERE/crash-loop.py" --input "$pods" "$@"
