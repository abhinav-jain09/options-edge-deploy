#!/usr/bin/env bash
# Fetch the pods and run oom.py, as ONE command the deliverer can wrap (see crash-loop-run.sh for why the fetch is inside).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${FLEET_NAMESPACE:-options-edge}"
pods="$(mktemp)"
err="$(mktemp)"
trap 'rm -f "$pods" "$err"' EXIT
if ! kubectl --request-timeout=20s -n "$NS" get pods -o json > "$pods" 2> "$err"; then
  echo "ERROR: kubectl -n $NS get pods failed: $(tr '\n' ' ' < "$err" | cut -c1-300)"
  exit 2
fi
python3 "$HERE/oom.py" --input "$pods" "$@"
