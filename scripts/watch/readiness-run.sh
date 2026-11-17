#!/usr/bin/env bash
# Fetch the workloads and run fleet-readiness.py, as ONE command the deliverer can wrap.
#
#   KUBECONFIG=... FLEET_ENV=dev [STUCK_MINUTES=30] scripts/watch/readiness-run.sh
#
# Same reason as crash-loop-run.sh: a kubectl failure, an unwritable state directory or a missing file must be an ordinary exit 2 INSIDE the
# wrapped command, so fleet-alert-deliver.sh can say "could not check" in Discord. The readiness clock for an un-timestamped StatefulSet rollout
# lives beside the job workspaces (fleet-<env>.json), the same file the Jenkinsfile used before this check paged.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${FLEET_NAMESPACE:-options-edge}"
FLEET_ENV="${FLEET_ENV:?FLEET_ENV must be set}"
MINUTES="${STUCK_MINUTES:-30}"
case "$MINUTES" in '' | *[!0-9]*) echo "ERROR: STUCK_MINUTES must be a number of minutes (got '$MINUTES')"; exit 2 ;; esac

STATE="${FLEET_STATE_DIR:-}"
if [ -z "$STATE" ]; then
  [ -n "${WORKSPACE:-}" ] || { echo "ERROR: neither FLEET_STATE_DIR nor WORKSPACE is set - cannot locate the readiness clock directory"; exit 2; }
  STATE="$(dirname "$WORKSPACE")/.oe-watch"
fi
mkdir -p "$STATE" 2>/dev/null || { echo "ERROR: cannot create the state directory $STATE"; exit 2; }
[ -w "$STATE" ] || { echo "ERROR: the state directory $STATE is not writable"; exit 2; }

out="$(mktemp)"
err="$(mktemp)"
trap 'rm -f "$out" "$err"' EXIT
if ! kubectl --request-timeout=20s -n "$NS" get deploy,statefulset,rs,pods -o json > "$out" 2> "$err"; then
  echo "ERROR: kubectl -n $NS get deploy,statefulset,rs,pods failed: $(tr '\n' ' ' < "$err" | cut -c1-300)"
  exit 2
fi
echo "=== $FLEET_ENV ==="
python3 "$HERE/fleet-readiness.py" --input "$out" --minutes "$MINUTES" --state "$STATE/fleet-$FLEET_ENV.json" \
  --expected-unready "$HERE/expected-unready.json"
