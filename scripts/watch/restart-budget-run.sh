#!/usr/bin/env bash
# Fetch the pods and deployments and run restart-budget.py, as ONE command the deliverer can wrap.
#
#   KUBECONFIG=... FLEET_ENV=dev RESTART_BUDGET_MODE=enforce scripts/watch/restart-budget-run.sh
#
# WHY A WRAPPER. Same reason as crash-loop-run.sh: a failed `kubectl get` inside the Jenkinsfile would exit the shell BEFORE the deliverer ran, and
# "the check could not run" is the one state that must always reach Discord. Here it is an ordinary exit 2 carrying kubectl's own words. The baseline
# directory is resolved here for the same reason as stateful-check-run.sh (a directory that cannot be created is an exit 2, never a silent skip).
#
# RESTART_BUDGET_MODE: enforce (stop a deployment over budget) or dry-run (print what would happen, change nothing). Anything else is exit 2:
# a typo must not silently turn enforcement off OR on.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${FLEET_NAMESPACE:-options-edge}"
FLEET_ENV="${FLEET_ENV:?FLEET_ENV must be set}"
MODE="${RESTART_BUDGET_MODE:-dry-run}"
case "$MODE" in
  enforce | dry-run) ;;
  *) echo "ERROR: RESTART_BUDGET_MODE must be enforce or dry-run (got '$MODE')"; exit 2 ;;
esac

STATE="${FLEET_STATE_DIR:-}"
if [ -z "$STATE" ]; then
  if [ -z "${WORKSPACE:-}" ]; then
    echo "ERROR: neither FLEET_STATE_DIR nor WORKSPACE is set - cannot locate the baseline directory"
    exit 2
  fi
  STATE="$(dirname "$WORKSPACE")/.oe-watch"
fi

. "$HERE/kube-reachable.sh"
oe_kube_reachable || exit 2
pods="$(mktemp)"
deploys="$(mktemp)"
err="$(mktemp)"
trap 'rm -f "$pods" "$deploys" "$err"' EXIT

for what in pods deployments; do
  out="$pods"; [ "$what" = deployments ] && out="$deploys"
  if ! kubectl -n "$NS" get "$what" -o json > "$out" 2> "$err"; then
    echo "ERROR: kubectl -n $NS get $what failed: $(tr '\n' ' ' < "$err" | cut -c1-300)"
    exit 2
  fi
done
exec python3 "$HERE/restart-budget.py" --env "$FLEET_ENV" --pods "$pods" --deploys "$deploys" --state "$STATE" \
  --mode "$MODE" --namespace "$NS" "$@"
