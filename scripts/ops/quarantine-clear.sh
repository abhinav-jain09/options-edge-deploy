#!/usr/bin/env bash
# Clear a restart-budget quarantine: remove the annotations and bring the deployment back to the replica count it had before it was stopped.
#
#   scripts/ops/quarantine-clear.sh dev level-absorption-service            # restores the recorded replicas
#   scripts/ops/quarantine-clear.sh dev level-absorption-service --replicas 2
#   scripts/ops/quarantine-clear.sh --list dev                              # what is quarantined, and why
#
# The fleet-watch "Restart budget" stage (scripts/watch/restart-budget.py) scales a deployment to 0 and annotates it options-edge.io/quarantined-at
# when its pods restart more than 5 times in 60 minutes or more than 5 times in 24 hours. It STAYS at 0: the autostart and bring-up scripts skip it
# (scripts/ops/quarantine-lib.sh) and the stage keeps reporting it. Fix the cause first (an OOM limit, a bad image), then run this ONE command.
# Its restart history was cleared when it was stopped, so clearing it does not stop it again on the old restarts.
#
# env is dev | es4 | production. Production needs the jenkins-deployer identity (the cluster's admission policy lets only that service account
# scale Deployments), so the kubeconfigs are the deployer ones fleet-watch uses. KUBECTL / KUBECONFIG override both, for the tests and for
# running on the prod host.
set -uo pipefail

NS="${FLEET_NAMESPACE:-options-edge}"
KUBECTL="${KUBECTL:-kubectl}"
Q_AT="options-edge.io/quarantined-at"
Q_WHY="options-edge.io/quarantine-reason"
Q_PREV="options-edge.io/quarantine-prev-replicas"

usage() { echo "usage: $0 [--list] <dev|es4|production> [<deployment> [--replicas N]]" >&2; exit 2; }

LIST=0
if [ "${1:-}" = "--list" ]; then LIST=1; shift; fi
ENVN="${1:-}"; [ -n "$ENVN" ] || usage
shift
case "$ENVN" in
  dev)             KC="/Users/abhinav/.kube/jenkins-deployer-dev-remote.kubeconfig" ;;
  es4)             KC="/Users/abhinav/.kube/es4-jenkins-deployer.kubeconfig" ;;
  production|prod) KC="/Users/abhinav/.kube/jenkins-deployer-prod.kubeconfig" ;;
  *) echo "unknown environment '$ENVN' (dev | es4 | production)" >&2; exit 2 ;;
esac
[ -n "${KUBECONFIG:-}" ] || export KUBECONFIG="$KC"

k() { "$KUBECTL" -n "$NS" "$@"; }

if [ "$LIST" = 1 ]; then
  out="$(k get deploy -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.options-edge\.io/quarantined-at}{"\t"}{.metadata.annotations.options-edge\.io/quarantine-prev-replicas}{"\t"}{.metadata.annotations.options-edge\.io/quarantine-reason}{"\n"}{end}')" || { echo "kubectl failed" >&2; exit 2; }
  printf '%s\n' "$out" | awk -F'\t' '$2 != "" { printf "%s  stopped %s (was %s replica(s)): %s\n", $1, $2, $3, $4; n++ } END { if (!n) print "nothing is quarantined" }'
  exit 0
fi

DEP="${1:-}"; [ -n "$DEP" ] || usage
shift
REPLICAS=""
if [ "${1:-}" = "--replicas" ]; then
  REPLICAS="${2:-}"
  case "$REPLICAS" in '' | *[!0-9]*) echo "--replicas needs a whole number (got '$REPLICAS')" >&2; exit 2 ;; esac
  shift 2
fi
[ "$#" -eq 0 ] || usage

at="$(k get "deploy/$DEP" -o jsonpath='{.metadata.annotations.options-edge\.io/quarantined-at}')" || { echo "kubectl could not read deploy/$DEP" >&2; exit 2; }
if [ -z "$at" ]; then
  echo "deploy/$DEP is not quarantined (no $Q_AT annotation) - nothing to clear"
  exit 0
fi
prev="$(k get "deploy/$DEP" -o jsonpath='{.metadata.annotations.options-edge\.io/quarantine-prev-replicas}')" || exit 2
why="$(k get "deploy/$DEP" -o jsonpath='{.metadata.annotations.options-edge\.io/quarantine-reason}')" || exit 2
[ -n "$REPLICAS" ] || REPLICAS="$prev"
case "$REPLICAS" in '' | *[!0-9]*) REPLICAS=1; echo "no usable recorded replica count, using 1" ;; esac
echo "clearing deploy/$DEP (stopped $at: $why); restoring $REPLICAS replica(s)"
# Scale first, then drop the marker: a failure between the two leaves the marker on a running deployment, which the budget check tolerates
# (it only stops a deployment that is restarting again), instead of an unmarked deployment at 0 that autostart would raise on its own.
k scale "deploy/$DEP" --replicas="$REPLICAS" || { echo "scale failed - the quarantine is still in place" >&2; exit 1; }
k annotate "deploy/$DEP" "$Q_AT-" "$Q_WHY-" "$Q_PREV-" || { echo "scaled to $REPLICAS but the annotations could not be removed - remove them by hand" >&2; exit 1; }
echo "done: deploy/$DEP is back at $REPLICAS replica(s) and no longer quarantined"
