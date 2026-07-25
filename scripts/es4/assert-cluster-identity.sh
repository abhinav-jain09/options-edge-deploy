#!/usr/bin/env bash
# assert-cluster-identity.sh — prove two kubeconfigs address DIFFERENT clusters before running a
# cross-cluster destructive command.
#
# WHY: the es4 pipeline runs destructive kubectl against two clusters in one job. If PROD_KUBECONFIG
# were ever regenerated to point at .4, a "delete the prod es-feed" step would delete the LIVE es4
# feed instead. Nothing previously checked.
#
# ⚠️ An earlier version discriminated by "does the SPX Deployment exist here". That was rejected in
# review for two sound reasons: it is MUTABLE (deleting the SPX Deployment for any reason would
# deadlock every es4 deploy, activation and clean-reset), and a named-GET that failed for RBAC or
# timeout reasons was read as proof of the opposite cluster — fail-open on the exact check meant to
# authorise a deletion.
#
# This version uses the UID of the `options-edge` NAMESPACE: assigned at namespace creation, stable
# for its life, and not mutable by any application deploy. Two kubeconfigs resolving to the same UID
# are the same cluster, whatever their server URLs say (a URL can alias). Every error path fails
# closed — an unreadable UID is never treated as "different".
#
# ⚠️ It deliberately does NOT use the kube-system namespace, which would be the more conventional
# cluster identifier: the Jenkins deploy identity is namespace-scoped
# (system:serviceaccount:options-edge:jenkins-deployer) and is Forbidden from reading kube-system.
# That was found by running this guard against the real kubeconfigs, not by inspection — a
# kube-system-based check would have failed closed on every single build.
set -euo pipefail

A_KUBECONFIG=${1:?usage: assert-cluster-identity.sh <kubeconfig-a> <kubeconfig-b> <label-a> <label-b>}
B_KUBECONFIG=${2:?missing second kubeconfig}
A_LABEL=${3:-A}
B_LABEL=${4:-B}

fail() { echo "CLUSTER IDENTITY CHECK FAILED: $*" >&2; exit 1; }

cluster_uid() {
  local kc=$1 label=$2 out rc
  set +e
  out=$(KUBECONFIG="$kc" kubectl get namespace options-edge -o jsonpath='{.metadata.uid}' 2>&1)
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || fail "cannot read the cluster identity of $label ($kc): rc=$rc: $out"
  [ -n "$out" ] || fail "empty cluster identity for $label ($kc)"
  case "$out" in *[!0-9a-fA-F-]*) fail "implausible cluster identity for $label ($kc): '$out'";; esac
  printf '%s' "$out"
}

a_uid=$(cluster_uid "$A_KUBECONFIG" "$A_LABEL")
b_uid=$(cluster_uid "$B_KUBECONFIG" "$B_LABEL")

[ "$a_uid" != "$b_uid" ] || fail "$A_LABEL and $B_LABEL resolve to the SAME cluster (options-edge namespace uid $a_uid). Refusing to run a cross-cluster destructive command."

echo "cluster identity OK: $A_LABEL and $B_LABEL are distinct clusters"
