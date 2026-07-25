#!/usr/bin/env bash
# assert-cluster-identity.sh — prove a kubeconfig addresses the cluster with the EXPECTED ROLE,
# before running a destructive command through it.
#
# WHY ROLE AND NOT JUST "DIFFERENT": an earlier version only proved the two kubeconfigs resolved to
# distinct clusters. Swap them and every check still passed — then the reconcile-delete would run
# against es4 and remove the LIVE feed. Distinctness is necessary but not sufficient; the guard must
# know which cluster is which.
#
# HOW: each cluster is identified by the UID of its `options-edge` namespace — assigned at namespace
# creation, stable for its life, unaffected by any application deploy. The expected UID per role is
# PINNED by the caller (Jenkinsfile environment). Pinning is the point: an unrecognised UID fails
# closed rather than being trusted. If a cluster is ever rebuilt the pin must be updated, which is
# correct for a value that authorises deletion.
#
# ⚠️ It deliberately does NOT use kube-system, the conventional cluster identifier: the deploy
# identity is namespace-scoped (system:serviceaccount:options-edge:jenkins-deployer) and is Forbidden
# from reading it. Verified against the live clusters — a kube-system check fails closed on every
# build. Every error path here fails closed; an unreadable UID is never treated as a match.
set -euo pipefail

KUBECONFIG_PATH=${1:?usage: assert-cluster-identity.sh <kubeconfig> <role-label> <expected-ns-uid>}
ROLE=${2:?missing role label}
EXPECTED_UID=${3:?missing expected options-edge namespace UID for role '"$ROLE"'}

fail() { echo "CLUSTER IDENTITY CHECK FAILED: $*" >&2; exit 1; }

set +e
uid=$(KUBECONFIG="$KUBECONFIG_PATH" kubectl get namespace options-edge -o jsonpath='{.metadata.uid}' 2>&1)
rc=$?
set -e
[ "$rc" -eq 0 ] || fail "cannot read the identity of the cluster behind $KUBECONFIG_PATH (rc=$rc: $uid)"
[ -n "$uid" ] || fail "empty options-edge namespace UID for $KUBECONFIG_PATH"
case "$uid" in *[!0-9a-fA-F-]*) fail "implausible UID '$uid' for $KUBECONFIG_PATH";; esac

[ "$uid" = "$EXPECTED_UID" ] || fail "$KUBECONFIG_PATH is NOT the '$ROLE' cluster: options-edge namespace UID is $uid, expected $EXPECTED_UID. Refusing to run a destructive command against an unidentified cluster (kubeconfigs swapped, regenerated, or a cluster rebuilt)."

echo "cluster identity OK: $KUBECONFIG_PATH is the '$ROLE' cluster ($uid)"
