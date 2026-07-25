#!/usr/bin/env bash
# assert-prod-es-feed-absent.sh — prove the prod cluster runs no es-feed before a clean-reset or an
# es4 activation.
#
# WHY BOTH CALLERS NEED IT:
#  1. clean-reset. cleanup-es4.sh refuses to run unless told ES4_FEED_FENCED=1, which it documents as
#     "the cross-cluster prod producer has been quiesced". Once the Jenkins stage stopped fencing
#     prod it kept passing that flag — an assertion nobody checked. A surviving prod es-feed can
#     publish straight through the wipe and recreate the dead-schema-id poison the reset exists to
#     cure.
#  2. activate-es-feed does not pass through the deploy stage that reconcile-deletes prod, so without
#     this it could start es4 while a prod Deployment still existed.
#
# ⚠️ SCOPE LIMITATION, stated rather than papered over. The Jenkins identity is namespace-scoped
# (system:serviceaccount:options-edge:jenkins-deployer) and is Forbidden from listing at cluster
# scope, so `--all-namespaces` is not available. This proves absence in `options-edge` ONLY. An
# es-feed deliberately deployed to a different namespace on prod would not be seen. Closing that gap
# requires broader RBAC, which is a separate decision — it is NOT closed here, and no caller should
# claim cluster-wide absence.
#
# ⚠️ Every check runs in THIS shell. An earlier version wrapped kubectl in a helper called from a
# command substitution; `exit` inside a command substitution only leaves the subshell, so every
# failure was swallowed and the guard passed while prod still had a Deployment. Verified live.
set -euo pipefail

PROD_KUBECONFIG_PATH=${1:?usage: assert-prod-es-feed-absent.sh <prod-kubeconfig>}
NS=options-edge

fail() { echo "PROD ES-FEED ABSENCE CHECK FAILED: $*" >&2; exit 1; }

KUBECONFIG="$PROD_KUBECONFIG_PATH" kubectl -n "$NS" get deploy >/dev/null 2>&1 \
  || fail "cannot reach the prod cluster namespace $NS via $PROD_KUBECONFIG_PATH"

# --- Deployment: distinguish not-found from a failed query; anything else fails closed -----------
# --ignore-not-found removes the text-matching entirely: rc=0 with EMPTY output means absent, rc=0
# with output means present, and any non-zero rc is a real failure. Matching on "not found" text was
# ambiguous — other API errors can contain those words.
set +e
dep_out=$(KUBECONFIG="$PROD_KUBECONFIG_PATH" kubectl -n "$NS" get deploy es-feed --ignore-not-found -o name 2>&1)
dep_rc=$?
set -e
[ "$dep_rc" -eq 0 ] || fail "could not query prod $NS for an es-feed Deployment (rc=$dep_rc: $dep_out) — failing closed"
[ -z "$dep_out" ] || fail "prod $NS still has a Deployment es-feed ($dep_out). DBP-R33 requires it DELETED, not parked at 0 — run an es4 deploy so the reconcile-delete removes it."

# --- Pods: a Deployment can be gone while a pod is still terminating, and a terminating pod keeps
#     producing to es4 Kafka for its whole grace period. Check by label AND by name prefix, so a
#     stray pod that lost its labels is still caught.
set +e
pod_out=$(KUBECONFIG="$PROD_KUBECONFIG_PATH" kubectl -n "$NS" get pods -o name 2>&1)
pod_rc=$?
set -e
[ "$pod_rc" -eq 0 ] || fail "could not list pods in prod $NS (rc=$pod_rc: $pod_out) — failing closed"
stray=$(printf '%s\n' "$pod_out" | grep -c '^pod/es-feed-' || true)
[ "${stray:-0}" = "0" ] || fail "$stray pod(s) named es-feed-* still present in prod $NS — wait for deletion"

set +e
lbl_out=$(KUBECONFIG="$PROD_KUBECONFIG_PATH" kubectl -n "$NS" get pods -l app.kubernetes.io/name=es-feed -o name 2>&1)
lbl_rc=$?
set -e
[ "$lbl_rc" -eq 0 ] || fail "could not list labelled es-feed pods in prod $NS (rc=$lbl_rc: $lbl_out) — failing closed"
labelled=$(printf '%s\n' "$lbl_out" | grep -c '^pod/' || true)
[ "${labelled:-0}" = "0" ] || fail "$labelled es-feed pod(s) still present in prod $NS — wait for deletion"

echo "prod es-feed absence OK in namespace $NS (no Deployment, no pods). Cluster-wide absence is NOT proven — see the scope limitation in this script's header."
