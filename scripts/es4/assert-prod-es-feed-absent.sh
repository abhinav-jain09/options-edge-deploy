#!/usr/bin/env bash
# assert-prod-es-feed-absent.sh — prove the prod cluster holds NO es-feed, deployment or pod.
#
# DBP-R33 retires the prod-cluster es-feed so that only one location can ever run one. Two paths
# need that proven rather than assumed:
#
#  1. clean-reset. scripts/es4/cleanup-es4.sh refuses to run unless told ES4_FEED_FENCED=1, and
#     documents that value as meaning "the cross-cluster prod es-feed producer has been quiesced".
#     Once the Jenkins stage stopped fencing prod, it was still passing ES4_FEED_FENCED=1 — an
#     unconditional assertion of something nobody had checked. A prod es-feed that exists and runs
#     can publish straight through the wipe and recreate the dead-schema-id poison the whole reset
#     exists to cure.
#  2. activate-es-feed. Activation does not go through the deploy stage that reconcile-deletes prod,
#     so it could start es4 while a prod Deployment still existed.
#
# Fail-closed throughout: unreachable cluster, unreadable state, or any pod present is a failure.
# Identity is verified first, so this can never be satisfied by pointing at the wrong cluster.
set -euo pipefail

PROD_KUBECONFIG_PATH=${1:?usage: assert-prod-es-feed-absent.sh <prod-kubeconfig>}
HERE="$(cd "$(dirname "$0")" && pwd)"

bash "$HERE/assert-cluster-identity.sh" "$PROD_KUBECONFIG_PATH" prod

fail() { echo "PROD ES-FEED ABSENCE CHECK FAILED: $*" >&2; exit 1; }

# Distinguish "not found" from "the query failed" — collapsing them is fail-open.
set +e
out=$(KUBECONFIG="$PROD_KUBECONFIG_PATH" kubectl -n options-edge get deploy es-feed -o name 2>&1)
rc=$?
set -e
if [ "$rc" -eq 0 ]; then
  fail "prod still has a Deployment es-feed. It must be deleted (DBP-R33) before a clean-reset or an activation."
elif ! printf '%s' "$out" | grep -qiE 'not ?found'; then
  fail "could not determine whether prod has an es-feed Deployment (kubectl rc=$rc: $out) — failing closed"
fi

# A Deployment can be gone while its pods are still terminating, and a terminating pod still
# produces to es4 Kafka during its grace period.
pods=$(KUBECONFIG="$PROD_KUBECONFIG_PATH" kubectl -n options-edge get pods -l app.kubernetes.io/name=es-feed --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "${pods:-0}" = "0" ] || fail "$pods es-feed pod(s) still present on prod — wait for deletion"

echo "prod es-feed absence OK: no Deployment, no pods"
