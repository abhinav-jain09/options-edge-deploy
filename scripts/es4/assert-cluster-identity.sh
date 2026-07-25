#!/usr/bin/env bash
# assert-cluster-identity.sh — prove a kubeconfig points at the cluster you think it does,
# BEFORE running a destructive command against it.
#
# WHY THIS EXISTS: the es4 pipeline runs destructive kubectl against TWO clusters in one job
# (ES4_KUBECONFIG and PROD_KUBECONFIG). Nothing previously verified they were actually different
# clusters. If PROD_KUBECONFIG were ever edited, copied, or regenerated to point at .4, a
# "delete the prod es-feed" step would silently delete the LIVE es4 es-feed instead — and the
# ConfigMap applied moments earlier in the same stage.
#
# The check is fail-closed and uses a positive discriminator rather than a URL string match,
# because a URL can be an alias while still resolving to the same API server:
#   * prod  MUST contain Deployment `options-edge-databento-feed`  (the SPX feed — prod only)
#   * es4   MUST NOT contain it
# Both clusters share the `options-edge` namespace and both host(ed) an `es-feed`, so es-feed
# itself is NOT a usable discriminator; the SPX feed is.
set -euo pipefail

KUBECONFIG_PATH=${1:?usage: assert-cluster-identity.sh <kubeconfig> <expect: prod|es4>}
EXPECT=${2:?missing expectation (prod|es4)}

fail() { echo "CLUSTER IDENTITY CHECK FAILED: $*" >&2; exit 1; }

KUBECONFIG="$KUBECONFIG_PATH" kubectl -n options-edge get deploy >/dev/null 2>&1 \
  || fail "cannot reach the cluster via $KUBECONFIG_PATH"

set +e
KUBECONFIG="$KUBECONFIG_PATH" kubectl -n options-edge get deploy options-edge-databento-feed >/dev/null 2>&1
spx_rc=$?
set -e

case "$EXPECT" in
  prod)
    [ "$spx_rc" -eq 0 ] || fail "$KUBECONFIG_PATH was expected to be the PROD cluster, but Deployment options-edge-databento-feed (the SPX feed) is absent. Refusing to run a destructive command against an unidentified cluster."
    ;;
  es4)
    [ "$spx_rc" -ne 0 ] || fail "$KUBECONFIG_PATH was expected to be the ES4 cluster, but Deployment options-edge-databento-feed (the SPX feed) is present — this looks like the PROD cluster."
    ;;
  *) fail "expectation must be prod or es4 (got '$EXPECT')" ;;
esac

echo "cluster identity OK: $KUBECONFIG_PATH is the $EXPECT cluster"
