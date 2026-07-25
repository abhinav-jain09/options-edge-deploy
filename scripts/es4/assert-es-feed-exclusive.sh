#!/usr/bin/env bash
# assert-es-feed-exclusive.sh — prove that starting es-feed at $TARGET cannot create a second
# concurrent ES option-chain session on the single shared Databento API key.
#
# WHY THIS IS A SEPARATE SCRIPT: the same proof is needed by the deploy preflight, by the
# activate action, and by clean-reset. Duplicating it inline three times is how one copy drifts
# and silently becomes fail-open — which is exactly the defect Codex found in the first version.
#
# FAIL-CLOSED CONTRACT. Every unknown is a failure:
#   * a cluster we cannot reach            -> FAIL (never "assume it is safe")
#   * a Deployment GET that errors while the cluster IS reachable -> FAIL (a transient
#     GET/RBAC error must not be indistinguishable from "absent")
#   * replicas > 0 at the other location   -> FAIL
#   * ANY pod still present at the other location, even with replicas 0 and even if the
#     Deployment itself is gone -> FAIL (a terminating or orphaned pod still holds the session)
#   * a Secret that exists but carries no non-empty DATABENTO_API_KEY -> FAIL
#
# ⚠️ WHAT THIS IS NOT: an atomic lease. There remains a time-of-check/time-of-use window, and
# nothing here prevents a second Jenkins job, a manual `kubectl scale`, or another controller from
# starting the other location a moment later. `disableConcurrentBuilds()` only serialises THIS
# job. Requirement DBP-R28 / design ESM-R40 call for an external publisher lease with a fencing
# token; until that exists this script is a best-effort observation, and that limitation must not
# be described as a guarantee.
set -euo pipefail

ES4_KUBECONFIG=${1:?usage: assert-es-feed-exclusive.sh <es4-kubeconfig> <prod-kubeconfig> <target: es4|prod>}
PROD_KUBECONFIG=${2:?missing prod kubeconfig}
TARGET=${3:?missing target (es4|prod)}

case "$TARGET" in
  es4)  other_kc="$PROD_KUBECONFIG"; other_name="prod";  self_kc="$ES4_KUBECONFIG" ;;
  prod) other_kc="$ES4_KUBECONFIG";  other_name="es4";   self_kc="$PROD_KUBECONFIG" ;;
  *) echo "assert-es-feed-exclusive: target must be es4 or prod (got '$TARGET')" >&2; exit 1 ;;
esac

fail() { echo "ES-FEED EXCLUSIVITY CHECK FAILED: $*" >&2; exit 1; }

# --- the target cluster must hold a usable API-key secret ------------------------------------
KUBECONFIG="$self_kc" kubectl -n options-edge get deploy >/dev/null 2>&1 \
  || fail "cannot reach the $TARGET cluster to verify the es-feed secret"
KUBECONFIG="$self_kc" kubectl -n options-edge get secret options-edge-databento-feed-env >/dev/null 2>&1 \
  || fail "secret options-edge-databento-feed-env is absent in the $TARGET cluster (it carries DATABENTO_API_KEY)"
# Presence is not enough: config.py rejects an empty key at boot, so an empty Secret would only
# surface as a CrashLoop. Assert a non-empty value WITHOUT printing it.
key_len=$(KUBECONFIG="$self_kc" kubectl -n options-edge get secret options-edge-databento-feed-env \
            -o jsonpath='{.data.DATABENTO_API_KEY}' 2>/dev/null | wc -c | tr -d ' ')
[ "${key_len:-0}" -gt 0 ] || fail "secret options-edge-databento-feed-env has no non-empty DATABENTO_API_KEY in the $TARGET cluster"

# --- the OTHER location must be provably down ------------------------------------------------
KUBECONFIG="$other_kc" kubectl -n options-edge get deploy >/dev/null 2>&1 \
  || fail "cannot reach the $other_name cluster to prove es-feed is down there — refusing rather than assuming"

if KUBECONFIG="$other_kc" kubectl -n options-edge get deploy es-feed >/dev/null 2>&1; then
  other_rep=$(KUBECONFIG="$other_kc" kubectl -n options-edge get deploy es-feed -o jsonpath='{.spec.replicas}') \
    || fail "could not read $other_name es-feed replicas (reachable cluster, failed GET) — failing closed"
  [ -n "$other_rep" ] || fail "empty replica count for $other_name es-feed — failing closed"
  [ "$other_rep" = "0" ] || fail "$other_name es-feed is at replicas=$other_rep; one shared Databento key cannot carry two concurrent ES option-chain sessions (and a second ES session can degrade the SPX OPRA session sharing that key)"
else
  # Deployment absent is acceptable, but pods are still checked below: a Deployment can be
  # deleted while its pods are still terminating, and an orphaned pod still holds the session.
  echo "es-feed exclusivity: no es-feed Deployment in $other_name (checking for stray pods anyway)"
fi

# Pods are checked UNCONDITIONALLY — replicas 0 does not mean "no pod", and neither does a
# missing Deployment.
stray=$(KUBECONFIG="$other_kc" kubectl -n options-edge get pods -l app.kubernetes.io/name=es-feed \
          --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "${stray:-0}" = "0" ] || fail "$stray es-feed pod(s) still present in $other_name; wait for deletion before starting $TARGET"

echo "es-feed exclusivity OK: target=$TARGET, $other_name has no es-feed replicas and no es-feed pods, target secret usable"
echo "  NOTE: this is a point-in-time observation, not an atomic lease (see DBP-R28 / ESM-R40)."
