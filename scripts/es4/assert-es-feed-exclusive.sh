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
# ⚠️ WHAT THIS IS NOT: an atomic lease. There remains a time-of-check/time-of-use window.
# DBP-R28 (a publisher lease with a fencing token) was SUPERSEDED by DBP-R33: rather than arbitrate
# between two locations, the prod-cluster es-feed is deleted so only one location exists. This script
# therefore now ENFORCES that absence for prod rather than tolerating a parked Deployment. The
# residual TOCTOU window is accepted because there is no second Deployment to race with.
set -euo pipefail

ES4_KUBECONFIG=${1:?usage: assert-es-feed-exclusive.sh <es4-kubeconfig> <prod-kubeconfig> <target: es4>}
PROD_KUBECONFIG=${2:?missing prod kubeconfig}
TARGET=${3:?missing target (es4)}

# DBP-R33 removed prod as a location entirely, so target=prod is no longer a legal request. Leaving
# it accepted contradicted the very guarantee this script is supposed to enforce.
case "$TARGET" in
  es4) other_kc="$PROD_KUBECONFIG"; other_name="prod"; self_kc="$ES4_KUBECONFIG" ;;
  *) echo "assert-es-feed-exclusive: target must be es4 (got '$TARGET'); prod was retired by DBP-R33" >&2; exit 1 ;;
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

# Distinguish the three outcomes explicitly. A bare `if kubectl get ...` collapses
# "does not exist" and "call failed" into one branch, which is fail-open: a transient API or RBAC
# error would be read as "the other location is absent, go ahead".
# --ignore-not-found removes text matching: rc=0 + empty means absent, rc=0 + output means present,
# any non-zero rc is a real failure. Matching on "not found" text was ambiguous.
set +e
other_out=$(KUBECONFIG="$other_kc" kubectl -n options-edge get deploy es-feed --ignore-not-found -o name 2>&1)
other_rc=$?
set -e
[ "$other_rc" -eq 0 ] || fail "could not determine $other_name es-feed state (rc=$other_rc: $other_out) — failing closed"
if [ -n "$other_out" ]; then
  # DBP-R33: prod must not have an es-feed Deployment AT ALL, not merely one at replicas 0.
  fail "$other_name still has an es-feed Deployment ($other_out). DBP-R33 requires it deleted, not parked at 0 — run an es4 deploy so the reconcile-delete removes it."
fi
echo "es-feed exclusivity: no es-feed Deployment in $other_name (checking for stray pods anyway)"

# Pods are checked UNCONDITIONALLY — replicas 0 does not mean "no pod", and neither does a
# missing Deployment.
stray=$(KUBECONFIG="$other_kc" kubectl -n options-edge get pods -l app.kubernetes.io/name=es-feed \
          --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "${stray:-0}" = "0" ] || fail "$stray es-feed pod(s) still present in $other_name; wait for deletion before starting $TARGET"

echo "es-feed exclusivity OK: target=$TARGET, $other_name has no es-feed replicas and no es-feed pods, target secret usable"
echo "  NOTE: point-in-time observation. Safety rests on DBP-R33 — only one location has a Deployment at all."
