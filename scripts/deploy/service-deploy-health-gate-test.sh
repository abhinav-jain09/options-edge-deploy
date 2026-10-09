#!/usr/bin/env bash
# Regression fixture for the post-rollout health gate in service-deploy.sh (BEGIN/END HEALTH_GATE
# markers), driven against a mocked kubectl so it never touches a real cluster.
#
# THE BUG THIS PINS (web-service-deploy build #773, 2026-10-09): `kubectl rollout status` returns as
# soon as the Deployment condition is Available, but the OUTGOING ReplicaSet's pod can still report
# phase=Running for several more seconds while it terminates. The gate's ORIGINAL bare label-selector
# query matched that pod alongside the brand-new one, and `sort -rn | head -1` on restartCount took
# whichever was higher — a 2-day-old outgoing pod's unrelated history failed a rollout that introduced
# zero new crashes. Case 1 below reproduces exactly that pod set and asserts the gate now passes.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

# The gate block, verbatim, extracted between its own markers so this test tracks the real script
# instead of a copy that can drift out of sync with it.
GATE_SCRIPT="$(sed -n '/^# BEGIN HEALTH_GATE/,/^# END HEALTH_GATE$/p' "$HERE/service-deploy.sh")"
[ -n "$GATE_SCRIPT" ] || { echo "FATAL: BEGIN/END HEALTH_GATE markers not found in service-deploy.sh" >&2; exit 1; }

# run_gate <scenario-mock-kubectl-script> -> sets $GATE_FAIL, $GATE_OUT (stdout+stderr)
run_gate() {
  local tmp mockbin
  tmp="$(mktemp -d)"; mockbin="$tmp/bin"; mkdir -p "$mockbin"
  {
    echo '#!/usr/bin/env bash'
    printf '%s\n' "$1"
  } > "$mockbin/kubectl"
  chmod +x "$mockbin/kubectl"
  GATE_OUT="$(PATH="$mockbin:$PATH" NAMESPACE=options-edge DEPLOYMENTS=options-edge-web \
    PINNED_DIGEST=sha256:newdigest000000000000000000000000000000000000000000000000000new \
    bash -c "$GATE_SCRIPT
echo GATE_FAIL=\$gate_fail" 2>&1)"
  GATE_FAIL="$(printf '%s\n' "$GATE_OUT" | sed -n 's/^GATE_FAIL=//p')"
  rm -rf "$tmp"
}

echo "1. a terminating OLD pod's stale restartCount must not fail a healthy new rollout (the real bug)"
run_gate '
case "$*" in
  "-n options-edge get deployment options-edge-web -o jsonpath={.spec.replicas}")
    echo 1 ;;
  "-n options-edge get deployment options-edge-web -o json")
    echo "{\"spec\":{\"selector\":{\"matchLabels\":{\"app.kubernetes.io/name\":\"options-edge-web\"}}}}" ;;
  "-n options-edge get rs -l app.kubernetes.io/name=options-edge-web -o json")
    # old RS already scaled to 0 by the time rollout status returns (matches the real build #773
    # timeline); new RS is the only one with replicas>0.
    cat <<JSON
{"items":[
  {"metadata":{"name":"options-edge-web-76fc7b5687","creationTimestamp":"2026-10-07T22:59:48Z","labels":{"pod-template-hash":"76fc7b5687"}},"spec":{"replicas":0}},
  {"metadata":{"name":"options-edge-web-7d75d6cc75","creationTimestamp":"2026-10-09T13:30:34Z","labels":{"pod-template-hash":"7d75d6cc75"}},"spec":{"replicas":1}}
]}
JSON
    ;;
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web,pod-template-hash=7d75d6cc75 --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].imageID}{\"\\n\"}{end}")
    echo "docker-pullable://host.docker.internal:5001/options-edge-web@sha256:newdigest000000000000000000000000000000000000000000000000000new" ;;
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web,pod-template-hash=7d75d6cc75 --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].restartCount}{\"\\n\"}{end}")
    echo 0 ;;
  *) echo "unmocked kubectl call: $*" >&2; exit 1 ;;
esac
'
if [ "$GATE_FAIL" = "0" ]; then
  ok "gate_fail=0 — the current ReplicaSet (7d75d6cc75, restartCount=0) decides it, not the terminating one (76fc7b5687, which would have reported restartCount=2 under the old bare selector)"
else
  bad "expected gate_fail=0, got '$GATE_FAIL': $GATE_OUT"
fi

echo "2. no resolvable current ReplicaSet fails CLOSED, not open"
run_gate '
case "$*" in
  "-n options-edge get deployment options-edge-web -o jsonpath={.spec.replicas}")
    echo 1 ;;
  "-n options-edge get deployment options-edge-web -o json")
    echo "{\"spec\":{\"selector\":{\"matchLabels\":{\"app.kubernetes.io/name\":\"options-edge-web\"}}}}" ;;
  "-n options-edge get rs -l app.kubernetes.io/name=options-edge-web -o json")
    echo "{\"items\":[]}" ;;
  *) echo "unmocked kubectl call: $*" >&2; exit 1 ;;
esac
'
if [ "$GATE_FAIL" = "1" ]; then
  ok "gate_fail=1 — an unresolvable ReplicaSet hash refuses rather than silently falling back to the unscoped (buggy) selector"
else
  bad "expected gate_fail=1, got '$GATE_FAIL': $GATE_OUT"
fi

echo "3. sanity: a genuinely bad digest on the current ReplicaSet's pod still fails (the gate still catches real problems)"
run_gate '
case "$*" in
  "-n options-edge get deployment options-edge-web -o jsonpath={.spec.replicas}")
    echo 1 ;;
  "-n options-edge get deployment options-edge-web -o json")
    echo "{\"spec\":{\"selector\":{\"matchLabels\":{\"app.kubernetes.io/name\":\"options-edge-web\"}}}}" ;;
  "-n options-edge get rs -l app.kubernetes.io/name=options-edge-web -o json")
    cat <<JSON
{"items":[{"metadata":{"name":"options-edge-web-xyz","creationTimestamp":"2026-10-09T13:30:34Z","labels":{"pod-template-hash":"xyz"}},"spec":{"replicas":1}}]}
JSON
    ;;
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web,pod-template-hash=xyz --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].imageID}{\"\\n\"}{end}")
    echo "docker-pullable://host.docker.internal:5001/options-edge-web@sha256:wrongdigest0000000000000000000000000000000000000000000000wrong" ;;
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web,pod-template-hash=xyz --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].restartCount}{\"\\n\"}{end}")
    echo 0 ;;
  *) echo "unmocked kubectl call: $*" >&2; exit 1 ;;
esac
'
if [ "$GATE_FAIL" = "1" ]; then
  ok "gate_fail=1 — the current ReplicaSet's own pod reporting the wrong digest still fails the gate"
else
  bad "expected gate_fail=1, got '$GATE_FAIL': $GATE_OUT"
fi

if [ "$fails" -eq 0 ]; then
  echo "=== service-deploy-health-gate: OK (3 cases) ==="
  exit 0
else
  echo "=== service-deploy-health-gate: $fails FAILURE(S) ===" >&2
  exit 1
fi
