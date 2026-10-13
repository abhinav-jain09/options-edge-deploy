#!/usr/bin/env bash
# Regression fixture for the post-rollout health gate in service-deploy.sh (BEGIN/END HEALTH_GATE
# markers), driven against a mocked kubectl so it never touches a real cluster.
#
# THE BUG THIS PINS (web-service-deploy build #773, 2026-10-09): `kubectl rollout status` returns as
# soon as the Deployment condition is Available, but the OUTGOING ReplicaSet's pod can still report
# phase=Running for several more seconds while it terminates. The gate's ORIGINAL bare label-selector
# query matched that pod alongside the brand-new one, and `sort -rn | head -1` on restartCount took
# whichever was higher — a 2-day-old outgoing pod's unrelated history failed a rollout that introduced
# zero new crashes.
#
# Codex round 2 (PR #1175) correctly caught that round 1's fixture only mocked the NEW code's
# hash-qualified pod queries, so "running it against the pre-fix gate" actually exercised an unmocked-
# command error path, not the real stale-restart regression. Case 1 below fixes that: the SAME mock
# kubectl answers BOTH the bare selector (what the PRE-fix gate at the merge-base, 147976cd, actually
# issues) and the hash-qualified selector (what THIS file's gate issues), with the real two-pod data
# (old pod restartCount=2, new pod restartCount=0) — then runs BOTH gate versions against it and
# asserts the pre-fix one fails (gate_fail=1, reproducing the real bug) while the fixed one passes
# (gate_fail=0). One mock, two code paths, one real regression proven both ways.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MERGE_BASE="147976cd34c433e9f2f62e67b01ce8dfa22f2eac"   # origin/main immediately before this fix
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

# The CURRENT gate block, verbatim, extracted between its own markers so this test tracks the real
# script instead of a copy that can drift out of sync with it.
GATE_SCRIPT_NEW="$(sed -n '/^# BEGIN HEALTH_GATE/,/^# END HEALTH_GATE$/p' "$HERE/service-deploy.sh")"
[ -n "$GATE_SCRIPT_NEW" ] || { echo "FATAL: BEGIN/END HEALTH_GATE markers not found in service-deploy.sh" >&2; exit 1; }

# The PRE-fix gate block, from git history rather than a hand-copied, driftable snapshot — the
# merge-base predates the BEGIN/END markers, so this extracts by the loop's own unique boundaries
# (gate_fail=0 ... the matching done), which are unchanged by the fix and specific enough in that
# file not to match anything else.
GATE_SCRIPT_OLD="$(git -C "$HERE" show "$MERGE_BASE:scripts/deploy/service-deploy.sh" 2>/dev/null \
  | sed -n '/^gate_fail=0$/,/^done$/p')"
[ -n "$GATE_SCRIPT_OLD" ] || { echo "FATAL: could not extract the pre-fix gate block from $MERGE_BASE" >&2; exit 1; }
# Sanity: the pre-fix block must NOT already contain this fix's scoping (otherwise the comparison
# below would prove nothing — both "old" and "new" would be the same code).
if printf '%s\n' "$GATE_SCRIPT_OLD" | grep -q "pod-template-hash"; then
  echo "FATAL: the pre-fix reference at $MERGE_BASE already has pod-template-hash scoping — MERGE_BASE is wrong" >&2
  exit 1
fi

# run_gate <gate-script> <scenario-mock-kubectl-script> -> sets $GATE_FAIL, $GATE_OUT
run_gate() {
  local gate="$1" tmp mockbin
  tmp="$(mktemp -d)"; mockbin="$tmp/bin"; mkdir -p "$mockbin"
  {
    echo '#!/usr/bin/env bash'
    printf '%s\n' "$2"
  } > "$mockbin/kubectl"
  chmod +x "$mockbin/kubectl"
  GATE_OUT="$(PATH="$mockbin:$PATH" NAMESPACE=options-edge DEPLOYMENTS=options-edge-web \
    PINNED_DIGEST=sha256:newdigest000000000000000000000000000000000000000000000000000new \
    bash -c "$gate
echo GATE_FAIL=\$gate_fail" 2>&1)"
  GATE_FAIL="$(printf '%s\n' "$GATE_OUT" | sed -n 's/^GATE_FAIL=//p')"
  rm -rf "$tmp"
}

echo "1. a terminating OLD pod's stale restartCount must not fail a healthy new rollout (the real bug) — proven against BOTH the pre-fix and the fixed gate, same mock"
SCENARIO_1='
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
  # The PRE-fix gate'"'"'s bare selector: BOTH pods are still phase=Running at this instant (the old
  # one mid-termination), exactly the race this fixture pins.
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].imageID}{\"\\n\"}{end}")
    printf "docker-pullable://host.docker.internal:5001/options-edge-web@sha256:olddigest00000000000000000000000000000000000000000000000000old\ndocker-pullable://host.docker.internal:5001/options-edge-web@sha256:newdigest000000000000000000000000000000000000000000000000000new\n" ;;
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].restartCount}{\"\\n\"}{end}")
    printf "2\n0\n" ;;
  # The FIXED gate'"'"'s hash-qualified selector: only the current ReplicaSet'"'"'s pod.
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web,pod-template-hash=7d75d6cc75 --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].imageID}{\"\\n\"}{end}")
    echo "docker-pullable://host.docker.internal:5001/options-edge-web@sha256:newdigest000000000000000000000000000000000000000000000000000new" ;;
  "-n options-edge get pods -l app.kubernetes.io/name=options-edge-web,pod-template-hash=7d75d6cc75 --field-selector=status.phase=Running -o jsonpath={range .items[*]}{.status.containerStatuses[0].restartCount}{\"\\n\"}{end}")
    echo 0 ;;
  *) echo "unmocked kubectl call: $*" >&2; exit 1 ;;
esac
'
run_gate "$GATE_SCRIPT_OLD" "$SCENARIO_1"
if [ "$GATE_FAIL" = "1" ]; then
  ok "pre-fix gate (merge-base $MERGE_BASE): gate_fail=1 — reproduces the real build #773 failure (restartCount=2 from the terminating old pod)"
else
  bad "pre-fix gate: expected gate_fail=1 (the real bug), got '$GATE_FAIL' — this fixture would not actually be pinning anything: $GATE_OUT"
fi
run_gate "$GATE_SCRIPT_NEW" "$SCENARIO_1"
if [ "$GATE_FAIL" = "0" ]; then
  ok "fixed gate: gate_fail=0 — the current ReplicaSet (7d75d6cc75, restartCount=0) decides it, not the terminating one"
else
  bad "fixed gate: expected gate_fail=0, got '$GATE_FAIL': $GATE_OUT"
fi

echo "2. no resolvable current ReplicaSet fails CLOSED, not open (fixed gate only — the pre-fix gate has no such resolution step)"
run_gate "$GATE_SCRIPT_NEW" '
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
run_gate "$GATE_SCRIPT_NEW" '
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
  echo "=== service-deploy-health-gate: OK (4 assertions across 3 cases) ==="
  exit 0
else
  echo "=== service-deploy-health-gate: $fails FAILURE(S) ===" >&2
  exit 1
fi
