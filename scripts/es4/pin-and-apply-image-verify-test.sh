#!/usr/bin/env bash
# pin-and-apply-image-verify-test.sh — the post-rollout "the running pods carry the pinned digest"
# gate of scripts/es4/pin-and-apply.sh, driven end to end against a FAKE registry (curl) and a FAKE
# kubectl, over the pod sets that actually occur at the moment `kubectl rollout status` returns.
#
# Why this exists. `rollout status` returns as soon as the NEW ReplicaSet is complete, while the
# OUTGOING pod is still terminating and still matches the bare app.kubernetes.io/name selector. The
# gate used to compare EVERY matched pod's EVERY containerStatus against ONE digest taken from the
# first image line of the whole file, so a perfectly good rollout failed with "STALE IMAGE" naming
# the dying pod's digest — es4-deploy #768 and #769, tape-zones-service, 2026-10-13.
#
# The gate must still FAIL a genuinely stale deployment, so every case below is paired with a
# mutation run at the end: each clause of the selector is deleted one at a time and the case that
# names that clause must go red. A test that stays green with the fix reverted proves nothing.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/pin-and-apply.sh"
command -v yq >/dev/null || { echo "FAIL: yq is required by this test's fake kubectl"; exit 1; }
command -v jq >/dev/null || { echo "FAIL: jq is required by pin-and-apply.sh"; exit 1; }

W="$(cd "$(mktemp -d)" && pwd -P)"
trap 'find "$W" -mindepth 1 -delete 2>/dev/null; rmdir "$W" 2>/dev/null' EXIT
mkdir -p "$W/bin" "$W/fake"
F="$W/fake"
REG="registry.test:5000"

hex() { printf "$1%.0s" $(seq 64); }
NEW="sha256:$(hex e)"      # what this build pinned
OLD="sha256:$(hex d)"      # the previous build, still on the terminating pod
SIDE="sha256:$(hex c)"     # a sidecar's own, different, legitimately pinned image
OTHER="sha256:$(hex b)"    # the second Deployment of a two-Deployment manifest

# --- the fakes -------------------------------------------------------------------------------
cat > "$W/bin/curl" <<'SH'
#!/usr/bin/env bash
# fake registry: HEAD /v2/<name>/manifests/<tag> → Docker-Content-Digest from $FAKE/digest.<name>
url=""; for a in "$@"; do case "$a" in http://*) url="$a" ;; esac; done
rest="${url#*/v2/}"; name="${rest%%/manifests/*}"
d="$(cat "${FAKE:?}/digest.$name" 2>/dev/null || true)"
[ -n "$d" ] || exit 22
printf 'HTTP/1.1 200 OK\r\nDocker-Content-Digest: %s\r\n\r\n' "$d"
SH
cat > "$W/bin/kubectl" <<'SH'
#!/usr/bin/env bash
args="$*"
echo "kubectl $args" >> "${FAKE:?}/journal"
case "$args" in
  *"apply --dry-run=client -f "*" -o json")
    # kubectl emits the bare object for a single-document render and a v1.List for several
    f="${args#*--dry-run=client -f }"; f="${f%% -o json}"
    if [ "$(yq -o=json -I=0 ea '[.] | length' "$f")" = 1 ]; then yq -o=json ea '[.][0]' "$f"
    else yq -o=json ea '[.] | {"kind":"List","items": .}' "$f"; fi ;;
  "apply -f "*) echo "deployment.apps/applied configured" ;;
  *"get deployment/"*"-o jsonpath={.spec.replicas}")
    d="${args#*get deployment/}"; d="${d%% *}"
    cat "$FAKE/replicas.$d" 2>/dev/null || echo 1 ;;
  *"rollout status deployment/"*)
    d="${args#*rollout status deployment/}"; d="${d%% *}"
    [ "${FAKE_ROLLOUT_FAILS:-0}" = 1 ] && { echo "error: deployment exceeded its progress deadline" >&2; exit 1; }
    echo "deployment \"$d\" successfully rolled out" ;;
  *"get pods -l app.kubernetes.io/name="*" -o json")
    d="${args#*app.kubernetes.io/name=}"; d="${d%% *}"
    [ "${FAKE_PODS_FAIL:-0}" = 1 ] && { echo "Unable to connect to the server" >&2; exit 1; }
    cat "$FAKE/pods.$d" 2>/dev/null || printf '{"items":[]}' ;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99 ;;
esac
SH
chmod +x "$W/bin/curl" "$W/bin/kubectl"

# --- the manifests ---------------------------------------------------------------------------
deployment() { # deployment <name> <container> <image-name> [extra container block]
  cat <<YAML
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $1
  labels:
    app.kubernetes.io/name: $1
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: $1
  template:
    metadata:
      labels:
        app.kubernetes.io/name: $1
    spec:
      containers:
      - name: $2
        image: $REG/$3:prod
${4:-}
YAML
}
{ printf 'apiVersion: v1\nkind: Service\nmetadata:\n  name: tape-zones-service\nspec:\n  selector:\n    app.kubernetes.io/name: tape-zones-service\n'
  deployment tape-zones-service tape-zones options-edge-tape-zones; } > "$W/one.yaml"
deployment tape-zones-service tape-zones options-edge-tape-zones \
"      - name: metrics-sidecar
        image: $REG/options-edge-metrics:prod" > "$W/sidecar.yaml"
{ deployment tape-zones-service tape-zones options-edge-tape-zones
  deployment dealer-ledger-calibration-accumulator accumulator options-edge-dealer-ledger-calibration; } > "$W/two.yaml"
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lonely\ndata:\n  a: "b"\n' > "$W/nodeploy.yaml"

echo "$NEW"   > "$F/digest.options-edge-tape-zones"
echo "$SIDE"  > "$F/digest.options-edge-metrics"
echo "$OTHER" > "$F/digest.options-edge-dealer-ledger-calibration"

# --- pod fixtures ----------------------------------------------------------------------------
# pod <name> <ready:True|False> <phase> <deleting:yes|no> <container>=<imageID> [more...]
pod() {
  local n="$1" ready="$2" phase="$3" del="$4"; shift 4
  local cs="" c
  for c in "$@"; do cs="$cs${cs:+,}$(printf '{"name":"%s","imageID":"%s"}' "${c%%=*}" "${c#*=}")"; done
  printf '{"metadata":{"name":"%s"%s},"status":{"phase":"%s","conditions":[{"type":"Ready","status":"%s"}],"containerStatuses":[%s]}}' \
    "$n" "$([ "$del" = yes ] && printf ',"deletionTimestamp":"2026-10-13T14:12:40Z"')" "$phase" "$ready" "$cs"
}
pods() { printf '{"items":[%s]}' "$(IFS=,; echo "$*")"; }

# --- the runner ------------------------------------------------------------------------------
pass=0; fail=0
go() { # go <script> <manifest> → RC, OUTPUT
  rm -f "$F/journal"
  OUTPUT="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE="$F" \
    FAKE_ROLLOUT_FAILS="${FAKE_ROLLOUT_FAILS:-0}" FAKE_PODS_FAIL="${FAKE_PODS_FAIL:-0}" \
    bash "$1" "$2" "$REG" 2>&1)"; RC=$?
}
# Every case is a named function so the mutation pass can re-run exactly one of them.
declare -a CASES=()
case_() { CASES+=("$1"); }
expect() { # expect <name> <want_rc> <want substring> <manifest>
  local name="$1" want_rc="$2" want="$3" manifest="$4"
  go "$SCRIPT" "$W/$manifest"
  if [ "$RC" = "$want_rc" ] && printf '%s' "$OUTPUT" | grep -qF -- "$want"; then
    pass=$((pass+1)); printf '  ok   %s (rc=%s)\n' "$name" "$RC"; return 0
  fi
  fail=$((fail+1)); printf '  FAIL %s: rc=%s want %s; want [%s]\n' "$name" "$RC" "$want_rc" "$want"
  printf '%s\n' "$OUTPUT" | tail -6 | sed 's/^/       | /'; return 1
}

# ============================================================================================
# 1. THE REAL FAILURE: a Ready pod on the pin beside the outgoing pod, still Running and Ready,
#    still matching the label selector, carrying the PREVIOUS digest. es4-deploy #769 exactly.
case_ c_terminating_old_pod
c_terminating_old_pod() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
       "$(pod tape-zones-service-7c8b6d5f9-x2k4m True Running yes "tape-zones=$REG/options-edge-tape-zones@$OLD")" \
       > "$F/pods.tape-zones-service"
  expect "a Ready pod on the pin beside a TERMINATING pod on the old digest passes" \
    0 "verified tape-zones-service container tape-zones runs pinned digest $NEW" one.yaml
}

# 2. A GENUINELY STALE DEPLOYMENT: the only live, Ready pod is on another build. Must still fail.
case_ c_genuinely_stale
c_genuinely_stale() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-7c8b6d5f9-x2k4m True Running no "tape-zones=$REG/options-edge-tape-zones@$OLD")" \
       > "$F/pods.tape-zones-service"
  expect "a live Ready pod on an OLD digest still fails the build" 1 "(STALE IMAGE)" one.yaml
}

# 3. NO PODS AT ALL for a Deployment that wants replicas: nothing is proven, so nothing passes.
case_ c_no_pods
c_no_pods() {
  rm -f "$F/replicas.tape-zones-service"
  printf '{"items":[]}' > "$F/pods.tape-zones-service"
  expect "no pods at all fails closed" 1 "has no live (not deleting) Running and Ready pod" one.yaml
}

# 4a. A pod whose container is NOT READY proves nothing, even on the right digest.
case_ c_not_ready_only
c_not_ready_only() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-9f4b497cf-w7qqj False Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
       > "$F/pods.tape-zones-service"
  expect "a single Running but NOT READY pod on the pin fails closed" \
    1 "has no live (not deleting) Running and Ready pod" one.yaml
}

# 4b. ...and a not-ready straggler on an old digest beside a Ready pod on the pin does not fail it.
case_ c_not_ready_straggler
c_not_ready_straggler() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
       "$(pod tape-zones-service-7c8b6d5f9-x2k4m False Running no "tape-zones=$REG/options-edge-tape-zones@$OLD")" \
       > "$F/pods.tape-zones-service"
  expect "a NOT READY straggler on the old digest does not fail a good rollout" \
    0 "verified tape-zones-service container tape-zones runs pinned digest $NEW" one.yaml
}

# 4c. An EVICTED pod keeps phase=Failed with a stale Ready=True condition; it is not running code.
case_ c_evicted_pod
c_evicted_pod() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
       "$(pod tape-zones-service-7c8b6d5f9-x2k4m True Failed no "tape-zones=$REG/options-edge-tape-zones@$OLD")" \
       > "$F/pods.tape-zones-service"
  expect "an EVICTED (phase Failed) pod on the old digest does not fail a good rollout" \
    0 "verified tape-zones-service container tape-zones runs pinned digest $NEW" one.yaml
}

# 5a. A SIDECAR on its own, different, correctly pinned image: each container against ITS digest.
case_ c_sidecar_ok
c_sidecar_ok() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no \
            "metrics-sidecar=$REG/options-edge-metrics@$SIDE" "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
       > "$F/pods.tape-zones-service"
  expect "a sidecar on a DIFFERENT digest does not fail the app container's check" \
    0 "verified tape-zones-service container tape-zones runs pinned digest $NEW" sidecar.yaml
}

# 5b. ...and the sidecar is itself asserted, against its own pin — not ignored.
case_ c_sidecar_stale
c_sidecar_stale() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no \
            "metrics-sidecar=$REG/options-edge-metrics@$OLD" "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
       > "$F/pods.tape-zones-service"
  expect "a STALE sidecar fails the build against its own pinned digest" \
    1 "tape-zones-service container metrics-sidecar pinned $SIDE (STALE IMAGE)" sidecar.yaml
}

# 5c. A declared container with no containerStatus on the live pod is unproven, so it fails.
case_ c_missing_container_status
c_missing_container_status() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
       > "$F/pods.tape-zones-service"
  expect "a declared container missing from the pod's statuses fails closed" \
    1 "<no containerStatus>" sidecar.yaml
}

# 6. TWO Deployments on DIFFERENT images: each is judged against its own container's digest, not
#    against the first image line of the file.
case_ c_two_deployments_ok
c_two_deployments_ok() {
  rm -f "$F/replicas.tape-zones-service" "$F/replicas.dealer-ledger-calibration-accumulator"
  pods "$(pod tz-1 True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" > "$F/pods.tape-zones-service"
  pods "$(pod dl-1 True Running no "accumulator=$REG/options-edge-dealer-ledger-calibration@$OTHER")" \
       > "$F/pods.dealer-ledger-calibration-accumulator"
  expect "two Deployments on different images are each judged against their own digest" \
    0 "verified dealer-ledger-calibration-accumulator container accumulator runs pinned digest $OTHER" two.yaml
}
case_ c_two_deployments_second_stale
c_two_deployments_second_stale() {
  rm -f "$F/replicas.tape-zones-service" "$F/replicas.dealer-ledger-calibration-accumulator"
  pods "$(pod tz-1 True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" > "$F/pods.tape-zones-service"
  pods "$(pod dl-1 True Running no "accumulator=$REG/options-edge-dealer-ledger-calibration@$OLD")" \
       > "$F/pods.dealer-ledger-calibration-accumulator"
  expect "a stale SECOND Deployment fails even though the first is on its pin" \
    1 "dealer-ledger-calibration-accumulator container accumulator pinned $OTHER (STALE IMAGE)" two.yaml
}

# 7. Both canonical imageID forms Kubernetes reports.
case_ c_imageid_forms
c_imageid_forms() {
  rm -f "$F/replicas.tape-zones-service"
  pods "$(pod a True Running no "tape-zones=docker-pullable://$REG/options-edge-tape-zones@$NEW")" \
       "$(pod b True Running no "tape-zones=$NEW")" > "$F/pods.tape-zones-service"
  expect "docker-pullable:// and bare sha256 imageIDs both count as the pin" \
    0 "verified tape-zones-service container tape-zones runs pinned digest $NEW" one.yaml
}

# 8. A Deployment held at replicas 0 has no pod to prove anything on, and must not be failed.
case_ c_zero_replicas
c_zero_replicas() {
  echo 0 > "$F/replicas.tape-zones-service"
  printf '{"items":[]}' > "$F/pods.tape-zones-service"
  expect "a Deployment held at replicas 0 is passed without a pod assertion" \
    0 "is at replicas 0 by design" one.yaml
}

# 9. Surrounding fail-closed behaviour that the rewrite must not have dropped.
case_ c_no_deployment
c_no_deployment() { expect "a render with no Deployment fails closed" 1 "no Deployment found" nodeploy.yaml; }
case_ c_rollout_fails
c_rollout_fails() {
  rm -f "$F/replicas.tape-zones-service"
  printf '{"items":[]}' > "$F/pods.tape-zones-service"
  FAKE_ROLLOUT_FAILS=1
  expect "a failed rollout aborts before any image assertion" \
    1 "ROLLOUT FAILED: deployment/tape-zones-service" one.yaml
  FAKE_ROLLOUT_FAILS=0
}
case_ c_pods_list_fails
c_pods_list_fails() {
  rm -f "$F/replicas.tape-zones-service"
  FAKE_PODS_FAIL=1
  expect "an unreadable pod listing fails closed" \
    1 "cannot list the pods of tape-zones-service" one.yaml
  FAKE_PODS_FAIL=0
}

echo "--- the image-verify gate ---"
for c in "${CASES[@]}"; do "$c"; done

# ============================================================================================
# MUTATIONS — delete ONE clause of the selector at a time; the case that names it must go red.
# Each mutant is a throwaway copy; the anchor is a FIXED string, so a mutation that no longer
# matches the script is itself reported as a failure rather than silently proving nothing.
echo "--- mutations: one clause of the selector removed at a time ---"
mutate() { # mutate <label> <case-fn> <fixed string to find> <replacement>
  local label="$1" casefn="$2" m="$W/mutant.sh"
  if ! FIND="$3" REPL="$4" python3 - "$SCRIPT_REAL" "$m" <<'PY'
import os, sys
src = open(sys.argv[1]).read()
f, r = os.environ["FIND"], os.environ["REPL"]
if src.count(f) != 1:
    sys.stderr.write("anchor matched %d times\n" % src.count(f)); sys.exit(3)
open(sys.argv[2], "w").write(src.replace(f, r))
PY
  then fail=$((fail+1)); echo "  FAIL [$label] the mutation anchor no longer matches the script"; return; fi
  if ! bash -n "$m" 2>/dev/null; then fail=$((fail+1)); echo "  FAIL [$label] the mutant does not parse"; return; fi
  local bf=$fail bp=$pass
  SCRIPT="$m"; "$casefn" >/dev/null 2>&1; SCRIPT="$SCRIPT_REAL"
  local flipped=0; [ "$fail" -gt "$bf" ] && flipped=1
  fail=$bf; pass=$bp
  if [ "$flipped" = 1 ]; then pass=$((pass+1)); printf '  ok   [%s] removing it turns %s red\n' "$label" "$casefn"
  else fail=$((fail+1)); printf '  FAIL [%s] %s still passes without it — that case does not bind this clause\n' "$label" "$casefn"; fi
}
SCRIPT_REAL="$SCRIPT"
D_TS='
      | select(.metadata.deletionTimestamp == null)'
D_PHASE='
      | select(.status.phase == "Running")'
D_READY='
      | select(([(.status.conditions // [])[] | select(.type == "Ready" and .status == "True")] | length) >= 1)'

mutate "deletionTimestamp == null"      c_terminating_old_pod          "$D_TS"    ""
mutate 'phase == "Running"'             c_evicted_pod                  "$D_PHASE" ""
mutate "the Ready condition"            c_not_ready_straggler          "$D_READY" ""
mutate "the Ready condition"            c_not_ready_only               "$D_READY" ""
mutate 'select(.name == $c)'            c_sidecar_ok                   'select(.name == $c) | ' ''
mutate "the digest comparison itself"   c_genuinely_stale \
  '| select((((.id | endswith("@" + $dg)) or (.id == $dg))) | not)' '| select(false)'
mutate '"at least one live Ready pod"'  c_no_pods                      'if [ "${live:-0}" -lt 1 ]; then' 'if false; then'
mutate "the per-Deployment line filter" c_two_deployments_ok           '[ "$pdeploy" = "$name" ] || continue' ':'
mutate "the per-CONTAINER expectation (back to the file's FIRST digest, as head -1 did)" c_sidecar_ok \
  '--arg dg "$pdigest"' '--arg dg "$(echo $pinned | cut -d" " -f3)"'

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" = 0 ]
