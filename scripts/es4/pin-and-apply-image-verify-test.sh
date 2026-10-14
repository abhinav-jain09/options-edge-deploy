#!/usr/bin/env bash
# pin-and-apply-image-verify-test.sh — the post-rollout "the running pods carry the pinned digest"
# gate of scripts/es4/pin-and-apply.sh, driven end to end against a FAKE registry (curl) and a FAKE
# kubectl, over the pod sets that actually occur at the moment `kubectl rollout status` returns.
#
# Why this exists. `rollout status` returns as soon as the NEW ReplicaSet is complete, while the
# OUTGOING pod is still terminating and still matches the Deployment's label selector. The gate used
# to compare EVERY matched pod's EVERY containerStatus against ONE digest taken from the first image
# line of the whole file, so a perfectly good rollout failed with "STALE IMAGE" naming the dying
# pod's digest — es4-deploy #768 and #769, tape-zones-service, 2026-10-13.
#
# The gate must still FAIL a genuinely stale deployment, so the two rules it now applies are
# asymmetric and are tested as such:
#   (a) NEGATIVE — no live (not deleting) Running pod may report any digest other than the pin on
#       the named container. Readiness is NOT part of this rule: an unready pod still runs its image.
#   (b) POSITIVE — at least one live Running AND Ready pod must report the pin on that container.
# Every case below is paired with a mutation run at the end: each clause is deleted one at a time
# and the case that names it must go red. A test that stays green with the fix reverted proves
# nothing, so a mutant that dies of an unrelated harness error does not count as a kill either.
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
  *"rollout status deployment/"*)
    d="${args#*rollout status deployment/}"; d="${d%% *}"
    [ "${FAKE_ROLLOUT_FAILS:-0}" = 1 ] && { echo "error: deployment exceeded its progress deadline" >&2; exit 1; }
    echo "deployment \"$d\" successfully rolled out" ;;
  *"get pods -l "*" -o json")
    sel="${args#*get pods -l }"; sel="${sel%% -o json}"
    [ "${FAKE_PODS_FAIL:-0}" = 1 ] && { echo "Unable to connect to the server" >&2; exit 1; }
    cat "$FAKE/pods.$(printf '%s' "$sel" | tr -c 'a-zA-Z0-9' '_')" 2>/dev/null || printf '{"items":[]}' ;;
  *"get deployment/"*"-o jsonpath={.spec.replicas}")
    # The gate must take the desired replica count from the RENDER, never from the live object.
    # Any call to this is itself the bug Codex r1 BLOCKER 2 named, so it answers 0 — the value
    # that would skip the whole proof — and a case that passes because of it is a case that lied.
    d="${args#*get deployment/}"; d="${d%% *}"
    echo "live-replicas-read-for-$d" >> "$FAKE/journal.liveread"; echo 0 ;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99 ;;
esac
SH
chmod +x "$W/bin/curl" "$W/bin/kubectl"

# --- the manifests ---------------------------------------------------------------------------
deployment() { # deployment <name> <pod-label> <replicas> <container> <image-name> [extra container block]
  cat <<YAML
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $1
  labels:
    app.kubernetes.io/name: $2
spec:
  replicas: $3
  selector:
    matchLabels:
      app.kubernetes.io/name: $2
  template:
    metadata:
      labels:
        app.kubernetes.io/name: $2
    spec:
      containers:
      - name: $4
        image: $REG/$5:prod
${6:-}
YAML
}
TZ=tape-zones-service
{ printf 'apiVersion: v1\nkind: Service\nmetadata:\n  name: %s\nspec:\n  selector:\n    app.kubernetes.io/name: %s\n' "$TZ" "$TZ"
  deployment "$TZ" "$TZ" 1 tape-zones options-edge-tape-zones; } > "$W/one.yaml"
deployment "$TZ" "$TZ" 1 tape-zones options-edge-tape-zones \
"      - name: metrics-sidecar
        image: $REG/options-edge-metrics:prod" > "$W/sidecar.yaml"
{ deployment "$TZ" "$TZ" 1 tape-zones options-edge-tape-zones
  deployment dealer-ledger-calibration-accumulator dealer-ledger-calibration-accumulator 1 \
    accumulator options-edge-dealer-ledger-calibration; } > "$W/two.yaml"
deployment "$TZ" "$TZ" 0 tape-zones options-edge-tape-zones > "$W/zero.yaml"
deployment "$TZ" "$TZ" 1 app options-edge-tape-zones \
"      - name: helper
        image: $REG/options-edge-tape-zones:prod" > "$W/samedigest.yaml"
# a Deployment whose pod label is NOT its name — the assumption the old selector made
deployment es-web es-web-ui 1 web options-edge-web > "$W/label.yaml"
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lonely\ndata:\n  a: "b"\n' > "$W/nodeploy.yaml"

echo "$NEW"   > "$F/digest.options-edge-tape-zones"
echo "$NEW"   > "$F/digest.options-edge-web"
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
setpods() { # setpods <label selector> <pod json>...   — keyed exactly as the fake kubectl reads it
  local sel="$1"; shift
  printf '{"items":[%s]}' "$(IFS=,; echo "$*")" > "$F/pods.$(printf '%s' "$sel" | tr -c 'a-zA-Z0-9' '_')"
}
TZSEL="app.kubernetes.io/name=$TZ"
reset_pods() { rm -f "$F"/pods.* "$F/journal.liveread"; }

# --- the runner ------------------------------------------------------------------------------
pass=0; fail=0
go() { # go <script> <manifest> → RC, OUTPUT
  rm -f "$F/journal"
  OUTPUT="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE="$F" \
    FAKE_ROLLOUT_FAILS="${FAKE_ROLLOUT_FAILS:-0}" FAKE_PODS_FAIL="${FAKE_PODS_FAIL:-0}" \
    bash "$1" "$2" "$REG" 2>&1)"; RC=$?
}
declare -a CASES=()
case_() { CASES+=("$1"); }
expect() { # expect <name> <want_rc> <want substring> <manifest>
  local name="$1" want_rc="$2" want="$3" manifest="$4"
  go "$SCRIPT" "$W/$manifest"
  if [ "$RC" = "$want_rc" ] && printf '%s' "$OUTPUT" | grep -qF -- "$want"; then
    pass=$((pass+1)); printf '  ok   %s (rc=%s)\n' "$name" "$RC"; return 0
  fi
  fail=$((fail+1)); printf '  FAIL %s: rc=%s want %s; want [%s]\n' "$name" "$RC" "$want_rc" "$want"
  printf '%s\n' "$OUTPUT" | tail -8 | sed 's/^/       | /'; return 1
}
VERIFIED="verified $TZ container tape-zones runs pinned digest $NEW"

# ============================================================================================
# 1. THE REAL FAILURE: a Ready pod on the pin beside the outgoing pod, still Running and Ready,
#    still matching the label selector, carrying the PREVIOUS digest. es4-deploy #769 exactly.
case_ c_terminating_old_pod
c_terminating_old_pod() {
  reset_pods
  setpods "$TZSEL" \
    "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
    "$(pod tape-zones-service-7c8b6d5f9-x2k4m True Running yes "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "a Ready pod on the pin beside a TERMINATING pod on the old digest passes" 0 "$VERIFIED" one.yaml
}

# 2. A GENUINELY STALE DEPLOYMENT: the only live, Ready pod is on another build. Must still fail.
case_ c_genuinely_stale
c_genuinely_stale() {
  reset_pods
  setpods "$TZSEL" "$(pod tape-zones-service-7c8b6d5f9-x2k4m True Running no "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "a live Ready pod on an OLD digest still fails the build" 1 "(STALE IMAGE)" one.yaml
}

# 3. NO PODS AT ALL for a Deployment this manifest wants running: nothing is proven.
case_ c_no_pods
c_no_pods() {
  reset_pods; setpods "$TZSEL"
  expect "no pods at all fails closed" 1 "has no live (not deleting) Running and Ready pod" one.yaml
}

# 4a. A pod whose container is NOT READY proves nothing, even on the right digest — so the
#     POSITIVE rule fails even though the NEGATIVE rule has nothing to complain about.
case_ c_not_ready_only
c_not_ready_only() {
  reset_pods
  setpods "$TZSEL" "$(pod tape-zones-service-9f4b497cf-w7qqj False Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a single Running but NOT READY pod on the pin proves nothing and fails closed" \
    1 "has no live (not deleting) Running and Ready pod" one.yaml
}

# 4b. ...but a NOT READY pod on an OLD digest is still running old code, so it must FAIL the
#     build even beside a Ready pod on the pin. Readiness gates the proof, never the refusal.
case_ c_not_ready_straggler_on_old_digest
c_not_ready_straggler_on_old_digest() {
  reset_pods
  setpods "$TZSEL" \
    "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
    "$(pod tape-zones-service-7c8b6d5f9-x2k4m False Running no "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "a NOT READY, NOT deleting pod on the OLD digest still fails the build" \
    1 "tape-zones-service-7c8b6d5f9-x2k4m runs" one.yaml
}

# 4c. ...while a not-yet-Ready pod of the NEW build is simply a pod still starting: not a refusal.
case_ c_not_ready_straggler_on_pin
c_not_ready_straggler_on_pin() {
  reset_pods
  setpods "$TZSEL" \
    "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
    "$(pod tape-zones-service-9f4b497cf-q8t2p False Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a second pod of the SAME build still coming up does not fail the build" 0 "$VERIFIED" one.yaml
}

# 4d. A Running pod whose kubelet has not reported an imageID yet is not evidence of staleness.
case_ c_no_imageid_yet
c_no_imageid_yet() {
  reset_pods
  setpods "$TZSEL" \
    "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
    "$(pod tape-zones-service-9f4b497cf-q8t2p False Running no "tape-zones=")"
  expect "a Running pod with no imageID reported yet is not treated as stale" 0 "$VERIFIED" one.yaml
}

# 5. An EVICTED pod keeps phase=Failed with a stale Ready=True condition; it runs no code.
case_ c_evicted_pod
c_evicted_pod() {
  reset_pods
  setpods "$TZSEL" \
    "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
    "$(pod tape-zones-service-7c8b6d5f9-x2k4m True Failed no "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "an EVICTED (phase Failed) pod on the old digest does not fail a good rollout" 0 "$VERIFIED" one.yaml
}

# 6a. A SIDECAR on its own, different, correctly pinned image: each container against ITS digest.
case_ c_sidecar_ok
c_sidecar_ok() {
  reset_pods
  setpods "$TZSEL" "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no \
    "metrics-sidecar=$REG/options-edge-metrics@$SIDE" "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a sidecar on a DIFFERENT digest does not fail the app container's check" 0 "$VERIFIED" sidecar.yaml
}

# 6b. ...and the sidecar is itself asserted, against its own pin — not ignored.
case_ c_sidecar_stale
c_sidecar_stale() {
  reset_pods
  setpods "$TZSEL" "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no \
    "metrics-sidecar=$REG/options-edge-metrics@$OLD" "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a STALE sidecar fails the build against its own pinned digest" \
    1 "$TZ container metrics-sidecar pinned $SIDE (STALE IMAGE)" sidecar.yaml
}

# 6c. A declared container with no containerStatus on the live pod is unproven, so it fails.
case_ c_missing_container_status
c_missing_container_status() {
  reset_pods
  setpods "$TZSEL" "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a declared container missing from the pod's statuses fails closed" \
    1 "has no live (not deleting) Running and Ready pod carrying the pinned digest $SIDE on container metrics-sidecar" sidecar.yaml
}

# 6d. Two containers sharing ONE image: a container that never reported a status is unproven even
#     though its sibling carries the very same digest.
case_ c_same_digest_missing_container
c_same_digest_missing_container() {
  reset_pods
  setpods "$TZSEL" "$(pod tape-zones-service-9f4b497cf-w7qqj True Running no "app=$REG/options-edge-tape-zones@$NEW")"
  expect "a container with no status is unproven even when its sibling shares the pinned digest" \
    1 "carrying the pinned digest $NEW on container helper" samedigest.yaml
}

# 7. TWO Deployments on DIFFERENT images: each judged against its own container's digest, not
#    against the first image line of the file.
case_ c_two_deployments_ok
c_two_deployments_ok() {
  reset_pods
  setpods "$TZSEL" "$(pod tz-1 True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  setpods "app.kubernetes.io/name=dealer-ledger-calibration-accumulator" \
    "$(pod dl-1 True Running no "accumulator=$REG/options-edge-dealer-ledger-calibration@$OTHER")"
  expect "two Deployments on different images are each judged against their own digest" \
    0 "verified dealer-ledger-calibration-accumulator container accumulator runs pinned digest $OTHER" two.yaml
}
case_ c_two_deployments_second_stale
c_two_deployments_second_stale() {
  reset_pods
  setpods "$TZSEL" "$(pod tz-1 True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  setpods "app.kubernetes.io/name=dealer-ledger-calibration-accumulator" \
    "$(pod dl-1 True Running no "accumulator=$REG/options-edge-dealer-ledger-calibration@$OLD")"
  expect "a stale SECOND Deployment fails even though the first is on its pin" \
    1 "dealer-ledger-calibration-accumulator container accumulator pinned $OTHER (STALE IMAGE)" two.yaml
}

# 8. Both canonical imageID forms Kubernetes reports.
case_ c_imageid_forms
c_imageid_forms() {
  reset_pods
  setpods "$TZSEL" \
    "$(pod a True Running no "tape-zones=docker-pullable://$REG/options-edge-tape-zones@$NEW")" \
    "$(pod b True Running no "tape-zones=$NEW")"
  expect "docker-pullable:// and bare sha256 imageIDs both count as the pin" 0 "$VERIFIED" one.yaml
}

# 9. The pod set comes from .spec.selector.matchLabels, not from the Deployment's name.
case_ c_pod_label_differs_from_name
c_pod_label_differs_from_name() {
  reset_pods
  setpods "app.kubernetes.io/name=es-web-ui" "$(pod es-web-1 True Running no "web=$REG/options-edge-web@$NEW")"
  expect "a Deployment whose pod label is not its name is still found by its own selector" \
    0 "verified es-web container web runs pinned digest $NEW" label.yaml
}

# 10. replicas comes from the RENDER, not from the live object.
case_ c_zero_replicas_declared
c_zero_replicas_declared() {
  reset_pods
  expect "a Deployment the MANIFEST declares at replicas 0 is passed without a pod assertion" \
    0 "is declared at replicas 0 by" zero.yaml
}
case_ c_live_zero_does_not_skip
c_live_zero_does_not_skip() {
  # The fake kubectl answers 0 to any live replica read and records it. A manifest that declares
  # replicas 1 must therefore still demand the proof; if the gate ever consults the live object
  # again, this case silently passes and the recorded read gives it away.
  reset_pods; setpods "$TZSEL"
  if expect "a manifest declaring replicas 1 is proven even when the live object reads 0" \
       1 "has no live (not deleting) Running and Ready pod" one.yaml; then
    if [ -f "$F/journal.liveread" ]; then
      fail=$((fail+1)); echo "  FAIL the gate read the LIVE replica count: $(cat "$F/journal.liveread")"
    else
      pass=$((pass+1)); echo "  ok   the live replica count was never read"
    fi
  fi
}

# 11. Surrounding fail-closed behaviour that the rewrite must not have dropped.
case_ c_no_deployment
c_no_deployment() { reset_pods; expect "a render with no Deployment fails closed" 1 "no Deployment found" nodeploy.yaml; }
case_ c_rollout_fails
c_rollout_fails() {
  reset_pods; setpods "$TZSEL"
  FAKE_ROLLOUT_FAILS=1
  expect "a failed rollout aborts before any image assertion" 1 "ROLLOUT FAILED: deployment/$TZ" one.yaml
  FAKE_ROLLOUT_FAILS=0
}
case_ c_pods_list_fails
c_pods_list_fails() {
  reset_pods
  FAKE_PODS_FAIL=1
  expect "an unreadable pod listing fails closed" 1 "cannot list the pods of $TZ" one.yaml
  FAKE_PODS_FAIL=0
}
case_ c_jq_required_before_apply
c_jq_required_before_apply() {
  # jq must be demanded BEFORE `kubectl apply`, or a missing tool leaves the cluster mutated with
  # the proof step unrunnable. Build a PATH that has everything the script needs except jq.
  local d="$W/nojq"; rm -rf "$d"; mkdir -p "$d"; local t p
  for t in bash basename mkdir cp sed grep awk tr sort head cat uname; do
    p="$(command -v "$t" 2>/dev/null)" || { pass=$((pass+1)); echo "  ok   (skipped: $t not resolvable for the no-jq PATH)"; return 0; }
    ln -sf "$p" "$d/$t"
  done
  ln -sf "$W/bin/curl" "$d/curl"; ln -sf "$W/bin/kubectl" "$d/kubectl"
  reset_pods
  local out rc
  out="$(cd "$W" && env -i PATH="$d" FAKE="$F" bash "$SCRIPT" "$W/one.yaml" "$REG" 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = 1 ] && printf '%s' "$out" | grep -qF "jq is required" && ! printf '%s' "$out" | grep -qF "configured"; then
    pass=$((pass+1)); echo "  ok   a missing jq fails closed BEFORE the apply"
  else
    fail=$((fail+1)); echo "  FAIL a missing jq did not fail closed before the apply (rc=$rc)"
    printf '%s\n' "$out" | tail -6 | sed 's/^/       | /'
  fi
}

echo "--- the image-verify gate ---"
for c in "${CASES[@]}"; do "$c"; done

# ============================================================================================
# MUTATIONS — delete ONE clause at a time; the case that names it must go red, and must go red
# for the right reason (a mutant that dies of a harness error is not a kill).
echo "--- mutations: one clause removed at a time ---"
SCRIPT_REAL="$SCRIPT"
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
  SCRIPT="$m"; OUTPUT=""; "$casefn" >/dev/null 2>&1; SCRIPT="$SCRIPT_REAL"
  local flipped=0; [ "$fail" -gt "$bf" ] && flipped=1
  fail=$bf; pass=$bp
  if [ "$flipped" != 1 ]; then
    fail=$((fail+1)); printf '  FAIL [%s] %s still passes without it — that case does not bind this clause\n' "$label" "$casefn"
  elif printf '%s' "$OUTPUT" | grep -qE 'fake kubectl: unexpected call|command not found|syntax error|jq: error|parse error'; then
    fail=$((fail+1)); printf '  FAIL [%s] %s went red on a HARNESS error, not on the clause\n' "$label" "$casefn"
    printf '%s\n' "$OUTPUT" | tail -4 | sed 's/^/       | /'
  else
    pass=$((pass+1)); printf '  ok   [%s] removing it turns %s red\n' "$label" "$casefn"
  fi
}
D_TS='
      | select(.metadata.deletionTimestamp == null)'
D_PHASE='
      | select(.status.phase == "Running")'
D_READY='
      | select(([(.status.conditions // [])[] | select(.type == "Ready" and .status == "True")] | length) >= 1)'

mutate "deletionTimestamp == null"       c_terminating_old_pod   "$D_TS"    ""
mutate 'phase == "Running"'              c_evicted_pod           "$D_PHASE" ""
mutate "readiness on the PROOF rule"     c_not_ready_only        "$D_READY" ""
mutate 'select(.name == $c) on the NEGATIVE rule'  c_sidecar_ok \
  'id: (([(.status.containerStatuses // [])[] | select(.name == $c) | (.imageID // "")] | first) // "")}' \
  'id: (([(.status.containerStatuses // [])[] | (.imageID // "")] | first) // "")}'
mutate 'select(.name == $c) on the POSITIVE rule'  c_same_digest_missing_container \
  '| select(([(.status.containerStatuses // [])[] | select(.name == $c)
                   | select(' \
  '| select(([(.status.containerStatuses // [])[]
                   | select('""
mutate "the NEGATIVE rule (no live pod on another digest)" c_genuinely_stale \
  '| select((((.id | endswith("@" + $dg)) or (.id == $dg))) | not)' '| select(false)'
mutate "the POSITIVE rule (a Ready pod must carry the pin)" c_no_pods \
  'if [ "${proof:-0}" -lt 1 ]; then' 'if false; then'
mutate "readiness NOT gating the negative rule"  c_not_ready_straggler_on_old_digest \
  '"[$running_filter"'"'"'
        | {pod: .metadata.name,' '"[$ready_filter"'"'"'
        | {pod: .metadata.name,'
mutate "the per-Deployment line filter"  c_two_deployments_ok \
  '[ "$pdeploy" = "$name" ] || continue' ':'
mutate "the per-CONTAINER expectation (back to the file's FIRST digest, as head -1 did)" c_sidecar_ok \
  '--arg dg "$pdigest" "[$running_filter"' '--arg dg "$(echo $pinned | cut -d" " -f3)" "[$running_filter"'
mutate "replicas read from the RENDER, not the live object" c_live_zero_does_not_skip \
  'while read -r name want selector; do' 'while read -r name want selector; do
    want=$(kubectl -n options-edge get "deployment/$name" -o jsonpath='"'"'{.spec.replicas}'"'"' 2>/dev/null || echo ERR)'
mutate "the selector read from .spec.selector.matchLabels"  c_pod_label_differs_from_name \
  'kubectl -n options-edge get pods -l "$selector" -o json' 'kubectl -n options-edge get pods -l "app.kubernetes.io/name=$name" -o json'

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" = 0 ]
