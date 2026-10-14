#!/usr/bin/env bash
# pin-and-apply-image-verify-test.sh — the post-rollout "the running pods carry the pinned digest"
# gate of scripts/es4/pin-and-apply.sh, driven end to end against a FAKE registry (curl) and a FAKE
# kubectl, over the pod sets that actually occur at the moment `kubectl rollout status` returns.
#
# Why this exists. `rollout status` returns as soon as the NEW ReplicaSet is complete, while the
# OUTGOING pod is still terminating and still owned by this Deployment. The gate used to compare
# EVERY pod matching a bare `app.kubernetes.io/name` label, and EVERY containerStatus of it,
# against ONE digest taken from the first image line of the whole file — so a perfectly good
# rollout failed with "STALE IMAGE" naming the dying pod's digest (es4-deploy #768 and #769,
# tape-zones-service, 2026-10-13).
#
# What the gate now asserts, per (Deployment, container), over the pods it reaches by OWNERSHIP
# (deployment uid -> ReplicaSet -> pod), init containers included:
#   (a) NEGATIVE — no live (not deleting) Running pod may be on any digest but the pin. Readiness
#       is NOT consulted: an unready pod still runs its image. When the kubelet has not reported
#       an imageID, the pod's own spec image for that container is judged instead.
#   (b) POSITIVE — at least one live, Running AND Ready pod must actually report the pin.
# Every case is paired with a mutation run at the end: each clause is deleted one at a time and
# the case that names it must go red — and must go red for that reason, not because the mutant
# tripped over the harness. A test that stays green with the fix reverted proves nothing.
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
TZ=tape-zones-service

# --- the fakes -------------------------------------------------------------------------------
cat > "$W/bin/curl" <<'SH'
#!/usr/bin/env bash
# fake registry: HEAD /v2/<name>/manifests/<tag> -> Docker-Content-Digest from $FAKE/digest.<name>
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
  *"get deployment/"*"-o jsonpath={.metadata.uid}")
    # Deployment "<d>" has uid "uid-<d>"; its ReplicaSet "rs-<d>" has uid "rsuid-<d>"; its pods
    # carry an ownerReference to that. FAKE_NO_UID makes one read come back empty.
    d="${args#*get deployment/}"; d="${d%% *}"
    [ "${FAKE_NO_UID:-}" = "$d" ] && exit 0
    printf 'uid-%s' "$d" ;;
  "-n options-edge get rs -o json")
    [ "${FAKE_RS_FAIL:-0}" = 1 ] && { echo "Unable to connect to the server" >&2; exit 1; }
    cat "$FAKE/rs.json" 2>/dev/null || printf '{"items":[]}' ;;
  "-n options-edge get pods -o json")
    [ "${FAKE_PODS_FAIL:-0}" = 1 ] && { echo "Unable to connect to the server" >&2; exit 1; }
    cat "$FAKE/pods.json" 2>/dev/null || printf '{"items":[]}' ;;
  *"get deployment/"*"-o jsonpath={.spec.replicas}")
    # The gate must take the desired replica count from the RENDER, never from the live object.
    # Any call to this IS the bug Codex r1 BLOCKER 2 named, so it answers 0 — the value that
    # would skip the whole proof — and records itself, so a case that passes because of it is
    # caught rather than quietly blessed.
    d="${args#*get deployment/}"; d="${d%% *}"
    echo "live-replicas-read-for-$d" >> "$FAKE/journal.liveread"; echo 0 ;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99 ;;
esac
SH
chmod +x "$W/bin/curl" "$W/bin/kubectl"

# --- the manifests ---------------------------------------------------------------------------
deployment() { # deployment <name> <pod-label> <replicas> <container> <image> [extra containers] [initContainers block]
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
${7:-}      containers:
      - name: $4
        image: $REG/$5:prod
${6:-}
YAML
}
{ printf 'apiVersion: v1\nkind: Service\nmetadata:\n  name: %s\nspec:\n  selector:\n    app.kubernetes.io/name: %s\n' "$TZ" "$TZ"
  deployment "$TZ" "$TZ" 1 tape-zones options-edge-tape-zones; } > "$W/one.yaml"
deployment "$TZ" "$TZ" 1 tape-zones options-edge-tape-zones \
"      - name: metrics-sidecar
        image: $REG/options-edge-metrics:prod" > "$W/sidecar.yaml"
deployment "$TZ" "$TZ" 1 app options-edge-tape-zones \
"      - name: helper
        image: $REG/options-edge-tape-zones:prod" > "$W/samedigest.yaml"
{ deployment "$TZ" "$TZ" 1 tape-zones options-edge-tape-zones
  deployment dealer-ledger-calibration-accumulator dealer-ledger-calibration-accumulator 1 \
    accumulator options-edge-dealer-ledger-calibration; } > "$W/two.yaml"
deployment "$TZ" "$TZ" 0 tape-zones options-edge-tape-zones > "$W/zero.yaml"
# a Deployment whose pod label is NOT its name — the assumption the old selector made
deployment es-web es-web-ui 1 web options-edge-web > "$W/label.yaml"
# the es-feed-gateway shape: an initContainer reusing the service image, declared ABOVE the app
# container (so a `head -1` expectation would read IT as the whole file's digest)
deployment "$TZ" "$TZ" 1 tape-zones options-edge-tape-zones "" \
"      initContainers:
      - name: wait-for-kafka
        image: $REG/options-edge-tape-zones:prod
" > "$W/init.yaml"
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lonely\ndata:\n  a: "b"\n' > "$W/nodeploy.yaml"

echo "$NEW"   > "$F/digest.options-edge-tape-zones"
echo "$NEW"   > "$F/digest.options-edge-web"
echo "$SIDE"  > "$F/digest.options-edge-metrics"
echo "$OTHER" > "$F/digest.options-edge-dealer-ledger-calibration"

# --- the fake cluster's pods, reached the way the gate reaches them: by ownership -------------
# pod <owner-deployment> <name> <ready> <phase> <deleting> [init:]<container>=<imageID>[|<spec image>] ...
# With no "|<spec image>" the pod's spec image mirrors its imageID (docker-pullable:// stripped).
pod() {
  local owner="$1" n="$2" ready="$3" phase="$4" del="$5"; shift 5
  local cs="" sp="" ics="" isp="" c cn cid cspec init
  for c in "$@"; do
    init=no; case "$c" in init:*) init=yes; c="${c#init:}" ;; esac
    cn="${c%%=*}"; cid="${c#*=}"
    case "$cid" in *"|"*) cspec="${cid#*|}"; cid="${cid%%|*}" ;; *) cspec="${cid#docker-pullable://}" ;; esac
    if [ "$init" = yes ]; then
      ics="$ics${ics:+,}$(printf '{"name":"%s","imageID":"%s"}' "$cn" "$cid")"
      isp="$isp${isp:+,}$(printf '{"name":"%s","image":"%s"}' "$cn" "$cspec")"
    else
      cs="$cs${cs:+,}$(printf '{"name":"%s","imageID":"%s"}' "$cn" "$cid")"
      sp="$sp${sp:+,}$(printf '{"name":"%s","image":"%s"}' "$cn" "$cspec")"
    fi
  done
  printf '{"metadata":{"name":"%s"%s,"ownerReferences":[{"kind":"ReplicaSet","name":"rs-%s","uid":"rsuid-%s"}]},"spec":{"containers":[%s],"initContainers":[%s]},"status":{"phase":"%s","conditions":[{"type":"Ready","status":"%s"}],"containerStatuses":[%s],"initContainerStatuses":[%s]}}' \
    "$n" "$([ "$del" = yes ] && printf ',"deletionTimestamp":"2026-10-13T14:12:40Z"')" \
    "$owner" "$owner" "$sp" "$isp" "$phase" "$ready" "$cs" "$ics"
}
unowned_pod() { # same labels, owned by something that is NOT this Deployment's ReplicaSet
  printf '{"metadata":{"name":"%s","labels":{"app.kubernetes.io/name":"%s"},"ownerReferences":[{"kind":"StatefulSet","name":"someone-else","uid":"uid-someone-else"}]},"spec":{"containers":[{"name":"%s","image":"%s"}],"initContainers":[]},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"%s","imageID":"%s"}],"initContainerStatuses":[]}}' \
    "$1" "$TZ" "$2" "$3" "$2" "$3"
}
ALL_PODS=()
owners() { # owners <deployment>... — each gets one ReplicaSet in the fake cluster
  local d out=""
  for d in "$@"; do out="$out${out:+,}$(printf '{"metadata":{"name":"rs-%s","uid":"rsuid-%s","ownerReferences":[{"kind":"Deployment","name":"%s","uid":"uid-%s"}]}}' "$d" "$d" "$d" "$d")"; done
  printf '{"items":[%s]}' "$out" > "$F/rs.json"
}
setpods() { ALL_PODS+=("$@"); printf '{"items":[%s]}' "$(IFS=,; echo "${ALL_PODS[*]}")" > "$F/pods.json"; }
reset_pods() { ALL_PODS=(); rm -f "$F/journal.liveread"; setpods; owners "$TZ"; }

# --- the runner ------------------------------------------------------------------------------
pass=0; fail=0
go() { # go <script> <manifest> -> RC, OUTPUT
  rm -f "$F/journal"
  OUTPUT="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE="$F" \
    FAKE_ROLLOUT_FAILS="${FAKE_ROLLOUT_FAILS:-0}" FAKE_PODS_FAIL="${FAKE_PODS_FAIL:-0}" \
    FAKE_RS_FAIL="${FAKE_RS_FAIL:-0}" FAKE_NO_UID="${FAKE_NO_UID:-}" \
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
NOPROOF="has no live (not deleting) Running and Ready pod carrying the pinned digest"

# ============================================================================================
# 1. THE REAL FAILURE: a Ready pod on the pin beside the outgoing pod, still Running and Ready,
#    still owned by this Deployment, carrying the PREVIOUS digest. es4-deploy #769 exactly.
case_ c_terminating_old_pod
c_terminating_old_pod() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod "$TZ" tape-zones-service-7c8b6d5f9-x2k4m True Running yes "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "a Ready pod on the pin beside a TERMINATING pod on the old digest passes" 0 "$VERIFIED" one.yaml
}

# 2. A GENUINELY STALE DEPLOYMENT: the only live, Ready pod is on another build. Must still fail.
case_ c_genuinely_stale
c_genuinely_stale() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-7c8b6d5f9-x2k4m True Running no "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "a live Ready pod on an OLD digest still fails the build" 1 "(STALE IMAGE)" one.yaml
}

# 3. NO PODS AT ALL for a Deployment this manifest wants running: nothing is proven.
case_ c_no_pods
c_no_pods() { reset_pods; expect "no pods at all fails closed" 1 "$NOPROOF" one.yaml; }

# 4a. A pod whose container is NOT READY proves nothing, even on the right digest: the POSITIVE
#     rule fails even though the NEGATIVE rule has nothing to complain about.
case_ c_not_ready_only
c_not_ready_only() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj False Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a single Running but NOT READY pod on the pin proves nothing and fails closed" 1 "$NOPROOF" one.yaml
}

# 4b. ...but a NOT READY pod on an OLD digest is still running old code, so it must FAIL the build
#     even beside a Ready pod on the pin. Readiness gates the proof, never the refusal.
case_ c_not_ready_straggler_on_old_digest
c_not_ready_straggler_on_old_digest() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod "$TZ" tape-zones-service-7c8b6d5f9-x2k4m False Running no "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "a NOT READY, NOT deleting pod on the OLD digest still fails the build" \
    1 "tape-zones-service-7c8b6d5f9-x2k4m runs" one.yaml
}

# 4c. ...while a not-yet-Ready pod of the NEW build is a pod still starting, not a refusal.
case_ c_not_ready_straggler_on_pin
c_not_ready_straggler_on_pin() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod "$TZ" tape-zones-service-9f4b497cf-q8t2p False Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a second pod of the SAME build still coming up does not fail the build" 0 "$VERIFIED" one.yaml
}

# 5a. No imageID reported yet, spec image on the pin: a pod still being reported, not a stale one.
case_ c_no_imageid_yet
c_no_imageid_yet() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod "$TZ" tape-zones-service-9f4b497cf-q8t2p False Running no "tape-zones=|$REG/options-edge-tape-zones@$NEW")"
  expect "a Running pod with no imageID reported yet, on the pinned spec image, is not stale" 0 "$VERIFIED" one.yaml
}

# 5b. ...but an OLD-template pod must not hide behind an unreported imageID: with no status to
#     judge, its own spec image is judged instead.
case_ c_no_imageid_stale_spec
c_no_imageid_stale_spec() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod "$TZ" tape-zones-service-7c8b6d5f9-x2k4m False Running no "tape-zones=|$REG/options-edge-tape-zones@$OLD")"
  expect "an old-template pod with no imageID yet is judged on its SPEC image and fails" \
    1 "tape-zones-service-7c8b6d5f9-x2k4m runs spec image" one.yaml
}

# 6. An EVICTED pod keeps phase=Failed with a stale Ready=True condition; it runs no code.
case_ c_evicted_pod
c_evicted_pod() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod "$TZ" tape-zones-service-7c8b6d5f9-x2k4m True Failed no "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "an EVICTED (phase Failed) pod on the old digest does not fail a good rollout" 0 "$VERIFIED" one.yaml
}

# 7. OWNERSHIP, not labels: a pod of another controller wearing the same label, and a pod of
#    another Deployment's ReplicaSet, are both none of this Deployment's business.
case_ c_foreign_pods
c_foreign_pods() {
  reset_pods; owners "$TZ" other-deployment
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(unowned_pod hand-made-1 tape-zones "$REG/options-edge-tape-zones@$OLD")" \
          "$(pod other-deployment other-1 True Running no "tape-zones=$REG/options-edge-tape-zones@$OLD")"
  expect "pods of another controller and of another Deployment's ReplicaSet are not judged" 0 "$VERIFIED" one.yaml
}

# 8a. A SIDECAR on its own, different, correctly pinned image: each container against ITS digest.
case_ c_sidecar_ok
c_sidecar_ok() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no \
    "metrics-sidecar=$REG/options-edge-metrics@$SIDE" "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a sidecar on a DIFFERENT digest does not fail the app container's check" 0 "$VERIFIED" sidecar.yaml
}

# 8b. ...and the sidecar is itself asserted, against its own pin — not ignored.
case_ c_sidecar_stale
c_sidecar_stale() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no \
    "metrics-sidecar=$REG/options-edge-metrics@$OLD" "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a STALE sidecar fails the build against its own pinned digest" \
    1 "$TZ container metrics-sidecar pinned $SIDE (STALE IMAGE)" sidecar.yaml
}

# 8c. A declared container with no status and no spec entry on the live pod is unproven.
case_ c_missing_container_status
c_missing_container_status() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "a declared container missing from the pod entirely fails closed" \
    1 "$NOPROOF $SIDE on container metrics-sidecar" sidecar.yaml
}

# 8d. Two containers sharing ONE image: a container that never reported a status is unproven even
#     though its sibling carries the very same digest.
case_ c_same_digest_missing_container
c_same_digest_missing_container() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no "app=$REG/options-edge-tape-zones@$NEW")"
  expect "a container with no status is unproven even when its sibling shares the pinned digest" \
    1 "$NOPROOF $NEW on container helper" samedigest.yaml
}

# 9. INIT CONTAINERS (the es-feed-gateway shape) are pinned, so they are proven too.
case_ c_init_container_ok
c_init_container_ok() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no \
    "init:wait-for-kafka=$REG/options-edge-tape-zones@$NEW" "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "an init container on the pin is verified alongside the app container" \
    0 "verified $TZ container wait-for-kafka runs pinned digest $NEW" init.yaml
}
case_ c_init_container_stale
c_init_container_stale() {
  reset_pods
  setpods "$(pod "$TZ" tape-zones-service-9f4b497cf-w7qqj True Running no \
    "init:wait-for-kafka=$REG/options-edge-tape-zones@$OLD" "tape-zones=$REG/options-edge-tape-zones@$NEW")"
  expect "an init container from ANOTHER build fails the rollout" \
    1 "$TZ container wait-for-kafka pinned $NEW (STALE IMAGE)" init.yaml
}

# 10. TWO Deployments on DIFFERENT images: each judged against its own container's digest, not
#     against the first image line of the file.
case_ c_two_deployments_ok
c_two_deployments_ok() {
  reset_pods; owners "$TZ" dealer-ledger-calibration-accumulator
  setpods "$(pod "$TZ" tz-1 True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod dealer-ledger-calibration-accumulator dl-1 True Running no "accumulator=$REG/options-edge-dealer-ledger-calibration@$OTHER")"
  expect "two Deployments on different images are each judged against their own digest" \
    0 "verified dealer-ledger-calibration-accumulator container accumulator runs pinned digest $OTHER" two.yaml
}
case_ c_two_deployments_second_stale
c_two_deployments_second_stale() {
  reset_pods; owners "$TZ" dealer-ledger-calibration-accumulator
  setpods "$(pod "$TZ" tz-1 True Running no "tape-zones=$REG/options-edge-tape-zones@$NEW")" \
          "$(pod dealer-ledger-calibration-accumulator dl-1 True Running no "accumulator=$REG/options-edge-dealer-ledger-calibration@$OLD")"
  expect "a stale SECOND Deployment fails even though the first is on its pin" \
    1 "dealer-ledger-calibration-accumulator container accumulator pinned $OTHER (STALE IMAGE)" two.yaml
}

# 11. Both canonical imageID forms Kubernetes reports.
case_ c_imageid_forms
c_imageid_forms() {
  reset_pods
  setpods "$(pod "$TZ" a True Running no "tape-zones=docker-pullable://$REG/options-edge-tape-zones@$NEW")" \
          "$(pod "$TZ" b True Running no "tape-zones=$NEW|$REG/options-edge-tape-zones@$NEW")"
  expect "docker-pullable:// and bare sha256 imageIDs both count as the pin" 0 "$VERIFIED" one.yaml
}

# 12. A Deployment whose pod label is not its name is reached all the same — ownership ignores
#     labels entirely, so the old `app.kubernetes.io/name=<deployment>` assumption is gone.
case_ c_pod_label_differs_from_name
c_pod_label_differs_from_name() {
  reset_pods; owners es-web
  setpods "$(pod es-web es-web-1 True Running no "web=$REG/options-edge-web@$NEW")"
  expect "a Deployment whose pod label is not its name is still reached by ownership" \
    0 "verified es-web container web runs pinned digest $NEW" label.yaml
}

# 13. replicas comes from the RENDER, not from the live object.
case_ c_zero_replicas_declared
c_zero_replicas_declared() {
  reset_pods
  expect "a Deployment the MANIFEST declares at replicas 0 is passed without a pod assertion" \
    0 "is declared at replicas 0 by" zero.yaml
}
case_ c_live_zero_does_not_skip
c_live_zero_does_not_skip() {
  # The fake kubectl answers 0 to any live replica read and records it. A manifest that declares
  # replicas 1 must still demand the proof; if the gate ever consults the live object again, this
  # case silently passes and the recorded read gives it away.
  reset_pods
  if expect "a manifest declaring replicas 1 is proven even when the live object reads 0" \
       1 "$NOPROOF" one.yaml; then
    if [ -f "$F/journal.liveread" ]; then
      fail=$((fail+1)); echo "  FAIL the gate read the LIVE replica count: $(cat "$F/journal.liveread")"
    else
      pass=$((pass+1)); echo "  ok   the live replica count was never read"
    fi
  fi
}

# 14. Surrounding fail-closed behaviour that the rewrite must not have dropped.
case_ c_no_deployment
c_no_deployment() { reset_pods; expect "a render with no Deployment fails closed" 1 "no Deployment found" nodeploy.yaml; }
case_ c_rollout_fails
c_rollout_fails() {
  reset_pods; FAKE_ROLLOUT_FAILS=1
  expect "a failed rollout aborts before any image assertion" 1 "ROLLOUT FAILED: deployment/$TZ" one.yaml
  FAKE_ROLLOUT_FAILS=0
}
case_ c_uid_unreadable
c_uid_unreadable() {
  reset_pods; FAKE_NO_UID="$TZ"
  expect "an unreadable Deployment uid fails closed" 1 "cannot read the uid of deployment/$TZ" one.yaml
  FAKE_NO_UID=""
}
case_ c_rs_list_fails
c_rs_list_fails() {
  reset_pods; FAKE_RS_FAIL=1
  expect "an unreadable ReplicaSet listing fails closed" 1 "cannot list the ReplicaSets of $TZ" one.yaml
  FAKE_RS_FAIL=0
}
case_ c_no_owned_rs
c_no_owned_rs() {
  reset_pods; owners other-deployment
  expect "a Deployment that owns no ReplicaSet after a completed rollout fails closed" \
    1 "owns no ReplicaSet after a completed rollout" one.yaml
}
case_ c_pods_list_fails
c_pods_list_fails() {
  reset_pods; FAKE_PODS_FAIL=1
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
# MUTATIONS — delete ONE clause at a time; the case that names it must go red, and for the right
# reason (a mutant that dies of a harness error is not a kill).
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

mutate "deletionTimestamp == null"      c_terminating_old_pod   "$D_TS"    ""
mutate 'phase == "Running"'             c_evicted_pod           "$D_PHASE" ""
mutate "readiness on the PROOF rule"    c_not_ready_only        "$D_READY" ""
mutate "pods scoped to the owning ReplicaSets" c_foreign_pods \
  'select(any((.metadata.ownerReferences // [])[]; .uid as $x | ($rs | index($x)) != null))' 'select(true)'
mutate "ReplicaSets scoped to the owning Deployment" c_foreign_pods \
  'select(any((.metadata.ownerReferences // [])[]; .uid == $u))' 'select(true)'
mutate 'select(.name == $c) on the NEGATIVE rule' c_sidecar_ok \
  'id:   (([ '"'"'"$statuses"'"'"'[] | select(.name == $c) | (.imageID // "")] | first) // ""),' \
  'id:   (([ '"'"'"$statuses"'"'"'[] | (.imageID // "")] | first) // ""),'
mutate 'select(.name == $c) on the POSITIVE rule' c_same_digest_missing_container \
  '| select(([ '"'"'"$statuses"'"'"'[] | select(.name == $c)' \
  '| select(([ '"'"'"$statuses"'"'"'[]'
mutate "the spec-image fallback when no imageID is reported" c_no_imageid_stale_spec \
  'else {pod: .pod, what: "spec image", ref: .spec} end)' 'else {pod: .pod, what: "spec image", ref: ""} end)'
mutate "the NEGATIVE rule (no live pod on another digest)" c_genuinely_stale \
  '| select((((.ref | endswith("@" + $dg)) or (.ref == $dg))) | not)' '| select(false)'
mutate "the POSITIVE rule (a Ready pod must carry the pin)" c_no_pods \
  'if [ "${proof:-0}" -lt 1 ]; then' 'if false; then'
mutate "readiness NOT gating the negative rule" c_not_ready_straggler_on_old_digest \
  'bad=$(printf '"'"'%s'"'"' "$pods_json" | jq -r --arg c "$pcontainer" --arg dg "$pdigest" "[$running_filter"' \
  'bad=$(printf '"'"'%s'"'"' "$pods_json" | jq -r --arg c "$pcontainer" --arg dg "$pdigest" "[$ready_filter"'
mutate "init containers in the pinned set" c_init_container_stale \
  '| ((.spec.template.spec.initContainers // []) + (.spec.template.spec.containers // []))[]' \
  '| ((.spec.template.spec.containers // []))[]'
# (the STALE case survives this one by falling back to the init container's spec image — defence
# in depth, not a kill — so the PROOF case is what binds the status union)
mutate "initContainerStatuses in the judged set" c_init_container_ok \
  "statuses='((.status.initContainerStatuses // []) + (.status.containerStatuses // []))'" \
  "statuses='((.status.containerStatuses // []))'"
mutate "the per-Deployment line filter" c_two_deployments_ok \
  '[ "$pdeploy" = "$name" ] || continue' ':'
mutate "the per-CONTAINER expectation (back to the file's FIRST digest, as head -1 did)" c_sidecar_ok \
  'bad=$(printf '"'"'%s'"'"' "$pods_json" | jq -r --arg c "$pcontainer" --arg dg "$pdigest"' \
  'bad=$(printf '"'"'%s'"'"' "$pods_json" | jq -r --arg c "$pcontainer" --arg dg "$(echo $pinned | cut -d" " -f3)"'
mutate "replicas read from the RENDER, not the live object" c_live_zero_does_not_skip \
  'while read -r name want; do' 'while read -r name want; do
    want=$(kubectl -n options-edge get "deployment/$name" -o jsonpath='"'"'{.spec.replicas}'"'"' 2>/dev/null || echo ERR)'

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" = 0 ]
