#!/usr/bin/env bash
# Freeze one session's closing GEX board — the operator side of
# k8s/jobs/stock-gex-close-board-job.yaml (read that file's header first: it explains why the
# workload is a Job template in git and not a CronJob, and why this job needs neither a staging
# directory nor a rollout).
#
# WHY THIS IS SO MUCH SHORTER THAN stock-gex-oi-snapshot.sh, and it is not a corner cut:
#
#   That script exists to maintain an invariant — "which index generation is the running pod
#   serving" — because the service loads its OI index ONCE, in main(), with no hot-reload. That
#   makes the answer unobservable state inside a process, which is why it needs a pod-template
#   annotation, a drift reconciliation, a rollout and a post-restart proof.
#
#   Closing boards have no such state. CloseBoardStore rescans the directory every 60 seconds and
#   opens the requested root's file per request, so a published session is live within a minute
#   with nothing restarted. There is no generation to record in the cluster, nothing to be in
#   drift with, and no pod to prove anything about. Publication is the builder moving the
#   dt=<session> symlink onto a finished generation directory: one atomic step, so a session is
#   replaced whole or not at all, and a killed run leaves a generation nothing points at.
#
# WHAT IT STILL DOES, fail-closed at every step — these are not optional:
#   1. asserts the kubectl identity IS the deployer SA (the only principal the
#      options-edge-jenkins-only-workloads admission policy lets create Jobs) AND that the
#      kubeconfig points at the expected production API server,
#   2. resolves the env's stock-gex image by EXACT key, digest-pins it, and requires it to be the
#      digest the service Deployment is running — the Job's builder shares its arithmetic with the
#      service's own board code, so freezing with a different build would freeze a board the
#      running service would not have drawn,
#   3. pins the Job to the node the service pod runs on (the boards are a node-local hostPath),
#   4. refuses to start while another close-board Job is active,
#   5. creates the Job, waits, prints its whole log, and requires a receipt NAMING this session,
#   6. re-reads the SERVING pod's image and node AFTER the build, because the start-up check
#      proves nothing about the state at publication time.
#
# THE ONE THING IT CANNOT UNDO. The promote happens inside the container, on a node this identity
# has no filesystem access to (the deployer SA has no pods/exec). So step 6 can only DISCOVER
# that a published session should not be live; it cannot retract it. When that happens the run
# fails, says so in those words, and leaves close-board-published.txt in the workspace so the
# Jenkins alert reports "a session IS visible and may be wrong" rather than the comfortable
# "nothing was published". A re-run replaces it atomically.
#
# WHAT IT DELIBERATELY DOES NOT DO:
#   * It does not restart anything. See above.
#
# ACCEPTED RESIDUAL RISKS (stated, not papered over):
#   * THE SESSIONS LIVE ON ONE NODE. That is checked (replicas=1, and the Job is pinned to the
#     pod's node) but not GUARANTEED by this script: if the service is ever rescheduled onto a
#     different node, its published history does not follow, and the new node starts with no
#     sessions until this job runs again. Moving the pod means moving
#     /home/options-edge/stock-gex-oi/close. Shared storage is the real fix and is out of scope
#     here.
#   * A concurrent `service-deploy SERVICE=stock-gex` can move the image, and the node, DURING
#     the run. The image and node are therefore re-checked after the Job finishes, before the run
#     is called a success — the initial check alone proves nothing about the state at publication
#     time. The two pipelines are still not mutually excluded; Jenkins has no shared lock.
#   * Nothing here notices a Jenkins job that never runs. Whatever watches prod should alert on
#     `stockgex-close-board` having no successful build on a trading day.
#
# Usage (normally from Jenkinsfile.stockgex-close-board):
#   ENVIRONMENT=production KUBECONFIG=<deployer kubeconfig> scripts/ops/stock-gex-close-board.sh
#
# Env:
#   ENVIRONMENT      production (the only supported env — dev has no closing-board mount)
#   SESSION          optional YYYY-MM-DD override; EMPTY = today in America/New_York, which is
#                    what the scheduled post-close run must always use. A non-trading day is
#                    SKIPPED by the CLI, not failed (--skip-non-session).
#   KEEP_SESSIONS    published sessions to retain, default 10 (~21 MB each)
#   JOB_TIMEOUT_S    client-side wait, default 2100 (> the Job's own 1800s activeDeadlineSeconds)
#   KEEP_JOBS        terminal Jobs to retain, default 5
#   NAMESPACE        default options-edge
#   EXPECTED_API_SERVER  default https://192.168.100.252:6443
#   DRY_RUN          true = render + pin + server-side validate only; creates nothing
set -euo pipefail
cd "$(dirname "$0")/../.."

ENVIRONMENT="${ENVIRONMENT:-production}"
NAMESPACE="${NAMESPACE:-options-edge}"
SESSION="${SESSION:-}"
KEEP_SESSIONS="${KEEP_SESSIONS:-10}"
JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-2100}"
KEEP_JOBS="${KEEP_JOBS:-5}"
DRY_RUN="${DRY_RUN:-false}"
EXPECTED_API_SERVER="${EXPECTED_API_SERVER:-https://192.168.100.252:6443}"

TEMPLATE="k8s/jobs/stock-gex-close-board-job.yaml"
DEPLOYMENT="stock-gex-service"
SERVICE_CONTAINER="stock-gex"
IMAGE_KEY="stock-gex-service"
IMAGE_REPO_SUFFIX="/options-edge-stock-gex"
JOB_LABEL="app.kubernetes.io/name=stock-gex-close-board"
DEPLOYER="system:serviceaccount:options-edge:jenkins-deployer"
JOB_NAME=""
JOB_OWNED=false
SUCCESS=false

fatal() { echo "FATAL: $*" >&2; exit 1; }

# PUBLICATION STATE, for the Jenkins alert — which cannot read the cluster once the run has
# failed (this identity has no pods/exec). Three values, and the middle one is the honest answer
# to a question this script genuinely cannot decide:
#
#   (absent)  the Job was never created, so nothing can have been published.
#   UNKNOWN   the Job was created. The promote happens INSIDE the container, atomically, and the
#             container can be killed between that move and the receipt it prints. So from the
#             moment the Job exists, "was a session promoted" is unanswerable from here.
#   PUBLISHED a receipt naming this session was read: a session is visible now.
#
# The old marker was written only on a receipt, which made a killed-after-promote container
# report "nothing was published" while the session was live.
PUBLISH_STATE_FILE="close-board-published.txt"
rm -f "$PUBLISH_STATE_FILE"
publish_state() { printf '%s\n' "$*" >"$PUBLISH_STATE_FILE" || true; }

# An abandoned RUNNING Job holds a Databento key and half a session's chunks, so an unsuccessful
# exit must take it down — but ONLY one this invocation created, never one whose name matches. A
# terminal Job is left in place: its pod log is the post-mortem evidence.
cleanup() {
  local rc=$? terminal
  rm -f "${RENDER:-}" "${LOGS:-}"
  if [ "$SUCCESS" != "true" ] && [ "$JOB_OWNED" = "true" ] && [ -n "$JOB_NAME" ]; then
    terminal="$(kubectl -n "$NAMESPACE" get "job/$JOB_NAME" -o json 2>/dev/null \
      | jq -r '[(.status.conditions // [])[] | select((.type == "Complete" or .type == "Failed") and .status == "True") | .type] | join(",")' 2>/dev/null || echo '')"
    if [ -n "$terminal" ]; then
      echo "cleanup: Job $JOB_NAME is terminal ($terminal) — keeping it for post-mortem (script rc=$rc)" >&2
    else
      echo "cleanup: deleting still-active Job $JOB_NAME (script exiting rc=$rc without a successful finish)" >&2
      kubectl -n "$NAMESPACE" delete "job/$JOB_NAME" --cascade=foreground --wait=true \
        --timeout=180s --ignore-not-found >&2 || true
    fi
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- 0. parameter validation ------------------------------------------------------------
# Free-text Jenkins parameters used in arithmetic and in kubectl arguments. Bash evaluates
# arithmetic operands recursively, so an unvalidated string here is a code-execution surface.
[ "$ENVIRONMENT" = "production" ] \
  || fatal "ENVIRONMENT='$ENVIRONMENT' is not supported (production only — see header)"
case "$DRY_RUN" in true|false) : ;; *) fatal "DRY_RUN must be true or false, got '$DRY_RUN'" ;; esac
case "$JOB_TIMEOUT_S" in ''|*[!0-9]*) fatal "JOB_TIMEOUT_S must be digits, got '$JOB_TIMEOUT_S'" ;; esac
case "$KEEP_JOBS" in ''|*[!0-9]*) fatal "KEEP_JOBS must be digits, got '$KEEP_JOBS'" ;; esac
case "$KEEP_SESSIONS" in ''|*[!0-9]*) fatal "KEEP_SESSIONS must be digits, got '$KEEP_SESSIONS'" ;; esac
[ "$JOB_TIMEOUT_S" -ge 1900 ] && [ "$JOB_TIMEOUT_S" -le 7200 ] \
  || fatal "JOB_TIMEOUT_S must be within 1900..7200 (the Job's own deadline is 1800s), got $JOB_TIMEOUT_S"
[ "$KEEP_JOBS" -ge 1 ] && [ "$KEEP_JOBS" -le 100 ] || fatal "KEEP_JOBS must be within 1..100, got $KEEP_JOBS"
[ "$KEEP_SESSIONS" -ge 1 ] && [ "$KEEP_SESSIONS" -le 400 ] \
  || fatal "KEEP_SESSIONS must be within 1..400, got $KEEP_SESSIONS"
[ -f "$TEMPLATE" ] || fatal "missing Job template $TEMPLATE"
command -v yq >/dev/null 2>&1 || fatal "yq is required"
command -v jq >/dev/null 2>&1 || fatal "jq is required"

# The session to freeze. EMPTY means today IN NEW YORK — never the agent's local date: the agent
# runs on a Mac whose timezone is not the market's, and after 20:00 ET local-date arithmetic in
# any zone east of it names tomorrow. The date is resolved once, here, and passed explicitly.
if [ -z "$SESSION" ]; then
  SESSION="$(TZ=America/New_York date +%Y-%m-%d)"
  echo "SESSION not given — freezing today in America/New_York: $SESSION"
fi
case "$SESSION" in
  [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) : ;;
  *) fatal "SESSION='$SESSION' is not in YYYY-MM-DD form" ;;
esac
# The spelling AND a real calendar date: fromisoformat alone also accepts 20260811 and ISO week
# dates, and a regex alone accepts 2026-99-99. Whether it is a TRADING day is the CLI's call.
python3 -c "import datetime,sys; datetime.date.fromisoformat(sys.argv[1])" "$SESSION" 2>/dev/null \
  || fatal "SESSION='$SESSION' is not a valid calendar date"

# --- 1. identity AND cluster ---------------------------------------------------------------
WHOAMI="$(kubectl auth whoami -o jsonpath='{.status.userInfo.username}' 2>/dev/null || echo '')"
echo "kubectl identity: ${WHOAMI:-<unknown>}"
[ "$WHOAMI" = "$DEPLOYER" ] || fatal "kubeconfig identity is '${WHOAMI:-<unknown>}', expected '$DEPLOYER'.
       Job creation is denied for every other principal by the
       options-edge-jenkins-only-workloads admission policy."
API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || echo '')"
echo "api server: ${API_SERVER:-<unknown>}"
[ "$API_SERVER" = "$EXPECTED_API_SERVER" ] \
  || fatal "kubeconfig points at '${API_SERVER:-<unknown>}', expected '$EXPECTED_API_SERVER'.
       The deployer service-account name alone does not identify a cluster."

DEPLOY_JSON="$(kubectl -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o json 2>/dev/null)" \
  || fatal "deployment/$DEPLOYMENT not found in $NAMESPACE on $API_SERVER"
# BY NAME, never containers[0]: a sidecar inserted ahead of the service container would silently
# become the thing whose image is compared.
RUNNING_IMAGE="$(printf '%s' "$DEPLOY_JSON" | jq -er --arg c "$SERVICE_CONTAINER" \
  '[.spec.template.spec.containers[] | select(.name == $c)] | if length == 1 then .[0].image else error("expected exactly one container") end')" \
  || fatal "$DEPLOYMENT does not have exactly one container named '$SERVICE_CONTAINER'"

# --- 2. resolve + digest-pin the image, and match it to the DEPLOYED one -----------------
MUTABLE_IMAGE="$(yq -er ".images.\"${IMAGE_KEY}\"" "image-tags/${ENVIRONMENT}.yaml" 2>/dev/null || true)"
[ -n "$MUTABLE_IMAGE" ] && [ "$MUTABLE_IMAGE" != "null" ] \
  || fatal "image-tags/${ENVIRONMENT}.yaml has no '${IMAGE_KEY}' entry"
case "$MUTABLE_IMAGE" in
  *"${IMAGE_REPO_SUFFIX}":*) : ;;
  *) fatal "image-tags/${ENVIRONMENT}.yaml '${IMAGE_KEY}' is '$MUTABLE_IMAGE', which is not a ${IMAGE_REPO_SUFFIX} image" ;;
esac
# FACTS about the target, not defaults to inherit: an agent exporting DEPLOY_PLATFORM=linux/arm64
# would otherwise pin a digest that cannot run on the prod node.
export DEPLOY_PLATFORM="linux/amd64"
export REGISTRY_SCHEME="http"
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" \
  || fatal "cannot resolve registry digest for $MUTABLE_IMAGE (is the image built + pushed for $ENVIRONMENT?)"
case "$PINNED_IMAGE" in
  *@sha256:*) : ;;
  *) fatal "refusing to run a Job on an unpinned image ref: $PINNED_IMAGE" ;;
esac
echo "image: $MUTABLE_IMAGE -> $PINNED_IMAGE"
echo "deployment image: $RUNNING_IMAGE"
# Compare DIGESTS, not whole refs: a tag spelling difference is not a build difference.
[ "${PINNED_IMAGE##*@}" = "${RUNNING_IMAGE##*@}" ] \
  || fatal "image-tags/${ENVIRONMENT}.yaml resolves to ${PINNED_IMAGE##*@} but $DEPLOYMENT runs
       ${RUNNING_IMAGE##*@}. The freeze shares its arithmetic with the service's own board code,
       so building it from a different image would freeze a board the running service would not
       have drawn. Deploy the service first (service-deploy SERVICE=stock-gex) or roll the tag back."

# --- 3. the node that owns the hostPath ----------------------------------------------------
SELECTOR="$(printf '%s' "$DEPLOY_JSON" | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')"
[ -n "$SELECTOR" ] && [ "$SELECTOR" != "null" ] || fatal "$DEPLOYMENT has no matchLabels selector"
# matchExpressions cannot be expressed as a label-selector string; refuse rather than silently
# select the wrong pods.
[ "$(printf '%s' "$DEPLOY_JSON" | jq -r '(.spec.selector.matchExpressions // []) | length')" = "0" ] \
  || fatal "$DEPLOYMENT uses selector matchExpressions — this script only understands matchLabels"
# ONE replica, and that is load-bearing rather than incidental: the boards live on a node-local
# hostPath, so with replicas on two nodes a reader routed to the other one sees no frozen session
# at all. stock-gex is single-writer by design (replicas: 1 + Recreate, one Databento live
# subscription per env); if that ever changes, this job needs shared storage, not a bigger loop.
REPLICAS="$(printf '%s' "$DEPLOY_JSON" | jq -r '.spec.replicas')"
[ "$REPLICAS" = "1" ] \
  || fatal "$DEPLOYMENT has spec.replicas=$REPLICAS. Closing boards are written to ONE node's
       hostPath, so a second replica on another node would serve a session that does not exist
       there. Give this service shared storage before scaling it out."
# THE POD THAT IS ACTUALLY SERVING, not the Deployment's intent. During a rollout the template
# already names the new image while the newest Running pod is still unready or still on the old
# digest — and it is the SERVING pod whose node owns the hostPath and whose code will read what
# this job writes. Prints "<pod> <node> <image digest>" for a READY pod, or nothing.
serving_pod() {
  kubectl -n "$NAMESPACE" get pods -l "$SELECTOR" --field-selector=status.phase=Running -o json 2>/dev/null \
    | jq -r --arg c "$SERVICE_CONTAINER" '
        [ .items[]
          | select([ (.status.conditions // [])[] | select(.type == "Ready" and .status == "True") ] | length > 0)
          | . as $p
          | ($p.status.containerStatuses // [])[]
          | select(.name == $c)
          | { pod: $p.metadata.name, node: $p.spec.nodeName, image: .imageID,
              t: $p.metadata.creationTimestamp } ]
        | sort_by(.t) | last
        | if . == null then "" else "\(.pod) \(.node) \(.image)" end' 2>/dev/null || true
}
read -r SERVICE_POD NODE_NAME POD_IMAGE_ID <<EOF
$(serving_pod)
EOF
[ -n "${SERVICE_POD:-}" ] && [ -n "${NODE_NAME:-}" ] \
  || fatal "no READY $DEPLOYMENT pod — cannot determine the node that owns the closing-board
       hostPath, and a rollout in progress would have this job publish onto whichever node the
       scheduler happened to pick. Wait for the rollout to finish, then re-run."
# imageID is the digest the kubelet actually pulled, e.g. registry/repo@sha256:...
POD_DIGEST="${POD_IMAGE_ID##*@}"
[ -n "$POD_DIGEST" ] || fatal "pod $SERVICE_POD reports no resolvable image digest for container
       '$SERVICE_CONTAINER' — refusing to freeze against an unknown build"
[ "$POD_DIGEST" = "${PINNED_IMAGE##*@}" ] \
  || fatal "the READY pod $SERVICE_POD is running $POD_DIGEST, but image-tags/${ENVIRONMENT}.yaml
       resolves to ${PINNED_IMAGE##*@}. The freeze shares its arithmetic with the code that will
       READ the board, so it must be built from the digest that pod is running. A rollout is
       probably in flight — wait for it and re-run."
echo "service pod: $SERVICE_POD on node $NODE_NAME (image $POD_DIGEST)"
echo "NOTE: published sessions live on $NODE_NAME only. If the service is ever rescheduled to a
      different node, its history does NOT follow — that node starts with no sessions and this job
      republishes one per run. Moving the pod means moving /home/options-edge/stock-gex-oi/close."

# --- 3b. refuse to start alongside another close-board Job ---------------------------------
# Two runs for one session would interleave writes into a single directory: each per-root file
# lands atomically, but the manifest could name a set a later run then changed. Jenkins
# serialises this pipeline; this catches a manual run, or a leftover from an aborted build.
ACTIVE="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null \
  | jq -r '[.items[] | select((.status.active // 0) > 0) | .metadata.name] | join(" ")' 2>/dev/null || echo '')"
[ -z "$ACTIVE" ] \
  || fatal "another close-board Job is still active ($ACTIVE). Two writers in one session
       directory can leave a manifest that names a set the other run changed. Wait for it, or
       delete it if it is a leftover."

# --- 4. render + create ----------------------------------------------------------------
# Job names are immutable; the UTC stamp keeps creation order and 8 random bytes make a
# same-second collision effectively impossible. Ownership is claimed only AFTER create succeeds,
# so a losing invocation can never delete the winner's Job.
JOB_NAME="stock-gex-close-board-$(date -u +%Y%m%d-%H%M%S)-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
RENDER="$(mktemp)"
LOGS="$(mktemp)"
sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" \
    -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__SESSION__|${SESSION}|g" \
    -e "s|__NODE_NAME__|${NODE_NAME}|g" \
    -e "s|__KEEP_SESSIONS__|${KEEP_SESSIONS}|g" \
    "$TEMPLATE" >"$RENDER"
grep -q '__[A-Z_]*__' "$RENDER" && fatal "unsubstituted placeholder left in the render"
_ns="$(yq -r '.metadata.namespace' "$RENDER")"
[ "$_ns" = "$NAMESPACE" ] || fatal "template namespace '$_ns' != NAMESPACE '$NAMESPACE'"

echo "=== server-side validate ==="
kubectl -n "$NAMESPACE" apply --dry-run=server -f "$RENDER" >/dev/null
if [ "$DRY_RUN" = "true" ]; then
  SUCCESS=true
  echo "DRY_RUN=true — validated only, nothing created."
  exit 0
fi

echo "=== creating Job $JOB_NAME (session $SESSION, node $NODE_NAME) ==="
# OWNERSHIP IS CLAIMED FIRST, not after a successful create. The name carries 8 random bytes and
# has never been used, so a Job with that name can only be this invocation's — including the case
# the old order could not clean up: the API server creates it and the response is lost, or the
# agent is killed between the call and the assignment. Claiming late left that Job running, still
# writing, and blocking every later run on the active-Job check.
JOB_OWNED=true
# UNKNOWN goes down BEFORE the create call, not after: a create whose response is lost still
# started a container that can promote.
publish_state "UNKNOWN session=$SESSION node=$NODE_NAME — a Job was created; whether it promoted
a session before it stopped cannot be decided from here. Check what dt=$SESSION points at under
/home/options-edge/stock-gex-oi/close on $NODE_NAME."
kubectl -n "$NAMESPACE" create -f "$RENDER"

# --- 5. wait, then require the receipt --------------------------------------------------
echo "waiting up to ${JOB_TIMEOUT_S}s for $JOB_NAME ..."
deadline=$(( $(date +%s) + JOB_TIMEOUT_S ))
# ONE snapshot per poll: reading `succeeded` and the Failed condition in two calls can straddle
# the completion and see neither.
job_state() {
  local snap
  snap="$(kubectl -n "$NAMESPACE" get "job/$JOB_NAME" -o json 2>/dev/null)" || return 1
  printf '%s' "$snap" | jq -r 'if ((.status.succeeded // 0) >= 1) then "succeeded"
      elif ([(.status.conditions // [])[] | select(.type == "Failed" and .status == "True")] | length) >= 1 then "failed"
      else "active" end'
}
state="active"
while [ "$(date +%s)" -lt "$deadline" ]; do
  state="$(job_state || echo 'unreadable')"
  case "$state" in succeeded|failed) break ;; esac
  sleep 10
done
case "$state" in succeeded|failed) : ;; *) state="$(job_state || echo 'unreadable')" ;; esac
echo "job state: $state"

# Logs are evidence; a transient log-serving error must not be mistaken for "no receipt".
for _try in 1 2 3 4 5; do
  if kubectl -n "$NAMESPACE" logs "job/$JOB_NAME" --tail=-1 >"$LOGS" 2>/dev/null; then break; fi
  echo "logs for $JOB_NAME not available yet (attempt $_try) ..."
  : >"$LOGS"
  sleep 6
done
echo "===== $JOB_NAME log ====="
cat "$LOGS"
echo "===== end log ====="

# Anchored grammars. A holiday SKIP is a legitimate, successful outcome and must be reported as
# what it is — reading it as "no receipt" would fail every exchange holiday.
# The grammars are BOUND TO THE REQUESTED SESSION, not merely well-formed. A CLI regression that
# froze or skipped a different date would otherwise be reported as this session succeeding —
# the wrapper would say the day was published and nothing would have been.
RECEIPT_RE="^STOCK_GEX_CLOSE_BOARD_OK session=${SESSION} oiTradeDate=[0-9]{4}-[0-9]{2}-[0-9]{2} roots=[0-9]+ quotes=[0-9]+$"
SKIP_RE="^STOCK_GEX_CLOSE_BOARD_SKIP session=${SESSION} reason=[A-Z_]+$"
ANY_OUTCOME_RE='^STOCK_GEX_CLOSE_BOARD_(OK|SKIP) '
RECEIPT="$(grep -E "$RECEIPT_RE" "$LOGS" | tail -1 || true)"
SKIP="$(grep -E "$SKIP_RE" "$LOGS" | tail -1 || true)"
OUTCOMES="$(grep -cE "$ANY_OUTCOME_RE" "$LOGS" || true)"

if [ "$state" != "succeeded" ]; then
  # NOT "nothing was published". A container can promote and then exit nonzero, or be killed
  # before the Job records success — the promote is atomic and happens before the receipt is
  # printed. The marker already says UNKNOWN; this message must not contradict it.
  fatal "$JOB_NAME did not succeed (state=$state). Whether a session was promoted for $SESSION
       is UNKNOWN: the promote happens inside the container, before its receipt, and this
       identity cannot read the node. Check what dt=$SESSION points at under
       /home/options-edge/stock-gex-oi/close on $NODE_NAME. Every OTHER session is untouched —
       a build only ever writes a new generation and moves one symlink."
fi
# EXACTLY ONE outcome line, and it must be the one for the session that was asked for.
[ "${OUTCOMES:-0}" = "1" ] || fatal "$JOB_NAME printed ${OUTCOMES:-0} outcome lines — expected
       exactly one. Refusing to guess which session, if any, was published."
if [ -n "$SKIP" ]; then
  # The CLI refuses a non-session day BEFORE it builds anything, so this is the one post-create
  # path on which "nothing was published" is a fact rather than an assumption.
  publish_state "NONE session=$SESSION — not a trading day; the CLI exited before building."
  SUCCESS=true
  echo "$SKIP"
  echo "OK: $SESSION is not a trading day — nothing to freeze."
  exit 0
fi
[ -n "$RECEIPT" ] || fatal "$JOB_NAME reported success but printed no receipt naming $SESSION.
       Refusing to call this a publish: treat it as a failure and re-run."
# roots=0 cannot occur (the builder refuses below its coverage floor), but a receipt is only
# useful if it is READ — parse it so a collapse is visible in the build log and the alert.
ROOTS="$(printf '%s' "$RECEIPT" | sed -n 's/.* roots=\([0-9]*\) .*/\1/p')"
# THE STATE AT PUBLICATION TIME, not at start-up. A service-deploy during the ~10-minute build
# can move the image (the freeze then used arithmetic the running service does not have) or the
# node (the boards were written where the current pod cannot read them). Both are silent.
# A SESSION IS NOW LIVE. Everything past this point can only DISCOVER that it should not be —
# it cannot take it back: the promote happened inside the container, on a node this identity has
# no filesystem access to. The marker below is what lets the Jenkins alert say which of the two
# happened instead of asserting the comfortable one.
publish_state "PUBLISHED session=$SESSION roots=${ROOTS:-?} node=$NODE_NAME"
echo "$RECEIPT"

# THE STATE AT PUBLICATION TIME, re-read against the SERVING pod. A service-deploy during the
# ~10-minute build can move the image (the freeze then used arithmetic the reader does not have)
# or the node (the boards are where the current pod cannot see them). Both are silent otherwise.
read -r AFTER_POD AFTER_NODE AFTER_IMAGE_ID <<EOF
$(serving_pod)
EOF
[ -n "${AFTER_NODE:-}" ] \
  || fatal "no READY $DEPLOYMENT pod after the build — the session for $SESSION IS PUBLISHED, and
       this run cannot confirm the pod that will read it. Check the deployment, then re-run this
       job to replace the session."
[ "${AFTER_IMAGE_ID##*@}" = "$POD_DIGEST" ] \
  || fatal "the serving pod changed image during this run ($POD_DIGEST -> ${AFTER_IMAGE_ID##*@}).
       THE SESSION FOR $SESSION IS ALREADY PUBLISHED and was frozen with the old build's
       arithmetic — it is visible now. Re-run this job once the deploy has settled; the re-run
       publishes a new generation and replaces it atomically."
[ "$AFTER_NODE" = "$NODE_NAME" ] \
  || fatal "$DEPLOYMENT moved from node $NODE_NAME to $AFTER_NODE during this run. The session was
       written to $NODE_NAME's hostPath, which the current pod does not mount — it is published
       nowhere the service can see, and $NODE_NAME still holds it. Re-run this job to publish on
       $AFTER_NODE."
# roots=0 cannot occur (the builder refuses an empty session), but a receipt is only useful if it
# is READ — parse it and say the number out loud so a collapse is visible in the build log.
echo "OK: closing board published for $SESSION — ${ROOTS:-?} roots"
SUCCESS=true

# --- 6. prune old TERMINAL Jobs (best-effort, never blocks) ------------------------------
# Nothing else prunes them: there is no CronJob history limit here.
TERMINAL="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null \
  | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1) or ([(.status.conditions // [])[] | select(.type == "Failed" and .status == "True")] | length) >= 1)]
           | sort_by(.metadata.creationTimestamp) | .[].metadata.name' 2>/dev/null || echo '')"
COUNT="$(printf '%s\n' "$TERMINAL" | grep -c . || true)"
if [ "${COUNT:-0}" -gt "$KEEP_JOBS" ]; then
  # No pipeline into `head`: under `pipefail`, head closing early can SIGPIPE the writer and fail
  # the whole run — publication would have succeeded and the build would still alert. The list is
  # small and already sorted, so a plain loop with a counter is both simpler and can't do that.
  DROP=$(( COUNT - KEEP_JOBS ))
  i=0
  while IFS= read -r old; do
    [ -n "$old" ] || continue
    i=$(( i + 1 ))
    [ "$i" -le "$DROP" ] || break
    echo "pruning old close-board Job $old"
    kubectl -n "$NAMESPACE" delete "job/$old" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  done <<EOF
$TERMINAL
EOF
else
  echo "no old close-board Jobs to prune (terminal=${COUNT:-0}, keep=$KEEP_JOBS)"
fi
