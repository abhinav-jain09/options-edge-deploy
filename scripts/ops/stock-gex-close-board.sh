#!/usr/bin/env bash
# Freeze one session's closing GEX board — the operator side of
# k8s/jobs/stock-gex-close-batch-job.yaml (read that file's header first: it explains why the
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
#   WAIT_FOR_VENDOR_S  how long the CLI may wait for the vendor archive to reach the closing
#                    minute, default 3600, max 4800 (the Job deadline is 6000)
#   JOB_TIMEOUT_S    client-side wait, default 6300 (> the Job's own 6000s activeDeadlineSeconds)
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
WAIT_FOR_VENDOR_S="${WAIT_FOR_VENDOR_S:-3600}"
JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-6300}"
# The vendor's historical API 504s in the first ~20-45 minutes after the close while its data
# ripens (measured 2026-08-31: chunk c016 refused four times at 22:35-22:38 CEST and the whole
# night's board went unpublished until a HUMAN retriggered next morning — the owner's verdict:
# "we cannot do rerun everyday"). So the chain retries ITSELF: a failed batch attempt is
# recreated after a pause, and Spring Batch resumes the same JobInstance — every chunk already
# fetched is reused, so a retry only pays for what actually failed.
CLOSE_ATTEMPTS="${CLOSE_ATTEMPTS:-3}"
CLOSE_RETRY_WAIT_S="${CLOSE_RETRY_WAIT_S:-900}"
KEEP_JOBS="${KEEP_JOBS:-5}"
DRY_RUN="${DRY_RUN:-false}"
EXPECTED_API_SERVER="${EXPECTED_API_SERVER:-https://192.168.100.252:6443}"

TEMPLATE="k8s/jobs/stock-gex-close-batch-job.yaml"
# The service is still consulted — but ONLY to find the node that owns the hostPath. Its
# image is no longer the freeze's image; see the note above the pin below.
DEPLOYMENT="stock-gex-service"
SERVICE_CONTAINER="stock-gex"
IMAGE_KEY="close-batch"
IMAGE_REPO_SUFFIX="/oe-close-batch"
JOB_LABEL="app.kubernetes.io/name=stock-gex-close-batch"
# A deliberate re-freeze of a session that already completed is asked for by raising this:
# session and rebuild are the only parameters that identify a run, which is what makes a
# failed night RESUME rather than start again beside itself.
REBUILD="${REBUILD:-0}"
# The batch types `rebuild` as a Long, and Spring REFUSES "false" with a conversion error after
# the pod has already been scheduled — a whole attempt burned on a spelling. Humans (and one
# assistant, 2026-09-01, at one in the morning) type booleans here, so booleans are accepted
# and normalised; anything else that is not 0/1 stops NOW, in this log, with the fix named.
case "$REBUILD" in
  true|TRUE|True) REBUILD=1 ;;
  false|FALSE|False) REBUILD=0 ;;
  0|1) : ;;
  *) fatal "REBUILD must be 0 or 1 (or true/false), got '$REBUILD' — the batch types it as a Long" ;;
esac
RUN_SOURCE="${RUN_SOURCE:-cron}"
DEPLOYER="system:serviceaccount:options-edge:jenkins-deployer"
JOB_NAME=""
JOB_OWNED=false
SUCCESS=false

fatal() { echo "FATAL: $*" >&2; exit 1; }

# Capture the evening's Near Flip lists (internal tracking). AFTER a session is known to be
# published and NEVER fatal to it: a tracking miss is a gap the CLI's self-healing fills on the
# next run. Called from BOTH publish paths — a fresh freeze and the already-published no-op —
# because the tracker heals forward and an extra call inserts nothing.
run_flip_track() {
  # AFTER promotion, BEFORE pruning, and NEVER fatal to the publication above: the board is
  # already out; a tracking miss is a gap the CLI's self-healing fills tomorrow, not a reason
  # to alert the desk that the close failed. The image is the stock-gex service's own (the CLI
  # ships in the same package), pinned the same way.
  TRACK_IMAGE_MUTABLE="$(yq -er '.images."stock-gex-service"' "image-tags/${ENVIRONMENT}.yaml" 2>/dev/null || true)"
  if [ -n "$TRACK_IMAGE_MUTABLE" ] && [ "$TRACK_IMAGE_MUTABLE" != "null" ]; then
    TRACK_DIGEST="$(skopeo inspect --tls-verify=false "docker://${TRACK_IMAGE_MUTABLE}" --format '{{.Digest}}' 2>/dev/null     || kubectl -n "$NAMESPACE" get deploy stock-gex-service -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null | sed 's/.*@//' )"
    # %:*, NOT %%:*: the registry lives at host:port, so the FIRST colon is the port's — %%
  # stripped the whole repository path and rendered 192.168.100.252@sha256:..., which the
  # kubelet read as a docker.io library image and could never pull (caught by proof run #31).
  TRACK_IMAGE="${TRACK_IMAGE_MUTABLE%:*}@${TRACK_DIGEST}"
    case "$TRACK_DIGEST" in
      sha256:*)
        TRACK_JOB="stock-gex-flip-track-$(date -u +%Y%m%d-%H%M%S)"
        sed -e "s|__IMAGE__|${TRACK_IMAGE}|g" -e "s|__JOB_NAME__|${TRACK_JOB}|g"         "k8s/jobs/stock-gex-flip-track-job.yaml" | kubectl -n "$NAMESPACE" create -f -         && echo "flip-track Job created: $TRACK_JOB"         || echo "WARNING: flip-track Job could not be created — self-healing covers it tomorrow"
        # Bounded wait for the receipt; a slow run is left to finish on its own.
        if kubectl -n "$NAMESPACE" wait --for=condition=complete "job/$TRACK_JOB" --timeout=180s >/dev/null 2>&1; then
          kubectl -n "$NAMESPACE" logs "job/$TRACK_JOB" 2>/dev/null | grep "^flip-track" || true
        else
          echo "WARNING: flip-track Job not complete after 180s — check job/$TRACK_JOB; tomorrow's run heals any gap"
        fi
        ;;
      *) echo "WARNING: could not resolve a digest for ${TRACK_IMAGE_MUTABLE} — flip-track skipped tonight" ;;
    esac
  else
    echo "WARNING: image-tags/${ENVIRONMENT}.yaml has no stock-gex-service entry — flip-track skipped tonight"
  fi

}


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
case "$WAIT_FOR_VENDOR_S" in ''|*[!0-9]*) fatal "WAIT_FOR_VENDOR_S must be digits, got '$WAIT_FOR_VENDOR_S'" ;; esac
# The Job's own deadline is 6000s; a wait that outlived it would be killed with no message of
# its own, turning "the vendor was late" into an unexplained timeout.
[ "$WAIT_FOR_VENDOR_S" -le 4800 ] \
  || fatal "WAIT_FOR_VENDOR_S must be <= 4800 (the Job deadline is 6000s), got $WAIT_FOR_VENDOR_S"
[ "$JOB_TIMEOUT_S" -ge 6100 ] && [ "$JOB_TIMEOUT_S" -le 14400 ] \
  || fatal "JOB_TIMEOUT_S must be within 6100..14400 (the Job's own deadline is 6000s), got $JOB_TIMEOUT_S"
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
  # BEFORE NOON IN NEW YORK, "the session to freeze" means YESTERDAY'S: a run at that hour is
  # the morning CATCH-UP (see the second cron in the Jenkinsfile), healing a night the evening
  # chain lost — today's close has not happened and cannot be frozen. A weekend/holiday result
  # exits as NOT_A_TRADING_DAY below, harmlessly.
  #
  # BOTH the hour and the date come from et_session.py, NOT from `date`: this was `date -v-1d`,
  # the BSD spelling, and the agent's PATH resolves `date` to GNU coreutils, where that flag is
  # an error. It cost the 2026-09-01 morning catch-up (build #33, dead in one second on
  # "date: invalid option -- 'v'"). Shell has no portable "yesterday"; python3 does, and the
  # wrapper already requires it.
  ET_FIELDS="$(python3 scripts/ops/et_session.py)" \
    || fatal "could not resolve the New York session date (scripts/ops/et_session.py)"
  ET_HOUR="${ET_FIELDS%% *}"
  SESSION="${ET_FIELDS##* }"
  if [ "$((10#$ET_HOUR))" -lt 12 ]; then
    echo "SESSION not given and it is ${ET_HOUR}:xx in New York — catch-up mode, healing yesterday: $SESSION"
  else
    echo "SESSION not given — freezing today in America/New_York: $SESSION"
  fi
fi
case "$SESSION" in
  [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) : ;;
  *) fatal "SESSION='$SESSION' is not in YYYY-MM-DD form" ;;
esac
# The spelling AND a real calendar date: fromisoformat alone also accepts 20260811 and ISO week
# dates, and a regex alone accepts 2026-99-99.
python3 -c "import datetime,sys; datetime.date.fromisoformat(sys.argv[1])" "$SESSION" 2>/dev/null \
  || fatal "SESSION='$SESSION' is not a valid calendar date"

# WHETHER IT IS A TRADING DAY, AND WHEN IT CLOSED, ARE DECIDED HERE.
#
# They used to be the CLI's call — it carried --skip-non-session and derived the window from
# its own calendar. The batch has neither on purpose: a job that decided for itself that a
# night did not count would be a job that could report success for a night it never ran. So
# the calendar is read here, once, and the answer is passed in.
#
# The CLOSE TIME is not a constant and treating it as one is the expensive mistake. The day
# after Thanksgiving and Christmas Eve close at 13:00; asking for 15:59-16:00 on one of those
# returns nothing at all, and the run would spend twenty-one requests discovering that the
# market had been shut for three hours.
CAL_OUT="$(PYTHONPATH=scripts/jenkins python3 - "$SESSION" <<'CAL'
import datetime, sys
from market_calendar import MarketCalendar
day = datetime.date.fromisoformat(sys.argv[1])
cal = MarketCalendar()
print("trading" if cal.is_trading_day(day) else "closed", cal.close_time(day).strftime("%H:%M"))
CAL
)" || fatal "cannot read the market calendar for $SESSION"
TRADING="${CAL_OUT%% *}"
CLOSE_TIME="${CAL_OUT##* }"
if [ "$TRADING" != "trading" ]; then
  # A Mon-Fri timer must not second-guess the exchange, and a holiday is not an incident.
  echo "STOCK_GEX_CLOSE_BOARD_SKIP session=$SESSION reason=NOT_A_TRADING_DAY"
  SUCCESS=true
  exit 0
fi

# --- ALREADY PUBLISHED? ------------------------------------------------------------------
# The catch-up firing (01:00 ET) and any manual daytime rerun land here on nights the evening
# chain already published. Re-running the batch then is not a no-op — Spring Batch REFUSES an
# already-COMPLETED instance with JobInstanceAlreadyCompleteException, which reads as a failure
# and would paint every healthy morning red. So the question is asked of the STORE, before any
# Job exists. Fail-open on purpose: if this check cannot run (a DB blip), the normal path
# proceeds and the batch itself is the arbiter.
if [ "$REBUILD" = "0" ]; then
  PUBLISHED_GEN="$(kubectl -n bleedingoptions exec statefulset/bo-app-postgres -- \
      psql -U bleedingoptions -d bleedingoptions -tAc \
      "SELECT generation || '|' || board_count FROM gex_close_generation \
       WHERE session = '${SESSION}'::date AND promoted_at IS NOT NULL \
       ORDER BY promoted_at DESC, id DESC LIMIT 1" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ -n "$PUBLISHED_GEN" ]; then
    echo "STOCK_GEX_CLOSE_BOARD_OK session=${SESSION} generation=${PUBLISHED_GEN%%|*} boards=${PUBLISHED_GEN##*|}"
    echo "OK: $SESSION is already published (generation ${PUBLISHED_GEN%%|*}, ${PUBLISHED_GEN##*|} boards) — nothing to do"
    run_flip_track
    SUCCESS=true
    exit 0
  fi
fi
echo "session $SESSION is a trading day, closing at $CLOSE_TIME America/New_York"

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
echo "service image (for the node, not for the maths): $RUNNING_IMAGE"
# THE OLD GUARD IS GONE, AND ITS ABSENCE IS THE POINT — do not restore it without reading this.
#
# It required the freeze's image to be the SERVICE's image, on the reasoning that the freeze
# "shares its arithmetic with the service's own board code, so building it from a different
# image would freeze a board the running service would not have drawn". That was true while
# the freeze drove the service's own Python. It is not true any more: the batch prices in
# Java and the live service prices in Python, so the two can no longer be the same build and
# comparing their digests would only ever fail.
#
# What replaces it is weaker and has to be said plainly. The image is still pinned by digest
# and the digest is recorded in gex_close_run, so "which code drew this board" stays
# answerable. What is NOT enforced any longer is that the frozen board matches the live one.
# That was established by comparison instead — 504 boards for 2026-08-26, structure identical,
# every implied spot bit-identical, worst strike cell $0.034 — and it has to be re-established
# by comparison whenever EITHER side's pricing changes. No check here can do it for you.

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
# The pod's digest is recorded here and compared against ITSELF at the end of the run.
#
# It used to be checked for its image digest too, as a second line after the deployment-level
# comparison — the SERVING pod rather than the Deployment's intent, so a rollout in progress
# could not slip a half-old build past. That check compared the freeze's image against the
# service's, and the freeze no longer has the service's image: it prices in Java, the service
# prices in Python. See the note above the pin. Leaving the comparison in place would fail
# every single night, which is a guard that protects nothing and blocks everything.
#
# What the pod still decides is which node owns the hostPath, and that is not optional: with
# the boards on a node-local path, publishing onto the wrong node writes a session no reader
# can see. Whether that pod was ROLLED mid-run is still checked at the end, and that check is
# untouched: it is about the node moving under the job, not about shared arithmetic.
POD_DIGEST="${POD_IMAGE_ID##*@}"
[ -n "$POD_DIGEST" ] || fatal "pod $SERVICE_POD reports no resolvable image digest for container
       '$SERVICE_CONTAINER' — refusing to freeze without knowing what is serving"

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
JOB_NAME="stock-gex-close-batch-$(date -u +%Y%m%d-%H%M%S)-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
RENDER="$(mktemp)"
LOGS="$(mktemp)"
sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" \
    -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__SESSION__|${SESSION}|g" \
    -e "s|__NODE_NAME__|${NODE_NAME}|g" \
    -e "s|__KEEP_SESSIONS__|${KEEP_SESSIONS}|g" \
    -e "s|__WAIT_FOR_VENDOR_S__|${WAIT_FOR_VENDOR_S}|g" \
    -e "s|__REBUILD__|${REBUILD}|g" \
    -e "s|__RUN_SOURCE__|${RUN_SOURCE}|g" \
    -e "s|__IMAGE_DIGEST__|${PINNED_IMAGE##*@}|g" \
    -e "s|__CLOSE_TIME__|${CLOSE_TIME}|g" \
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

CLOSE_ATTEMPT=1
while :; do
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
# The BATCH's grammar (oe-close-batch JobExit, since 2026-09-01), not the retired CLI's. The
# old oiTradeDate/roots/quotes line was the CLI's; the batch never printed it, so every night
# since the switchover was reported FAILED while the boards published fine — the receipt and
# this regex must move in lockstep or the chain cries wolf nightly.
RECEIPT_RE="^STOCK_GEX_CLOSE_BOARD_OK session=${SESSION} generation=[^ ]+ boards=[0-9]+$"
SKIP_RE="^STOCK_GEX_CLOSE_BOARD_SKIP session=${SESSION} reason=[A-Z_]+$"
ANY_OUTCOME_RE='^STOCK_GEX_CLOSE_BOARD_(OK|SKIP) '
RECEIPT="$(grep -E "$RECEIPT_RE" "$LOGS" | tail -1 || true)"
SKIP="$(grep -E "$SKIP_RE" "$LOGS" | tail -1 || true)"
OUTCOMES="$(grep -cE "$ANY_OUTCOME_RE" "$LOGS" || true)"

if [ "$state" = "succeeded" ]; then
  break
fi
if [ "$CLOSE_ATTEMPT" -lt "$CLOSE_ATTEMPTS" ]; then
  # A vendor 504 heals with time, and a resumed batch reuses every chunk already fetched, so a
  # retry costs only the failed remainder. The pause matters more than the count: attempt 1
  # started right at the close; by attempt 2 or 3 the vendor's data has ripened.
  echo "attempt ${CLOSE_ATTEMPT}/${CLOSE_ATTEMPTS} did not succeed (state=$state) — retrying in ${CLOSE_RETRY_WAIT_S}s; the batch RESUMES, chunks already fetched are reused"
  CLOSE_ATTEMPT=$(( CLOSE_ATTEMPT + 1 ))
  sleep "$CLOSE_RETRY_WAIT_S"
  JOB_NAME="stock-gex-close-batch-$(date -u +%Y%m%d-%H%M%S)-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
  sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" \
      -e "s|__JOB_NAME__|${JOB_NAME}|g" \
      -e "s|__SESSION__|${SESSION}|g" \
      -e "s|__NODE_NAME__|${NODE_NAME}|g" \
      -e "s|__KEEP_SESSIONS__|${KEEP_SESSIONS}|g" \
      -e "s|__WAIT_FOR_VENDOR_S__|${WAIT_FOR_VENDOR_S}|g" \
      -e "s|__REBUILD__|${REBUILD}|g" \
      -e "s|__RUN_SOURCE__|${RUN_SOURCE}|g" \
      -e "s|__IMAGE_DIGEST__|${PINNED_IMAGE##*@}|g" \
      -e "s|__CLOSE_TIME__|${CLOSE_TIME}|g" \
      "$TEMPLATE" >"$RENDER"
  continue
fi
# NOT "nothing was published". A container can promote and then exit nonzero, or be killed
# before the Job records success — the promote is atomic and happens before the receipt is
# printed. The marker already says UNKNOWN; this message must not contradict it.
fatal "$JOB_NAME did not succeed (state=$state) after ${CLOSE_ATTEMPTS} attempts. Whether a
     session was promoted for $SESSION is UNKNOWN: the promote happens inside the container,
     before its receipt, and this identity cannot read the node. Check what dt=$SESSION points
     at under /home/options-edge/stock-gex-oi/close on $NODE_NAME. Every OTHER session is
     untouched — a build only ever writes a new generation and moves one symlink."
done
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
ROOTS="$(printf '%s' "$RECEIPT" | sed -n 's/.* boards=\([0-9]*\)$/\1/p')"
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

run_flip_track

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
