#!/usr/bin/env bash
# Nightly stock-gex OI-index refresh — the operator side of
# k8s/jobs/stock-gex-oi-snapshot-job.yaml (read that file's header first: it explains why the
# workload is a Job template in git and NOT a CronJob object, and why publication is staged).
#
# THE INVARIANT THIS SCRIPT MAINTAINS
#
#   The pod-template annotation options-edge.io/oi-index-sha on deployment/stock-gex-service
#   names the index generation the service is SUPPOSED to be serving. The service loads its index
#   exactly once, in main() (OiIndex.load) — there is no hot-reload — so "which generation is the
#   pod serving" is otherwise unobservable and unrecoverable state living inside a process.
#
#   Writing that sha into the pod template is what triggers the rollout AND what records the
#   intent durably, in the cluster, where the next run can see it:
#       annotation == installed sha   =>  the running pod loaded the installed index
#       annotation != installed sha   =>  an index was published that nothing rolled out onto
#   Every run reconciles that drift FIRST and re-establishes it LAST, so a run that dies anywhere
#   (Jenkins abort, agent loss, a killed Job) leaves a state the NEXT run can see and repair. No
#   run has to prove what a previous run did.
#
#   A MISSING annotation is treated as "no claim", not as drift: `service-deploy` renders the
#   Deployment from git and its apply drops the annotation, and that must not cost a spurious
#   restart. The next publish writes it back.
#
# WHAT IT DOES, in order, fail-closed at every step:
#   1. asserts the kubectl identity IS the deployer SA (the only principal the
#      options-edge-jenkins-only-workloads admission policy lets create Jobs) AND that the
#      kubeconfig points at the expected production API server,
#   2. resolves the env's stock-gex image by EXACT key from image-tags/<env>.yaml, digest-pins it,
#      and requires it to be the digest the service Deployment is ACTUALLY running: the Job
#      validates the staged index with that image's copy of the loader, so validating with a
#      different build proves nothing about the pod that has to boot on it,
#   3. reads the installed generation (tradeDate + sha256, from ONE file descriptor) through the
#      service pod, pins the Job to that pod's node (the index is a node-local hostPath), and
#      reconciles any pre-existing annotation/index drift before producing anything new,
#   4. renders + creates the Job, waits for it, prints its whole log,
#   5. establishes what was published: the receipt (STOCK_GEX_OI_INDEX_OK ... sha256=...) when
#      there is one, otherwise the installed generation read back — a missing receipt does NOT
#      prove nothing was published, because the container can be killed between the rename and
#      the receipt,
#   6. publishes the intent (annotation = published sha) and lets the Deployment controller roll
#      the pod, then proves the result: a NEW pod (different UID) carrying that annotation, whose
#      boot line reports the published tradeDate, whose mounted file still hashes to the published
#      sha, and whose own /readyz answers 200,
#   7. prunes old TERMINAL Jobs, best-effort, never blocking the critical path.
#
# ACCEPTED RESIDUAL RISKS (stated, not papered over):
#   * A concurrent `service-deploy SERVICE=stock-gex` can change the service image while this
#     run's Job is producing. The digest is re-checked immediately before the rollout and the run
#     aborts on a change, but the two pipelines are not mutually excluded — Jenkins has no shared
#     lock between them. Do not deploy stock-gex at 20:30 ET.
#   * The rename is fsynced by the Job, but the directory fsync happens a few instructions later.
#     A host crash inside that window can lose a rename this script already saw. The next run's
#     drift check finds it: the annotation would name a generation the file no longer has.
#   * Nothing here notices a Jenkins job that never runs. Whatever watches prod should alert on
#     `stockgex-oi-snapshot` having no successful build in > 3 days.
#
# Usage (normally from Jenkinsfile.stockgex-oi-snapshot):
#   ENVIRONMENT=production KUBECONFIG=<deployer kubeconfig> scripts/ops/stock-gex-oi-snapshot.sh
#
# Env:
#   ENVIRONMENT      production (the only supported env today — dev parity is deliberately out of
#                    scope: dev is scaled to 0 nightly and its hostPath lives inside the
#                    docker-desktop VM, so it needs an artifact-copy step, not a second pull)
#   TRADE_DATE       optional YYYY-MM-DD operator override; EMPTY on the scheduled run
#   RESTART_SERVICE  true (default) | false — false publishes but leaves the pod on its old index
#   JOB_TIMEOUT_S    client-side wait, default 5700 (> the Job's own 5400s activeDeadlineSeconds)
#   KEEP_JOBS        default 5 (total retained, including this run's Job)
#   NAMESPACE        default options-edge
#   EXPECTED_API_SERVER  default https://192.168.100.252:6443
#   DRY_RUN          true = render + pin + server-side validate only; creates nothing
set -euo pipefail
cd "$(dirname "$0")/../.."

ENVIRONMENT="${ENVIRONMENT:-production}"
NAMESPACE="${NAMESPACE:-options-edge}"
TRADE_DATE="${TRADE_DATE:-}"
RESTART_SERVICE="${RESTART_SERVICE:-true}"
JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-5700}"
KEEP_JOBS="${KEEP_JOBS:-5}"
DRY_RUN="${DRY_RUN:-false}"
EXPECTED_API_SERVER="${EXPECTED_API_SERVER:-https://192.168.100.252:6443}"

TEMPLATE="k8s/jobs/stock-gex-oi-snapshot-job.yaml"
DEPLOYMENT="stock-gex-service"
SERVICE_CONTAINER="stock-gex"          # the service container inside that Deployment's pod
IMAGE_KEY="stock-gex-service"          # exact key in image-tags/<env>.yaml
IMAGE_REPO_SUFFIX="/options-edge-stock-gex"
JOB_LABEL="app.kubernetes.io/name=stock-gex-oi-snapshot"
DEPLOYER="system:serviceaccount:options-edge:jenkins-deployer"
INDEX_PATH="/oi-index/stock-gex-oi-index.json.gz"
SHA_ANNOTATION="options-edge.io/oi-index-sha"
HEALTH_PORT=8022
JOB_NAME=""
JOB_OWNED=false
SUCCESS=false

fatal() { echo "FATAL: $*" >&2; exit 1; }

# An abandoned RUNNING Job keeps the container's writer lock, so an unsuccessful exit (failure,
# Jenkins abort) must take it down — but ONLY a Job this invocation actually created
# (JOB_OWNED), never one whose name merely matches. A Job that already reached a terminal state
# is left in place: it holds nothing and its pod log is the post-mortem evidence.
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
# These arrive as free-text Jenkins parameters and are used in arithmetic and in kubectl
# arguments. Bash evaluates arithmetic operands recursively, so an unvalidated string here is a
# code-execution surface on the agent, and a malformed one is an unpredictable failure.
[ "$ENVIRONMENT" = "production" ] \
  || fatal "ENVIRONMENT='$ENVIRONMENT' is not supported (production only for now — see header)"
case "$RESTART_SERVICE" in true|false) : ;; *) fatal "RESTART_SERVICE must be true or false, got '$RESTART_SERVICE'" ;; esac
case "$DRY_RUN" in true|false) : ;; *) fatal "DRY_RUN must be true or false, got '$DRY_RUN'" ;; esac
case "$JOB_TIMEOUT_S" in ''|*[!0-9]*) fatal "JOB_TIMEOUT_S must be digits, got '$JOB_TIMEOUT_S'" ;; esac
case "$KEEP_JOBS" in ''|*[!0-9]*) fatal "KEEP_JOBS must be digits, got '$KEEP_JOBS'" ;; esac
[ "$JOB_TIMEOUT_S" -ge 5500 ] && [ "$JOB_TIMEOUT_S" -le 10800 ] \
  || fatal "JOB_TIMEOUT_S must be within 5500..10800 (the Job's own deadline is 5400s), got $JOB_TIMEOUT_S"
[ "$KEEP_JOBS" -ge 1 ] && [ "$KEEP_JOBS" -le 100 ] || fatal "KEEP_JOBS must be within 1..100, got $KEEP_JOBS"
[ -f "$TEMPLATE" ] || fatal "missing Job template $TEMPLATE"
command -v yq >/dev/null 2>&1 || fatal "yq is required"
command -v jq >/dev/null 2>&1 || fatal "jq is required"
if [ -n "$TRADE_DATE" ]; then
  # The documented spelling AND a real calendar date: fromisoformat alone also accepts 20260811
  # and ISO week dates, and a regex alone accepts 2026-99-99.
  case "$TRADE_DATE" in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) : ;;
    *) fatal "TRADE_DATE='$TRADE_DATE' is not in YYYY-MM-DD form" ;;
  esac
  python3 -c "import datetime,sys; datetime.date.fromisoformat(sys.argv[1])" "$TRADE_DATE" 2>/dev/null \
    || fatal "TRADE_DATE='$TRADE_DATE' is not a valid calendar date"
fi

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
       The deployer service-account name alone does not identify a cluster — refusing to write
       a production index against an unexpected API server."

# Reads the CURRENT Deployment. Called again before the rollout: the image must not change under
# this run (a concurrent service-deploy) and the annotation is read where it is written.
deployment_json() { kubectl -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o json; }
container_image() {
  printf '%s' "$1" | jq -er --arg c "$SERVICE_CONTAINER" \
    '[.spec.template.spec.containers[] | select(.name == $c)] | if length == 1 then .[0].image else error("expected exactly one \($c) container") end'
}
DEPLOY_JSON="$(deployment_json 2>/dev/null)" \
  || fatal "deployment/$DEPLOYMENT not found in $NAMESPACE on $API_SERVER"
REPLICAS="$(printf '%s' "$DEPLOY_JSON" | jq -r '.spec.replicas')"
# The verification below proves ONE pod is serving the new generation. stock-gex is single-writer
# by design (replicas: 1 + Recreate, one Databento live subscription per env); if that ever
# changes, this script must enumerate every replica instead of silently proving less.
[ "$REPLICAS" = "1" ] \
  || fatal "$DEPLOYMENT has spec.replicas=$REPLICAS — this script's proof covers ONE pod. Teach
       it to enumerate the ReplicaSet's pods before running a multi-replica service."
# By NAME, never containers[0]: a sidecar inserted ahead of the service container would otherwise
# silently become the thing whose image is compared and whose logs are read.
RUNNING_IMAGE="$(container_image "$DEPLOY_JSON")" \
  || fatal "$DEPLOYMENT does not have exactly one container named '$SERVICE_CONTAINER'"

# --- 2. resolve + digest-pin the image, and match it to the DEPLOYED one -----------------
# EXACT key, not a substring scan of the values: another entry whose value happens to contain the
# same repository would otherwise win by document order.
MUTABLE_IMAGE="$(yq -er ".images.\"${IMAGE_KEY}\"" "image-tags/${ENVIRONMENT}.yaml" 2>/dev/null || true)"
[ -n "$MUTABLE_IMAGE" ] && [ "$MUTABLE_IMAGE" != "null" ] \
  || fatal "image-tags/${ENVIRONMENT}.yaml has no '${IMAGE_KEY}' entry"
case "$MUTABLE_IMAGE" in
  *"${IMAGE_REPO_SUFFIX}":*) : ;;
  *) fatal "image-tags/${ENVIRONMENT}.yaml '${IMAGE_KEY}' is '$MUTABLE_IMAGE', which is not a ${IMAGE_REPO_SUFFIX} image" ;;
esac
# Production is a linux/amd64 node behind an insecure (http) registry. These are FACTS about the
# target, not defaults to inherit: an agent that exports DEPLOY_PLATFORM=linux/arm64 would
# otherwise pin an arm64 digest that cannot run there.
export DEPLOY_PLATFORM="linux/amd64"
export REGISTRY_SCHEME="http"
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" \
  || fatal "cannot resolve registry digest for $MUTABLE_IMAGE (is the image built + pushed for $ENVIRONMENT?)"
case "$PINNED_IMAGE" in
  *@sha256:*) : ;;
  *) fatal "refusing to run a Job on an unpinned image ref: $PINNED_IMAGE" ;;
esac
PINNED_DIGEST="${PINNED_IMAGE##*@}"
RUNNING_DIGEST="${RUNNING_IMAGE##*@}"
echo "image: $MUTABLE_IMAGE -> $PINNED_IMAGE"
echo "deployment image: $RUNNING_IMAGE"
# Compare DIGESTS, not the whole ref: the Deployment's string is written by service-deploy.sh as
# repo:tag@sha256:..., which pin_ref also produces, but a tag spelling difference must not be
# mistaken for a build difference.
[ "$PINNED_DIGEST" = "$RUNNING_DIGEST" ] \
  || fatal "image-tags/${ENVIRONMENT}.yaml resolves to $PINNED_DIGEST but $DEPLOYMENT runs
       $RUNNING_DIGEST. The Job validates the new index with ITS image's copy of the loader, so
       building it with a different image proves nothing about the pod that must boot on it.
       Deploy the service first (service-deploy SERVICE=stock-gex) or roll the tag back."

# --- 3. service pod: node, installed generation, pre-existing drift -----------------------
SELECTOR="$(printf '%s' "$DEPLOY_JSON" | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')"
[ -n "$SELECTOR" ] && [ "$SELECTOR" != "null" ] || fatal "$DEPLOYMENT has no matchLabels selector"
# matchExpressions cannot be expressed as a label-selector string here; refuse rather than
# silently select the wrong pods.
[ "$(printf '%s' "$DEPLOY_JSON" | jq -r '(.spec.selector.matchExpressions // []) | length')" = "0" ] \
  || fatal "$DEPLOYMENT uses selector matchExpressions — this script only understands matchLabels"

service_pod() {
  kubectl -n "$NAMESPACE" get pods -l "$SELECTOR" --field-selector=status.phase=Running \
    --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true
}
# "<tradeDate> <sha256>" for the index the service pod can see, read through ONE descriptor so
# the hash and the tradeDate cannot come from two different files across an atomic rename.
# Prints MISSING when the file positively does not exist, and nothing at all when the read
# failed — callers must never treat an empty result as "unchanged".
installed_generation() {
  local pod="$1"
  [ -n "$pod" ] || return 0
  kubectl -n "$NAMESPACE" exec "$pod" -c "$SERVICE_CONTAINER" -- python -c "
import gzip, hashlib, io, json, sys
try:
    fh = open('$INDEX_PATH', 'rb')
except FileNotFoundError:
    print('MISSING')
    sys.exit(0)
with fh:
    h = hashlib.sha256()
    for b in iter(lambda: fh.read(1 << 20), b''):
        h.update(b)
    fh.seek(0)
    with gzip.GzipFile(fileobj=fh) as gz:
        d = json.load(io.TextIOWrapper(gz, encoding='utf-8'))
print(d['tradeDate'] + ' ' + h.hexdigest())" 2>/dev/null | tr -d '\r' || true
}
# One anchored grammar for a generation string, used everywhere one is consumed.
valid_generation() {
  printf '%s' "$1" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9a-f]{64}$'
}
desired_sha() {
  printf '%s' "$1" | jq -r --arg k "$SHA_ANNOTATION" '.spec.template.metadata.annotations[$k] // ""'
}

SERVICE_POD="$(service_pod)"
[ -n "$SERVICE_POD" ] || fatal "no Running $DEPLOYMENT pod — cannot determine the node that owns
       the index hostPath, nor read the installed generation. Bring the service up first."
NODE_NAME="$(kubectl -n "$NAMESPACE" get pod "$SERVICE_POD" -o jsonpath='{.spec.nodeName}')"
[ -n "$NODE_NAME" ] || fatal "pod $SERVICE_POD reports no nodeName"
PRE_GENERATION="$(installed_generation "$SERVICE_POD")"
DESIRED_SHA="$(desired_sha "$DEPLOY_JSON")"
echo "service pod: $SERVICE_POD on node $NODE_NAME"
echo "installed generation (before): ${PRE_GENERATION:-<unreadable>}"
echo "pod-template ${SHA_ANNOTATION}: ${DESIRED_SHA:-<unset>}"
# An unreadable pre-run generation must not be compared later as if it were a value: an empty
# string differs from every real generation and would turn a failed producer into a false
# "publication happened". Refuse up front instead.
valid_generation "$PRE_GENERATION" \
  || fatal "could not read the installed generation from $SERVICE_POD (got '${PRE_GENERATION:-<nothing>}').
       Refusing to start: without a trustworthy before-value this run cannot tell a publication
       from a failure. Check $INDEX_PATH on node $NODE_NAME."
PRE_SHA="${PRE_GENERATION##* }"

# RESTART_SERVICE=false publishes deliberately without rolling out, and relies on the annotation
# still naming the OLD generation so the next run sees drift and recovers. With no annotation at
# all there is nothing to be in drift WITH: the next run would see "no claim", skip
# reconciliation, and — if its own producer failed — leave the service on a stale in-memory index
# indefinitely. Refuse before anything is created.
if [ "$RESTART_SERVICE" != "true" ] && [ -z "$DESIRED_SHA" ] && [ "$DRY_RUN" != "true" ]; then
  fatal "RESTART_SERVICE=false requires an existing ${SHA_ANNOTATION} annotation on $DEPLOYMENT
       (it is what the next run reconciles against), and there is none — service-deploy's apply
       drops it. Run once with RESTART_SERVICE=true to re-establish the annotation."
fi

# rollout_to <sha> <expected trade date>: make the pod-template annotation name that generation
# and prove the resulting pod is actually serving it. Writing the sha into the pod template IS
# the rollout trigger, so the pod cannot fail to be replaced, and it is the durable record the
# next run reads.
rollout_to() {
  local sha="$1" trade_date="$2" json old_uid old_ann pod pod_uid pod_ann boot live ready
  json="$(deployment_json)"
  # Re-check the image RIGHT BEFORE rolling: a service-deploy that landed while the Job was
  # producing would mean rolling a pod whose loader never validated this file.
  local now_digest; now_digest="$(container_image "$json")"
  now_digest="${now_digest##*@}"
  [ "$now_digest" = "$RUNNING_DIGEST" ] \
    || fatal "$DEPLOYMENT image changed during this run ($RUNNING_DIGEST -> $now_digest) — the
       published index was validated by the OLD image's loader. Not rolling out. Re-run this job
       so the index is re-validated against the deployed image."
  old_uid="$(kubectl -n "$NAMESPACE" get pods -l "$SELECTOR" --field-selector=status.phase=Running \
    --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.uid}' 2>/dev/null || true)"
  old_ann="$(desired_sha "$json")"
  # A byte-identical rebuild (same session, same bytes) produces the SAME sha. The merge patch is
  # then a no-op, the controller creates no new pod, and demanding a new UID would fail a run
  # whose pod is already serving exactly these bytes. Require replacement only when the template
  # annotation actually changes; the proofs below run either way.
  local expect_new_pod=true
  if [ "$old_ann" = "$sha" ]; then
    expect_new_pod=false
    echo "=== rollout: ${SHA_ANNOTATION} already $sha — no template change, verifying in place ==="
  else
    echo "=== rollout: ${SHA_ANNOTATION}=$sha (was ${old_ann:-<unset>}) ==="
  fi
  kubectl -n "$NAMESPACE" patch deployment "$DEPLOYMENT" --type=merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"${SHA_ANNOTATION}\":\"${sha}\"}}}}}"
  kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout=300s

  pod="$(service_pod)"
  [ -n "$pod" ] || fatal "no running $DEPLOYMENT pod after the rollout"
  pod_uid="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}')"
  # jq, not jsonpath: the annotation key contains both a dot and a slash, and getting that
  # escaping subtly wrong would silently yield an empty string — i.e. a check that never fires.
  pod_ann="$(kubectl -n "$NAMESPACE" get pod "$pod" -o json | jq -r --arg k "$SHA_ANNOTATION" '.metadata.annotations[$k] // ""')"
  # (a) a genuinely NEW pod carrying this generation's annotation — not a survivor that happens
  #     to predate the rollout, and not one from an older template revision.
  if [ "$expect_new_pod" = "true" ] && [ -n "$old_uid" ] && [ "$pod_uid" = "$old_uid" ]; then
    fatal "$pod (uid $pod_uid) is the same pod as before the rollout — the template change did
       not replace it"
  fi
  [ "$pod_ann" = "$sha" ] \
    || fatal "$pod carries ${SHA_ANNOTATION}='${pod_ann:-<unset>}', expected '$sha' — it is from a
       different pod-template revision"
  # (b) the boot line must report the session this run published.
  boot="$(kubectl -n "$NAMESPACE" logs "$pod" -c "$SERVICE_CONTAINER" --tail=-1 | grep -E 'OI index loaded: tradeDate=' | tail -1 || true)"
  echo "pod $pod boot line: ${boot:-<none>}"
  printf '%s' "$boot" | grep -q "tradeDate=$trade_date" \
    || fatal "$pod did not load the expected index (wanted tradeDate=$trade_date)"
  # (c) the file on the mount must STILL be that generation. It is not a read of the pod's
  #     in-memory copy — that is not observable — but with (a) it pins what the pod opened.
  live="$(installed_generation "$pod")"
  valid_generation "$live" || fatal "could not re-read the installed generation from $pod"
  [ "${live##* }" = "$sha" ] \
    || fatal "the index on the mount ($live) is no longer the generation this rollout targeted
       ($sha) — another writer replaced it"
  # (d) /readyz is the service's own freshness gate (engine.ready() re-runs is_fresh on the
  #     LOADED index), so a 200 from the pod's own health port is independent proof.
  ready="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$SERVICE_CONTAINER" -- python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:${HEALTH_PORT}/readyz', timeout=10).status)" 2>/dev/null | tr -d '\r')"
  [ "$ready" = "200" ] || fatal "$pod /readyz returned '${ready:-<no answer>}', expected 200"
  DESIRED_SHA="$sha"
  SERVICE_POD="$pod"
  echo "OK: $DEPLOYMENT is serving OI index tradeDate=$trade_date sha256=$sha"
}

# Pre-existing drift: a previous run published but never rolled out (agent loss, abort). An UNSET
# annotation is not drift — service-deploy's apply drops it, and that must not cost a restart.
# DRY_RUN is excluded: it promises to create and change NOTHING, and reconciling drift would
# patch the Deployment and restart the pod.
if [ "$DRY_RUN" = "true" ] && [ -n "$DESIRED_SHA" ] && [ "$DESIRED_SHA" != "$PRE_SHA" ]; then
  echo "DRY_RUN=true — NOT reconciling the existing drift (${SHA_ANNOTATION}=$DESIRED_SHA vs"
  echo "               installed $PRE_SHA); a real run would roll the service onto the installed"
  echo "               index first."
fi
if [ "$DRY_RUN" != "true" ] && [ -n "$DESIRED_SHA" ] && [ "$DESIRED_SHA" != "$PRE_SHA" ]; then
  echo "DRIFT: the pod template asks for $DESIRED_SHA but the installed index is $PRE_SHA —"
  echo "       a previous run published without completing its rollout. Reconciling first."
  if [ "$RESTART_SERVICE" = "true" ]; then
    rollout_to "$PRE_SHA" "${PRE_GENERATION%% *}"
  else
    echo "       RESTART_SERVICE=false — leaving the drift in place."
  fi
fi

# --- 4. render + create ----------------------------------------------------------------
# Job names are immutable; the UTC stamp keeps creation order and 8 random bytes make a
# same-second collision between two invocations effectively impossible. Ownership is only
# claimed AFTER create succeeds, so a losing invocation can never delete the winner's Job.
JOB_SUFFIX="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
JOB_NAME="stock-gex-oi-snapshot-$(date -u +%Y%m%d-%H%M%S)-${JOB_SUFFIX}"
# Both are removed by the EXIT trap, on every path.
RENDER="$(mktemp)"
LOGS="$(mktemp)"
sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" \
    -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__TRADE_DATE__|${TRADE_DATE}|g" \
    -e "s|__NODE_NAME__|${NODE_NAME}|g" \
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

echo "=== creating Job $JOB_NAME (node $NODE_NAME) ==="
kubectl -n "$NAMESPACE" create -f "$RENDER"
JOB_OWNED=true

# --- 5. wait, then establish what was published -------------------------------------------
echo "waiting up to ${JOB_TIMEOUT_S}s for $JOB_NAME ..."
deadline=$(( $(date +%s) + JOB_TIMEOUT_S ))
# ONE snapshot per poll: reading `succeeded` and the Failed condition with two separate calls can
# straddle the completion and see neither.
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
if [ "$state" != "succeeded" ] && [ "$state" != "failed" ]; then
  # One authoritative final read — the Job may have completed between the last poll and the
  # deadline.
  state="$(job_state || echo 'unreadable')"
fi
echo "job state: $state"

# Logs are evidence, and a transient log-serving error must not be mistaken for "no receipt".
for _try in 1 2 3 4 5; do
  if kubectl -n "$NAMESPACE" logs "job/$JOB_NAME" --tail=-1 >"$LOGS" 2>/dev/null; then break; fi
  echo "logs for $JOB_NAME not available yet (attempt $_try) ..."
  : >"$LOGS"
  sleep 6
done
echo "===== $JOB_NAME log ====="
cat "$LOGS"
echo "===== end log ====="

# The receipt grammar is fixed and anchored; anything else is not a receipt. Exactly one must be
# present (the Job prints it once, after the rename).
RECEIPT_RE='^STOCK_GEX_OI_INDEX_OK tradeDate=[0-9]{4}-[0-9]{2}-[0-9]{2} roots=[0-9]+ contracts=[0-9]+ bytes=[0-9]+ sha256=[0-9a-f]{64}$'
RECEIPT_COUNT="$(grep -cE "$RECEIPT_RE" "$LOGS" || true)"
RECEIPT=""
if [ "$RECEIPT_COUNT" = "1" ]; then
  RECEIPT="$(grep -E "$RECEIPT_RE" "$LOGS")"
elif [ "$RECEIPT_COUNT" != "0" ]; then
  fatal "$JOB_NAME printed $RECEIPT_COUNT receipt lines — expected exactly one; refusing to guess
       which generation was published"
fi

PUBLISHED_TRADE_DATE=""
PUBLISHED_SHA=""
if [ -n "$RECEIPT" ] && [ "$state" = "succeeded" ]; then
  PUBLISHED_TRADE_DATE="$(printf '%s' "$RECEIPT" | sed -n 's/^.* tradeDate=\([0-9-]*\) .*$/\1/p')"
  PUBLISHED_SHA="$(printf '%s' "$RECEIPT" | sed -n 's/^.* sha256=\([0-9a-f]*\)$/\1/p')"
  echo "index receipt: $RECEIPT"
else
  # NO RECEIPT (or a Job the API says did not succeed) does not prove nothing was published: the
  # container can be killed between os.replace and the receipt, and logs can be unavailable. Ask
  # the mount what is installed now and compare with the pre-run generation.
  echo "no usable receipt (state=$state, receipts=$RECEIPT_COUNT) — reconciling the installed generation"
  RECON_POD="$(service_pod)"
  POST_GENERATION="$(installed_generation "$RECON_POD")"
  echo "installed generation (after): ${POST_GENERATION:-<unreadable>}"
  valid_generation "$POST_GENERATION" \
    || fatal "$JOB_NAME produced no receipt (state=$state) AND the installed index could not be
       read back — publication state is UNKNOWN. Inspect the Job pod and $INDEX_PATH on node
       $NODE_NAME before the next run."
  if [ "$POST_GENERATION" = "$PRE_GENERATION" ]; then
    if [ "$state" = "succeeded" ]; then
      # The Job COMPLETED — the container ran past the rename, the directory fsync, the receipt
      # and the staging cleanup, so publication did happen. An unchanged generation here just
      # means the rebuild was byte-identical (the same session rebuilt, e.g. a holiday re-run or
      # an operator re-run of the same day); with the logs unavailable the before/after
      # comparison simply cannot distinguish that from "nothing was written", and a completed
      # Job settles it.
      echo "Job succeeded but its logs were unavailable, and the installed generation is
       unchanged ($PRE_GENERATION) — a byte-identical rebuild of the same session. Proceeding
       with that generation."
    else
      fatal "$JOB_NAME did not publish (state=$state). The installed index is unchanged
       ($PRE_GENERATION) — the producer writes into a per-Job staging directory, so a failure
       before the rename leaves the previous index exactly as it was."
    fi
  else
    # A changed generation proves SOMETHING published; it does not prove this Job did (a manual
    # run could have). Either way the invariant below is the same: the pod must serve what is
    # installed.
    echo "WARNING: the installed index changed without a usable receipt"
    echo "         ($PRE_GENERATION -> $POST_GENERATION). Whatever wrote it, it passed the Job's"
    echo "         pre-rename checks; continuing so the service does not keep a stale in-memory copy."
  fi
  PUBLISHED_TRADE_DATE="${POST_GENERATION%% *}"
  PUBLISHED_SHA="${POST_GENERATION##* }"
fi

valid_generation "$PUBLISHED_TRADE_DATE $PUBLISHED_SHA" \
  || fatal "could not determine the published generation (receipt='$RECEIPT')"
if [ -n "$TRADE_DATE" ] && [ "$PUBLISHED_TRADE_DATE" != "$TRADE_DATE" ]; then
  fatal "requested TRADE_DATE=$TRADE_DATE but the published index says $PUBLISHED_TRADE_DATE"
fi

prune_terminal_jobs() {
  # Best-effort and LAST: nothing else prunes these (there is no CronJob history limit), but a
  # transient API error here must never undo a successful publish + rollout. Only Jobs whose
  # Complete/Failed condition is True are candidates; this run's own Job is counted against the
  # retention budget but never deleted.
  set +e
  local names total del
  names="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" --sort-by=.metadata.creationTimestamp -o json 2>/dev/null \
    | jq -r --arg self "$JOB_NAME" '.items[]
        | select(.metadata.name != $self)
        | select([(.status.conditions // [])[] | select((.type == "Complete" or .type == "Failed") and .status == "True")] | length >= 1)
        | .metadata.name')"
  total="$(printf '%s\n' "$names" | grep -c .)"
  del=$(( total + 1 - KEEP_JOBS ))     # +1: this run's Job also occupies a retention slot
  if [ "$del" -gt 0 ]; then
    echo "pruning $del terminal snapshot Job(s) (keeping $(( KEEP_JOBS - 1 )) of $total plus this run's)"
    printf '%s\n' "$names" | sed -n "1,${del}p" | while read -r n; do
      [ -n "$n" ] && kubectl -n "$NAMESPACE" delete "job/$n" --ignore-not-found
    done
  else
    echo "no old snapshot Jobs to prune (terminal=$total, keep=$KEEP_JOBS)"
  fi
  set -e
}

# --- 6. roll the service onto the published generation ------------------------------------
if [ "$RESTART_SERVICE" != "true" ]; then
  SUCCESS=true
  echo "RESTART_SERVICE=false — index $PUBLISHED_TRADE_DATE ($PUBLISHED_SHA) is published, but the"
  echo "RUNNING pod still serves the previous one (stock-gex loads the index once, at boot), and"
  echo "the ${SHA_ANNOTATION} annotation is left pointing at the old generation so the NEXT run"
  echo "reconciles it."
  prune_terminal_jobs
  exit 0
fi
rollout_to "$PUBLISHED_SHA" "$PUBLISHED_TRADE_DATE"
SUCCESS=true

# --- 7. prune old TERMINAL Jobs ------------------------------------------------------------
prune_terminal_jobs
exit 0
