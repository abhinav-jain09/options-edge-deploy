#!/usr/bin/env bash
# ACTIVATE (or check, or deactivate) THE DEDICATED v7 RESEARCH WRITER of one environment — the operator side of
# k8s/base/zerodte-research-writer-deployment.yaml (replicas 0 as shipped) and k8s/jobs/zerodte-writer-check-job.yaml. Increment 9e (design
# §5.3 / §14 / §16; inc 9 consult Q9: "Activation to one replica requires an exact committed provisioned/<env>.yaml, verified era, and a
# successful writer dry/readiness check"). The pattern is scripts/ops/zerodte-research-migrate.sh (increment 9c): the cluster pinned by its
# CA, the deployment / migration exclusion lock held through the effect, the receipt held to the letter, the same-build check receipt binding
# the activation, HEAD == PERMITTED_SHA re-checked before the effect.
#
# ACTION=check (the stage that ALWAYS runs): the committed receipt deploy/zerodte/provisioned/<env>.yaml is validated and BOUND to its
#   declaration (scripts/ci/validate-zerodte-provisioning.sh), its identity read (eraId, generation, lineage, clusterId, ledgerTopicId); the
#   live Deployment zerodte-research-writer must already run the digest-pinned image this run pins (the service slice is deployed FIRST by
#   Jenkinsfile.service-deploy); under the lock, a CHECK Job runs ZeroDteResearchWriterMain --check with the receipt's identity as LITERALS and
#   prints ONE line: WRITER_CHECK READY eraId= generation= clusterId= framesTopicId= position= cursorOffset= (0) — bound to the receipt —
#   or WRITER_CHECK REFUSED code=<Code> exit=68 / UNAVAILABLE exit=69 / USAGE exit=64 (each a refusal here). Nothing is consumed, nothing is
#   written; the check receipt (build, env, receipt sha256, HEAD) is written for the activate stage of the SAME build.
# ACTION=activate (CONFIRM=true): the same-build check receipt required, HEAD == PERMITTED_SHA re-checked, the CHECK Job run AGAIN right
#   before the effect (the live world may have moved), then the identity ConfigMap zerodte-writer-identity applied from the receipt and
#   the Deployment scaled to ONE replica; the rollout awaited and the ONE pod required Running + Ready on the pinned digest.
# ACTION=deactivate (CONFIRM=true): the Deployment scaled to zero and its pods awaited gone.
#
# THE LOCK: zerodte-research-migrate-lock (scripts/deploy/zerodte-migrate-barrier.sh, holder-kind writer-activate) — a research migration,
# a rollout of the service and an activation EXCLUDE each other; released on exit once this run's Job is gone or terminal.
#
# Env:
#   ENVIRONMENT      dev | production
#   ACTION           check (default) | activate | deactivate
#   CONFIRM          false (default) | true — REQUIRED true for activate / deactivate
#   PERMITTED_SHA    activate / deactivate: must equal `git rev-parse HEAD` of this checkout (40 lowercase hex)
#   CHECK_RECEIPT    path of the same-build check receipt: written by ACTION=check, REQUIRED by ACTION=activate
#   BUILD_NUMBER     the Jenkins build number recorded in / required of that receipt
#   JOB_TIMEOUT_S    client-side wait for the check Job, default 420 (> the Job's own 300s activeDeadlineSeconds); 330..1800
#   ROLLOUT_TIMEOUT_S  how long to await the scaled Deployment's rollout / drain, default 300; 60..1800
#   KEEP_JOBS        terminal check Jobs to retain, default 5
#   NAMESPACE        default options-edge
set -euo pipefail
cd "$(dirname "$0")/../.."

ENVIRONMENT="${ENVIRONMENT:-}"
ACTION="${ACTION:-check}"
CONFIRM="${CONFIRM:-false}"
PERMITTED_SHA="${PERMITTED_SHA:-}"
CHECK_RECEIPT="${CHECK_RECEIPT:-}"
BUILD_NUMBER="${BUILD_NUMBER:-}"
JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-420}"
ROLLOUT_TIMEOUT_S="${ROLLOUT_TIMEOUT_S:-300}"
KEEP_JOBS="${KEEP_JOBS:-5}"
NAMESPACE="${NAMESPACE:-options-edge}"
CLUSTERS="deploy/zerodte/clusters.yaml"
TEMPLATE="k8s/jobs/zerodte-writer-check-job.yaml"
IMAGE_KEY="vix-option-inteligence-service"
IMAGE_REPO_SUFFIX="/options-edge-vix-option-inteligence"
DEPLOYMENT="zerodte-research-writer"
IDENTITY_CM="zerodte-writer-identity"
JOB_LABEL="app.kubernetes.io/name=zerodte-writer-check"
DEPLOYER="system:serviceaccount:options-edge:jenkins-deployer"
JOB_NAME=""
JOB_OWNED=false
SUCCESS=false

fatal() { echo "FATAL: $*" >&2; exit 1; }

. scripts/deploy/zerodte-migrate-barrier.sh

job_observe() {
  local snap err
  err="$(mktemp)"
  if snap="$(kubectl -n "$NAMESPACE" get "job/$JOB_NAME" -o json 2>"$err")"; then
    rm -f "$err"
    printf '%s' "$snap" | jq -r 'if ((.status.succeeded // 0) >= 1) or (([(.status.conditions // [])[] | select((.type == "Complete" or .type == "Failed") and .status == "True")] | length) >= 1) then "TERMINAL" else "ACTIVE" end' 2>/dev/null || echo UNKNOWN
  else
    if grep -q "NotFound" "$err"; then echo ABSENT; else echo UNKNOWN; fi
    rm -f "$err"
  fi
}

cleanup() {
  local rc=$? observed release=false
  rm -f "${RENDER:-}" "${CM_RENDER:-}" "${LOGS:-}" "${ERRLOG:-}"
  if [ "$JOB_OWNED" != "true" ] || [ -z "$JOB_NAME" ]; then
    release=true
  elif [ "$SUCCESS" = "true" ]; then
    release=true
  else
    observed="$(job_observe)"
    case "$observed" in
      TERMINAL) echo "cleanup: Job $JOB_NAME is terminal — keeping it for post-mortem (script rc=$rc)" >&2; release=true ;;
      ABSENT)   echo "cleanup: Job $JOB_NAME does not exist (script rc=$rc)" >&2; release=true ;;
      *)
        echo "cleanup: deleting Job $JOB_NAME (observed $observed; script exiting rc=$rc without a successful finish)" >&2
        if kubectl -n "$NAMESPACE" delete "job/$JOB_NAME" --cascade=foreground --wait=true --timeout=180s --ignore-not-found >&2; then
          observed="$(job_observe)"
          case "$observed" in ABSENT|TERMINAL) release=true ;; *) echo "cleanup: Job $JOB_NAME still $observed after the delete" >&2 ;; esac
        else
          echo "cleanup: the delete of Job $JOB_NAME failed or timed out" >&2
        fi ;;
    esac
  fi
  if [ "$ZERODTE_MIGRATE_BARRIER_HELD" = true ]; then
    if [ "$release" != "true" ]; then
      echo "cleanup: lock $ZERODTE_MIGRATE_LOCK_NAME RETAINED — Job $JOB_NAME may still be running. Read this run's log, confirm the Job is gone or terminal, then \`kubectl -n $NAMESPACE delete configmap $ZERODTE_MIGRATE_LOCK_NAME\` by hand. Every migration, service rollout and activation refuses until then." >&2
    else
      zerodte_migrate_barrier_release "$NAMESPACE"
    fi
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- 0. parameter validation ------------------------------------------------------------
case "$ENVIRONMENT" in dev|production) : ;; *) fatal "ENVIRONMENT must be dev or production, got '${ENVIRONMENT:-<empty>}'" ;; esac
case "$ACTION" in check|activate|deactivate) : ;; *) fatal "ACTION must be check, activate or deactivate, got '$ACTION'" ;; esac
case "$CONFIRM" in true|false) : ;; *) fatal "CONFIRM must be true or false, got '$CONFIRM'" ;; esac
[ "$ACTION" = check ] || [ "$CONFIRM" = true ] || fatal "ACTION=$ACTION needs CONFIRM=true — a check is the only action a dry build takes"
case "$JOB_TIMEOUT_S" in ''|*[!0-9]*) fatal "JOB_TIMEOUT_S must be digits, got '$JOB_TIMEOUT_S'" ;; esac
[ "$JOB_TIMEOUT_S" -ge 330 ] && [ "$JOB_TIMEOUT_S" -le 1800 ] || fatal "JOB_TIMEOUT_S must be within 330..1800 (the check Job's own deadline is 300s), got $JOB_TIMEOUT_S"
case "$ROLLOUT_TIMEOUT_S" in ''|*[!0-9]*) fatal "ROLLOUT_TIMEOUT_S must be digits, got '$ROLLOUT_TIMEOUT_S'" ;; esac
[ "$ROLLOUT_TIMEOUT_S" -ge 60 ] && [ "$ROLLOUT_TIMEOUT_S" -le 1800 ] || fatal "ROLLOUT_TIMEOUT_S must be within 60..1800, got $ROLLOUT_TIMEOUT_S"
case "$KEEP_JOBS" in ''|*[!0-9]*) fatal "KEEP_JOBS must be digits, got '$KEEP_JOBS'" ;; esac
[ "$KEEP_JOBS" -ge 1 ] && [ "$KEEP_JOBS" -le 100 ] || fatal "KEEP_JOBS must be within 1..100, got $KEEP_JOBS"
RECEIPT_FILE="deploy/zerodte/provisioned/${ENVIRONMENT}.yaml"
DECLARATION="deploy/zerodte/provisioning/${ENVIRONMENT}.yaml"
[ -f "$RECEIPT_FILE" ] || fatal "no committed provisioning receipt at $RECEIPT_FILE — the dedicated writer is activated only against an exact committed receipt (run the provisioning job, commit its printed block through review)"
[ -f "$DECLARATION" ] || fatal "no provisioning declaration at $DECLARATION"
[ -f "$TEMPLATE" ] || fatal "missing Job template $TEMPLATE"
[ -f "$CLUSTERS" ] || fatal "missing the cluster pins $CLUSTERS"
command -v yq >/dev/null 2>&1 || fatal "yq is required"
command -v jq >/dev/null 2>&1 || fatal "jq is required"
command -v python3 >/dev/null 2>&1 || fatal "python3 is required"

# --- 1. the committed receipt: validated and BOUND to its declaration the way the PR validates it, then its identity read ----------------
echo "=== validating $RECEIPT_FILE against $DECLARATION ==="
bash scripts/ci/validate-zerodte-provisioning.sh || fatal "the provisioning files do not pass scripts/ci/validate-zerodte-provisioning.sh — nothing is activated"
IDENTITY="$(python3 - "$RECEIPT_FILE" "$DECLARATION" <<'PY'
import json, sys
sys.path.insert(0, "scripts/ci")
import zerodte_attestation as z
r = z.load(open(sys.argv[1], encoding="utf-8").read())
d = z.parse_provisioning(open(sys.argv[2], encoding="utf-8").read())
print(json.dumps({"symbol": z._text(r["symbol"], "symbol"), "environmentLineageId": z._text(r["environmentLineageId"], "environmentLineageId"), "generation": r["generation"], "eraId": r["eraId"],
                  "clusterId": z._text(r["clusterId"], "clusterId", z.TEXT, quoted=True),
                  "ledgerTopicId": z._text(r["ledgerTopicId"], "ledgerTopicId", z.HEX32, quoted=True), "framesTopic": d["outputs"]["FRAMES"]["topic"]}, sort_keys=True))
PY
)" || fatal "could not read the receipt's identity"
read -r SYMBOL LINEAGE GENERATION ERA_ID CLUSTER_ID LEDGER_TOPIC_ID FRAMES_TOPIC <<EOF2
$(printf '%s' "$IDENTITY" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["symbol"], d["environmentLineageId"], d["generation"], d["eraId"], d["clusterId"], d["ledgerTopicId"], d["framesTopic"])')
EOF2
[ -n "${SYMBOL:-}" ] && [ -n "${GENERATION:-}" ] && [ -n "${ERA_ID:-}" ] && [ -n "${CLUSTER_ID:-}" ] && [ -n "${FRAMES_TOPIC:-}" ] || fatal "could not read symbol / generation / eraId / clusterId / the FRAMES topic from the receipt and the declaration"
case "$FRAMES_TOPIC" in *[!a-zA-Z0-9._-]*|'') fatal "the declaration's FRAMES topic '$FRAMES_TOPIC' is not a topic name" ;; esac
RECEIPT_SHA256="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$RECEIPT_FILE")"
echo "receipt: env=$ENVIRONMENT symbol=$SYMBOL lineage=$LINEAGE generation=$GENERATION eraId=$ERA_ID clusterId=$CLUSTER_ID framesTopic=$FRAMES_TOPIC receipt_sha256=$RECEIPT_SHA256"
echo "mode: ACTION=$ACTION CONFIRM=$CONFIRM"
HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || echo '')"
RECEIPT_LINE="build=${BUILD_NUMBER} env=${ENVIRONMENT} generation=${GENERATION} eraId=${ERA_ID} receipt_sha256=${RECEIPT_SHA256} head=${HEAD_SHA}"
if [ "$ACTION" != check ]; then
  case "$PERMITTED_SHA" in ''|*[!0-9a-f]*) fatal "ACTION=$ACTION needs PERMITTED_SHA, the full 40-character lowercase commit id this effect is permitted for (got '${PERMITTED_SHA:-<empty>}')" ;; esac
  [ "${#PERMITTED_SHA}" -eq 40 ] || fatal "PERMITTED_SHA '$PERMITTED_SHA' has ${#PERMITTED_SHA} characters, not 40"
  [ -n "$HEAD_SHA" ] || fatal "cannot resolve HEAD of this checkout — refusing to act without knowing what is checked out"
  [ "$HEAD_SHA" = "$PERMITTED_SHA" ] || fatal "checked-out HEAD $HEAD_SHA is not the permitted commit $PERMITTED_SHA — nothing may be changed under it"
  echo "permitted commit re-checked before the effect: HEAD $HEAD_SHA == PERMITTED_SHA"
fi
if [ "$ACTION" = activate ]; then
  [ -n "$CHECK_RECEIPT" ] || fatal "ACTION=activate needs CHECK_RECEIPT, the receipt file this build's check wrote"
  [ -f "$CHECK_RECEIPT" ] || fatal "no check receipt at $CHECK_RECEIPT — the check of THIS build has not passed, so nothing may be activated"
  case "$BUILD_NUMBER" in ''|*[!0-9]*) fatal "ACTION=activate needs BUILD_NUMBER (digits) to bind the receipt to this build, got '${BUILD_NUMBER:-<empty>}'" ;; esac
  GOT="$(head -1 "$CHECK_RECEIPT")"
  [ "$GOT" = "$RECEIPT_LINE" ] || fatal "the check receipt does not describe this activation.
       receipt: $GOT
       effect:  $RECEIPT_LINE
       The check that passed was for another build, environment, receipt or commit. Re-run the whole build."
  echo "check receipt of this build accepted: $GOT"
fi

# --- 2. identity AND cluster (the cluster is its CA — deploy/zerodte/clusters.yaml) ------------
WHOAMI="$(kubectl auth whoami -o jsonpath='{.status.userInfo.username}' 2>/dev/null || echo '')"
echo "kubectl identity: ${WHOAMI:-<unknown>}"
[ "$WHOAMI" = "$DEPLOYER" ] || fatal "kubeconfig identity is '${WHOAMI:-<unknown>}', expected '$DEPLOYER'."
API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || echo '')"
SKIP_TLS="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.insecure-skip-tls-verify}' 2>/dev/null || echo '')"
CA_FILE="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority}' 2>/dev/null || echo '')"
[ "$SKIP_TLS" != "true" ] || fatal "the kubeconfig sets insecure-skip-tls-verify: true — refusing"
[ -z "$CA_FILE" ] || fatal "the kubeconfig names its CA by file path ($CA_FILE); only inline certificate-authority-data is pinned — refusing"
CA_FINGERPRINT="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' 2>/dev/null | base64 -d 2>/dev/null | openssl x509 -noout -fingerprint -sha256 2>/dev/null | sed 's/^.*=//; s/://g' || echo '')"
PIN_CA="$(yq -r ".clusters.\"${ENVIRONMENT}\".caSha256" "$CLUSTERS" 2>/dev/null || echo '')"
PIN_SERVER="$(yq -r ".clusters.\"${ENVIRONMENT}\".apiServer" "$CLUSTERS" 2>/dev/null || echo '')"
case "$PIN_CA" in ''|null) fatal "$CLUSTERS pins no CA for $ENVIRONMENT" ;; *[!0-9A-F]*) fatal "$CLUSTERS caSha256 for $ENVIRONMENT is not upper-case hex" ;; esac
[ "${#PIN_CA}" -eq 64 ] || fatal "$CLUSTERS caSha256 for $ENVIRONMENT is not 64 hex characters"
echo "api server: ${API_SERVER:-<unknown>}; kubeconfig CA sha256: ${CA_FINGERPRINT:-<none>}"
[ -n "$CA_FINGERPRINT" ] || fatal "the kubeconfig carries no certificate-authority-data to pin the cluster by — refusing"
[ "$CA_FINGERPRINT" = "$PIN_CA" ] || fatal "the kubeconfig's CA fingerprint $CA_FINGERPRINT is not the pinned $ENVIRONMENT cluster's ($PIN_CA in $CLUSTERS)"
if [ -n "$PIN_SERVER" ] && [ "$PIN_SERVER" != "null" ]; then
  [ "$API_SERVER" = "$PIN_SERVER" ] || fatal "kubeconfig points at '${API_SERVER:-<unknown>}', the pinned $ENVIRONMENT API server is '$PIN_SERVER' ($CLUSTERS)"
fi

# --- 3. resolve + digest-pin the SERVICE image; the live Deployment must ALREADY run it ------------------------------------------
MUTABLE_IMAGE="$(yq -er ".images.\"${IMAGE_KEY}\"" "image-tags/${ENVIRONMENT}.yaml" 2>/dev/null || true)"
[ -n "$MUTABLE_IMAGE" ] && [ "$MUTABLE_IMAGE" != "null" ] || fatal "image-tags/${ENVIRONMENT}.yaml has no '${IMAGE_KEY}' entry"
case "$MUTABLE_IMAGE" in *"${IMAGE_REPO_SUFFIX}":*) : ;; *) fatal "image-tags/${ENVIRONMENT}.yaml '${IMAGE_KEY}' is '$MUTABLE_IMAGE', which is not a ${IMAGE_REPO_SUFFIX} image" ;; esac
export DEPLOY_PLATFORM="linux/amd64"
export REGISTRY_SCHEME="http"
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" || fatal "cannot resolve registry digest for $MUTABLE_IMAGE (is the service image built + pushed for $ENVIRONMENT?)"
case "$PINNED_IMAGE" in *@sha256:*) : ;; *) fatal "refusing to run on an unpinned image ref: $PINNED_IMAGE" ;; esac
PINNED_DIGEST="${PINNED_IMAGE##*@}"
echo "image: $MUTABLE_IMAGE -> $PINNED_IMAGE"
LIVE_IMAGE="$(kubectl -n "$NAMESPACE" get "deployment/$DEPLOYMENT" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null)" || fatal "the Deployment $DEPLOYMENT does not exist in $NAMESPACE — deploy the zerodte-research-writer service slice first (Jenkinsfile.service-deploy); it ships at replicas 0"
case "$LIVE_IMAGE" in *"@${PINNED_DIGEST}") : ;; *) fatal "the live Deployment $DEPLOYMENT runs '$LIVE_IMAGE', not the pinned digest $PINNED_DIGEST this run would check and activate — deploy the slice at this image first" ;; esac
LIVE_REPLICAS="$(kubectl -n "$NAMESPACE" get "deployment/$DEPLOYMENT" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo '')"
echo "live Deployment $DEPLOYMENT: image $LIVE_IMAGE, replicas ${LIVE_REPLICAS:-<unknown>}"

RENDER="$(mktemp)"; CM_RENDER="$(mktemp)"; LOGS="$(mktemp)"; ERRLOG="$(mktemp)"

# --- 4. the LOCK (the deployment / migration / activation exclusion), then the inventory ---------------------------------------
zerodte_migrate_barrier_acquire "$NAMESPACE" "$DEPLOYMENT" "writer-activate-${ACTION}-${ENVIRONMENT}-build-${BUILD_NUMBER:-manual}-$(date -u +%Y%m%dT%H%M%SZ)" writer-activate || fatal "the exclusion lock could not be taken — a research migration, a service rollout or another activation holds it; wait for it, or read its log before removing a stale lock by hand"
JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json)" || fatal "cannot list zerodte-writer-check Jobs in $NAMESPACE — refusing beside an inventory that could not be read"
ACTIVE="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1 or ([(.status.conditions // [])[] | select((.type == "Failed" or .type == "Complete") and .status == "True")] | length) >= 1) | not) | .metadata.name] | join(",")' 2>/dev/null)" \
  || fatal "cannot parse the zerodte-writer-check Job list"
[ -z "$ACTIVE" ] || fatal "another zerodte-writer-check Job is not terminal ($ACTIVE). Wait for it, or delete it if it is a leftover."

# --- 5. the identity ConfigMap from the receipt (rendered always; APPLIED on activate only) ------------------------------------
cat > "$CM_RENDER" <<EOF2
apiVersion: v1
kind: ConfigMap
metadata:
  name: $IDENTITY_CM
  namespace: $NAMESPACE
  labels:
    app.kubernetes.io/name: zerodte-writer-identity
    app.kubernetes.io/part-of: options-edge
    options-edge.io/zerodte-environment: "$ENVIRONMENT"
  annotations:
    options-edge.io/receipt-sha256: "$RECEIPT_SHA256"
data:
  environment: "$ENVIRONMENT"
  symbol: "$SYMBOL"
  environmentLineageId: "$LINEAGE"
  generation: "$GENERATION"
  provisioningGeneration: "$GENERATION"
  eraId: "$ERA_ID"
  clusterId: "$CLUSTER_ID"
  ledgerTopicId: "$LEDGER_TOPIC_ID"
EOF2
kubectl -n "$NAMESPACE" apply --dry-run=server -f "$CM_RENDER" >/dev/null || fatal "the identity ConfigMap render does not validate server-side"

run_check_job() { # the CHECK Job: render, validate, create, wait, log, the receipt held to the letter and BOUND to the committed receipt
  JOB_NAME="zerodte-writer-check-$(date -u +%Y%m%d-%H%M%S)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
  sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" -e "s|__JOB_NAME__|${JOB_NAME}|g" -e "s|__ENVIRONMENT__|${ENVIRONMENT}|g" -e "s|__SYMBOL__|${SYMBOL}|g" -e "s|__FRAMES_TOPIC__|${FRAMES_TOPIC}|g" \
      -e "s|__ERA_ID__|${ERA_ID}|g" -e "s|__GENERATION__|${GENERATION}|g" "$TEMPLATE" >"$RENDER"
  grep -q '__[A-Z_]*__' "$RENDER" && fatal "unsubstituted placeholder left in the render"
  [ "$(yq -r '.metadata.namespace' "$RENDER")" = "$NAMESPACE" ] || fatal "template namespace != NAMESPACE '$NAMESPACE'"
  grep -q "POSTGRES_PASSWORD" "$RENDER" && ! grep -qE "POSTGRES_PASSWORD[[:space:]]*[:=][[:space:]]*[^[:space:]]" "$RENDER" || fatal "the render must reference POSTGRES_PASSWORD by secretKeyRef only"
  echo "=== server-side validate (Job $JOB_NAME) ==="
  kubectl -n "$NAMESPACE" create --dry-run=server -f "$RENDER" >/dev/null
  echo "=== creating check Job $JOB_NAME (env=$ENVIRONMENT eraId=$ERA_ID generation=$GENERATION) ==="
  JOB_OWNED=true
  kubectl -n "$NAMESPACE" create -f "$RENDER"
  local deadline state
  deadline=$(( $(date +%s) + JOB_TIMEOUT_S ))
  state="active"
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$(job_observe)"
    case "$state" in TERMINAL|ABSENT|UNKNOWN) break ;; esac
    sleep 5
  done
  [ "$state" = TERMINAL ] || { [ "$state" = ACTIVE ] && fatal "$JOB_NAME is still active after ${JOB_TIMEOUT_S}s — the client stopped waiting; the lock is retained until the Job is gone or terminal"; fatal "the state of $JOB_NAME could not be read ($state) — refusing to judge a run whose Job cannot be observed"; }
  local succeeded
  succeeded="$(kubectl -n "$NAMESPACE" get "job/$JOB_NAME" -o json 2>/dev/null | jq -r 'if ((.status.succeeded // 0) >= 1) then "succeeded" else "failed" end' 2>/dev/null || echo unreadable)"
  echo "job state: $succeeded"
  local read_ok=false try
  for try in 1 2 3 4 5; do
    if kubectl -n "$NAMESPACE" logs "job/$JOB_NAME" --tail=-1 >"$LOGS" 2>/dev/null; then read_ok=true; break; fi
    echo "logs for $JOB_NAME not available yet (attempt $try) ..."; : >"$LOGS"; sleep 6
  done
  [ "$read_ok" = true ] || fatal "the log of $JOB_NAME could not be read in 5 attempts — without the log there is no receipt; refusing to guess"
  echo "===== $JOB_NAME log ====="; cat "$LOGS"; echo "===== end log ====="
  EXIT_CODE="$(kubectl -n "$NAMESPACE" get pods -l "job-name=$JOB_NAME" -o json 2>/dev/null | jq -r '[.items[] | (.status.containerStatuses // [])[] | select(.name == "writer-check") | .state.terminated.exitCode // empty] | first // ""' 2>/dev/null || echo '')"
  echo "writer-check exit code: ${EXIT_CODE:-<unknown>}"
  if [ "$succeeded" != succeeded ] && grep -q 'Could not find or load main class' "$LOGS"; then
    fatal "$PINNED_IMAGE does not carry ZeroDteResearchWriterMain: the ${IMAGE_KEY} tag holds a build older than options-edge-processing increment 9d. Build and push the service image first."
  fi
  local outcomes n
  outcomes="$(grep -E '^WRITER_CHECK ' "$LOGS" || true)"
  n="$(printf '%s\n' "$outcomes" | grep -c . || true)"
  [ "${n:-0}" = 1 ] || fatal "$JOB_NAME printed ${n:-0} receipt line(s) — expected exactly one (state=$succeeded, exit ${EXIT_CODE:-unknown}). Read the log."
  CHECK_LINE="$outcomes"
  local HEX32='[0-9a-f]{32}' NUM='(0|[1-9][0-9]*)'
  if printf '%s\n' "$CHECK_LINE" | grep -Eq -- "^WRITER_CHECK READY eraId=$NUM generation=$NUM clusterId=[A-Za-z0-9._-]+ framesTopicId=$HEX32 position=$NUM cursorOffset=($NUM|none)\$"; then
    [ "${EXIT_CODE:-}" = 0 ] && [ "$succeeded" = succeeded ] || fatal "the receipt says READY but the container exited '${EXIT_CODE:-<unknown>}' (state=$succeeded) — the receipt and the process disagree; refused"
    local r_era r_gen r_cluster
    r_era="$(printf '%s' "$CHECK_LINE" | sed -E 's/.* eraId=([0-9]+) .*/\1/')"; r_gen="$(printf '%s' "$CHECK_LINE" | sed -E 's/.* generation=([0-9]+) .*/\1/')"; r_cluster="$(printf '%s' "$CHECK_LINE" | sed -E 's/.* clusterId=([A-Za-z0-9._-]+) .*/\1/')"
    [ "$r_era" = "$ERA_ID" ] && [ "$r_gen" = "$GENERATION" ] || fatal "the writer verified era $r_era of generation $r_gen, the committed receipt names era $ERA_ID of generation $GENERATION — whatever the writer checked, it was not this receipt's era ('$CHECK_LINE')"
    [ "$r_cluster" = "$CLUSTER_ID" ] || fatal "the writer runs against cluster '$r_cluster', the committed receipt names '$CLUSTER_ID' — not the provisioned cluster ('$CHECK_LINE')"
    echo "$CHECK_LINE"
    echo "OK: WRITER_CHECK READY — schema v7, one frames partition, era $ERA_ID of generation $GENERATION verified on cluster $CLUSTER_ID, the calendar's reach, the cursor recovered (position $(printf '%s' "$CHECK_LINE" | sed -E 's/.* position=([0-9]+) .*/\1/'), cursor $(printf '%s' "$CHECK_LINE" | sed -E 's/.* cursorOffset=([0-9]+|none)$/\1/')); nothing consumed, nothing written."
    return 0
  fi
  if printf '%s\n' "$CHECK_LINE" | grep -Eq -- '^WRITER_CHECK REFUSED code=[A-Z_]+ exit=68$'; then
    [ "${EXIT_CODE:-}" = 68 ] || fatal "the receipt says REFUSED (68) but the container exited '${EXIT_CODE:-<unknown>}' — refused"
    local code; code="$(printf '%s' "$CHECK_LINE" | sed -E 's/.* code=([A-Z_]+) .*/\1/')"
    case "$code" in
      RESEARCH_SCHEMA_VERSION) fatal "the writer REFUSED: the research store is not at schema v7 — run Jenkinsfile.zerodte-research-migrate (dry run, then CONFIRM) first" ;;
      RESEARCH_ERA_MISSING) fatal "the writer REFUSED: the era bound to the live frames topic is not era $ERA_ID of generation $GENERATION — run Jenkinsfile.zerodte-provision (dry run, then CONFIRM) and commit ITS receipt; a stale committed receipt names another provisioning" ;;
      RESEARCH_TOPIC_INVALID) fatal "the writer REFUSED: the frames topic has not exactly one partition or its ids could not be read — the provisioning declaration pins one partition; inspect the topic" ;;
      RESEARCH_CALENDAR_COVERAGE) fatal "the writer REFUSED: the loaded calendar does not reach far enough — a new reviewed migration ships the calendar" ;;
      RESEARCH_OFFSET_LOST) fatal "the writer REFUSED: the store's cursor or newest feature row has no matching retained frame — an inconsistent store and log; nothing is activated until this is explained (see the log)" ;;
      *) fatal "the writer REFUSED with code $code — see the log; nothing is activated" ;;
    esac
  fi
  if printf '%s\n' "$CHECK_LINE" | grep -Eq -- '^WRITER_CHECK UNAVAILABLE exit=69$'; then fatal "the writer could not reach the store or the broker (UNAVAILABLE, 69) — see the log; re-run once reachable"; fi
  if printf '%s\n' "$CHECK_LINE" | grep -Eq -- '^WRITER_CHECK USAGE exit=64$'; then fatal "the writer refused its invocation (USAGE, 64) — the Job template and the main class disagree; see the log"; fi
  fatal "receipt '$CHECK_LINE' is not a WRITER_CHECK line in its canonical grammar — refused"
}

case "$ACTION" in
  check)
    run_check_job
    if [ -n "$CHECK_RECEIPT" ]; then
      printf '%s\n' "$RECEIPT_LINE" > "$CHECK_RECEIPT" || fatal "could not write the check receipt to $CHECK_RECEIPT"
      echo "check receipt written: $RECEIPT_LINE"
    fi ;;
  activate)
    run_check_job
    echo "=== applying the identity ConfigMap $IDENTITY_CM from the committed receipt and scaling $DEPLOYMENT to 1 ==="
    kubectl -n "$NAMESPACE" apply -f "$CM_RENDER"
    kubectl -n "$NAMESPACE" scale "deployment/$DEPLOYMENT" --replicas=1
    kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout="${ROLLOUT_TIMEOUT_S}s" || fatal "the rollout of $DEPLOYMENT did not complete within ${ROLLOUT_TIMEOUT_S}s — the writer's readiness probe is failing (read its /health/ready and /metrics); the Deployment stays at 1 replica NOT READY for the operator to inspect, or scale it to 0 with ACTION=deactivate"
    PODS="$(kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=$DEPLOYMENT" --field-selector=status.phase=Running -o json 2>/dev/null)" || fatal "cannot list the writer's pods after the rollout"
    READY_ON_DIGEST="$(printf '%s' "$PODS" | jq -r --arg d "$PINNED_DIGEST" '[.items[] | select(.metadata.deletionTimestamp == null) | select(([(.status.conditions // [])[] | select(.type == "Ready" and .status == "True")] | length) >= 1) | select(([(.status.containerStatuses // [])[] | .imageID] | map(test("@" + $d + "$")) | (length > 0 and all)))] | length')"
    TOTAL="$(printf '%s' "$PODS" | jq -r '[.items[] | select(.metadata.deletionTimestamp == null)] | length')"
    [ "$READY_ON_DIGEST" = 1 ] && [ "$TOTAL" = 1 ] || fatal "after the rollout $DEPLOYMENT has $TOTAL running pod(s) of which $READY_ON_DIGEST ready on the pinned digest — expected exactly one"
    echo "OK: ACTIVATED the dedicated v7 research writer on $ENVIRONMENT — one replica Running and Ready on $PINNED_DIGEST, era $ERA_ID of generation $GENERATION (identity ConfigMap $IDENTITY_CM, receipt sha256 $RECEIPT_SHA256). Its lag, cursor and primary-code metrics are on :8080/metrics." ;;
  deactivate)
    echo "=== scaling $DEPLOYMENT to 0 ==="
    kubectl -n "$NAMESPACE" scale "deployment/$DEPLOYMENT" --replicas=0
    deadline=$(( $(date +%s) + ROLLOUT_TIMEOUT_S ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
      left="$(kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=$DEPLOYMENT" -o json 2>/dev/null | jq -r '.items | length' 2>/dev/null || echo unknown)"
      [ "$left" = 0 ] && break
      sleep 5
    done
    [ "${left:-unknown}" = 0 ] || fatal "pods of $DEPLOYMENT are still present after ${ROLLOUT_TIMEOUT_S}s (${left:-unknown}) — the Deployment is at 0; wait for them to go, then inspect"
    echo "OK: DEACTIVATED the dedicated v7 research writer on $ENVIRONMENT — the Deployment is at 0 replicas and its pods are gone." ;;
esac
SUCCESS=true

# --- 6. prune old TERMINAL check Jobs (best-effort) -----------------------------------------------------------------------------
if JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null)"; then
  TERMINAL="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1) or ([(.status.conditions // [])[] | select(.type == "Failed" and .status == "True")] | length) >= 1)] | sort_by(.metadata.creationTimestamp) | .[].metadata.name' 2>/dev/null)"
  COUNT="$(printf '%s\n' "$TERMINAL" | grep -c . || true)"
  if [ "${COUNT:-0}" -gt "$KEEP_JOBS" ]; then
    DROP=$(( COUNT - KEEP_JOBS )); i=0
    while IFS= read -r old; do
      [ -n "$old" ] || continue
      i=$(( i + 1 )); [ "$i" -le "$DROP" ] || break
      echo "pruning old zerodte-writer-check Job $old"
      kubectl -n "$NAMESPACE" delete "job/$old" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done <<EOF2
$TERMINAL
EOF2
  else
    echo "no old zerodte-writer-check Jobs to prune (terminal=${COUNT:-0}, keep=$KEEP_JOBS)"
  fi
fi
