#!/usr/bin/env bash
# Migrate the 0DTE research store of ONE environment from schema v6 to v7 — the operator side of k8s/jobs/zerodte-research-migrate-job.yaml
# (read that file's header first). Increment 9c (design "BUILD decisions for increment 9", Q2 / Q3 / Q9); the pattern is
# scripts/ops/zerodte-provision.sh (increment 7b), whose guarantees this script repeats: the cluster pinned by its CA, one invocation at a
# time through an atomic lock, the receipt held to the letter, the same-build dry-run receipt binding the confirm, HEAD == PERMITTED_SHA
# re-checked before the effect.
#
# The migrator is the service image's ZeroDteResearchMigrator (options-edge-processing increment 9a): ONE transaction under the research
# advisory lock — the typed v6 archive, the v7 DDL and views, the shipped calendar, the migration record — executed and ROLLED BACK on a dry
# run (CONFIRM=false, the stage that ALWAYS runs: the receipt's counts are the real rows'), COMMITTED and verified on CONFIRM=true. At v7 it
# verifies and prints ALREADY_MIGRATED; nothing is ever re-applied.
#
# THE RECEIPT. The migrator prints exactly ONE outcome line on stdout (diagnostics on stderr; neither carries a credential or a backend's text):
#   MIGRATABLE fromVersion=6 toVersion=7 calendarVersion= expectedSessions= archiveFeatureFamilies= nullQualityFlags= nullSurfaceStatus= nullSurfaceActionable= scheduledBoundaries= lateStartBoundaries= calibratedShadowRows= planDigest=
#   MIGRATED   fromVersion=6 toVersion=7 calendarVersion= expectedSessions= archiveFeatureFamilies= … calibratedShadowRows= schemaDigest=
#   ALREADY_MIGRATED version=7 calendarVersion= expectedSessions= schemaDigest=
#   REFUSED reason=<TOKEN> exit=<n>     (64 usage, 65 the shipped calendar not the rules, 68 precondition, 69 unavailable / lock, 70 mutation)
# This script requires exactly one such line, parses its fields as whole tokens with an EXACT field set per outcome, judges every field by
# its domain, requires the container's exit code to AGREE with the outcome, and binds calendarVersion / fromVersion / toVersion to the
# reviewed declaration this run migrates under (read by the same validator that gated the PR). The outcome must be one the mode allows
# (dry run: MIGRATABLE or ALREADY_MIGRATED; confirm: MIGRATED or ALREADY_MIGRATED). Anything else is a refusal, never a success.
#
# THE QUIESCENCE ATTESTATION (inc 9 consult Q3): the legacy (v6) in-process research writer must be GONE before the migration — the V1
# compatibility image (increment 9a) rolled to every vix-option-inteligence pod with ZERODTE_RESEARCH_ENABLED != true. This script verifies
# it on the live cluster: every pod of that service must run the digest-pinned image this Job runs AND carry no ZERODTE_RESEARCH_ENABLED=true
# in its container env; a pod of another image, a pod with the flag on, an unreadable pod list — each is a refusal. Only then does it render
# --legacy-writers-quiesced into the Job (the migrator refuses without it).
#
# WHAT IT DOES, fail-closed at every step:
#   0. validates every parameter;
#   1. validates the declaration with scripts/ci/validate-zerodte-research-migration.sh and reads its identity;
#   2. asserts the kubectl identity IS the deployer SA AND the kubeconfig's CA is THIS environment's pinned cluster (deploy/zerodte/clusters.yaml);
#   3. resolves the env's vix-option-inteligence SERVICE image by EXACT key (image-tags/<env>.yaml) and digest-pins it;
#   4. verifies the legacy writers are quiesced (every service pod on the pinned image, the flag off);
#   5. takes the lock (zerodte-research-migrate-lock, atomic create), refuses to start while another migration Job is active;
#   6. renders the ConfigMap (the declaration, named by its sha256) and the Job, validates both server-side, creates them, waits, prints the
#      whole pod log, maps the container's exit code, and requires the receipt;
#   7. prunes old terminal Jobs and the ConfigMaps no remaining Job references (best-effort).
#
# Env:
#   ENVIRONMENT      dev | production
#   CONFIRM          false (dry run, default) | true (migrate)
#   PERMITTED_SHA    CONFIRM=true only: must equal `git rev-parse HEAD` of this checkout (40 lowercase hex)
#   DRY_RUN_RECEIPT  path of the same-build dry-run receipt: written on CONFIRM=false, REQUIRED on CONFIRM=true
#   BUILD_NUMBER     the Jenkins build number recorded in / required of that receipt
#   JOB_TIMEOUT_S    client-side wait, default 1200 (> the Job's own 900s activeDeadlineSeconds); 960..3600
#   KEEP_JOBS        terminal Jobs to retain, default 5
#   NAMESPACE        default options-edge
set -euo pipefail
cd "$(dirname "$0")/../.."

ENVIRONMENT="${ENVIRONMENT:-}"
CONFIRM="${CONFIRM:-false}"
PERMITTED_SHA="${PERMITTED_SHA:-}"
DRY_RUN_RECEIPT="${DRY_RUN_RECEIPT:-}"
BUILD_NUMBER="${BUILD_NUMBER:-}"
JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-1200}"
KEEP_JOBS="${KEEP_JOBS:-5}"
NAMESPACE="${NAMESPACE:-options-edge}"
CLUSTERS="deploy/zerodte/clusters.yaml"
LOCK_NAME="zerodte-research-migrate-lock"

TEMPLATE="k8s/jobs/zerodte-research-migrate-job.yaml"
IMAGE_KEY="vix-option-inteligence-service"
IMAGE_REPO_SUFFIX="/options-edge-vix-option-inteligence"
SERVICE_POD_LABEL="app.kubernetes.io/name=vix-option-inteligence-service"
JOB_LABEL="app.kubernetes.io/name=zerodte-research-migrate"
CM_LABEL="app.kubernetes.io/name=zerodte-research-migrate-files"
DEPLOYER="system:serviceaccount:options-edge:jenkins-deployer"
JOB_NAME=""
JOB_OWNED=false
LOCK_OWNED=false
SUCCESS=false

fatal() { echo "FATAL: $*" >&2; exit 1; }

# job_observe → TERMINAL | ABSENT | ACTIVE | UNKNOWN (the Job's state as the API reports it now; UNKNOWN = the API could not be read)
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
  # The LOCK's lifetime (as zerodte-provision.sh): released by its OWNER only, and only once no Job of this run can still be running.
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
  if [ "$LOCK_OWNED" = "true" ]; then
    if [ "$release" != "true" ]; then
      echo "cleanup: lock $LOCK_NAME RETAINED — Job $JOB_NAME may still be running. Read this run's log, confirm the Job is gone or terminal, then \`kubectl -n $NAMESPACE delete configmap $LOCK_NAME\` by hand. The next migration refuses until then." >&2
    elif kubectl -n "$NAMESPACE" delete "configmap/$LOCK_NAME" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1; then
      echo "lock $LOCK_NAME released" >&2
    else
      echo "cleanup: could not release lock $LOCK_NAME (held by this run) — remove it by hand after reading this run's log" >&2
    fi
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- 0. parameter validation ------------------------------------------------------------
case "$ENVIRONMENT" in dev|production) : ;; *) fatal "ENVIRONMENT must be dev or production, got '${ENVIRONMENT:-<empty>}'" ;; esac
case "$CONFIRM" in true|false) : ;; *) fatal "CONFIRM must be true or false, got '$CONFIRM'" ;; esac
case "$JOB_TIMEOUT_S" in ''|*[!0-9]*) fatal "JOB_TIMEOUT_S must be digits, got '$JOB_TIMEOUT_S'" ;; esac
case "$KEEP_JOBS" in ''|*[!0-9]*) fatal "KEEP_JOBS must be digits, got '$KEEP_JOBS'" ;; esac
[ "$JOB_TIMEOUT_S" -ge 960 ] && [ "$JOB_TIMEOUT_S" -le 3600 ] || fatal "JOB_TIMEOUT_S must be within 960..3600 (the Job's own deadline is 900s), got $JOB_TIMEOUT_S"
[ "$KEEP_JOBS" -ge 1 ] && [ "$KEEP_JOBS" -le 100 ] || fatal "KEEP_JOBS must be within 1..100, got $KEEP_JOBS"
FILE="deploy/zerodte/research-migration/${ENVIRONMENT}.yaml"
[ -f "$FILE" ] || fatal "no migration declaration for $ENVIRONMENT at $FILE"
[ -f "$TEMPLATE" ] || fatal "missing Job template $TEMPLATE"
[ -f "$CLUSTERS" ] || fatal "missing the cluster pins $CLUSTERS"
command -v yq >/dev/null 2>&1 || fatal "yq is required"
command -v jq >/dev/null 2>&1 || fatal "jq is required"
command -v python3 >/dev/null 2>&1 || fatal "python3 is required"
FILE_BASENAME="$(basename "$FILE")"

# --- 1. the declaration: validated the way the PR validates it, then its identity read -----------------------------
echo "=== validating $FILE ==="
bash scripts/ci/validate-zerodte-research-migration.sh || fatal "the declaration does not pass scripts/ci/validate-zerodte-research-migration.sh — nothing is migrated"
IDENTITY="$(python3 scripts/ci/zerodte_attestation.py migration "$FILE")" || fatal "could not read the declaration's identity"
read -r FROM_VERSION TO_VERSION CALENDAR_VERSION SESSIONS_AHEAD <<EOF2
$(printf '%s' "$IDENTITY" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["fromVersion"], d["toVersion"], d["calendarVersion"], d["expectedSessionsAhead"])')
EOF2
[ "${FROM_VERSION:-}" = 6 ] && [ "${TO_VERSION:-}" = 7 ] && [ -n "${CALENDAR_VERSION:-}" ] && [ -n "${SESSIONS_AHEAD:-}" ] || fatal "could not read fromVersion / toVersion / calendarVersion / expectedSessionsAhead from $FILE"
FILE_SHA256="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$FILE")"
echo "declaration: env=$ENVIRONMENT from=$FROM_VERSION to=$TO_VERSION calendarVersion=$CALENDAR_VERSION sessionsAhead=$SESSIONS_AHEAD file_sha256=$FILE_SHA256"
echo "mode: $([ "$CONFIRM" = true ] && echo 'CONFIRM=true — the migrator COMMITS (archive, v7 DDL, calendar, version 7)' || echo 'CONFIRM=false — DRY RUN, the migrator executes everything and ROLLS BACK')"
HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || echo '')"
RECEIPT_LINE="build=${BUILD_NUMBER} env=${ENVIRONMENT} from=${FROM_VERSION} to=${TO_VERSION} calendarVersion=${CALENDAR_VERSION} file_sha256=${FILE_SHA256} head=${HEAD_SHA}"
if [ "$CONFIRM" = true ]; then
  case "$PERMITTED_SHA" in ''|*[!0-9a-f]*) fatal "CONFIRM=true needs PERMITTED_SHA, the full 40-character lowercase commit id this write is permitted for (got '${PERMITTED_SHA:-<empty>}')" ;; esac
  [ "${#PERMITTED_SHA}" -eq 40 ] || fatal "PERMITTED_SHA '$PERMITTED_SHA' has ${#PERMITTED_SHA} characters, not 40"
  [ -n "$HEAD_SHA" ] || fatal "cannot resolve HEAD of this checkout — refusing to write without knowing what is checked out"
  [ "$HEAD_SHA" = "$PERMITTED_SHA" ] || fatal "checked-out HEAD $HEAD_SHA is not the permitted commit $PERMITTED_SHA — nothing may be written under it"
  echo "permitted commit re-checked before the write: HEAD $HEAD_SHA == PERMITTED_SHA"
  [ -n "$DRY_RUN_RECEIPT" ] || fatal "CONFIRM=true needs DRY_RUN_RECEIPT, the receipt file this build's dry run wrote"
  [ -f "$DRY_RUN_RECEIPT" ] || fatal "no dry-run receipt at $DRY_RUN_RECEIPT — the dry run of THIS build has not passed, so nothing may be written"
  case "$BUILD_NUMBER" in ''|*[!0-9]*) fatal "CONFIRM=true needs BUILD_NUMBER (digits) to bind the receipt to this build, got '${BUILD_NUMBER:-<empty>}'" ;; esac
  GOT_RECEIPT="$(head -1 "$DRY_RUN_RECEIPT")"
  [ "$GOT_RECEIPT" = "$RECEIPT_LINE" ] || fatal "the dry-run receipt does not describe this write.
       receipt: $GOT_RECEIPT
       write:   $RECEIPT_LINE
       The dry run that passed was for another build, environment, declaration or commit. Re-run the whole build."
  echo "dry-run receipt of this build accepted: $GOT_RECEIPT"
fi

# --- 2. identity AND cluster (the cluster is its CA — deploy/zerodte/clusters.yaml; as zerodte-provision.sh) ------------
WHOAMI="$(kubectl auth whoami -o jsonpath='{.status.userInfo.username}' 2>/dev/null || echo '')"
echo "kubectl identity: ${WHOAMI:-<unknown>}"
[ "$WHOAMI" = "$DEPLOYER" ] || fatal "kubeconfig identity is '${WHOAMI:-<unknown>}', expected '$DEPLOYER'. Job and ConfigMap creation is denied for every other principal by the options-edge-jenkins-only-workloads admission policy."
API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || echo '')"
SKIP_TLS="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.insecure-skip-tls-verify}' 2>/dev/null || echo '')"
CA_FILE="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority}' 2>/dev/null || echo '')"
[ "$SKIP_TLS" != "true" ] || fatal "the kubeconfig sets insecure-skip-tls-verify: true — kubectl would accept ANY API server regardless of the pinned CA; refusing"
[ -z "$CA_FILE" ] || fatal "the kubeconfig names its CA by file path ($CA_FILE); only inline certificate-authority-data is pinned — refusing"
CA_FINGERPRINT="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' 2>/dev/null | base64 -d 2>/dev/null | openssl x509 -noout -fingerprint -sha256 2>/dev/null | sed 's/^.*=//; s/://g' || echo '')"
PIN_CA="$(yq -r ".clusters.\"${ENVIRONMENT}\".caSha256" "$CLUSTERS" 2>/dev/null || echo '')"
PIN_SERVER="$(yq -r ".clusters.\"${ENVIRONMENT}\".apiServer" "$CLUSTERS" 2>/dev/null || echo '')"
case "$PIN_CA" in ''|null) fatal "$CLUSTERS pins no CA for $ENVIRONMENT" ;; *[!0-9A-F]*) fatal "$CLUSTERS caSha256 for $ENVIRONMENT is not upper-case hex" ;; esac
[ "${#PIN_CA}" -eq 64 ] || fatal "$CLUSTERS caSha256 for $ENVIRONMENT is not 64 hex characters"
echo "api server: ${API_SERVER:-<unknown>}; kubeconfig CA sha256: ${CA_FINGERPRINT:-<none>}"
[ -n "$CA_FINGERPRINT" ] || fatal "the kubeconfig carries no certificate-authority-data to pin the cluster by — refusing (a cluster is identified by its CA, not by a name)"
[ "$CA_FINGERPRINT" = "$PIN_CA" ] || fatal "the kubeconfig's CA fingerprint $CA_FINGERPRINT is not the pinned $ENVIRONMENT cluster's ($PIN_CA in $CLUSTERS). The deployer service-account name and a cluster NAME do not identify a cluster; its CA does."
if [ -n "$PIN_SERVER" ] && [ "$PIN_SERVER" != "null" ]; then
  [ "$API_SERVER" = "$PIN_SERVER" ] || fatal "kubeconfig points at '${API_SERVER:-<unknown>}', the pinned $ENVIRONMENT API server is '$PIN_SERVER' ($CLUSTERS)"
fi

# --- 3. resolve + digest-pin the SERVICE image ------------------------------------------------
MUTABLE_IMAGE="$(yq -er ".images.\"${IMAGE_KEY}\"" "image-tags/${ENVIRONMENT}.yaml" 2>/dev/null || true)"
[ -n "$MUTABLE_IMAGE" ] && [ "$MUTABLE_IMAGE" != "null" ] || fatal "image-tags/${ENVIRONMENT}.yaml has no '${IMAGE_KEY}' entry"
case "$MUTABLE_IMAGE" in *"${IMAGE_REPO_SUFFIX}":*) : ;; *) fatal "image-tags/${ENVIRONMENT}.yaml '${IMAGE_KEY}' is '$MUTABLE_IMAGE', which is not a ${IMAGE_REPO_SUFFIX} image" ;; esac
export DEPLOY_PLATFORM="linux/amd64"
export REGISTRY_SCHEME="http"
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" || fatal "cannot resolve registry digest for $MUTABLE_IMAGE (is the service image built + pushed for $ENVIRONMENT?)"
case "$PINNED_IMAGE" in *@sha256:*) : ;; *) fatal "refusing to run a Job on an unpinned image ref: $PINNED_IMAGE" ;; esac
PINNED_DIGEST="${PINNED_IMAGE##*@}"
echo "image: $MUTABLE_IMAGE -> $PINNED_IMAGE"
# STATED, NOT ENFORCED: the tag must carry a build with ZeroDteResearchMigrator (options-edge-processing increment 9a). If not, the
# container fails on "Could not find or load main class" and step 6 names that.

# --- 4. the legacy writers are QUIESCED: every service pod on the pinned image, the research flag off --------------------------
# The compatibility image carries the legacy writer retired at v7; what must be TRUE before the migration is that no pod can still be
# writing v6: every vix-option-inteligence pod runs the SAME digest this Job runs (the image whose ResearchWriter refuses v7) and carries
# ZERODTE_RESEARCH_ENABLED != true. A pod list that cannot be read is not an empty one.
PODS_JSON="$(kubectl -n "$NAMESPACE" get pods -l "$SERVICE_POD_LABEL" -o json)" || fatal "cannot list the vix-option-inteligence pods in $NAMESPACE — the legacy writers' quiescence cannot be judged; refusing"
POD_COUNT="$(printf '%s' "$PODS_JSON" | jq -r '.items | length')" || fatal "cannot parse the pod list"
# a pod is judged by its container STATUS image id (the digest the kubelet actually runs), not by its spec's tag
NOT_QUIESCED="$(printf '%s' "$PODS_JSON" | jq -r --arg digest "$PINNED_DIGEST" '
  [ .items[] | select(.metadata.deletionTimestamp == null) | . as $p
    | ( [ (.status.containerStatuses // [])[] | .imageID ] | map(test("@" + $digest + "$")) | (length > 0 and all) ) as $onImage
    | ( [ (.spec.containers[].env // [])[] | select(.name == "ZERODTE_RESEARCH_ENABLED") | .value ] | any(. == "true") ) as $flagOn
    | select(($onImage | not) or $flagOn)
    | $p.metadata.name + "(" + (if $onImage then "image ok" else "another image" end) + "," + (if $flagOn then "ZERODTE_RESEARCH_ENABLED=true" else "flag off" end) + ")" ]
  | join(" ")')" || fatal "cannot judge the pods"
echo "vix-option-inteligence pods: $POD_COUNT; pinned digest $PINNED_DIGEST"
[ -z "$NOT_QUIESCED" ] || fatal "the legacy research writers are NOT quiesced: $NOT_QUIESCED. Roll the V1 compatibility image (increment 9a) with ZERODTE_RESEARCH_ENABLED unset or false to every pod first; the migration refuses until no pod can still write v6."
echo "legacy writers quiesced: every running pod is on the pinned image with the research flag off — the Job will carry --legacy-writers-quiesced"
QUIESCED=true

RENDER="$(mktemp)"; CM_RENDER="$(mktemp)"; LOGS="$(mktemp)"; ERRLOG="$(mktemp)"

# --- 5. the LOCK, then the inventory --------------------------------------------------------------------------------------------
LOCK_HOLDER="$(cat <<EOF2
apiVersion: v1
kind: ConfigMap
metadata:
  name: $LOCK_NAME
  namespace: $NAMESPACE
  labels:
    app.kubernetes.io/name: zerodte-research-migrate-lock
    app.kubernetes.io/part-of: options-edge
  annotations:
    options-edge.io/holder: "build-${BUILD_NUMBER:-manual}-$(date -u +%Y%m%dT%H%M%SZ)"
    options-edge.io/environment: "$ENVIRONMENT"
data:
  held: "true"
EOF2
)"
if printf '%s\n' "$LOCK_HOLDER" | kubectl -n "$NAMESPACE" create -f - >/dev/null 2>"$ERRLOG"; then
  LOCK_OWNED=true
  echo "lock $LOCK_NAME acquired"
else
  HELD_BY="$(kubectl -n "$NAMESPACE" get "configmap/$LOCK_NAME" -o jsonpath='{.metadata.annotations.options-edge\.io/holder}' 2>/dev/null || echo '<unreadable>')"
  fatal "could not acquire lock $LOCK_NAME (held by ${HELD_BY:-<unknown>}): another migration is running, or a crashed one left its lock. Read that run's log, then \`kubectl -n $NAMESPACE delete configmap $LOCK_NAME\` by hand. ($(tr '\n' ' ' < "$ERRLOG"))"
fi
JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json)" || fatal "cannot list zerodte-research-migrate Jobs in $NAMESPACE — refusing to migrate beside an inventory that could not be read"
ACTIVE="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1 or ([(.status.conditions // [])[] | select((.type == "Failed" or .type == "Complete") and .status == "True")] | length) >= 1) | not) | .metadata.name] | join(" ")')" \
  || fatal "cannot parse the zerodte-research-migrate Job list — refusing to migrate beside an inventory that could not be read"
[ -z "$ACTIVE" ] || fatal "another zerodte-research-migrate Job is not terminal ($ACTIVE). Wait for it, or delete it if it is a leftover."

# --- 6. render + create ---------------------------------------------------------------------
CM_NAME="zerodte-research-migrate-${ENVIRONMENT}-${FILE_SHA256:0:12}"
JOB_NAME="zerodte-research-migrate-$(date -u +%Y%m%d-%H%M%S)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
kubectl -n "$NAMESPACE" create configmap "$CM_NAME" --from-file="${FILE_BASENAME}=${FILE}" --dry-run=client -o yaml > "$CM_RENDER"
yq -i ".metadata.labels.\"app.kubernetes.io/name\" = \"zerodte-research-migrate-files\" | .metadata.labels.\"app.kubernetes.io/part-of\" = \"options-edge\" | .metadata.labels.\"options-edge.io/zerodte-environment\" = \"${ENVIRONMENT}\"" "$CM_RENDER"
[ "$(yq -r '.data | keys | length' "$CM_RENDER")" = 1 ] || fatal "the rendered ConfigMap does not carry exactly one key"
sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" -e "s|__JOB_NAME__|${JOB_NAME}|g" -e "s|__ENVIRONMENT__|${ENVIRONMENT}|g" -e "s|__FILE_BASENAME__|${FILE_BASENAME}|g" \
    -e "s|__FILE_SHA256__|${FILE_SHA256}|g" -e "s|__SESSIONS_AHEAD__|${SESSIONS_AHEAD}|g" -e "s|__QUIESCED__|${QUIESCED}|g" -e "s|__CONFIRM__|${CONFIRM}|g" -e "s|__CONFIGMAP__|${CM_NAME}|g" \
    "$TEMPLATE" >"$RENDER"
grep -q '__[A-Z_]*__' "$RENDER" && fatal "unsubstituted placeholder left in the render"
_ns="$(yq -r '.metadata.namespace' "$RENDER")"
[ "$_ns" = "$NAMESPACE" ] || fatal "template namespace '$_ns' != NAMESPACE '$NAMESPACE'"
# The render carries a REFERENCE to the password, never a value; assert that shape before it leaves this host.
grep -q "POSTGRES_PASSWORD" "$RENDER" && ! grep -qE "POSTGRES_PASSWORD[[:space:]]*[:=][[:space:]]*[^[:space:]]" "$RENDER" || fatal "the render must reference POSTGRES_PASSWORD by secretKeyRef only"

echo "=== server-side validate (ConfigMap $CM_NAME, Job $JOB_NAME) ==="
kubectl -n "$NAMESPACE" apply --dry-run=server -f "$CM_RENDER" >/dev/null
kubectl -n "$NAMESPACE" create --dry-run=server -f "$RENDER" >/dev/null

echo "=== creating ConfigMap $CM_NAME and Job $JOB_NAME (env=$ENVIRONMENT from=$FROM_VERSION to=$TO_VERSION confirm=$CONFIRM) ==="
kubectl -n "$NAMESPACE" apply -f "$CM_RENDER"
JOB_OWNED=true
kubectl -n "$NAMESPACE" create -f "$RENDER"

echo "waiting up to ${JOB_TIMEOUT_S}s for $JOB_NAME ..."
deadline=$(( $(date +%s) + JOB_TIMEOUT_S ))
job_state() {
  local snap
  snap="$(kubectl -n "$NAMESPACE" get "job/$JOB_NAME" -o json 2>/dev/null)" || return 1
  printf '%s' "$snap" | jq -r 'if ((.status.succeeded // 0) >= 1) then "succeeded" elif ([(.status.conditions // [])[] | select(.type == "Failed" and .status == "True")] | length) >= 1 then "failed" else "active" end'
}
state="active"
while [ "$(date +%s)" -lt "$deadline" ]; do
  state="$(job_state || echo 'unreadable')"
  case "$state" in succeeded|failed) break ;; esac
  sleep 5
done
case "$state" in succeeded|failed) : ;; *) state="$(job_state || echo 'unreadable')" ;; esac
echo "job state: $state"
for _try in 1 2 3 4 5; do
  if kubectl -n "$NAMESPACE" logs "job/$JOB_NAME" --tail=-1 >"$LOGS" 2>/dev/null; then break; fi
  echo "logs for $JOB_NAME not available yet (attempt $_try) ..."; : >"$LOGS"; sleep 6
done
echo "===== $JOB_NAME log ====="; cat "$LOGS"; echo "===== end log ====="
EXIT_CODE="$(kubectl -n "$NAMESPACE" get pods -l "job-name=$JOB_NAME" -o json 2>/dev/null \
  | jq -r '[.items[] | (.status.containerStatuses // [])[] | select(.name == "migrator") | .state.terminated.exitCode // empty] | first // ""' 2>/dev/null || echo '')"
echo "migrator exit code: ${EXIT_CODE:-<unknown>}"

# --- the receipt: exactly one outcome line, an EXACT field set per outcome, every field in its domain, the exit code in agreement -------------
OUTCOMES="$(grep -E '^(MIGRATABLE|MIGRATED|ALREADY_MIGRATED|REFUSED)( |$)' "$LOGS" || true)"
N_OUTCOMES="$(printf '%s\n' "$OUTCOMES" | grep -c . || true)"
if [ "$state" != "succeeded" ] && grep -q 'Could not find or load main class' "$LOGS"; then
  fatal "$PINNED_IMAGE does not carry ZeroDteResearchMigrator: the ${IMAGE_KEY} tag holds a build older than options-edge-processing increment 9a. Build and push the service image first; nothing was migrated."
fi
[ "${N_OUTCOMES:-0}" = 1 ] || fatal "$JOB_NAME printed ${N_OUTCOMES:-0} receipt line(s) — expected exactly one (state=$state, exit ${EXIT_CODE:-unknown}). Refusing to guess which, if any, describes this run. Read the log."
RECEIPT="$OUTCOMES"
OUTCOME="${RECEIPT%% *}"
field() { # field <name> → the value of the ONE token name=value; never empty
  local name="$1" n=0 v="" tok
  for tok in $RECEIPT; do case "$tok" in "$name="*) n=$((n + 1)); v="${tok#"$name"=}" ;; esac; done
  [ "$n" = 1 ] || fatal "receipt must carry $name= exactly once; got it $n times in '$RECEIPT' — ambiguous, refused"
  [ -n "$v" ] || fatal "receipt field $name= is empty in '$RECEIPT' — refused"
  printf '%s' "$v"
}
exact_fields() { # every token after the outcome is name=value with an allowed name, each present exactly once, no extras
  local allowed=" $* " tok name
  for tok in ${RECEIPT#* }; do
    case "$tok" in *=*) name="${tok%%=*}" ;; *) fatal "receipt token '$tok' is not name=value in '$RECEIPT' — refused" ;; esac
    case "$allowed" in *" $name "*) : ;; *) fatal "receipt carries a field this outcome does not have: $name= in '$RECEIPT' — refused" ;; esac
  done
  for name in "$@"; do field "$name" >/dev/null; done
}
hex() { # hex <value> <length> <what>
  case "$1" in *[!0-9a-f]*|'') fatal "receipt $3 is not lowercase hex: '$1'" ;; esac
  [ "${#1}" -eq "$2" ] || fatal "receipt $3 has ${#1} characters, not $2: '$1'"
}
digits() { case "$1" in ''|*[!0-9]*) fatal "receipt $2 is not a non-negative integer: '$1'" ;; esac; }
exit_agrees() { [ "${EXIT_CODE:-}" = "$1" ] || fatal "the receipt says $OUTCOME, which exits $1, but the container exited '${EXIT_CODE:-<unknown>}' — the receipt and the process disagree; refused"; }
COUNTS="archiveFeatureFamilies nullQualityFlags nullSurfaceStatus nullSurfaceActionable scheduledBoundaries lateStartBoundaries calibratedShadowRows"
case "$OUTCOME" in
  REFUSED)
    exact_fields reason exit
    REASON="$(field reason)"; EXIT_TOKEN="$(field exit)"
    case "$EXIT_TOKEN" in 64|65|68|69|70) : ;; *) fatal "the receipt names exit=$EXIT_TOKEN, which is not a migrator refusal code ('$RECEIPT')" ;; esac
    exit_agrees "$EXIT_TOKEN"
    case "$EXIT_TOKEN" in
      64) fatal "the migrator refused its INVOCATION ($REASON) — the Job template and the migrator's CLI disagree; see the log. Nothing was written." ;;
      65) fatal "the migrator refused the SHIPPED CALENDAR ($REASON: the resource is not the SessionCalendar rules) — the image is wrong; see the log. Nothing was written." ;;
      68) fatal "a PRECONDITION failed ($REASON: the recorded version is not 6 or 7, the calendar does not cover the required years or differs from the recorded one, the catalog drifted after a migration, the legacy writers were not attested quiesced) — see the log. Nothing was written." ;;
      69) fatal "the research database was UNAVAILABLE or the migration lock was not acquired ($REASON) — see the log. Re-run once reachable; a dry run never writes." ;;
      70) fatal "the MIGRATION FAILED or stayed uncertain ($REASON) — the migrator rolled back or could not verify its commit: READ zerodte_schema_version and zerodte_research_migration before trying again (the server log holds the statement)." ;;
    esac ;;
esac
case "$CONFIRM:$OUTCOME" in
  true:MIGRATED|true:ALREADY_MIGRATED|false:MIGRATABLE|false:ALREADY_MIGRATED) : ;;
  *) fatal "receipt outcome '$OUTCOME' is not one a CONFIRM=$CONFIRM run may report ('$RECEIPT') — a dry-run line is not a migration, and a MIGRATED line on a dry run means the migrator committed when told not to" ;;
esac
[ "$state" = "succeeded" ] || fatal "$JOB_NAME printed '$OUTCOME' but did not succeed (state=$state, exit ${EXIT_CODE:-unknown}) — see the log above."
exit_agrees 0
MISMATCH=""
case "$OUTCOME" in
  MIGRATABLE)
    exact_fields fromVersion toVersion calendarVersion expectedSessions $COUNTS planDigest
    hex "$(field planDigest)" 64 planDigest ;;
  MIGRATED)
    exact_fields fromVersion toVersion calendarVersion expectedSessions $COUNTS schemaDigest
    hex "$(field schemaDigest)" 64 schemaDigest ;;
  ALREADY_MIGRATED)
    exact_fields version calendarVersion expectedSessions schemaDigest
    hex "$(field schemaDigest)" 64 schemaDigest
    [ "$(field version)" = "$TO_VERSION" ] || MISMATCH="$MISMATCH version='$(field version)'!='$TO_VERSION'" ;;
esac
if [ "$OUTCOME" != ALREADY_MIGRATED ]; then
  [ "$(field fromVersion)" = "$FROM_VERSION" ] || MISMATCH="$MISMATCH fromVersion='$(field fromVersion)'!='$FROM_VERSION'"
  [ "$(field toVersion)" = "$TO_VERSION" ]     || MISMATCH="$MISMATCH toVersion='$(field toVersion)'!='$TO_VERSION'"
  for n in $COUNTS; do digits "$(field "$n")" "$n"; done
fi
hex "$(field calendarVersion)" 64 calendarVersion
[ "$(field calendarVersion)" = "$CALENDAR_VERSION" ] || MISMATCH="$MISMATCH calendarVersion='$(field calendarVersion)'!='$CALENDAR_VERSION'"
digits "$(field expectedSessions)" expectedSessions
[ "$(field expectedSessions)" -ge 1 ] || MISMATCH="$MISMATCH expectedSessions=0"
[ -z "$MISMATCH" ] || fatal "the receipt does not describe the declaration this run migrates under:$MISMATCH
       receipt:  '$RECEIPT'
       declared: from=$FROM_VERSION to=$TO_VERSION calendarVersion=$CALENDAR_VERSION
       The image's calendar is not the reviewed one, or the migrator ran another migration. Refusing to report success."
echo "$RECEIPT"
case "$OUTCOME" in
  MIGRATABLE)       echo "OK: DRY RUN — the migration was executed and ROLLED BACK on $ENVIRONMENT; the counts above are the real rows' (archiveFeatureFamilies=$(field archiveFeatureFamilies), expectedSessions=$(field expectedSessions)). Re-run with CONFIRM=true to migrate." ;;
  MIGRATED)         echo "OK: MIGRATED $ENVIRONMENT research store $FROM_VERSION -> $TO_VERSION — calendarVersion=$(field calendarVersion) schemaDigest=$(field schemaDigest) (verified on a fresh connection). The provisioning Job (zerodte-provision) may now run." ;;
  ALREADY_MIGRATED) echo "OK: the $ENVIRONMENT research store is already at version $(field version) with this image's calendar ($(field calendarVersion)) and an unchanged catalog ($(field schemaDigest)) — nothing to do." ;;
esac
if [ "$CONFIRM" = false ] && [ -n "$DRY_RUN_RECEIPT" ]; then
  printf '%s\n' "$RECEIPT_LINE" > "$DRY_RUN_RECEIPT" || fatal "could not write the dry-run receipt to $DRY_RUN_RECEIPT"
  echo "dry-run receipt written: $RECEIPT_LINE"
fi
SUCCESS=true

# --- 7. prune old TERMINAL Jobs and the ConfigMaps nothing references (best-effort; skipped when an inventory cannot be read) --------
if JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null)"; then
  TERMINAL="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1) or ([(.status.conditions // [])[] | select(.type == "Failed" and .status == "True")] | length) >= 1)] | sort_by(.metadata.creationTimestamp) | .[].metadata.name' 2>/dev/null)" || TERMINAL=""
  COUNT="$(printf '%s\n' "$TERMINAL" | grep -c . || true)"
  if [ "${COUNT:-0}" -gt "$KEEP_JOBS" ]; then
    DROP=$(( COUNT - KEEP_JOBS )); i=0
    while IFS= read -r old; do
      [ -n "$old" ] || continue
      i=$(( i + 1 )); [ "$i" -le "$DROP" ] || break
      echo "pruning old zerodte-research-migrate Job $old"
      kubectl -n "$NAMESPACE" delete "job/$old" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done <<EOF2
$TERMINAL
EOF2
  else
    echo "no old zerodte-research-migrate Jobs to prune (terminal=${COUNT:-0}, keep=$KEEP_JOBS)"
  fi
  if JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null)" \
     && REFERENCED="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[].spec.template.spec.volumes[]? | .configMap.name // empty] | unique | .[]' 2>/dev/null)" \
     && CMS="$(kubectl -n "$NAMESPACE" get configmaps -l "$CM_LABEL" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)"; then
    for cm in $CMS; do
      [ "$cm" = "$CM_NAME" ] && continue
      printf '%s\n' "$REFERENCED" | grep -qxF "$cm" && continue
      echo "pruning unreferenced zerodte-research-migrate-files ConfigMap $cm"
      kubectl -n "$NAMESPACE" delete "configmap/$cm" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done
  else
    echo "ConfigMap pruning skipped: the Job or ConfigMap inventory could not be read (nothing deleted)"
  fi
else
  echo "pruning skipped: the zerodte-research-migrate Job list could not be read (nothing deleted)"
fi
