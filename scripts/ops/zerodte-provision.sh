#!/usr/bin/env bash
# Provision the 0DTE Stage-B topology of ONE environment — the operator side of k8s/jobs/zerodte-provision-job.yaml (read that file's
# header first: why a Job the Jenkins deployer creates, and what the container proves before it runs).
#
# The provisioner is the service image's ZeroDteProvisioner (options-edge-processing increment 7a): from the REVIEWED declaration
# deploy/zerodte/provisioning/<ENVIRONMENT>.yaml and the attestation deploy/zerodte/virgin-attestation.yaml, idempotently and in the
# design's order (schema v7 → output contract → verify present topics → create absent outputs → live ids → ledger → attestation → era →
# PROVISIONED + read-back). ONE INVOCATION = ONE PROVISIONER RUN: CONFIRM=false (the default, the stage that ALWAYS runs) performs every
# read and verification and writes nothing; CONFIRM=true is the write, run by the Jenkinsfile only when the operator ticked CONFIRM and
# only after the dry run passed in the same build.
#
# THE RECEIPT. The provisioner prints exactly ONE outcome line on stdout (its diagnostics go to stderr; secrets appear in neither):
#   PROVISIONABLE symbol= lineage= generation= eraId= planDigest= provisionedDigest= topicIds=RESOLVED|PENDING wouldCreate= wouldAssert= wouldAttest= wouldInsertEra= wouldAppend=PROVISIONED
#   PROVISIONED generation= eraId= ledgerOffset= provisionedDigest= ledgerTopicId= clusterId=
#   ALREADY_PROVISIONED generation= eraId= ledgerOffset= provisionedDigest= ledgerTopicId= clusterId=
#   ATTESTATION_REQUIRED generation= ledgerTopicId= clusterId=               (exit 67: the topics exist; add the printed entry through review, run again)
#   CONFLICTING_PROVISIONED generation=                                        (exit 66)
#   REFUSED reason=<TOKEN> exit=<n>                                            (64 file/usage, 65 attestation/contract, 68 precondition, 69 unavailable, 70 mutation)
# This script requires exactly one such line, PARSES its fields as whole tokens, counts each field's occurrences, and compares symbol,
# lineage, generation and eraId EXACTLY to the declaration this run provisions (read by the same validator that gated the PR); the outcome
# must be one the mode allows (dry run: PROVISIONABLE or ALREADY_PROVISIONED; confirm: PROVISIONED or ALREADY_PROVISIONED). Anything else
# is a refusal, never a success: this identity cannot read the ledger back, so the receipt is the only evidence and is held to the letter.
# On PROVISIONED / ALREADY_PROVISIONED it prints the deploy/zerodte/provisioned/<ENVIRONMENT>.yaml block the operator commits (increment
# 8's render reads ZERO_DTE_LEDGER_TOPIC_ID_EXPECTED from it). On ATTESTATION_REQUIRED it prints the exact attestation entry to append,
# with prevEntryHash = the current chain tail.
#
# THE WRITE NEEDS TWO THINGS RE-ESTABLISHED IN THIS PROCESS: PERMITTED_SHA must equal `git rev-parse HEAD` of this checkout (the
# Deployment Permission Rule, checked again right before the effect), and DRY_RUN_RECEIPT must name a receipt file THIS build's dry run
# wrote for THIS environment, declaration (sha256), attestation (sha256) and HEAD. A SEQUENCING MARKER inside a trusted Jenkins
# workspace, not authentication (read vol-premium-ledger-publish.sh for the exact scope of that claim).
#
# WHAT IT DOES, fail-closed at every step:
#   0. validates every parameter;
#   1. validates the declaration and the attestation with scripts/ci/validate-zerodte-provisioning.sh / validate-zerodte-attestation.sh
#      (the Job's own rules; append-only against origin/main) and reads the declaration's identity (symbol, lineage, generation, eraId);
#   2. asserts the kubectl identity IS the deployer SA AND the kubeconfig points at THIS environment's cluster;
#   3. resolves the env's vix-option-inteligence SERVICE image by EXACT key (image-tags/<env>.yaml) and digest-pins it;
#   4. refuses to start while another zerodte-provision Job is active — or while the Job list cannot be read;
#   5. renders the ConfigMap (both files, named by their sha256) and the Job, validates both server-side, creates them, waits, prints the
#      whole pod log, maps the container's exit code, and requires the receipt;
#   6. prunes old terminal Jobs and the ConfigMaps no remaining Job references (best-effort, skipped when an inventory cannot be read).
#
# Env:
#   ENVIRONMENT      dev | production
#   CONFIRM          false (dry run, default) | true (write)
#   PERMITTED_SHA    CONFIRM=true only: must equal `git rev-parse HEAD` of this checkout (40 lowercase hex)
#   DRY_RUN_RECEIPT  path of the same-build dry-run receipt: written on CONFIRM=false, REQUIRED on CONFIRM=true
#   BUILD_NUMBER     the Jenkins build number recorded in / required of that receipt
#   JOB_TIMEOUT_S    client-side wait, default 900 (> the Job's own 600s activeDeadlineSeconds)
#   KEEP_JOBS        terminal Jobs to retain, default 5
#   NAMESPACE        default options-edge
#   EXPECTED_API_SERVER   production only: default https://192.168.100.252:6443
#   EXPECTED_CLUSTER_NAME dev only: the kubeconfig's cluster name, default docker-desktop (Jenkins runs inside it; the deployer kubeconfig
#                         names that cluster, whose API server address differs per host)
set -euo pipefail
cd "$(dirname "$0")/../.."

ENVIRONMENT="${ENVIRONMENT:-}"
CONFIRM="${CONFIRM:-false}"
PERMITTED_SHA="${PERMITTED_SHA:-}"
DRY_RUN_RECEIPT="${DRY_RUN_RECEIPT:-}"
BUILD_NUMBER="${BUILD_NUMBER:-}"
JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-900}"
KEEP_JOBS="${KEEP_JOBS:-5}"
NAMESPACE="${NAMESPACE:-options-edge}"
EXPECTED_API_SERVER="${EXPECTED_API_SERVER:-https://192.168.100.252:6443}"
EXPECTED_CLUSTER_NAME="${EXPECTED_CLUSTER_NAME:-docker-desktop}"

TEMPLATE="k8s/jobs/zerodte-provision-job.yaml"
ATTESTATION="deploy/zerodte/virgin-attestation.yaml"
IMAGE_KEY="vix-option-inteligence-service"
IMAGE_REPO_SUFFIX="/options-edge-vix-option-inteligence"
JOB_LABEL="app.kubernetes.io/name=zerodte-provision"
CM_LABEL="app.kubernetes.io/name=zerodte-provision-files"
DEPLOYER="system:serviceaccount:options-edge:jenkins-deployer"
JOB_NAME=""
JOB_OWNED=false
SUCCESS=false

fatal() { echo "FATAL: $*" >&2; exit 1; }

cleanup() {
  local rc=$? terminal
  rm -f "${RENDER:-}" "${CM_RENDER:-}" "${LOGS:-}" "${ERRLOG:-}"
  if [ "$SUCCESS" != "true" ] && [ "$JOB_OWNED" = "true" ] && [ -n "$JOB_NAME" ]; then
    terminal="$(kubectl -n "$NAMESPACE" get "job/$JOB_NAME" -o json 2>/dev/null \
      | jq -r '[(.status.conditions // [])[] | select((.type == "Complete" or .type == "Failed") and .status == "True") | .type] | join(",")' 2>/dev/null || echo '')"
    if [ -n "$terminal" ]; then
      echo "cleanup: Job $JOB_NAME is terminal ($terminal) — keeping it for post-mortem (script rc=$rc)" >&2
    else
      echo "cleanup: deleting still-active Job $JOB_NAME (script exiting rc=$rc without a successful finish)" >&2
      kubectl -n "$NAMESPACE" delete "job/$JOB_NAME" --cascade=foreground --wait=true --timeout=180s --ignore-not-found >&2 || true
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
[ "$JOB_TIMEOUT_S" -ge 660 ] && [ "$JOB_TIMEOUT_S" -le 3600 ] || fatal "JOB_TIMEOUT_S must be within 660..3600 (the Job's own deadline is 600s), got $JOB_TIMEOUT_S"
[ "$KEEP_JOBS" -ge 1 ] && [ "$KEEP_JOBS" -le 100 ] || fatal "KEEP_JOBS must be within 1..100, got $KEEP_JOBS"
FILE="deploy/zerodte/provisioning/${ENVIRONMENT}.yaml"
[ -f "$FILE" ] || fatal "no declaration for $ENVIRONMENT at $FILE"
[ -f "$ATTESTATION" ] || fatal "no attestation at $ATTESTATION"
[ -f "$TEMPLATE" ] || fatal "missing Job template $TEMPLATE"
command -v yq >/dev/null 2>&1 || fatal "yq is required"
command -v jq >/dev/null 2>&1 || fatal "jq is required"
command -v python3 >/dev/null 2>&1 || fatal "python3 is required"
FILE_BASENAME="$(basename "$FILE")"

# --- 1. the files: validated the way the Job validates them, then the declaration's identity read -----------------------------
echo "=== validating $FILE and $ATTESTATION ==="
bash scripts/ci/validate-zerodte-provisioning.sh || fatal "the declarations do not pass scripts/ci/validate-zerodte-provisioning.sh — nothing is provisioned"
bash scripts/ci/validate-zerodte-attestation.sh || fatal "$ATTESTATION does not pass scripts/ci/validate-zerodte-attestation.sh — nothing is provisioned"
IDENTITY="$(python3 scripts/ci/zerodte_attestation.py provisioning "$FILE")" || fatal "could not read the declaration's identity"
read -r SYMBOL LINEAGE GENERATION ERA_ID <<EOF2
$(printf '%s' "$IDENTITY" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["symbol"], d["environmentLineageId"], d["generation"], d["eraId"])')
EOF2
[ -n "${SYMBOL:-}" ] && [ -n "${LINEAGE:-}" ] && [ -n "${GENERATION:-}" ] && [ -n "${ERA_ID:-}" ] || fatal "could not read symbol / lineage / generation / eraId from $FILE"
TAIL_HASH="$(python3 scripts/ci/zerodte_attestation.py tail "$ATTESTATION")" || fatal "could not compute the attestation's chain tail"
FILE_SHA256="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$FILE")"
ATTESTATION_SHA256="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$ATTESTATION")"
echo "declaration: env=$ENVIRONMENT symbol=$SYMBOL lineage=$LINEAGE generation=$GENERATION eraId=$ERA_ID file_sha256=$FILE_SHA256 attestation_sha256=$ATTESTATION_SHA256 attestation_tail=$TAIL_HASH"
echo "mode: $([ "$CONFIRM" = true ] && echo 'CONFIRM=true — the provisioner WRITES (topics, era, PROVISIONED)' || echo 'CONFIRM=false — DRY RUN, the provisioner writes nothing')"
HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || echo '')"
RECEIPT_LINE="build=${BUILD_NUMBER} env=${ENVIRONMENT} symbol=${SYMBOL} generation=${GENERATION} eraId=${ERA_ID} file_sha256=${FILE_SHA256} attestation_sha256=${ATTESTATION_SHA256} head=${HEAD_SHA}"
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
       The dry run that passed was for another build, environment, declaration, attestation or commit. Re-run the whole build."
  echo "dry-run receipt of this build accepted: $GOT_RECEIPT"
fi

# --- 2. identity AND cluster ---------------------------------------------------------------
WHOAMI="$(kubectl auth whoami -o jsonpath='{.status.userInfo.username}' 2>/dev/null || echo '')"
echo "kubectl identity: ${WHOAMI:-<unknown>}"
[ "$WHOAMI" = "$DEPLOYER" ] || fatal "kubeconfig identity is '${WHOAMI:-<unknown>}', expected '$DEPLOYER'. Job and ConfigMap creation is denied for every other principal by the options-edge-jenkins-only-workloads admission policy."
API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || echo '')"
CLUSTER_NAME="$(kubectl config view --minify -o jsonpath='{.clusters[0].name}' 2>/dev/null || echo '')"
echo "api server: ${API_SERVER:-<unknown>} (cluster ${CLUSTER_NAME:-<unknown>})"
case "$ENVIRONMENT" in
  production) [ "$API_SERVER" = "$EXPECTED_API_SERVER" ] || fatal "kubeconfig points at '${API_SERVER:-<unknown>}', expected '$EXPECTED_API_SERVER' for production. The deployer service-account name alone does not identify a cluster." ;;
  dev)        [ "$CLUSTER_NAME" = "$EXPECTED_CLUSTER_NAME" ] || fatal "kubeconfig names cluster '${CLUSTER_NAME:-<unknown>}', expected '$EXPECTED_CLUSTER_NAME' for dev. The deployer service-account name alone does not identify a cluster." ;;
esac

# --- 3. resolve + digest-pin the SERVICE image ------------------------------------------------
MUTABLE_IMAGE="$(yq -er ".images.\"${IMAGE_KEY}\"" "image-tags/${ENVIRONMENT}.yaml" 2>/dev/null || true)"
[ -n "$MUTABLE_IMAGE" ] && [ "$MUTABLE_IMAGE" != "null" ] || fatal "image-tags/${ENVIRONMENT}.yaml has no '${IMAGE_KEY}' entry"
case "$MUTABLE_IMAGE" in *"${IMAGE_REPO_SUFFIX}":*) : ;; *) fatal "image-tags/${ENVIRONMENT}.yaml '${IMAGE_KEY}' is '$MUTABLE_IMAGE', which is not a ${IMAGE_REPO_SUFFIX} image" ;; esac
export DEPLOY_PLATFORM="linux/amd64"
export REGISTRY_SCHEME="http"
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" || fatal "cannot resolve registry digest for $MUTABLE_IMAGE (is the service image built + pushed for $ENVIRONMENT?)"
case "$PINNED_IMAGE" in *@sha256:*) : ;; *) fatal "refusing to run a Job on an unpinned image ref: $PINNED_IMAGE" ;; esac
echo "image: $MUTABLE_IMAGE -> $PINNED_IMAGE"
# STATED, NOT ENFORCED: the tag must carry a build with ZeroDteProvisioner (options-edge-processing ≥ #920). If not, the container fails on
# "Could not find or load main class" and step 5 names that.

# --- 4. refuse to start alongside another provisioning Job --------------------------------
JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json)" || fatal "cannot list zerodte-provision Jobs in $NAMESPACE — refusing to provision beside an inventory that could not be read"
ACTIVE="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1 or ([(.status.conditions // [])[] | select((.type == "Failed" or .type == "Complete") and .status == "True")] | length) >= 1) | not) | .metadata.name] | join(" ")')" \
  || fatal "cannot parse the zerodte-provision Job list — refusing to provision beside an inventory that could not be read"
[ -z "$ACTIVE" ] || fatal "another zerodte-provision Job is not terminal ($ACTIVE). Wait for it, or delete it if it is a leftover."

# --- 5. render + create ---------------------------------------------------------------------
CM_NAME="zerodte-provision-${ENVIRONMENT}-${FILE_SHA256:0:8}-${ATTESTATION_SHA256:0:8}"
JOB_NAME="zerodte-provision-$(date -u +%Y%m%d-%H%M%S)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
RENDER="$(mktemp)"; CM_RENDER="$(mktemp)"; LOGS="$(mktemp)"; ERRLOG="$(mktemp)"
kubectl -n "$NAMESPACE" create configmap "$CM_NAME" --from-file="${FILE_BASENAME}=${FILE}" --from-file="virgin-attestation.yaml=${ATTESTATION}" --dry-run=client -o yaml > "$CM_RENDER"
yq -i ".metadata.labels.\"app.kubernetes.io/name\" = \"zerodte-provision-files\" | .metadata.labels.\"app.kubernetes.io/part-of\" = \"options-edge\" | .metadata.labels.\"options-edge.io/zerodte-environment\" = \"${ENVIRONMENT}\"" "$CM_RENDER"
[ "$(yq -r '.data | keys | length' "$CM_RENDER")" = 2 ] || fatal "the rendered ConfigMap does not carry exactly two keys"
sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" -e "s|__JOB_NAME__|${JOB_NAME}|g" -e "s|__ENVIRONMENT__|${ENVIRONMENT}|g" -e "s|__FILE_BASENAME__|${FILE_BASENAME}|g" \
    -e "s|__FILE_SHA256__|${FILE_SHA256}|g" -e "s|__ATTESTATION_SHA256__|${ATTESTATION_SHA256}|g" -e "s|__CONFIRM__|${CONFIRM}|g" -e "s|__CONFIGMAP__|${CM_NAME}|g" \
    "$TEMPLATE" >"$RENDER"
grep -q '__[A-Z_]*__' "$RENDER" && fatal "unsubstituted placeholder left in the render"
_ns="$(yq -r '.metadata.namespace' "$RENDER")"
[ "$_ns" = "$NAMESPACE" ] || fatal "template namespace '$_ns' != NAMESPACE '$NAMESPACE'"
# The render carries REFERENCES to the two secrets, never values; assert that shape before it leaves this host.
for secret in POSTGRES_PASSWORD ZERO_DTE_LEDGER_KEY; do
  grep -q "$secret" "$RENDER" && ! grep -qE "${secret}[[:space:]]*[:=][[:space:]]*[^[:space:]]" "$RENDER" || fatal "the render must reference $secret by secretKeyRef only"
done

echo "=== server-side validate (ConfigMap $CM_NAME, Job $JOB_NAME) ==="
kubectl -n "$NAMESPACE" apply --dry-run=server -f "$CM_RENDER" >/dev/null
kubectl -n "$NAMESPACE" create --dry-run=server -f "$RENDER" >/dev/null

echo "=== creating ConfigMap $CM_NAME and Job $JOB_NAME (env=$ENVIRONMENT generation=$GENERATION confirm=$CONFIRM) ==="
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
  | jq -r '[.items[] | (.status.containerStatuses // [])[] | select(.name == "provisioner") | .state.terminated.exitCode // empty] | first // ""' 2>/dev/null || echo '')"
echo "provisioner exit code: ${EXIT_CODE:-<unknown>}"

# --- the receipt: exactly one outcome line, parsed, every field matched EXACTLY --------------------------------------------------
OUTCOMES="$(grep -E '^(PROVISIONABLE|PROVISIONED|ALREADY_PROVISIONED|ATTESTATION_REQUIRED|CONFLICTING_PROVISIONED|REFUSED)( |$)' "$LOGS" || true)"
N_OUTCOMES="$(printf '%s\n' "$OUTCOMES" | grep -c . || true)"
if [ "$state" != "succeeded" ] && grep -q 'Could not find or load main class' "$LOGS"; then
  fatal "$PINNED_IMAGE does not carry ZeroDteProvisioner: the ${IMAGE_KEY} tag holds a build older than options-edge-processing #920. Build and push the service image first; nothing was provisioned."
fi
[ "${N_OUTCOMES:-0}" = 1 ] || fatal "$JOB_NAME printed ${N_OUTCOMES:-0} receipt line(s) — expected exactly one (state=$state, exit ${EXIT_CODE:-unknown}). Refusing to guess which, if any, describes this run. Read the log."
RECEIPT="$OUTCOMES"
OUTCOME="${RECEIPT%% *}"
field() { # field <name> → the value of the ONE token name=value; counts occurrences separately from the value
  local name="$1" n=0 v="" tok
  for tok in $RECEIPT; do case "$tok" in "$name="*) n=$((n + 1)); v="${tok#"$name"=}" ;; esac; done
  [ "$n" = 1 ] || fatal "receipt must carry $name= exactly once; got it $n times in '$RECEIPT' — ambiguous, refused"
  printf '%s' "$v"
}
case "$OUTCOME" in
  ATTESTATION_REQUIRED)
    R_GEN="$(field generation)"; R_LEDGER="$(field ledgerTopicId)"; R_CLUSTER="$(field clusterId)"
    [ "$R_GEN" = "$GENERATION" ] || fatal "the receipt names generation '$R_GEN', this run declared $GENERATION: whatever the provisioner did, it was not this provisioning"
    [ "$CONFIRM" = true ] || fatal "ATTESTATION_REQUIRED is a CONFIRM outcome; a dry run reports wouldAttest=YES instead — the provisioner and this wrapper disagree"
    echo "$RECEIPT"
    cat <<EOF3
ATTESTATION REQUIRED (exit 67, nothing else was written): the output topics now exist and the ledger topic's live id is known. Append EXACTLY this entry to
$ATTESTATION through the reviewed, append-only change (prevEntryHash is the current chain tail), merge it, then run this job again (dry run, then CONFIRM):
  - symbol: $SYMBOL
    environmentLineageId: $LINEAGE
    ledgerTopicId: "$R_LEDGER"
    clusterId: "$R_CLUSTER"
    createdAt: "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    operator: <your operator id, 1-64 printable ASCII>
    prevEntryHash: "$TAIL_HASH"
EOF3
    exit 67 ;;
  CONFLICTING_PROVISIONED)
    fatal "CONFLICTING_PROVISIONED: the ledger already holds generation $(field generation) with a DIFFERENT declaration. A generation is immutable once provisioned — declare the next generation as a migration, or find out which declaration is the one that should have been provisioned. Nothing was written. ($RECEIPT)" ;;
  REFUSED)
    REASON="$(field reason)"; EXIT_TOKEN="$(field exit)"
    case "$EXIT_TOKEN" in
      64) fatal "the provisioner refused its INVOCATION or the declaration ($REASON) — the CLI contract and this wrapper disagree, or the file is malformed; see the log. Nothing was written." ;;
      65) fatal "the provisioner refused the ATTESTATION or the declared generation contract ($REASON) — see the log. Nothing was written." ;;
      68) fatal "a PRECONDITION failed ($REASON: the research schema is not v7, a topic's policy, a missing input, the era binding, the ledger's integrity) — see the log. Nothing was written past the verified prerequisites." ;;
      69) fatal "Kafka or the research database was UNAVAILABLE ($REASON) — see the log. Re-run once reachable; the provisioner is create-or-verify." ;;
      70) fatal "a MUTATION failed or stayed uncertain after the read-back ($REASON) — READ THE LEDGER AND THE TOPICS before trying again (the log names what was created). The provisioner is create-or-verify: a re-run verifies what exists." ;;
      *)  fatal "the provisioner refused ($REASON, exit $EXIT_TOKEN) — see the log." ;;
    esac ;;
esac
case "$CONFIRM:$OUTCOME" in
  true:PROVISIONED|true:ALREADY_PROVISIONED|false:PROVISIONABLE|false:ALREADY_PROVISIONED) : ;;
  *) fatal "receipt outcome '$OUTCOME' is not one a CONFIRM=$CONFIRM run may report ('$RECEIPT') — a dry-run line is not a provisioning, and a PROVISIONED line on a dry run means the provisioner wrote when told not to" ;;
esac
[ "$state" = "succeeded" ] || fatal "$JOB_NAME printed '$OUTCOME' but did not succeed (state=$state, exit ${EXIT_CODE:-unknown}) — see the log above."
R_GEN="$(field generation)"; R_ERA="$(field eraId)"
MISMATCH=""
[ "$R_GEN" = "$GENERATION" ] || MISMATCH="$MISMATCH generation='$R_GEN'!='$GENERATION'"
[ "$R_ERA" = "$ERA_ID" ]     || MISMATCH="$MISMATCH eraId='$R_ERA'!='$ERA_ID'"
if [ "$OUTCOME" = PROVISIONABLE ]; then
  [ "$(field symbol)" = "$SYMBOL" ]   || MISMATCH="$MISMATCH symbol"
  [ "$(field lineage)" = "$LINEAGE" ] || MISMATCH="$MISMATCH lineage"
  field planDigest >/dev/null; field topicIds >/dev/null; field wouldAttest >/dev/null; field wouldInsertEra >/dev/null
  [ "$(field wouldAppend)" = PROVISIONED ] || MISMATCH="$MISMATCH wouldAppend"
else
  R_LEDGER="$(field ledgerTopicId)"; R_CLUSTER="$(field clusterId)"; R_DIGEST="$(field provisionedDigest)"; R_OFFSET="$(field ledgerOffset)"
  case "$R_LEDGER" in *[!0-9a-f]*|'') MISMATCH="$MISMATCH ledgerTopicId" ;; esac
  [ "${#R_LEDGER}" -eq 32 ] || MISMATCH="$MISMATCH ledgerTopicId-length"
  case "$R_DIGEST" in *[!0-9a-f]*|'') MISMATCH="$MISMATCH provisionedDigest" ;; esac
  [ "${#R_DIGEST}" -eq 64 ] || MISMATCH="$MISMATCH provisionedDigest-length"
  case "$R_OFFSET" in ''|*[!0-9]*) MISMATCH="$MISMATCH ledgerOffset" ;; esac
fi
[ -z "$MISMATCH" ] || fatal "the receipt does not describe the declaration this run provisioned:$MISMATCH
       receipt: '$RECEIPT'
       declared: symbol=$SYMBOL lineage=$LINEAGE generation=$GENERATION eraId=$ERA_ID
       Whatever the provisioner did, it was not this provisioning. Refusing to report success."
echo "$RECEIPT"
if [ "$OUTCOME" = PROVISIONABLE ]; then
  case "$(field topicIds)" in
    PENDING)  echo "OK: DRY RUN — the declaration is lawful; the output topics do not exist yet (topicIds=PENDING), so the live ids and the record digest cannot be known before a CONFIRM run creates them. wouldAttest=$(field wouldAttest) wouldInsertEra=$(field wouldInsertEra)." ;;
    RESOLVED) echo "OK: DRY RUN — the declaration is lawful and fully resolved against the live cluster (provisionedDigest=$(field provisionedDigest)); nothing was written. wouldAttest=$(field wouldAttest) wouldInsertEra=$(field wouldInsertEra). Re-run with CONFIRM=true to provision." ;;
    *)        fatal "topicIds is neither RESOLVED nor PENDING in '$RECEIPT'" ;;
  esac
else
  case "$CONFIRM:$OUTCOME" in
    true:PROVISIONED)  echo "OK: PROVISIONED generation $R_GEN of $SYMBOL on $ENVIRONMENT — ledgerOffset=$R_OFFSET ledgerTopicId=$R_LEDGER clusterId=$R_CLUSTER" ;;
    *)                 echo "OK: the ledger already holds generation $R_GEN of $SYMBOL on $ENVIRONMENT with this exact declaration (ledgerOffset=$R_OFFSET) — nothing to do." ;;
  esac
  cat <<EOF3
Commit this as deploy/zerodte/provisioned/${ENVIRONMENT}.yaml (increment 8's runtime render reads ZERO_DTE_PROVISIONING_GENERATION and ZERO_DTE_LEDGER_TOPIC_ID_EXPECTED from it):
environment: $ENVIRONMENT
symbol: $SYMBOL
generation: $R_GEN
eraId: $R_ERA
ledgerTopicId: "$R_LEDGER"
clusterId: "$R_CLUSTER"
provisionedDigest: "$R_DIGEST"
ledgerOffset: $R_OFFSET
EOF3
fi
if [ "$CONFIRM" = false ] && [ -n "$DRY_RUN_RECEIPT" ]; then
  printf '%s\n' "$RECEIPT_LINE" > "$DRY_RUN_RECEIPT" || fatal "could not write the dry-run receipt to $DRY_RUN_RECEIPT"
  echo "dry-run receipt written: $RECEIPT_LINE"
fi
SUCCESS=true

# --- 6. prune old TERMINAL Jobs and the ConfigMaps nothing references (best-effort; skipped when an inventory cannot be read) --------
if JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null)"; then
  TERMINAL="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1) or ([(.status.conditions // [])[] | select(.type == "Failed" and .status == "True")] | length) >= 1)] | sort_by(.metadata.creationTimestamp) | .[].metadata.name' 2>/dev/null)" || TERMINAL=""
  COUNT="$(printf '%s\n' "$TERMINAL" | grep -c . || true)"
  if [ "${COUNT:-0}" -gt "$KEEP_JOBS" ]; then
    DROP=$(( COUNT - KEEP_JOBS )); i=0
    while IFS= read -r old; do
      [ -n "$old" ] || continue
      i=$(( i + 1 )); [ "$i" -le "$DROP" ] || break
      echo "pruning old zerodte-provision Job $old"
      kubectl -n "$NAMESPACE" delete "job/$old" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done <<EOF2
$TERMINAL
EOF2
  else
    echo "no old zerodte-provision Jobs to prune (terminal=${COUNT:-0}, keep=$KEEP_JOBS)"
  fi
  if JOBS_JSON="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null)" \
     && REFERENCED="$(printf '%s' "$JOBS_JSON" | jq -r '[.items[].spec.template.spec.volumes[]? | .configMap.name // empty] | unique | .[]' 2>/dev/null)" \
     && CMS="$(kubectl -n "$NAMESPACE" get configmaps -l "$CM_LABEL" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)"; then
    for cm in $CMS; do
      [ "$cm" = "$CM_NAME" ] && continue
      printf '%s\n' "$REFERENCED" | grep -qxF "$cm" && continue
      echo "pruning unreferenced zerodte-provision-files ConfigMap $cm"
      kubectl -n "$NAMESPACE" delete "configmap/$cm" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done
  else
    echo "ConfigMap pruning skipped: the Job or ConfigMap inventory could not be read (nothing deleted)"
  fi
else
  echo "pruning skipped: the zerodte-provision Job list could not be read (nothing deleted)"
fi
