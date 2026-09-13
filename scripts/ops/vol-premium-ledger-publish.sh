#!/usr/bin/env bash
# Publish one vol-premium ledger artefact (the event calendar) to the Postgres ledger — the operator side of
# k8s/jobs/vol-premium-ledger-publish-job.yaml (read that file's header first: it explains why the publisher runs
# as a Job the Jenkins deployer creates, and what the container proves before it writes).
#
# Owner decision 2026-09-13: the calendar ledger is the Postgres table vol_premium_calendar_ledger, written only by
# the engine image's VolPremiumLedgerPublisher, run by a person through Jenkinsfile.vol-premium-ledger-publish.
#
# ONE INVOCATION = ONE PUBLISHER RUN. CONFIRM=false (the default, and the stage that ALWAYS runs) is the dry run:
# the publisher validates the artefact, scans the ledger, prints PUBLISHABLE (or ALREADY_PRESENT) and writes
# nothing. CONFIRM=true is the write, run by the Jenkinsfile only when the operator ticked CONFIRM, and only after
# the dry run passed in the same build. The publisher's contract (engine side, options-edge-processing PR #790):
#     VolPremiumLedgerPublisher --kind calendar --file <json> [--confirm] [--published-by <who>]
#     dry run  -> exit 0, prints PUBLISHABLE, writes nothing
#     --confirm -> inserts once; the same content again is a no-op (ALREADY_PRESENT); different content under an
#                  existing version is exit 66 (LEDGER_KEY_CONFLICT — a version is immutable once written)
#     65 the artefact fails validation, 64 usage, 70 the ledger is unreachable or the write failed
#
# WHAT THIS SCRIPT REQUIRES OF THE PUBLISHER'S OUTPUT (the receipt grammar — read from the pod log, anchored, and
# bound to the KIND and VERSION that were asked for, so a run that published a different artefact cannot be
# reported as this one succeeding):
#     dry run:  a line starting with PUBLISHABLE or ALREADY_PRESENT that names "<kind>" and "<version>"
#     confirm:  a line starting with PUBLISHED   or ALREADY_PRESENT that names "<kind>" and "<version>"
# and the Job's container must exit 0. Any other exit is mapped to the publisher's own code (66 = conflict, ...)
# and the run FAILS with that reason. An exit 0 without a receipt is also a failure: success is never assumed.
#
# WHAT IT DOES, fail-closed at every step:
#   0. validates every parameter (free text from Jenkins reaches kubectl arguments and Job names);
#   1. validates the artefact FILE with scripts/ci/validate-vol-premium-calendar.sh (the boot-time rules and the
#      canonical content hash), and reads its version + content hash — the (version, hash) this run publishes;
#   2. asserts the kubectl identity IS the deployer SA (the only principal the options-edge-jenkins-only-workloads
#      admission policy lets create Jobs and ConfigMaps) AND that the kubeconfig points at production's API server;
#   3. resolves the env's vol-premium ENGINE image by EXACT key (image-tags/production.yaml: vol-premium-service)
#      and digest-pins it — a floating tag is never run;
#   4. refuses to start while another ledger-publish Job is active;
#   5. renders the ConfigMap (the file, named by its sha256) and the Job, validates both server-side, creates
#      them, waits, prints the whole pod log, maps the container's exit code, and requires the receipt;
#   6. prunes old terminal Jobs and the ConfigMaps no remaining Job references (best-effort).
#
# WHAT IT DOES NOT DO: it never reads the Postgres row back — this identity has no path to that database (it is
# not in the cluster, and the deployer SA has no pods/exec). The receipt the publisher prints, and the (version,
# hash) validated from the file, are what this run logs as "the row it inserted".
#
# Usage (normally from Jenkinsfile.vol-premium-ledger-publish):
#   ENVIRONMENT=production KIND=calendar FILE=scripts/vol-premium/calendars/calendar-2026.1.json \
#     CONFIRM=false PUBLISHED_BY=<who> KUBECONFIG=<deployer kubeconfig> scripts/ops/vol-premium-ledger-publish.sh
#
# Env:
#   ENVIRONMENT      production (the only supported env for now: dev has no publish path yet — see the Jenkinsfile)
#   KIND             calendar (the only kind this job publishes; baseline stays on its Kafka ledger)
#   FILE             repository path of the artefact (under scripts/vol-premium/)
#   CONFIRM          false (dry run, default) | true (write)
#   PUBLISHED_BY     free text recorded in the row's published_by (letters, digits, . _ - @ only); empty = the
#                    publisher's default
#   JOB_TIMEOUT_S    client-side wait, default 900 (> the Job's own 600s activeDeadlineSeconds)
#   KEEP_JOBS        terminal Jobs to retain, default 5
#   NAMESPACE        default options-edge
#   EXPECTED_API_SERVER  default https://192.168.100.252:6443
set -euo pipefail
cd "$(dirname "$0")/../.."

ENVIRONMENT="${ENVIRONMENT:-production}"
KIND="${KIND:-calendar}"
FILE="${FILE:-scripts/vol-premium/calendars/calendar-2026.1.json}"
CONFIRM="${CONFIRM:-false}"
PUBLISHED_BY="${PUBLISHED_BY:-}"
JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-900}"
KEEP_JOBS="${KEEP_JOBS:-5}"
NAMESPACE="${NAMESPACE:-options-edge}"
EXPECTED_API_SERVER="${EXPECTED_API_SERVER:-https://192.168.100.252:6443}"

TEMPLATE="k8s/jobs/vol-premium-ledger-publish-job.yaml"
IMAGE_KEY="vol-premium-service"
IMAGE_REPO_SUFFIX="/options-edge-vol-premium"
JOB_LABEL="app.kubernetes.io/name=vol-premium-ledger-publish"
CM_LABEL="app.kubernetes.io/name=vol-premium-ledger-file"
DEPLOYER="system:serviceaccount:options-edge:jenkins-deployer"
JOB_NAME=""
JOB_OWNED=false
SUCCESS=false

fatal() { echo "FATAL: $*" >&2; exit 1; }

# A still-active Job this invocation created is taken down on an unsuccessful exit (it holds the production
# Postgres password); a terminal Job is kept — its pod log is the post-mortem evidence.
cleanup() {
  local rc=$? terminal
  rm -f "${RENDER:-}" "${CM_RENDER:-}" "${LOGS:-}"
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
[ "$ENVIRONMENT" = "production" ] \
  || fatal "ENVIRONMENT='$ENVIRONMENT' is not supported (production only — see the Jenkinsfile header)"
[ "$KIND" = "calendar" ] || fatal "KIND='$KIND' is not supported (calendar only: the baseline ledger stays on Kafka)"
case "$CONFIRM" in true|false) : ;; *) fatal "CONFIRM must be true or false, got '$CONFIRM'" ;; esac
case "$JOB_TIMEOUT_S" in ''|*[!0-9]*) fatal "JOB_TIMEOUT_S must be digits, got '$JOB_TIMEOUT_S'" ;; esac
case "$KEEP_JOBS" in ''|*[!0-9]*) fatal "KEEP_JOBS must be digits, got '$KEEP_JOBS'" ;; esac
[ "$JOB_TIMEOUT_S" -ge 660 ] && [ "$JOB_TIMEOUT_S" -le 3600 ] \
  || fatal "JOB_TIMEOUT_S must be within 660..3600 (the Job's own deadline is 600s), got $JOB_TIMEOUT_S"
[ "$KEEP_JOBS" -ge 1 ] && [ "$KEEP_JOBS" -le 100 ] || fatal "KEEP_JOBS must be within 1..100, got $KEEP_JOBS"
# PUBLISHED_BY lands in a YAML value and a CLI argument: a closed alphabet, no quotes, no whitespace, bounded.
case "$PUBLISHED_BY" in
  *[!A-Za-z0-9._@-]*) fatal "PUBLISHED_BY may contain only letters, digits, . _ - @ (got '$PUBLISHED_BY')" ;;
esac
[ "${#PUBLISHED_BY}" -le 64 ] || fatal "PUBLISHED_BY is longer than 64 characters"
# FILE must be a repository artefact, named plainly (it becomes a ConfigMap key and a mount path).
case "$FILE" in
  scripts/vol-premium/*.json) : ;;
  *) fatal "FILE='$FILE' must be a .json artefact under scripts/vol-premium/" ;;
esac
case "$FILE" in *..*|*' '*) fatal "FILE='$FILE' must not contain '..' or spaces" ;; esac
[ -f "$FILE" ] || fatal "FILE '$FILE' does not exist in this checkout"
FILE_BASENAME="$(basename "$FILE")"
case "$FILE_BASENAME" in *[!A-Za-z0-9._-]*) fatal "FILE basename '$FILE_BASENAME' has characters a ConfigMap key cannot carry" ;; esac
[ -f "$TEMPLATE" ] || fatal "missing Job template $TEMPLATE"
command -v yq >/dev/null 2>&1 || fatal "yq is required"
command -v jq >/dev/null 2>&1 || fatal "jq is required"
command -v python3 >/dev/null 2>&1 || fatal "python3 is required"

# --- 1. the artefact: validated the way the engine validates it, then its identity read ---
echo "=== validating $FILE ($KIND) ==="
bash scripts/ci/validate-vol-premium-calendar.sh "$FILE" \
  || fatal "$FILE does not pass scripts/ci/validate-vol-premium-calendar.sh — nothing is published"
read -r VERSION CONTENT_HASH <<EOF
$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["calendarVersion"], d["calendarContentHash"])' "$FILE")
EOF
[ -n "${VERSION:-}" ] && [ -n "${CONTENT_HASH:-}" ] || fatal "could not read calendarVersion / calendarContentHash from $FILE"
FILE_SHA256="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$FILE")"
echo "artefact: kind=$KIND version=$VERSION content_hash=$CONTENT_HASH file_sha256=$FILE_SHA256"
echo "mode: $([ "$CONFIRM" = true ] && echo 'CONFIRM=true — the publisher WRITES the row' || echo 'CONFIRM=false — DRY RUN, the publisher writes nothing')"

# --- 2. identity AND cluster ---------------------------------------------------------------
WHOAMI="$(kubectl auth whoami -o jsonpath='{.status.userInfo.username}' 2>/dev/null || echo '')"
echo "kubectl identity: ${WHOAMI:-<unknown>}"
[ "$WHOAMI" = "$DEPLOYER" ] || fatal "kubeconfig identity is '${WHOAMI:-<unknown>}', expected '$DEPLOYER'.
       Job and ConfigMap creation is denied for every other principal by the
       options-edge-jenkins-only-workloads admission policy."
API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || echo '')"
echo "api server: ${API_SERVER:-<unknown>}"
[ "$API_SERVER" = "$EXPECTED_API_SERVER" ] \
  || fatal "kubeconfig points at '${API_SERVER:-<unknown>}', expected '$EXPECTED_API_SERVER'.
       The deployer service-account name alone does not identify a cluster."

# --- 3. resolve + digest-pin the ENGINE image ------------------------------------------------
MUTABLE_IMAGE="$(yq -er ".images.\"${IMAGE_KEY}\"" "image-tags/${ENVIRONMENT}.yaml" 2>/dev/null || true)"
[ -n "$MUTABLE_IMAGE" ] && [ "$MUTABLE_IMAGE" != "null" ] \
  || fatal "image-tags/${ENVIRONMENT}.yaml has no '${IMAGE_KEY}' entry"
case "$MUTABLE_IMAGE" in
  *"${IMAGE_REPO_SUFFIX}":*) : ;;
  *) fatal "image-tags/${ENVIRONMENT}.yaml '${IMAGE_KEY}' is '$MUTABLE_IMAGE', which is not a ${IMAGE_REPO_SUFFIX} image" ;;
esac
# FACTS about the target, not defaults to inherit: an agent exporting DEPLOY_PLATFORM=linux/arm64 would otherwise
# pin a digest that cannot run on the prod node.
export DEPLOY_PLATFORM="linux/amd64"
export REGISTRY_SCHEME="http"
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" \
  || fatal "cannot resolve registry digest for $MUTABLE_IMAGE (is the engine image built + pushed for $ENVIRONMENT?)"
case "$PINNED_IMAGE" in
  *@sha256:*) : ;;
  *) fatal "refusing to run a Job on an unpinned image ref: $PINNED_IMAGE" ;;
esac
echo "image: $MUTABLE_IMAGE -> $PINNED_IMAGE"
# STATED, NOT ENFORCED: the tag must already carry the engine build (the one with VolPremiumLedgerPublisher and
# the Postgres ledger). Nothing here can read a jar's contents through a registry; if the tag still holds the
# realised-only service, the container fails on "Could not find or load main class" and step 5 names that.

# --- 4. refuse to start alongside another ledger-publish Job --------------------------------
ACTIVE="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null \
  | jq -r '[.items[] | select((.status.active // 0) > 0) | .metadata.name] | join(" ")' 2>/dev/null || echo '')"
[ -z "$ACTIVE" ] \
  || fatal "another ledger-publish Job is still active ($ACTIVE). Wait for it, or delete it if it is a leftover."

# --- 5. render + create ---------------------------------------------------------------------
CM_NAME="vol-premium-ledger-${KIND}-${FILE_SHA256:0:12}"
JOB_NAME="vol-premium-ledger-publish-$(date -u +%Y%m%d-%H%M%S)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
RENDER="$(mktemp)"
CM_RENDER="$(mktemp)"
LOGS="$(mktemp)"
# The ConfigMap: the repository file, byte for byte, under its own basename. Labelled so step 6 can find it.
kubectl -n "$NAMESPACE" create configmap "$CM_NAME" --from-file="${FILE_BASENAME}=${FILE}" \
  --dry-run=client -o yaml > "$CM_RENDER"
yq -i ".metadata.labels.\"app.kubernetes.io/name\" = \"vol-premium-ledger-file\"
       | .metadata.labels.\"app.kubernetes.io/part-of\" = \"options-edge\"
       | .metadata.labels.\"options-edge.io/ledger-kind\" = \"${KIND}\"" "$CM_RENDER"
[ "$(yq -r ".data | keys | .[]" "$CM_RENDER")" = "$FILE_BASENAME" ] \
  || fatal "the rendered ConfigMap does not carry exactly one key, $FILE_BASENAME"
# Byte identity of what the pod will read is proved INSIDE the container (LEDGER_FILE_SHA256 against the mounted
# file), where the bytes actually are — not inferred here from a YAML round-trip.
sed -e "s|__IMAGE__|${PINNED_IMAGE}|g" \
    -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__KIND__|${KIND}|g" \
    -e "s|__FILE_BASENAME__|${FILE_BASENAME}|g" \
    -e "s|__FILE_SHA256__|${FILE_SHA256}|g" \
    -e "s|__CONFIRM__|${CONFIRM}|g" \
    -e "s|__PUBLISHED_BY__|${PUBLISHED_BY}|g" \
    -e "s|__CONFIGMAP__|${CM_NAME}|g" \
    "$TEMPLATE" >"$RENDER"
grep -q '__[A-Z_]*__' "$RENDER" && fatal "unsubstituted placeholder left in the render"
_ns="$(yq -r '.metadata.namespace' "$RENDER")"
[ "$_ns" = "$NAMESPACE" ] || fatal "template namespace '$_ns' != NAMESPACE '$NAMESPACE'"
# The render carries REFERENCES to the password, never the value; assert that shape before it leaves this host.
grep -q 'POSTGRES_PASSWORD' "$RENDER" && ! grep -qE 'POSTGRES_PASSWORD[[:space:]]*[:=][[:space:]]*[^[:space:]]' "$RENDER" \
  || fatal "the render must reference POSTGRES_PASSWORD by secretKeyRef only"

echo "=== server-side validate (ConfigMap $CM_NAME, Job $JOB_NAME) ==="
kubectl -n "$NAMESPACE" apply --dry-run=server -f "$CM_RENDER" >/dev/null
kubectl -n "$NAMESPACE" create --dry-run=server -f "$RENDER" >/dev/null

echo "=== creating ConfigMap $CM_NAME and Job $JOB_NAME (kind=$KIND version=$VERSION confirm=$CONFIRM) ==="
kubectl -n "$NAMESPACE" apply -f "$CM_RENDER"
# Ownership is claimed BEFORE the create call: the name carries random bytes and has never been used, so a Job
# with that name can only be this invocation's, including when the API server creates it and the response is lost.
JOB_OWNED=true
kubectl -n "$NAMESPACE" create -f "$RENDER"

echo "waiting up to ${JOB_TIMEOUT_S}s for $JOB_NAME ..."
deadline=$(( $(date +%s) + JOB_TIMEOUT_S ))
job_state() { # ONE snapshot per poll, so a completion cannot fall between two reads
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
  sleep 5
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

# The container's own exit code, read from the pod: the publisher's codes are the diagnosis.
EXIT_CODE="$(kubectl -n "$NAMESPACE" get pods -l "job-name=$JOB_NAME" -o json 2>/dev/null \
  | jq -r '[.items[] | (.status.containerStatuses // [])[] | select(.name == "publisher") | .state.terminated.exitCode // empty] | first // ""' 2>/dev/null || echo '')"
echo "publisher exit code: ${EXIT_CODE:-<unknown>}"
if [ "$state" != "succeeded" ]; then
  if grep -q 'Could not find or load main class' "$LOGS"; then
    fatal "$PINNED_IMAGE does not carry VolPremiumLedgerPublisher: the ${IMAGE_KEY} tag still holds the realised-only service, not the engine build. Build and push the engine image first; nothing was published."
  fi
  case "${EXIT_CODE:-}" in
    66) fatal "LEDGER_KEY_CONFLICT: the ledger already holds $KIND $VERSION with DIFFERENT content. A version is immutable once written — publish this content under a NEW version, or stop and find out which artefact is the one that should have been pinned. Nothing was written." ;;
    65) fatal "the publisher REFUSED the artefact (it would not load at boot) — see the log above. Nothing was written." ;;
    64) fatal "publisher usage error — the CLI contract and this wrapper disagree; see the log above. Nothing was written." ;;
    70) fatal "the ledger was unreachable or the write failed and was aborted (exit 70) — see the log above. Re-run once Postgres is reachable; the publisher is idempotent on the same content." ;;
    *)  fatal "$JOB_NAME did not succeed (state=$state, exit ${EXIT_CODE:-unknown}) — see the log above." ;;
  esac
fi

# --- the receipt, anchored and bound to this KIND and VERSION -------------------------------------
if [ "$CONFIRM" = true ]; then
  RECEIPT_RE="^(PUBLISHED|ALREADY_PRESENT)[^A-Za-z0-9_].*${KIND}.*$(printf '%s' "$VERSION" | sed 's/[.[\*^$]/\\&/g')"
else
  RECEIPT_RE="^(PUBLISHABLE|ALREADY_PRESENT)[^A-Za-z0-9_].*${KIND}.*$(printf '%s' "$VERSION" | sed 's/[.[\*^$]/\\&/g')"
fi
RECEIPT="$(grep -E "$RECEIPT_RE" "$LOGS" | tail -1 || true)"
[ -n "$RECEIPT" ] || fatal "$JOB_NAME exited 0 but printed no receipt matching /$RECEIPT_RE/ — refusing to call this a $([ "$CONFIRM" = true ] && echo publish || echo 'clean dry run'). Treat it as a failure: read the log, then re-run."
echo "$RECEIPT"
if [ "$CONFIRM" = true ]; then
  case "$RECEIPT" in
    ALREADY_PRESENT*) echo "OK: the ledger already held $KIND $VERSION with this exact content (content_hash=$CONTENT_HASH) — no row written, nothing to do." ;;
    *)                echo "OK: PUBLISHED row — calendar_version=$VERSION content_hash=$CONTENT_HASH published_by=${PUBLISHED_BY:-<publisher default>} (table vol_premium_calendar_ledger)" ;;
  esac
else
  case "$RECEIPT" in
    ALREADY_PRESENT*) echo "OK: DRY RUN — the ledger already holds $KIND $VERSION with this exact content (content_hash=$CONTENT_HASH); a CONFIRM run would be a no-op." ;;
    *)                echo "OK: DRY RUN — $KIND $VERSION (content_hash=$CONTENT_HASH) is PUBLISHABLE; nothing was written. Re-run with CONFIRM=true to insert the row." ;;
  esac
fi
SUCCESS=true

# --- 6. prune old TERMINAL Jobs and the ConfigMaps nothing references (best-effort, never blocks) -----
TERMINAL="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null \
  | jq -r '[.items[] | select(((.status.succeeded // 0) >= 1) or ([(.status.conditions // [])[] | select(.type == "Failed" and .status == "True")] | length) >= 1)]
           | sort_by(.metadata.creationTimestamp) | .[].metadata.name' 2>/dev/null || echo '')"
COUNT="$(printf '%s\n' "$TERMINAL" | grep -c . || true)"
if [ "${COUNT:-0}" -gt "$KEEP_JOBS" ]; then
  DROP=$(( COUNT - KEEP_JOBS ))
  i=0
  while IFS= read -r old; do
    [ -n "$old" ] || continue
    i=$(( i + 1 ))
    [ "$i" -le "$DROP" ] || break
    echo "pruning old ledger-publish Job $old"
    kubectl -n "$NAMESPACE" delete "job/$old" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  done <<EOF
$TERMINAL
EOF
else
  echo "no old ledger-publish Jobs to prune (terminal=${COUNT:-0}, keep=$KEEP_JOBS)"
fi
REFERENCED="$(kubectl -n "$NAMESPACE" get jobs -l "$JOB_LABEL" -o json 2>/dev/null \
  | jq -r '[.items[].spec.template.spec.volumes[]? | .configMap.name // empty] | unique | .[]' 2>/dev/null || echo '')"
for cm in $(kubectl -n "$NAMESPACE" get configmaps -l "$CM_LABEL" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
  [ "$cm" = "$CM_NAME" ] && continue
  printf '%s\n' "$REFERENCED" | grep -qxF "$cm" && continue
  echo "pruning unreferenced ledger-file ConfigMap $cm"
  kubectl -n "$NAMESPACE" delete "configmap/$cm" --ignore-not-found --wait=false >/dev/null 2>&1 || true
done
