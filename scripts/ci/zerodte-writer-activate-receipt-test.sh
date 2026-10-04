#!/usr/bin/env bash
# The writer activation's RECEIPT and GATE contract (scripts/ops/zerodte-writer-activate.sh, increment 9e): driven through every outcome of the
# CHECK Job, every refusal before it (the committed receipt missing or unbound, the live Deployment absent or on another digest, the lock held
# by a migration / a rollout / another activation), the activation (the identity ConfigMap applied FROM THE RECEIPT, the scale, the rollout,
# the ONE pod Ready on the pinned digest) and the deactivation — against a FAKE kubectl, a FAKE CLOCK, a stub image pinner and a throwaway
# checkout carrying an APPROVED fixture attestation, the dev declaration and a committed receipt bound to it. The Job manifest and the
# ConfigMap the wrapper creates are CAPTURED and asserted. Each case asserts the exit status AND the verdict text.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
W="$T/repo"
mkdir -p "$W/scripts/ops" "$W/scripts/ci" "$W/scripts/deploy" "$W/k8s/jobs" "$W/deploy/zerodte/provisioning" "$W/deploy/zerodte/provisioned" "$W/image-tags" "$W/bin" "$T/ca" "$T/k8s"
cp scripts/ops/zerodte-writer-activate.sh "$W/scripts/ops/"
cp scripts/deploy/zerodte-migrate-barrier.sh "$W/scripts/deploy/"
cp scripts/ci/zerodte_attestation.py scripts/ci/validate-zerodte-provisioning.sh "$W/scripts/ci/"
mkdir -p "$W/scripts/ci/fixtures/zerodte/corpus" && cp scripts/ci/fixtures/zerodte/golden.tsv "$W/scripts/ci/fixtures/zerodte/" && cp scripts/ci/fixtures/zerodte/corpus/* "$W/scripts/ci/fixtures/zerodte/corpus/"
cp k8s/jobs/zerodte-writer-check-job.yaml "$W/k8s/jobs/"
cp deploy/zerodte/provisioning/dev.yaml deploy/zerodte/provisioning/production.yaml "$W/deploy/zerodte/provisioning/"
sed 's/^    approvedBy: UNAPPROVED$/    approvedBy: Test Owner/' deploy/zerodte/virgin-attestation.yaml > "$W/deploy/zerodte/virgin-attestation.yaml"
grep -q "approvedBy: Test Owner" "$W/deploy/zerodte/virgin-attestation.yaml" || { echo "FAIL: the fixture attestation was not approved"; exit 1; }
L=6b2c7c1a-5d3e-4a8f-9b41-2f0d7e9c4a10
LT="$(printf 'c%.0s' $(seq 32))"; PD="$(printf 'b%.0s' $(seq 64))"
receipt() { # receipt <generation> <eraId> <clusterId> → the committed receipt block, in the shape the provisioning wrapper prints
  printf 'environment: dev\nsymbol: SPX\nenvironmentLineageId: %s\nbootstrapKind: VIRGIN\ngeneration: %s\neraId: %s\nledgerTopicId: "%s"\nclusterId: "%s"\nprovisionedDigest: "%s"\nledgerOffset: 0\n' "$L" "$1" "$2" "$LT" "$3" "$PD"
}
receipt 1 1 cluster-dev-A > "$W/deploy/zerodte/provisioned/dev.yaml"
printf 'images:\n  vix-option-inteligence-service: 192.168.100.252:5000/options-edge-vix-option-inteligence:dev\n' > "$W/image-tags/dev.yaml"
printf 'images:\n  vix-option-inteligence-service: 192.168.100.252:5000/options-edge-vix-option-inteligence:prod\n' > "$W/image-tags/production.yaml"
DIGEST="sha256:$(printf 'a%.0s' $(seq 64))"
printf 'pin_ref() { printf "%%s@%s\\n" "${1%%:*}"; }\n' "$DIGEST" > "$W/scripts/deploy/pin-image.sh"
for ca in dev prod other; do
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout "$T/ca/$ca.key" -out "$T/ca/$ca.pem" -subj "/CN=$ca" -days 1 >/dev/null 2>&1
done
fp() { openssl x509 -in "$T/ca/$1.pem" -noout -fingerprint -sha256 | sed 's/^.*=//; s/://g'; }
printf 'clusters:\n  dev:\n    caSha256: "%s"\n    apiServer: ""\n  production:\n    caSha256: "%s"\n    apiServer: "https://192.168.100.252:6443"\n' "$(fp dev)" "$(fp prod)" > "$W/deploy/zerodte/clusters.yaml"
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
git -C "$W" init -q -b main && git -C "$W" add -A && git -C "$W" commit -q -m seed
HEAD="$(git -C "$W" rev-parse HEAD)"
git -C "$W" update-ref refs/remotes/origin/main "$HEAD"
cat > "$W/bin/date" <<'FAKE'
#!/usr/bin/env bash
if [ "$#" -eq 1 ] && [ "$1" = "+%s" ]; then cat "${FAKE_CLOCK:?}"; else exec /bin/date "$@"; fi
FAKE
cat > "$W/bin/sleep" <<'FAKE'
#!/usr/bin/env bash
now="$(cat "${FAKE_CLOCK:?}")"; echo $(( now + ${1:-1} )) > "$FAKE_CLOCK"
FAKE
cat > "$W/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
args="$*"
echo "kubectl $args" >> "${FAKE_JOURNAL:?}"
K="${FAKE_K8S:?}"
case "$args" in
  "auth whoami -o jsonpath={.status.userInfo.username}") printf '%s' "${FAKE_WHOAMI:-system:serviceaccount:options-edge:jenkins-deployer}" ;;
  *"config view --minify -o jsonpath={.clusters[0].cluster.server}") printf '%s' "${FAKE_SERVER:-https://127.0.0.1:1}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority-data}")
    case "${FAKE_CA:-dev}" in none|file) printf '' ;; *) base64 < "${FAKE_CA_DIR:?}/${FAKE_CA:-dev}.pem" | tr -d '\n' ;; esac ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.insecure-skip-tls-verify}") printf '%s' "${FAKE_SKIP_TLS:-}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority}") [ "${FAKE_CA:-dev}" = file ] && printf '/etc/kube/ca.crt' || printf '' ;;
  *'get deployment/zerodte-research-writer -o jsonpath={.spec.template.spec.containers[?(@.name=="zerodte-research-writer")].image}')
    # the FIRST read is before the lock, every later one is under it (the race: FAKE_LIVE_IMAGE_UNDER_LOCK replaces the image from the second read on)
    n="$(grep -c 'containers\[?(@.name==' "$FAKE_JOURNAL")"
    img="${FAKE_LIVE_IMAGE:-pinned}"; [ "$n" -le 1 ] || [ -z "${FAKE_LIVE_IMAGE_UNDER_LOCK:-}" ] || img="$FAKE_LIVE_IMAGE_UNDER_LOCK"
    case "$img" in absent) echo 'Error from server (NotFound): deployments.apps "zerodte-research-writer" not found' >&2; exit 1 ;; nocontainer) printf '' ;; pinned) printf '%s' "host.docker.internal:5001/options-edge-vix-option-inteligence:dev@${FAKE_PINNED_DIGEST:?}" ;; *) printf '%s' "$img" ;; esac ;;
  *"get deployment/zerodte-research-writer -o jsonpath={.spec.replicas}") if [ -f "$K/replicas" ]; then cat "$K/replicas"; else printf '%s' "${FAKE_LIVE_REPLICAS:-0}"; fi ;;
  *"get jobs -l app.kubernetes.io/name=zerodte-writer-check -o json") jobs="${FAKE_JOBS:-}"; [ -n "$jobs" ] || jobs='{"items":[]}'; printf '%s' "$jobs" ;;
  *"get configmap/zerodte-research-migrate-lock"*) [ -f "$K/lock" ] && cat "$K/lock" || { echo 'Error from server (NotFound)' >&2; exit 1; } ;;
  *"delete configmap/zerodte-research-migrate-lock"*) rm -f "$K/lock" ;;
  *"create -f -")
    body="$(cat)"
    if printf '%s' "$body" | grep -q "name: zerodte-research-migrate-lock"; then
      [ -f "$K/lock" ] && { echo 'Error from server (AlreadyExists): configmaps "zerodte-research-migrate-lock" already exists' >&2; exit 1; }
      printf '%s' "$body" | sed -n 's/^ *options-edge.io\/holder: "\(.*\)"$/\1/p' | head -1 > "$K/lock"
    fi ;;
  *"apply --dry-run=server -f "*) f="${args##*-f }"; [ "${FAKE_ADMISSION:-}" != cm ] || { echo "admission webhook denied the ConfigMap" >&2; exit 1; }; [ "$(yq -r '.kind' "$f")" = ConfigMap ] || { echo "admission: not a ConfigMap" >&2; exit 1; } ;;
  *"create --dry-run=server -f "*) f="${args##*-f }"; [ "${FAKE_ADMISSION:-}" != job ] || { echo "admission webhook denied the Job" >&2; exit 1; }; [ "$(yq -r '.kind' "$f")" = Job ] || { echo "admission: not a Job" >&2; exit 1; }; grep -q '__[A-Z_]*__' "$f" && { echo "admission: placeholder" >&2; exit 1; }; : ;;
  *"apply -f "*) f="${args##*-f }"; cp "$f" "$FAKE_JOURNAL.cm.yaml"; cp "$f" "$K/identity-cm.yaml" ;;          # the ConfigMap PERSISTS in the fake cluster
  *"create -f "*) f="${args##*create -f }"; cp "$f" "$FAKE_JOURNAL.job.yaml"; if [ "${FAKE_CREATE_JOB_FAILS:-0}" = 1 ]; then echo "Error from server: admission webhook denied the Job" >&2; exit 1; fi ;;
  *"delete job/"*) if [ "${FAKE_DELETE_JOB_FAILS:-0}" = 1 ]; then echo "error: timed out" >&2; exit 1; fi ;;
  *"get job/"*"-o json")
    if grep -q "delete job/" "$FAKE_JOURNAL" && [ "${FAKE_DELETE_JOB_FAILS:-0}" != 1 ]; then echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1; fi
    case "${FAKE_JOB_GET:-normal}" in error) echo 'Unable to connect to the server: EOF' >&2; exit 1 ;; absent) echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1 ;; active) printf '{"status":{"active":1}}'; exit 0 ;; esac
    case "${FAKE_SUCCEEDED:-1}" in 1) printf '{"status":{"succeeded":1,"conditions":[{"type":"Complete","status":"True"}]}}' ;; partial) printf '{"spec":{"completions":2},"status":{"succeeded":1,"active":1,"conditions":[]}}' ;; *) printf '{"status":{"succeeded":0,"conditions":[{"type":"Failed","status":"True"}]}}' ;; esac ;;
  *"logs job/"*) [ "${FAKE_LOGS_FAIL:-0}" = 1 ] && { echo "container is waiting" >&2; exit 1; }; printf '%b' "${FAKE_LOG:-}" ;;
  *"get pods -l job-name="*) printf '{"items":[{"status":{"containerStatuses":[{"name":"writer-check","state":{"terminated":{"exitCode":%s}}}]}}]}' "${FAKE_EXIT:-0}" ;;
  *"scale deployment/zerodte-research-writer --replicas="*) r="${args##*--replicas=}"; echo "scaled-to=$r" >> "$FAKE_JOURNAL"; printf '%s' "$r" > "$K/replicas" ;;   # the scale PERSISTS
  *"rollout status deployment/zerodte-research-writer"*)
    [ "${FAKE_ROLLOUT_FAILS:-0}" = 1 ] && { echo "error: deployment exceeded its progress deadline" >&2; exit 1; }
    # a replica without the identity ConfigMap cannot start (its env is read BY KEY from it): the rollout never completes
    if [ "$(cat "$K/replicas" 2>/dev/null || echo 0)" = 1 ] && ! [ -f "$K/identity-cm.yaml" ]; then echo "error: deployment exceeded its progress deadline (CreateContainerConfigError: configmap zerodte-writer-identity not found)" >&2; exit 1; fi
    : ;;
  *"get pods -l app.kubernetes.io/name=zerodte-research-writer -o json")
    # EVERY pod of the writer, derived from the fake cluster's state unless a case dictates the listing
    [ "${FAKE_PODS_LIST_FAILS:-0}" = 1 ] && { echo "Unable to connect to the server" >&2; exit 1; }
    ready_pod() { printf '{"metadata":{"name":"%s"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"zerodte-research-writer","imageID":"%s"}]}}' "$1" "$2"; }
    if [ -n "${FAKE_WRITER_PODS:-}" ]; then
      case "$FAKE_WRITER_PODS" in
        ready) printf '{"items":[%s]}' "$(ready_pod w-1 "x@${FAKE_PINNED_DIGEST:?}")" ;;
        bare) printf '{"items":[%s]}' "$(ready_pod w-1 "${FAKE_PINNED_DIGEST:?}")" ;;
        notready) printf '{"items":[{"metadata":{"name":"w-1"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"False"}],"containerStatuses":[{"name":"zerodte-research-writer","imageID":"x@%s"}]}}]}' "${FAKE_PINNED_DIGEST:?}" ;;
        other) printf '{"items":[%s]}' "$(ready_pod w-1 "x@sha256:$(printf 'c%.0s' $(seq 64))")" ;;
        two) printf '{"items":[%s,%s]}' "$(ready_pod w-1 "x@${FAKE_PINNED_DIGEST:?}")" "$(ready_pod w-2 "x@${FAKE_PINNED_DIGEST:?}")" ;;
        ready-plus-pending) printf '{"items":[%s,{"metadata":{"name":"w-2"},"status":{"phase":"Pending","conditions":[],"containerStatuses":[]}}]}' "$(ready_pod w-1 "x@${FAKE_PINNED_DIGEST:?}")" ;;
        ready-plus-deleting) printf '{"items":[%s,{"metadata":{"name":"w-old","deletionTimestamp":"2026-10-04T12:00:00Z"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"zerodte-research-writer","imageID":"x@%s"}]}}]}' "$(ready_pod w-1 "x@${FAKE_PINNED_DIGEST:?}")" "${FAKE_PINNED_DIGEST:?}" ;;
        sidecar-first) printf '{"items":[{"metadata":{"name":"w-1"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"side","imageID":"y@sha256:%s"},{"name":"zerodte-research-writer","imageID":"x@%s"}]}}]}' "$(printf 'c%.0s' $(seq 64))" "${FAKE_PINNED_DIGEST:?}" ;;
        none) printf '{"items":[]}' ;;
      esac
    elif [ -n "${FAKE_PODS_LEFT:-}" ]; then printf '{"items":%s}' "$FAKE_PODS_LEFT"
    elif [ "$(cat "$K/replicas" 2>/dev/null || echo 0)" = 1 ]; then printf '{"items":[%s]}' "$(ready_pod w-1 "x@${FAKE_PINNED_DIGEST:?}")"
    else printf '{"items":[]}'; fi ;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99 ;;
esac
FAKE
chmod +x "$W/bin/kubectl" "$W/bin/date" "$W/bin/sleep"
pass=0; fail=0
run() { # run <name> <want_rc> <want substring> <ACTION> <CONFIRM> <FAKE_LOG> [VAR=value ...]
  local name="$1" want_rc="$2" want="$3" action="$4" confirm="$5" log="$6"; shift 6
  local out rc journal="$T/journal.$RANDOM$RANDOM"
  : > "$journal"; echo 1700000000 > "$journal.clock"
  rm -f "$T/k8s/lock" "$T/k8s/replicas" "$T/k8s/identity-cm.yaml"; local a; for a in "$@"; do case "$a" in FAKE_LOCK_HELD_BY=*) printf '%s' "${a#*=}" > "$T/k8s/lock" ;; esac; done
  out="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE_JOURNAL="$journal" FAKE_CLOCK="$journal.clock" FAKE_K8S="$T/k8s" FAKE_LOG="$log" FAKE_CA_DIR="$T/ca" FAKE_PINNED_DIGEST="$DIGEST" ENVIRONMENT=dev ACTION="$action" CONFIRM="$confirm" BUILD_NUMBER=7 CHECK_RECEIPT="$T/check.receipt" JOB_TIMEOUT_S=420 "$@" bash scripts/ops/zerodte-writer-activate.sh 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then pass=$((pass+1)); echo "  ok   $name (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -8 | sed 's/^/       | /'; fi
  LAST_OUT="$out"; LAST_JOURNAL="$journal"
}
check() { local name="$1"; shift; if "$@"; then pass=$((pass+1)); echo "  ok   $name"; else fail=$((fail+1)); echo "  FAIL $name"; fi; }
no_job() { ! [ -f "$LAST_JOURNAL.job.yaml" ]; }
lock_released() { grep -q "delete configmap/zerodte-research-migrate-lock" "$LAST_JOURNAL"; }
READY="WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-dev-A framesTopicId=$LT position=0 cursorOffset=none logBeginning=0 logEnd=0"
DIAG="mode=CHECK: boot only — nothing consumed, nothing written\n"
echo "--- check ---"
rm -f "$T/check.receipt"
run "check: READY"                                   0 "OK: WRITER_CHECK READY — schema v7, one frames partition, era 1 of generation 1 verified on cluster cluster-dev-A" check false "$DIAG$READY\n"
check "the check wrote this build's receipt" bash -c "grep -q 'build=7 env=dev generation=1 eraId=1 receipt_sha256=[0-9a-f]\{64\} head=$HEAD' '$T/check.receipt'"
check "the check Job was created and the lock released afterwards" bash -c "[ -f '$LAST_JOURNAL.job.yaml' ] && grep -q 'delete configmap/zerodte-research-migrate-lock' '$LAST_JOURNAL'"
check "the Job runs the writer main with --check on the pinned image" bash -c "[ \"\$(yq -r '.spec.template.spec.containers[0].command | join(\" \")' '$LAST_JOURNAL.job.yaml')\" = 'java -cp /app/app.jar com.optionsedge.processing.zerodte.research.ZeroDteResearchWriterMain --check' ] && [ \"\$(yq -r '.spec.template.spec.containers[0].image' '$LAST_JOURNAL.job.yaml')\" = '192.168.100.252:5000/options-edge-vix-option-inteligence@$DIGEST' ]"
check "the Job carries the receipt's identity as LITERALS (eraId 1, generation 1, SPX, the declaration's FRAMES topic)" bash -c "e() { yq -r \".spec.template.spec.containers[0].env[] | select(.name == \\\"\$1\\\") | .value\" '$LAST_JOURNAL.job.yaml'; }; [ \"\$(e ZERO_DTE_ERA_ID)\" = 1 ] && [ \"\$(e ZERO_DTE_PROVISIONING_GENERATION)\" = 1 ] && [ \"\$(e ZERO_DTE_SYMBOL)\" = SPX ] && [ \"\$(e ZERO_DTE_FRAMES_TOPIC)\" = vix-option-inteligence.spx.frames ]"
check "the Job has no envFrom and the password by secretKeyRef only" bash -c "[ \"\$(yq -r '.spec.template.spec.containers[0].envFrom // [] | length' '$LAST_JOURNAL.job.yaml')\" = 0 ] && [ \"\$(yq -r '.spec.template.spec.containers[0].env[] | select(.name == \"POSTGRES_PASSWORD\") | .valueFrom.secretKeyRef.name' '$LAST_JOURNAL.job.yaml')\" = options-edge-runtime-secrets ]"
check "a check applies NO ConfigMap and scales nothing" bash -c "! [ -f '$LAST_JOURNAL.cm.yaml' ] && ! grep -q 'scaled-to=' '$LAST_JOURNAL'"
first_lock="$(grep -n 'create -f -' "$LAST_JOURNAL" | head -1 | cut -d: -f1)"; create="$(grep -n 'create -f /' "$LAST_JOURNAL" | head -1 | cut -d: -f1)"
check "the lock is taken BEFORE the check Job is created ($first_lock < $create)" test "$first_lock" -lt "$create"
run "check: READY with a cursor"                     0 "the cursor recovered (position 9, cursor 8, the log [3, 12))" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-dev-A framesTopicId=$LT position=9 cursorOffset=8 logBeginning=3 logEnd=12\n"
run "check: READY by recovery (b) on a non-empty log (a cursor recovered from the newest feature row)" 0 "OK: WRITER_CHECK READY" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-dev-A framesTopicId=$LT position=5 cursorOffset=4 logBeginning=0 logEnd=5\n"
run "check: a BOOTSTRAP on a NON-EMPTY log (no cursor, frames nobody persisted)" 1 "would BOOTSTRAP (no cursor, no feature row of this log) on a log that is not empty [0, 5)" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-dev-A framesTopicId=$LT position=0 cursorOffset=none logBeginning=0 logEnd=5\n"
run "check: a bootstrap on an empty log at a non-zero beginning (retention trimmed everything)" 0 "OK: WRITER_CHECK READY" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-dev-A framesTopicId=$LT position=7 cursorOffset=none logBeginning=7 logEnd=7\n"
echo "--- the check's refusals, each named ---"
rm -f "$T/check.receipt"
run "check: REFUSED schema"                          1 "run Jenkinsfile.zerodte-research-migrate (dry run, then CONFIRM) first" check false "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_SCHEMA_VERSION exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "check: REFUSED era"                             1 "run Jenkinsfile.zerodte-provision (dry run, then CONFIRM) and commit ITS receipt" check false "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_ERA_MISSING exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "check: REFUSED topic"                           1 "the frames topic has not exactly one partition" check false "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_TOPIC_INVALID exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "check: REFUSED calendar"                        1 "a new reviewed migration ships the calendar" check false "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_CALENDAR_COVERAGE exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "check: REFUSED offset lost"                     1 "an inconsistent store and log; nothing is activated" check false "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_OFFSET_LOST exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "check: REFUSED another code"                    1 "REFUSED with code STORE_CONFLICT" check false "${DIAG}WRITER_CHECK REFUSED code=STORE_CONFLICT exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "check: UNAVAILABLE"                             1 "could not reach the store or the broker (UNAVAILABLE, 69)" check false "${DIAG}WRITER_CHECK UNAVAILABLE exit=69\n" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "check: USAGE"                                   1 "refused its invocation (USAGE, 64)" check false "WRITER_CHECK USAGE exit=64\n" FAKE_SUCCEEDED=0 FAKE_EXIT=64
check "no refused check wrote a receipt" test ! -e "$T/check.receipt"
echo "--- the receipt held to the letter and BOUND to the committed receipt ---"
run "READY with another eraId"                       1 "the writer verified era 2 of generation 1, the committed receipt names era 1 of generation 1" check false "${DIAG}WRITER_CHECK READY eraId=2 generation=1 clusterId=cluster-dev-A framesTopicId=$LT position=0 cursorOffset=none logBeginning=0 logEnd=0\n"
run "READY with another generation"                  1 "the writer verified era 1 of generation 2" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=2 clusterId=cluster-dev-A framesTopicId=$LT position=0 cursorOffset=none logBeginning=0 logEnd=0\n"
run "READY on another cluster"                       1 "the writer runs against cluster 'cluster-other', the committed receipt names 'cluster-dev-A'" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-other framesTopicId=$LT position=0 cursorOffset=none logBeginning=0 logEnd=0\n"
run "READY with its tokens reordered"                1 "is not a WRITER_CHECK line in its canonical grammar" check false "${DIAG}WRITER_CHECK READY generation=1 eraId=1 clusterId=cluster-dev-A framesTopicId=$LT position=0 cursorOffset=none logBeginning=0 logEnd=0\n"
run "READY without the log bounds (the pre-r3 grammar)" 1 "is not a WRITER_CHECK line in its canonical grammar" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-dev-A framesTopicId=$LT position=0 cursorOffset=none\n"
run "READY with an extra token"                      1 "is not a WRITER_CHECK line in its canonical grammar" check false "$DIAG$READY extra=1\n"
run "READY with a short topic id"                    1 "is not a WRITER_CHECK line in its canonical grammar" check false "${DIAG}WRITER_CHECK READY eraId=1 generation=1 clusterId=cluster-dev-A framesTopicId=abc position=0 cursorOffset=none logBeginning=0 logEnd=0\n"
run "READY but the container exited 1"               1 "the receipt and the process disagree" check false "$DIAG$READY\n" FAKE_EXIT=1
run "REFUSED but the container exited 0"             1 "the receipt says REFUSED (68) but the container exited '0'" check false "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_ERA_MISSING exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=0
run "no receipt line"                                1 "printed 0 receipt line(s)" check false "$DIAG" FAKE_SUCCEEDED=0 FAKE_EXIT=1
run "two receipt lines"                              1 "printed 2 receipt line(s)" check false "$DIAG$READY\n$READY\n"
run "an image without the writer"                    1 "does not carry ZeroDteResearchWriterMain" check false "Error: Could not find or load main class com.optionsedge.processing.zerodte.research.ZeroDteResearchWriterMain\n" FAKE_SUCCEEDED=0 FAKE_EXIT=1
run "the Job's log cannot be read"                   1 "could not be read in 5 attempts" check false "$DIAG$READY\n" FAKE_LOGS_FAIL=1
run "the client stops waiting on an active Job"      1 "is still active after 420s" check false "$DIAG$READY\n" FAKE_JOB_GET=active
echo "--- before the Job: the committed receipt, the live Deployment, the lock ---"
mv "$W/deploy/zerodte/provisioned/dev.yaml" "$T/receipt.away"
run "no committed receipt"                           1 "no committed provisioning receipt at deploy/zerodte/provisioned/dev.yaml" check false "$DIAG$READY\n"
mv "$T/receipt.away" "$W/deploy/zerodte/provisioned/dev.yaml"
commit_receipt() { git -C "$W" add -- deploy/zerodte >/dev/null && git -C "$W" commit -q -m "$1"; }   # only the zerodte files: the fakes under bin/ stay untracked, so a reset to the seed keeps them
restore_seed() { git -C "$W" reset -q --hard "$HEAD"; }
receipt 2 1 cluster-dev-A > "$W/deploy/zerodte/provisioned/dev.yaml"; commit_receipt "another generation"
run "a COMMITTED receipt not bound to the declaration (another generation)" 1 "do not pass scripts/ci/validate-zerodte-provisioning.sh" check false "$DIAG$READY\n"
restore_seed
receipt 1 1 "" > "$W/deploy/zerodte/provisioned/dev.yaml"; commit_receipt "empty clusterId"
run "a COMMITTED receipt with an empty clusterId" 1 "do not pass scripts/ci/validate-zerodte-provisioning.sh" check false "$DIAG$READY\n"
restore_seed
# a DIRTY but VALID receipt: the worktree names another cluster (the validator binds no cluster — it would pass), nothing committed → refused before the validator, before any lock
receipt 1 1 cluster-dev-B > "$W/deploy/zerodte/provisioned/dev.yaml"
run "a dirty-but-valid receipt in the worktree (another clusterId, uncommitted)" 1 "deploy/zerodte/provisioned/dev.yaml differs from HEAD — only the COMMITTED receipt" check false "$DIAG$READY\n"
check "… refused before the validator, before the lock, before any Job" bash -c "! printf '%s' \"\$1\" | grep -q 'validating deploy/zerodte' && ! grep -q 'create -f -' '$LAST_JOURNAL' && ! [ -f '$LAST_JOURNAL.job.yaml' ]" _ "$LAST_OUT"
restore_seed
cp "$W/deploy/zerodte/provisioning/dev.yaml" "$T/decl.away"; printf '# touched\n' >> "$W/deploy/zerodte/provisioning/dev.yaml"
run "a dirty declaration in the worktree"            1 "deploy/zerodte/provisioning/dev.yaml differs from HEAD" check false "$DIAG$READY\n"
restore_seed
git -C "$W" rm -q --cached deploy/zerodte/provisioned/dev.yaml
run "an untracked receipt"                           1 "is not tracked by git" check false "$DIAG$READY\n"
restore_seed
check "the seed is restored (the receipt bound again)" bash -c "cd '$W' && git diff --quiet HEAD && [ \"\$(git rev-parse HEAD)\" = '$HEAD' ]"
run "the live Deployment does not exist"             1 "deploy the zerodte-research-writer service slice first" check false "$DIAG$READY\n" FAKE_LIVE_IMAGE=absent
run "the live Deployment runs another digest"        1 "not the pinned digest" check false "$DIAG$READY\n" FAKE_LIVE_IMAGE="host.docker.internal:5001/options-edge-vix-option-inteligence:dev@sha256:$(printf 'c%.0s' $(seq 64))"
check "… refused before the lock and before any Job" bash -c "! grep -q 'create -f -' '$LAST_JOURNAL' && ! [ -f '$LAST_JOURNAL.job.yaml' ]"
run "the live Deployment has no container named zerodte-research-writer" 1 "has no container named zerodte-research-writer" check false "$DIAG$READY\n" FAKE_LIVE_IMAGE=nocontainer
run "the image replaced by a rollout between the pre-lock read and the lock" 1 "under the lock the live Deployment zerodte-research-writer runs" check false "$DIAG$READY\n" FAKE_LIVE_IMAGE_UNDER_LOCK="host.docker.internal:5001/options-edge-vix-option-inteligence:dev@sha256:$(printf 'c%.0s' $(seq 64))"
check "… the lock was taken, no Job created, the lock released" bash -c "grep -q 'create -f -' '$LAST_JOURNAL' && ! [ -f '$LAST_JOURNAL.job.yaml' ] && grep -q 'delete configmap/zerodte-research-migrate-lock' '$LAST_JOURNAL'"
run "check while the writer already has a replica (allowed: a check is read-only)" 0 "OK: WRITER_CHECK READY" check false "$DIAG$READY\n" FAKE_LIVE_REPLICAS=1
run "the lock is held by a migration"                1 "the exclusion lock could not be taken" check false "$DIAG$READY\n" FAKE_LOCK_HELD_BY=research-migrate-build-3-20261003T120000Z
check "… no Job, the holder named" bash -c "$(declare -f no_job); LAST_JOURNAL='$LAST_JOURNAL'; no_job && printf '%s' \"\$1\" | grep -q 'is held by research-migrate-build-3'" _ "$LAST_OUT"
run "the lock is held by a service rollout"          1 "the exclusion lock could not be taken" check false "$DIAG$READY\n" FAKE_LOCK_HELD_BY=service-deploy-vix-option-inteligence-dev-build-12
run "an active check Job already"                    1 "another zerodte-writer-check Job is not terminal" check false "$DIAG$READY\n" FAKE_JOBS='{"items":[{"metadata":{"name":"zerodte-writer-check-x"},"status":{"active":1}}]}'
run "Job create refused, API unreadable, delete fails" 1 "RETAINED" check false "$DIAG$READY\n" FAKE_CREATE_JOB_FAILS=1 FAKE_JOB_GET=error FAKE_DELETE_JOB_FAILS=1
echo "--- activate ---"
rm -f "$T/check.receipt"; run "check (the receipt for the activation below)" 0 "OK: WRITER_CHECK READY" check false "$DIAG$READY\n"
run "activate: the check again, the identity ConfigMap, the scale, one pod Ready on the digest" 0 "OK: ACTIVATED the dedicated v7 research writer on dev" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD"
check "the check Job ran AGAIN before the effect" test -f "$LAST_JOURNAL.job.yaml"
check "the identity ConfigMap was applied FROM THE RECEIPT (eraId 1, generation 1, the lineage, the cluster, the ledger topic id)" bash -c "d() { yq -r \".data.\$1\" '$LAST_JOURNAL.cm.yaml'; }; [ \"\$(yq -r '.metadata.name' '$LAST_JOURNAL.cm.yaml')\" = zerodte-writer-identity ] && [ \"\$(d eraId)\" = 1 ] && [ \"\$(d provisioningGeneration)\" = 1 ] && [ \"\$(d environmentLineageId)\" = $L ] && [ \"\$(d clusterId)\" = cluster-dev-A ] && [ \"\$(d ledgerTopicId)\" = $LT ] && [ \"\$(d symbol)\" = SPX ]"
check "the ORDER: check Job → ConfigMap → scale 1 → rollout → pods; the lock released last" bash -c "j='$LAST_JOURNAL'; a=\$(grep -n 'create -f /' \$j | tail -1 | cut -d: -f1); b=\$(grep -n 'apply -f /' \$j | tail -1 | cut -d: -f1); c=\$(grep -n 'scaled-to=1' \$j | cut -d: -f1); d=\$(grep -n 'rollout status' \$j | cut -d: -f1); e=\$(grep -n 'get pods -l app.kubernetes.io/name=zerodte-research-writer -o json' \$j | tail -1 | cut -d: -f1); f=\$(grep -n 'delete configmap/zerodte-research-migrate-lock' \$j | tail -1 | cut -d: -f1); [ \$a -lt \$b ] && [ \$b -lt \$c ] && [ \$c -lt \$d ] && [ \$d -lt \$e ] && [ \$e -lt \$f ] && [ \$f = \$(wc -l < \$j | tr -d ' ') ]"
check "the fake cluster's state after the activation: the identity ConfigMap persisted, the Deployment at 1, one pod derived from it" bash -c "[ -f '$T/k8s/identity-cm.yaml' ] && [ \"\$(cat '$T/k8s/replicas')\" = 1 ] && [ \"\$(yq -r '.data.eraId' '$T/k8s/identity-cm.yaml')\" = 1 ]"
check "the live image was read once BEFORE the lock and re-read once UNDER it, right before the check Job" bash -c "j='$LAST_JOURNAL'; l=\$(grep -n 'create -f -' \$j | head -1 | cut -d: -f1); c=\$(grep -n 'create -f /' \$j | head -1 | cut -d: -f1); before=0; under=0; for r in \$(grep -n 'containers\[?(@.name==' \$j | cut -d: -f1); do if [ \$r -lt \$l ]; then before=\$((before+1)); elif [ \$r -lt \$c ]; then under=\$((under+1)); fi; done; [ \$before = 1 ] && [ \$under = 1 ]"
rm -f "$T/check.receipt"
run "activate without this build's check receipt"   1 "no check receipt at" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD"
printf 'build=6 env=dev generation=1 eraId=1 receipt_sha256=x head=%s\n' "$HEAD" > "$T/check.receipt"
run "activate with another build's check receipt"    1 "does not describe this activation" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD"
rm -f "$T/check.receipt"; run "check (again)" 0 "OK: WRITER_CHECK READY" check false "$DIAG$READY\n"
run "activate: the check REFUSED right before the effect" 1 "run Jenkinsfile.zerodte-provision" activate true "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_ERA_MISSING exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
check "… nothing applied, nothing scaled" bash -c "! [ -f '$LAST_JOURNAL.cm.yaml' ] && ! grep -q 'scaled-to=' '$LAST_JOURNAL'"
run "activate: the rollout fails"                    1 "the rollout of zerodte-research-writer did not complete" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_ROLLOUT_FAILS=1
run "activate: the one pod is not Ready"             1 "of which 0 Running and Ready on the pinned digest" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=notready
run "activate: the pod runs another digest"          1 "of which 0 Running and Ready on the pinned digest" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=other
run "activate: two pods"                             1 "has 2 pod(s) (every phase, deleting ones excluded)" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=two
run "activate: no pod"                               1 "has 0 pod(s)" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=none
run "activate: one Ready pod PLUS a Pending one (every phase counts)" 1 "has 2 pod(s) (every phase, deleting ones excluded) of which 1 Running and Ready" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=ready-plus-pending
run "activate: one Ready pod plus a DELETING old one (excluded)" 0 "OK: ACTIVATED" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=ready-plus-deleting
run "activate: the pod's imageID in the bare sha256 form" 0 "OK: ACTIVATED" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=bare
run "activate: a sidecar listed first; the NAMED writer container judged" 0 "OK: ACTIVATED" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_WRITER_PODS=sidecar-first
run "activate while the Deployment already has a replica" 1 "an activation starts from zero; a replica from outside this pipeline is refused" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_LIVE_REPLICAS=1
check "… refused before the lock and before any Job" bash -c "! grep -q 'create -f -' '$LAST_JOURNAL' && ! [ -f '$LAST_JOURNAL.job.yaml' ]"
run "activate: the identity ConfigMap render refused server-side" 1 "the identity ConfigMap render does not validate server-side" activate true "$DIAG$READY\n" PERMITTED_SHA="$HEAD" FAKE_ADMISSION=cm
check "… no Job, nothing scaled (the ConfigMap is validated before the lock)" bash -c "! [ -f '$LAST_JOURNAL.job.yaml' ] && ! grep -q 'scaled-to=' '$LAST_JOURNAL'"
run "activate without CONFIRM"                       1 "ACTION=activate needs CONFIRM=true" activate false "$DIAG$READY\n" PERMITTED_SHA="$HEAD"
run "activate with another HEAD"                     1 "is not the permitted commit" activate true "$DIAG$READY\n" PERMITTED_SHA="$(printf '0%.0s' $(seq 40))"
run "activate without PERMITTED_SHA"                 1 "needs PERMITTED_SHA" activate true "$DIAG$READY\n"
echo "--- deactivate ---"
run "deactivate: scale 0, pods gone"                 0 "OK: DEACTIVATED the dedicated v7 research writer on dev" deactivate true "" PERMITTED_SHA="$HEAD"
check "no check Job for a deactivation; scaled to 0 under the lock; the lock released" bash -c "! [ -f '$LAST_JOURNAL.job.yaml' ] && grep -q 'scaled-to=0' '$LAST_JOURNAL' && grep -q 'create -f -' '$LAST_JOURNAL' && grep -q 'delete configmap/zerodte-research-migrate-lock' '$LAST_JOURNAL'"
run "deactivate: pods still present after the wait"  1 "are still present after 300s" deactivate true "" PERMITTED_SHA="$HEAD" FAKE_PODS_LEFT='[{"metadata":{"name":"w-1"}}]'
run "deactivate: the pod listing cannot be read"     1 "could not be listed — the Deployment is at 0 but the drain cannot be declared" deactivate true "" PERMITTED_SHA="$HEAD" FAKE_PODS_LIST_FAILS=1
run "deactivate without CONFIRM"                     1 "ACTION=deactivate needs CONFIRM=true" deactivate false ""
run "deactivate with another HEAD"                   1 "is not the permitted commit" deactivate true "" PERMITTED_SHA="$(printf '0%.0s' $(seq 40))"
echo "--- parameters, identity, cluster ---"
run "ACTION unknown"                                 1 "ACTION must be check, activate or deactivate" nothing false ""
run "CONFIRM neither true nor false"                 1 "CONFIRM must be true or false" check maybe ""
run "KEEP_JOBS zero"                                 1 "KEEP_JOBS must be within 1..100" check false "" KEEP_JOBS=0
run "KEEP_JOBS not digits"                           1 "KEEP_JOBS must be digits" check false "" KEEP_JOBS=abc
run "ENVIRONMENT unset"                              1 "ENVIRONMENT must be dev or production" check false "" ENVIRONMENT=
run "JOB_TIMEOUT_S below the Job's deadline"         1 "JOB_TIMEOUT_S must be within 330..1800" check false "" JOB_TIMEOUT_S=200
run "ROLLOUT_TIMEOUT_S out of range"                 1 "ROLLOUT_TIMEOUT_S must be within 60..1800" check false "" ROLLOUT_TIMEOUT_S=30
run "another kubectl identity"                       1 "kubeconfig identity is" check false "$DIAG$READY\n" FAKE_WHOAMI=system:admin
run "another cluster's CA"                           1 "is not the pinned dev cluster's" check false "$DIAG$READY\n" FAKE_CA=other
run "no CA data in the kubeconfig"                   1 "carries no certificate-authority-data" check false "$DIAG$READY\n" FAKE_CA=none
run "the CA named by file path"                      1 "names its CA by file path" check false "$DIAG$READY\n" FAKE_CA=file
run "insecure-skip-tls-verify"                       1 "insecure-skip-tls-verify: true" check false "$DIAG$READY\n" FAKE_SKIP_TLS=true
mv "$W/k8s/jobs/zerodte-writer-check-job.yaml" "$T/template.away"
run "the Job template is missing"                    1 "missing Job template" check false "$DIAG$READY\n"
mv "$T/template.away" "$W/k8s/jobs/zerodte-writer-check-job.yaml"
echo "--- the Job's states, the admission, the grammar, the pruning ---"
run "the Job's state cannot be read (UNKNOWN)"       1 "could not be read (UNKNOWN)" check false "$DIAG$READY\n" FAKE_JOB_GET=error
run "the Job vanished (ABSENT)"                      1 "could not be read (ABSENT)" check false "$DIAG$READY\n" FAKE_JOB_GET=absent
run "the Job render refused server-side"             1 "the check Job render does not validate server-side" check false "$DIAG$READY\n" FAKE_ADMISSION=job
check "… nothing created, the lock released" bash -c "! [ -f '$LAST_JOURNAL.job.yaml' ] && grep -q 'delete configmap/zerodte-research-migrate-lock' '$LAST_JOURNAL'"
run "REFUSED with a lowercase code"                  1 "is not a WRITER_CHECK line in its canonical grammar" check false "${DIAG}WRITER_CHECK REFUSED code=research_era_missing exit=68\n" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "REFUSED with another exit"                      1 "is not a WRITER_CHECK line in its canonical grammar" check false "${DIAG}WRITER_CHECK REFUSED code=RESEARCH_ERA_MISSING exit=67\n" FAKE_SUCCEEDED=0 FAKE_EXIT=67
run "UNAVAILABLE with an extra token"                1 "is not a WRITER_CHECK line in its canonical grammar" check false "${DIAG}WRITER_CHECK UNAVAILABLE exit=69 reason=db\n" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "USAGE with another exit"                        1 "is not a WRITER_CHECK line in its canonical grammar" check false "WRITER_CHECK USAGE exit=65\n" FAKE_SUCCEEDED=0 FAKE_EXIT=65
OLD_JOBS='{"items":[{"metadata":{"name":"zerodte-writer-check-a","creationTimestamp":"2026-10-01T00:00:00Z"},"status":{"succeeded":1,"conditions":[{"type":"Complete","status":"True"}]}},{"metadata":{"name":"zerodte-writer-check-b","creationTimestamp":"2026-10-02T00:00:00Z"},"status":{"succeeded":0,"conditions":[{"type":"Failed","status":"True"}]}},{"metadata":{"name":"zerodte-writer-check-c","creationTimestamp":"2026-10-03T00:00:00Z"},"status":{"succeeded":1,"conditions":[{"type":"Complete","status":"True"}]}}]}'
run "pruning: three terminal Jobs, KEEP_JOBS=1"      0 "OK: WRITER_CHECK READY" check false "$DIAG$READY\n" KEEP_JOBS=1 FAKE_JOBS="$OLD_JOBS"
check "… the two OLDEST terminal Jobs deleted, the newest kept" bash -c "grep -q 'delete job/zerodte-writer-check-a' '$LAST_JOURNAL' && grep -q 'delete job/zerodte-writer-check-b' '$LAST_JOURNAL' && ! grep -q 'delete job/zerodte-writer-check-c' '$LAST_JOURNAL' || { echo '       journal deletes:'; grep 'delete job\|pruning\|no old' '$LAST_JOURNAL' | sed 's/^/       | /'; printf '%s\n' \"\$1\" | grep 'prun\|no old' | sed 's/^/       out: /'; exit 1; }" _ "$LAST_OUT"
PARTIAL_JOB='{"items":[{"metadata":{"name":"zerodte-writer-check-p","creationTimestamp":"2026-10-01T00:00:00Z"},"spec":{"completions":2},"status":{"succeeded":1,"active":1,"conditions":[]}}]}'
run "a multi-completion check Job with one success and no terminal condition is NOT terminal (the inventory refuses)" 1 "another zerodte-writer-check Job is not terminal (zerodte-writer-check-p)" check false "$DIAG$READY\n" FAKE_JOBS="$PARTIAL_JOB"
run "this run's Job reports one success but no terminal condition: not a verdict" 1 "is still active after 420s" check false "$DIAG$READY\n" FAKE_SUCCEEDED=partial
echo "zerodte-writer-activate receipt + gate contract: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-writer-activate-receipt-test: OK ==="; exit 0; }
echo "=== zerodte-writer-activate-receipt-test: FAILED ==="; exit 1
