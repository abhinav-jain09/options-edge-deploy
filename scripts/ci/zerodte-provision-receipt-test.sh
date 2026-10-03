#!/usr/bin/env bash
# The wrapper's RECEIPT contract (scripts/ops/zerodte-provision.sh): the provisioner's ONE outcome line is the only evidence this identity
# has, so the wrapper is driven here through every outcome — against a FAKE kubectl that answers each call the wrapper makes (identity,
# cluster CA, lock, inventory, render, create, wait, log, exit code) and a stub image pinner — from a throwaway copy of the repository (a git
# checkout with an origin/main ref, so HEAD and the append-only base exist; an APPROVED attestation, since the shipped one is refused on
# its UNAPPROVED marker by design; cluster pins for CERTIFICATES MINTED HERE, so the CA pipeline — kubeconfig data → base64 → openssl
# fingerprint → pin — runs for real). Each case asserts the exit status AND the verdict text, so a refusal for the wrong reason is a failure too.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
W="$T/repo"
mkdir -p "$W/scripts/ops" "$W/scripts/ci" "$W/scripts/deploy" "$W/k8s/jobs" "$W/deploy/zerodte/provisioning" "$W/deploy/zerodte/provisioned" "$W/image-tags" "$W/bin" "$T/ca"
cp scripts/ops/zerodte-provision.sh "$W/scripts/ops/"
cp scripts/ci/zerodte_attestation.py scripts/ci/validate-zerodte-provisioning.sh scripts/ci/validate-zerodte-attestation.sh "$W/scripts/ci/"
mkdir -p "$W/scripts/ci/fixtures/zerodte/corpus" && cp scripts/ci/fixtures/zerodte/golden.tsv "$W/scripts/ci/fixtures/zerodte/" && cp scripts/ci/fixtures/zerodte/corpus/* "$W/scripts/ci/fixtures/zerodte/corpus/"
cp k8s/jobs/zerodte-provision-job.yaml "$W/k8s/jobs/"
cp deploy/zerodte/provisioning/dev.yaml deploy/zerodte/provisioning/production.yaml "$W/deploy/zerodte/provisioning/"
# the shipped attestation is REFUSED on its UNAPPROVED marker (by design, until the owner's edit); this throwaway carries an approved copy
sed 's/^    approvedBy: UNAPPROVED$/    approvedBy: Test Owner/' deploy/zerodte/virgin-attestation.yaml > "$W/deploy/zerodte/virgin-attestation.yaml"
grep -q "approvedBy: Test Owner" "$W/deploy/zerodte/virgin-attestation.yaml" || { echo "FAIL: the fixture attestation was not approved"; exit 1; }
printf 'images:\n  vix-option-inteligence-service: 192.168.100.252:5000/options-edge-vix-option-inteligence:dev\n' > "$W/image-tags/dev.yaml"
printf 'images:\n  vix-option-inteligence-service: 192.168.100.252:5000/options-edge-vix-option-inteligence:prod\n' > "$W/image-tags/production.yaml"
# the clusters: CERTIFICATES MINTED HERE stand in for the dev and production CAs; a third one is "some other cluster"
for ca in dev prod other; do
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout "$T/ca/$ca.key" -out "$T/ca/$ca.pem" -subj "/CN=$ca" -days 1 >/dev/null 2>&1
done
fp() { openssl x509 -in "$T/ca/$1.pem" -noout -fingerprint -sha256 | sed 's/^.*=//; s/://g'; }
printf 'clusters:\n  dev:\n    caSha256: "%s"\n    apiServer: ""\n  production:\n    caSha256: "%s"\n    apiServer: "https://192.168.100.252:6443"\n' "$(fp dev)" "$(fp prod)" > "$W/deploy/zerodte/clusters.yaml"
# the image pinner stub: a digest-pinned ref, no registry
printf 'pin_ref() { printf "%%s@sha256:%s\\n" "${1%%:*}"; }\n' "$(printf 'a%.0s' $(seq 64))" > "$W/scripts/deploy/pin-image.sh"
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
git -C "$W" init -q -b main && git -C "$W" add -A && git -C "$W" commit -q -m seed
HEAD="$(git -C "$W" rev-parse HEAD)"
git -C "$W" update-ref refs/remotes/origin/main "$HEAD"   # the append-only base the attestation validator refuses to run without
# the fake kubectl: every call the wrapper makes, answered from FAKE_* variables; mutations are journaled
cat > "$W/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
args="$*"
echo "kubectl $args" >> "${FAKE_JOURNAL:?}"
case "$args" in
  "auth whoami -o jsonpath={.status.userInfo.username}") printf '%s' "${FAKE_WHOAMI:-system:serviceaccount:options-edge:jenkins-deployer}" ;;
  *"config view --minify -o jsonpath={.clusters[0].cluster.server}") printf '%s' "${FAKE_SERVER:-https://127.0.0.1:1}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority-data}")
    case "${FAKE_CA:-dev}" in none|file) printf '' ;; *) base64 < "${FAKE_CA_DIR:?}/${FAKE_CA:-dev}.pem" | tr -d '\n' ;; esac ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.insecure-skip-tls-verify}") printf '%s' "${FAKE_SKIP_TLS:-}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority}") [ "${FAKE_CA:-dev}" = file ] && printf '/etc/kube/ca.crt' || printf '' ;;
  *"get configmap/zerodte-provision-lock"*) printf 'build-3-20261003T120000Z' ;;
  *"create -f -")
    body="$(cat)"
    if [ "${FAKE_LOCK_HELD:-0}" = 1 ] && printf '%s' "$body" | grep -q "name: zerodte-provision-lock"; then
      echo 'Error from server (AlreadyExists): configmaps "zerodte-provision-lock" already exists' >&2; exit 1
    fi ;;
  *"get jobs -l app.kubernetes.io/name=zerodte-provision -o json") jobs="${FAKE_JOBS:-}"; [ -n "$jobs" ] || jobs='{"items":[]}'; printf '%s' "$jobs" ;;
  *"create configmap"*"--dry-run=client -o yaml")
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: options-edge\ndata:\n'
    for a in "$@"; do case "$a" in --from-file=*) k="${a#--from-file=}"; printf '  %s: |\n    x\n' "${k%%=*}" ;; esac; done ;;
  *"create -f "*)   # the Job
    if [ "${FAKE_CREATE_JOB_FAILS:-0}" = 1 ]; then echo "Error from server: admission webhook denied the Job" >&2; exit 1; fi ;;
  *"delete job/"*)
    if [ "${FAKE_DELETE_JOB_FAILS:-0}" = 1 ]; then echo "error: timed out waiting for the condition" >&2; exit 1; fi ;;
  *"apply --dry-run=server"*|*"create --dry-run=server"*|*"apply -f"*|*"delete"*) : ;;
  *"get job/"*"-o json")
    # FAKE_JOB_GET: normal (default) | absent (NotFound) | error (unreadable) | active; after a successful delete the Job is ABSENT
    if grep -q "delete job/" "$FAKE_JOURNAL" && [ "${FAKE_DELETE_JOB_FAILS:-0}" != 1 ]; then echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1; fi
    case "${FAKE_JOB_GET:-normal}" in
      absent) echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1 ;;
      error)  echo 'Unable to connect to the server: EOF' >&2; exit 1 ;;
      active) printf '{"status":{"active":1,"succeeded":0,"conditions":[]}}' ;;
    esac
    # a Job whose container exited non-zero is FAILED (backoffLimit 0): the fake reports that condition unless a case says otherwise
    if [ "${FAKE_SUCCEEDED:-1}" = 1 ]; then printf '{"status":{"succeeded":1,"conditions":[]}}'; else cond="${FAKE_CONDITIONS:-}"; [ -n "$cond" ] || cond='{"type":"Failed","status":"True"}'; printf '{"status":{"succeeded":0,"conditions":[%s]}}' "$cond"; fi ;;
  *"logs job/"*) printf '%b' "${FAKE_LOG:-}" ;;
  *"get pods -l job-name="*) printf '{"items":[{"status":{"containerStatuses":[{"name":"provisioner","state":{"terminated":{"exitCode":%s}}}]}}]}' "${FAKE_EXIT:-0}" ;;
  *"get configmaps"*) printf '' ;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99 ;;
esac
FAKE
chmod +x "$W/bin/kubectl"
pass=0; fail=0
run() { # run <name> <want_rc> <want substring> <CONFIRM> <FAKE_LOG> [VAR=value ...]
  local name="$1" want_rc="$2" want="$3" confirm="$4" log="$5"; shift 5
  local out rc journal="$T/journal.$RANDOM"
  : > "$journal"
  out="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE_JOURNAL="$journal" FAKE_LOG="$log" FAKE_CA_DIR="$T/ca" ENVIRONMENT=dev CONFIRM="$confirm" BUILD_NUMBER=7 DRY_RUN_RECEIPT="$T/receipt" JOB_TIMEOUT_S=660 "$@" bash scripts/ops/zerodte-provision.sh 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then pass=$((pass+1)); echo "  ok   $name (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -6 | sed 's/^/       | /'; fi
  LAST_OUT="$out"; LAST_JOURNAL="$journal"
}
L=6b2c7c1a-5d3e-4a8f-9b41-2f0d7e9c4a10
PD="$(printf 'b%.0s' $(seq 64))"; LT="$(printf 'c%.0s' $(seq 32))"
PENDING="PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=FRAMES,HEAD,CURRENT,PULSE,DEPLOYMENTS wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED"
RESOLVED="PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=$PD topicIds=RESOLVED wouldCreate=NONE wouldAssert=$PD wouldAttest=NO wouldInsertEra=YES wouldAppend=PROVISIONED"
DONE="PROVISIONED generation=1 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT clusterId=cluster-dev-A"
ALREADY="ALREADY_PROVISIONED generation=1 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT clusterId=cluster-dev-A"
REQUIRED="ATTESTATION_REQUIRED generation=1 ledgerTopicId=$LT clusterId=cluster-dev-A"
DIAG="ACL_UNENFORCED authorizer=UNVERIFIABLE\nmode=DRY_RUN symbol=SPX generation=1 bootstrapKind=VIRGIN\n"
echo "--- dry runs ---"
rm -f "$T/receipt"
run "dry run, topics pending"                        0 "topicIds=PENDING" false "$DIAG$PENDING\n"
[ -f "$T/receipt" ] && grep -q "build=7 env=dev symbol=SPX generation=1 eraId=1 file_sha256=" "$T/receipt" && { pass=$((pass+1)); echo "  ok   the dry run wrote this build's receipt"; } || { fail=$((fail+1)); echo "  FAIL no dry-run receipt written"; }
grep -q "create -f" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   the Job was created"; } || { fail=$((fail+1)); echo "  FAIL the Job was not created"; }
grep -q "delete configmap/zerodte-provision-lock" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   the lock was released after the run"; } || { fail=$((fail+1)); echo "  FAIL the lock was not released"; }
grep -n "create -f -\|get jobs" "$LAST_JOURNAL" | head -2 | grep -q "create -f -" && { pass=$((pass+1)); echo "  ok   the lock was taken BEFORE the inventory"; } || { fail=$((fail+1)); echo "  FAIL the lock was not taken before the inventory"; }
run "dry run, fully resolved"                        0 "fully resolved against the live cluster" false "$DIAG$RESOLVED\n"
run "dry run on a provisioned ledger"                0 "already holds generation 1" false "$DIAG$ALREADY\n"
run "dry run: PROVISIONED is a write when told not to" 1 "wrote when told not to" false "$DIAG$DONE\n"
run "dry run: ATTESTATION_REQUIRED is a confirm outcome" 1 "is a CONFIRM outcome" false "$DIAG$REQUIRED\n" FAKE_SUCCEEDED=0 FAKE_EXIT=67
echo "--- confirm ---"
rm -f "$T/receipt"; run "seed the receipt" 0 "topicIds=PENDING" false "$DIAG$PENDING\n"
run "confirm without PERMITTED_SHA"                  1 "needs PERMITTED_SHA" true "$DIAG$DONE\n"
run "confirm with another commit permitted"          1 "is not the permitted commit" true "$DIAG$DONE\n" PERMITTED_SHA="$(printf '1%.0s' $(seq 40))"
run "confirm: PROVISIONED"                           0 "OK: PROVISIONED generation 1 of SPX on dev" true "$DIAG$DONE\n" PERMITTED_SHA="$HEAD"
# END TO END: the block the wrapper printed, committed as the receipt, passes the CI validator BOUND to the declaration
printf '%s\n' "$LAST_OUT" | sed -n '/^environment: dev$/,/^ledgerOffset: /p' > "$W/deploy/zerodte/provisioned/dev.yaml"
grep -q "^environmentLineageId: $L$" "$W/deploy/zerodte/provisioned/dev.yaml" && grep -q "^bootstrapKind: VIRGIN$" "$W/deploy/zerodte/provisioned/dev.yaml" && [ "$(cd "$W" && bash scripts/ci/validate-zerodte-provisioning.sh 2>&1 | grep -c "ok   deploy/zerodte/provisioned/dev.yaml (bound to")" = 1 ] \
  && { pass=$((pass+1)); echo "  ok   the printed receipt block validates as deploy/zerodte/provisioned/dev.yaml bound to its declaration"; } || { fail=$((fail+1)); echo "  FAIL the printed receipt block does not validate"; cat "$W/deploy/zerodte/provisioned/dev.yaml"; }
sed -i.bak 's/^generation: 1$/generation: 2/' "$W/deploy/zerodte/provisioned/dev.yaml" && rm -f "$W/deploy/zerodte/provisioned/dev.yaml.bak"
(cd "$W" && bash scripts/ci/validate-zerodte-provisioning.sh >/dev/null 2>&1) && { fail=$((fail+1)); echo "  FAIL a receipt of another generation passed the validator"; } || { pass=$((pass+1)); echo "  ok   a receipt of another generation is refused by the validator"; }
rm -f "$W/deploy/zerodte/provisioned/dev.yaml"
printf '%s' "$LAST_OUT" | grep -q "ledgerTopicId: \"$LT\"" && printf '%s' "$LAST_OUT" | grep -q "Commit this as deploy/zerodte/provisioned/dev.yaml" && { pass=$((pass+1)); echo "  ok   the provisioned/<env>.yaml block is printed with the live ledger topic id"; } || { fail=$((fail+1)); echo "  FAIL the provisioned block is missing"; }
run "confirm: ALREADY_PROVISIONED"                   0 "already holds generation 1" true "$DIAG$ALREADY\n" PERMITTED_SHA="$HEAD"
run "confirm: ATTESTATION_REQUIRED"                  67 "ATTESTATION REQUIRED (exit 67" true "$DIAG$REQUIRED\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=67
printf '%s' "$LAST_OUT" | grep -q "ledgerTopicId: \"$LT\"" && printf '%s' "$LAST_OUT" | grep -q "prevEntryHash: \"$(printf '0%.0s' $(seq 64))\"" && { pass=$((pass+1)); echo "  ok   the exact attestation entry to append is printed, chained on the current tail"; } || { fail=$((fail+1)); echo "  FAIL the attestation entry is not printed as required"; }
run "confirm: ATTESTATION_REQUIRED, odd clusterId"   1 "clusterId is not [A-Za-z0-9._-]+" true "${DIAG}ATTESTATION_REQUIRED generation=1 ledgerTopicId=$LT clusterId=a/b\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=67
run "confirm: PROVISIONABLE is a dry-run line"       1 "a dry-run line is not a provisioning" true "$DIAG$RESOLVED\n" PERMITTED_SHA="$HEAD"
run "confirm: CONFLICTING_PROVISIONED"               1 "CONFLICTING_PROVISIONED: the ledger already holds generation 1" true "${DIAG}CONFLICTING_PROVISIONED generation=1\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=66
run "confirm: REFUSED precondition (schema v6)"      1 "a PRECONDITION failed (SCHEMA_VERSION" true "${DIAG}REFUSED reason=SCHEMA_VERSION exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "confirm: REFUSED unavailable"                   1 "UNAVAILABLE (LEDGER_READ)" true "${DIAG}REFUSED reason=LEDGER_READ exit=69\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "confirm: REFUSED mutation"                      1 "READ THE LEDGER AND THE TOPICS" true "${DIAG}REFUSED reason=READBACK exit=70\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=70
run "confirm: REFUSED usage"                         1 "refused its INVOCATION" true "${DIAG}REFUSED reason=USAGE exit=64\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=64
echo "--- the receipt held to the letter ---"
run "no receipt line"                                1 "printed 0 receipt line(s)" true "$DIAG" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=1
run "two receipt lines"                              1 "printed 2 receipt line(s)" true "$DIAG$DONE\n$DONE\n" PERMITTED_SHA="$HEAD"
run "another generation in the receipt"              1 "generation='2'!='1'" true "${DIAG}PROVISIONED generation=2 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT clusterId=c\n" PERMITTED_SHA="$HEAD"
run "another era in the receipt"                     1 "eraId='9'!='1'" true "${DIAG}PROVISIONED generation=1 eraId=9 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT clusterId=c\n" PERMITTED_SHA="$HEAD"
run "a field twice"                                  1 "generation= exactly once; got it 2 times" true "${DIAG}PROVISIONED generation=1 generation=1 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT clusterId=c\n" PERMITTED_SHA="$HEAD"
run "a short ledger topic id"                        1 "ledgerTopicId has 3 characters, not 32" true "${DIAG}PROVISIONED generation=1 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=abc clusterId=c\n" PERMITTED_SHA="$HEAD"
run "an extra token"                                 1 "a field this outcome does not have: extra=" true "$DIAG$DONE extra=1\n" PERMITTED_SHA="$HEAD"
run "a token that is not name=value"                 1 "is not name=value" true "$DIAG$DONE junk\n" PERMITTED_SHA="$HEAD"
run "a missing field"                                1 "clusterId= exactly once; got it 0 times" true "${DIAG}PROVISIONED generation=1 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT\n" PERMITTED_SHA="$HEAD"
run "an empty clusterId"                             1 "field clusterId= is empty" true "${DIAG}PROVISIONED generation=1 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT clusterId=\n" PERMITTED_SHA="$HEAD"
run "a clusterId with a space-free odd character"    1 "clusterId" true "${DIAG}PROVISIONED generation=1 eraId=1 ledgerOffset=0 provisionedDigest=$PD ledgerTopicId=$LT clusterId=a/b\n" PERMITTED_SHA="$HEAD"
run "exit/outcome disagree: PROVISIONED, exit 3"     1 "the receipt and the process disagree" true "$DIAG$DONE\n" PERMITTED_SHA="$HEAD" FAKE_EXIT=3
run "exit/outcome disagree: REFUSED 68, exit 69"     1 "the receipt and the process disagree" true "${DIAG}REFUSED reason=SCHEMA_VERSION exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "exit/outcome disagree: ATTESTATION_REQUIRED, exit 1" 1 "the receipt and the process disagree" true "$DIAG$REQUIRED\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=1
run "REFUSED with an exit that is not a refusal"     1 "is not a provisioner refusal code" true "${DIAG}REFUSED reason=X exit=67\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=67
run "REFUSED with an extra field"                    1 "a field this outcome does not have: generation=" true "${DIAG}REFUSED reason=X exit=68 generation=1\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "confirm: REFUSED attestation (exit 65)"         1 "refused the ATTESTATION or the declared generation contract (ATTESTATION" true "${DIAG}REFUSED reason=ATTESTATION exit=65\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=65
run "CONFLICTING with an extra field"                1 "a field this outcome does not have: eraId=" true "${DIAG}CONFLICTING_PROVISIONED generation=1 eraId=1\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=66
run "dry run: a short planDigest"                    1 "planDigest has 3 characters, not 64" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=abc provisionedDigest=PENDING topicIds=PENDING wouldCreate=NONE wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: an upper-case wouldAssert"             1 "wouldAssert is not lowercase hex" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=NONE wouldAssert=$(printf 'B%.0s' $(seq 64)) wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: RESOLVED with PENDING digest"          1 "provisionedDigest is not lowercase hex" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=RESOLVED wouldCreate=NONE wouldAssert=$PD wouldAttest=NO wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: PENDING with a real digest"            1 "provisionedDigest-not-PENDING" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=$PD topicIds=PENDING wouldCreate=NONE wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: an unknown wouldCreate role"           1 "wouldCreate('FRAMES,LEDGER')" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=FRAMES,LEDGER wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: wouldCreate duplicate role"            1 "wouldCreate('FRAMES,FRAMES')" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=FRAMES,FRAMES wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: wouldCreate trailing comma"            1 "wouldCreate('FRAMES,')" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=FRAMES, wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: wouldCreate out of order"              1 "wouldCreate('HEAD,FRAMES')" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=HEAD,FRAMES wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: wouldCreate a subset in order"         0 "topicIds=PENDING" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=HEAD,PULSE wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: wouldAttest=MAYBE"                     1 "wouldAttest" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=NONE wouldAssert=$PD wouldAttest=MAYBE wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: another lineage"                       1 "lineage" false "${DIAG}PROVISIONABLE symbol=SPX lineage=9e4f1d6b-7a2c-4c35-8d0e-5b1a3f8c2e77 generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=NONE wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED\n"
run "dry run: a missing dry-run field"               1 "wouldAppend= exactly once; got it 0 times" false "${DIAG}PROVISIONABLE symbol=SPX lineage=$L generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=NONE wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES\n"
run "dry run: a PROVISIONED-only field"              1 "a field this outcome does not have: ledgerOffset=" false "$DIAG$PENDING ledgerOffset=0\n"
run "a successful line from a failed Job"            1 "did not succeed (state=failed" true "$DIAG$DONE\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_CONDITIONS='{"type":"Failed","status":"True"}'
run "an image without the provisioner"               1 "does not carry ZeroDteProvisioner" true "Error: Could not find or load main class com.optionsedge.processing.zerodte.provisioning.ZeroDteProvisioner\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=1
echo "--- identity, cluster, inventory, receipt binding ---"
run "another kubectl identity"                       1 "kubeconfig identity is" false "$DIAG$PENDING\n" FAKE_WHOAMI=system:admin
echo "--- the cluster is its CA, not a name ---"
run "another cluster's CA on dev"                    1 "is not the pinned dev cluster's" false "$DIAG$PENDING\n" FAKE_CA=other
run "production's CA on dev"                         1 "is not the pinned dev cluster's" false "$DIAG$PENDING\n" FAKE_CA=prod
run "a kubeconfig without CA data"                   1 "carries no certificate-authority-data" false "$DIAG$PENDING\n" FAKE_CA=none
run "the pinned CA but insecure-skip-tls-verify"     1 "insecure-skip-tls-verify: true" false "$DIAG$PENDING\n" FAKE_SKIP_TLS=true
run "the CA given as a file path"                    1 "names its CA by file path (/etc/kube/ca.crt)" false "$DIAG$PENDING\n" FAKE_CA=file
grep -q "create -f -" "$LAST_JOURNAL" && { fail=$((fail+1)); echo "  FAIL a refused cluster still took the lock"; } || { pass=$((pass+1)); echo "  ok   a refused cluster never reaches the lock"; }
(cd "$W" && sed -i.bak 's/^    caSha256: "\([0-9A-F]*\)"$/    caSha256: "\1"/; 3s/caSha256: "[0-9A-F]*"/caSha256: "0000"/' deploy/zerodte/clusters.yaml && rm -f deploy/zerodte/clusters.yaml.bak)
run "a malformed pin"                                1 "caSha256 for dev is not 64 hex characters" false "$DIAG$PENDING\n"
(cd "$W" && git checkout -q -- deploy/zerodte/clusters.yaml)
PL=9e4f1d6b-7a2c-4c35-8d0e-5b1a3f8c2e77
PENDING_PROD="PROVISIONABLE symbol=SPX lineage=$PL generation=1 eraId=1 planDigest=$PD provisionedDigest=PENDING topicIds=PENDING wouldCreate=FRAMES,HEAD,CURRENT,PULSE,DEPLOYMENTS wouldAssert=$PD wouldAttest=YES wouldInsertEra=YES wouldAppend=PROVISIONED"
run "production: dry run on the pinned cluster"      0 "topicIds=PENDING" false "$DIAG$PENDING_PROD\n" ENVIRONMENT=production FAKE_CA=prod FAKE_SERVER=https://192.168.100.252:6443
grep -q "build=7 env=production symbol=SPX" "$T/receipt" && { pass=$((pass+1)); echo "  ok   production's dry run wrote its receipt"; } || { fail=$((fail+1)); echo "  FAIL production's dry-run receipt"; }
run "production: dev's CA"                           1 "is not the pinned production cluster's" false "$DIAG$PENDING_PROD\n" ENVIRONMENT=production FAKE_CA=dev FAKE_SERVER=https://192.168.100.252:6443
run "production: the right CA, another API server"  1 "the pinned production API server is 'https://192.168.100.252:6443'" false "$DIAG$PENDING_PROD\n" ENVIRONMENT=production FAKE_CA=prod FAKE_SERVER=https://10.0.0.9:6443
run "production: dev's receipt line"                 1 "lineage" false "$DIAG$PENDING\n" ENVIRONMENT=production FAKE_CA=prod FAKE_SERVER=https://192.168.100.252:6443
rm -f "$T/receipt"
echo "--- one at a time ---"
run "the lock is held"                               1 "could not acquire lock zerodte-provision-lock (held by build-3-20261003T120000Z)" false "$DIAG$PENDING\n" FAKE_LOCK_HELD=1
grep -q "create -f -" "$LAST_JOURNAL" && ! grep -q "get jobs" "$LAST_JOURNAL" && ! grep -q "delete configmap/zerodte-provision-lock" "$LAST_JOURNAL" \
  && { pass=$((pass+1)); echo "  ok   a held lock stops before the inventory and is NOT released by the loser"; } || { fail=$((fail+1)); echo "  FAIL the loser touched the inventory or the lock"; }
run "an active Job already"                          1 "another zerodte-provision Job is not terminal" false "$DIAG$PENDING\n" FAKE_JOBS='{"items":[{"metadata":{"name":"zerodte-provision-x"},"status":{"active":1}}]}'
grep -q "delete configmap/zerodte-provision-lock" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   the lock is released on a refusal too"; } || { fail=$((fail+1)); echo "  FAIL the lock leaked on a refusal"; }
echo "--- the lock's lifetime: released only when no Job of this run can still be running ---"
lock_released() { grep -q "delete configmap/zerodte-provision-lock" "$LAST_JOURNAL"; }
run "Job create refused, then absent"                1 "admission webhook denied the Job" false "$DIAG$PENDING\n" FAKE_CREATE_JOB_FAILS=1 FAKE_JOB_GET=absent
lock_released && ! grep -q "delete job/" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   an absent Job releases the lock without a delete"; } || { fail=$((fail+1)); echo "  FAIL absent Job: lock/delete"; }
run "Job create refused, API unreadable, delete fails" 1 "lock zerodte-provision-lock RETAINED" false "$DIAG$PENDING\n" FAKE_CREATE_JOB_FAILS=1 FAKE_JOB_GET=error FAKE_DELETE_JOB_FAILS=1
! lock_released && grep -q "delete job/" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   a failed delete RETAINS the lock"; } || { fail=$((fail+1)); echo "  FAIL failed delete: the lock was released"; }
run "Job create refused, API unreadable, delete succeeds, then absent" 1 "admission webhook denied the Job" false "$DIAG$PENDING\n" FAKE_CREATE_JOB_FAILS=1 FAKE_JOB_GET=error
lock_released && grep -q "delete job/" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   a delete re-observed as absent releases the lock"; } || { fail=$((fail+1)); echo "  FAIL delete+absent: lock"; }
run "a dry run for this build's receipt"             0 "topicIds=PENDING" false "$DIAG$PENDING\n"
run "a failed Job is kept and the lock released"     1 "did not succeed (state=failed" true "$DIAG$DONE\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0
lock_released && ! grep -q "delete job/" "$LAST_JOURNAL" && grep -q "keeping it for post-mortem" <<<"$LAST_OUT" && { pass=$((pass+1)); echo "  ok   a terminal Job is kept, the lock released"; } || { fail=$((fail+1)); echo "  FAIL terminal Job: kept/lock"; }
printf 'build=6 env=dev symbol=SPX generation=1 eraId=1 file_sha256=x attestation_sha256=y head=%s\n' "$HEAD" > "$T/receipt"
run "confirm with another build's receipt"           1 "does not describe this write" true "$DIAG$DONE\n" PERMITTED_SHA="$HEAD"
rm -f "$T/receipt"
run "confirm without a receipt"                      1 "no dry-run receipt at" true "$DIAG$DONE\n" PERMITTED_SHA="$HEAD"
run "ENVIRONMENT unset"                              1 "ENVIRONMENT must be dev or production" false "$DIAG$PENDING\n" ENVIRONMENT=
echo "zerodte-provision receipt contract: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-provision-receipt-test: OK ==="; exit 0; }
echo "=== zerodte-provision-receipt-test: FAILED ==="; exit 1
