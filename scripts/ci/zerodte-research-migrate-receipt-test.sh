#!/usr/bin/env bash
# The wrapper's RECEIPT contract (scripts/ops/zerodte-research-migrate.sh): the migrator's ONE outcome line is the only evidence this identity
# has, so the wrapper is driven here through every outcome — against a FAKE kubectl that answers each call the wrapper makes (identity, the
# cluster CA, the service pods, the lock, the inventory, render, create, wait, log, exit code) and a stub image pinner — from a throwaway copy
# of the repository (a git checkout with an origin/main ref; cluster pins for CERTIFICATES MINTED HERE, so the CA pipeline runs for real).
# Each case asserts the exit status AND the verdict text, so a refusal for the wrong reason is a failure too.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
W="$T/repo"
mkdir -p "$W/scripts/ops" "$W/scripts/ci" "$W/scripts/deploy" "$W/k8s/jobs" "$W/deploy/zerodte/research-migration" "$W/image-tags" "$W/bin" "$T/ca"
cp scripts/ops/zerodte-research-migrate.sh "$W/scripts/ops/"
cp scripts/ci/zerodte_attestation.py scripts/ci/validate-zerodte-research-migration.sh "$W/scripts/ci/"
mkdir -p "$W/scripts/ci/fixtures/zerodte/corpus" && cp scripts/ci/fixtures/zerodte/golden.tsv "$W/scripts/ci/fixtures/zerodte/" && cp scripts/ci/fixtures/zerodte/corpus/* "$W/scripts/ci/fixtures/zerodte/corpus/"
cp k8s/jobs/zerodte-research-migrate-job.yaml "$W/k8s/jobs/"
cp deploy/zerodte/research-migration/dev.yaml deploy/zerodte/research-migration/production.yaml "$W/deploy/zerodte/research-migration/"
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
# the fake kubectl: every call the wrapper makes, answered from FAKE_* variables; mutations are journaled
cat > "$W/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
args="$*"
echo "kubectl $args" >> "${FAKE_JOURNAL:?}"
pods() { # the service pods: FAKE_PODS = <imageID digest or ok>|<flag>[,...]  (ok = the pinned digest; empty = no pods)
  local spec="${FAKE_PODS-ok|false}" items="" first=1 p image flag
  [ -n "$spec" ] || { printf '{"items":[]}'; return; }
  for p in ${spec//,/ }; do
    image="${p%%|*}"; flag="${p#*|}"
    [ "$image" = ok ] && image="${FAKE_PINNED_DIGEST:?}"
    [ "$first" = 1 ] || items="$items,"; first=0
    items="$items{\"metadata\":{\"name\":\"vix-$image-$flag\"},\"spec\":{\"containers\":[{\"name\":\"vix\",\"env\":[{\"name\":\"ZERODTE_RESEARCH_ENABLED\",\"value\":\"$flag\"}]}]},\"status\":{\"containerStatuses\":[{\"name\":\"vix\",\"imageID\":\"registry/x@$image\"}]}}"
  done
  printf '{"items":[%s]}' "$items"
}
case "$args" in
  "auth whoami -o jsonpath={.status.userInfo.username}") printf '%s' "${FAKE_WHOAMI:-system:serviceaccount:options-edge:jenkins-deployer}" ;;
  *"config view --minify -o jsonpath={.clusters[0].cluster.server}") printf '%s' "${FAKE_SERVER:-https://127.0.0.1:1}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority-data}")
    case "${FAKE_CA:-dev}" in none|file) printf '' ;; *) base64 < "${FAKE_CA_DIR:?}/${FAKE_CA:-dev}.pem" | tr -d '\n' ;; esac ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.insecure-skip-tls-verify}") printf '%s' "${FAKE_SKIP_TLS:-}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority}") [ "${FAKE_CA:-dev}" = file ] && printf '/etc/kube/ca.crt' || printf '' ;;
  *"get pods -l app.kubernetes.io/name=vix-option-inteligence-service -o json") [ "${FAKE_PODS_UNREADABLE:-0}" = 1 ] && { echo "Unable to connect" >&2; exit 1; }; pods ;;
  *"get jobs -l app.kubernetes.io/name=zerodte-research-migrate -o json") jobs="${FAKE_JOBS:-}"; [ -n "$jobs" ] || jobs='{"items":[]}'; printf '%s' "$jobs" ;;
  *"create configmap"*"--dry-run=client -o yaml")
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: options-edge\ndata:\n'
    for a in "$@"; do case "$a" in --from-file=*) k="${a#--from-file=}"; printf '  %s: |\n    x\n' "${k%%=*}" ;; esac; done ;;
  *"get configmap/zerodte-research-migrate-lock"*) printf 'build-3-20261003T120000Z' ;;
  *"create -f -")
    body="$(cat)"
    if [ "${FAKE_LOCK_HELD:-0}" = 1 ] && printf '%s' "$body" | grep -q "name: zerodte-research-migrate-lock"; then
      echo 'Error from server (AlreadyExists): configmaps "zerodte-research-migrate-lock" already exists' >&2; exit 1
    fi ;;
  *"create -f "*) if [ "${FAKE_CREATE_JOB_FAILS:-0}" = 1 ]; then echo "Error from server: admission webhook denied the Job" >&2; exit 1; fi ;;
  *"delete job/"*) if [ "${FAKE_DELETE_JOB_FAILS:-0}" = 1 ]; then echo "error: timed out waiting for the condition" >&2; exit 1; fi ;;
  *"apply --dry-run=server"*|*"create --dry-run=server"*|*"apply -f"*|*"delete"*) : ;;
  *"get job/"*"-o json")
    if grep -q "delete job/" "$FAKE_JOURNAL" && [ "${FAKE_DELETE_JOB_FAILS:-0}" != 1 ]; then echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1; fi
    case "${FAKE_JOB_GET:-normal}" in
      absent) echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1 ;;
      error)  echo 'Unable to connect to the server: EOF' >&2; exit 1 ;;
    esac
    if [ "${FAKE_SUCCEEDED:-1}" = 1 ]; then printf '{"status":{"succeeded":1,"conditions":[]}}'; else cond="${FAKE_CONDITIONS:-}"; [ -n "$cond" ] || cond='{"type":"Failed","status":"True"}'; printf '{"status":{"succeeded":0,"conditions":[%s]}}' "$cond"; fi ;;
  *"logs job/"*) printf '%b' "${FAKE_LOG:-}" ;;
  *"get pods -l job-name="*) printf '{"items":[{"status":{"containerStatuses":[{"name":"migrator","state":{"terminated":{"exitCode":%s}}}]}}]}' "${FAKE_EXIT:-0}" ;;
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
  out="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE_JOURNAL="$journal" FAKE_LOG="$log" FAKE_CA_DIR="$T/ca" FAKE_PINNED_DIGEST="$DIGEST" ENVIRONMENT=dev CONFIRM="$confirm" BUILD_NUMBER=7 DRY_RUN_RECEIPT="$T/receipt" JOB_TIMEOUT_S=960 "$@" bash scripts/ops/zerodte-research-migrate.sh 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then pass=$((pass+1)); echo "  ok   $name (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -6 | sed 's/^/       | /'; fi
  LAST_OUT="$out"; LAST_JOURNAL="$journal"
}
CAL="$(grep -o '"[0-9a-f]\{64\}"' deploy/zerodte/research-migration/dev.yaml | tr -d '"')"
H64="$(printf 'b%.0s' $(seq 64))"
COUNTS="archiveFeatureFamilies=4 nullQualityFlags=4 nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 calibratedShadowRows=0"
MIGRATABLE="MIGRATABLE fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS planDigest=$H64"
MIGRATED="MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64"
ALREADY="ALREADY_MIGRATED version=7 calendarVersion=$CAL expectedSessions=2261 schemaDigest=$H64"
DIAG="mode=DRY_RUN: the migration was executed and ROLLED BACK; the counts are the real rows'\n"
echo "--- dry runs ---"
rm -f "$T/receipt"
run "dry run: MIGRATABLE"                            0 "OK: DRY RUN — the migration was executed and ROLLED BACK" false "$DIAG$MIGRATABLE\n"
[ -f "$T/receipt" ] && grep -q "build=7 env=dev from=6 to=7 calendarVersion=$CAL file_sha256=" "$T/receipt" && { pass=$((pass+1)); echo "  ok   the dry run wrote this build's receipt"; } || { fail=$((fail+1)); echo "  FAIL no dry-run receipt written"; }
grep -q "create -f" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   the Job was created"; } || { fail=$((fail+1)); echo "  FAIL the Job was not created"; }
printf '%s' "$LAST_OUT" | grep -q "legacy writers quiesced: every running pod is on the pinned image" && { pass=$((pass+1)); echo "  ok   the quiescence was verified and rendered"; } || { fail=$((fail+1)); echo "  FAIL quiescence"; }
grep -q "delete configmap/zerodte-research-migrate-lock" "$LAST_JOURNAL" && { pass=$((pass+1)); echo "  ok   the lock was released after the run"; } || { fail=$((fail+1)); echo "  FAIL the lock was not released"; }
grep -n "get pods -l app.kubernetes.io/name=vix\|create -f -" "$LAST_JOURNAL" | head -2 | head -1 | grep -q "get pods" && { pass=$((pass+1)); echo "  ok   the quiescence is judged BEFORE the lock"; } || { fail=$((fail+1)); echo "  FAIL order: pods vs lock"; }
run "dry run on a migrated store"                    0 "already at version 7" false "$DIAG$ALREADY\n"
run "dry run: MIGRATED is a commit when told not to" 1 "committed when told not to" false "$DIAG$MIGRATED\n"
echo "--- confirm ---"
run "confirm: MIGRATED"                              0 "OK: MIGRATED dev research store 6 -> 7" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
run "confirm: ALREADY_MIGRATED"                      0 "already at version 7" true "$DIAG$ALREADY\n" PERMITTED_SHA="$HEAD"
run "confirm: MIGRATABLE is a dry-run line"          1 "a dry-run line is not a migration" true "$DIAG$MIGRATABLE\n" PERMITTED_SHA="$HEAD"
run "confirm: REFUSED precondition (version)"        1 "a PRECONDITION failed (SCHEMA_VERSION" true "${DIAG}REFUSED reason=SCHEMA_VERSION exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "confirm: REFUSED calendar resource (65)"        1 "refused the SHIPPED CALENDAR (CALENDAR_RESOURCE" true "${DIAG}REFUSED reason=CALENDAR_RESOURCE exit=65\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=65
run "confirm: REFUSED unavailable"                   1 "UNAVAILABLE or the migration lock was not acquired (LOCK_TIMEOUT)" true "${DIAG}REFUSED reason=LOCK_TIMEOUT exit=69\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "confirm: REFUSED mutation"                      1 "READ zerodte_schema_version and zerodte_research_migration" true "${DIAG}REFUSED reason=MIGRATION_FAILED exit=70\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=70
run "confirm: REFUSED usage"                         1 "refused its INVOCATION" true "${DIAG}REFUSED reason=USAGE exit=64\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=64
run "confirm: REFUSED with an exit that is not a refusal" 1 "is not a migrator refusal code" true "${DIAG}REFUSED reason=X exit=66\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=66
echo "--- the receipt held to the letter ---"
run "no receipt line"                                1 "printed 0 receipt line(s)" true "$DIAG" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=1
run "two receipt lines"                              1 "printed 2 receipt line(s)" true "$DIAG$MIGRATABLE\n$MIGRATED\n" PERMITTED_SHA="$HEAD"
run "another calendar in the receipt"                1 "calendarVersion='$H64'!='$CAL'" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$H64 expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "another toVersion in the receipt"               1 "toVersion='8'!='7'" true "${DIAG}MIGRATED fromVersion=6 toVersion=8 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "a field twice"                                  1 "toVersion= exactly once; got it 2 times" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "a missing count"                                1 "calibratedShadowRows= exactly once; got it 0 times" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 archiveFeatureFamilies=4 nullQualityFlags=4 nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "an extra token"                                 1 "a field this outcome does not have: extra=" true "$DIAG$MIGRATED extra=1\n" PERMITTED_SHA="$HEAD"
run "a token that is not name=value"                 1 "is not name=value" true "$DIAG$MIGRATED junk\n" PERMITTED_SHA="$HEAD"
run "a short schemaDigest"                           1 "schemaDigest has 3 characters, not 64" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=abc\n" PERMITTED_SHA="$HEAD"
run "a count that is not a number"                   1 "nullQualityFlags is not a non-negative integer" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 archiveFeatureFamilies=4 nullQualityFlags=x nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 calibratedShadowRows=0 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "zero expected sessions"                         1 "expectedSessions=0" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=0 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "ALREADY_MIGRATED with a MIGRATED field"         1 "a field this outcome does not have: fromVersion=" true "$DIAG$ALREADY fromVersion=6\n" PERMITTED_SHA="$HEAD"
run "ALREADY_MIGRATED of another version"            1 "version='8'!='7'" true "${DIAG}ALREADY_MIGRATED version=8 calendarVersion=$CAL expectedSessions=2261 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "exit/outcome disagree: MIGRATED, exit 3"        1 "the receipt and the process disagree" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD" FAKE_EXIT=3
run "exit/outcome disagree: REFUSED 68, exit 69"     1 "the receipt and the process disagree" true "${DIAG}REFUSED reason=SCHEMA_VERSION exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "a successful line from a failed Job"            1 "did not succeed (state=failed" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0
run "an image without the migrator"                  1 "does not carry ZeroDteResearchMigrator" true "Error: Could not find or load main class com.optionsedge.processing.zerodte.research.ZeroDteResearchMigrator\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=1
echo "--- the legacy writers must be quiesced ---"
run "a pod with ZERODTE_RESEARCH_ENABLED=true"       1 "the legacy research writers are NOT quiesced: vix-$DIGEST-true(image ok,ZERODTE_RESEARCH_ENABLED=true)" false "$DIAG$MIGRATABLE\n" FAKE_PODS="ok|false,ok|true"
grep -q "create -f -" "$LAST_JOURNAL" && { fail=$((fail+1)); echo "  FAIL an unquiesced writer still took the lock"; } || { pass=$((pass+1)); echo "  ok   an unquiesced writer never reaches the lock"; }
run "a pod on another image"                         1 "NOT quiesced: vix-sha256:$(printf 'c%.0s' $(seq 64))-false(another image,flag off)" false "$DIAG$MIGRATABLE\n" FAKE_PODS="sha256:$(printf 'c%.0s' $(seq 64))|false"
run "no service pod at all"                          0 "vix-option-inteligence pods: 0" false "$DIAG$MIGRATABLE\n" FAKE_PODS=
run "the pod list cannot be read"                    1 "quiescence cannot be judged" false "$DIAG$MIGRATABLE\n" FAKE_PODS_UNREADABLE=1
echo "--- identity, cluster, lock, inventory, receipt binding ---"
run "another kubectl identity"                       1 "kubeconfig identity is" false "$DIAG$MIGRATABLE\n" FAKE_WHOAMI=system:admin
run "another cluster's CA"                           1 "is not the pinned dev cluster's" false "$DIAG$MIGRATABLE\n" FAKE_CA=other
run "insecure-skip-tls-verify"                       1 "insecure-skip-tls-verify: true" false "$DIAG$MIGRATABLE\n" FAKE_SKIP_TLS=true
run "production: the pinned cluster"                 0 "OK: DRY RUN" false "$DIAG$MIGRATABLE\n" ENVIRONMENT=production FAKE_CA=prod FAKE_SERVER=https://192.168.100.252:6443
run "production: another API server"                 1 "the pinned production API server is" false "$DIAG$MIGRATABLE\n" ENVIRONMENT=production FAKE_CA=prod FAKE_SERVER=https://10.0.0.9:6443
run "the lock is held"                               1 "could not acquire lock zerodte-research-migrate-lock (held by build-3-20261003T120000Z)" false "$DIAG$MIGRATABLE\n" FAKE_LOCK_HELD=1
run "an active Job already"                          1 "another zerodte-research-migrate Job is not terminal" false "$DIAG$MIGRATABLE\n" FAKE_JOBS='{"items":[{"metadata":{"name":"zerodte-research-migrate-x"},"status":{"active":1}}]}'
run "Job create refused, API unreadable, delete fails" 1 "lock zerodte-research-migrate-lock RETAINED" false "$DIAG$MIGRATABLE\n" FAKE_CREATE_JOB_FAILS=1 FAKE_JOB_GET=error FAKE_DELETE_JOB_FAILS=1
printf 'build=6 env=dev from=6 to=7 calendarVersion=%s file_sha256=x head=%s\n' "$CAL" "$HEAD" > "$T/receipt"
run "confirm with another build's receipt"           1 "does not describe this write" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
rm -f "$T/receipt"
run "confirm without a receipt"                      1 "no dry-run receipt at" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
run "confirm with another HEAD"                      1 "is not the permitted commit" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$(printf '0%.0s' $(seq 40))"
run "ENVIRONMENT unset"                              1 "ENVIRONMENT must be dev or production" false "$DIAG$MIGRATABLE\n" ENVIRONMENT=
run "JOB_TIMEOUT_S below the Job's deadline"         1 "JOB_TIMEOUT_S must be within 960..3600" false "$DIAG$MIGRATABLE\n" JOB_TIMEOUT_S=600
echo "zerodte-research-migrate receipt contract: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-research-migrate-receipt-test: OK ==="; exit 0; }
echo "=== zerodte-research-migrate-receipt-test: FAILED ==="; exit 1
