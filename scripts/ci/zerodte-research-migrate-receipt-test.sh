#!/usr/bin/env bash
# The wrapper's RECEIPT and QUIESCENCE contract (scripts/ops/zerodte-research-migrate.sh): the migrator's ONE outcome line is the only evidence
# this identity has, and the quiescence proof is the only thing standing between a running v6 writer and a migration — so the wrapper is driven
# here through every outcome and every quiescence case against a FAKE kubectl (every call the wrapper and scripts/ops/zerodte-quiescence.py make:
# identity, the cluster CA, the lock, the inventory, pods / Deployments / ReplicaSets / StatefulSets / DaemonSets / Jobs / CronJobs, ConfigMaps,
# Secret key names, render, create, wait, log, exit code), a FAKE CLOCK (date +%s and sleep — so bounded waits and timeouts are exercised in
# full without waiting), the Kubernetes objects built by zerodte-research-migrate-fixtures.py, and a stub image pinner — from a throwaway copy of
# the repository (a git checkout with an origin/main ref; cluster pins for CERTIFICATES MINTED HERE, so the CA pipeline runs for real).
# Each case asserts the exit status AND the verdict text, so a refusal for the wrong reason is a failure too. The Job manifest the wrapper
# creates is CAPTURED and its container's shell block EXECUTED against a fake java, so the attestation is proven to reach the migrator's argv.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
W="$T/repo"
mkdir -p "$W/scripts/ops" "$W/scripts/ci" "$W/scripts/deploy" "$W/k8s/jobs" "$W/deploy/zerodte/research-migration" "$W/image-tags" "$W/bin" "$T/ca" "$T/k8s"
cp scripts/ops/zerodte-research-migrate.sh scripts/ops/zerodte-quiescence.py "$W/scripts/ops/"
cp scripts/ci/zerodte_attestation.py scripts/ci/validate-zerodte-research-migration.sh "$W/scripts/ci/"
mkdir -p "$W/scripts/ci/fixtures/zerodte/corpus" && cp scripts/ci/fixtures/zerodte/golden.tsv "$W/scripts/ci/fixtures/zerodte/" && cp scripts/ci/fixtures/zerodte/corpus/* "$W/scripts/ci/fixtures/zerodte/corpus/"
cp k8s/jobs/zerodte-research-migrate-job.yaml "$W/k8s/jobs/"
cp deploy/zerodte/research-migration/dev.yaml deploy/zerodte/research-migration/production.yaml deploy/zerodte/research-migration/legacy-writers.yaml "$W/deploy/zerodte/research-migration/"
printf 'images:\n  vix-option-inteligence-service: 192.168.100.252:5000/options-edge-vix-option-inteligence:dev\n' > "$W/image-tags/dev.yaml"
printf 'images:\n  vix-option-inteligence-service: 192.168.100.252:5000/options-edge-vix-option-inteligence:prod\n' > "$W/image-tags/production.yaml"
DIGEST="sha256:$(printf 'a%.0s' $(seq 64))"
export FAKE_PINNED_DIGEST="$DIGEST"
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
FX="python3 $PWD/scripts/ci/zerodte-research-migrate-fixtures.py"
# --- the fake clock: `date +%s` reads a counter that `sleep` advances; every other date call is the real one ---
cat > "$W/bin/date" <<'FAKE'
#!/usr/bin/env bash
if [ "$#" -eq 1 ] && [ "$1" = "+%s" ]; then cat "${FAKE_CLOCK:?}"; else exec /bin/date "$@"; fi
FAKE
cat > "$W/bin/sleep" <<'FAKE'
#!/usr/bin/env bash
now="$(cat "${FAKE_CLOCK:?}")"; echo $(( now + ${1:-1} )) > "$FAKE_CLOCK"
FAKE
# --- the fake kubectl: every call the wrapper and the quiescence helper make, answered from $FAKE_K8S fixtures; mutations are journaled ---
cat > "$W/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
args="$*"
echo "kubectl $args" >> "${FAKE_JOURNAL:?}"
K="${FAKE_K8S:?}"
listing() { # listing <kind>: the fixture file, or an empty list; pods may switch to pods-2.json from the SECOND listing on
  local f="$K/$1.json"
  if [ "$1" = pods ] && [ -f "$K/pods-2.json" ] && [ "$(grep -c 'get pods -o json' "$FAKE_JOURNAL")" -ge 2 ]; then f="$K/pods-2.json"; fi
  if [ -f "$f" ]; then cat "$f"; else printf '{"items":[]}'; fi
}
case "$args" in
  "auth whoami -o jsonpath={.status.userInfo.username}") printf '%s' "${FAKE_WHOAMI:-system:serviceaccount:options-edge:jenkins-deployer}" ;;
  *"config view --minify -o jsonpath={.clusters[0].cluster.server}") printf '%s' "${FAKE_SERVER:-https://127.0.0.1:1}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority-data}")
    case "${FAKE_CA:-dev}" in none|file) printf '' ;; *) base64 < "${FAKE_CA_DIR:?}/${FAKE_CA:-dev}.pem" | tr -d '\n' ;; esac ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.insecure-skip-tls-verify}") printf '%s' "${FAKE_SKIP_TLS:-}" ;;
  *"config view --minify --raw -o jsonpath={.clusters[0].cluster.certificate-authority}") [ "${FAKE_CA:-dev}" = file ] && printf '/etc/kube/ca.crt' || printf '' ;;
  *"get pods -o json") [ "${FAKE_PODS_UNREADABLE:-0}" = 1 ] && { echo "Unable to connect" >&2; exit 1; }; listing pods ;;
  *"get deployments -o json") listing deployments ;;
  *"get statefulsets -o json") listing statefulsets ;;
  *"get daemonsets -o json") listing daemonsets ;;
  *"get replicasets -o json") listing replicasets ;;
  *"get cronjobs -o json") listing cronjobs ;;
  *"get jobs -o json") listing jobs ;;
  *"get jobs -l app.kubernetes.io/name=zerodte-research-migrate -o json") jobs="${FAKE_JOBS:-}"; [ -n "$jobs" ] || jobs='{"items":[]}'; printf '%s' "$jobs" ;;
  *"get configmap "*" -o json") n="${args#*get configmap }"; n="${n%% *}"; if [ -f "$K/cm-$n.json" ]; then cat "$K/cm-$n.json"; else echo "Error from server (NotFound): configmaps \"$n\" not found" >&2; exit 1; fi ;;
  *"get secret "*" -o go-template="*) n="${args#*get secret }"; n="${n%% *}"; if [ -f "$K/secret-$n.keys" ]; then cat "$K/secret-$n.keys"; else echo "Error from server (NotFound): secrets \"$n\" not found" >&2; exit 1; fi ;;
  *"create configmap"*"--dry-run=client -o yaml")
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: options-edge\ndata:\n'
    for a in "$@"; do case "$a" in --from-file=*) k="${a#--from-file=}"; printf '  %s: |\n    x\n' "${k%%=*}" ;; esac; done ;;
  *"get configmap/zerodte-research-migrate-lock"*) printf 'build-3-20261003T120000Z' ;;
  *"create -f -")
    body="$(cat)"
    if [ "${FAKE_LOCK_HELD:-0}" = 1 ] && printf '%s' "$body" | grep -q "name: zerodte-research-migrate-lock"; then
      echo 'Error from server (AlreadyExists): configmaps "zerodte-research-migrate-lock" already exists' >&2; exit 1
    fi ;;
  *"create -f "*) f="${args##*create -f }"; cp "$f" "$FAKE_JOURNAL.job.yaml"; if [ "${FAKE_CREATE_JOB_FAILS:-0}" = 1 ]; then echo "Error from server: admission webhook denied the Job" >&2; exit 1; fi ;;
  *"delete job/"*) if [ "${FAKE_DELETE_JOB_FAILS:-0}" = 1 ]; then echo "error: timed out waiting for the condition" >&2; exit 1; fi ;;
  *"apply --dry-run=server"*|*"create --dry-run=server"*|*"apply -f"*|*"delete"*) : ;;
  *"get job/"*"-o json")
    if grep -q "delete job/" "$FAKE_JOURNAL" && [ "${FAKE_DELETE_JOB_FAILS:-0}" != 1 ]; then echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1; fi
    case "${FAKE_JOB_GET:-normal}" in
      absent) echo 'Error from server (NotFound): jobs.batch "x" not found' >&2; exit 1 ;;
      error)  echo 'Unable to connect to the server: EOF' >&2; exit 1 ;;
      active) printf '{"status":{"active":1,"startTime":"2026-10-04T12:00:00Z"}}'; exit 0 ;;
    esac
    times='"startTime":"2026-10-04T12:00:00Z","completionTime":"2026-10-04T12:00:42Z"'
    if [ "${FAKE_SUCCEEDED:-1}" = 1 ]; then printf '{"status":{"succeeded":1,"conditions":[],%s}}' "$times"; else cond="${FAKE_CONDITIONS:-}"; [ -n "$cond" ] || cond='{"type":"Failed","status":"True"}'; printf '{"status":{"succeeded":0,"conditions":[%s],%s}}' "$cond" "$times"; fi ;;
  *"logs job/"*) [ "${FAKE_LOGS_FAIL:-0}" = 1 ] && { echo "Error from server: container is waiting" >&2; exit 1; }; printf '%b' "${FAKE_LOG:-}" ;;
  *"get pods -l job-name="*) printf '{"items":[{"status":{"containerStatuses":[{"name":"migrator","state":{"terminated":{"exitCode":%s}}}]}}]}' "${FAKE_EXIT:-0}" ;;
  *"get configmaps"*) printf '' ;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99 ;;
esac
FAKE
chmod +x "$W/bin/kubectl" "$W/bin/date" "$W/bin/sleep"
pass=0; fail=0
# the default cluster: one quiescent writer pod on the pinned digest with the flag off; the declared Deployment settled, pinned, flag off; its ReplicaSet
k8s_reset() {
  rm -rf "$T/k8s"; mkdir -p "$T/k8s"
  $FX pods "vix-a:ok:lit=false" > "$T/k8s/pods.json"
  $FX deployments "vix-option-inteligence-service:ok:lit=false:1" > "$T/k8s/deployments.json"
  $FX replicasets "vix-rs:dep-uid-0001" > "$T/k8s/replicasets.json"
}
k8s_reset
run() { # run <name> <want_rc> <want substring> <CONFIRM> <FAKE_LOG> [VAR=value ...]
  local name="$1" want_rc="$2" want="$3" confirm="$4" log="$5"; shift 5
  local out rc journal="$T/journal.$RANDOM$RANDOM"
  : > "$journal"; echo 1700000000 > "$journal.clock"
  out="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE_JOURNAL="$journal" FAKE_CLOCK="$journal.clock" FAKE_K8S="$T/k8s" FAKE_LOG="$log" FAKE_CA_DIR="$T/ca" FAKE_PINNED_DIGEST="$DIGEST" ENVIRONMENT=dev CONFIRM="$confirm" BUILD_NUMBER=7 DRY_RUN_RECEIPT="$T/receipt" JOB_TIMEOUT_S=960 "$@" bash scripts/ops/zerodte-research-migrate.sh 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then pass=$((pass+1)); echo "  ok   $name (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -8 | sed 's/^/       | /'; fi
  LAST_OUT="$out"; LAST_JOURNAL="$journal"
}
check() { # check <name> <condition...>
  local name="$1"; shift
  if "$@"; then pass=$((pass+1)); echo "  ok   $name"; else fail=$((fail+1)); echo "  FAIL $name"; fi
}
no_job_created() { ! [ -f "$LAST_JOURNAL.job.yaml" ]; }
lock_released() { grep -q "delete configmap/zerodte-research-migrate-lock" "$LAST_JOURNAL"; }
CAL="$(grep -o '"[0-9a-f]\{64\}"' deploy/zerodte/research-migration/dev.yaml | tr -d '"')"
H64="$(printf 'b%.0s' $(seq 64))"
COUNTS="archiveFeatureFamilies=4 nullQualityFlags=4 nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 calibratedShadowRows=0"
MIGRATABLE="MIGRATABLE fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS planDigest=$H64"
MIGRATED="MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64"
ALREADY="ALREADY_MIGRATED version=7 calendarVersion=$CAL expectedSessions=2261 schemaDigest=$H64"
DIAG="mode=DRY_RUN: the migration was executed and ROLLED BACK; the counts are the real rows'\n"
GRAMMAR_WANT="is not the canonical"
echo "--- dry runs ---"
rm -f "$T/receipt"
run "dry run: MIGRATABLE"                            0 "OK: DRY RUN — the migration was executed and ROLLED BACK on dev in 42s of Job wall time" false "$DIAG$MIGRATABLE\n"
check "the dry run wrote this build's receipt" grep -q "build=7 env=dev from=6 to=7 calendarVersion=$CAL file_sha256=" "$T/receipt"
check "the Job was created" test -f "$LAST_JOURNAL.job.yaml"
check "the lock was released after the run" lock_released
# the ORDER: the lock (create -f -) is taken BEFORE the first pod listing; the LAST pod listing comes AFTER the server-side validation and BEFORE the Job create
first_lock="$(grep -n 'create -f -' "$LAST_JOURNAL" | head -1 | cut -d: -f1)"; first_pods="$(grep -n 'get pods -o json' "$LAST_JOURNAL" | head -1 | cut -d: -f1)"
last_pods="$(grep -n 'get pods -o json' "$LAST_JOURNAL" | tail -1 | cut -d: -f1)"; dry="$(grep -n 'create --dry-run=server' "$LAST_JOURNAL" | tail -1 | cut -d: -f1)"; create="$(grep -n 'create -f /' "$LAST_JOURNAL" | tail -1 | cut -d: -f1)"
check "the quiescence is judged UNDER the lock (lock line $first_lock < pods line $first_pods)" test "$first_lock" -lt "$first_pods"
check "the pods are re-listed after the server-side validation and immediately before the Job create ($dry < $last_pods < $create)" test "$dry" -lt "$last_pods" -a "$last_pods" -lt "$create"
check "the proof read every controller kind" bash -c "for k in deployments statefulsets daemonsets replicasets jobs cronjobs; do grep -q \"get \$k -o json\" '$LAST_JOURNAL' || exit 1; done"
# the CAPTURED manifest: the attestation and the mode are rendered into the container's environment …
check "the Job carries MIGRATE_QUIESCED=true" test "$(yq -r '.spec.template.spec.containers[0].env[] | select(.name == "MIGRATE_QUIESCED") | .value' "$LAST_JOURNAL.job.yaml")" = "true"
check "the Job carries MIGRATE_CONFIRM=false on a dry run" test "$(yq -r '.spec.template.spec.containers[0].env[] | select(.name == "MIGRATE_CONFIRM") | .value' "$LAST_JOURNAL.job.yaml")" = "false"
check "the Job runs the digest-pinned image" test "$(yq -r '.spec.template.spec.containers[0].image' "$LAST_JOURNAL.job.yaml")" = "192.168.100.252:5000/options-edge-vix-option-inteligence@$DIGEST"
# … and the container's shell block, EXECUTED against a fake java, puts them on the migrator's argv
run_block() { # run_block <job manifest> → the argv the block hands to java (one token per line), or the block's error
  local m="$1" blk="$T/block.sh" bin="$T/jbin" envf
  mkdir -p "$bin" "$T/zerodte"; printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$@" > "%s"\n' "$T/java-argv" > "$bin/java"; chmod +x "$bin/java"
  yq -r '.spec.template.spec.containers[0].args[0]' "$m" > "$blk"
  cp deploy/zerodte/research-migration/dev.yaml "$T/zerodte/dev.yaml"
  # the container's environment, exactly as rendered (literal values only; the password arrives by secretKeyRef, modelled here by name)
  local -a envv=(PATH="$bin:/usr/bin:/bin" POSTGRES_JDBC_URL=jdbc:postgresql://db/x POSTGRES_USER=u POSTGRES_PASSWORD=never-printed)
  while IFS= read -r kv; do [ -n "$kv" ] && envv+=("$kv"); done < <(yq -r '.spec.template.spec.containers[0].env[] | select(.value != null) | .name + "=" + .value' "$m" | sed "s|^MIGRATE_FILE=/zerodte/|MIGRATE_FILE=$T/zerodte/|")
  printf '#!/usr/bin/env bash\nexec shasum -a 256 "$@"\n' > "$bin/sha256sum"; chmod +x "$bin/sha256sum"      # the image has coreutils; this Mac has shasum
  : > "$T/java-argv"
  (cd "$T" && env -i "${envv[@]}" sh -c "$(cat "$blk")" 2>&1) || true
  cat "$T/java-argv"
}
ARGV="$(run_block "$LAST_JOURNAL.job.yaml")"
check "the block hands --legacy-writers-quiesced to the migrator" bash -c "printf '%s\n' \"\$1\" | grep -qx -- '--legacy-writers-quiesced'" _ "$ARGV"
check "the block hands --expected-sessions-ahead 60 and the password BY NAME" bash -c "printf '%s\n' \"\$1\" | grep -qx -- '--jdbc-password-env' && printf '%s\n' \"\$1\" | grep -qx -- 'POSTGRES_PASSWORD' && printf '%s\n' \"\$1\" | grep -qx -- '60' && ! printf '%s\n' \"\$1\" | grep -q never-printed" _ "$ARGV"
check "the block does not hand --confirm on a dry run" bash -c "! printf '%s\n' \"\$1\" | grep -qx -- '--confirm'" _ "$ARGV"
check "the block names the migrator class" bash -c "printf '%s\n' \"\$1\" | grep -qx -- 'com.optionsedge.processing.zerodte.research.ZeroDteResearchMigrator'" _ "$ARGV"
run "dry run on a migrated store"                    0 "already at version 7" false "$DIAG$ALREADY\n"
run "dry run: MIGRATED is a commit when told not to" 1 "committed when told not to" false "$DIAG$MIGRATED\n"
echo "--- confirm ---"
run "confirm: MIGRATED"                              0 "OK: MIGRATED dev research store 6 -> 7" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
check "the Job carries MIGRATE_CONFIRM=true on a confirm" test "$(yq -r '.spec.template.spec.containers[0].env[] | select(.name == "MIGRATE_CONFIRM") | .value' "$LAST_JOURNAL.job.yaml")" = "true"
ARGV="$(run_block "$LAST_JOURNAL.job.yaml")"
check "the block hands --confirm AND --legacy-writers-quiesced on a confirm" bash -c "printf '%s\n' \"\$1\" | grep -qx -- '--confirm' && printf '%s\n' \"\$1\" | grep -qx -- '--legacy-writers-quiesced'" _ "$ARGV"
run "confirm: ALREADY_MIGRATED"                      0 "already at version 7" true "$DIAG$ALREADY\n" PERMITTED_SHA="$HEAD"
run "confirm: MIGRATABLE is a dry-run line"          1 "a dry-run line is not a migration" true "$DIAG$MIGRATABLE\n" PERMITTED_SHA="$HEAD"
run "confirm: REFUSED precondition (version)"        1 "a PRECONDITION failed (SCHEMA_VERSION" true "${DIAG}REFUSED reason=SCHEMA_VERSION exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "confirm: REFUSED calendar resource (65)"        1 "refused the SHIPPED CALENDAR (CALENDAR_RESOURCE" true "${DIAG}REFUSED reason=CALENDAR_RESOURCE exit=65\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=65
run "confirm: REFUSED unavailable"                   1 "UNAVAILABLE or the migration lock was not acquired (LOCK_TIMEOUT)" true "${DIAG}REFUSED reason=LOCK_TIMEOUT exit=69\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "confirm: REFUSED mutation"                      1 "READ zerodte_schema_version and zerodte_research_migration" true "${DIAG}REFUSED reason=MIGRATION_FAILED exit=70\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=70
run "confirm: REFUSED usage"                         1 "refused its INVOCATION" true "${DIAG}REFUSED reason=USAGE exit=64\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=64
echo "--- the receipt held to the letter: the canonical grammar, then the (reason, exit) pairs, then the binding ---"
run "REFUSED with an exit that is not a refusal"     1 "$GRAMMAR_WANT REFUSED grammar" true "${DIAG}REFUSED reason=X exit=66\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=66
run "REFUSED with a reason the migrator never emits" 1 "reason=X exit=68, which is not a (reason, exit) pair ZeroDteResearchMigrator emits" true "${DIAG}REFUSED reason=X exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "REFUSED with a real reason under the wrong exit" 1 "reason=SCHEMA_VERSION exit=69, which is not a (reason, exit) pair" true "${DIAG}REFUSED reason=SCHEMA_VERSION exit=69\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "REFUSED with a lower-case reason"               1 "$GRAMMAR_WANT REFUSED grammar" true "${DIAG}REFUSED reason=schema_version exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "REFUSED with its tokens reordered"              1 "$GRAMMAR_WANT REFUSED grammar" true "${DIAG}REFUSED exit=68 reason=SCHEMA_VERSION\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=68
run "MIGRATED with its tokens reordered"             1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED toVersion=7 fromVersion=6 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "MIGRATED with two spaces between tokens"        1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED fromVersion=6  toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "MIGRATED with a trailing space"                 1 "$GRAMMAR_WANT MIGRATED grammar" true "$DIAG$MIGRATED \n" PERMITTED_SHA="$HEAD"
run "MIGRATED with a leading zero in a count"        1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 archiveFeatureFamilies=04 nullQualityFlags=4 nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 calibratedShadowRows=0 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "ALREADY_MIGRATED with its tokens reordered"     1 "$GRAMMAR_WANT ALREADY_MIGRATED grammar" true "${DIAG}ALREADY_MIGRATED calendarVersion=$CAL version=7 expectedSessions=2261 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "MIGRATABLE with its counts reordered"           1 "$GRAMMAR_WANT MIGRATABLE grammar" false "${DIAG}MIGRATABLE fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 nullQualityFlags=4 archiveFeatureFamilies=4 nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 calibratedShadowRows=0 planDigest=$H64\n"
run "no receipt line"                                1 "printed 0 receipt line(s)" true "$DIAG" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=1
run "two receipt lines"                              1 "printed 2 receipt line(s)" true "$DIAG$MIGRATABLE\n$MIGRATED\n" PERMITTED_SHA="$HEAD"
run "another calendar in the receipt"                1 "calendarVersion='$H64'!='$CAL'" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$H64 expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "another toVersion in the receipt"               1 "toVersion='8'!='7'" true "${DIAG}MIGRATED fromVersion=6 toVersion=8 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "a field twice"                                  1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "a missing count"                                1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 archiveFeatureFamilies=4 nullQualityFlags=4 nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "an extra token"                                 1 "$GRAMMAR_WANT MIGRATED grammar" true "$DIAG$MIGRATED extra=1\n" PERMITTED_SHA="$HEAD"
run "a token that is not name=value"                 1 "$GRAMMAR_WANT MIGRATED grammar" true "$DIAG$MIGRATED junk\n" PERMITTED_SHA="$HEAD"
run "a short schemaDigest"                           1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=abc\n" PERMITTED_SHA="$HEAD"
run "an upper-case schemaDigest"                     1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 $COUNTS schemaDigest=$(printf 'B%.0s' $(seq 64))\n" PERMITTED_SHA="$HEAD"
run "a count that is not a number"                   1 "$GRAMMAR_WANT MIGRATED grammar" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=2261 archiveFeatureFamilies=4 nullQualityFlags=x nullSurfaceStatus=4 nullSurfaceActionable=4 scheduledBoundaries=1 lateStartBoundaries=0 calibratedShadowRows=0 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "zero expected sessions"                         1 "expectedSessions=0" true "${DIAG}MIGRATED fromVersion=6 toVersion=7 calendarVersion=$CAL expectedSessions=0 $COUNTS schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "ALREADY_MIGRATED with a MIGRATED field"         1 "$GRAMMAR_WANT ALREADY_MIGRATED grammar" true "$DIAG$ALREADY fromVersion=6\n" PERMITTED_SHA="$HEAD"
run "ALREADY_MIGRATED of another version"            1 "version='8'!='7'" true "${DIAG}ALREADY_MIGRATED version=8 calendarVersion=$CAL expectedSessions=2261 schemaDigest=$H64\n" PERMITTED_SHA="$HEAD"
run "exit/outcome disagree: MIGRATED, exit 3"        1 "the receipt and the process disagree" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD" FAKE_EXIT=3
run "exit/outcome disagree: REFUSED 68, exit 69"     1 "the receipt and the process disagree" true "${DIAG}REFUSED reason=SCHEMA_VERSION exit=68\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=69
run "a successful line from a failed Job"            1 "did not succeed (state=failed" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0
run "an image without the migrator"                  1 "does not carry ZeroDteResearchMigrator" true "Error: Could not find or load main class com.optionsedge.processing.zerodte.research.ZeroDteResearchMigrator\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=1
echo "--- the Job's wait: the client timeout and the log ---"
run "the client stops waiting on an active Job"      1 "is still active after 960s" false "$DIAG$MIGRATABLE\n" FAKE_JOB_GET=active
check "an active Job at the timeout is deleted by the cleanup, then the lock released" bash -c "grep -q 'delete job/' '$LAST_JOURNAL'" 
check "… the lock released once the Job is gone" lock_released
run "the Job's log cannot be read"                   1 "could not be read in 5 attempts" false "$DIAG$MIGRATABLE\n" FAKE_LOGS_FAIL=1
run "Job create refused, API unreadable, delete fails" 1 "lock zerodte-research-migrate-lock RETAINED" false "$DIAG$MIGRATABLE\n" FAKE_CREATE_JOB_FAILS=1 FAKE_JOB_GET=error FAKE_DELETE_JOB_FAILS=1
echo "--- the legacy writers must be quiesced: pods ---"
q() { # q <name> <want_rc> <want> <pods spec...>   (the default Deployment / ReplicaSet; a dry run)
  local name="$1" want_rc="$2" want="$3"; shift 3
  k8s_reset; $FX pods "$@" > "$T/k8s/pods.json"
  run "$name" "$want_rc" "$want" false "$DIAG$MIGRATABLE\n"
}
q "a pod with ZERODTE_RESEARCH_ENABLED=true"         1 "NOT quiesced: NOT_QUIESCENT: pod vix-b container vix has ZERODTE_RESEARCH_ENABLED=true" "vix-a:ok:lit=false" "vix-b:ok:lit=true"
check "an unquiesced writer: no Job was created and the lock was released" bash -c "$(declare -f no_job_created lock_released); LAST_JOURNAL='$LAST_JOURNAL'; no_job_created && lock_released"
q "a pod with the flag TRUE (case-insensitive, as the service reads it)" 1 "has ZERODTE_RESEARCH_ENABLED=true" "vix-a:ok:lit=TRUE"
q "a pod whose flag is 'yes' (the service reads it as off)" 0 "OK: DRY RUN" "vix-a:ok:lit=yes"
q "a pod with the flag unset"                        0 "OK: DRY RUN" "vix-a:ok:none"
q "a pod on another build of the image"              1 "NOT_QUIESCENT: pod vix-a container vix runs another build of the service image" "vix-a:ok:lit=false:uid=1" "vix-a:other:lit=false"
q "a docker-pullable:// imageID on the pinned digest" 0 "OK: DRY RUN" "vix-a:ok-pullable:lit=false"
q "a bare sha256 imageID on the pinned digest"       0 "OK: DRY RUN" "vix-a:ok-bare:lit=false"
q "a pod of another repository is not a writer"      0 "writerPods=0" "other-a:foreign:lit=true"
q "a terminating pod: waited for, then refused"      1 "not provably quiescent after 120s: TRANSIENT: pod vix-a is terminating" "vix-a:ok:lit=false:terminating"
check "the bounded wait re-listed the pods more than once" test "$(grep -c 'get pods -o json' "$LAST_JOURNAL")" -ge 3
k8s_reset; $FX pods "vix-a:ok:lit=false:terminating" "vix-b:ok:lit=false" > "$T/k8s/pods.json"; $FX pods "vix-b:ok:lit=false" > "$T/k8s/pods-2.json"
run "a terminating pod that leaves during the wait"  0 "OK: DRY RUN" false "$DIAG$MIGRATABLE\n"
q "a Pending pod: waited for, then refused"          1 "TRANSIENT: pod vix-a is Pending" "vix-a:none:lit=false:phase=Pending"
q "a Running pod without an imageID yet"             1 "runs the service image but reports no imageID yet" "vix-a:none:lit=false"
q "a Succeeded pod cannot write"                     0 "OK: DRY RUN" "vix-a:other:lit=true:phase=Succeeded"
q "a Failed pod cannot write"                        0 "OK: DRY RUN" "vix-a:other:lit=true:phase=Failed"
q "an init container on another build"              1 "pod vix-a container init runs another build" "vix-a:ok:lit=false:init=other"
q "an init container on the pinned build"            0 "OK: DRY RUN" "vix-a:ok:lit=false:init=ok"
q "a sidecar on another build of the image"          1 "pod vix-a container side runs another build" "vix-a:ok:lit=false:sidecar=other"
q "a sidecar of another repository is ignored"       0 "OK: DRY RUN" "vix-a:ok:lit=false:sidecar=foreign"
q "a pod owned by an exempt maintenance Job (the provisioner) on another build" 0 "writerPods=0" "prov-1:other:none:job=zerodte-provision"
q "a pod owned by an exempt Job (this migration's own)" 0 "writerPods=0" "mig-1:ok:none:job=zerodte-research-migrate"
q "a pod with an exempt label but NOT owned by a Job is judged" 1 "runs another build" "imp-1:other:none:label=zerodte-provision"
q "a pod owned by a Job with a label that is not exempt" 1 "runs another build" "x-1:other:none:job=nightly-thing"
echo "--- the legacy writers must be quiesced: the EFFECTIVE flag ---"
$FX cm ZERODTE_RESEARCH_ENABLED=true OTHER=x > "$T/cm-on.json"; $FX cm ZERODTE_RESEARCH_ENABLED=false > "$T/cm-off.json"; $FX cm RESEARCH_ENABLED=true > "$T/cm-suffix.json"; $FX cm UNRELATED=1 > "$T/cm-none.json"
qcm() { # qcm <name> <want_rc> <want> <pod spec> [cm files to install (basename without cm-/.json) ...] [secret:<name>=<keys,>]
  local name="$1" want_rc="$2" want="$3" spec="$4"; shift 4
  k8s_reset; $FX pods "$spec" > "$T/k8s/pods.json"
  local x; for x in "$@"; do case "$x" in secret:*) x="${x#secret:}"; printf '%s\n' "${x#*=}" | tr ',' '\n' > "$T/k8s/secret-${x%%=*}.keys" ;; *) cp "$T/cm-$x.json" "$T/k8s/cm-$x.json" ;; esac; done
  run "$name" "$want_rc" "$want" false "$DIAG$MIGRATABLE\n"
}
qcm "envFrom a ConfigMap carrying the flag true"     1 "pod vix-a container vix has ZERODTE_RESEARCH_ENABLED=true" "vix-a:ok:from-cm=on" on
qcm "envFrom a ConfigMap carrying the flag false"    0 "OK: DRY RUN" "vix-a:ok:from-cm=off" off
qcm "envFrom a ConfigMap without the key"            0 "OK: DRY RUN" "vix-a:ok:from-cm=none" none
qcm "envFrom a ConfigMap that does not exist"        1 "imports env from ConfigMap missing, which does not exist" "vix-a:ok:from-cm=missing"
qcm "envFrom an OPTIONAL ConfigMap that does not exist" 0 "OK: DRY RUN" "vix-a:ok:from-cm-opt=missing"
qcm "envFrom with a prefix that completes the flag name" 1 "has ZERODTE_RESEARCH_ENABLED=true" "vix-a:ok:from-cm=suffix/ZERODTE_" suffix
qcm "envFrom with a prefix that cannot produce the flag name" 0 "OK: DRY RUN" "vix-a:ok:from-cm=on/X_" on
qcm "a literal env entry overrides an envFrom source (kubelet order)" 0 "OK: DRY RUN" "vix-a:ok:lit-and-from-cm=false/on" on
qcm "valueFrom a ConfigMap key that is true"         1 "has ZERODTE_RESEARCH_ENABLED=true" "vix-a:ok:cmkey=on/ZERODTE_RESEARCH_ENABLED" on
qcm "valueFrom a ConfigMap key that is false"        0 "OK: DRY RUN" "vix-a:ok:cmkey=off/ZERODTE_RESEARCH_ENABLED" off
qcm "valueFrom a ConfigMap key that is absent"       1 "key NOPE, which is absent" "vix-a:ok:cmkey=on/NOPE" on
qcm "valueFrom an absent OPTIONAL ConfigMap key"     0 "OK: DRY RUN" "vix-a:ok:cmkey=on/NOPE/opt" on
qcm "valueFrom a Secret key: unknowable, refused"    1 "reads ZERODTE_RESEARCH_ENABLED from a secretKeyRef — the flag must be a literal or a ConfigMap key" "vix-a:ok:seckey=runtime/ZERODTE_RESEARCH_ENABLED"
qcm "valueFrom a fieldRef: refused"                  1 "from a fieldRef" "vix-a:ok:fieldref"
qcm "envFrom a Secret carrying the flag key: refused" 1 "would take ZERODTE_RESEARCH_ENABLED from Secret runtime — a flag that lives in a Secret cannot be judged" "vix-a:ok:from-sec=runtime" "secret:runtime=POSTGRES_PASSWORD,ZERODTE_RESEARCH_ENABLED"
qcm "envFrom a Secret without the flag key"          0 "OK: DRY RUN" "vix-a:ok:from-sec=runtime" "secret:runtime=POSTGRES_PASSWORD,KAFKA_KEY"
check "the Secret was read as KEY NAMES only (a go-template), never as JSON" bash -c "grep -q 'get secret runtime -o go-template=' '$LAST_JOURNAL' && ! grep -q 'get secret runtime -o json' '$LAST_JOURNAL'"
qcm "envFrom a Secret that does not exist"           1 "imports env from Secret gone, which does not exist" "vix-a:ok:from-sec=gone"
qcm "the flag named twice in env: ambiguous"         1 "names ZERODTE_RESEARCH_ENABLED more than once in env — ambiguous" "vix-a:ok:dup"
qcm "the flag set to an expansion"                   1 "sets ZERODTE_RESEARCH_ENABLED to an expansion" "vix-a:ok:expand"
echo "--- the legacy writers must be quiesced: the controllers (the closed world) ---"
qc() { # qc <name> <want_rc> <want> <kind> <spec...>  (default pods; the named controller fixture replaced)
  local name="$1" want_rc="$2" want="$3" kind="$4"; shift 4
  k8s_reset; $FX "$kind" "$@" > "$T/k8s/$kind.json"
  run "$name" "$want_rc" "$want" false "$DIAG$MIGRATABLE\n"
}
qc "an undeclared Deployment running the image"      1 "Deployment shadow runs the service image but is not a declared legacy-writer workload" deployments "vix-option-inteligence-service:ok:lit=false:1" "shadow:ok:lit=false:1"
qc "the declared Deployment's template on another build" 1 "Deployment vix-option-inteligence-service template container vix is not pinned to the digest this Job runs" deployments "vix-option-inteligence-service:other:lit=false:1"
qc "the declared Deployment's template on a mutable tag" 1 "is not pinned to the digest this Job runs" deployments "vix-option-inteligence-service:tag:lit=false:1"
qc "the declared Deployment's template with the flag on" 1 "template container vix has ZERODTE_RESEARCH_ENABLED=true — the next pod would write" deployments "vix-option-inteligence-service:ok:lit=true:1"
qc "the declared Deployment mid-rollout"             1 "rollout is not settled (desired 2, updated 1, replicas 2, available 2)" deployments "vix-option-inteligence-service:ok:lit=false:2:rolling"
qc "the declared Deployment with an unobserved generation" 1 "has not observed its latest generation — a rollout is in flight" deployments "vix-option-inteligence-service:ok:lit=false:1:genlag"
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:lit=false:0" > "$T/k8s/deployments.json"; rm -f "$T/k8s/pods.json"
run "the declared Deployment scaled to zero (KEEP_DOWN), no pod at all" 0 "pods=0 writerPods=0" false "$DIAG$MIGRATABLE\n"
$FX pods "prov-1:other:none:job=zerodte-provision" > "$T/k8s/pods.json"
run "… and with only an exempt maintenance pod"      0 "pods=1 writerPods=0" false "$DIAG$MIGRATABLE\n"
qc "the declared Deployment scaled to zero but still reporting a replica" 1 "is scaled to zero but still reports 1 replicas" deployments "vix-option-inteligence-service:ok:lit=false:0:zero-still-running"
k8s_reset; rm -f "$T/k8s/deployments.json" "$T/k8s/replicasets.json" "$T/k8s/pods.json"
run "no declared Deployment at all (never deployed here)" 0 "declaredAbsent=vix-option-inteligence-service" false "$DIAG$MIGRATABLE\n"
qc "a StatefulSet running the image"                 1 "statefulset stateful-vix runs the service image — not a declared workload" statefulsets "stateful-vix"
qc "a DaemonSet running the image"                   1 "daemonset ds-vix runs the service image — not a declared workload" daemonsets "ds-vix"
qc "a ReplicaSet not owned by the declared Deployment" 1 "ReplicaSet orphan-rs runs the service image and is not owned by a declared Deployment" replicasets "vix-rs:dep-uid-0001" "orphan-rs:none"
qc "a ReplicaSet owned by another Deployment"        1 "ReplicaSet other-rs runs the service image and is not owned by a declared Deployment" replicasets "other-rs:uid-shadow"
qc "a CronJob running the image"                     1 "CronJob nightly runs the service image — it could create a writer pod at any moment" cronjobs "nightly"
qc "a Job running the image that is not exempt"      1 "Job adhoc runs the service image and is not an exempt maintenance Job" jobs "adhoc:none"
qc "an exempt maintenance Job (the provisioner)"     0 "OK: DRY RUN" jobs "zerodte-provision-x:zerodte-provision"
qc "a Job with a label that is not exempt"           1 "Job tool-1 runs the service image and is not an exempt maintenance Job" jobs "tool-1:some-tool"
echo "--- the race: the pod set changes between the proof and the Job creation ---"
k8s_reset; $FX pods "vix-a:ok:lit=false" "vix-b:ok:lit=false" > "$T/k8s/pods-2.json"
run "a pod appears after the proof: refused at creation, nothing created" 1 "no longer holds at Job creation: NOT_QUIESCENT: the writer pod set changed since the proof" false "$DIAG$MIGRATABLE\n"
check "… no Job was created and the lock was released" bash -c "$(declare -f no_job_created lock_released); LAST_JOURNAL='$LAST_JOURNAL'; no_job_created && lock_released"
k8s_reset; $FX pods "vix-a:ok:lit=false:uid=changed" > "$T/k8s/pods-2.json"
run "the same pod name with a new uid after the proof: refused" 1 "the writer pod set changed since the proof" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX pods "vix-a:ok:lit=true" > "$T/k8s/pods-2.json"
run "a pod turns writer after the proof: refused at creation" 1 "no longer holds at Job creation: NOT_QUIESCENT: pod vix-a container vix has ZERODTE_RESEARCH_ENABLED=true" false "$DIAG$MIGRATABLE\n"
k8s_reset
echo "--- the closed-world declaration and the API ---"
run "the pod list cannot be read"                    1 "quiescence cannot be judged: UNREADABLE: the pods list could not be read" false "$DIAG$MIGRATABLE\n" FAKE_PODS_UNREADABLE=1
check "an unreadable API: no Job was created and the lock was released" bash -c "$(declare -f no_job_created lock_released); LAST_JOURNAL='$LAST_JOURNAL'; no_job_created && lock_released"
cp "$W/deploy/zerodte/research-migration/legacy-writers.yaml" "$T/writers.bak"
sed -i.tmp 's/^imageRepository: .*/imageRepository: options-edge-something-else/' "$W/deploy/zerodte/research-migration/legacy-writers.yaml"
run "the declaration names another image repository" 1 "imageRepository must be options-edge-vix-option-inteligence (the image the migration Job runs), got 'options-edge-something-else'" false "$DIAG$MIGRATABLE\n"
cp "$T/writers.bak" "$W/deploy/zerodte/research-migration/legacy-writers.yaml"; rm -f "$W/deploy/zerodte/research-migration/legacy-writers.yaml.tmp"
mv "$W/deploy/zerodte/research-migration/legacy-writers.yaml" "$T/writers.away"
run "the declaration is missing"                     1 "missing the legacy-writer declaration" false "$DIAG$MIGRATABLE\n"
mv "$T/writers.away" "$W/deploy/zerodte/research-migration/legacy-writers.yaml"
run "QUIESCE_WAIT_S out of range"                    1 "QUIESCE_WAIT_S must be within 0..900" false "$DIAG$MIGRATABLE\n" QUIESCE_WAIT_S=901
echo "--- identity, cluster, lock, inventory, receipt binding ---"
run "another kubectl identity"                       1 "kubeconfig identity is" false "$DIAG$MIGRATABLE\n" FAKE_WHOAMI=system:admin
run "another cluster's CA"                           1 "is not the pinned dev cluster's" false "$DIAG$MIGRATABLE\n" FAKE_CA=other
run "insecure-skip-tls-verify"                       1 "insecure-skip-tls-verify: true" false "$DIAG$MIGRATABLE\n" FAKE_SKIP_TLS=true
run "production: the pinned cluster"                 0 "OK: DRY RUN" false "$DIAG$MIGRATABLE\n" ENVIRONMENT=production FAKE_CA=prod FAKE_SERVER=https://192.168.100.252:6443
run "production: another API server"                 1 "the pinned production API server is" false "$DIAG$MIGRATABLE\n" ENVIRONMENT=production FAKE_CA=prod FAKE_SERVER=https://10.0.0.9:6443
run "the lock is held"                               1 "could not acquire lock zerodte-research-migrate-lock (held by build-3-20261003T120000Z)" false "$DIAG$MIGRATABLE\n" FAKE_LOCK_HELD=1
check "a held lock: the pods were never judged (nothing happens outside the barrier)" bash -c "! grep -q 'get pods -o json' '$LAST_JOURNAL'"
run "an active Job already"                          1 "another zerodte-research-migrate Job is not terminal" false "$DIAG$MIGRATABLE\n" FAKE_JOBS='{"items":[{"metadata":{"name":"zerodte-research-migrate-x"},"status":{"active":1}}]}'
printf 'build=6 env=dev from=6 to=7 calendarVersion=%s file_sha256=x head=%s\n' "$CAL" "$HEAD" > "$T/receipt"
run "confirm with another build's receipt"           1 "does not describe this write" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
rm -f "$T/receipt"
run "confirm without a receipt"                      1 "no dry-run receipt at" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
run "confirm with another HEAD"                      1 "is not the permitted commit" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$(printf '0%.0s' $(seq 40))"
run "ENVIRONMENT unset"                              1 "ENVIRONMENT must be dev or production" false "$DIAG$MIGRATABLE\n" ENVIRONMENT=
run "JOB_TIMEOUT_S below the Job's deadline"         1 "JOB_TIMEOUT_S must be within 960..3600" false "$DIAG$MIGRATABLE\n" JOB_TIMEOUT_S=600
echo "zerodte-research-migrate receipt + quiescence contract: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-research-migrate-receipt-test: OK ==="; exit 0; }
echo "=== zerodte-research-migrate-receipt-test: FAILED ==="; exit 1
