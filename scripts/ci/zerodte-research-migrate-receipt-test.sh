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
cp scripts/deploy/zerodte-migrate-barrier.sh "$W/scripts/deploy/"
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
listing() { # listing <kind>: the fixture file, or an empty list; pods may switch to pods-2.json from the SECOND listing on; FAKE_UNREADABLE / FAKE_MALFORMED name kinds
  local f="$K/$1.json"
  case ",${FAKE_UNREADABLE:-}," in *",$1,"*) echo "Unable to connect to the server: dial tcp: i/o timeout" >&2; exit 1 ;; esac
  case ",${FAKE_MALFORMED:-}," in *",$1,"*) printf '{"items": 5}'; return ;; *",$1-top,"*) printf '[]'; return ;; *",$1-item,"*) printf '{"items": [7]}'; return ;; esac
  if [ "$1" = pods ] && [ -f "$K/pods-2.json" ] && [ "$(grep -c 'get pods -o json' "$FAKE_JOURNAL")" -ge 2 ]; then f="$K/pods-2.json"; fi
  if [ -f "$f" ]; then cat "$f"; else printf '{"items":[]}'; fi
}
validate_job() { # the server-side shape of the Job the wrapper renders (what a real API server would refuse: a wrong kind / namespace, an unpinned image, an unsubstituted placeholder, a missing run variable)
  local f="$1"
  [ "$(yq -r '.kind' "$f")" = Job ] || { echo "admission: not a Job" >&2; return 1; }
  [ "$(yq -r '.metadata.namespace' "$f")" = options-edge ] || { echo "admission: wrong namespace" >&2; return 1; }
  grep -q '__[A-Z_]*__' "$f" && { echo "admission: an unsubstituted placeholder" >&2; return 1; }
  case "$(yq -r '.spec.template.spec.containers[0].image' "$f")" in *@sha256:*) : ;; *) echo "admission: the image is not digest-pinned" >&2; return 1 ;; esac
  local v; for v in MIGRATE_FILE MIGRATE_FILE_SHA256 MIGRATE_SESSIONS_AHEAD MIGRATE_QUIESCED MIGRATE_CONFIRM; do
    [ "$(yq -r ".spec.template.spec.containers[0].env[] | select(.name == \"$v\") | .value" "$f")" != "" ] || { echo "admission: $v is not set" >&2; return 1; }
  done
  [ "$(yq -r '.spec.template.spec.containers[0].env[] | select(.name == "POSTGRES_PASSWORD") | .valueFrom.secretKeyRef.name' "$f")" = options-edge-runtime-secrets ] || { echo "admission: the password is not a secretKeyRef" >&2; return 1; }
  return 0
}
validate_cm() { # the ConfigMap the wrapper applies: one data key, the declaration's basename
  local f="$1"
  [ "$(yq -r '.kind' "$f")" = ConfigMap ] || { echo "admission: not a ConfigMap" >&2; return 1; }
  [ "$(yq -r '.data | keys | length' "$f")" = 1 ] || { echo "admission: not exactly one data key" >&2; return 1; }
  return 0
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
  *"get configmap/zerodte-research-migrate-lock"*) [ -f "$K/lock" ] && cat "$K/lock" || { echo 'Error from server (NotFound): configmaps "zerodte-research-migrate-lock" not found' >&2; exit 1; } ;;
  *"delete configmap/zerodte-research-migrate-lock"*) rm -f "$K/lock" ;;
  *"get configmap "*" -o json") case ",${FAKE_UNREADABLE:-}," in *",configmap,"*) echo "Unable to connect" >&2; exit 1 ;; esac; case ",${FAKE_MALFORMED:-}," in *",configmap,"*) printf 'not json {'; exit 0 ;; *",configmap-list,"*) printf '[1, 2]'; exit 0 ;; *",configmap-data,"*) printf '{"metadata":{"name":"x"},"data":{"ZERODTE_RESEARCH_ENABLED":5}}'; exit 0 ;; *",configmap-binary-data,"*) printf '{"metadata":{"name":"x"},"data":{"ZERODTE_RESEARCH_ENABLED":"false"},"binaryData":{"unrelated":5}}'; exit 0 ;; esac; n="${args#*get configmap }"; n="${n%% *}"; if [ -f "$K/cm-$n.json" ]; then cat "$K/cm-$n.json"; else echo "Error from server (NotFound): configmaps \"$n\" not found" >&2; exit 1; fi ;;
  *"get secret "*" -o go-template="*) case ",${FAKE_UNREADABLE:-}," in *",secret,"*) echo "Unable to connect" >&2; exit 1 ;; esac; n="${args#*get secret }"; n="${n%% *}"; if [ -f "$K/secret-$n.keys" ]; then cat "$K/secret-$n.keys"; else echo "Error from server (NotFound): secrets \"$n\" not found" >&2; exit 1; fi ;;
  *"create configmap"*"--dry-run=client -o yaml")
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: options-edge\ndata:\n'
    for a in "$@"; do case "$a" in --from-file=*) k="${a#--from-file=}"; printf '  %s: |\n    x\n' "${k%%=*}" ;; esac; done ;;
  *"create -f -")
    body="$(cat)"
    if printf '%s' "$body" | grep -q "name: zerodte-research-migrate-lock"; then
      # the ATOMIC lock: one ConfigMap name; a second create fails AlreadyExists whoever holds it (a migration, a service deploy)
      if [ -f "$K/lock" ]; then echo 'Error from server (AlreadyExists): configmaps "zerodte-research-migrate-lock" already exists' >&2; exit 1; fi
      printf '%s' "$body" | sed -n 's/^ *options-edge.io\/holder: "\(.*\)"$/\1/p' | head -1 > "$K/lock"
    fi ;;
  *"create -f "*)
    f="${args##*create -f }"; cp "$f" "$FAKE_JOURNAL.job.yaml"
    if [ "${FAKE_DEPLOY_DURING_JOB:-0}" = 1 ]; then
      # a service deploy admitted at this very moment must be EXCLUDED by the held lock: run the real barrier against this same fake
      bash "$FAKE_REPO/scripts/deploy/zerodte-migrate-barrier.sh" acquire options-edge vix-option-inteligence deploy-at-job-create > "$FAKE_JOURNAL.deploy" 2>&1 && echo "deploy-acquire rc=0" >> "$FAKE_JOURNAL.deploy" || echo "deploy-acquire rc=$?" >> "$FAKE_JOURNAL.deploy"
    fi
    if [ "${FAKE_CREATE_JOB_FAILS:-0}" = 1 ]; then echo "Error from server: admission webhook denied the Job" >&2; exit 1; fi ;;
  *"delete job/"*) if [ "${FAKE_DELETE_JOB_FAILS:-0}" = 1 ]; then echo "error: timed out waiting for the condition" >&2; exit 1; fi ;;
  *"create --dry-run=server -f "*) validate_job "${args##*-f }" || exit 1 ;;
  *"apply --dry-run=server -f "*) validate_cm "${args##*-f }" || exit 1 ;;
  *"apply -f "*) validate_cm "${args##*-f }" || exit 1 ;;
  *"delete"*) : ;;
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
check_no_job() { if ! [ -f "$LAST_JOURNAL.job.yaml" ] && ! grep -q "create -f /" "$LAST_JOURNAL"; then pass=$((pass+1)); echo "  ok   $1"; else fail=$((fail+1)); echo "  FAIL $1"; fi; }
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
  rm -f "$T/k8s/lock"; local a; for a in "$@"; do case "$a" in FAKE_LOCK_HELD_BY=*) printf '%s' "${a#*=}" > "$T/k8s/lock" ;; esac; done   # a lock someone else holds before this run
  out="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE_JOURNAL="$journal" FAKE_CLOCK="$journal.clock" FAKE_K8S="$T/k8s" FAKE_REPO="$W" FAKE_LOG="$log" FAKE_CA_DIR="$T/ca" FAKE_PINNED_DIGEST="$DIGEST" ENVIRONMENT=dev CONFIRM="$confirm" BUILD_NUMBER=7 DRY_RUN_RECEIPT="$T/receipt" JOB_TIMEOUT_S=960 "$@" bash scripts/ops/zerodte-research-migrate.sh 2>&1)" && rc=0 || rc=$?
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
check "the dry run wrote this build's receipt with the Job's wall time" bash -c "grep -q 'build=7 env=dev from=6 to=7 calendarVersion=$CAL file_sha256=.* wall=42\$' '$T/receipt'"
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
  mkdir -p "$bin" "$T/zerodte"; printf '#!/usr/bin/env bash\necho "--- invocation" >> "%s"; printf "%%s\\n" "$@" >> "%s"\n' "$T/java-argv" "$T/java-argv" > "$bin/java"; chmod +x "$bin/java"
  yq -r '.spec.template.spec.containers[0].args[0]' "$m" > "$blk"
  cp deploy/zerodte/research-migration/dev.yaml "$T/zerodte/dev.yaml"
  # the container's environment, exactly as rendered (literal values only; the password arrives by secretKeyRef, modelled here by name)
  local -a envv=(PATH="$bin:/usr/bin:/bin" POSTGRES_JDBC_URL=jdbc:postgresql://db/x POSTGRES_USER=u POSTGRES_PASSWORD=never-printed)
  while IFS= read -r kv; do [ -n "$kv" ] && envv+=("$kv"); done < <(yq -r '.spec.template.spec.containers[0].env[] | select(.value != null) | .name + "=" + .value' "$m" | sed "s|^MIGRATE_FILE=/zerodte/|MIGRATE_FILE=$T/zerodte/|")
  printf '#!/usr/bin/env bash\nexec shasum -a 256 "$@"\n' > "$bin/sha256sum"; chmod +x "$bin/sha256sum"      # the image has coreutils; this Mac has shasum
  : > "$T/java-argv"
  local rc=0
  (cd "$T" && env -i "${envv[@]}" sh -c "$(cat "$blk")" > "$T/block.out" 2>&1) || rc=$?
  # the block must END by exec-ing java exactly once and exit 0 through it: a block that fails after invoking java, or invokes it twice, proves nothing
  [ "$rc" = 0 ] || { echo "BLOCK-FAILED rc=$rc: $(tr '\n' ' ' < "$T/block.out")"; return 0; }
  [ "$(grep -c '^--- invocation$' "$T/java-argv")" = 1 ] || { echo "BLOCK-INVOKED-JAVA-$(grep -c '^--- invocation$' "$T/java-argv")-TIMES"; return 0; }
  grep -v '^--- invocation$' "$T/java-argv"
}
ARGV="$(run_block "$LAST_JOURNAL.job.yaml")"
check "the block hands --legacy-writers-quiesced to the migrator" bash -c "printf '%s\n' \"\$1\" | grep -qx -- '--legacy-writers-quiesced'" _ "$ARGV"
check "the block hands --expected-sessions-ahead 60 and the password BY NAME" bash -c "printf '%s\n' \"\$1\" | grep -qx -- '--jdbc-password-env' && printf '%s\n' \"\$1\" | grep -qx -- 'POSTGRES_PASSWORD' && printf '%s\n' \"\$1\" | grep -qx -- '60' && ! printf '%s\n' \"\$1\" | grep -q never-printed" _ "$ARGV"
check "the block does not hand --confirm on a dry run" bash -c "! printf '%s\n' \"\$1\" | grep -qx -- '--confirm'" _ "$ARGV"
check "the block names the migrator class" bash -c "printf '%s\n' \"\$1\" | grep -qx -- 'com.optionsedge.processing.zerodte.research.ZeroDteResearchMigrator'" _ "$ARGV"
check "the block exited 0 through exactly one java invocation" bash -c "! printf '%s\n' \"\$1\" | grep -q '^BLOCK-'" _ "$ARGV"
# the block's own refusals, exercised: a mounted declaration whose bytes differ from the reviewed sha refuses before java; so does a missing run variable
BROKEN="$(yq '(.spec.template.spec.containers[0].env[] | select(.name == "MIGRATE_FILE_SHA256") | .value) = "0000000000000000000000000000000000000000000000000000000000000000"' "$LAST_JOURNAL.job.yaml")"
printf '%s\n' "$BROKEN" > "$T/broken.yaml"; ARGV2="$(run_block "$T/broken.yaml")"
check "the block refuses a declaration whose sha256 is not the reviewed one, before java" bash -c "printf '%s\n' \"\$1\" | grep -q '^BLOCK-FAILED rc=1: .*refusing to migrate under bytes nobody reviewed'" _ "$ARGV2"
run "dry run on a migrated store"                    0 "already at version 7" false "$DIAG$ALREADY\n"
run "dry run: MIGRATED is a commit when told not to" 1 "committed when told not to" false "$DIAG$MIGRATED\n"
echo "--- confirm ---"
run "confirm: MIGRATED"                              0 "OK: MIGRATED dev research store 6 -> 7" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
check "the confirm accepted the capacity evidence of this build's dry run" bash -c "printf '%s' \"\$1\" | grep -q 'capacity: 42s <= 600s'" _ "$LAST_OUT"
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
run "confirm: REFUSED indeterminate (a rollback that failed)" 1 "could not roll back (INDETERMINATE" true "${DIAG}REFUSED reason=MIGRATION_INDETERMINATE exit=70\n" PERMITTED_SHA="$HEAD" FAKE_SUCCEEDED=0 FAKE_EXIT=70
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
echo "--- exemption: a LISTED Job (name and uid) carrying a declared maintenance label — nothing else exempts a pod ---"
qj() { # qj <name> <want_rc> <want> <pod spec> [jobs spec...]
  local name="$1" want_rc="$2" want="$3" spec="$4"; shift 4
  k8s_reset; $FX pods "$spec" > "$T/k8s/pods.json"; [ "$#" -eq 0 ] || $FX jobs "$@" > "$T/k8s/jobs.json"
  run "$name" "$want_rc" "$want" false "$DIAG$MIGRATABLE\n"
}
qj "a pod owned by a LISTED exempt Job (the provisioner) on another build" 0 "writerPods=0" "prov-1:other:none:job=zerodte-provision" "job-prov-1:zerodte-provision"
qj "a pod owned by this migration's own (listed) Job" 0 "writerPods=0" "mig-1:ok:none:job=zerodte-research-migrate" "job-mig-1:zerodte-research-migrate"
qj "a pod whose owner Job is NOT listed (a dangling reference): judged" 1 "pod prov-1 container vix runs another build" "prov-1:other:none:job=zerodte-provision"
qj "a pod whose owner uid differs from the listed Job's: judged" 1 "pod prov-1 container vix runs another build" "prov-1:other:none:job=zerodte-provision:jobuid=spoofed" "job-prov-1:zerodte-provision"
qj "a pod whose owner name differs from the listed Job's: judged" 1 "pod prov-1 container vix runs another build" "prov-1:other:none:job=zerodte-provision:jobname=job-other" "job-prov-1:zerodte-provision"
qj "a pod owned by a listed Job whose label is NOT exempt: judged" 1 "pod prov-1 container vix runs another build" "prov-1:other:none:job=zerodte-provision" "job-prov-1:nightly-thing"
qj "a pod with an exempt label but NOT owned by a Job is judged" 1 "runs another build" "imp-1:other:none:label=zerodte-provision"
echo "--- a RUNNING pod's flag: its environment was captured at its start, so only a LITERAL in its spec is evidence (Codex 9c r2) ---"
$FX cm ZERODTE_RESEARCH_ENABLED=true OTHER=x > "$T/cm-on.json"; $FX cm ZERODTE_RESEARCH_ENABLED=false > "$T/cm-off.json"; $FX cm RESEARCH_ENABLED=true > "$T/cm-suffix.json"; $FX cm UNRELATED=1 > "$T/cm-none.json"
install_sources() { local x; for x in "$@"; do case "$x" in secret:*) x="${x#secret:}"; printf '%s\n' "${x#*=}" | tr ',' '\n' > "$T/k8s/secret-${x%%=*}.keys" ;; *) cp "$T/cm-$x.json" "$T/k8s/cm-$x.json" ;; esac; done; }
qcm() { # qcm <name> <want_rc> <want> <pod spec> [cm files (basename without cm-/.json) | secret:<name>=<keys,> ...]
  local name="$1" want_rc="$2" want="$3" spec="$4"; shift 4
  k8s_reset; $FX pods "$spec" > "$T/k8s/pods.json"; install_sources "$@"
  run "$name" "$want_rc" "$want" false "$DIAG$MIGRATABLE\n"
}
UNKNOWABLE="could have supplied ZERODTE_RESEARCH_ENABLED when the container started — unknowable now"
qcm "envFrom a ConfigMap carrying the flag true"     1 "pod vix-a container vix imports env from configMapRef on, which $UNKNOWABLE" "vix-a:ok:from-cm=on" on
check "no ConfigMap was read to judge a live pod (a read now would say nothing about the start)" bash -c "! grep -q 'get configmap on -o json' '$LAST_JOURNAL'"
qcm "envFrom a ConfigMap carrying the flag false NOW: unknowable then, refused" 1 "$UNKNOWABLE" "vix-a:ok:from-cm=off" off
qcm "envFrom a ConfigMap that does not exist now: refused" 1 "$UNKNOWABLE" "vix-a:ok:from-cm=missing"
qcm "envFrom an OPTIONAL ConfigMap: refused too"     1 "$UNKNOWABLE" "vix-a:ok:from-cm-opt=missing"
qcm "envFrom with a prefix that cannot produce the flag name" 0 "OK: DRY RUN" "vix-a:ok:from-cm=on/X_" on
qcm "a literal env entry governs over every envFrom source (kubelet precedence)" 0 "OK: DRY RUN" "vix-a:ok:lit-and-from-cm=false/on" on
qcm "valueFrom a ConfigMap key on a live pod: refused" 1 "reads ZERODTE_RESEARCH_ENABLED from configMapKeyRef — a running container's environment was captured at its start; only a literal in the pod spec is evidence" "vix-a:ok:cmkey=off/ZERODTE_RESEARCH_ENABLED" off
qcm "valueFrom a Secret key: refused"                1 "from secretKeyRef — a running container's environment" "vix-a:ok:seckey=runtime/ZERODTE_RESEARCH_ENABLED"
qcm "valueFrom a fieldRef: refused"                  1 "from fieldRef — a running container's environment" "vix-a:ok:fieldref"
qcm "valueFrom a resourceFieldRef: refused"          1 "from resourceFieldRef — a running container's environment" "vix-a:ok:resfieldref"
qcm "envFrom a Secret: refused (it could have supplied the key)" 1 "imports env from secretRef runtime, which $UNKNOWABLE" "vix-a:ok:from-sec=runtime" "secret:runtime=POSTGRES_PASSWORD"
check "no Secret was read to judge a live pod" bash -c "! grep -q 'get secret' '$LAST_JOURNAL'"
qcm "envFrom naming both a ConfigMap and a Secret: refused" 1 "imports env from configMapRef and secretRef off, runtime, which $UNKNOWABLE" "vix-a:ok:from-cm-and-sec=off/runtime" off "secret:runtime=X"
qcm "an envFrom entry naming nothing: refused"       1 "neither a ConfigMap nor a Secret" "vix-a:ok:envfrom-empty"
qcm "the flag named twice in env: ambiguous"         1 "names ZERODTE_RESEARCH_ENABLED more than once in env — ambiguous" "vix-a:ok:dup"
qcm "the flag set to an expansion"                   1 "sets ZERODTE_RESEARCH_ENABLED to an expansion" "vix-a:ok:expand"
echo "--- a controller TEMPLATE's flag: what the kubelet WILL resolve for the next pod (a ConfigMap key is read; a Secret source refuses) ---"
qt() { # qt <name> <want_rc> <want> <deployment flag spec> [sources...]
  local name="$1" want_rc="$2" want="$3" flag="$4"; shift 4
  k8s_reset; $FX deployments "vix-option-inteligence-service:ok:$flag:1" > "$T/k8s/deployments.json"; install_sources "$@"
  run "$name" "$want_rc" "$want" false "$DIAG$MIGRATABLE\n"
}
WOULD_WRITE="template container vix has ZERODTE_RESEARCH_ENABLED=true — the next pod would write"
qt "template envFrom a ConfigMap carrying true"      1 "$WOULD_WRITE" "from-cm=on" on
qt "template envFrom a ConfigMap carrying false"     0 "OK: DRY RUN" "from-cm=off" off
qt "template envFrom two ConfigMaps, off then on: the later overrides" 1 "$WOULD_WRITE" "from-cms=off,on" off on
qt "template envFrom two ConfigMaps, on then off: the later overrides" 0 "OK: DRY RUN" "from-cms=on,off" on off
qt "template envFrom a ConfigMap without the key"    0 "OK: DRY RUN" "from-cm=none" none
qt "template envFrom a missing ConfigMap"            1 "imports env from ConfigMap missing, which does not exist" "from-cm=missing"
qt "template envFrom an OPTIONAL missing ConfigMap"  0 "OK: DRY RUN" "from-cm-opt=missing"
qt "template envFrom with a prefix completing the flag name" 1 "$WOULD_WRITE" "from-cm=suffix/ZERODTE_" suffix
qt "template envFrom with a prefix that cannot produce it" 0 "OK: DRY RUN" "from-cm=on/X_" on
qt "template valueFrom a ConfigMap key that is true" 1 "$WOULD_WRITE" "cmkey=on/ZERODTE_RESEARCH_ENABLED" on
qt "template valueFrom a ConfigMap key that is false" 0 "OK: DRY RUN" "cmkey=off/ZERODTE_RESEARCH_ENABLED" off
qt "template valueFrom an absent key"                1 "key NOPE, which is absent" "cmkey=on/NOPE" on
qt "template valueFrom an absent OPTIONAL key"       0 "OK: DRY RUN" "cmkey=on/NOPE/opt" on
qt "template valueFrom a Secret key: refused"        1 "reads ZERODTE_RESEARCH_ENABLED from a secretKeyRef — the flag must be a literal or a ConfigMap key" "seckey=runtime/ZERODTE_RESEARCH_ENABLED"
qt "template valueFrom a fieldRef: refused"          1 "from a fieldRef" "fieldref"
qt "template valueFrom a resourceFieldRef: refused"  1 "from a resourceFieldRef" "resfieldref"
qt "template envFrom a Secret carrying the key: refused" 1 "would take ZERODTE_RESEARCH_ENABLED from Secret runtime — a flag that lives in a Secret cannot be judged" "from-sec=runtime" "secret:runtime=POSTGRES_PASSWORD,ZERODTE_RESEARCH_ENABLED"
check "the Secret was read as KEY NAMES only (a go-template), never as JSON" bash -c "grep -q 'get secret runtime -o go-template=' '$LAST_JOURNAL' && ! grep -q 'get secret runtime -o json' '$LAST_JOURNAL'"
qt "template envFrom a Secret without the key"       0 "OK: DRY RUN" "from-sec=runtime" "secret:runtime=POSTGRES_PASSWORD,KAFKA_KEY"
qt "template envFrom a Secret that does not exist"   1 "imports env from Secret gone, which does not exist" "from-sec=gone"
qt "template envFrom naming both a ConfigMap and a Secret: refused" 1 "has an envFrom entry naming both ConfigMap off and Secret runtime" "from-cm-and-sec=off/runtime" off "secret:runtime=X"
qt "template envFrom naming nothing: refused"        1 "neither a ConfigMap nor a Secret" "envfrom-empty"
qt "template with the flag twice: ambiguous"         1 "more than once in env — ambiguous" "dup"
qt "template with an expansion"                      1 "to an expansion" "expand"
qt "template: a literal overrides every envFrom source" 0 "OK: DRY RUN" "lit-and-from-cm=false/on" on
echo "--- the API: every list and every object read fails CLOSED (unreadable is never empty; malformed is unreadable) ---"
for kind in deployments statefulsets daemonsets replicasets jobs cronjobs; do k8s_reset; run "the $kind list cannot be read" 1 "cannot be judged: UNREADABLE: the $kind list could not be read" false "$DIAG$MIGRATABLE\n" FAKE_UNREADABLE=$kind; done
k8s_reset; run "the pod list is malformed"           1 "UNREADABLE: the pods list carries no items" false "$DIAG$MIGRATABLE\n" FAKE_MALFORMED=pods
k8s_reset; run "the pod list is a top-level array"   1 "UNREADABLE: the pods list carries no items" false "$DIAG$MIGRATABLE\n" FAKE_MALFORMED=pods-top
k8s_reset; run "the deployment list has a non-object item" 1 "UNREADABLE: the deployments list carries no items" false "$DIAG$MIGRATABLE\n" FAKE_MALFORMED=deployments-item
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:from-cm=on:1" > "$T/k8s/deployments.json"
run "a template's ConfigMap is not JSON"             1 "UNREADABLE: ConfigMap on is not JSON" false "$DIAG$MIGRATABLE\n" FAKE_MALFORMED=configmap
run "a template's ConfigMap is a JSON array"         1 "UNREADABLE: ConfigMap on is not a ConfigMap object with string data" false "$DIAG$MIGRATABLE\n" FAKE_MALFORMED=configmap-list
run "a template's ConfigMap carries a non-string datum" 1 "UNREADABLE: ConfigMap on is not a ConfigMap object with string data" false "$DIAG$MIGRATABLE\n" FAKE_MALFORMED=configmap-data
run "a template's ConfigMap carries a non-string binaryData datum (the flag itself a string)" 1 "UNREADABLE: ConfigMap on is not a ConfigMap object with string data" false "$DIAG$MIGRATABLE\n" FAKE_MALFORMED=configmap-binary-data
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:from-cm=on:1" > "$T/k8s/deployments.json"; install_sources on
run "a template's ConfigMap cannot be read"          1 "UNREADABLE: ConfigMap on could not be read" false "$DIAG$MIGRATABLE\n" FAKE_UNREADABLE=configmap
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:from-sec=runtime:1" > "$T/k8s/deployments.json"; install_sources "secret:runtime=X"
run "a template's Secret key names cannot be read"   1 "UNREADABLE: Secret runtime could not be read" false "$DIAG$MIGRATABLE\n" FAKE_UNREADABLE=secret
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
$FX pods "prov-1:other:none:job=zerodte-provision" > "$T/k8s/pods.json"; $FX jobs "job-prov-1:zerodte-provision" > "$T/k8s/jobs.json"
run "… and with only an exempt maintenance pod (its Job listed)" 0 "pods=1 writerPods=0" false "$DIAG$MIGRATABLE\n"
qc "the declared Deployment scaled to zero but still reporting a replica" 1 "is scaled to zero but still reports 1 replicas" deployments "vix-option-inteligence-service:ok:lit=false:0:zero-still-running"
k8s_reset; rm -f "$T/k8s/deployments.json" "$T/k8s/replicasets.json" "$T/k8s/pods.json"
run "no declared Deployment at all (never deployed here)" 0 "declaredAbsent=vix-option-inteligence-service" false "$DIAG$MIGRATABLE\n"
echo "--- the DEDICATED v7 writer (9d / 9e): judged by what it RUNS — at zero it is a known workload, anything running its main class refuses ---"
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:lit=false:1" "zerodte-research-writer:ok:none:0:writer" > "$T/k8s/deployments.json"
run "the writer Deployment declared, at ZERO, settled" 0 "MIGRATABLE" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:lit=false:1" "zerodte-research-writer:ok:none:1:writer" > "$T/k8s/deployments.json"; $FX pods "vix-a:ok:lit=false" "w-1:ok:none:writer" > "$T/k8s/pods.json"
run "the writer ACTIVE (its Deployment at 1, settled, flag absent) and its pod" 1 "runs the dedicated v7 writer (ZeroDteResearchWriterMain)" false "$DIAG$MIGRATABLE\n"
check_no_job "… no Job created"
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:lit=false:1" "zerodte-research-writer:ok:none:1:writer" > "$T/k8s/deployments.json"
run "the writer Deployment at 1 with no pod yet"      1 "is not at zero (desired 1, replicas 1) — deactivate it before a migration" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:lit=false:1" "zerodte-research-writer:ok:none:0:writer:zero-still-running" > "$T/k8s/deployments.json"
run "the writer Deployment at 0 but still reporting a replica" 1 "is not at zero (desired 0, replicas 1)" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX pods "vix-a:ok:lit=false" "w-stray:ok:none:writer" > "$T/k8s/pods.json"
run "a stray pod running the writer main (no Deployment)" 1 "pod w-stray container vix runs the dedicated v7 writer" false "$DIAG$MIGRATABLE\n"
echo "--- the rule's REACH: by the main class alone — before the image filter, before the Job exemption, every container kind, every controller (Codex 9e r2) ---"
k8s_reset; $FX pods "vix-a:ok:lit=false" "w-copy:foreign:none:writer" > "$T/k8s/pods.json"
run "a stray pod of a COPIED (foreign) image running the writer main" 1 "pod w-copy container vix runs the dedicated v7 writer" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX pods "vix-a:ok:lit=false" "prov-1:ok:none:job=zerodte-provision:writer" > "$T/k8s/pods.json"; $FX jobs "job-prov-1:zerodte-provision" > "$T/k8s/jobs.json"
run "a pod owned by a LISTED exempt Job whose container runs the writer main (the exemption exempts nothing from this rule)" 1 "pod prov-1 container vix runs the dedicated v7 writer" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX pods "vix-a:ok:lit=false:initwriter=ok" > "$T/k8s/pods.json"
run "an INIT container running the writer main"      1 "pod vix-a initContainer init runs the dedicated v7 writer" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX pods "vix-a:ok:lit=false:ephemeralwriter=foreign" > "$T/k8s/pods.json"
run "an EPHEMERAL container (foreign image, the main in args) running the writer main" 1 "pod vix-a ephemeralContainer debug runs the dedicated v7 writer" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX pods "vix-a:ok:lit=false" "w-done:ok:none:writer:phase=Succeeded" > "$T/k8s/pods.json"
run "a Succeeded pod that ran the writer main (a finished check) is not running" 0 "MIGRATABLE" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:lit=false:1" "writer-copy:foreign:none:1:writer" > "$T/k8s/deployments.json"
run "a Deployment of a COPIED image whose template names the writer main, at 1" 1 "Deployment writer-copy runs the dedicated v7 writer (ZeroDteResearchWriterMain) and is not at zero (desired 1, replicas 1)" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX deployments "vix-option-inteligence-service:ok:lit=false:1" "writer-copy:foreign:none:0:writer" > "$T/k8s/deployments.json"
run "the same at ZERO (a foreign image at zero is not a legacy workload either)" 0 "MIGRATABLE" false "$DIAG$MIGRATABLE\n"
qc "a CronJob of a FOREIGN image whose template names the writer main" 1 "CronJob nightly-copy runs the dedicated v7 writer (ZeroDteResearchWriterMain) — it could create a writer pod at any moment" cronjobs "nightly-copy:writer:foreign"
qc "a StatefulSet naming the writer main, at 1"     1 "Statefulset ss-w runs the dedicated v7 writer (ZeroDteResearchWriterMain) and is not at zero" statefulsets "ss-w:writer:foreign"
qc "a StatefulSet naming the writer main, at zero"  0 "MIGRATABLE" statefulsets "ss-w:writer:foreign:zero"
qc "a DaemonSet naming the writer main"             1 "DaemonSet ds-w runs the dedicated v7 writer (ZeroDteResearchWriterMain) — it cannot be at zero" daemonsets "ds-w:writer:foreign"
qc "an ACTIVE Job naming the writer main (a check in flight)" 1 "Job chk runs the dedicated v7 writer (ZeroDteResearchWriterMain) and is not terminal" jobs "chk:zerodte-writer-check:writer"
qc "a TERMINAL Job naming the writer main (a kept check, post-mortem)" 0 "MIGRATABLE" jobs "chk:zerodte-writer-check:writer:terminal"
qc "a MULTI-COMPLETION Job naming the writer main: one pod succeeded, no terminal condition (another pod to come)" 1 "Job chk runs the dedicated v7 writer (ZeroDteResearchWriterMain) and is not terminal" jobs "chk:zerodte-writer-check:writer:partial"
k8s_reset; $FX replicasets "vix-rs:dep-uid-0001" "rs-w:none:writer:foreign" > "$T/k8s/replicasets.json"
run "a ReplicaSet (foreign image, no owner) naming the writer main, at 1" 1 "ReplicaSet rs-w runs the dedicated v7 writer (ZeroDteResearchWriterMain) and is not at zero (desired 1, replicas 1)" false "$DIAG$MIGRATABLE\n"
k8s_reset; $FX replicasets "vix-rs:dep-uid-0001" "rs-w:none:writer:foreign:zero" > "$T/k8s/replicasets.json"
run "the same ReplicaSet at zero"                    0 "MIGRATABLE" false "$DIAG$MIGRATABLE\n"
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
run "the lock is held by another migration"          1 "could not acquire lock zerodte-research-migrate-lock (held by research-migrate-build-3-20261003T120000Z)" false "$DIAG$MIGRATABLE\n" FAKE_LOCK_HELD_BY=research-migrate-build-3-20261003T120000Z
run "the lock is held by a service deploy (the mutual exclusion)" 1 "held by service-deploy-vix-option-inteligence-dev-build-12-20261004T120000Z): a rollout of the service" false "$DIAG$MIGRATABLE\n" FAKE_LOCK_HELD_BY=service-deploy-vix-option-inteligence-dev-build-12-20261004T120000Z
check "a held lock: the pods were never judged (nothing happens outside the barrier)" bash -c "! grep -q 'get pods -o json' '$LAST_JOURNAL'"
echo "--- the INTERLEAVING: a service deploy admitted while the migration holds the lock is excluded; the migration's lock is held through its Job ---"
k8s_reset
run "a deploy arrives at the moment the Job is created" 0 "OK: DRY RUN" false "$DIAG$MIGRATABLE\n" FAKE_DEPLOY_DURING_JOB=1
check "the deploy was REFUSED by the held lock (acquire rc=1, naming the migration as holder)" bash -c "grep -q 'deploy-acquire rc=1' '$LAST_JOURNAL.deploy' && grep -q 'is held by research-migrate-build-7-' '$LAST_JOURNAL.deploy'"
check "… and the migration still owned the lock afterwards (released by itself at the end)" bash -c "grep -c 'delete configmap/zerodte-research-migrate-lock' '$LAST_JOURNAL' | grep -qx 1"
: > "$T/k8s/lock-journal"; printf '%s' "deploy-held" > "$T/k8s/lock"
out="$(cd "$W" && env PATH="$W/bin:$PATH" FAKE_JOURNAL="$T/k8s/lock-journal" FAKE_K8S="$T/k8s" bash scripts/deploy/zerodte-migrate-barrier.sh release options-edge 2>&1)"; rm -f "$T/k8s/lock"
check "the barrier's release removes the lock it holds" bash -c "printf '%s' \"\$1\" | grep -q 'released' && ! [ -f '$T/k8s/lock' ]" _ "$out"
run "an active Job already"                          1 "another zerodte-research-migrate Job is not terminal" false "$DIAG$MIGRATABLE\n" FAKE_JOBS='{"items":[{"metadata":{"name":"zerodte-research-migrate-x"},"status":{"active":1}}]}'
printf 'build=6 env=dev from=6 to=7 calendarVersion=%s file_sha256=x head=%s wall=42\n' "$CAL" "$HEAD" > "$T/receipt"
run "confirm with another build's receipt"           1 "does not describe this write" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
rm -f "$T/receipt"
run "confirm without a receipt"                      1 "no dry-run receipt at" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
echo "--- the capacity evidence a CONFIRM requires: this build's dry-run wall time under the margin ---"
rm -f "$T/receipt"; run "dry run (the receipt for the confirms below)" 0 "OK: DRY RUN" false "$DIAG$MIGRATABLE\n"
sed -i.bak 's/ wall=42$/ wall=700/' "$T/receipt" && rm -f "$T/receipt.bak"
run "confirm: the dry run took longer than the margin" 1 "this build's dry run took 700s of Job wall time; a CONFIRM needs at most 600s" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
check "… no Job was created" no_job_created
sed -i.bak 's/ wall=700$/ wall=unknown/' "$T/receipt" && rm -f "$T/receipt.bak"
run "confirm: the dry run recorded no measurable wall time" 1 "records no measurable Job wall time" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
sed -i.bak 's/ wall=unknown$//' "$T/receipt" && rm -f "$T/receipt.bak"
run "confirm: a receipt without the wall time (an older wrapper's)" 1 "does not describe this write" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD"
rm -f "$T/receipt"; run "dry run (again)" 0 "OK: DRY RUN" false "$DIAG$MIGRATABLE\n"
run "confirm: a tighter margin refuses the same dry run" 1 "took 42s of Job wall time; a CONFIRM needs at most 30s" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD" CONFIRM_MAX_DRY_RUN_WALL_S=30
run "CONFIRM_MAX_DRY_RUN_WALL_S out of range"        1 "CONFIRM_MAX_DRY_RUN_WALL_S must be within 1..900" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$HEAD" CONFIRM_MAX_DRY_RUN_WALL_S=901
run "confirm with another HEAD"                      1 "is not the permitted commit" true "$DIAG$MIGRATED\n" PERMITTED_SHA="$(printf '0%.0s' $(seq 40))"
run "ENVIRONMENT unset"                              1 "ENVIRONMENT must be dev or production" false "$DIAG$MIGRATABLE\n" ENVIRONMENT=
run "JOB_TIMEOUT_S below the Job's deadline"         1 "JOB_TIMEOUT_S must be within 960..3600" false "$DIAG$MIGRATABLE\n" JOB_TIMEOUT_S=600
echo "zerodte-research-migrate receipt + quiescence contract: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-research-migrate-receipt-test: OK ==="; exit 0; }
echo "=== zerodte-research-migrate-receipt-test: FAILED ==="; exit 1
