#!/usr/bin/env bash
# What a dev wipe leaves behind for its dependents (owner decision 2026-11-10: "reset on every dev wipe").
#
# THE DEFECT. futures.level-absorption.events is on the shared RESET-PRESERVED list (evidence on prod), so the dev
# wipe KEPT it while deleting the inputs level-absorption-service replays from. The service then refused to start
# (REPLAY_SHORT) after every dev clean and was repaired by hand: delete events + closures, recreate through the
# barrier. The same bring-up left es-amt-service and market-structure-service unready for reasons that are not faults,
# and `dev-cleanup now` / `start` exited 0 after refusing or failing, so nothing but the log said so.
#
# Sections (no broker, no cluster, no docker; every function is lifted out BY NAME from the file under test and
# run against stubs, because the script itself dispatches on its arguments and would start doing things):
#   1  dev_wipe_keep_list removes EXACTLY the reset set and nothing near it
#   2  the REAL do_clean deletes level-absorption's events + closures and still keeps every other preserved topic
#   3  the REAL ensure_topics, fed the REAL topics.env, recreates those two topics at the shape the service demands
#   4  session_bound_line / report_session_bound_services: a cause is named only when the log CONFIRMS it
#   5  the dispatch carries do_clean / do_start / do_start_overnight's status (and the dry-run guard's) to the exit
#   6  mutants: take each fix out of a copy of the script (or topics.env) and run THIS WHOLE FILE against it: it must fail
# DEV_CLEANUP_UNDER_TEST / TOPICS_ENV_UNDER_TEST point the file at a mutant; POST_WIPE_NO_MUTANTS=1 skips section 6
# (the child runs of section 6 set it, so the mutant runs do not recurse).
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
TOPICS_ENV_FILE="${TOPICS_ENV_UNDER_TEST:-$HERE/../kafka/topics.env}"
RPT_REAL="$HERE/../kafka/reset-preserved-topics.sh"
SELF="$HERE/dev-cleanup-post-wipe-test.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
for f in "$SRC" "$RPT_REAL" "$TOPICS_ENV_FILE"; do [ -f "$f" ] || { echo "FAIL: $f missing" >&2; exit 1; }; done

fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p {print} p && /^\}/ {exit}' "$2"; }
var() { grep -E "^$1=" "$2" | head -1; }

# ---- fixture: a real origin + clone, so topics_ref_pin does a real fetch (as the pin test does) ----
ORIGIN="$WORK/origin.git"; REPO="$WORK/repo"
git init -q --bare -b main "$ORIGIN" || { echo "FAIL: cannot build the fixture origin" >&2; exit 1; }
git clone -q "$ORIGIN" "$REPO" 2>/dev/null
mkdir -p "$REPO/scripts/kafka"
cp "$RPT_REAL" "$REPO/scripts/kafka/reset-preserved-topics.sh"
cat > "$REPO/scripts/kafka/topics.env" <<'EOF'
OPTIONS_EDGE_RESET_PRESERVED_TOPICS="t.rp futures.level-absorption.events"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="t.prodkeep"
OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="t.devkeep"
EOF
( cd "$REPO" && git checkout -q -b main 2>/dev/null; git -c user.email=t@t -c user.name=t add -A \
  && git -c user.email=t@t -c user.name=t commit -qm c && git push -q origin main ) >/dev/null 2>&1 \
  || { echo "FAIL: could not build the fixture repository" >&2; exit 1; }

# ---- stubs for the external commands do_clean runs before the delete ----
mkdir -p "$WORK/bin"
cat > "$WORK/bin/kt" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$WORK/kt.calls"
EOF
cat > "$WORK/bin/kk" <<'EOF'
#!/usr/bin/env bash
case "$*" in *pvc*) echo '{"items":[]}' ;; esac
exit 0
EOF
chmod +x "$WORK/bin/kt" "$WORK/bin/kk"

build() { # $1 = script to cut from -> $WORK/functions.sh
  : > "$WORK/functions.sh"
  local _f BODY
  for _f in topics_ref_pin dev_clean_preserved_topics reset_preserved_from_main dev_wipe_keep_list do_clean; do
    BODY="$(fn "$_f" "$1")"
    [ -n "$BODY" ] || { echo "FAIL: function $_f not found in $1" >&2; exit 1; }
    printf '%s\n' "$BODY" >> "$WORK/functions.sh"
  done
  var DEV_ONLY_ESTIMATE_KEEP "$1" > "$WORK/vars.sh"
  var DEV_CLEAN_RESET_TOPICS "$1" >> "$WORK/vars.sh"
  [ "$(wc -l < "$WORK/vars.sh")" -ge 2 ] || { echo "FAIL: DEV_ONLY_ESTIMATE_KEEP / DEV_CLEAN_RESET_TOPICS not found in $1" >&2; exit 1; }
}

run_do_clean() { # -> stdout+stderr of the REAL do_clean up to (and including) the delete
  : > "$WORK/kt.calls"
  env DEPLOY_REPO="$REPO" TOPICS_ENV_REF="origin/main:scripts/kafka/topics.env" WIPE_KAFKA=true \
      FUNCTIONS="$WORK/functions.sh" VARS="$WORK/vars.sh" WORKDIR="$WORK" bash -c '
    TOPICS_ENV_SHA=""; TOPICS_ENV_PINNED_REF=""
    . "$VARS"; . "$FUNCTIONS"
    K="$WORKDIR/bin/kk"; KK="$WORKDIR/bin/kk"; KT="$WORKDIR/bin/kt"; BS=stub:0; KEEP="never-matches-anything"
    KAFKA_DATA="$WORKDIR/no-such-data-dir"
    clean_guard() { return 0; }
    require_kafka_cli() { return 0; }
    clean_logs() { return 0; }
    pause_dev_mirrors() { return 0; }
    kafka_list_topics() { printf "%s\n" t.rp t.devkeep t.other __consumer_offsets _schemas \
        futures.level-absorption.events futures.level-absorption.closures futures.level-absorption.voice \
        futures.level-absorption.events.archive; }
    # The first sleep after the delete: stop HERE, so nothing after the wipe (registry prune, docker, tmutil) can run.
    sleep() { echo "REACHED-AFTER-DELETE"; exit 0; }
    do_clean; echo "do_clean-returned rc=$?"' 2>&1
}
deleted() { grep -oE -- '--delete --topic [^ ]+' "$WORK/kt.calls" | awk '{print $3}' | sort | paste -sd' ' -; }

echo "== 1. dev_wipe_keep_list removes exactly the reset set"
# shellcheck disable=SC1090
eval "$(var DEV_CLEAN_RESET_TOPICS "$SRC")"
eval "$(fn dev_wipe_keep_list "$SRC")"
got="$(dev_wipe_keep_list t.rp futures.level-absorption.events t.devkeep futures.level-absorption.closures futures.level-absorption.voice futures.level-absorption.events.archive | paste -sd' ' -)"
want="t.rp t.devkeep futures.level-absorption.voice futures.level-absorption.events.archive"
[ "$got" = "$want" ] && ok "events + closures are removed; a sibling (voice) and a prefix-sharing name (events.archive) stay" \
  || bad "keep list wrong: got [$got] want [$want]"
[ -z "$(dev_wipe_keep_list)" ] && ok "no arguments -> empty list" || bad "no arguments did not give an empty list"
[ -z "$(dev_wipe_keep_list futures.level-absorption.events futures.level-absorption.closures)" ] \
  && ok "a list made only of reset topics gives an empty list and does not fail" || bad "an all-reset list did not come out empty"
case "$DEV_CLEAN_RESET_TOPICS" in
  *"futures.level-absorption.events"*"futures.level-absorption.closures"*|*"futures.level-absorption.closures"*"futures.level-absorption.events"*) ok "the reset set names level-absorption's events and closures" ;;
  *) bad "DEV_CLEAN_RESET_TOPICS does not name events + closures: [$DEV_CLEAN_RESET_TOPICS]" ;;
esac

echo "== 2. the REAL do_clean deletes the reset topics and keeps every other preserved one"
build "$SRC"
out="$(run_do_clean)"
case "$out" in *"REFUSING"*) bad "do_clean refused on the fixture: [$out]" ;; esac
case "$out" in *"REACHED-AFTER-DELETE"*) ok "do_clean ran through the delete step" ;; *) bad "do_clean did not reach the delete step: [$out]" ;; esac
d="$(deleted)"
want_del="futures.level-absorption.closures futures.level-absorption.events futures.level-absorption.events.archive futures.level-absorption.voice t.other"
[ "$d" = "$want_del" ] && ok "deleted exactly: events, closures, voice, events.archive, t.other" || bad "deleted [$d], wanted [$want_del]"
for keep in t.rp t.devkeep __consumer_offsets _schemas; do
  case " $d " in *" $keep "*) bad "$keep was DELETED but must be kept" ;; *) ok "$keep is kept" ;; esac
done
case "$out" in *"RESETTING"*"futures.level-absorption.events"*) ok "the log says the events topic is being RESET" ;; *) bad "no RESETTING log line naming events: [$out]" ;; esac
case "$out" in *"preserving"*"t.rp"*"t.devkeep"*) ok "the log lists what is preserved" ;; *) bad "no preserving log line: [$out]" ;; esac
pl="$(printf '%s\n' "$out" | grep 'preserving (' )"
case "$pl" in *"level-absorption.events"*) bad "the preserving line still names the events topic: [$pl]" ;; *) ok "the preserving line no longer names the events topic" ;; esac

echo "== 3. the REAL ensure_topics + the REAL topics.env recreate them at the shape the service demands"
# Deleting the topics is half the repair; the other half is that the topic pass brings them back with retention.ms=-1 and
# the artifact's exact retention.bytes (events) and 7 days (closures). level-absorption-service refuses any other shape.
# The expected cap is READ from the topics.env under test, so this does not become a second copy of the number; the
# number itself is pinned to the retention artifact by scripts/ci/validate-level-absorption-config.sh.
want_bytes="$(grep -oE 'futures\.level-absorption\.events=[0-9]+' "$TOPICS_ENV_FILE" | head -1 | cut -d= -f2)"
[ -n "$want_bytes" ] && ok "topics.env declares a retention.bytes for futures.level-absorption.events ($want_bytes)" \
  || bad "topics.env declares NO retention.bytes for futures.level-absorption.events"
for f in topic_desired reconcile_declared_topics ensure_topics create_topics_parallel kafka_list_topics; do
  body="$(fn "$f" "$SRC")"; [ -n "$body" ] || { echo "FAIL: function $f not found in $SRC" >&2; exit 1; }
  printf '%s\n' "$body"
done > "$WORK/topicfns.sh"
cat > "$WORK/fake-kt" <<'EOF'
#!/usr/bin/env bash
# A stateful fake broker: --create records the topic in the list and in the describe output with the configs it was
# created with, so the REAL reconcile pass sees what a real broker would show and ensure_topics can truly succeed.
case " $* " in
  *" --list "*)     cat "$FAKE_DIR/list" ;;
  *" --describe "*) cat "$FAKE_DIR/describe" ;;
  *" --create "*)
    echo "$*" >> "$FAKE_DIR/created"
    topic=""; parts=1; cfgs=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --topic) topic=$2; shift ;;
        --partitions) parts=$2; shift ;;
        --config) cfgs="$cfgs${cfgs:+,}$2"; shift ;;
      esac
      shift
    done
    echo "$topic" >> "$FAKE_DIR/list"
    printf 'Topic: %s\tTopicId: x\tPartitionCount: %s\tReplicationFactor: 1\tConfigs: %s\n' "$topic" "$parts" "$cfgs" >> "$FAKE_DIR/describe"
    ;;
esac
exit 0
EOF
cat > "$WORK/fake-kc" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_DIR/altered"
exit 0
EOF
chmod +x "$WORK/fake-kt" "$WORK/fake-kc"
export FAKE_DIR="$WORK/fakek"; mkdir -p "$FAKE_DIR"
# After the wipe the broker holds the preserved topics and its system topics; the two reset topics are GONE.
printf '%s\n' t.rp __consumer_offsets > "$FAKE_DIR/list"; : > "$FAKE_DIR/created"; : > "$FAKE_DIR/altered"
# describe: the preserved topic is there (undeclared, so reconcile ignores it); the created ones are added by the fake.
printf 'Topic: t.rp\tTopicId: x\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: \n' > "$FAKE_DIR/describe"
(
  KT="$WORK/fake-kt"; KC="$WORK/fake-kc"; BS=fake:9092; DEPLOY_REPO=/nonexistent; TOPICS_ENV_REF=origin/main:scripts/kafka/topics.env
  TOPICS_ENV="$(cat "$TOPICS_ENV_FILE")"
  git() { case " $* " in *" show "*) printf '%s\n' "$TOPICS_ENV" ;; esac; return 0; }
  ensure_partition_only_topics() { :; }
  ensure_dev_only_estimate_topics() { :; }
  topic_count() { echo 0; }
  # shellcheck source=/dev/null
  . "$WORK/topicfns.sh"
  export TOPICS_ENV KT BS
  ensure_topics >"$WORK/ensure.out" 2>&1; echo $? > "$WORK/ensure.rc"
)
if [ "$(cat "$WORK/ensure.rc" 2>/dev/null)" = 0 ]; then ok "the real ensure_topics SUCCEEDS over the real topics.env (every declared topic present at its declared shape)"
else bad "the real ensure_topics FAILED over the real topics.env (rc=$(cat "$WORK/ensure.rc" 2>/dev/null)): $(grep -E 'WARNING|ERROR|DRIFT|MISSING' "$WORK/ensure.out" | head -3 | tr '\n' '|')"; fi
created="$(cat "$FAKE_DIR/created")"
ev="$(printf '%s\n' "$created" | grep -- '--topic futures.level-absorption.events ' | head -1)"
cl="$(printf '%s\n' "$created" | grep -- '--topic futures.level-absorption.closures ' | head -1)"
if [ -z "$ev" ]; then bad "the topic pass did not recreate futures.level-absorption.events at all"
else
  case "$ev" in *"--partitions 1 "*) ok "events is recreated with ONE partition" ;; *) bad "events partitions wrong: [$ev]" ;; esac
  case "$ev" in *"--config retention.ms=-1"*) ok "events is recreated with retention.ms=-1" ;; *) bad "events lost retention.ms=-1: [$ev]" ;; esac
  case "$ev" in *"--config retention.bytes=$want_bytes"*) ok "events is recreated with retention.bytes=$want_bytes" ;; *) bad "events not recreated with retention.bytes=$want_bytes: [$ev]" ;; esac
  case "$ev" in *"cleanup.policy=delete"*) ok "events is a delete-policy topic" ;; *) bad "events cleanup.policy wrong: [$ev]" ;; esac
fi
if [ -z "$cl" ]; then bad "the topic pass did not recreate futures.level-absorption.closures at all"
else
  case "$cl" in *"--config retention.ms=604800000"*) ok "closures is recreated with 7-day retention" ;; *) bad "closures lost its 7-day retention: [$cl]" ;; esac
  case "$cl" in *retention.bytes*) bad "closures got a retention.bytes it does not declare: [$cl]" ;; *) ok "closures carries no retention.bytes" ;; esac
fi

echo "== 4. the session-bound report names a cause only when the log confirms it"
eval "$(fn session_bound_line "$SRC")"; eval "$(fn report_session_bound_services "$SRC")"
kk_stub() {
  case "$*" in
    *"get deploy"*) [ "${FAKE_ABSENT:-0}" = 1 ] && return 1
                    case "$*" in *spec.replicas*) echo "${FAKE_DES:-1}" ;; *readyReplicas*) echo "${FAKE_RR:-}" ;; esac ;;
    *"--previous"*) printf '%s\n' "${FAKE_PREV:-}"; return "${FAKE_PREV_RC:-0}" ;;
    *logs*)         printf '%s\n' "${FAKE_LOG:-}"; return "${FAKE_LOG_RC:-0}" ;;
  esac
}
KK=kk_stub
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='x ES_AMT_RETENTION_SHORT partition=0' session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
case "$r" in *"WAIT"*"es-amt-service"*"WHY-TEXT"*) ok "not ready + condition in the log -> WAIT with the reason" ;; *) bad "confirmed condition not reported: [$r]" ;; esac
case "$r" in *"after this wipe"*|*"expected after"*) bad "the WAIT line blames a wipe: [$r]" ;; *) ok "the WAIT line states the condition, not that a wipe caused it" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='nothing relevant' session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
case "$r" in *"???"*"does NOT show"*) ok "not ready + log read + condition absent -> not confirmed" ;; *) bad "unconfirmed condition mislabelled: [$r]" ;; esac
case "$r" in *"WHY-TEXT"*) bad "the reason text was printed without confirmation: [$r]" ;; *) ok "the reason is not printed without confirmation" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='' FAKE_LOG_RC=1 session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
case "$r" in *"???"*"could NOT be read"*"neither confirmed nor ruled out"*) ok "an UNREADABLE log is reported as unreadable, neither confirmed nor ruled out" ;; *) bad "an unreadable log was misreported: [$r]" ;; esac
case "$r" in *"does NOT show"*|*"not the wipe"*|*"WHY-TEXT"*) bad "an unreadable log was read as 'condition absent': [$r]" ;; *) ok "an unreadable log is never read as the condition being absent" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='x ES_AMT_RETENTION_SHORT partial output' FAKE_LOG_RC=1 FAKE_PREV='' FAKE_PREV_RC=1 session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
case "$r" in *"WAIT"*|*"WHY-TEXT"*) bad "partial output of a FAILED kubectl logs was taken as a confirmation: [$r]" ;; *"could NOT be read"*) ok "text printed by a FAILED kubectl logs is not a confirmation" ;; *) bad "failed-read-with-text case misreported: [$r]" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='' FAKE_LOG_RC=1 FAKE_PREV='this session has already ended' FAKE_PREV_RC=0 session_bound_line market-structure-service market-structure 'this session has already ended' 'WHY2')"
case "$r" in *"WAIT"*"WHY2"*) ok "an unreadable CURRENT log (CrashLoopBackOff) with a confirming --previous log is still confirmed" ;; *) bad "crash-loop case lost: [$r]" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='x' FAKE_PREV='this session has already ended' session_bound_line market-structure-service market-structure 'this session has already ended' 'WHY2')"
case "$r" in *"WAIT"*"WHY2"*) ok "the condition is also found in the PREVIOUS container's log (crash loop)" ;; *) bad "previous-container log ignored: [$r]" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='x ES_AMT_RETENTION_SHORT' FAKE_PREV_RC=1 session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
case "$r" in *"WAIT"*) ok "a missing previous container (normal) does not hide a confirmed condition" ;; *) bad "a missing previous container broke the report: [$r]" ;; esac
r="$(FAKE_DES=1 FAKE_RR=1 session_bound_line es-amt-service es-amt-service X W)"
case "$r" in *"OK"*"ready"*) ok "a ready deployment is reported OK" ;; *) bad "ready deployment not reported OK: [$r]" ;; esac
r="$(FAKE_DES=0 FAKE_RR= session_bound_line es-amt-service es-amt-service X W)"
[ -z "$r" ] && ok "a held-down deployment prints nothing" || bad "held-down deployment printed: [$r]"
r="$(FAKE_ABSENT=1 session_bound_line es-amt-service es-amt-service X W)"
[ -z "$r" ] && ok "an absent deployment prints nothing" || bad "absent deployment printed: [$r]"
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='ES_AMT_RETENTION_SHORT' report_session_bound_services; echo "rc=$?")"
case "$r" in *"es-amt-service"*"market-structure-service"*"rc=0"*) ok "the report covers both services and returns 0" ;; *) bad "report wrong: [$r]" ;; esac

echo "== 5. the dispatch carries the mode's status to the exit"
mk_driver() { # $1 = script to cut from, $2 = mode, $3 = do_clean rc, $4 = do_start rc, $5 = clean_guard rc, $6 = DKC_DRYRUN -> prints the exit status
  local s="$1"
  {
    echo 'set -uo pipefail'
    echo "do_clean() { return $3; }; do_start() { return $4; }; do_start_overnight() { return $4; }; do_es_down() { return 0; }; clean_logs() { return 0; }"
    echo "clean_guard() { return ${5:-0}; }; LOG=/dev/null; DKC_DRYRUN=${6:-0}"
    echo 'MODE="$1"'
    awk '/^# MAIN_RC carries the verdict/{p=1} p&&/^  auto\)/{exit} p{print}' "$s"
    echo '  *) ;; esac'
    awk '/^# EXIT STATUS CARRIES THE MIRROR VERDICT/{p=1} p{print}' "$s"
  } > "$WORK/driver.sh"
  grep -q 'MAIN_RC' "$WORK/driver.sh" || { echo "no-main-rc"; return; }
  bash "$WORK/driver.sh" "$2" >/dev/null 2>&1; echo $?
}
[ "$(mk_driver "$SRC" now 1 0)" = 1 ] && ok "now: a refused/failed clean (do_clean rc=1) exits 1" || bad "now exited $(mk_driver "$SRC" now 1 0) after do_clean failed"
[ "$(mk_driver "$SRC" now 0 0)" = 0 ] && ok "now: a good clean exits 0" || bad "now exited non-zero after a good clean"
[ "$(mk_driver "$SRC" start 0 1)" = 1 ] && ok "start: a failed start (do_start rc=1) exits 1" || bad "start exited $(mk_driver "$SRC" start 0 1) after do_start failed"
[ "$(mk_driver "$SRC" start 0 0)" = 0 ] && ok "start: a good start exits 0" || bad "start exited non-zero after a good start"
[ "$(mk_driver "$SRC" overnight 0 1)" = 1 ] && ok "overnight: a failed start exits 1" || bad "overnight exited $(mk_driver "$SRC" overnight 0 1)"
[ "$(mk_driver "$SRC" now 0 0 1 1)" = 1 ] && ok "now (DKC_DRYRUN=1): a refusing clean_guard exits 1" || bad "dry-run now exited $(mk_driver "$SRC" now 0 0 1 1) after clean_guard refused"
[ "$(mk_driver "$SRC" now 0 0 0 1)" = 0 ] && ok "now (DKC_DRYRUN=1): an allowing clean_guard exits 0" || bad "dry-run now exited non-zero though clean_guard allowed"

echo "== 6. mutants: this whole file, run against a copy with one fix removed, must fail"
if [ "${POST_WIPE_NO_MUTANTS:-0}" = 1 ]; then
  echo "  (skipped: this is a mutant run)"
else
python3 - "$SRC" "$TOPICS_ENV_FILE" "$WORK" <<'EOF'
import sys
s = open(sys.argv[1]).read(); t = open(sys.argv[2]).read(); w = sys.argv[3]
def put(name, text, orig, anchor):
    assert anchor in orig, "mutant %s: anchor not found: %r" % (name, anchor)
    open(w + "/" + name, "w").write(text)
a = 'KEEP_NAMES="$(dev_wipe_keep_list $RESET_PRESERVED_TOPICS $DEV_KEEP)"'
put("mut_keep.sh", s.replace(a, 'KEEP_NAMES="$(printf \'%s\\n\' $RESET_PRESERVED_TOPICS $DEV_KEEP)"', 1), s, a)
h = s.index("dev_wipe_keep_list() {"); e = s.index("\n}\n", h)
b = s[h:e]; bm = b.replace('| grep -vxF -e "$reset" || true', '| cat')
assert bm != b, "mutant helper: anchor not found"
open(w + "/mut_helper.sh", "w").write(s[:h] + bm + s[e:])
c1 = "do_start || MAIN_RC=$?"; c2 = "do_clean || MAIN_RC=$?"
assert c1 in s and c2 in s, "mutant rc: anchors not found"
open(w + "/mut_rc.sh", "w").write(s.replace(c1, "do_start", 1).replace(c2, "do_clean", 1))
d = 'clean_guard || MAIN_RC=$?; [ "$MAIN_RC" -eq 0 ] && echo "DRYRUN: would do_clean (WIPE)"'
put("mut_dry.sh", s.replace(d, 'clean_guard && echo "DRYRUN: would do_clean (WIPE)"', 1), s, d)
f = 'if printf \'%s\\n\' "$hay" | grep -qE "$pat"; then'
put("mut_report.sh", s.replace(f, 'if true; then', 1), s, f)
g = 'elif [ "$crc" -ne 0 ]; then'
put("mut_unreadable.sh", s.replace(g, 'elif false; then', 1), s, g)
k = '[ "$crc" -eq 0 ] && hay="$cur"'
put("mut_partial.sh", s.replace(k, 'hay="$cur"', 1), s, k)
# topics.env: the events cap is dropped from the declaration (a wipe would then recreate it uncapped)
lines = t.split("\n"); out = []; n = 0
for L in lines:
    if "futures.level-absorption.events=81675540" in L and "RETENTION_BYTES_OVERRIDES" in L:
        L = L.replace(" futures.level-absorption.events=81675540", ""); n += 1
    out.append(L)
assert n >= 1, "mutant topics.env: the events cap declaration was not found"
open(w + "/mut_topics.env", "w").write("\n".join(out))
EOF
  # mutant_must_fail NAME EXPECTED_FAIL_LABEL ENV=VALUE...: run this whole file against a mutant. It must exit non-zero AND
  # print a FAIL line matching EXPECTED_FAIL_LABEL -- the assertion that guards the removed fix. A child that dies in the
  # fixture or the extraction (no FAIL line, or a different one) is NOT a catch.
  mutant_must_fail() {
    local name="$1" label="$2" rc nfail
    shift 2
    env "$@" POST_WIPE_NO_MUTANTS=1 bash "$SELF" >"$WORK/mut.$name.out" 2>&1; rc=$?
    nfail="$(grep -c '^  FAIL' "$WORK/mut.$name.out")"
    if [ "$rc" -eq 0 ]; then bad "mutant $name was NOT caught: the whole file still passes"
    elif [ "$nfail" -lt 1 ]; then bad "mutant $name: the file exited $rc but printed no FAIL assertion (harness/fixture failure, not a catch)"
    elif grep -q "^  FAIL .*$label" "$WORK/mut.$name.out"; then ok "mutant $name: the whole file fails on its own guard ('$label'; $nfail failing assertion(s))"
    else bad "mutant $name: the file fails but NOT on the expected guard '$label': $(grep '^  FAIL' "$WORK/mut.$name.out" | head -2 | cut -c1-120 | tr '\n' '|')"; fi
  }
  for spec in "keep|deleted \\[" "helper|keep list wrong" "rc|after do_clean failed" "dry|dry-run now exited" \
              "report|reason text was printed without confirmation" "unreadable|unreadable log was misreported" \
              "partial|partial output of a FAILED kubectl logs"; do
    m="${spec%%|*}"; label="${spec#*|}"
    [ -s "$WORK/mut_$m.sh" ] || { bad "mutant $m was not produced"; continue; }
    mutant_must_fail "$m" "$label" DEV_CLEANUP_UNDER_TEST="$WORK/mut_$m.sh"
  done
  [ -s "$WORK/mut_topics.env" ] || bad "mutant topics.env was not produced"
  [ -s "$WORK/mut_topics.env" ] && mutant_must_fail topics.env "declares NO retention.bytes" TOPICS_ENV_UNDER_TEST="$WORK/mut_topics.env"
fi

echo
[ "$fails" -eq 0 ] && { echo "ALL OK"; exit 0; }
echo "$fails FAILED"; exit 1
