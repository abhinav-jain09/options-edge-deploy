#!/usr/bin/env bash
# What a dev wipe leaves behind for its dependents (owner decision 2026-11-10: "reset on every dev wipe").
#
# THE DEFECT. futures.level-absorption.events is on the shared RESET-PRESERVED list (evidence on prod), so the dev
# wipe KEPT it while deleting the inputs level-absorption-service replays from. The service then refused to start
# (REPLAY_SHORT) after every dev clean and was repaired by hand: delete events + closures, recreate through the
# barrier. The same clean left es-amt-service and market-structure-service unready for reasons that are not faults,
# and `dev-cleanup now` / `start` exited 0 after refusing or failing, so nothing but the log said so.
#
# Sections (no broker, no cluster, no docker; every function is lifted out BY NAME from the file under test and
# run against stubs, because the script itself dispatches on its arguments and would start doing things):
#   1  dev_wipe_keep_list removes EXACTLY the reset set and nothing near it
#   2  the REAL do_clean deletes level-absorption's events + closures and still keeps every other preserved topic
#   3  session_bound_line / report_session_bound_services name the cause only when the log confirms it
#   4  the dispatch carries do_clean / do_start / do_start_overnight's status to the exit
#   5  mutants: take each fix out of a copy of the script and require THIS FILE to fail
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
RPT_REAL="$HERE/../kafka/reset-preserved-topics.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
for f in "$SRC" "$RPT_REAL"; do [ -f "$f" ] || { echo "FAIL: $f missing" >&2; exit 1; }; done

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

echo "== 3. the session-bound report names a cause only when the log confirms it"
eval "$(fn session_bound_line "$SRC")"; eval "$(fn report_session_bound_services "$SRC")"
kk_stub() {
  case "$*" in
    *"get deploy"*) [ "${FAKE_ABSENT:-0}" = 1 ] && return 1
                    case "$*" in *spec.replicas*) echo "${FAKE_DES:-1}" ;; *readyReplicas*) echo "${FAKE_RR:-}" ;; esac ;;
    *"--previous"*) printf '%s\n' "${FAKE_PREV:-}" ;;
    *logs*)         printf '%s\n' "${FAKE_LOG:-}" ;;
  esac
}
KK=kk_stub
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='x ES_AMT_RETENTION_SHORT partition=0' session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
case "$r" in *"WAIT"*"es-amt-service"*"WHY-TEXT"*) ok "not ready + cause in the log -> WAIT with the reason" ;; *) bad "confirmed cause not reported: [$r]" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='nothing relevant' session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
case "$r" in *"???"*"NOT the wipe's doing"*|*"???"*"does NOT show"*) ok "not ready + cause absent -> says it is NOT the known cause" ;; *) bad "unconfirmed cause was blamed on the wipe: [$r]" ;; esac
case "$r" in *"WHY-TEXT"*) bad "the reason text was printed without confirmation: [$r]" ;; *) ok "the reason is not printed without confirmation" ;; esac
r="$(FAKE_DES=1 FAKE_RR= FAKE_PREV='this session has already ended' session_bound_line market-structure-service market-structure 'this session has already ended' 'WHY2')"
case "$r" in *"WAIT"*"WHY2"*) ok "the cause is also found in the PREVIOUS container's log (crash loop)" ;; *) bad "previous-container log ignored: [$r]" ;; esac
r="$(FAKE_DES=1 FAKE_RR=1 session_bound_line es-amt-service es-amt-service X W)"
case "$r" in *"OK"*"ready"*) ok "a ready deployment is reported OK" ;; *) bad "ready deployment not reported OK: [$r]" ;; esac
r="$(FAKE_DES=0 FAKE_RR= session_bound_line es-amt-service es-amt-service X W)"
[ -z "$r" ] && ok "a held-down deployment prints nothing" || bad "held-down deployment printed: [$r]"
r="$(FAKE_ABSENT=1 session_bound_line es-amt-service es-amt-service X W)"
[ -z "$r" ] && ok "an absent deployment prints nothing" || bad "absent deployment printed: [$r]"
r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='ES_AMT_RETENTION_SHORT' report_session_bound_services; echo "rc=$?")"
case "$r" in *"es-amt-service"*"market-structure-service"*"rc=0"*) ok "the report covers both services and returns 0" ;; *) bad "report wrong: [$r]" ;; esac

echo "== 4. the dispatch carries the mode's status to the exit"
mk_driver() { # $1 = script to cut from, $2 = mode, $3 = do_clean rc, $4 = do_start rc -> prints the exit status
  local s="$1"
  {
    echo 'set -uo pipefail'
    echo "do_clean() { return $3; }; do_start() { return $4; }; do_start_overnight() { return $4; }; do_es_down() { return 0; }; clean_logs() { return 0; }"
    echo "clean_guard() { return 0; }; LOG=/dev/null"
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

echo "== 5. mutants: each fix removed from a copy of the script must make this file fail"
python3 - "$SRC" "$WORK" <<'EOF'
import re, sys
s = open(sys.argv[1]).read(); w = sys.argv[2]
# (a) do_clean ignores the reset set: the keep set is the raw concatenation again
a = 'KEEP_NAMES="$(dev_wipe_keep_list $RESET_PRESERVED_TOPICS $DEV_KEEP)"'
assert a in s, "mutant a: anchor not found"
open(w + "/mut_keep.sh", "w").write(s.replace(a, 'KEEP_NAMES="$(printf \'%s\\n\' $RESET_PRESERVED_TOPICS $DEV_KEEP)"', 1))
# (b) the helper stops removing anything
h = s.index("dev_wipe_keep_list() {"); e = s.index("\n}\n", h)
body = s[h:e]
m = body.replace('| grep -vxF -e "$reset" || true', '| cat')
assert m != body, "mutant b: anchor not found"
open(w + "/mut_helper.sh", "w").write(s[:h] + m + s[e:])
# (c) the dispatch drops the status again
c = s.replace("do_start || MAIN_RC=$?", "do_start", 1).replace("do_clean || MAIN_RC=$?", "do_clean", 1)
assert c != s, "mutant c: anchor not found"
open(w + "/mut_rc.sh", "w").write(c)
# (d) the report prints the reason without confirming it
d = s.replace('if [ "${hits:-0}" -gt 0 ]; then', 'if true; then', 1)
assert d != s, "mutant d: anchor not found"
open(w + "/mut_report.sh", "w").write(d)
EOF
for m in mut_keep mut_helper mut_rc mut_report; do [ -s "$WORK/$m.sh" ] || { bad "mutant $m was not produced"; continue; }; done

if [ -s "$WORK/mut_keep.sh" ]; then
  build "$WORK/mut_keep.sh"; mo="$(run_do_clean)"; md="$(deleted)"
  case " $md " in *" futures.level-absorption.events "*) bad "mutant (a) still deleted events: the test cannot see do_clean ignoring the reset set" ;; *) ok "mutant (a) do_clean ignores the reset set -> events is KEPT, so section 2 fails (caught)" ;; esac
fi
if [ -s "$WORK/mut_helper.sh" ]; then
  build "$WORK/mut_helper.sh"; mo="$(run_do_clean)"; md="$(deleted)"
  case " $md " in *" futures.level-absorption.events "*) bad "mutant (b) still deleted events" ;; *) ok "mutant (b) the helper removes nothing -> events is KEPT (caught)" ;; esac
fi
if [ -s "$WORK/mut_rc.sh" ]; then
  [ "$(mk_driver "$WORK/mut_rc.sh" now 1 0)" = 0 ] && ok "mutant (c) the status is dropped -> now exits 0 after a failed clean (caught)" || bad "mutant (c) was not caught"
fi
if [ -s "$WORK/mut_report.sh" ]; then
  # shellcheck disable=SC1090
  eval "$(fn session_bound_line "$WORK/mut_report.sh")"
  r="$(FAKE_DES=1 FAKE_RR= FAKE_LOG='nothing relevant' session_bound_line es-amt-service es-amt-service ES_AMT_RETENTION_SHORT 'WHY-TEXT')"
  case "$r" in *"WHY-TEXT"*) ok "mutant (d) the cause is printed unconfirmed -> section 3 sees WHY-TEXT (caught)" ;; *) bad "mutant (d) was not caught: [$r]" ;; esac
fi

echo
[ "$fails" -eq 0 ] && { echo "ALL OK"; exit 0; }
echo "$fails FAILED"; exit 1
