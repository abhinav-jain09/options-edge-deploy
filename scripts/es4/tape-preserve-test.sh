#!/usr/bin/env bash
# tape-preserve-test.sh — pins the contract of tape-preserve.sh and how cleanup-es4.sh wires it.
#
# No broker is needed: the Java tool is replaced by a stub that records its arguments and answers from
# files each case writes. What this proves is the WRAPPER's decisions (what is exported, when an
# artifact is trusted, what is kept or retired, that nothing here can abort a clean) and the ORDER of the
# two calls inside cleanup-es4.sh. The tool itself was proven on real data against a live broker — see
# the PR description (byte-identical restore of 39,304 records, truncate-on-failure, non-empty refusal).
#
#   Optional compile check:  KAFKA_LIBS=/path/to/kafka/libs bash scripts/es4/tape-preserve-test.sh
#   (skipped when javac or kafka-clients is not present — CI images may lack both)
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
case_() { printf '\n%s\n' "$1"; }
W="$(cd "$(mktemp -d)" && pwd -P)"
# Never rm -rf (owner rule): empty the mktemp dir we just created with a bounded find, then remove it.
trap 'find "$W" -mindepth 1 -delete 2>/dev/null; rmdir "$W" 2>/dev/null' EXIT

command -v timeout >/dev/null 2>&1 || { command -v gtimeout >/dev/null 2>&1 && timeout() { gtimeout "$@"; }; }
command -v timeout >/dev/null 2>&1 || type timeout >/dev/null 2>&1 || { echo "FATAL: coreutils timeout required" >&2; exit 2; }

# ---- the stub "java": logs every call, behaves per $FIX/<cmd>.rc (default 0), and for `export` writes a
# tape + manifest holding $FIX/export.records records (default 5).
FIX="$W/fix"; mkdir -p "$FIX"; export FIX
cat > "$W/java" <<'SH'
#!/usr/bin/env bash
cmd=""; args=("$@")
for i in "${!args[@]}"; do
  case "${args[$i]}" in export|import|coverage|truncate|fingerprint|state|pin|topicid) cmd="${args[$i]}"; idx=$i; break ;; esac
done
echo "$cmd ${args[*]:$((idx+1))}" >> "$FIX/calls"
rc=$(cat "$FIX/$cmd.rc" 2>/dev/null || echo 0)
if [ "$cmd" = topicid ]; then [ -f "$FIX/topicid.fail" ] && exit 1; echo "TAPE_TOPIC_ID $(cat "$FIX/topicid" 2>/dev/null || echo AAAAAAAAAAAAAAAAAAAAA1)"; exit 0; fi
if [ "$cmd" = state ]; then
  st=$(head -1 "$FIX/state.seq" 2>/dev/null)
  if [ -n "$st" ]; then tail -n +2 "$FIX/state.seq" > "$FIX/state.seq.n"; mv "$FIX/state.seq.n" "$FIX/state.seq"
  elif [ -f "$FIX/imported" ]; then st=COMPLETE; else st=EMPTY; fi
  echo "TAPE_STATE $st live=1 expected=1"; exit 0
fi
if [ "$cmd" = export ] && [ "$rc" = 0 ]; then
  out="${args[$((idx+4))]}"; n=$(cat "$FIX/export.records" 2>/dev/null || echo 5)
  : > "$out"; printf 'records=%s\ntopic=%s\nmaxTs=%s\n' "$n" "${args[$((idx+2))]}" "$(cat "$FIX/export.maxts" 2>/dev/null || echo 9999999999999)" > "$out.manifest"
  echo "TAPE_EXPORTED topic=${args[$((idx+2))]} records=$n bytes=123 groups=es-trades-bridge-x,indicator-service-es4"
fi
if [ "$cmd" = import ] && [ "$rc" = 0 ]; then
  [ "$(cat "$FIX/import.lie" 2>/dev/null)" = 1 ] || : > "$FIX/imported"
  echo "TAPE_IMPORTED topic=x records=5 partitions=4"
  case "$(cat "$FIX/import.pin" 2>/dev/null || echo ok)" in
    ok)   echo "TAPE_PINNED group=es-trades-bridge-x partitions=4"; echo "TAPE_PINNED group=indicator-service-es4 partitions=4" ;;
    fail) echo "TAPE_PINNED group=indicator-service-es4 partitions=4"; echo "TAPE_PIN_FAILED group=es-trades-bridge-x reason=GroupNotEmptyException: busy" ;;
  esac
fi
[ "$cmd" = import ] && [ "$rc" = 5 ] && echo "TAPE_IMPORT_GROUP_ACTIVE group=es-trades-bridge-x members=1"
[ "$cmd" = import ] && [ "$rc" = 6 ] && echo "TAPE_IMPORT_TIMESTAMP_TYPE topic=x type=LogAppendTime (needs CreateTime)"
[ "$cmd" = import ] && [ "$rc" = 7 ] && echo "TAPE_IMPORT_ROLLED_BACK topic=x reason=pin-or-fence groups=es-trades-bridge-x"
[ "$cmd" = import ] && [ "$rc" = 7 ] && [ "$(cat "$FIX/import.breach" 2>/dev/null)" = 1 ] && echo "TAPE_IMPORT_FENCE_BREACH topic=x groups=intruder-group - they may have read part of the restored tape"
[ "$cmd" = import ] && [ "$rc" = 4 ] && echo "TAPE_IMPORT_TARGET_NOT_EMPTY topic=x liveRecords=9"
[ "$cmd" = import ] && [ "$rc" = 1 ] && echo "TAPE_PRESERVE_ERROR java.io.IOException: boom" >&2
exit "$rc"
SH
chmod +x "$W/java"

calls() { grep -c "^$1" "$FIX/calls" 2>/dev/null || true; }
fresh() { find "$FIX" "$W/art" -mindepth 1 -delete 2>/dev/null; mkdir -p "$FIX" "$W/art"; : > "$FIX/calls"; }
# Run a function from the real library in a subshell with the stub wired in. Output -> $W/out.
tp() {
  ( export ES4_TAPE_JAVA="$W/java" ES4_TAPE_PRESERVE_DIR="$W/art" ES4_TAPE_BOOTSTRAP=stub:9092 \
           ES4_TAPE_PRESERVE_TOPICS="${TOPICS:-es.underlying.es.trades}" FIX="$FIX" DRY="${DRY:-false}"
    [ "${TP_FAIL_MV:-0}" = 1 ] && mv() { return 1; }
    # fails only the publish of a NEW manifest (the second rename of an export), nothing else
    [ "${TP_FAIL_MV:-0}" = manifest ] && mv() { local a; for a; do :; done; case "$*" in *.tmp.*) [ "${a##*.}" = manifest ] && return 1 ;; esac; command mv "$@"; }
    [ "${TP_FAIL_RM:-0}" = 1 ] && rm() { return 1; }
    # shellcheck source=/dev/null
    . "$HERE/tape-preserve.sh"; "$@" ) > "$W/out" 2>&1
}

# ---------------------------------------------------------------------------------------------------
case_ "1. switched off — touches nothing"
fresh; ES4_TAPE_PRESERVE=off tp tape_preserve_export; r1=$?
ES4_TAPE_PRESERVE=off tp tape_preserve_import; r2=$?
[ $r1 = 0 ] && [ $r2 = 0 ] && [ ! -s "$FIX/calls" ] && ok "off: both steps return 0 and the tool is never run" || bad "off ran something: $(cat "$FIX/calls")"

case_ "2. DRY run — no tool call, no files"
fresh; DRY=true tp tape_preserve_export; r=$?
[ $r = 0 ] && [ ! -s "$FIX/calls" ] && [ -z "$(ls -A "$W/art")" ] && ok "DRY export: nothing exported, nothing written" || bad "DRY mutated: $(cat "$FIX/calls")"

case_ "3. export success publishes the tape, manifest last"
fresh; tp tape_preserve_export; r=$?
[ $r = 0 ] && [ -f "$W/art/es.underlying.es.trades.tape" ] && [ -f "$W/art/es.underlying.es.trades.tape.manifest" ] \
  && ok "artifact + manifest published" || bad "export did not publish: $(cat "$W/out")"
[ ! -e "$W/art/es.underlying.es.trades.required" ] && ok "no stale side marker: the requirement is recomputed at import time" || bad "a .required marker is still written"
[ -z "$(ls -A "$W/art" | grep '^\.tmp')" ] && ok "no temp files left behind" || bad "temp files left: $(ls -A "$W/art")"

case_ "3b. a failed second rename of a publish must not destroy the previous importable artifact (Codex r3)"
fresh; echo 5 > "$FIX/export.records"; tp tape_preserve_export
before="$(cksum < "$W/art/es.underlying.es.trades.tape.manifest")$(cksum < "$W/art/es.underlying.es.trades.tape")"
echo 9 > "$FIX/export.records"; TP_FAIL_MV=manifest tp tape_preserve_export; r=$?
after="$(cksum < "$W/art/es.underlying.es.trades.tape.manifest" 2>/dev/null)$(cksum < "$W/art/es.underlying.es.trades.tape" 2>/dev/null)"
[ $r = 1 ] && grep -q "previous artifact" "$W/out" && ok "the failed publish is reported (rc 1)" || bad "r=$r out=$(cat "$W/out")"
[ "$before" = "$after" ] && ok "the previous tape+manifest pair is back, byte for byte" || bad "previous pair lost or changed"
[ -z "$(ls -A "$W/art" | grep -E '^(prev|\.tmp)')" ] && ok "no parked or temp files left" || bad "leftovers: $(ls -A "$W/art")"
# a publish killed half-way (new tape in place, no manifest, previous pair parked) is recovered at the next run
fresh; echo 5 > "$FIX/export.records"; tp tape_preserve_export
cp "$W/art/es.underlying.es.trades.tape" "$W/prev.tape"; cp "$W/art/es.underlying.es.trades.tape.manifest" "$W/prev.mf"
mv "$W/art/es.underlying.es.trades.tape" "$W/art/prev.es.underlying.es.trades.tape"; mv "$W/art/es.underlying.es.trades.tape.manifest" "$W/art/prev.es.underlying.es.trades.tape.manifest"
echo partial > "$W/art/es.underlying.es.trades.tape"
tp tp_recover "$W/art" es.underlying.es.trades
if cmp -s "$W/prev.tape" "$W/art/es.underlying.es.trades.tape" && cmp -s "$W/prev.mf" "$W/art/es.underlying.es.trades.tape.manifest" && [ -z "$(ls -A "$W/art" | grep '^prev')" ]; then
  ok "a half-published artifact is rolled back to the previous pair (byte for byte) at the next run"
else bad "no recovery: $(ls -A "$W/art")"; fi

case_ "4. a failing export is non-fatal and KEEPS an earlier artifact (a resumed reset needs it)"
fresh; tp tape_preserve_export; echo 1 > "$FIX/export.rc"
tp tape_preserve_export; r=$?
[ $r = 1 ] && ok "failure is reported (rc 1) for the caller to warn on" || bad "failure rc=$r"
[ -f "$W/art/es.underlying.es.trades.tape" ] && ok "earlier artifact kept" || bad "earlier artifact was deleted"
grep -q "NOT_READY after this clean" "$W/out" && ok "the warning names the consequence" || bad "no consequence in warning: $(cat "$W/out")"

case_ "5. an export of ZERO records (topic already wiped on a resume) keeps the earlier artifact"
fresh; tp tape_preserve_export; echo 0 > "$FIX/export.records"
tp tape_preserve_export; r=$?
[ $r = 0 ] && [ -f "$W/art/es.underlying.es.trades.tape" ] && ok "empty export neither fails nor replaces the artifact" || bad "empty export clobbered: rc=$r"

case_ "6. import with nothing preserved is a quiet no-op"
fresh; tp tape_preserve_import; r=$?
[ $r = 0 ] && [ ! -s "$FIX/calls" ] && ok "no artifact -> rc 0, tool not run" || bad "ran without an artifact"

case_ "7. a STALE artifact is refused — never restore an old tape into an unrelated wipe"
fresh; tp tape_preserve_export
touch -t 202001010000 "$W/art/es.underlying.es.trades.tape.manifest"
: > "$FIX/calls"; tp tape_preserve_import; r=$?
[ $r = 1 ] && [ "$(calls import)" = 0 ] && ok "stale: not imported, rc 1" || bad "stale artifact imported (rc=$r)"
grep -q "refusing to restore a stale tape" "$W/out" && ok "says why" || bad "silent refusal"
[ -f "$W/art/es.underlying.es.trades.tape" ] && ok "stale artifact is left in place (not silently deleted)" || bad "stale artifact deleted"

case_ "8. import success retires the artifact (one export restores into one wipe) and checks coverage"
fresh; tp tape_preserve_export; : > "$FIX/calls"; tp tape_preserve_import; r=$?
[ $r = 0 ] && ok "import ok -> rc 0" || bad "import rc=$r: $(cat "$W/out")"
[ ! -f "$W/art/es.underlying.es.trades.tape" ] && [ -f "$W/art/consumed.es.underlying.es.trades.tape" ] && ok "artifact retired to consumed.*" || bad "artifact not retired: $(ls -A "$W/art")"
[ "$(calls coverage)" = 1 ] && ok "AMT's own coverage test ran after the import" || bad "coverage not checked"
grep -q "will start READY" "$W/out" && ok "reports READY outcome" || bad "no READY line: $(cat "$W/out")"
[ ! -e "$W/art/es.underlying.es.trades.importing" ] && ok "no restore-in-progress marker left after a clean restore" || bad "marker left behind"
: > "$FIX/calls"; tp tape_preserve_import
[ "$(calls import)" = 0 ] && ok "a second import finds nothing to restore (cannot double-import)" || bad "double import"

case_ "9. import onto a topic that already has records refuses (rc 4) and keeps the artifact"
fresh; tp tape_preserve_export; echo 4 > "$FIX/import.rc"; tp tape_preserve_import; r=$?
[ $r = 1 ] && [ -f "$W/art/es.underlying.es.trades.tape" ] && ok "kept; rc 1" || bad "rc=$r artifact=$(ls -A "$W/art")"
grep -q "already holds records" "$W/out" && ok "says why" || bad "silent"

case_ "10. a failed import is settled by inspecting the topic, and keeps the artifact"
fresh; tp tape_preserve_export; echo 1 > "$FIX/import.rc"; tp tape_preserve_import; r=$?
[ $r = 1 ] && [ -f "$W/art/es.underlying.es.trades.tape" ] && ok "kept; rc 1" || bad "rc=$r"
grep -q "rolled back: es.underlying.es.trades is empty" "$W/out" && ok "settled by LOOKING at the topic: it is empty, not half-restored" || bad "no settle result: $(cat "$W/out")"

case_ "8b. the tool claims success (rc 0) but the topic is EMPTY: the wrapper does not believe it"
fresh; tp tape_preserve_export; echo 1 > "$FIX/import.lie"; tp tape_preserve_import; r=$?
[ $r = 1 ] && ok "rc 1: not reported as restored" || bad "rc=$r out=$(cat "$W/out")"
grep -q "rolled back: es.underlying.es.trades is empty" "$W/out" && ! grep -q "restored es.underlying" "$W/out" && ok "settled from the topic's real state, never says 'restored'" || bad "believed the exit code: $(cat "$W/out")"
[ -f "$W/art/es.underlying.es.trades.tape" ] && ok "artifact kept" || bad "artifact lost"

case_ "11. import ok but the tape is still SHORT of the prior RTH open -> warn, still retire"
fresh; tp tape_preserve_export; echo 3 > "$FIX/coverage.rc"; tp tape_preserve_import; r=$?
[ $r = 1 ] && grep -q "does NOT reach the prior RTH open" "$W/out" && ok "short coverage is surfaced, not hidden" || bad "rc=$r out=$(cat "$W/out")"
[ -f "$W/art/consumed.es.underlying.es.trades.tape" ] && ok "the imported artifact is still retired" || bad "not retired"

case_ "9b. a recorded consumer group is ACTIVE -> refuse, name it, keep the artifact (never stream history to prod)"
fresh; tp tape_preserve_export; echo 5 > "$FIX/import.rc"; tp tape_preserve_import; r=$?
[ $r = 1 ] && [ -f "$W/art/es.underlying.es.trades.tape" ] && ok "refused; artifact kept; rc 1" || bad "rc=$r artifact=$(ls -A "$W/art")"
grep -q "es-trades-bridge-x" "$W/out" && grep -q "ACTIVE" "$W/out" && ok "names the active group" || bad "group not named: $(cat "$W/out")"
grep -q "launchctl bootout" "$W/out" && ok "tells the operator how to pause the bridge" || bad "no remedy printed"
[ ! -e "$W/art/es.underlying.es.trades.importing" ] && ok "a refusal (nothing produced) leaves no marker" || bad "marker left after a refusal"

case_ "9c. success pins every recorded group to the restored end and says so"
fresh; tp tape_preserve_export; : > "$FIX/calls"; tp tape_preserve_import; r=$?
[ $r = 0 ] && grep -q "pinned to the restored end: group=es-trades-bridge-x" "$W/out" && grep -q "pinned to the restored end: group=indicator-service-es4" "$W/out" && ok "both groups reported pinned" || bad "pins not reported (rc=$r): $(cat "$W/out")"
grep -q "^import .*\^es-amt-service" "$FIX/calls" && ok "es-amt-service is passed as the group NOT to pin" || bad "skip regex not passed: $(grep '^import' "$FIX/calls")"
: > "$FIX/calls"; fresh; tp tape_preserve_export; ES4_TAPE_UNPINNED_GROUPS='^custom' tp tape_preserve_import
grep -q "^import .*\^custom" "$FIX/calls" && ok "the skip regex is configurable" || bad "override ignored"

case_ "9d. a pin failure rolls the restore back (rc 7): the tool empties the topic, the wrapper proves it"
fresh; tp tape_preserve_export; echo 7 > "$FIX/import.rc"; tp tape_preserve_import; r=$?
[ $r = 1 ] && grep -q "rolled back: es.underlying.es.trades is empty" "$W/out" && ok "rc 1, topic proven empty" || bad "rc=$r out=$(cat "$W/out")"
[ -f "$W/art/es.underlying.es.trades.tape" ] && ok "artifact kept for a retry" || bad "artifact lost"

case_ "9d2. a consumer appeared WHILE the history was being restored (fence breach, rc 7): rolled back and shouted about"
fresh; tp tape_preserve_export; echo 7 > "$FIX/import.rc"; echo 1 > "$FIX/import.breach"; tp tape_preserve_import; r=$?
[ $r = 1 ] && grep -q "ERROR: a consumer started reading" "$W/out" && ok "an ERROR names the breach" || bad "rc=$r out=$(cat "$W/out")"
grep -q "intruder-group" "$W/out" && grep -q "check prod's underlying.es.trades" "$W/out" && ok "names the group and says what to check downstream" || bad "no group / no downstream hint"
[ -f "$W/art/es.underlying.es.trades.tape" ] && ok "topic proven empty and the artifact kept" || bad "artifact lost"

case_ "9e. rc 6 (topic is LogAppendTime) -> nothing produced, says why, artifact kept"
fresh; tp tape_preserve_export; echo 6 > "$FIX/import.rc"; tp tape_preserve_import; r=$?
[ $r = 1 ] && grep -q "LogAppendTime" "$W/out" && ok "refused with the reason" || bad "rc=$r out=$(cat "$W/out")"
[ -f "$W/art/es.underlying.es.trades.tape" ] && ok "artifact kept" || bad "artifact lost"

case_ "10b. the tool is KILLED mid-import (timeout, rc 124) leaving a PARTIAL tape: the wrapper empties it and proves it"
fresh; tp tape_preserve_export; echo 124 > "$FIX/import.rc"; printf 'PARTIAL\nEMPTY\n' > "$FIX/state.seq"; : > "$FIX/calls"
tp tape_preserve_import; r=$?
[ $r = 1 ] && [ "$(calls truncate)" = 1 ] && ok "rc 1 (a warning); the wrapper ran truncate itself" || bad "rc=$r truncates=$(calls truncate)"
grep -q "rolled back" "$W/out" && ok "reports the rollback" || bad "silent: $(cat "$W/out")"
[ ! -e "$W/art/es.underlying.es.trades.importing" ] && ok "a proven rollback clears the marker" || bad "marker left after a proven rollback"

case_ "10c. killed mid-import AND the partial tape cannot be emptied -> UNSAFE (rc 2): the clean must not start apps"
fresh; tp tape_preserve_export; echo 124 > "$FIX/import.rc"; printf 'PARTIAL\nPARTIAL\n' > "$FIX/state.seq"
tp tape_preserve_import; r=$?
[ $r = 2 ] && grep -q "UNSAFE" "$W/out" && ok "rc 2 and an UNSAFE message" || bad "rc=$r out=$(cat "$W/out")"
[ -e "$W/art/es.underlying.es.trades.importing" ] && ok "UNSAFE keeps the marker" || bad "marker cleared although the topic may hold a partial tape"
before="$(cksum < "$W/art/es.underlying.es.trades.tape")"; : > "$FIX/calls"; rm -f "$FIX/import.rc"
tp tape_preserve_export; r=$?
[ "$(calls export)" = 0 ] && [ "$(cksum < "$W/art/es.underlying.es.trades.tape")" = "$before" ] && ok "a resumed reset does NOT re-export the possibly partial topic: the good artifact is untouched" || bad "export ran / artifact changed ($(cat "$FIX/calls"))"
grep -q "NOT exporting" "$W/out" && ok "says why" || bad "silent skip"
# the wedge: the reset is resumed much later, AFTER the wipe has recreated the topic. The marker described the
# old incarnation and must not disable preservation for every later reset.
echo AAAAAAAAAAAAAAAAAAAAA2 > "$FIX/topicid"; : > "$FIX/calls"
touch -t 202001010000 "$W/art/es.underlying.es.trades.tape.manifest"     # the artifact is also stale by now
tp tape_preserve_export; r=$?
[ "$(calls export)" = 1 ] && [ ! -e "$W/art/es.underlying.es.trades.importing" ] && ok "after the topic is recreated the marker is obsolete: cleared, and a fresh export runs (no permanent wedge)" || bad "wedged: calls=$(cat "$FIX/calls") marker=$(ls -A "$W/art" | grep importing)"
grep -q "marker for .* is obsolete" "$W/out" && ok "says the marker was obsolete" || bad "silent"
[ "$(find "$W/art/es.underlying.es.trades.tape.manifest" -newer "$FIX/topicid" | wc -l | tr -d ' ')" = 1 ] && ok "the stale artifact was replaced by the fresh export" || bad "stale artifact survived"
# topic gone entirely
fresh; tp tape_preserve_export; echo 124 > "$FIX/import.rc"; printf 'PARTIAL\nPARTIAL\n' > "$FIX/state.seq"; tp tape_preserve_import >/dev/null
echo ABSENT > "$FIX/topicid"; rm -f "$FIX/import.rc" "$FIX/state.seq"; : > "$FIX/calls"; tp tape_preserve_export
[ "$(calls export)" = 1 ] && ok "a marker for a topic that no longer exists is obsolete too" || bad "blocked on an absent topic"
# the id cannot be read (broker down): the marker cannot be proven obsolete, so it keeps protecting the artifact
fresh; tp tape_preserve_export; echo 124 > "$FIX/import.rc"; printf 'PARTIAL\nPARTIAL\n' > "$FIX/state.seq"; tp tape_preserve_import >/dev/null
: > "$FIX/topicid.fail"; rm -f "$FIX/import.rc" "$FIX/state.seq"; : > "$FIX/calls"
tp tape_preserve_export
[ "$(calls export)" = 0 ] && [ -e "$W/art/es.underlying.es.trades.importing" ] && grep -q "NOT exporting" "$W/out" && ok "an unreadable topic id keeps the marker in force (fail toward protecting the artifact)" || bad "marker dropped without proof: $(cat "$W/out")"
rm -f "$FIX/topicid.fail"
# Codex r3: the marker must never be written without a usable id, and an id-less marker must not age-clear.
fresh; tp tape_preserve_export; : > "$FIX/topicid.fail"; : > "$FIX/calls"
tp tape_preserve_import; r=$?
[ $r = 1 ] && [ "$(calls import)" = 0 ] && [ ! -e "$W/art/es.underlying.es.trades.importing" ] && [ -f "$W/art/es.underlying.es.trades.tape" ] \
  && ok "an unreadable topic id at import time: nothing imported, no marker written, artifact kept (rc 1)" || bad "r=$r calls=$(cat "$FIX/calls") out=$(cat "$W/out")"
rm -f "$FIX/topicid.fail"
fresh; tp tape_preserve_export; echo ABSENT > "$FIX/topicid"; : > "$FIX/calls"
tp tape_preserve_import; r=$?
[ $r = 1 ] && [ "$(calls import)" = 0 ] && [ ! -e "$W/art/es.underlying.es.trades.importing" ] && ok "an ABSENT topic id is refused the same way" || bad "ABSENT: r=$r calls=$(cat "$FIX/calls")"
echo AAAAAAAAAAAAAAAAAAAAA1 > "$FIX/topicid"
# a marker from before ids were recorded (just an epoch), 25h old, the topic still holding records: age must NOT clear it
fresh; tp tape_preserve_export; date +%s > "$W/art/es.underlying.es.trades.importing"
touch -t 202001010000 "$W/art/es.underlying.es.trades.importing" "$W/art/es.underlying.es.trades.tape.manifest"
printf 'PARTIAL\n' > "$FIX/state.seq"; : > "$FIX/calls"
tp tape_preserve_export
[ "$(calls export)" = 0 ] && [ -e "$W/art/es.underlying.es.trades.importing" ] && grep -q "NOT exporting" "$W/out" && ok "an id-less marker survives 24h+ while the topic still holds a partial tape (no age expiry)" || bad "id-less marker age-cleared: calls=$(cat "$FIX/calls")"
# ...and is cleared once the topic is PROVEN empty
printf 'EMPTY\n' > "$FIX/state.seq"; : > "$FIX/calls"
tp tape_preserve_export
[ "$(calls export)" = 1 ] && [ ! -e "$W/art/es.underlying.es.trades.importing" ] && ok "an id-less marker is cleared when the topic is proven EMPTY" || bad "not cleared on proven-empty: calls=$(cat "$FIX/calls")"
rm -f "$FIX/state.seq"

case_ "10d. killed after the records landed but before pinning (state COMPLETE): the wrapper finishes the pin and keeps the tape"
fresh; tp tape_preserve_export; echo 124 > "$FIX/import.rc"; printf 'COMPLETE\n' > "$FIX/state.seq"; : > "$FIX/calls"
tp tape_preserve_import; r=$?
[ $r = 0 ] && [ "$(calls pin)" = 1 ] && ok "pin was run; treated as restored (rc 0)" || bad "rc=$r pins=$(calls pin) out=$(cat "$W/out")"
[ -f "$W/art/consumed.es.underlying.es.trades.tape" ] && ok "artifact retired" || bad "artifact not retired"

case_ "10e. state COMPLETE but pinning fails: the tape comes back out (all or nothing)"
fresh; tp tape_preserve_export; echo 124 > "$FIX/import.rc"; echo 7 > "$FIX/pin.rc"; printf 'COMPLETE\nEMPTY\n' > "$FIX/state.seq"; : > "$FIX/calls"
tp tape_preserve_import; r=$?
[ $r = 1 ] && [ "$(calls truncate)" = 1 ] && ok "rc 1; truncated because it could not be pinned" || bad "rc=$r truncates=$(calls truncate)"

case_ "10f. an unreadable state after a failure is treated as UNSAFE, never assumed fine"
fresh; tp tape_preserve_export; echo 124 > "$FIX/import.rc"; printf 'GARBAGE\nGARBAGE\n' > "$FIX/state.seq"
tp tape_preserve_import; r=$?
[ $r = 2 ] && ok "rc 2" || bad "rc=$r out=$(cat "$W/out")"

case_ "10g. retirement: a failed rename falls back to removing the artifact; if even that fails it is an ERROR"
fresh; tp tape_preserve_export; TP_FAIL_MV=1 tp tape_preserve_import; r=$?
[ ! -f "$W/art/es.underlying.es.trades.tape" ] && ok "artifact gone despite the rename failing" || bad "artifact still active"
fresh; tp tape_preserve_export; TP_FAIL_MV=1 TP_FAIL_RM=1 tp tape_preserve_import; r=$?
[ $r = 1 ] && grep -q "ERROR: could not retire the artifact" "$W/out" && ok "both failing is reported as an ERROR, rc 1" || bad "rc=$r out=$(cat "$W/out")"

case_ "11b. an artifact that stops BEFORE the prior session's close is refused (it would present a partial prior session as complete)"
fresh; echo 1 > "$FIX/export.maxts"; tp tape_preserve_export; : > "$FIX/calls"; tp tape_preserve_import; r=$?
[ $r = 1 ] && [ "$(calls import)" = 0 ] && ok "refused: the tool was never asked to import" || bad "partial artifact imported (rc=$r)"
grep -q "BEFORE the prior session's close" "$W/out" && ok "says why" || bad "silent: $(cat "$W/out")"
[ -f "$W/art/es.underlying.es.trades.tape" ] && ok "artifact left in place" || bad "artifact deleted"

case_ "11c. an artifact that runs through the close is restored (a normal clean exports after the close)"
fresh; echo 9999999999999 > "$FIX/export.maxts"; tp tape_preserve_export; : > "$FIX/calls"; tp tape_preserve_import; r=$?
[ $r = 0 ] && [ "$(calls import)" = 1 ] && ok "restored" || bad "rc=$r calls=$(cat "$FIX/calls")"

case_ "11d. without the calendar the import refuses (it cannot prove the close) instead of guessing"
fresh; tp tape_preserve_export; : > "$FIX/calls"; ES4_TAPE_CAL_DIR=/nonexistent tp tape_preserve_import; r=$?
[ $r = 1 ] && [ "$(calls import)" = 0 ] && grep -q "calendar unavailable" "$W/out" && ok "refused, fails toward the old behaviour" || bad "rc=$r out=$(cat "$W/out")"

case_ "12. two topics are handled independently"
fresh; TOPICS="a.t b.t" tp tape_preserve_export
[ -f "$W/art/a.t.tape" ] && [ -f "$W/art/b.t.tape" ] && ok "both exported" || bad "$(ls -A "$W/art")"

# ---------------------------------------------------------------------------------------------------
case_ "13. the window mirrors EsAmtSession (tradeDate rolls at 18:00 ET; required = prior trading day 09:30 ET)"
win() { # <y m d H M> in ET -> "<from_ET> | <required_ET> | <requiredClose_ET>"
  local e; e=$(python3 -c "import datetime as d;from zoneinfo import ZoneInfo as Z;print(int(d.datetime($1,$2,$3,$4,$5,tzinfo=Z('America/New_York')).timestamp()))")
  ( ES4_TAPE_NOW_EPOCH=$e; export ES4_TAPE_NOW_EPOCH; . "$HERE/tape-preserve.sh"; tp_window ) | python3 -c "
import sys,datetime as d;from zoneinfo import ZoneInfo as Z
f,r,c=map(int,sys.stdin.read().split())
t=lambda ms:d.datetime.fromtimestamp(ms/1000,Z('America/New_York')).strftime('%a %m-%d %H:%M')
print(t(f),'|',t(r),'|',t(c))"
}
chk() { [ "$(win $2 $3 $4 $5 $6)" = "$7" ] && ok "$1" || bad "$1: got '$(win $2 $3 $4 $5 $6)' want '$7'"; }
chk "Thu 16:40 ET restart needs WEDNESDAY's open (the 2026-10-08 failure)"  2026 10 8 16 40 "Wed 10-07 04:40 | Wed 10-07 09:30 | Wed 10-07 16:00"
chk "Fri 18:30 ET (session rolled to Monday) needs Friday's open"          2026 10 9 18 30 "Thu 10-08 06:30 | Fri 10-09 09:30 | Fri 10-09 16:00"
chk "Mon 07:00 ET needs Friday's open across the weekend"                  2026 10 12 7 0 "Fri 10-09 07:30 | Fri 10-09 09:30 | Fri 10-09 16:00"
chk "Labor Day 2026-09-07 12:00 ET: prior session skips the weekend AND the holiday (Fri 09-04)" 2026 9 7 12 0 "Fri 09-04 07:30 | Fri 09-04 09:30 | Fri 09-04 16:00"
chk "Mon 2026-11-30 07:00 ET: the prior session is the EARLY-CLOSE Friday after Thanksgiving, which closes at 13:00" 2026 11 30 7 0 "Fri 11-27 07:30 | Fri 11-27 09:30 | Fri 11-27 13:00"

# ---------------------------------------------------------------------------------------------------
case_ "14. cleanup-es4.sh wiring — order and failure policy (static)"
C="$HERE/cleanup-es4.sh"
ln() { grep -n -F -- "$1" "$C" | head -1 | cut -d: -f1; }
src=$(ln '. "$SCRIPT_DIR/tape-preserve.sh"'); il=$(ln 'strike_archive_interlock "before the wipe"')
ex=$(ln 'tape_preserve_export'); dn=$(ln "docker compose down)"); vf=$(ln "verify-topic-partition-contract.sh' created"); im=$(ln 'tape_preserve_import')
up=$(grep -n -F "docker compose up -d)\"" "$C" | head -1 | cut -d: -f1); rs=$(ln 'restoring es4 Deployments to captured replica counts')
[ -n "$src" ] && [ -n "$il" ] && [ -n "$ex" ] && [ -n "$dn" ] && [ -n "$vf" ] && [ -n "$im" ] && [ -n "$up" ] && [ -n "$rs" ] && ok "every anchor found" || bad "anchor missing (src=$src il=$il ex=$ex dn=$dn vf=$vf im=$im up=$up rs=$rs)"
[ "$src" -lt "$ex" ] && ok "library is sourced before it is used" || bad "sourced after use"
[ "$il" -lt "$ex" ] && [ "$ex" -lt "$dn" ] && ok "export: after the authoritative strike interlock (producers down), before compose down" || bad "export out of order"
[ "$vf" -lt "$im" ] && [ "$im" -lt "$up" ] && [ "$up" -lt "$rs" ] && ok "import: after topics verified, before mm2/infra start and before ANY app is restored" || bad "import out of order"
grep -F 'tape_preserve_export' "$C" | grep -qF '||' || grep -A1 -F 'tape_preserve_export' "$C" | grep -qF '||' && ok "export can never abort the clean (guarded with ||)" || bad "export is not guarded"
grep -A1 -F 'tape_preserve_import' "$C" | grep -qF '||' && ok "import can never abort the clean (guarded with ||)" || bad "import is not guarded"

case_ "14b. static guards"
grep -q -- '--kill-after' "$HERE/tape-preserve.sh" && ok "the tool runs under timeout --kill-after (a TERM-ignoring child cannot hang the clean)" || bad "no --kill-after"
grep -q '\[ "\$tp_rc" != 2 \] || die' "$C" && ok "cleanup-es4.sh DIES on rc 2 (partial tape) - the only non-warning outcome" || bad "rc 2 is not fatal in cleanup-es4.sh"
amt="$(sed -n '/ES_AMT_REPLAY_LOOKBACK_HOURS/{n;p;}' "$HERE/../../k8s/es4/services/es-amt.yaml" | sed -n "s/.*value: *'\{0,1\}\([0-9]*\)'\{0,1\}.*/\1/p" | head -1)"
lib="$(sed -n 's/.*ES4_TAPE_LOOKBACK_HOURS:-\([0-9]*\)}.*/\1/p' "$HERE/tape-preserve.sh" | head -1)"
[ -n "$amt" ] && [ "$amt" = "$lib" ] && ok "ES4_TAPE_LOOKBACK_HOURS ($lib) == ES_AMT_REPLAY_LOOKBACK_HOURS ($amt) in k8s/es4/services/es-amt.yaml - drift fails CI" || bad "lookback drift: lib='$lib' es-amt.yaml='$amt'"

case_ "15. the tool compiles for the box's Java 17 (when a JDK and kafka-clients are available)"
LIBS="${KAFKA_LIBS:-$HOME/kafka-4.3.0/libs}"
if command -v javac >/dev/null 2>&1 && ls "$LIBS"/kafka-clients-*.jar >/dev/null 2>&1; then
  mkdir -p "$W/cls"
  if javac --release 17 -proc:none -Xlint:all -cp "$LIBS/*" -d "$W/cls" "$HERE/tape-preserve/TapePreserve.java" >"$W/javac.out" 2>&1; then ok "javac --release 17: clean, no lint warnings"
  else bad "javac failed: $(head -5 "$W/javac.out")"; fi
  [ ! -s "$W/javac.out" ] || bad "javac lint output: $(head -3 "$W/javac.out")"
else
  echo "  skip javac or kafka-clients not available (set KAFKA_LIBS)"
fi

echo
if [ "$fails" -eq 0 ]; then echo "=== tape-preserve: OK ==="; exit 0; fi
echo "=== tape-preserve: $fails problem(s) ===" >&2; exit 1
