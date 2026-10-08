#!/usr/bin/env bash
# The paused-mirror ledger's one invariant: a row leaves the file ONLY when its agent is confirmed
# loaded. Driven over a fake ledger with a stubbed launchctl, because the operator script that uses it
# (scripts/ops/prod-clean-slate.sh) is ssh-driven and cannot be run by a test.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -r "$HERE/mirror-ledger.sh" ] || { echo "FAIL: $HERE/mirror-ledger.sh missing" >&2; exit 1; }

# A stub launchctl: `bootstrap` records the call and loads the label unless it is in LAUNCH_FAIL;
# `list` succeeds only for a loaded label. This is the only thing the ledger uses to decide "running".
mkdir -p "$WORK/bin"
cat > "$WORK/bin/launchctl" <<'EOF'
#!/usr/bin/env bash
STATE="${LAUNCHCTL_STATE:?}"
case "$1" in
  bootstrap)
    plist="$3"; label="$(basename "$plist")"; label="${label%.plist}"
    echo "bootstrap $label" >> "$STATE/calls"
    for f in ${LAUNCH_FAIL:-}; do [ "$f" = "$label" ] && exit 1; done
    echo "$label" >> "$STATE/loaded"; exit 0 ;;
  list)
    grep -qxF "$2" "$STATE/loaded" 2>/dev/null && exit 0 || exit 1 ;;
esac
exit 0
EOF
chmod +x "$WORK/bin/launchctl"
export PATH="$WORK/bin:$PATH"

# A unit whose run-mirror.sh whitelists one topic, plus its plist.
unit() { # <name> <whitelist-regex> -> plist path
  local d="$WORK/$1"; mkdir -p "$d"
  printf "exec kafka-mirror-maker --whitelist '%s' --num.streams 1\n" "$2" > "$d/run-mirror.sh"
  chmod +x "$d/run-mirror.sh"
  local plist="$WORK/com.optionsedge.$1.plist"
  python3 - "$plist" "$d/run-mirror.sh" "com.optionsedge.$1" <<'PY'
import plistlib, sys
plistlib.dump({"Label": sys.argv[3], "ProgramArguments": [sys.argv[2]]}, open(sys.argv[1], "wb"))
PY
  printf '%s\n' "$plist"
}
A=$(unit aaa 'topic\.aaa'); B=$(unit bbb 'topic\.bbb'); C=$(unit ccc 'topic\.ccc')

# run <ledger-content> [hold-topic...] -> sets RC, OUT, and leaves the ledger at $LEDGER
run() {
  local content="$1"; shift
  LEDGER="$WORK/ledger.$RANDOM"
  printf '%s' "$content" > "$LEDGER"
  rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
  OUT="$(LAUNCHCTL_STATE="$WORK/state" LAUNCH_FAIL="${LAUNCH_FAIL:-}" bash -c '
    . "$1/mirror-ledger.sh"
    mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh" "${@:3}"' _ "$HERE" "$LEDGER" "$@" 2>&1)"
  RC=$?
}
rows() { [ -f "$LEDGER" ] && cat "$LEDGER" || printf '(ledger removed)\n'; }
calls() { cat "$WORK/state/calls" 2>/dev/null; }

L3="com.optionsedge.aaa $A
com.optionsedge.bbb $B
com.optionsedge.ccc $C
"

echo "1. nothing held: every agent starts and the ledger is REMOVED"
run "$L3"
[ "$RC" -eq 0 ] && ok "returns 0" || { bad "returned $RC: $OUT"; }
[ "$(calls | wc -l | tr -d ' ')" = 3 ] && ok "three bootstraps" || bad "bootstraps: $(calls | tr '\n' ' ')"
[ ! -f "$LEDGER" ] && ok "the ledger is gone" || bad "the ledger still holds: $(rows)"

echo "2. a held topic: that agent is NOT started and STAYS in the ledger"
run "$L3" topic.bbb
[ "$RC" -eq 0 ] && ok "a deliberate hold is not a failure (returns 0)" || bad "returned $RC"
calls | grep -q 'bootstrap com.optionsedge.bbb' && bad "the held agent was bootstrapped" || ok "no bootstrap for the held agent"
[ "$(calls | wc -l | tr -d ' ')" = 2 ] && ok "the other two started" || bad "bootstraps: $(calls | tr '\n' ' ')"
[ "$(rows)" = "com.optionsedge.bbb $B" ] && ok "the ledger lists exactly the held agent" || bad "the ledger holds: $(rows)"
printf '%s' "$OUT" | grep -q 'HELD paused' && ok "and says it was held" || bad "no HELD line: $OUT"

echo "3. an agent that fails to load stays listed, and the call returns NON-ZERO"
LAUNCH_FAIL=com.optionsedge.ccc run "$L3"
unset LAUNCH_FAIL
[ "$RC" -ne 0 ] && ok "returns non-zero" || bad "returned 0 with an agent down"
[ "$(rows)" = "com.optionsedge.ccc $C" ] && ok "the failed agent is still listed" || bad "the ledger holds: $(rows)"

echo "4. a plist that is not there right now stays listed (it is still DOWN)"
run "com.optionsedge.aaa $A
com.optionsedge.gone $WORK/no-such.plist
"
[ "$RC" -eq 0 ] && ok "a missing plist is held, not failed" || bad "returned $RC"
[ "$(rows)" = "com.optionsedge.gone $WORK/no-such.plist" ] && ok "and keeps its row" || bad "the ledger holds: $(rows)"

echo "5. a duplicate row is bootstrapped ONCE"
run "com.optionsedge.aaa $A
com.optionsedge.aaa $A
"
[ "$(calls | grep -c 'com.optionsedge.aaa')" = 1 ] && ok "one bootstrap for two identical rows" || bad "bootstraps: $(calls | tr '\n' ' ')"
printf '%s' "$OUT" | grep -q 'duplicate ledger row' && ok "and says so" || bad "no duplicate note: $OUT"

echo "6. a row with no plist path is held, never dropped"
run "com.optionsedge.aaa $A
com.optionsedge.orphan
"
[ "$(rows)" = "com.optionsedge.orphan " ] && ok "the orphan row survives" || bad "the ledger holds: $(rows)"

echo "7. the filter itself missing or not executable is a HOLD, not a start"
LEDGER="$WORK/ledger.nofilter"; printf '%s' "$L3" > "$LEDGER"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "/nope/not-a-filter.sh" topic.zzz' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "nothing was started" || bad "bootstraps: $(calls | tr '\n' ' ')"
[ "$(rows | wc -l | tr -d ' ')" = 3 ] && ok "all three rows survive" || bad "the ledger holds: $(rows)"

echo "8. when nothing can be written beside the ledger, the ORIGINAL is left as it was"
# A read-only directory: the ledger can be read, but neither the lock nor the scratch file can be
# created. Either refusal is the same guarantee -- nothing starts and the file is untouched.
mkdir -p "$WORK/ro"; LEDGER="$WORK/ro/ledger"; printf '%s' "$L3" > "$LEDGER"; chmod 500 "$WORK/ro"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh" topic.bbb' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "returns non-zero" || bad "returned 0 although the ledger could not be rewritten"
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "and starts NOTHING" || bad "it started: $(calls | tr '\n' ' ')"
# diff against the bytes that were written, not "$(cat)" against "$L3": command substitution strips
# the trailing newline, so that comparison called an intact file changed.
diff <(printf '%s' "$L3") "$LEDGER" >/dev/null \
  && ok "the original ledger is byte-for-byte intact" || bad "the ledger changed: $(cat "$LEDGER")"
chmod 700 "$WORK/ro"

echo "8b. an UNREADABLE but non-empty ledger is not an empty one"
# `-s` is true for a file that cannot be read; the read loop then runs zero times. Treating that as
# "everything resumed" DELETED the only record of the agents that were down (deploy Codex round 3).
LEDGER="$WORK/ledger.unreadable"; printf '%s' "$L3" > "$LEDGER"; chmod 000 "$LEDGER"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh"' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
chmod 600 "$LEDGER"
[ "$RC" -ne 0 ] && ok "returns non-zero" || bad "an unreadable ledger returned 0"
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "and starts NOTHING" || bad "it started: $(calls | tr '\n' ' ')"
[ -f "$LEDGER" ] && diff <(printf '%s' "$L3") "$LEDGER" >/dev/null \
  && ok "and the ledger is still there, byte-for-byte" || bad "the ledger was changed or deleted"
printf '%s' "$OUT" | grep -q 'cannot READ' && ok "and says it could not read it" || bad "no diagnostic: $OUT"

echo "8c. a plist path containing SPACES round-trips"
# The row is "<label> <path>": the first field is the label, the REST is the path. Re-splitting the
# path into fields is how a ledger rewrites somebody's path into something that does not exist.
SP="$WORK/unit with space"; mkdir -p "$SP"
printf "exec kafka-mirror-maker --whitelist 'topic\\.spaced' --num.streams 1\n" > "$SP/run-mirror.sh"
chmod +x "$SP/run-mirror.sh"
SPP="$WORK/com.optionsedge.spaced.plist"
python3 - "$SPP" "$SP/run-mirror.sh" <<'PY'
import plistlib, sys
plistlib.dump({"Label": "com.optionsedge.spaced", "ProgramArguments": [sys.argv[2]]}, open(sys.argv[1], "wb"))
PY
run "com.optionsedge.spaced $SPP
" topic.spaced
[ "$RC" -eq 0 ] && ok "a held agent with a spaced path is not a failure" || bad "returned $RC"
diff <(printf '%s\n' "com.optionsedge.spaced $SPP") "$LEDGER" >/dev/null \
  && ok "and its row is written back verbatim" || bad "the row changed: $(rows)"
run "com.optionsedge.spaced $SPP
"
[ "$(calls | wc -l | tr -d ' ')" = 1 ] && ok "and it starts when nothing is held" || bad "bootstraps: $(calls | tr '\n' ' ')"

echo "8d. a STALE lock refuses, and leaves everything listed"
run "$L3"   # a normal run first, to be sure the lock is released afterwards
[ ! -d "$LEDGER.lock" ] && ok "a normal run releases the lock" || bad "the lock directory was left behind"
LEDGER="$WORK/ledger.locked"; printf '%s' "$L3" > "$LEDGER"; mkdir -p "$LEDGER.lock"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh"' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "a held lock refuses" || bad "it ran while another run held the ledger"
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "and starts NOTHING" || bad "it started: $(calls | tr '\n' ' ')"
diff <(printf '%s' "$L3") "$LEDGER" >/dev/null && ok "and the ledger is untouched" || bad "the ledger changed"
rmdir "$LEDGER.lock"

echo "8e. a SYMLINKED ledger is refused, dangling or not"
# A dangling symlink satisfies `! -e`, which would otherwise read as "nothing was paused" and let the
# clean slate bring prod up with its mirrors still down (deploy Codex round 4).
LEDGER="$WORK/ledger.dangling"; ln -sf "$WORK/not-there" "$LEDGER"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh"' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "a dangling symlink refuses (not 'nothing was paused')" || bad "a dangling symlink returned 0"
printf '%s' "$OUT" | grep -q 'SYMLINK' && ok "and says so" || bad "the diagnostic is [$OUT]"
REAL="$WORK/ledger.real"; printf '%s' "$L3" > "$REAL"
LEDGER="$WORK/ledger.live-link"; ln -sf "$REAL" "$LEDGER"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh"' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "a LIVE symlink is refused too" || bad "a live symlink was worked"
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "and nothing was started" || bad "it started: $(calls | tr '\n' ' ')"
diff <(printf '%s' "$L3") "$REAL" >/dev/null && ok "and the target file is untouched" || bad "the target changed"

echo "8f. rows the format cannot represent are kept and reported, never acted on"
# A label with a space would hand "<rest of label> <path>" to launchctl as a path. A CRLF row leaves a
# carriage return on the path. Both are held, with their row written back verbatim.
run "com.optionsedge.two words $A
"
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "a spaced label starts nothing" || bad "it started: $(calls | tr '\n' ' ')"
diff <(printf '%s %s\n' "com.optionsedge.two" "words $A") "$LEDGER" >/dev/null \
  && ok "and its row survives verbatim" || bad "the row became: $(rows)"
printf 'com.optionsedge.aaa %s\r\n' "$A" > "$WORK/ledger.crlf"; LEDGER="$WORK/ledger.crlf"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh"' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "a CRLF row starts nothing (the path does not exist)" || bad "it started: $(calls | tr '\n' ' ')"
[ -s "$LEDGER" ] && ok "and keeps its row" || bad "the CRLF row was dropped"

echo "9. an EMPTY or absent ledger is a no-op, not an error"
run ""
[ "$RC" -eq 0 ] && ok "an empty ledger returns 0" || bad "returned $RC"
OUT="$(bash -c '. "$1/mirror-ledger.sh"; mirror_ledger_resume "/nope/ledger" "$1/mirror-topic-filter.sh"' _ "$HERE" 2>&1)"; RC=$?
[ "$RC" -eq 0 ] && ok "an absent ledger returns 0" || bad "returned $RC for an absent ledger"
OUT="$(bash -c '. "$1/mirror-ledger.sh"; mirror_ledger_resume' _ "$HERE" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "no ledger path at all is refused" || bad "no arguments returned 0"

# The mutation helpers, defined before their first use (section 9c) rather than beside section 10.
mutant_dir() { # <OLD%%->%%NEW> -> prints the directory holding a mutated copy, or fails
  local edit="$1" dir="$WORK/mut.$RANDOM"
  mkdir -p "$dir" || return 1
  cp "$HERE/mirror-ledger.sh" "$HERE/mirror-topic-filter.sh" "$dir/" || return 1
  printf '%s' "$edit" > "$dir/edit"
  python3 -c '
import sys
path, editfile = sys.argv[1], sys.argv[2]
old, new = open(editfile).read().split("%%->%%", 1)
src = open(path).read()
assert old in src, "the mutation did not apply: %r not found" % old
open(path, "w").write(src.replace(old, new, 1))
' "$dir/mirror-ledger.sh" "$dir/edit" || return 1
  printf '%s\n' "$dir"
}
run_in() { # <dir> <ledger> [hold...] -> MUT_RC, MUT_OUT
  local dir="$1" led="$2"; shift 2
  rm -rf "$led.lock"   # a mutant that died mid-run leaves one, and the next run would only see that
  rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
  MUT_OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
    . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh" "${@:3}"' _ "$dir" "$led" "$@" 2>&1)"
  MUT_RC=$?
}

echo "9b. mirror_ledger_record: rows go in BEFORE anything is stopped, and are read back out"
rec() { # <ledger> <rows...> -> REC_RC, REC_OUT
  local led="$1"; shift
  local rowsf="$WORK/rows.$RANDOM"; printf '%s\n' "$@" > "$rowsf"
  rm -rf "$led.lock"
  REC_OUT="$(bash -c '. "$1/mirror-ledger.sh"; mirror_ledger_record "$2" "$3"' _ "$HERE" "$led" "$rowsf" 2>&1)"
  REC_RC=$?
}
LEDGER="$WORK/rec.fresh"; rm -f "$LEDGER"
rec "$LEDGER" "com.optionsedge.aaa $A"
[ "$REC_RC" -eq 0 ] && ok "recording into a ledger that does not exist yet works" || bad "returned $REC_RC: $REC_OUT"
grep -qxF "com.optionsedge.aaa $A" "$LEDGER" && ok "and the row is readable back" || bad "the row is not in the ledger: $(cat "$LEDGER" 2>/dev/null)"
rec "$LEDGER" "com.optionsedge.bbb $B"
grep -qxF "com.optionsedge.aaa $A" "$LEDGER" && grep -qxF "com.optionsedge.bbb $B" "$LEDGER" \
  && ok "a second record MERGES rather than replaces" || bad "the ledger holds: $(cat "$LEDGER")"
rec "$LEDGER" "com.optionsedge.bbb $B"
[ "$(grep -c "com.optionsedge.bbb" "$LEDGER")" = 1 ] && ok "and recording the same row twice leaves one" || bad "duplicated: $(cat "$LEDGER")"
rec "$LEDGER"
[ "$REC_RC" -eq 0 ] && ok "an empty row set is a no-op, not an error" || bad "returned $REC_RC"
# The cases where NOTHING may be stopped.
mkdir -p "$WORK/ro3"; LEDGER="$WORK/ro3/rec"; printf '%s %s\n' "com.optionsedge.aaa" "$A" > "$LEDGER"; chmod 500 "$WORK/ro3"
rec "$LEDGER" "com.optionsedge.bbb $B"
chmod 700 "$WORK/ro3"
[ "$REC_RC" -ne 0 ] && ok "a ledger directory that cannot be written refuses" || bad "returned 0: $REC_OUT"
diff <(printf '%s %s\n' "com.optionsedge.aaa" "$A") "$LEDGER" >/dev/null && ok "and leaves the ledger as it was" || bad "the ledger changed"
LEDGER="$WORK/rec.link"; ln -sf "$WORK/rec.fresh" "$LEDGER"
rec "$LEDGER" "com.optionsedge.ccc $C"
[ "$REC_RC" -ne 0 ] && ok "a symlinked ledger refuses" || bad "recorded into a symlink"
LEDGER="$WORK/rec.locked"; : > "$LEDGER"; mkdir -p "$LEDGER.lock"
REC_OUT="$(bash -c '. "$1/mirror-ledger.sh"; printf "%s\n" "x y" > "$2.rows"; mirror_ledger_record "$2" "$2.rows"' _ "$HERE" "$LEDGER" 2>&1)"; REC_RC=$?
[ "$REC_RC" -ne 0 ] && ok "a held lock refuses" || bad "recorded while the ledger was locked"
rmdir "$LEDGER.lock"
rec "$WORK/rec.fresh" "com.optionsedge.aaa $A"
[ ! -d "$WORK/rec.fresh.lock" ] && ok "and a successful record releases the lock" || bad "the lock was left behind"

echo "9c. MUTATION: the read-back is what makes recording a guarantee"
# The fault is INJECTED into the merge (it ignores the new rows), because a merge cannot be made to
# lose a row from outside. Held constant across both runs; the mutation under test is the removal of
# the read-back.
RECFAULT='sort -u -- "$list" "$rows" > "$merged"%%->%%sort -u -- "$list" /dev/null > "$merged"'
if d="$(mutant_dir "$RECFAULT")"; then
  led="$WORK/rec.m1"; : > "$led"; rowsf="$WORK/rec.m1.rows"; printf '%s\n' "com.optionsedge.zzz /nope.plist" > "$rowsf"
  REC_OUT="$(bash -c '. "$1/mirror-ledger.sh"; mirror_ledger_record "$2" "$3"' _ "$d" "$led" "$rowsf" 2>&1)"; REC_RC=$?
  [ "$REC_RC" -ne 0 ] && printf '%s' "$REC_OUT" | grep -q 'are NOT in' \
    && ok "with the merge losing a row, the read-back CATCHES it and refuses" \
    || bad "the injected merge fault was not caught: rc=$REC_RC out=[$REC_OUT]"
  if d2="$(mutant_dir "$RECFAULT")" && python3 -c '
import sys
p = sys.argv[1]; s = open(p).read()
old = "    grep -qxF -- \"$row\" \"$list\" 2>/dev/null || missing=$((missing+1))"
assert old in s, "the read-back removal did not apply"
open(p, "w").write(s.replace(old, "    true", 1))
' "$d2/mirror-ledger.sh"; then
    led="$WORK/rec.m2"; : > "$led"
    REC_OUT="$(bash -c '. "$1/mirror-ledger.sh"; mirror_ledger_record "$2" "$3"' _ "$d2" "$led" "$rowsf" 2>&1)"; REC_RC=$?
    [ "$REC_RC" -eq 0 ] && ! grep -q 'zzz' "$led" \
      && ok "and without the read-back the same loss REPORTS SUCCESS (it is load-bearing)" \
      || bad "removing the read-back changed nothing: rc=$REC_RC ledger=[$(cat "$led")]"
  else bad "(9c) the read-back removal did not apply"; fi
else bad "(9c) the merge-fault injection did not apply"; fi

echo "10. MUTATIONS: each half of the invariant is load-bearing"
# One explicit block per mutation. A shared helper plumbed the ledger content, the mutated copy and the
# hold topics through three layers and a `bash -c`, and a mistake in that plumbing reported "the mutant
# failed for its own reason (127)" -- a mutation that never ran, scored as a pass.
# (a) the missing-plist row: dropped instead of kept, so the agent stays paused and unlisted.
if d="$(mutant_dir '_ml_say "   KEPT paused (plist not found): $label"; _ml_hold_row "$label" "$plist"; held=$((held+1)); continue%%->%%_ml_say "   KEPT paused (plist not found): $label"; continue')"; then
  led="$WORK/mled.a"; printf '%s %s\n' "com.optionsedge.gone" "$WORK/no-such.plist" > "$led"
  run_in "$d" "$led"
  [ ! -f "$led" ] && ok "the mutant LOSES the missing-plist row (case 4 is load-bearing)" \
    || bad "the mutation changed nothing: the row is still listed"
else bad "(a) the missing-plist mutation did not apply"; fi

# (b) the completeness check. A failing APPEND cannot be forced from outside, so the fault is
#     INJECTED (the hold rows go to /dev/full, which always fails) and held constant across the two
#     runs below; the mutation under test is the removal of the check.
FAULT='>> "$keep"%%->%%>> /dev/full'
if d="$(mutant_dir "$FAULT")"; then
  led="$WORK/mled.b1"; printf '%s %s\n' "com.optionsedge.aaa" "$A" > "$led"
  run_in "$d" "$led" topic.aaa
  [ "$MUT_RC" -ne 0 ] && printf '%s' "$MUT_OUT" | grep -q 'incomplete' \
    && ok "with a failing append the check FIRES and says the ledger is incomplete" \
    || bad "the injected append failure was not caught: rc=$MUT_RC out=[$MUT_OUT]"
  diff <(printf '%s %s\n' "com.optionsedge.aaa" "$A") "$led" >/dev/null \
    && ok "and the original ledger survives" || bad "the ledger was changed: $(cat "$led" 2>/dev/null)"
  # ...and now the same injected failure with the check REMOVED: the row is lost.
  if d2="$(mutant_dir "$FAULT")" && python3 -c '
import sys
p = sys.argv[1]; s = open(p).read()
old = "if [ \"$broken\" -ne 0 ] || [ \"$processed\" -ne \"$total\" ]; then"
assert old in s, "the second mutation did not apply"
open(p, "w").write(s.replace(old, "if false; then", 1))
' "$d2/mirror-ledger.sh"; then
    led="$WORK/mled.b2"; printf '%s %s\n' "com.optionsedge.aaa" "$A" > "$led"
    run_in "$d2" "$led" topic.aaa
    [ ! -f "$led" ] && ok "without the check the held row is LOST (case 8 is load-bearing)" \
      || bad "removing the check changed nothing: the ledger still holds $(cat "$led")"
  else bad "(b) the check-removal mutation did not apply"; fi
else bad "(b) the append-failure injection did not apply"; fi

# (c) the read-status check: an unreadable ledger would then report SUCCESS and say nothing.
if d="$(mutant_dir 'if [ "$read_rc" -ne 0 ] || [ ! -r "$list" ]; then%%->%%if false; then')"; then
  led="$WORK/mled.c"; printf '%s' "$L3" > "$led"; chmod 000 "$led"
  run_in "$d" "$led"
  chmod 600 "$led"
  if [ "$MUT_RC" -eq 0 ] && ! printf '%s' "$MUT_OUT" | grep -q 'cannot READ'; then
    ok "the mutant calls an unreadable ledger a clean run (case 8b is load-bearing)"
  else
    bad "the read-check mutation changed nothing: rc=$MUT_RC out=[$MUT_OUT]"
  fi
else bad "(c) the read-check mutation did not apply"; fi

echo
if [ "$fails" -eq 0 ]; then echo "=== mirror-ledger: OK ==="; exit 0; fi
echo "=== mirror-ledger: $fails problem(s) ===" >&2; exit 1
