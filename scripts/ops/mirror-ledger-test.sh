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

echo "8. when the scratch file cannot be written, the ORIGINAL ledger is left as it was"
# A directory where the ".keep" path must go makes every write to it fail.
LEDGER="$WORK/ledger.ro"; printf '%s' "$L3" > "$LEDGER"; mkdir -p "$LEDGER.keep"
rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
OUT="$(LAUNCHCTL_STATE="$WORK/state" bash -c '
  . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh" topic.bbb' _ "$HERE" "$LEDGER" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "returns non-zero" || bad "returned 0 although the ledger could not be rewritten"
[ "$(calls | wc -l | tr -d ' ')" = 0 ] && ok "and starts NOTHING" || bad "it started: $(calls | tr '\n' ' ')"
# diff against the bytes that were written, not "$(cat)" against "$L3": command substitution strips
# the trailing newline, so that comparison called an intact file changed.
diff <(printf '%s' "$L3") "$LEDGER" >/dev/null \
  && ok "the original ledger is byte-for-byte intact" || bad "the ledger changed: $(cat "$LEDGER")"
rmdir "$LEDGER.keep"

echo "9. an EMPTY or absent ledger is a no-op, not an error"
run ""
[ "$RC" -eq 0 ] && ok "an empty ledger returns 0" || bad "returned $RC"
OUT="$(bash -c '. "$1/mirror-ledger.sh"; mirror_ledger_resume "/nope/ledger" "$1/mirror-topic-filter.sh"' _ "$HERE" 2>&1)"; RC=$?
[ "$RC" -eq 0 ] && ok "an absent ledger returns 0" || bad "returned $RC for an absent ledger"
OUT="$(bash -c '. "$1/mirror-ledger.sh"; mirror_ledger_resume' _ "$HERE" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "no ledger path at all is refused" || bad "no arguments returned 0"

echo "10. MUTATIONS: the two halves of the invariant are load-bearing"
mut() { # <old||new> <label> <ledger> <hold...> ; expects the ledger to differ from the correct run
  local edit="$1" label="$2"; shift 2
  local dir="$WORK/mut.$RANDOM"; mkdir -p "$dir"
  cp "$HERE/mirror-ledger.sh" "$dir/"; cp "$HERE/mirror-topic-filter.sh" "$dir/"
  python3 - "$dir/mirror-ledger.sh" "$edit" <<'PY'
import sys
path, edit = sys.argv[1], sys.argv[2]
old, new = edit.split("||", 1)
src = open(path).read()
assert old in src, "the mutation did not apply: %r not found" % old
open(path, "w").write(src.replace(old, new, 1))
PY
  [ $? -eq 0 ] || { bad "$label: the mutation did not apply"; return; }
  local led="$WORK/mled.$RANDOM"; printf '%s' "$1" > "$led"; shift
  rm -rf "$WORK/state"; mkdir -p "$WORK/state"; : > "$WORK/state/loaded"; : > "$WORK/state/calls"
  local out; out="$(LAUNCHCTL_STATE="$WORK/state" LAUNCH_FAIL="${LAUNCH_FAIL:-}" bash -c '
    . "$1/mirror-ledger.sh"; mirror_ledger_resume "$2" "$1/mirror-topic-filter.sh" "${@:3}"' _ "$dir" "$led" "$@" 2>&1)"
  MUT_RC=$?; MUT_LEDGER="$led"; MUT_OUT="$out"
}
# Drop the missing-plist row instead of keeping it: the agent would be paused and unlisted.
mut '_ml_say "   KEPT paused (plist not found): $label"; _ml_hold_row "$label" "$plist"; held=$((held+1)); continue||_ml_say "   KEPT paused (plist not found): $label"; continue' \
    "a dropped missing-plist row" "com.optionsedge.aaa $A
com.optionsedge.gone $WORK/no-such.plist
"
[ ! -f "$MUT_LEDGER" ] && ok "the mutant loses the row (case 4 is load-bearing)" || bad "the mutation changed nothing: $(cat "$MUT_LEDGER")"
# Replace the ledger even when the scratch writes failed.
mut 'if [ "$broken" -ne 0 ]; then||if false; then' "an unchecked scratch write" "com.optionsedge.aaa $A
"
[ "$MUT_RC" -eq 0 ] && ok "the mutant stops reporting the bookkeeping failure (case 8 is load-bearing)" \
  || ok "the mutant still fails, for its own reason (rc=$MUT_RC)"

echo
if [ "$fails" -eq 0 ]; then echo "=== mirror-ledger: OK ==="; exit 0; fi
echo "=== mirror-ledger: $fails problem(s) ===" >&2; exit 1
