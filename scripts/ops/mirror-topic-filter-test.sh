#!/usr/bin/env bash
# mirror-topic-filter.sh decides whether a paused es4->target mirror may be started while some topics
# are unreconciled or missing. Every answer it cannot establish must be a HOLD, so this drives it over
# real plists and unit directories built in a temp tree -- including the ones it cannot read.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
F="$HERE/mirror-topic-filter.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -x "$F" ] || { echo "FAIL: $F missing or not executable" >&2; exit 1; }

# unit <name> <run-mirror body> -> prints the plist path
unit() {
  local name="$1" body="$2" dir="$WORK/$1" plist="$WORK/com.optionsedge.$1.plist"
  mkdir -p "$dir"
  printf '%s\n' "$body" > "$dir/run-mirror.sh"; chmod +x "$dir/run-mirror.sh"
  python3 - "$plist" "$dir/run-mirror.sh" "com.optionsedge.$name" <<'PY'
import plistlib, sys
out, prog, label = sys.argv[1], sys.argv[2], sys.argv[3]
plistlib.dump({"Label": label, "ProgramArguments": [prog], "RunAtLoad": True}, open(out, "wb"))
PY
  printf '%s\n' "$plist"
}
verdict() { # <plist> <topic...> -> "<rc> <line>"
  local out rc; out="$(bash "$F" "$@" 2>&1)"; rc=$?; printf '%s %s\n' "$rc" "$out"
}
want_hold()  { local v; v="$(verdict "$@")"; case "$v" in 1\ HOLD*) return 0 ;; esac; printf '      got: %s\n' "$v"; return 1; }
want_clear() { local v; v="$(verdict "$@")"; case "$v" in 0\ CLEAR*) return 0 ;; esac; printf '      got: %s\n' "$v"; return 1; }

CVD=$(unit cvd-bars "exec kafka-mirror-maker --consumer.config c --producer.config p --whitelist 'es\.futures\.cvd\.bars' --num.streams 1")
AUC=$(unit auction  'exec kafka-mirror-maker --whitelist "es\.futures\.auction" --num.streams 1')
BARE=$(unit bare    'exec kafka-mirror-maker --whitelist es\.tape-zones\.board --num.streams 1')
FAM=$(unit family   "exec kafka-mirror-maker --whitelist 'es\.futures\.cvd' --num.streams 1")

echo "1. the 2026-10-07 case: a drifted topic no mirror copies starts every mirror"
for u in "$CVD" "$AUC" "$BARE" "$FAM"; do
  want_clear "$u" options.spx.strike-invasion.current \
    && ok "CLEAR on options.spx.strike-invasion.current: $(basename "$u")" \
    || bad "held $(basename "$u") over a topic it does not copy"
done

echo "2. a topic the mirror DOES copy is a HOLD, in all three quoting forms"
want_hold "$CVD"  es.futures.cvd.bars   && ok "single-quoted whitelist matches"  || bad "single-quoted whitelist did not match"
want_hold "$AUC"  es.futures.auction    && ok "double-quoted whitelist matches"  || bad "double-quoted whitelist did not match"
want_hold "$BARE" es.tape-zones.board   && ok "unquoted whitelist matches"       || bad "unquoted whitelist did not match"
want_hold "$CVD"  a.b es.futures.cvd.bars c.d && ok "matches anywhere in the topic list" || bad "only the first topic is checked"

echo "3. OVER-INCLUSIVE on purpose: a family prefix holds its children"
# MirrorMaker matches a topic against the pattern in FULL, so /es\.futures\.cvd/ would not really
# subscribe to es.futures.cvd.bars. Holding anyway is the cheap mistake; starting wrongly is not.
want_hold "$FAM" es.futures.cvd.bars && ok "a prefix pattern holds es.futures.cvd.bars" || bad "a prefix pattern did not hold its child topic"
want_hold "$FAM" es.futures.cvd      && ok "and holds its own exact topic"              || bad "a prefix pattern did not hold its exact topic"

echo "4. the verdict comes from the program the plist RUNS, and siblings can only ADD holds"
# A unit whose executed program whitelists A, with a stale sibling whitelisting B.
mkdir -p "$WORK/twoscripts"
printf '%s\n' "exec kafka-mirror-maker --whitelist 'topic\\.aaa' --num.streams 1" > "$WORK/twoscripts/run-mirror.sh"
printf '%s\n' "exec kafka-mirror-maker --whitelist 'topic\\.bbb' --num.streams 1" > "$WORK/twoscripts/run-mirror-old.sh"
chmod +x "$WORK/twoscripts"/*.sh
TWO="$WORK/com.optionsedge.twoscripts.plist"
python3 - "$TWO" "$WORK/twoscripts/run-mirror.sh" <<'PY'
import plistlib, sys
plistlib.dump({"Label": "com.optionsedge.twoscripts", "ProgramArguments": [sys.argv[2]]}, open(sys.argv[1], "wb"))
PY
want_hold "$TWO" topic.aaa && ok "the executed program's own whitelist holds"        || bad "the program's whitelist did not hold"
want_hold "$TWO" topic.bbb && ok "a sibling's whitelist ADDS a hold (conservative)"  || bad "a sibling's whitelist was ignored"
want_clear "$TWO" topic.ccc && ok "and a topic in neither is still CLEAR"            || bad "an unrelated topic was held"

# The program itself carries no whitelist; a sibling does. The sibling is not an answer about the
# program, so this is a HOLD rather than a verdict derived from the wrong file.
mkdir -p "$WORK/progless"
printf '%s\n' "exec kafka-mirror-maker --consumer.config c --num.streams 1" > "$WORK/progless/run-mirror.sh"
printf '%s\n' "exec kafka-mirror-maker --whitelist 'topic\\.zzz' --num.streams 1" > "$WORK/progless/run-mirror-legacy.sh"
chmod +x "$WORK/progless"/*.sh
PL="$WORK/com.optionsedge.progless.plist"
python3 - "$PL" "$WORK/progless/run-mirror.sh" <<'PY'
import plistlib, sys
plistlib.dump({"Label": "com.optionsedge.progless", "ProgramArguments": [sys.argv[2]]}, open(sys.argv[1], "wb"))
PY
want_hold "$PL" topic.qqq && ok "a program with no whitelist is a HOLD, even with a sibling that has one" \
  || bad "the verdict came from a sibling instead of the program"

# ProgramArguments[0] must BE the program: an absolute path later in the argument list is not it.
LATER="$WORK/com.optionsedge.later.plist"
python3 - "$LATER" "$WORK/twoscripts/run-mirror.sh" <<'PY'
import plistlib, sys
plistlib.dump({"Label": "com.optionsedge.later", "ProgramArguments": ["/bin/bash", sys.argv[2]]}, open(sys.argv[1], "wb"))
PY
want_hold "$LATER" topic.ccc && ok "a wrapper layout (ProgramArguments[0]=/bin/bash) is a HOLD" \
  || bad "a wrapper layout was CLEARED from a later argument's directory"

# A program path that is not there at all.
GONE="$WORK/com.optionsedge.gone.plist"
python3 - "$GONE" "$WORK/no-such-dir/run-mirror.sh" <<'PY'
import plistlib, sys
plistlib.dump({"Label": "com.optionsedge.gone", "ProgramArguments": [sys.argv[2]]}, open(sys.argv[1], "wb"))
PY
want_hold "$GONE" topic.ccc && ok "a program that does not exist is a HOLD" || bad "a missing program was CLEARED"

echo "4b. a whitelist it cannot READ from the file is a HOLD, and a comment is not a whitelist"
# A value assembled at runtime is not in the file, so no CLEAR can rest on it.
DYN=$(unit dyn 'WL="topic\.dyn"
exec kafka-mirror-maker --whitelist "$WL" --num.streams 1')
want_hold "$DYN" anything.at.all && ok "a --whitelist built from a variable is a HOLD" \
  || bad "a dynamic whitelist was CLEARED"
v="$(bash "$F" "$DYN" anything.at.all 2>&1 || true)"
printf '%s' "$v" | grep -q 'dynamically' && ok "and says it cannot be read from the file" || bad "the reason is [$v]"
DYNC=$(unit dync 'exec kafka-mirror-maker --whitelist "$(cat /etc/whitelist)" --num.streams 1')
want_hold "$DYNC" anything.at.all && ok "so is one built by a command substitution" || bad "a substituted whitelist was CLEARED"

# A COMMENT is not executed, so its whitelist is not the agent's: a program whose only --whitelist is
# commented out has none, and that is a HOLD rather than a verdict from dead text.
CMT=$(unit cmt '# exec kafka-mirror-maker --whitelist '"'"'topic\.commented'"'"' --num.streams 1
exec kafka-mirror-maker --consumer.config c --num.streams 1')
want_hold "$CMT" topic.commented && ok "a commented-out whitelist is not a whitelist" || bad "a comment was read as the whitelist"
v="$(bash "$F" "$CMT" topic.commented 2>&1 || true)"
printf '%s' "$v" | grep -q 'no --whitelist in the program' && ok "and it says the program has none" || bad "the reason is [$v]"

# A DEAD BRANCH is text that runs under some condition this file cannot evaluate, so its whitelist is
# unioned in: it can only add holds.
DEAD=$(unit dead 'if false; then
  exec kafka-mirror-maker --whitelist '"'"'topic\.dead'"'"' --num.streams 1
fi
exec kafka-mirror-maker --whitelist '"'"'topic\.live'"'"' --num.streams 1')
want_hold "$DEAD" topic.live && ok "the live branch holds"              || bad "the live whitelist did not hold"
want_hold "$DEAD" topic.dead && ok "and the dead branch holds too"      || bad "a dead branch's whitelist was ignored"
want_clear "$DEAD" topic.other && ok "and an unrelated topic is CLEAR"  || bad "an unrelated topic was held"

echo "5. FAIL CLOSED on anything it cannot read"
NOSCRIPT="$WORK/com.optionsedge.noscript.plist"
python3 - "$NOSCRIPT" "$WORK/empty-unit/run-mirror.sh" <<'PY'
import os, plistlib, sys
os.makedirs(os.path.dirname(sys.argv[2]), exist_ok=True)
plistlib.dump({"Label": "com.optionsedge.noscript", "ProgramArguments": [sys.argv[2]]}, open(sys.argv[1], "wb"))
PY
want_hold "$NOSCRIPT" any.topic && ok "no run-mirror*.sh in the unit dir" || bad "a unit with no run script was CLEARED"

NOWL=$(unit nowl 'exec kafka-mirror-maker --consumer.config c --producer.config p --num.streams 1')
want_hold "$NOWL" any.topic && ok "no --whitelist in the run script" || bad "a run script with no whitelist was CLEARED"

BADRX=$(unit badrx "exec kafka-mirror-maker --whitelist 'es\.futures\.[' --num.streams 1")
want_hold "$BADRX" any.topic && ok "a --whitelist that will not compile" || bad "an uncompilable whitelist was CLEARED"

want_hold "$WORK/no-such-file.plist" any.topic && ok "a plist that does not exist" || bad "a missing plist was CLEARED"
printf 'not a plist at all\n' > "$WORK/com.optionsedge.junk.plist"
want_hold "$WORK/com.optionsedge.junk.plist" any.topic && ok "a plist that does not parse" || bad "an unparseable plist was CLEARED"

NOARGS="$WORK/com.optionsedge.noargs.plist"
python3 - "$NOARGS" <<'PY'
import plistlib, sys
plistlib.dump({"Label": "com.optionsedge.noargs", "ProgramArguments": ["relative/run.sh"]}, open(sys.argv[1], "wb"))
PY
want_hold "$NOARGS" any.topic && ok "a relative ProgramArguments[0]" || bad "a relative program path was CLEARED"

rc=0; bash "$F" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 2 ] && ok "no arguments is a usage error (exit 2)" || bad "no arguments exited $rc, expected 2"

echo "6. the verdict names the topic and the pattern, so the operator log says WHY"
v="$(bash "$F" "$CVD" es.futures.cvd.bars 2>&1 || true)"
printf '%s' "$v" | grep -q 'es.futures.cvd.bars ~ /es\\.futures\\.cvd\\.bars/' \
  && ok "the HOLD line names both" || bad "the HOLD line does not name both: $v"

echo "7. MUTATION: a filter that stops matching must not silently CLEAR"
mkdir -p "$WORK/mut"; cp "$F" "$WORK/mut/f.sh"
python3 - "$WORK/mut/f.sh" <<'PY'
import sys
p = sys.argv[1]; s = open(p).read()
old = "if rx.fullmatch(t) or rx.search(t):"
assert old in s, "the mutation did not apply -- the match line has moved"
open(p, "w").write(s.replace(old, "if False:", 1))
PY
got="$(bash "$WORK/mut/f.sh" "$CVD" es.futures.cvd.bars 2>&1 || true)"
case "$got" in CLEAR*) ok "the neutered matcher CLEARS, so case 2 is load-bearing (mutant: $got)" ;;
  *) bad "the neutered matcher did not change the answer: $got" ;; esac

echo
if [ "$fails" -eq 0 ]; then echo "=== mirror-topic-filter: OK ==="; exit 0; fi
echo "=== mirror-topic-filter: $fails problem(s) ===" >&2; exit 1
