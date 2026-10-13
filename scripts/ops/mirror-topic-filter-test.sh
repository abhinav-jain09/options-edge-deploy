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
# $BARE is deliberately NOT in this list: its whitelist is unquoted, which section 2 shows is a HOLD
# whatever the topic, because the shell would rewrite the value before the program saw it.
for u in "$CVD" "$AUC" "$FAM"; do
  want_clear "$u" options.spx.strike-invasion.current \
    && ok "CLEAR on options.spx.strike-invasion.current: $(basename "$u")" \
    || bad "held $(basename "$u") over a topic it does not copy"
done

echo "2. a topic the mirror DOES copy is a HOLD, in all three quoting forms"
want_hold "$CVD"  es.futures.cvd.bars   && ok "single-quoted whitelist matches"  || bad "single-quoted whitelist did not match"
want_hold "$AUC"  es.futures.auction    && ok "double-quoted whitelist matches"  || bad "double-quoted whitelist did not match"
# An UNQUOTED whitelist is refused outright now: the shell rewrites it before kafka-mirror-maker sees
# it, so the text in the file is not the runtime pattern (`es\.x` arrives as `es.x`, which matches MORE).
want_hold "$BARE" es.tape-zones.board   && ok "an unquoted whitelist is a HOLD, whatever the topic"  || bad "an unquoted whitelist produced a verdict"
want_hold "$BARE" nothing.to.do.with.it && ok "...and holds for an unrelated topic too"              || bad "an unquoted whitelist CLEARED a topic"
v="$(bash "$F" "$BARE" nothing.to.do.with.it 2>&1 || true)"
printf '%s' "$v" | grep -q 'unquoted' && ok "and says why" || bad "the reason is [$v]"
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

echo "4a. the two shapes the install paths really generate are ACCEPTED"
# One-line (the es-* units) and backslash-continued (the dl-mirror-* units), both taken from the live
# files under ~/oe-ops. If either stopped being accepted, every mirror of that shape would be held
# forever by a filter that is supposed to hold only the ones with a stake in an unreconciled topic.
ONELINE=$(unit real1 'XX')   # placeholder, overwritten below
cat > "$WORK/real1/run-mirror.sh" <<'RM1'
#!/usr/bin/env bash
set -euo pipefail
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:/Users/abhinav/oe-ops/x/log4j.properties"
exec "/Users/abhinav/development/confluent-7.3.1/bin/kafka-mirror-maker"   --consumer.config "/Users/abhinav/oe-ops/x/consumer.properties"   --producer.config "/Users/abhinav/oe-ops/x/producer.properties"   --offset.commit.interval.ms 5000   --whitelist 'es\.futures\.auction' --num.streams 1
RM1
want_hold "$ONELINE" es.futures.auction && ok "the one-line generated shape is read, and holds its topic" || bad "the real one-line shape was not read"
want_clear "$ONELINE" something.else && ok "and clears an unrelated topic" || bad "the real one-line shape held an unrelated topic"

CONTD=$(unit real2 'XX')
cat > "$WORK/real2/run-mirror.sh" <<'RM2'
#!/usr/bin/env bash
# DEV -> .74 : a comment block, as the real file has.
set -euo pipefail
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:/Users/abhinav/oe-ops/y/log4j.properties"
exec "/Users/abhinav/development/confluent-7.3.1/bin/kafka-mirror-maker" \
  --consumer.config "/Users/abhinav/oe-ops/y/consumer.properties" \
  --producer.config "/Users/abhinav/oe-ops/y/producer.properties" \
  --whitelist 'dealer-ledger-profile|dealer-ledger-state' \
  --num.streams 3
RM2
want_hold "$CONTD" dealer-ledger-state && ok "the continued generated shape is read, and holds its topic" || bad "the real continued shape was not read"
want_clear "$CONTD" something.else && ok "and clears an unrelated topic" || bad "the real continued shape held an unrelated topic"

echo "4b. only the ACCEPTED launcher format can produce a CLEAR"
# A scan over script text cannot be made to hold: a decoy in an inline comment, a here-doc body, or a
# wrapper that execs the real launcher from elsewhere are all text a scan reads as code or not at all.
# So the format is accepted and everything else is a HOLD (deploy Codex round 5).
DECOY=$(unit decoy 'exec /usr/local/bin/real-launcher   # --whitelist '"'"'topic\.decoy'"'"'')
want_hold "$DECOY" topic.decoy && ok "a launcher with a commented decoy is a HOLD" || bad "a decoy comment was read as the whitelist"
want_hold "$DECOY" anything.else && ok "and it holds for every topic, since nothing is established" || bad "a wrapper CLEARED a topic"

HEREDOC=$(unit heredoc 'cat > /tmp/x <<EOF
--whitelist '"'"'topic\.inheredoc'"'"'
EOF
exec kafka-mirror-maker --consumer.config c --num.streams 1')
want_hold "$HEREDOC" anything && ok "a file containing a here-doc is a HOLD" || bad "a here-doc file was CLEARED"

INLINE=$(unit inline "exec kafka-mirror-maker --whitelist 'topic\.live' --num.streams 1  # --whitelist 'topic\.extra'")
want_hold "$INLINE" anything && ok "a second --whitelist anywhere in the file is a HOLD" || bad "a file with two --whitelist mentions was CLEARED"

TWOEXEC=$(unit twoexec "if [ -f /tmp/a ]; then
  exec kafka-mirror-maker --consumer.config c --num.streams 1
fi
exec kafka-mirror-maker --whitelist 'topic\.second' --num.streams 1")
want_hold "$TWOEXEC" anything && ok "two kafka-mirror-maker lines are a HOLD" || bad "a file with two launcher lines was CLEARED"

SOURCED=$(unit sourced "source /etc/mirror.env
exec kafka-mirror-maker --whitelist 'topic\.sourced' --num.streams 1")
want_hold "$SOURCED" anything && ok "a file that sources another is a HOLD" || bad "a sourcing file was CLEARED"
DOTINC=$(unit dotinc ". /etc/mirror.env
exec kafka-mirror-maker --whitelist 'topic\.dotinc' --num.streams 1")
want_hold "$DOTINC" anything && ok "so is a dot-include" || bad "a dot-including file was CLEARED"
EVAL=$(unit evaled "eval exec kafka-mirror-maker --whitelist 'topic\.evaled' --num.streams 1")
want_hold "$EVAL" anything && ok "so is an eval" || bad "an eval'd launcher was CLEARED"
PIPED=$(unit piped "exec kafka-mirror-maker --whitelist 'topic\.piped' --num.streams 1 | tee /tmp/log")
want_hold "$PIPED" anything && ok "a launcher line with a pipe is not the accepted shape" || bad "a piped launcher was CLEARED"

echo "4c. a whitelist it cannot READ from the file is a HOLD"
# A value assembled at runtime is not in the file, so no CLEAR can rest on it.
DYN=$(unit dyn 'WL="topic\.dyn"
exec kafka-mirror-maker --whitelist "$WL" --num.streams 1')
want_hold "$DYN" anything.at.all && ok "a --whitelist built from a variable is a HOLD" \
  || bad "a dynamic whitelist was CLEARED"
v="$(bash "$F" "$DYN" anything.at.all 2>&1 || true)"
# The reason is the FORMAT one: a value with a $ in it is not an accepted literal, so the launcher
# line is not the accepted shape. (It used to be a separate "built dynamically" message, from the
# scan that the accepted format replaced.)
# The reason is a FORMAT one: the variable assignment that builds the value is itself a line outside the
# accepted shape, and even without it a `"$WL"` value is not an accepted literal. Either way no verdict
# comes out of a value this file cannot read.
printf '%s' "$v" | grep -qE 'outside the accepted shape|not the accepted' \
  && ok "and says the file is not the accepted shape" || bad "the reason is [$v]"
DYNC=$(unit dync 'exec kafka-mirror-maker --whitelist "$(cat /etc/whitelist)" --num.streams 1')
want_hold "$DYNC" anything.at.all && ok "so is one built by a command substitution" || bad "a substituted whitelist was CLEARED"

# A COMMENTED-OUT launcher leaves the file with no accepted launcher line at all.
CMT=$(unit cmt '# exec kafka-mirror-maker --whitelist '"'"'topic\.commented'"'"' --num.streams 1
exec /usr/local/bin/other-thing')
want_hold "$CMT" topic.commented && ok "a commented-out launcher is not a launcher" || bad "a comment was read as the launcher"

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

echo "7. MUTATIONS: the accepted format and the matcher are each load-bearing"
# A launcher line with a TRAILING COMMENT carrying the only --whitelist in the file: the real filter
# refuses it (the line is not the accepted shape), so no verdict can come from a comment. A mutant
# whose line pattern tolerates trailing text reads that comment as an argument, and then a decoy
# decides which mirrors start.
INLDEC=$(unit inldec "exec kafka-mirror-maker --consumer.config c --num.streams 1  # --whitelist 'topic\\.inlinedecoy'")
want_hold "$INLDEC" topic.inlinedecoy && ok "a trailing-comment decoy is a HOLD" || bad "the trailing-comment decoy was CLEARED"
want_hold "$INLDEC" unrelated.topic   && ok "and holds every topic, since nothing is established" || bad "the decoy file CLEARED an unrelated topic"

# TWO rules keep that comment out of the verdict: the line must END after its flags, and only the
# FLAG REGION is parsed for arguments. Mutating the first alone leaves the second refusing (the comment
# is outside the flag region), which is itself worth knowing. The mutation below removes the second --
# the args capture becomes "everything after the program" -- and the comment then decides.
mkdir -p "$WORK/mutfmt"; cp "$F" "$WORK/mutfmt/f.sh"
python3 - "$WORK/mutfmt/f.sh" <<'PYEOF'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
hits = [i for i, l in enumerate(lines) if l.strip().startswith('r"(?P<args>')]
assert len(hits) == 1, "the mutation did not apply -- the args capture has moved (%d candidates)" % len(hits)
lines[hits[0]] = '    r"(?P<args>.*)$"'
open(p, "w").write("\n".join(lines))
PYEOF
if [ $? -eq 0 ]; then
  got="$(bash "$WORK/mutfmt/f.sh" "$INLDEC" topic.inlinedecoy 2>&1 || true)"
  printf '%s' "$got" | grep -q 'inlinedecoy ~' \
    && ok "the mutant takes its verdict from the COMMENT's whitelist (the accepted format is load-bearing)" \
    || bad "the args mutation produced [$got]"
  got="$(bash "$WORK/mutfmt/f.sh" "$INLDEC" unrelated.topic 2>&1 || true)"
  case "$got" in CLEAR*) ok "and CLEARS everything the comment does not name" ;;
    *) bad "the mutant answered [$got] for an unrelated topic" ;; esac
fi

echo "7b. MUTATION: a filter that stops matching must not silently CLEAR"
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
