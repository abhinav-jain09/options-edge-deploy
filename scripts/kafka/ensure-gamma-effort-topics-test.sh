#!/usr/bin/env bash
# Drives the REAL ensure-gamma-effort-topics.sh against the STATEFUL stand-in tests/fake_kafka_cli.py (as kafka-topics / kafka-configs), the same stand-in
# ensure-es-liquidity-wall-topics-test.sh drives its barrier with.
#
# WHY THIS TEST EXISTS, dated: until 2026-11-14 this barrier had NO test, and the thing that went wrong is exactly what a test would have caught. The cap was raised
# from 256 to 512 MiB in scripts/kafka/topics.env on 2026-11-13 (#1637) and the barrier's own LOG_BYTES literal was left at 256 MiB, so the raise never reached the
# cluster: the live dev topic was still 268435456 a day later. Nothing failed, because the barrier only ever APPLIED the cap on the create path and the topic already
# existed. So the cases below are not generic coverage — the central three are the declared cap reaching a NEW topic, the declared cap reaching an EXISTING one
# (raised), and a cap BELOW the live value being refused rather than silently deleting the corpus it would trim.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
FAKE="$HERE/../../tests/fake_kafka_cli.py"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
CURRENT=options.spx.gamma-effort.current
LOG=options.spx.gamma-effort.log
# The DECLARED cap. Read out of scripts/kafka/topics.env rather than typed here, so this test cannot
# agree with a barrier that has drifted from the declaration -- the whole failure being tested.
# The BYTES overrides line, not any line mentioning the topic: topics.env also declares
# `options.spx.gamma-effort.log=-1` in the retention.MS overrides, and a pattern loose enough to
# match both reads whichever comes first.
CAP="$(grep "^OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES=" "$HERE/topics.env" | grep -o "options\.spx\.gamma-effort\.log=[0-9][0-9]*" | head -1 | sed 's/.*=//')"
case "${CAP:-}" in ''|*[!0-9]*) echo "FAIL: could not read the declared retention.bytes for $LOG out of topics.env"; exit 1 ;; esac
OLD_CAP=268435456   # what the live dev topic actually carried on 2026-11-14, the value this change raises
ABOVE=1073741824    # gamma-zone's cap: a plausible wrong-way edit, and the case that must be REFUSED
mkdir -p "$T/bin"; for n in kafka-topics kafka-configs kafka-get-offsets; do cp "$FAKE" "$T/bin/$n"; chmod +x "$T/bin/$n"; done

fresh() { echo '{"topics":{}}' > "$T/state.json"; : > "$T/calls"; }
# topic <name> <partitions> <policy> <retention.ms> <retention.bytes> [max.message.bytes] [min.insync.replicas]
topic() {
  python3 - "$T/state.json" "$@" <<'PY'
import json, sys
f, name, parts, policy, ms, by = sys.argv[1:7]; mb = sys.argv[7] if len(sys.argv) > 7 else "1048576"; isr = sys.argv[8] if len(sys.argv) > 8 else "1"
s = json.load(open(f)); s["topics"][name] = {"partitions": int(parts), "rf": 1, "end": 0, "configs": {"cleanup.policy": policy, "retention.ms": ms, "retention.bytes": by, "max.message.bytes": mb, "min.insync.replicas": isr}}
json.dump(s, open(f, "w"))
PY
}
cfg() { python3 -c 'import json,sys; t=json.load(open(sys.argv[1]))["topics"].get(sys.argv[2]); print("" if t is None else str(t["partitions"])+" "+" ".join(t["configs"].get(k,"") for k in ("cleanup.policy","retention.ms","retention.bytes")))' "$T/state.json" "$1"; }
run() {
  env -u ENVIRONMENT PATH="$T/bin:$PATH" FAKE_KAFKA_STATE="$T/state.json" FAKE_KAFKA_CALLS="$T/calls" KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 "$@" bash "$HERE/ensure-gamma-effort-topics.sh" > "$T/out" 2>&1
}
expect_ok() { local m="$1"; shift; run ENVIRONMENT=dev "$@"; if [ $? -eq 0 ]; then ok "$m"; else bad "$m"; cat "$T/out"; fi; }
expect_fail() { local m="$1"; shift; run ENVIRONMENT=dev "$@"; if [ $? -ne 0 ] && grep -q "FAILED" "$T/out"; then ok "$m"; else bad "$m"; cat "$T/out"; fi; }
no_mutation() { if grep -qE -- "--delete|--alter" "$T/calls"; then bad "$1: altered or deleted something"; cat "$T/calls"; else ok "$1: nothing altered or deleted"; fi; }
both_good() { fresh; topic $CURRENT 1 compact -1 -1; topic $LOG 1 delete -1 "$CAP"; }

echo "--- the declared cap, read from topics.env: $CAP"

# --- dev only ---------------------------------------------------------------------------------------------------------------------------------
for e in production es4 ""; do
  fresh; run ENVIRONMENT="$e"; rc=$?
  { [ $rc -eq 1 ] && grep -q "DEV ONLY" "$T/out" && [ ! -s "$T/calls" ]; } && ok "ENVIRONMENT='$e' is refused before any CLI call" || { bad "ENVIRONMENT='$e'"; cat "$T/out"; }
done
fresh; run; rc=$?
{ [ $rc -eq 1 ] && grep -q "DEV ONLY" "$T/out" && [ ! -s "$T/calls" ]; } && ok "ENVIRONMENT unset is refused before any CLI call" || { bad "ENVIRONMENT unset"; cat "$T/out"; }

# --- a renamed topic is refused: the corpus log is preserved by nothing under another name ------------------------------------------------------
fresh; run ENVIRONMENT=dev KAFKA_GAMMA_EFFORT_LOG_TOPIC=options.spx.gamma-effort.log2; rc=$?
{ [ $rc -eq 1 ] && [ ! -s "$T/calls" ]; } && ok "an overridden LOG topic name is refused before any CLI call" || { bad "renamed log"; cat "$T/out"; }
fresh; run ENVIRONMENT=dev KAFKA_GAMMA_EFFORT_CURRENT_TOPIC=options.spx.gamma-effort.cur; rc=$?
{ [ $rc -eq 1 ] && [ ! -s "$T/calls" ]; } && ok "an overridden CURRENT topic name is refused before any CLI call" || { bad "renamed current"; cat "$T/out"; }

# --- an empty dev cluster: the DECLARED cap reaches a NEW topic ---------------------------------------------------------------------------------
fresh; expect_ok "empty cluster -> ok"
grep -q "=== ensure-gamma-effort-topics OK ===" "$T/out" && ok "...reports OK" || bad "...no OK line"
[ "$(cfg $CURRENT)" = "1 compact -1 -1" ] && ok "current created: 1 partition, pure compact, retention.ms -1, retention.bytes -1" || bad "current: $(cfg $CURRENT)"
[ "$(cfg $LOG)" = "1 delete -1 $CAP" ] && ok "log created: 1 partition, delete, retention.ms -1 WITH the declared retention.bytes $CAP" || bad "log: $(cfg $LOG)"
grep -q -- "--create --if-not-exists --topic $LOG --partitions 1 .*cleanup.policy=delete .*retention.ms=-1 .*retention.bytes=$CAP" "$T/calls" && ok "the log's create call carries the declared cap" || { bad "log create call lacks the declared cap"; cat "$T/calls"; }
[ "$(grep -c -- '--create' "$T/calls")" = 2 ] && ok "exactly two creates" || bad "$(grep -c -- '--create' "$T/calls") creates"
if grep -qE -- "--entity-type brokers|--entity-default|message.max.bytes" "$T/calls"; then bad "a broker config was touched"; else ok "no broker-wide change"; fi
no_mutation "empty cluster"

# --- a second run changes nothing ---------------------------------------------------------------------------------------------------------------
: > "$T/calls"; expect_ok "second run on the result -> ok"
if grep -qE -- "--create|--delete|--alter" "$T/calls"; then bad "second run mutated"; cat "$T/calls"; else ok "second run created, altered and deleted nothing"; fi

# --- both present at the declared shapes -------------------------------------------------------------------------------------------------------
both_good; expect_ok "both present at the declared shapes -> ok"
if grep -q -- "--create" "$T/calls"; then bad "created on a complete cluster"; else ok "...nothing created"; fi
no_mutation "complete cluster"

# --- THE REGRESSION: an EXISTING log below the declared cap is RAISED, in one alter, and the run passes ----------------------------------------
fresh; topic $CURRENT 1 compact -1 -1; topic $LOG 1 delete -1 "$OLD_CAP"
expect_ok "an existing log at the old $OLD_CAP is accepted after being raised"
[ "$(cfg $LOG)" = "1 delete -1 $CAP" ] && ok "...the live cap is now the declared $CAP" || bad "...the live cap is $(cfg $LOG)"
grep -q "raising '$LOG' retention.bytes $OLD_CAP -> $CAP" "$T/out" && ok "...and the raise is reported, with both numbers" || { bad "...the raise is not reported"; cat "$T/out"; }
[ "$(grep -c -- "--alter" "$T/calls")" = 1 ] && ok "...exactly one alter" || { bad "$(grep -c -- '--alter' "$T/calls") alters"; cat "$T/calls"; }
grep -q -- "--entity-name $LOG --alter --add-config retention.bytes=$CAP" "$T/calls" && ok "...the alter sets retention.bytes only, on the log only" || { bad "...wrong alter"; cat "$T/calls"; }
if grep -q -- "--create" "$T/calls"; then bad "the log was recreated"; else ok "...the log was NOT recreated: no record is lost raising a cap"; fi
: > "$T/calls"; expect_ok "the run after the raise -> ok"
if grep -qE -- "--create|--delete|--alter" "$T/calls"; then bad "the raise is not idempotent"; cat "$T/calls"; else ok "...and it is idempotent: the second run alters nothing"; fi

# --- THE OTHER DIRECTION: a live cap ABOVE the declared one is REFUSED, not trimmed -----------------------------------------------------------
fresh; topic $CURRENT 1 compact -1 -1; topic $LOG 1 delete -1 "$ABOVE"
expect_fail "a live log cap ABOVE the declared one is refused"
grep -q "'$LOG'" "$T/out" && ok "...names the topic" || bad "...does not name the topic"
grep -qi "reviewed operation" "$T/out" && ok "...and says lowering a cap is a separate reviewed operation" || { bad "...no reason given"; cat "$T/out"; }
[ "$(cfg $LOG)" = "1 delete -1 $ABOVE" ] && ok "...the live cap is untouched" || bad "...the live cap became $(cfg $LOG)"
no_mutation "a cap above the declared one"

# --- the CURRENT topic is pure compact and is never raised toward a cap ----------------------------------------------------------------------
fresh; topic $CURRENT 1 compact -1 "$OLD_CAP"; topic $LOG 1 delete -1 "$CAP"
expect_fail "a finite cap on the pure-compact current topic is refused"
grep -q "'$CURRENT'" "$T/out" && ok "...names the current topic" || bad "...does not name it"
no_mutation "a cap on the current topic"

# --- every other difference fails closed, names the topic, and alters nothing ----------------------------------------------------------------
f() { local label="$1" t="$2"; shift 2; both_good; topic "$t" "$@"; expect_fail "$label"; grep -q "'$t'" "$T/out" && ok "...names $t" || bad "...does not name $t"; no_mutation "$label"; }
f "the log COMPACTED - it would keep one record per session and discard the corpus" $LOG 1 compact -1 "$CAP"
f "the log compact,delete" $LOG 1 compact,delete -1 "$CAP"
f "the log at 32 partitions (the dev auto-create default)" $LOG 32 delete -1 "$CAP"
f "the log aged out by time as well" $LOG 1 delete 604800000 "$CAP"
f "the log at an unlimited cap" $LOG 1 delete -1 -1
f "the log below the message bound" $LOG 1 delete -1 "$CAP" 524288
f "the current topic on delete" $CURRENT 1 delete -1 -1
f "the current topic at 32 partitions" $CURRENT 32 compact -1 -1
f "the current topic aged out by time" $CURRENT 1 compact 604800000 -1

# --- A WRONG SHAPE *AND* A LOW CAP: refused, and the cap NOT raised on the way out ------------------------------------------------------
# The mirror of the raise, and the case the first version of this test missed (Codex r1). Every
# wrong-shape case above left the log already AT the declared cap, so the raise had nothing to do
# and `no_mutation` passed for free. With the old 256 MiB cap as well, the raise would run on a
# topic this barrier is about to REFUSE -- altering a topic on the way to refusing it is the one
# thing a fail-closed barrier must never do, and the deploy fails anyway, so the alter is pure loss.
g() { local label="$1"; shift
  fresh; topic $CURRENT 1 compact -1 -1; topic $LOG "$@"
  expect_fail "$label"
  grep -q "'$LOG'" "$T/out" && ok "...names $LOG" || bad "...does not name $LOG"
  no_mutation "$label"
  if grep -q "retention.bytes=$CAP" "$T/calls"; then bad "$label: the cap was raised on a topic being refused"; else ok "$label: the cap was NOT raised on a topic being refused"; fi
}
g "the log at 32 partitions AND still at the old cap" 32 delete -1 "$OLD_CAP"
g "the log COMPACTED AND still at the old cap" 1 compact -1 "$OLD_CAP"
g "the log aged out by time AND still at the old cap" 1 delete 604800000 "$OLD_CAP"
g "the log below the message bound AND still at the old cap" 1 delete -1 "$OLD_CAP" 524288
# ...and the control: the SAME low cap with a RIGHT shape IS raised, so the gate above is not just
# a blanket refusal to ever raise.
fresh; topic $CURRENT 1 compact -1 -1; topic $LOG 1 delete -1 "$OLD_CAP"
expect_ok "the control: a right shape at the old cap is still raised"
[ "$(cfg $LOG)" = "1 delete -1 $CAP" ] && ok "...to the declared $CAP" || bad "...left at $(cfg $LOG)"

echo "--- $pass ok, $fail failed"
[ "$fail" -eq 0 ]
