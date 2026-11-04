#!/usr/bin/env bash
# Drives the REAL ensure-es-liquidity-wall-topics.sh against the STATEFUL stand-in tests/fake_kafka_cli.py (as kafka-topics / kafka-configs), the same stand-in
# tests/test_line_acceptance_deploy.py drives its barrier with. The barrier's points: DEV ONLY (any other ENVIRONMENT fails before a CLI call); an empty cluster gets the seven topics at
# their declared shapes with NO broker-wide change (the events log created WITH its 256 MiB cap, the four pure-compact topics at -1/-1); a second run creates and alters nothing; and every
# difference (partitions, policy, retention.ms, retention.bytes, message bound, ISR) fails closed, names the topic and alters nothing.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
FAKE="$HERE/../../tests/fake_kafka_cli.py"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
P=futures.liquidity-wall
CL=$P.closures; BU=$P.buckets; DA=$P.dashboard; EV=$P.events; ST=$P.state; SN=$P.snapshot; VO=$P.voice
SEVEN=604800000; DAY=86400000; CAP=268435456
mkdir -p "$T/bin"; cp "$FAKE" "$T/bin/kafka-topics"; cp "$FAKE" "$T/bin/kafka-configs"; chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"

# state helpers: topic <name> <partitions> <policy> <retention.ms> <retention.bytes> [max.message.bytes] [min.insync.replicas]
fresh() { echo '{"topics":{}}' > "$T/state.json"; : > "$T/calls"; }
topic() {
  python3 - "$T/state.json" "$@" <<'PY'
import json, sys
f, name, parts, policy, ms, by = sys.argv[1:7]; mb = sys.argv[7] if len(sys.argv) > 7 else "1048576"; isr = sys.argv[8] if len(sys.argv) > 8 else "1"
s = json.load(open(f)); s["topics"][name] = {"partitions": int(parts), "rf": 1, "end": 0, "configs": {"cleanup.policy": policy, "retention.ms": ms, "retention.bytes": by, "max.message.bytes": mb, "min.insync.replicas": isr}}
json.dump(s, open(f, "w"))
PY
}
all_good() { fresh; topic $CL 1 delete $SEVEN -1; topic $BU 1 delete $DAY -1; topic $DA 1 compact -1 -1; topic $EV 1 delete -1 $CAP; topic $ST 1 compact -1 -1; topic $SN 1 compact -1 -1; topic $VO 1 compact -1 -1; }
cfg() { python3 -c 'import json,sys; t=json.load(open(sys.argv[1]))["topics"].get(sys.argv[2]); print("" if t is None else str(t["partitions"])+" "+" ".join(t["configs"].get(k,"") for k in ("cleanup.policy","retention.ms","retention.bytes")))' "$T/state.json" "$1"; }
run() { # [ENV=...] -> rc; output in $T/out, calls in $T/calls
  env -u ENVIRONMENT PATH="$T/bin:$PATH" FAKE_KAFKA_STATE="$T/state.json" FAKE_KAFKA_CALLS="$T/calls" KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 "$@" bash "$HERE/ensure-es-liquidity-wall-topics.sh" > "$T/out" 2>&1
}
expect_fail() { local m="$1"; shift; run ENVIRONMENT=dev "$@"; if [ $? -ne 0 ] && grep -q "FAILED" "$T/out"; then ok "$m"; else bad "$m"; cat "$T/out"; fi; }
expect_ok() { local m="$1"; shift; run ENVIRONMENT=dev "$@"; if [ $? -eq 0 ]; then ok "$m"; else bad "$m"; cat "$T/out"; fi; }
no_mutation() { if grep -qE -- "--delete|--alter" "$T/calls"; then bad "$1: a barrier never alters or recreates a topic"; else ok "$1: nothing altered or deleted"; fi; }

# --- dev only ---------------------------------------------------------------------------------------------------------------------------
for e in production es4 ""; do
  fresh; run ENVIRONMENT="$e"; rc=$?
  { [ $rc -eq 1 ] && grep -q "DEV ONLY" "$T/out" && [ ! -s "$T/calls" ]; } && ok "ENVIRONMENT='$e' is refused before any CLI call" || { bad "ENVIRONMENT='$e'"; cat "$T/out"; }
done
fresh; run; rc=$?
{ [ $rc -eq 1 ] && grep -q "DEV ONLY" "$T/out" && [ ! -s "$T/calls" ]; } && ok "ENVIRONMENT unset is refused before any CLI call" || { bad "ENVIRONMENT unset"; cat "$T/out"; }

# --- an empty dev cluster -------------------------------------------------------------------------------------------------------------------
fresh; run ENVIRONMENT=dev; rc=$?
[ $rc -eq 0 ] && ok "empty cluster → rc 0" || { bad "empty cluster rc $rc"; cat "$T/out"; }
grep -q "=== ensure-es-liquidity-wall-topics OK ===" "$T/out" && ok "…reports OK" || bad "…no OK line"
[ "$(cfg $CL)" = "1 delete $SEVEN " ] && ok "closures created: 1 partition, delete, 7 days" || bad "closures: $(cfg $CL)"
[ "$(cfg $BU)" = "1 delete $DAY " ] && ok "buckets created: 1 partition, delete, 1 day" || bad "buckets: $(cfg $BU)"
for t in $DA $ST $SN $VO; do [ "$(cfg $t)" = "1 compact -1 -1" ] && ok "$t created: 1 partition, pure compact, retention.ms -1, retention.bytes -1" || bad "$t: $(cfg $t)"; done
[ "$(cfg $EV)" = "1 delete -1 $CAP" ] && ok "events created: 1 partition, delete, retention.ms -1 WITH retention.bytes $CAP" || bad "events: $(cfg $EV)"
grep -q -- "--create --if-not-exists --topic $EV --partitions 1 .*cleanup.policy=delete .*retention.ms=-1 .*retention.bytes=$CAP" "$T/calls" && ok "events create carries the cap" || bad "events create call lacks the cap"
for t in $DA $ST $SN $VO; do grep -q -- "--create --if-not-exists --topic $t --partitions 1 .*cleanup.policy=compact .*retention.ms=-1 .*retention.bytes=-1" "$T/calls" && ok "$t create carries retention.bytes=-1 explicitly" || bad "$t create lacks retention.bytes=-1"; done
[ "$(grep -c -- '--create' "$T/calls")" = 7 ] && ok "exactly seven creates" || bad "$(grep -c -- '--create' "$T/calls") creates"
if grep -qE -- "--entity-type brokers|--entity-default|message.max.bytes" "$T/calls"; then bad "a broker config was touched"; else ok "no broker-wide change"; fi
if grep -qE -- "--delete|--alter" "$T/calls"; then bad "a topic was altered or deleted"; else ok "nothing altered or deleted"; fi

# --- a second run changes nothing -----------------------------------------------------------------------------------------------------------
: > "$T/calls"; expect_ok "second run on the result → ok"
if grep -qE -- "--create|--delete|--alter" "$T/calls"; then bad "second run mutated"; else ok "second run created, altered and deleted nothing"; fi

# --- a complete, correct cluster (the fake's -1 default on retention.bytes for the broker-default topics) --------------------------------------
all_good; expect_ok "all seven present at the declared shapes → ok, nothing created"
if grep -q -- "--create" "$T/calls"; then bad "created on a complete cluster"; else ok "…nothing created"; fi

# --- every difference fails closed, names the topic, alters nothing ----------------------------------------------------------------------------
f() { local label="$1" t="$2"; shift 2; all_good; topic "$t" "$@"; expect_fail "$label"; grep -q "'$t'" "$T/out" && ok "…names $t" || bad "…does not name $t"; no_mutation "$label"; }
f "closures retention one millisecond short" $CL 1 delete 604799999 -1
f "closures compacted" $CL 1 compact,delete $SEVEN -1
f "closures at 32 partitions (the dev auto-create default)" $CL 32 delete $SEVEN -1
f "buckets at 7 days instead of 1" $BU 1 delete $SEVEN -1
f "buckets compacted" $BU 1 compact $DAY -1
f "dashboard delete" $DA 1 delete -1 -1
f "dashboard compact,delete" $DA 1 compact,delete -1 -1
f "dashboard finite retention.ms" $DA 1 compact $SEVEN -1
f "dashboard with a finite retention.bytes (byte-based deletion)" $DA 1 compact -1 $CAP
f "state at 3 partitions" $ST 3 compact -1 -1
f "snapshot delete" $SN 1 delete -1 -1
f "voice finite retention.bytes" $VO 1 compact -1 1048576
f "events COMPACTED (every event must survive until the cap)" $EV 1 compact -1 $CAP
f "events compact,delete" $EV 1 compact,delete -1 $CAP
f "events finite retention.ms" $EV 1 delete $SEVEN $CAP
f "events without its cap (unbounded)" $EV 1 delete -1 -1
f "events with a different cap" $EV 1 delete -1 134217728
f "events at 32 partitions" $EV 32 delete -1 $CAP
f "events message bound below 1 MiB" $EV 1 delete -1 $CAP 524288
f "min.insync.replicas below the minimum" $VO 1 compact -1 -1 1048576 0
all_good; topic $EV 1 delete -1 $CAP 16777216; expect_ok "events message bound above the floor → ok"

# --- a missing events topic is created with the cap even when the others exist ------------------------------------------------------------------
all_good; python3 -c 'import json,sys; f=sys.argv[1]; s=json.load(open(f)); del s["topics"][sys.argv[2]]; json.dump(s, open(f,"w"))' "$T/state.json" $EV
expect_ok "only the events log absent → created with the cap, verified"
[ "$(cfg $EV)" = "1 delete -1 $CAP" ] && ok "…events at the declared shape" || bad "…events: $(cfg $EV)"

# --- infrastructure failures are never success ------------------------------------------------------------------------------------------------
all_good; rm "$T/bin/kafka-topics"; run ENVIRONMENT=dev; [ $? -ne 0 ] && ok "topic list unreadable → fail" || bad "list unreadable reported success"
cp "$FAKE" "$T/bin/kafka-topics"; chmod +x "$T/bin/kafka-topics"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
