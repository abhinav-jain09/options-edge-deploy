#!/usr/bin/env bash
# Drives the REAL ensure-break-line-topics.sh against the STATEFUL stand-in tests/fake_kafka_cli.py (as kafka-topics / kafka-configs), the same stand-in
# ensure-es-liquidity-wall-topics-test.sh drives its barrier with. The barrier's points: DEV ONLY (any other ENVIRONMENT fails before a CLI call); an empty cluster gets the five topics at
# their declared shapes with NO broker-wide change (the events log at 30 days, the four pure-compact topics at -1/-1, the snapshot with its 9437184-byte message bound); a second run creates and
# alters nothing; and every difference (partitions, policy, retention.ms, retention.bytes, message bound, ISR) fails closed, names the topic and alters nothing.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
FAKE="$HERE/../../tests/fake_kafka_cli.py"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
P=futures.break-line
LI=$P.lines; ST=$P.state; EV=$P.events; SA=$P.status; SN=$P.snapshot
DAYS30=2592000000; BIG=9437184
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
all_good() { fresh; topic $LI 1 compact -1 -1; topic $ST 1 compact -1 -1; topic $EV 1 delete $DAYS30 -1; topic $SA 1 compact -1 -1; topic $SN 1 compact -1 -1 $BIG; }
cfg() { python3 -c 'import json,sys; t=json.load(open(sys.argv[1]))["topics"].get(sys.argv[2]); print("" if t is None else str(t["partitions"])+" "+" ".join(t["configs"].get(k,"") for k in ("cleanup.policy","retention.ms","retention.bytes","max.message.bytes")))' "$T/state.json" "$1"; }
run() { # [ENV=...] -> rc; output in $T/out, calls in $T/calls
  env -u ENVIRONMENT PATH="$T/bin:$PATH" FAKE_KAFKA_STATE="$T/state.json" FAKE_KAFKA_CALLS="$T/calls" KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 "$@" bash "$HERE/ensure-break-line-topics.sh" > "$T/out" 2>&1
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
grep -q "=== ensure-break-line-topics OK ===" "$T/out" && ok "…reports OK" || bad "…no OK line"
for t in $LI $ST $SA; do [ "$(cfg $t)" = "1 compact -1 -1 1048576" ] && ok "$t created: 1 partition, pure compact, retention.ms -1, retention.bytes -1" || bad "$t: $(cfg $t)"; done
[ "$(cfg $SN)" = "1 compact -1 -1 $BIG" ] && ok "snapshot created: pure compact WITH max.message.bytes $BIG" || bad "snapshot: $(cfg $SN)"
case "$(cfg $EV)" in "1 delete $DAYS30 "*" 1048576") ok "events created: 1 partition, delete, retention.ms 30 days" ;; *) bad "events: $(cfg $EV)" ;; esac # || bad "events: $(cfg $EV)"
for t in $LI $ST $SA $SN; do grep -q -- "--create --if-not-exists --topic $t --partitions 1 .*cleanup.policy=compact .*retention.ms=-1 .*retention.bytes=-1" "$T/calls" && ok "$t create carries retention.bytes -1" || bad "$t create without retention.bytes -1"; done
grep -q -- "--create --if-not-exists --topic $EV --partitions 1 .*cleanup.policy=delete .*retention.ms=$DAYS30" "$T/calls" && ok "events create carries 30 days" || bad "events create without 30 days"
[ "$(grep -c -- '--create' "$T/calls")" = 5 ] && ok "exactly five creates" || bad "$(grep -c -- '--create' "$T/calls") creates"
if grep -qE -- "--entity-type brokers|--entity-default|message.max.bytes" "$T/calls"; then bad "a broker config was touched"; else ok "no broker-wide change"; fi
if grep -qE -- "--delete|--alter" "$T/calls"; then bad "a topic was altered or deleted"; else ok "nothing altered or deleted"; fi

# --- a second run changes nothing -----------------------------------------------------------------------------------------------------------
: > "$T/calls"; expect_ok "second run on the result → ok"
if grep -qE -- "--create|--delete|--alter" "$T/calls"; then bad "second run mutated"; else ok "second run created, altered and deleted nothing"; fi

# --- a complete, correct cluster ------------------------------------------------------------------------------------------------------------
all_good; expect_ok "all five present at the declared shapes → ok, nothing created"
if grep -q -- "--create" "$T/calls"; then bad "created on a complete cluster"; else ok "…nothing created"; fi

# --- every difference fails closed, names the topic, alters nothing ----------------------------------------------------------------------------
f() { local label="$1" t="$2"; shift 2; all_good; topic "$t" "$@"; expect_fail "$label"; grep -q "'$t'" "$T/out" && ok "…names $t" || bad "…does not name $t"; no_mutation "$label"; }
f "lines delete instead of compact" $LI 1 delete -1 -1
f "lines compact,delete" $LI 1 compact,delete -1 -1
f "lines finite retention.ms" $LI 1 compact 604800000 -1
f "lines with a finite retention.bytes (byte-based deletion of user data)" $LI 1 compact -1 268435456
f "lines at 32 partitions (the dev auto-create default)" $LI 32 compact -1 -1
f "state delete" $ST 1 delete -1 -1
f "state at 3 partitions" $ST 3 compact -1 -1
f "status finite retention.bytes" $SA 1 compact -1 1048576
f "status compact,delete" $SA 1 compact,delete -1 -1
f "snapshot message bound below 9437184" $SN 1 compact -1 -1 1048576
f "snapshot delete" $SN 1 delete -1 -1
f "events COMPACTED (every event must survive its 30 days)" $EV 1 compact $DAYS30 -1
f "events compact,delete" $EV 1 compact,delete $DAYS30 -1
f "events retention.ms one millisecond short" $EV 1 delete 2591999999 -1
f "events retention -1 (never ages out)" $EV 1 delete -1 -1
f "events at 32 partitions" $EV 32 delete $DAYS30 -1
f "events message bound below 1 MiB" $EV 1 delete $DAYS30 -1 524288
f "min.insync.replicas below the minimum" $SA 1 compact -1 -1 1048576 0
all_good; topic $SN 1 compact -1 -1 16777216; expect_ok "snapshot message bound above the floor → ok"

# --- a missing topic is created at its shape even when the others exist ------------------------------------------------------------------------
all_good; python3 -c 'import json,sys; f=sys.argv[1]; s=json.load(open(f)); del s["topics"][sys.argv[2]]; json.dump(s, open(f,"w"))' "$T/state.json" $SN
expect_ok "only the snapshot absent → created with its bound, verified"
[ "$(cfg $SN)" = "1 compact -1 -1 $BIG" ] && ok "…snapshot at the declared shape" || bad "…snapshot: $(cfg $SN)"

# --- infrastructure failures are never success ------------------------------------------------------------------------------------------------
all_good; rm "$T/bin/kafka-topics"; run ENVIRONMENT=dev; [ $? -ne 0 ] && ok "topic list unreadable → fail" || bad "list unreadable reported success"
cp "$FAKE" "$T/bin/kafka-topics"; chmod +x "$T/bin/kafka-topics"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
