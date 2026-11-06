#!/usr/bin/env bash
# Drives the REAL ensure-line-health-topics.sh against the STATEFUL stand-in tests/fake_kafka_cli.py (as kafka-topics / kafka-configs), the same stand-in
# tests/test_line_acceptance_deploy.py drives its barrier with. The barrier's points: DEV ONLY (any other ENVIRONMENT fails before a CLI call); an empty cluster gets the seven topics at
# their declared shapes with NO broker-wide change (retention.bytes -1 everywhere, the snapshot at a 9 MiB message bound); a second run creates and alters nothing; and every
# difference (partitions, policy, retention.ms, retention.bytes, message bound, ISR) fails closed, names the topic and alters nothing.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
FAKE="$HERE/../../tests/fake_kafka_cli.py"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
P=futures.line-health
ST=$P.state; EV=$P.events; VO=$P.voice; UL=$P.user-lines; SU=$P.status; CL=$P.closures; SN=$P.snapshot
SEVEN=604800000; BIG=9437184
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
all_good() { fresh; topic $ST 1 compact,delete $SEVEN -1; topic $EV 1 delete $SEVEN -1; topic $VO 1 delete $SEVEN -1; topic $UL 1 compact,delete $SEVEN -1; topic $SU 1 compact -1 -1; topic $CL 1 delete $SEVEN -1; topic $SN 1 compact -1 -1 $BIG; }
cfg() { python3 -c 'import json,sys; t=json.load(open(sys.argv[1]))["topics"].get(sys.argv[2]); print("" if t is None else str(t["partitions"])+" "+" ".join(t["configs"].get(k,"") for k in ("cleanup.policy","retention.ms","retention.bytes")))' "$T/state.json" "$1"; }
run() { # [ENV=...] -> rc; output in $T/out, calls in $T/calls
  env -u ENVIRONMENT PATH="$T/bin:$PATH" FAKE_KAFKA_STATE="$T/state.json" FAKE_KAFKA_CALLS="$T/calls" KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 "$@" bash "$HERE/ensure-line-health-topics.sh" > "$T/out" 2>&1
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
grep -q "=== ensure-line-health-topics OK ===" "$T/out" && ok "…reports OK" || bad "…no OK line"
for t in $ST $UL; do [ "$(cfg $t)" = "1 compact,delete $SEVEN -1" ] && ok "$t created: 1 partition, compact,delete, 7 days, retention.bytes -1" || bad "$t: $(cfg $t)"; done
for t in $EV $VO $CL; do [ "$(cfg $t)" = "1 delete $SEVEN -1" ] && ok "$t created: 1 partition, delete, 7 days, retention.bytes -1" || bad "$t: $(cfg $t)"; done
for t in $SU $SN; do [ "$(cfg $t)" = "1 compact -1 -1" ] && ok "$t created: 1 partition, pure compact, -1/-1" || bad "$t: $(cfg $t)"; done
grep -q -- "--create --if-not-exists --topic $SN --partitions 1 .*max.message.bytes=$BIG" "$T/calls" && ok "snapshot created with the 9 MiB message bound" || bad "snapshot create lacks the bound"
[ "$(grep -c -- '--create' "$T/calls")" = 7 ] && ok "exactly seven creates" || bad "$(grep -c -- '--create' "$T/calls") creates"
[ "$(grep -c -- 'retention.bytes=-1' "$T/calls")" -ge 7 ] && ok "every create carries retention.bytes=-1 explicitly" || bad "a create lacks retention.bytes=-1"
if grep -qE -- "--entity-type brokers|--entity-default|message.max.bytes" "$T/calls"; then bad "a broker config was touched"; else ok "no broker-wide change"; fi
if grep -qE -- "--delete|--alter" "$T/calls"; then bad "a topic was altered or deleted"; else ok "nothing altered or deleted"; fi
# a read-back count with a sentinel: seven topics, and the sentinel proves the counter is not defaulting
n=$(python3 -c 'import json,sys; print(len([t for t in json.load(open(sys.argv[1]))["topics"] if t.startswith("futures.line-health.")]))' "$T/state.json")
[ "$n" = 7 ] && ok "state read-back counts 7 line-health topics" || bad "read-back counted '$n'"

# --- a second run changes nothing -----------------------------------------------------------------------------------------------------------
: > "$T/calls"; expect_ok "second run on the result → ok"
if grep -qE -- "--create|--delete|--alter" "$T/calls"; then bad "second run mutated"; else ok "second run created, altered and deleted nothing"; fi

# --- a complete, correct cluster ----------------------------------------------------------------------------------------------------------------
all_good; expect_ok "all seven present at the declared shapes → ok, nothing created"
if grep -q -- "--create" "$T/calls"; then bad "created on a complete cluster"; else ok "…nothing created"; fi

# --- every difference fails closed, names the topic, alters nothing ----------------------------------------------------------------------------
f() { local label="$1" t="$2"; shift 2; all_good; topic "$t" "$@"; expect_fail "$label"; grep -q "'$t'" "$T/out" && ok "…names $t" || bad "…does not name $t"; no_mutation "$label"; }
f "state retention one millisecond short" $ST 1 compact,delete 604799999 -1
f "state plain delete (the latest line would age out)" $ST 1 delete $SEVEN -1
f "state pure compact" $ST 1 compact $SEVEN -1
f "state at 32 partitions (the dev auto-create default)" $ST 32 compact,delete $SEVEN -1
f "events compacted" $EV 1 compact,delete $SEVEN -1
f "events kept forever" $EV 1 delete -1 -1
f "events with a byte cap nobody asked for" $EV 1 delete $SEVEN 268435456
f "voice at 3 partitions" $VO 3 delete $SEVEN -1
f "user-lines plain delete" $UL 1 delete $SEVEN -1
f "user-lines pure compact" $UL 1 compact $SEVEN -1
f "status delete" $SU 1 delete -1 -1
f "status compact,delete" $SU 1 compact,delete $SEVEN -1
f "status with a finite retention.bytes" $SU 1 compact -1 1048576
f "closures compacted" $CL 1 compact $SEVEN -1
f "closures finite retention.ms of 1 day" $CL 1 delete 86400000 -1
f "snapshot delete" $SN 1 delete -1 -1
f "snapshot message bound below 9 MiB" $SN 1 compact -1 -1 1048576
f "snapshot finite retention.ms" $SN 1 compact $SEVEN -1 $BIG
f "min.insync.replicas below the minimum" $VO 1 delete $SEVEN -1 1048576 0
all_good; topic $SN 1 compact -1 -1 33554432; expect_ok "snapshot message bound above the floor → ok"

# --- a missing topic is created even when the others exist -----------------------------------------------------------------------------------------
all_good; python3 -c 'import json,sys; f=sys.argv[1]; s=json.load(open(f)); del s["topics"][sys.argv[2]]; json.dump(s, open(f,"w"))' "$T/state.json" $SN
expect_ok "only the snapshot absent → created with its bound, verified"
[ "$(cfg $SN)" = "1 compact -1 -1" ] && ok "…snapshot at the declared shape" || bad "…snapshot: $(cfg $SN)"

# --- the level-absorption barrier chains to this one on dev and says so for anything else ---------------------------------------------------
LA="$HERE/ensure-level-absorption-topics.sh"
chain() { env -u ENVIRONMENT PATH="$T/bin:$PATH" FAKE_KAFKA_STATE="$T/state.json" FAKE_KAFKA_CALLS="$T/calls" KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 "$@" bash "$LA" > "$T/out" 2>&1; }
fresh; chain ENVIRONMENT=production; rc=$?
{ [ $rc -eq 0 ] && grep -q "line-health topics: NOT ensured (ENVIRONMENT='production'" "$T/out" && ! grep -q "futures.line-health" "$T/calls"; } && ok "level-absorption barrier on production: line-health topics not touched, said out loud" || { bad "production chain"; cat "$T/out"; }
fresh; chain; rc=$?
{ [ $rc -eq 0 ] && grep -q "line-health topics: NOT ensured (ENVIRONMENT='<unset>'" "$T/out" && ! grep -q "futures.line-health" "$T/calls"; } && ok "level-absorption barrier with ENVIRONMENT unset: line-health topics not touched, said out loud" || { bad "unset chain"; cat "$T/out"; }
fresh; chain ENVIRONMENT=dev LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES=2152919; rc=$?
n=$(python3 -c 'import json,sys; print(len([t for t in json.load(open(sys.argv[1]))["topics"] if t.startswith("futures.line-health.")]))' "$T/state.json")
{ [ $rc -eq 0 ] && [ "$n" = 7 ] && grep -q "=== ensure-line-health-topics OK ===" "$T/out"; } && ok "level-absorption barrier on dev creates and verifies the seven line-health topics" || { bad "dev chain (rc $rc, $n topics)"; cat "$T/out"; }
fresh; topic $P.closures 1 delete 1 -1; chain ENVIRONMENT=dev LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES=2152919; rc=$?
{ [ $rc -ne 0 ] && grep -q "line-health topics are wrong" "$T/out"; } && ok "a wrong line-health topic fails the level-absorption barrier" || { bad "dev chain failure"; cat "$T/out"; }

# --- infrastructure failures are never success ------------------------------------------------------------------------------------------------
all_good; rm "$T/bin/kafka-topics"; run ENVIRONMENT=dev; [ $? -ne 0 ] && ok "topic list unreadable → fail" || bad "list unreadable reported success"
cp "$FAKE" "$T/bin/kafka-topics"; chmod +x "$T/bin/kafka-topics"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
