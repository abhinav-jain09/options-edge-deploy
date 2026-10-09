#!/usr/bin/env bash
# tape-preserve-broker-test.sh — drives the REAL TapePreserve.java against a REAL broker. The stubbed
# tape-preserve-test.sh proves the wrapper's decisions; this proves the tool's guarantees, including the
# failure paths, by forcing them (TAPE_PRESERVE_FAULT is a test-only hook in the tool).
#
#   TAPE_PRESERVE_TEST_BOOTSTRAP=127.0.0.1:19092 bash scripts/es4/tape-preserve-broker-test.sh
#
# SKIPS (exit 0) unless TAPE_PRESERVE_TEST_BOOTSTRAP is set: CI has no broker. Point it at a DEV/scratch
# broker only. It creates topics and groups named zz-tape-bt-* and removes them again; it never touches
# anything else, and it never uses a recursive delete.
#   Needs: a JDK 17+ (source launch), the kafka-clients jar (KAFKA_LIBS), and Kafka CLIs (KBIN).
set -uo pipefail
BS="${TAPE_PRESERVE_TEST_BOOTSTRAP:-}"
[ -n "$BS" ] || { echo "skip: TAPE_PRESERVE_TEST_BOOTSTRAP not set (needs a scratch broker)"; exit 0; }
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIBS="${KAFKA_LIBS:-$HOME/kafka-4.3.0/libs}"
KBIN="${KBIN:-$HOME/development/confluent-7.3.1/bin}"
KAFKA_BIN="${KAFKA_BIN:-$HOME/kafka-4.3.0/bin}"   # Kafka 4.x CLIs: the share-group console consumer does not exist in 7.3.1
for x in kafka-topics kafka-console-producer kafka-console-consumer kafka-consumer-groups kafka-get-offsets; do
  [ -x "$KBIN/$x" ] || { echo "FATAL: $KBIN/$x not found (set KBIN)"; exit 2; }
done
ls "$LIBS"/kafka-clients-*.jar >/dev/null 2>&1 || { echo "FATAL: no kafka-clients jar in $LIBS (set KAFKA_LIBS)"; exit 2; }
[ -x "$KAFKA_BIN/kafka-console-share-consumer.sh" ] || { echo "skip: $KAFKA_BIN/kafka-console-share-consumer.sh not found (Kafka 4.x CLIs needed for the share-group drills; set KAFKA_BIN)"; exit 0; }
command -v timeout >/dev/null 2>&1 || { echo "FATAL: coreutils timeout required"; exit 2; }

fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
case_() { printf '\n%s\n' "$1"; }
W="$(cd "$(mktemp -d)" && pwd -P)"
RUN="zz-tape-bt-$$"
GROUPS_=(); PIDS=()
cleanup() {
  local p t g
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  sleep 2
  # mk() runs in a command substitution, so anything it registered is lost with that subshell: find our topics
  # by the unique run prefix instead.
  for t in $("$KBIN/kafka-topics" --bootstrap-server "$BS" --list 2>/dev/null | grep -F "$RUN."); do
    "$KBIN/kafka-topics" --bootstrap-server "$BS" --delete --topic "$t" >/dev/null 2>&1
  done
  for g in "${GROUPS_[@]:-}"; do
    [ -n "$g" ] || continue
    "$KBIN/kafka-consumer-groups" --bootstrap-server "$BS" --delete --group "$g" >/dev/null 2>&1
    "$KAFKA_BIN/kafka-share-groups.sh" --bootstrap-server "$BS" --delete --group "$g" >/dev/null 2>&1
  done
  find "$W" -mindepth 1 -delete 2>/dev/null; rmdir "$W" 2>/dev/null
}
trap cleanup EXIT

tool() { java -cp "$LIBS/*" "$HERE/tape-preserve/TapePreserve.java" "$@"; }
mk() { # <name> [extra --config ...]  -> creates a 4-partition topic and registers it for cleanup
  local t="$RUN.$1"; shift
  "$KBIN/kafka-topics" --bootstrap-server "$BS" --create --topic "$t" --partitions 4 --replication-factor 1 --config retention.ms=3600000 "$@" >/dev/null 2>&1
  printf '%s' "$t"
}
recreate() { # <topic> [extra --config ...]
  local t="$1"; shift
  "$KBIN/kafka-topics" --bootstrap-server "$BS" --delete --topic "$t" >/dev/null 2>&1; sleep 4
  "$KBIN/kafka-topics" --bootstrap-server "$BS" --create --topic "$t" --partitions 4 --replication-factor 1 --config retention.ms=3600000 "$@" >/dev/null 2>&1; sleep 2
}
seed() { # <topic> <n>  keyed records so every partition gets some
  python3 -c "
import sys
for i in range(int(sys.argv[1])): print('k%d:{\"seq\":%d,\"px\":%d.25}' % (i, i, 5000+i%97))" "$2" \
   | "$KBIN/kafka-console-producer" --bootstrap-server "$BS" --topic "$1" --property parse.key=true --property key.separator=: >/dev/null 2>&1
}
ends() { "$KBIN/kafka-get-offsets" --bootstrap-server "$BS" --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }
live() { paste -d' ' <("$KBIN/kafka-get-offsets" --bootstrap-server "$BS" --topic "$1" --time -2 2>/dev/null) <("$KBIN/kafka-get-offsets" --bootstrap-server "$BS" --topic "$1" 2>/dev/null) \
         | awk '{split($1,a,":"); split($2,b,":"); s+=b[3]-a[3]} END{print s+0}'; }
FROM=0

# ---------------------------------------------------------------------------------------------------
case_ "1. byte-exact round trip: partition, timestamp, key, value, headers survive a wipe"
T=$(mk rt); seed "$T" 3000
tool fingerprint "$BS" "$T" $FROM > "$W/fp-before.txt" 2>&1
tool export "$BS" "$T" $FROM "$W/rt.tape" > "$W/export.out" 2>&1
grep -q "records=3000" "$W/export.out" && ok "exported all 3000 records" || bad "export: $(cat "$W/export.out")"
recreate "$T"
tool import "$BS" "$W/rt.tape" > "$W/import.out" 2>&1; rc=$?
[ $rc = 0 ] && ok "import rc 0" || bad "import rc=$rc: $(cat "$W/import.out")"
tool fingerprint "$BS" "$T" $FROM > "$W/fp-after.txt" 2>&1
cmp -s "$W/fp-before.txt" "$W/fp-after.txt" && ok "per-partition sha256 over (partition, ts, key, value, headers) is IDENTICAL" || bad "fingerprints differ: $(diff "$W/fp-before.txt" "$W/fp-after.txt" | head -4)"

case_ "2. a second import onto the now-populated topic refuses (rc 4) and adds nothing"
before=$(ends "$T"); tool import "$BS" "$W/rt.tape" > "$W/o2.txt" 2>&1; rc=$?
[ $rc = 4 ] && [ "$(ends "$T")" = "$before" ] && ok "rc 4, end offsets unchanged" || bad "rc=$rc ends $before -> $(ends "$T")"

case_ "3. a recorded group with live members: refuse (rc 5) and produce NOTHING"
T=$(mk grp); seed "$T" 2000; G="$RUN-bridge"; GROUPS_+=("$G")
"$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$T" --group "$G" --from-beginning --consumer-property auto.commit.interval.ms=500 >/dev/null 2>&1 &
PIDS+=("$!"); CONSUMER=$!
sleep 6
tool export "$BS" "$T" $FROM "$W/grp.tape" > "$W/e3.txt" 2>&1
grep -q "groups=.*$G" "$W/e3.txt" && ok "export recorded the reading group" || bad "group not recorded: $(cat "$W/e3.txt")"
recreate "$T"
tool import "$BS" "$W/grp.tape" > "$W/i3.txt" 2>&1; rc=$?
[ $rc = 5 ] && [ "$(ends "$T")" = 0 ] && ok "rc 5 with the group active; end offsets still 0" || bad "rc=$rc ends=$(ends "$T") out=$(cat "$W/i3.txt")"

case_ "4. group stopped: restored, pinned to the end, and the resumed consumer reads NOTHING"
kill "$CONSUMER" 2>/dev/null; sleep 6
tool import "$BS" "$W/grp.tape" > "$W/i4.txt" 2>&1; rc=$?
[ $rc = 0 ] && grep -q "TAPE_PINNED group=$G" "$W/i4.txt" && ok "rc 0 and the group was pinned" || bad "rc=$rc out=$(cat "$W/i4.txt")"
# --from-beginning only applies to a group WITHOUT committed offsets, so 0 here can only mean the pin is in force;
# the control (a fresh group, same flag) proves the topic really does hold records to re-read.
n=$("$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$T" --group "$G" --from-beginning --timeout-ms 8000 2>/dev/null | wc -l | tr -d ' ')
GC="$RUN-control"; GROUPS_+=("$GC")
nc=$("$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$T" --group "$GC" --from-beginning --timeout-ms 8000 2>/dev/null | wc -l | tr -d ' ')
[ "$nc" -gt 0 ] && ok "control: an un-pinned group re-reads all $nc records (so the topic is not simply empty)" || bad "control read $nc"
[ "$n" = 0 ] && ok "the pinned group re-read 0 records (committed offsets override --from-beginning)" || bad "pinned group re-read $n records"

case_ "5. FORCED pin failure rolls the restore back to an empty topic (rc 7)"
T=$(mk pinf); seed "$T" 1500; G2="$RUN-pinf"; GROUPS_+=("$G2")
"$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$T" --group "$G2" --from-beginning --timeout-ms 6000 >/dev/null 2>&1
sleep 3
tool export "$BS" "$T" $FROM "$W/pinf.tape" > /dev/null 2>&1; recreate "$T"
TAPE_PRESERVE_FAULT=pin tool import "$BS" "$W/pinf.tape" > "$W/i5.txt" 2>&1; rc=$?
[ $rc = 7 ] && [ "$(live "$T")" = 0 ] && ok "rc 7; the topic holds 0 live records (fail closed)" || bad "rc=$rc live=$(live "$T") out=$(cat "$W/i5.txt")"
grep -q "TAPE_IMPORT_ROLLED_BACK" "$W/i5.txt" && ok "says it rolled back" || bad "no rollback line"

case_ "6. FORCED mid-import failure truncates what was produced (rc 1), and a retry then succeeds"
T=$(mk mid); seed "$T" 1500
tool export "$BS" "$T" $FROM "$W/mid.tape" > /dev/null 2>&1; recreate "$T"
TAPE_PRESERVE_FAULT=produce-midway tool import "$BS" "$W/mid.tape" > "$W/i6.txt" 2>&1; rc=$?
[ $rc = 1 ] && [ "$(live "$T")" = 0 ] && ok "rc 1; half-produced records were truncated away" || bad "rc=$rc live=$(live "$T")"
tool import "$BS" "$W/mid.tape" > "$W/i6b.txt" 2>&1; rc=$?
[ $rc = 0 ] && [ "$(live "$T")" = 1500 ] && ok "retry onto the truncated topic restores all 1500" || bad "retry rc=$rc live=$(live "$T")"

case_ "7. a LogAppendTime topic would overwrite the timestamps: refuse (rc 6), produce nothing"
T=$(mk lat); seed "$T" 800
tool export "$BS" "$T" $FROM "$W/lat.tape" > /dev/null 2>&1
recreate "$T" --config message.timestamp.type=LogAppendTime
tool import "$BS" "$W/lat.tape" > "$W/i7.txt" 2>&1; rc=$?
[ $rc = 6 ] && [ "$(ends "$T")" = 0 ] && ok "rc 6, end offsets 0" || bad "rc=$rc ends=$(ends "$T") out=$(cat "$W/i7.txt")"

case_ "8. state: EMPTY on a fresh topic, COMPLETE after a restore, PARTIAL when the count differs"
T=$(mk st); seed "$T" 900
tool export "$BS" "$T" $FROM "$W/st.tape" > /dev/null 2>&1; recreate "$T"
tool state "$BS" "$W/st.tape" 2>&1 | grep -q "TAPE_STATE EMPTY" && ok "EMPTY" || bad "not EMPTY"
tool import "$BS" "$W/st.tape" > /dev/null 2>&1
tool state "$BS" "$W/st.tape" 2>&1 | grep -q "TAPE_STATE COMPLETE" && ok "COMPLETE" || bad "not COMPLETE"
seed "$T" 5
tool state "$BS" "$W/st.tape" 2>&1 | grep -q "TAPE_STATE PARTIAL" && ok "PARTIAL once extra records exist" || bad "not PARTIAL"

case_ "10. the import process is KILLED mid-produce (a process that dies cannot roll itself back): the wrapper proves the topic empty"
T=$(mk kill); seed "$T" 3000
tool export "$BS" "$T" $FROM "$W/kill.tape" > /dev/null 2>&1; recreate "$T"
cp "$W/kill.tape" "$W/$T.tape"; sed -e "s/^topic=.*/topic=$T/" -e "s/^groups=.*/groups=/" "$W/kill.tape.manifest" > "$W/$T.tape.manifest"
out=$( ( export ES4_TAPE_JAVA="$(command -v java)" ES4_KAFKA_LIBS="$LIBS" ES4_TAPE_BOOTSTRAP="$BS" ES4_TAPE_PRESERVE_DIR="$W" \
              ES4_TAPE_PRESERVE_TOPICS="$T" ES4_TAPE_TIMEOUT_S=120 ES4_TAPE_IMPORT_TIMEOUT_S=25 DRY=false TAPE_PRESERVE_FAULT=hang-midway
         . "$HERE/tape-preserve.sh"
         tp_window() { echo "0 0 0"; }          # the calendar is not under test here
         tape_preserve_import; echo "WRAPPER_RC=$?" ) 2>&1 )
echo "$out" | grep -q "WRAPPER_RC=1" && ok "wrapper rc 1 (a warning, not UNSAFE)" || bad "wrapper: $(echo "$out" | tail -4)"
[ "$(live "$T")" = 0 ] && ok "the topic holds 0 live records after the kill" || bad "live=$(live "$T") (a partial tape survived)"
echo "$out" | grep -q "rolled back: $T is empty" && ok "reports the rollback it verified" || bad "no rollback line: $(echo "$out" | tail -3)"
[ -f "$W/$T.tape" ] && ok "artifact kept for a retry" || bad "artifact lost"

case_ "11. an UNRECORDED consumer that is reading the topic also blocks the restore (rc 5): we do not only trust the export's list"
T=$(mk unrec); seed "$T" 1200
tool export "$BS" "$T" $FROM "$W/unrec.tape" > /dev/null 2>&1; recreate "$T"
GU="$RUN-unrecorded"; GROUPS_+=("$GU")
"$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$T" --group "$GU" --from-beginning >/dev/null 2>&1 &
PIDS+=("$!"); UP=$!
sleep 7
tool import "$BS" "$W/unrec.tape" '^none' > "$W/i11.txt" 2>&1; rc=$?
[ $rc = 5 ] && [ "$(ends "$T")" = 0 ] && ok "rc 5, nothing produced" || bad "rc=$rc ends=$(ends "$T") out=$(cat "$W/i11.txt")"
grep -q "$GU" "$W/i11.txt" && ok "names the unrecorded reader" || bad "reader not named: $(cat "$W/i11.txt")"
kill "$UP" 2>/dev/null; sleep 6

case_ "12. a consumer that starts WHILE the history is being restored (fence 2): detected, rolled back (rc 7), named"
T=$(mk intr); seed "$T" 1200
tool export "$BS" "$T" $FROM "$W/intr.tape" > /dev/null 2>&1; recreate "$T"
GI="$RUN-intruder"; GROUPS_+=("$GI")
( TAPE_PRESERVE_FAULT=pause-before-fence2 tool import "$BS" "$W/intr.tape" '^none' > "$W/i12.txt" 2>&1; echo "RC=$?" >> "$W/i12.txt" ) &
IP=$!
for _ in $(seq 1 60); do grep -q "TAPE_IMPORTED" "$W/i12.txt" 2>/dev/null && break; sleep 1; done
"$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$T" --group "$GI" --from-beginning --consumer-property auto.commit.interval.ms=500 >/dev/null 2>&1 &
PIDS+=("$!"); XP=$!
wait "$IP"
kill "$XP" 2>/dev/null
grep -q "RC=7" "$W/i12.txt" && ok "rc 7" || bad "not rolled back: $(tail -3 "$W/i12.txt")"
grep -q "TAPE_IMPORT_FENCE_BREACH.*$GI" "$W/i12.txt" && ok "the intruding group is named in the breach report" || bad "breach not reported: $(cat "$W/i12.txt")"
[ "$(live "$T")" = 0 ] && ok "the topic was emptied again" || bad "live=$(live "$T")"

case_ "13. a SHARE group is invisible to the consumer-group tools: a live share member blocks the restore (rc 5), named, nothing produced"
T=$(mk shr); seed "$T" 1200
tool export "$BS" "$T" $FROM "$W/shr.tape" > /dev/null 2>&1; recreate "$T"
GS="$RUN-share"; GROUPS_+=("$GS")
"$KBIN/kafka-consumer-groups" --bootstrap-server "$BS" --list 2>/dev/null | grep -qF "$GS" && bad "premise: share group visible to consumer-groups" || ok "premise: the consumer-group tools do not list it"
"$KAFKA_BIN/kafka-console-share-consumer.sh" --bootstrap-server "$BS" --topic "$T" --group "$GS" >/dev/null 2>&1 &
PIDS+=("$!"); SP=$!
sleep 8
tool import "$BS" "$W/shr.tape" '^none' > "$W/i13.txt" 2>&1; rc=$?
[ $rc = 5 ] && [ "$(ends "$T")" = 0 ] && ok "rc 5, nothing produced" || bad "rc=$rc ends=$(ends "$T") out=$(cat "$W/i13.txt")"
grep -q "$GS" "$W/i13.txt" && ok "names the share group" || bad "share group not named: $(cat "$W/i13.txt")"
kill "$SP" 2>/dev/null; sleep 8

case_ "14. (routing + fail-closed smoke test, NOT a proof that a share pin prevents re-reads) a recorded SHARE group is pinned through the SHARE API, or the restore fails closed (rc 7, topic emptied) - never skipped"
# Whether this broker's share coordinator keeps state for an idle group varies, so the export may or may not list
# the group on its own. The manifest is therefore written to record it as SHARE (the shape an export produces on
# a broker that does) and the real import must then either pin it or roll back.
T=$(mk shr2); seed "$T" 900
tool export "$BS" "$T" $FROM "$W/shr2.tape" > "$W/e14.txt" 2>&1; recreate "$T"
sed -i.bak -e "s/^groups=.*/groups=$GS/" "$W/shr2.tape.manifest"; echo "groupTypes=SHARE" >> "$W/shr2.tape.manifest"
tool import "$BS" "$W/shr2.tape" > "$W/i14.txt" 2>&1; rc=$?
if [ $rc = 0 ] && grep -q "TAPE_PINNED group=$GS type=SHARE" "$W/i14.txt"; then ok "rc 0, pinned via the SHARE API (type=SHARE in the report)"
elif [ $rc = 7 ] && [ "$(live "$T")" = 0 ] && grep -q "TAPE_PIN_FAILED group=$GS" "$W/i14.txt"; then ok "pin via the SHARE API refused by this broker: rc 7 and the topic was emptied (fail closed)"
else bad "rc=$rc live=$(live "$T") out=$(cat "$W/i14.txt")"; fi

case_ "15. importer KILLED after the records landed but before its own scan, while an UNRECORDED consumer reads: the wrapper's settle must detect it, empty the topic and say so"
T=$(mk kfence); seed "$T" 1200
tool export "$BS" "$T" $FROM "$W/kf.tape" > /dev/null 2>&1; recreate "$T"
cp "$W/kf.tape" "$W/$T.tape"; sed -e "s/^topic=.*/topic=$T/" -e "s/^groups=.*/groups=/" -e "s/^groupTypes=.*/groupTypes=/" "$W/kf.tape.manifest" > "$W/$T.tape.manifest"
GK="$RUN-killed-intruder"; GROUPS_+=("$GK")
( for _ in $(seq 1 60); do [ "$(live "$T")" = 1200 ] && break; sleep 1; done
  "$KBIN/kafka-console-consumer" --bootstrap-server "$BS" --topic "$T" --group "$GK" --from-beginning --consumer-property auto.commit.interval.ms=500 >/dev/null 2>&1 ) &
PIDS+=("$!"); KP=$!
out=$( ( export ES4_TAPE_JAVA="$(command -v java)" ES4_KAFKA_LIBS="$LIBS" ES4_TAPE_BOOTSTRAP="$BS" ES4_TAPE_PRESERVE_DIR="$W" \
              ES4_TAPE_PRESERVE_TOPICS="$T" ES4_TAPE_TIMEOUT_S=120 ES4_TAPE_IMPORT_TIMEOUT_S=25 ES4_TAPE_UNPINNED_GROUPS='^none' DRY=false TAPE_PRESERVE_FAULT=pause-before-fence2
         . "$HERE/tape-preserve.sh"
         tp_window() { echo "0 0 0"; }
         tape_preserve_import; echo "WRAPPER_RC=$?" ) 2>&1 )
kill "$KP" 2>/dev/null; pkill -f "group $GK" 2>/dev/null; sleep 2
echo "$out" | grep -q "WRAPPER_RC=1" && ok "wrapper rc 1 (not restored, not UNSAFE)" || bad "wrapper: $(echo "$out" | tail -4)"
echo "$out" | grep -q "ERROR: a consumer started reading.*$GK" && ok "the unrecorded intruder is named in an ERROR" || bad "intruder not reported: $(echo "$out" | tail -5)"
[ "$(live "$T")" = 0 ] && ok "the topic was emptied" || bad "live=$(live "$T")"

case_ "9. truncate empties a topic and is idempotent"
tool truncate "$BS" "$T" > /dev/null 2>&1; tool truncate "$BS" "$T" > /dev/null 2>&1
[ "$(live "$T")" = 0 ] && ok "0 live records after two truncates" || bad "live=$(live "$T")"

echo
if [ "$fails" -eq 0 ]; then echo "=== tape-preserve-broker: OK ==="; exit 0; fi
echo "=== tape-preserve-broker: $fails problem(s) ===" >&2; exit 1
