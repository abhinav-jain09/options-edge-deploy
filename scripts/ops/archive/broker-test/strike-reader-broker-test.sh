#!/usr/bin/env bash
# strike-reader-broker-test.sh — runs the REAL StrikeArchiveReader.java against a REAL, DISPOSABLE broker.
#
# test-archive-reset.sh proves what the archiver does with a reader's answer, through a shim. This proves
# the answer itself: that endOffsets() under read_committed is the last stable offset, that the position
# walks over commit/abort markers and aborted records, that a transaction left open holds the boundary
# below it and its records are captured once it commits, that the time cap is exclusive, that
# LogAppendTime is printed as such, that an offset below the log start FAILS, and that the archive marker
# round-trips through a consumer group. It needs a broker, so it is NOT part of the Jenkins suite.
#
# It CREATES one uniquely named topic (and one uniquely named consumer group), writes only to them, and
# deletes both on exit. It refuses any bootstrap that is not on this machine: never point it at a shared
# broker. Recorded run (dev Kafka, 2026-09-11): ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-FINAL.md.
#
#   KAFKA_HOME=/path/to/kafka BOOTSTRAP=127.0.0.1:19092 bash strike-reader-broker-test.sh
set -uo pipefail
KAFKA_HOME="${KAFKA_HOME:?set KAFKA_HOME to a Kafka install (bin/ and libs/)}"
B="${BOOTSTRAP:-127.0.0.1:9092}"
case "$B" in
  127.0.0.1:*|localhost:*) : ;;
  *) echo "REFUSING: BOOTSTRAP=$B is not a local broker — this harness creates and deletes topics" >&2; exit 2 ;;
esac
K="$KAFKA_HOME/bin"; L="$KAFKA_HOME/libs"
HERE="$(cd "$(dirname "$0")" && pwd)"
READER="$HERE/../StrikeArchiveReader.java"
T="oe-strike-reader-test-$(date +%s)"
G="$T-archive-marker"
W="$(mktemp -d)"
echo "TOPIC=$T GROUP=$G WORKDIR=$W"
fails=0
chk() { if [ "$2" = "$3" ]; then echo "  ok   $1 ($3)"; else echo "  FAIL $1 (want $2, got $3)"; fails=$((fails+1)); fi; }
field() { tr ' ' '\n' < "$1" | awk -F= -v k="$2" '$1==k{print $2}'; }
offsets() { awk -F'\t' '{sub("Offset:","",$3); printf "%s ", $3}' "$1" | sed 's/ $//'; }
reader() { java -cp "$L/*" "$READER" --bootstrap "$B" --topic "$T" --partition 0 "$@" 2>>"$W/reader.stderr"; }
cleanup() {
  [ -n "${FPID:-}" ] && kill "$FPID" 2>/dev/null
  "$K/kafka-consumer-groups.sh" --bootstrap-server "$B" --delete --group "$G" 2>&1 | sed 's/^/  cleanup: /'
  "$K/kafka-topics.sh" --bootstrap-server "$B" --delete --topic "$T" 2>&1 | sed 's/^/  cleanup: /'
  sleep 2
  if "$K/kafka-topics.sh" --bootstrap-server "$B" --list 2>/dev/null | grep -qx "$T"; then echo "  cleanup: TOPIC STILL PRESENT"; else echo "  cleanup: topic $T deleted"; fi
  rm -rf "$W"
}
trap cleanup EXIT

"$K/kafka-topics.sh" --bootstrap-server "$B" --create --topic "$T" --partitions 1 --replication-factor 1 >/dev/null || { echo "create failed"; exit 1; }
java -cp "$L/*" "$HERE/TxnFixture.java" "$B" "$T" "$W" 2>"$W/fixture.stderr" &
FPID=$!
for _ in $(seq 1 150); do [ -f "$W/OPEN" ] && break; sleep 0.2; done
[ -f "$W/OPEN" ] || { echo "fixture never opened its transaction"; cat "$W/fixture.stderr"; exit 1; }
echo "HWM with the transaction open: $("$K/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$T")"

echo "== 1. committed + aborted below an OPEN transaction (from 0)"
reader --from 0 --deadline-ms 30000 --out "$W/r1.out" --summary "$W/r1.sum"; rc=$?
cat "$W/r1.sum"
chk "exit 0" 0 "$rc"; chk "status" COMPLETE "$(field "$W/r1.sum" status)"
chk "boundary = LSO = first offset of the open txn" 10 "$(field "$W/r1.sum" boundary)"
chk "position reached boundary" 1 "$([ "$(field "$W/r1.sum" position)" -ge 10 ] && echo 1 || echo 0)"
chk "records = the 5 committed" 5 "$(field "$W/r1.sum" records)"
chk "lines in file = records" 5 "$(wc -l < "$W/r1.out" | tr -d ' ')"
chk "offsets written" "0 1 2 7 8" "$(offsets "$W/r1.out")"
chk "no aborted record" 0 "$(grep -c aborted "$W/r1.out")"
chk "no open-txn record" 0 "$(grep -c late "$W/r1.out")"
chk "escaped count (k2 carries a raw TAB+LF)" 1 "$(field "$W/r1.sum" escaped)"
chk "k2 escaped as \\t and \\n on ONE line" 1 "$(grep -c 'tab\\there\\nnewline' "$W/r1.out")"
chk "timestamp type printed" 5 "$(grep -c '^CreateTime:[0-9]*	Partition:0	Offset:' "$W/r1.out")"

echo "== 2. the open transaction COMMITS, then an abort-only transaction follows"
touch "$W/COMMIT"
for _ in $(seq 1 100); do [ -f "$W/DONE" ] && break; sleep 0.2; done
[ -f "$W/DONE" ] || { echo "fixture did not finish"; cat "$W/fixture.stderr"; exit 1; }
wait "$FPID"; FPID=""
reader --from 10 --deadline-ms 30000 --out "$W/r2.out" --summary "$W/r2.sum"; rc=$?
cat "$W/r2.sum"
chk "retry exit 0" 0 "$rc"; chk "retry boundary = HWM (nothing open)" 15 "$(field "$W/r2.sum" boundary)"
chk "retry captures BOTH later-committed records" 2 "$(field "$W/r2.sum" records)"
chk "retry offsets" "10 11" "$(offsets "$W/r2.out")"
chk "no aborted a2" 0 "$(grep -c aborted "$W/r2.out")"

echo "== 3. marker + aborted-only range [12,15): completes with ZERO records"
reader --from 12 --deadline-ms 30000 --out "$W/r3.out" --summary "$W/r3.sum"; rc=$?
cat "$W/r3.sum"
chk "marker-only exit 0" 0 "$rc"; chk "status" COMPLETE "$(field "$W/r3.sum" status)"
chk "records 0" 0 "$(field "$W/r3.sum" records)"; chk "boundary" 15 "$(field "$W/r3.sum" boundary)"
chk "position walked over markers+aborted to the boundary" 1 "$([ "$(field "$W/r3.sum" position)" -ge 15 ] && echo 1 || echo 0)"
chk "empty file" 0 "$(wc -c < "$W/r3.out" | tr -d ' ')"

echo "== 4. already at the boundary (from 15): nothing to do, COMPLETE"
reader --from 15 --deadline-ms 30000 --out "$W/r4.out" --summary "$W/r4.sum"; rc=$?
chk "exit 0" 0 "$rc"; chk "records 0" 0 "$(field "$W/r4.sum" records)"

echo "== 5. time-bounded (--max-end 8): boundary = min(LSO, 8), record at 8 NOT written"
reader --from 0 --max-end 8 --deadline-ms 30000 --out "$W/r5.out" --summary "$W/r5.sum"; rc=$?
cat "$W/r5.sum"
chk "exit 0" 0 "$rc"; chk "boundary 8" 8 "$(field "$W/r5.sum" boundary)"
chk "offsets < 8 only" "0 1 2 7" "$(offsets "$W/r5.out")"

echo "== 6. LogAppendTime is printed as such"
"$K/kafka-configs.sh" --bootstrap-server "$B" --entity-type topics --entity-name "$T" --alter --add-config message.timestamp.type=LogAppendTime >/dev/null
printf 'lk\t{"lat":1}\n' | "$K/kafka-console-producer.sh" --bootstrap-server "$B" --topic "$T" --property parse.key=true >/dev/null 2>&1
reader --from 15 --deadline-ms 30000 --out "$W/r6.out" --summary "$W/r6.sum"; rc=$?
cat "$W/r6.sum"; echo "   line: $(cat "$W/r6.out")"
chk "exit 0" 0 "$rc"; chk "LogAppendTime line" 1 "$(grep -c '^LogAppendTime:[0-9]*	Partition:0	Offset:15	lk	{"lat":1}$' "$W/r6.out")"

echo "== 7. offset below the retained log start is a FAILURE (auto.offset.reset=none)"
"$K/kafka-delete-records.sh" --bootstrap-server "$B" --offset-json-file <(printf '{"partitions":[{"topic":"%s","partition":0,"offset":7}],"version":1}' "$T") >/dev/null 2>&1
reader --from 0 --deadline-ms 15000 --out "$W/r7.out" --summary "$W/r7.sum"; rc=$?
cat "$W/r7.sum"
chk "non-zero exit" 1 "$([ "$rc" -ne 0 ] && echo 1 || echo 0)"; chk "status FAILED" FAILED "$(field "$W/r7.sum" status)"

echo "== 8. unreachable broker within the deadline is a FAILURE, not a short success"
java -cp "$L/*" "$READER" --bootstrap 127.0.0.1:1 --topic "$T" --partition 0 --from 0 --deadline-ms 3000 --out "$W/r8.out" --summary "$W/r8.sum" 2>>"$W/reader.stderr"; rc=$?
cat "$W/r8.sum"
chk "non-zero exit" 1 "$([ "$rc" -ne 0 ] && echo 1 || echo 0)"

echo "== 9. archive marker: commit boundary 16 to a dedicated group, read it back"
reader --mark-group "$G" --mark-offset 16 --mark-metadata "dt=test" --deadline-ms 30000 --summary "$W/r9.sum"; rc=$?
cat "$W/r9.sum"
chk "mark exit 0" 0 "$rc"
"$K/kafka-consumer-groups.sh" --bootstrap-server "$B" --describe --group "$G" 2>/dev/null | tee "$W/r9.describe"
chk "describe shows CURRENT-OFFSET 16, LAG 0" "16 16 0" "$(awk -v t="$T" '$2==t && $3==0 {print $4, $5, $6}' "$W/r9.describe")"

echo "RESULT: fails=$fails"
exit "$fails"
