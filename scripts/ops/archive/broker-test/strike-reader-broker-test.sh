#!/usr/bin/env bash
# strike-reader-broker-test.sh — runs the REAL StrikeArchiveReader.java against a REAL, DISPOSABLE broker.
#
# test-archive-reset.sh proves what the archiver does with a reader's answer, through a shim. This proves
# the answer itself: that endOffsets() under read_committed is the last stable offset, that the position
# walks over commit/abort markers and aborted records, that a transaction left open holds the boundary
# below it and its records are captured once it commits, that the time cap is exclusive, that
# LogAppendTime is printed as such, that an offset below the log start FAILS, and that the archive marker
# round-trips through a consumer group. Sections 10-12 then run scripts/es4/strike-archive-interlock.sh — the
# es4 wipe interlock — against this broker through every Kafka CLI given (KAFKA_HOME plus EXTRA_KAFKA_HOMES),
# and with FIXTURE_OUT set record each CLI's raw answers (healthy, absent topic, absent group, a timestamp past
# every record, unreachable broker) as the fixtures the unit suites replay (cli-fixtures/<version>/). It needs
# a broker, so it is NOT part of the Jenkins suite.
#
# It CREATES two uniquely named topics (1 and 3 partitions) and two uniquely named consumer groups, writes only
# to them, reads one uniquely named topic that must NOT exist (and checks it was not created), and deletes the
# rest on exit. It refuses any bootstrap that is not on this machine: never point it at a shared broker.
# Recorded runs (dev Kafka, 2026-09-11): ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-FINAL.md.
#
#   KAFKA_HOME=/path/to/kafka BOOTSTRAP=127.0.0.1:19092 [EXTRA_KAFKA_HOMES="/path/to/kafka-3.9"] \
#   [FIXTURE_OUT=scripts/ops/archive/broker-test/cli-fixtures] bash strike-reader-broker-test.sh
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
  local g t
  for g in "$G" "$T-p3-archive-marker"; do
    "$K/kafka-consumer-groups.sh" --bootstrap-server "$B" --delete --group "$g" 2>&1 | sed 's/^/  cleanup: /'
  done
  for t in "$T" "$T-p3"; do
    "$K/kafka-topics.sh" --bootstrap-server "$B" --delete --topic "$t" 2>&1 | sed 's/^/  cleanup: /'
  done
  sleep 2
  left=$("$K/kafka-topics.sh" --bootstrap-server "$B" --list 2>/dev/null | grep -c "^$T")
  if [ "$left" -ne 0 ]; then echo "  cleanup: $left TEST TOPIC(S) STILL PRESENT"; else echo "  cleanup: every $T* topic deleted"; fi
  left=$("$K/kafka-consumer-groups.sh" --bootstrap-server "$B" --list 2>/dev/null | grep -c "^$T")
  if [ "$left" -ne 0 ]; then echo "  cleanup: $left TEST GROUP(S) STILL PRESENT"; else echo "  cleanup: every $T* group deleted"; fi
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

# ---------------------------------------------------------------------------------------------------------
# 10-12. THE es4 WIPE INTERLOCK AGAINST THE REAL KAFKA CLI (deploy re-review round 2, findings 1 and 3).
# scripts/es4/strike-archive-interlock.sh parses three CLIs. Its unit suite replays their answers from
# scripts/ops/archive/broker-test/cli-fixtures/<version>/, and THIS is where those answers come from: with
# FIXTURE_OUT set, every raw answer (stdout, stderr, exit status) is written there, with the test topic and
# group names replaced by @TOPIC1@ (1 partition), @TOPIC3@ (3 partitions), @ABSENT@, @GROUP1@, @GROUP3@,
# @NOGROUP@. Each CLI in KAFKA_HOME and EXTRA_KAFKA_HOMES (space-separated Kafka homes with bin/*.sh and
# libs/) is run against this broker. es4 runs the interlock inside cp-kafka 7.7.1 (Kafka 3.7.1) through
# kafka-cli-shim/; GetOffsetShell.fetchOffsets is identical in 3.7.1 and 3.9.0, so a 3.9 CLI stands in for it.
# The unreachable-broker answers take about a minute each (the AdminClient's default timeouts), so they run
# in the background.
INTERLOCK="$HERE/../../../es4/strike-archive-interlock.sh"
P3="$T-p3"; G3="$P3-archive-marker"; ABSENT="$T-never-created"; NOGROUP="$T-no-such-group"
FIXTURE_OUT="${FIXTURE_OUT:-}"
"$K/kafka-topics.sh" --bootstrap-server "$B" --create --topic "$P3" --partitions 3 --replication-factor 1 >/dev/null || { echo "create $P3 failed"; exit 1; }
printf 'a\t1\nb\t2\nc\t3\nd\t4\ne\t5\nf\t6\ng\t7\nh\t8\ni\t9\n' | "$K/kafka-console-producer.sh" --bootstrap-server "$B" --topic "$P3" --property parse.key=true >/dev/null 2>&1
P3ENDS=$("$K/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$P3")
echo "   $P3 log ends: $(echo $P3ENDS)"
mark3() { # <partition> <offset>
  java -cp "$L/*" "$READER" --bootstrap "$B" --topic "$P3" --partition "$1" --mark-group "$G3" --mark-offset "$2" \
       --deadline-ms 30000 --summary "$W/m3.sum" 2>>"$W/reader.stderr" || echo "  FAIL could not mark $P3 p$1 at $2"
}
SHORT_P=""; SHORT_E=""
while IFS=: read -r _ p e; do
  [ -n "$p" ] || continue
  mark3 "$p" "$e"
  if [ -z "$SHORT_P" ] && [ "$e" -gt 0 ]; then SHORT_P="$p"; SHORT_E="$e"; fi
done <<< "$P3ENDS"
chk "at least one of the 3 partitions holds records" 1 "$([ -n "$SHORT_P" ] && echo 1 || echo 0)"
FUTURE_MS=$(( $(date +%s) * 1000 + 86400000 ))

template() { # <file> — replace the test names by the placeholders the unit suites substitute back
  sed -e "s/$G3/@GROUP3@/g" -e "s/$NOGROUP/@NOGROUP@/g" -e "s/$G/@GROUP1@/g" -e "s/$ABSENT/@ABSENT@/g" \
      -e "s/$P3/@TOPIC3@/g" -e "s/$T/@TOPIC1@/g" "$1" > "$1.t" && mv "$1.t" "$1"
}
cap() { # <dir> <name> <command...> — one raw CLI answer: .out .err .rc
  local dir="$1" name="$2"; shift 2
  "$@" > "$dir/$name.out" 2> "$dir/$name.err"; echo $? > "$dir/$name.rc"
  template "$dir/$name.out"; template "$dir/$name.err"
}
il() { # <shim dir> <topics> <group> <bootstrap> <out file> — the REAL interlock, exactly as cleanup-es4.sh sources it
  env PATH="$1:$PATH" ES4_STRIKE_ARCHIVE_TOPICS="$2" ES4_STRIKE_ARCHIVE_GROUP="$3" ES4_STRIKE_ARCHIVE_BOOTSTRAP="$4" \
    bash -c '. "$0"; strike_archive_interlock broker-test' "$INTERLOCK" > "$5" 2>&1
}

i=0; BG=""
for H in $KAFKA_HOME ${EXTRA_KAFKA_HOMES:-}; do
  i=$((i + 1))
  ver=$(ls "$H/libs" | sed -n 's/^kafka-clients-\(.*\)\.jar$/\1/p' | head -1)
  S="$W/shim-$i"; mkdir -p "$S"
  for c in kafka-topics kafka-get-offsets kafka-consumer-groups; do
    printf '#!/usr/bin/env bash\nexec "%s/bin/%s.sh" "$@"\n' "$H" "$c" > "$S/$c"; chmod +x "$S/$c"
  done
  echo "== 10.$i the real Kafka $ver CLI ($H): raw answers"
  F="$W/fix-$i"; mkdir -p "$F"
  cap "$F" list        "$H/bin/kafka-topics.sh" --bootstrap-server "$B" --list
  # the broker holds other topics: keep only the test topics' lines (verbatim) so the fixture is the test's
  grep -xE '@TOPIC1@|@TOPIC3@' "$F/list.out" > "$F/list.t"; mv "$F/list.t" "$F/list.out"
  cap "$F" describe-1p "$H/bin/kafka-topics.sh" --bootstrap-server "$B" --describe --topic "$T"
  cap "$F" describe-3p "$H/bin/kafka-topics.sh" --bootstrap-server "$B" --describe --topic "$P3"
  cap "$F" describe-absent "$H/bin/kafka-topics.sh" --bootstrap-server "$B" --describe --topic "$ABSENT"
  cap "$F" ends-1p     "$H/bin/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$T"
  cap "$F" ends-3p     "$H/bin/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$P3"
  cap "$F" ends-absent "$H/bin/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$ABSENT"
  cap "$F" until-nomatch "$H/bin/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$T" --time "$FUTURE_MS"
  cap "$F" until-match-3p "$H/bin/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$P3" --time 0
  cap "$F" groups-3p   "$H/bin/kafka-consumer-groups.sh" --bootstrap-server "$B" --describe --group "$G3"
  cap "$F" groups-absent "$H/bin/kafka-consumer-groups.sh" --bootstrap-server "$B" --describe --group "$NOGROUP"
  for c in list describe-1p describe-3p ends-1p ends-3p until-nomatch; do
    chk "$ver: healthy $c exits 0" 0 "$(cat "$F/$c.rc")"
    chk "$ver: healthy $c writes NOTHING to stderr (the interlock treats stderr diagnostics as a failed read)" 0 "$(wc -c < "$F/$c.err" | tr -d ' ')"
  done
  chk "$ver: the list holds both test topics" "@TOPIC1@ @TOPIC3@" "$(sort "$F/list.out" | tr '\n' ' ' | sed 's/ $//')"
  chk "$ver: get-offsets for an ABSENT topic exits 1 (the reviewer's finding 3)" 1 "$(cat "$F/ends-absent.rc")"
  chk "$ver:   saying so on stderr" 1 "$(grep -c 'Could not match any topic-partitions with the specified filters' "$F/ends-absent.err")"
  chk "$ver: describe of an ABSENT topic exits 1" 1 "$(cat "$F/describe-absent.rc")"
  chk "$ver: a timestamp after every record: NO line for the partition (UNKNOWN_OFFSET is dropped), exit 0" "0 0" \
      "$(cat "$F/until-nomatch.rc") $(grep -c . "$F/until-nomatch.out")"
  chk "$ver: get-offsets lists all 3 partitions" "0 1 2" "$(awk -F: '{print $2}' "$F/ends-3p.out" | sort -n | tr '\n' ' ' | sed 's/ $//')"
  chk "$ver: describe says PartitionCount 3" 1 "$(grep -c 'PartitionCount: 3' "$F/describe-3p.out")"
  # unreachable broker: in the background (each answer is the AdminClient timing out, about a minute)
  ( cap "$F" unreach-list "$H/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:1 --list ) &
  BG="$BG $!"
  ( cap "$F" unreach-describe "$H/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:1 --describe --topic "$T" ) &
  BG="$BG $!"
  ( cap "$F" unreach-ends "$H/bin/kafka-get-offsets.sh" --bootstrap-server 127.0.0.1:1 --topic "$T" ) &
  BG="$BG $!"
  ( cap "$F" unreach-groups "$H/bin/kafka-consumer-groups.sh" --bootstrap-server 127.0.0.1:1 --describe --group "$G3" ) &
  BG="$BG $!"
  ( il "$S" "$T" "$G" 127.0.0.1:1 "$W/il-unreach-$i.out"; echo $? > "$W/il-unreach-$i.rc" ) &
  BG="$BG $!"

  echo "== 11.$i the REAL interlock through the Kafka $ver CLI"
  il "$S" "$P3" "$G3" "$B" "$W/il.out"; rc=$?
  chk "$ver: 3 partitions, each marked at its log end -> the wipe may proceed" 0 "$rc"
  chk "$ver:   every partition was checked" 3 "$(grep -cE "$P3 p[0-2] (archived through|is empty)" "$W/il.out")"
  mark3 "$SHORT_P" $(( SHORT_E - 1 ))
  il "$S" "$P3" "$G3" "$B" "$W/il.out"; rc=$?
  chk "$ver: one partition marked one offset short -> REFUSE" 1 "$rc"
  chk "$ver:   naming that partition" 1 "$(grep -c "$P3 p$SHORT_P: archived through $(( SHORT_E - 1 )), log end $SHORT_E" "$W/il.out")"
  mark3 "$SHORT_P" "$SHORT_E"
  il "$S" "$ABSENT" "$G3" "$B" "$W/il.out"; rc=$?
  chk "$ver: a topic that does not exist -> CONFIRMED absent, the wipe may proceed (finding 3)" 0 "$rc"
  chk "$ver:   from the topic list" 1 "$(grep -c "$ABSENT is not in the broker's topic list (read successfully) — confirmed absent" "$W/il.out")"
  il "$S" "$T" "$G" "$B" "$W/il.out"; rc=$?
  chk "$ver: the 1-partition log, marked at 16 by section 9 -> proceed" 0 "$rc"
  chk "$ver:   archived through 16, log end 16" 1 "$(grep -c "$T p0 archived through 16, log end 16 — covered" "$W/il.out")"
done

echo "== 12. unreachable broker (waiting for the background answers)"
for pid in $BG; do wait "$pid"; done
i=0
for H in $KAFKA_HOME ${EXTRA_KAFKA_HOMES:-}; do
  i=$((i + 1)); F="$W/fix-$i"
  ver=$(ls "$H/libs" | sed -n 's/^kafka-clients-\(.*\)\.jar$/\1/p' | head -1)
  chk "$ver: kafka-topics --list, unreachable: exit 1" 1 "$(cat "$F/unreach-list.rc")"
  chk "$ver: kafka-get-offsets, unreachable: exit 1" 1 "$(cat "$F/unreach-ends.rc")"
  echo "   $ver: kafka-consumer-groups, unreachable: exit $(cat "$F/unreach-groups.rc"), stdout: $(grep -m1 . "$F/unreach-groups.out")"
  chk "$ver: the REAL interlock, unreachable broker -> REFUSE" 1 "$(cat "$W/il-unreach-$i.rc")"
  chk "$ver:   because the topic list cannot be read" 1 "$(grep -c "the broker's topic list cannot be read" "$W/il-unreach-$i.out")"
  if [ -n "$FIXTURE_OUT" ]; then
    rm -rf "${FIXTURE_OUT:?}/$ver"; mkdir -p "$FIXTURE_OUT/$ver"
    cp "$F"/*.out "$F"/*.err "$F"/*.rc "$FIXTURE_OUT/$ver/"
    printf 'Kafka CLI %s (%s), against a Kafka 4.3.0 broker, captured %s by strike-reader-broker-test.sh\n' \
      "$ver" "$(basename "$H")" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$FIXTURE_OUT/$ver/VERSION"
    echo "   fixtures written: $FIXTURE_OUT/$ver ($(ls "$FIXTURE_OUT/$ver" | wc -l | tr -d ' ') files)"
  fi
done
chk "the absent topic was NOT created by any of the reads" 0 "$("$K/kafka-topics.sh" --bootstrap-server "$B" --list 2>/dev/null | grep -cx "$ABSENT")"

echo "RESULT: fails=$fails"
exit "$fails"
