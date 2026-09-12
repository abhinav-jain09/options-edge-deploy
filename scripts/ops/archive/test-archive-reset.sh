#!/usr/bin/env bash
# test-archive-reset.sh — asserts the LOG-RESET contract that the 2026-08-11 es4 data loss came
# from, and that by 2026-08-13 had turned the whole es4 archive into a permanent silent no-op.
#
# THE BUG: `from` (the checkpoint) AHEAD of `endoff` (the log end) was unhandled. A clean-reset
# re-creates the topics and every partition restarts at 0, so the stored offsets point into a log
# that no longer exists; count=(endoff-from) went negative, the `-gt 0` guard skipped the
# partition, and the run reported failed=0. Every es4 partition was in that state.
#
# WHY A SHIM AND NOT A REAL BROKER: the cases that matter are a topic re-created with a NEW
# TopicId, a partition that is EMPTY after the reset, and time-bounded mode. None of those can be
# arranged on a live broker without writing to it, and a suite that waits for whatever a real topic
# happens to contain can pass while guarding nothing. The shim below stands in for the three Kafka
# CLIs the archiver calls, so every partition count, offset, id and record count is exact. Section 12
# adds a stand-in for StrikeArchiveReader.java (the committed-read capture) through the archiver's
# STRIKE_READER seam; the Java program itself is exercised against a real broker, not here. Section 17
# routes the vol-premium ledgers (Kafka Streams exactly-once) through the same seam, in the four-field layout.
set -uo pipefail
OE="$(cd "$(dirname "$0")" && pwd)"
ARCH="$OE/oe-archive-kafka.sh"
# Bind a FREE port rather than a fixed one: a leftover server from an earlier run keeps the
# socket and every alert assertion then measures nothing.
PORT="${PORT:-$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")}"
TOPIC=oe.test.reset

# Alert delivery goes through curl (oe-alert.sh). Without it every alert assertion below would
# report zero deliveries and read as "no alert was sent" — a vacuous pass in the one dimension this
# suite exists to prove. Refuse to run instead.
for tool in curl flock python3 gzip sha256sum timeout awk; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' is required — refusing to run a suite that would pass vacuously without it"; exit 1; }
done
# REAL Kafka CLI answers, recorded against a real broker by broker-test/strike-reader-broker-test.sh. The shim
# below replays them for the time-bounded lookup (section 12q): a failed lookup and a lookup that found no
# record look the same on stdout — nothing — and only the recorded exit status and stderr tell them apart.
export OE_CLI="$OE/broker-test/cli-fixtures/4.3.0"
export OE_SKIP="$OE/broker-test/cli-fixtures/constructed/skip-diagnostic.err"
for f in "$OE_CLI/unreach-ends.err" "$OE_CLI/unreach-ends.rc" "$OE_CLI/until-nomatch.rc" "$OE_SKIP" \
         "$OE_CLI/unreach-list.out" "$OE_CLI/unreach-list.rc" "$OE_CLI/unreach-describe.out" "$OE_CLI/unreach-describe.rc" \
         "$OE_CLI/describe-absent.out" "$OE_CLI/describe-absent.rc" "$OE_CLI/describe-1p.out" "$OE_CLI/list.out" \
         "$OE_CLI/ends-absent.err" "$OE_CLI/ends-absent.rc"; do
  [ -f "$f" ] || { echo "FATAL: recorded CLI answer $f missing — refusing to run section 12q against invented output"; exit 1; }
done

T=$(mktemp -d)
BIN="$T/bin"; mkdir -p "$BIN"
export OE_FIXTURE="$T/fixture"     # the shim reads the broker's "state" from here

# ---- mock Discord, so alerting is asserted by real delivery and counted ------------------------
python3 - "$PORT" "$T/alerts.txt" <<'PY' &
import sys, http.server
out = sys.argv[2]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        open(out, 'ab').write(self.rfile.read(n) + b'\n')
        self.send_response(204); self.send_header('Content-Length','0'); self.end_headers()
    def log_message(self, *a): pass
http.server.HTTPServer(('127.0.0.1', int(sys.argv[1])), H).serve_forever()
PY
SRV=$!
cleanup() { kill $SRV 2>/dev/null; rm -rf "$T"; }
trap cleanup EXIT
ready=false
for _ in $(seq 1 40); do (echo > /dev/tcp/127.0.0.1/$PORT) 2>/dev/null && { ready=true; break; }; sleep 0.25; done
[ "$ready" = true ] || { echo "FATAL: mock endpoint never came up — a dead server makes the alert cases pass vacuously"; exit 1; }
export DISCORD_WEBHOOK_URL="http://127.0.0.1:$PORT/204"

# ---- the shim ---------------------------------------------------------------------------------
# fixture format, one line per partition:  <part> <earliest> <end> [<until_off>]
# plus a line:                             topicid <uuid>
cat > "$BIN/kafka-get-offsets.sh" <<'SH'
#!/usr/bin/env bash
topic=""; time_arg=""
while [ $# -gt 0 ]; do
  case "$1" in
    --topic) topic="$2"; shift 2 ;;
    --time)  time_arg="$2"; shift 2 ;;
    *) shift ;;
  esac
done
echo "get-offsets ${time_arg:-latest} $topic" >> "$OE_FIXTURE.offsets-calls"
replay() { cat "$OE_CLI/$1.out"; cat "$OE_CLI/$1.err" >&2; exit "$(cat "$OE_CLI/$1.rc")"; }
# Section 16 (re-review round 3): OE_BROKER=down replays the RECORDED unreachable answer to every query;
# OE_ENDS_QUERY / OE_EARLIEST_QUERY = fail (recorded unreachable) | skip (GetOffsetShell's per-partition
# "Skip getting offsets", exit 0) | missing (the partition silently absent, exit 0), for partition OE_SKIP_PART.
[ "${OE_BROKER:-up}" = down ] && replay unreach-ends
case "$time_arg" in ""|latest) q="${OE_ENDS_QUERY:-ok}" ;; earliest) q="${OE_EARLIEST_QUERY:-ok}" ;; *) q=ok ;; esac
[ "$q" = fail ] && replay unreach-ends
# A topic the broker does not have: the RECORDED answer (exit 1, "Could not match any topic-partitions").
grep -qE '^[0-9]+ ' "$OE_FIXTURE" || replay ends-absent
case "$time_arg:${OE_TIME_QUERY:-ok}" in
  earliest:*|:*|latest:*|*:ok|*:skip) : ;;
  # the RECORDED answer of kafka-get-offsets 4.3.0 against an unreachable broker: nothing on stdout, exit 1
  *:fail) cat "$OE_CLI/unreach-ends.err" >&2; exit "$(cat "$OE_CLI/unreach-ends.rc")" ;;
esac
while read -r a b c d; do
  [ "$a" = "topicid" ] && continue
  [ -n "$a" ] || continue
  if [ "$a" = "${OE_SKIP_PART:-0}" ]; then
    case "$q" in
      skip) sed "s/@TOPIC3@-1/$topic-$a/" "$OE_SKIP" >&2; continue ;;
      missing) continue ;;
    esac
  fi
  case "$time_arg" in
    earliest) echo "$topic:$a:$b" ;;
    ""|latest) echo "$topic:$a:$c" ;;
    *) # GetOffsetShell's per-partition failure: p0 omitted, "Skip getting offsets ..." on stderr, exit 0
       if [ "${OE_TIME_QUERY:-ok}" = skip ] && [ "$a" = 0 ]; then sed "s/@TOPIC3@-1/$topic-0/" "$OE_SKIP" >&2; continue; fi
       [ -n "${d:-}" ] && echo "$topic:$a:$d" ;;   # no line, exit 0 = no record that new (recorded: until-nomatch)
  esac
done < "$OE_FIXTURE"
# Exit 0 explicitly, as the real CLI does (recorded: until-nomatch.rc). Without this line the shim's status is
# its last test's, i.e. 1 whenever the last partition has no record after the cutoff — an answer the real CLI
# never gives, which the archiver (rightly) refuses since it validates the lookup's status.
exit 0
SH
cat > "$BIN/kafka-topics.sh" <<'SH'
#!/usr/bin/env bash
id=$(awk '$1=="topicid"{print $2}' "$OE_FIXTURE")
[ -n "$id" ] && echo "Topic: oe.test.reset	TopicId: $id	PartitionCount: 2	ReplicationFactor: 1"
SH
cat > "$BIN/kafka-console-consumer.sh" <<'SH'
#!/usr/bin/env bash
# Emits exactly the records that exist in [offset, end) for the partition, capped by
# --max-messages, in the archiver's expected "CreateTime:<ms>\t..." shape.
part=""; off=0; maxm=0
while [ $# -gt 0 ]; do
  case "$1" in
    --partition) part="$2"; shift 2 ;;
    --offset) off="$2"; shift 2 ;;
    --max-messages) maxm="$2"; shift 2 ;;
    *) shift ;;
  esac
done
end=$(awk -v p="$part" '$1==p{print $3}' "$OE_FIXTURE")
avail=$(( end - off )); [ "$avail" -lt 0 ] && avail=0
[ "$avail" -gt "$maxm" ] && avail="$maxm"
i=0
while [ "$i" -lt "$avail" ]; do
  echo -e "CreateTime:1786000000000\t$part\tk$i\t{\"schemaVersion\":1}"
  i=$(( i + 1 ))
done
SH
chmod +x "$BIN"/*.sh

FAILED=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; FAILED=$(( FAILED + 1 )); }
want() { # $1=label $2=expected $3=actual
  [ "$2" = "$3" ] && pass "$1" || fail "$(printf '%s (want %s, got %s)' "$1" "$2" "$3")"; }
has()  { printf '%s' "$3" | grep -Fq "$2" && pass "$1" || { fail "$1 (missing: $2)"; printf '%s\n' "$3" | sed 's/^/      | /' | tail -20; }; }
hasnt(){ printf '%s' "$3" | grep -Fq "$2" && { fail "$1 (unexpected: $2)"; printf '%s\n' "$3" | sed 's/^/      | /' | tail -20; } || pass "$1"; }

fixture() { printf '%s\n' "$@" > "$OE_FIXTURE"; }
run() { # $@ = extra env assignments
  env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-reset BOOTSTRAP=shim:9092 \
      KAFKA_BIN="$BIN" TOPICS="$TOPIC" ALLOW_NON_NAS=true "$@" "$ARCH" 2>&1
}
ck()  { awk -v p="$1" '{split($1,a,"="); if (a[1]==p) print a[2]}' "$A/kafka/prod/_manifest/$TOPIC.offsets" 2>/dev/null | tail -1; }
runs(){ awk -v k="$1" '{for(i=1;i<=NF;i++){split($i,x,"="); if (x[1]==k) v=x[2]}} END{print v}' "$A/kafka/prod/_manifest/runs.log" 2>/dev/null; }
alerts(){ local n; n=$(grep -c "log RESET on" "$T/alerts.txt" 2>/dev/null); echo "${n:-0}"; }
reset_alerts(){ : > "$T/alerts.txt"; }

A="$T/a"; mkdir -p "$A/kafka/prod/_manifest"
reset_alerts

# ================= 1. baseline session, then the topic is WIPED and re-created =================
# p0 has records, p1 is EMPTY after the reset — the case that broke my first fix.
fixture "topicid AAAAAAAAAAAAAAAAAAAAAA" "0 0 500" "1 0 300"
OUT=$(run); RC=$?
want "first run archives cleanly (rc)"            0 "$RC"
want "  p0 checkpoint after run 1"              500 "$(ck 0)"
want "  p1 checkpoint after run 1"              300 "$(ck 1)"
want "  no reset on a first run"                  0 "$(runs rebaselined)"

# the wipe: new TopicId, offsets restart at 0, p1 receives nothing at all
fixture "topicid BBBBBBBBBBBBBBBBBBBBBB" "0 0 120" "1 0 0"
OUT=$(run); RC=$?
want "reset run succeeds (rc)"                    0 "$RC"
has  "reset is attributed to the TopicId"  "TopicId changed" "$OUT"
want "  BOTH partitions re-baselined"             2 "$(runs rebaselined)"
want "  p0 re-read from 0 and checkpointed"     120 "$(ck 0)"
want "  EMPTY p1 checkpoint corrected to 0"       0 "$(ck 1)"
want "  records archived = the whole new log"   120 "$(runs records)"
want "  exactly one alert delivered"              1 "$(alerts)"
reset_alerts
hasnt "a recovered run is not a failure"  "failed=1" "$OUT"

# ================= 2. the latch CLEARS — the next run is incremental again =====================
fixture "topicid BBBBBBBBBBBBBBBBBBBBBB" "0 0 170" "1 0 0"
OUT=$(run)
hasnt "second run does not re-detect a reset" "RESET" "$OUT"
want "  second run rebaselined=0"                 0 "$(runs rebaselined)"
want "  only the 50 new records are read"        50 "$(runs records)"
want "  and no NEW alert on the clean run"        0 "$(alerts)"

# ================= 3. a re-created topic that GREW PAST the old checkpoint =====================
# The offset test alone cannot see this one; only the TopicId can. This is the case that would
# have hidden a wipe on es4 after two busy sessions.
B="$A"; A="$T/c"; mkdir -p "$A/kafka/prod/_manifest"
reset_alerts
fixture "topicid CCCCCCCCCCCCCCCCCCCCCC" "0 0 100" "1 0 100"
run >/dev/null
fixture "topicid DDDDDDDDDDDDDDDDDDDDDD" "0 0 900" "1 0 900"
OUT=$(run)
has  "a longer re-created log is STILL caught" "TopicId changed" "$OUT"
want "  both partitions re-baselined"             2 "$(runs rebaselined)"
want "  the whole new log is archived"         1800 "$(runs records)"
want "  and it alerted once"                      1 "$(alerts)"
reset_alerts

# ================= 4. UNTIL_TS must not look like a reset =====================================
# Time-bounded mode pulls endoff back to a past instant, so a healthy checkpoint sits above it.
# Reading that as a reset would re-archive the whole log into the wrong dt= folder.
A="$T/d"; mkdir -p "$A/kafka/prod/_manifest"
fixture "topicid EEEEEEEEEEEEEEEEEEEEEE" "0 0 500" "1 0 500"
run >/dev/null
want "  checkpoint is at the log end"           500 "$(ck 0)"
fixture "topicid EEEEEEEEEEEEEEEEEEEEEE" "0 0 800 200" "1 0 800 200"
OUT=$(run UNTIL_TS=1786000000000)
hasnt "bounded mode is NOT a reset"          "RESET" "$OUT"
want "  bounded run rebaselined=0"                0 "$(runs rebaselined)"
want "  no alert from bounded mode"               0 "$(alerts)"

# ================= 5. expired checkpoint (GAP) still works ====================================
# Regression guard: the reset branch sits in the same if-chain and must not swallow this.
A="$T/e"; mkdir -p "$A/kafka/prod/_manifest"
fixture "topicid FFFFFFFFFFFFFFFFFFFFFF" "0 0 500" "1 0 500"
run >/dev/null
fixture "topicid FFFFFFFFFFFFFFFFFFFFFF" "0 900 1000" "1 900 1000"
OUT=$(run)
has  "expired checkpoint still reports GAP"    "GAP" "$OUT"
hasnt "  and is not mislabelled a reset"     "RESET" "$OUT"

# ================= 6. the alert helper is missing =============================================
# The archiver must still archive, and must say the alert was not delivered.
A="$T/f"; mkdir -p "$A/kafka/prod/_manifest"
SOLO="$T/solo"; mkdir -p "$SOLO"; cp "$ARCH" "$OE/oe-topics.env" "$SOLO/"   # but NO oe-alert.sh
reset_alerts
fixture "topicid GGGGGGGGGGGGGGGGGGGGGG" "0 0 500" "1 0 500"
env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-reset BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" \
    TOPICS="$TOPIC" ALLOW_NON_NAS=true "$SOLO/$(basename "$ARCH")" >/dev/null 2>&1
fixture "topicid HHHHHHHHHHHHHHHHHHHHHH" "0 0 60" "1 0 60"
OUT=$(env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-reset BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" \
      TOPICS="$TOPIC" ALLOW_NON_NAS=true "$SOLO/$(basename "$ARCH")" 2>&1); RC=$?
want "runs without the alert helper (rc)"         0 "$RC"
has  "  warns that alerts are log-only"  "LOGGED ONLY" "$OUT"
has  "  still detects the reset"      "TopicId changed" "$OUT"
has  "  logs the undelivered alert"        "ALERT (undelivered" "$OUT"
want "  and archived the new log"               120 "$(runs records)"
want "  and delivered NOTHING to the endpoint"    0 "$(alerts)"

# ===== 6b. TopicId PRESENT and UNCHANGED but the end offset reads 0 — a BAD READ, not a reset ===
# The prod incident this exists for (2026-08-14):
#   "RESET options.databento.gex.strike p0: checkpoint 1882516 is AHEAD of log end 0"
# Log end ZERO on a topic holding 58 million records — an empty/failed kafka-get-offsets answer.
# The archiver re-baselined on it and threw the checkpoint away, and the day's session was lost.
# Across twelve archived days only two ended up with a full session.
#
# The TopicId is the AUTHORITY on re-creation and the offset test is a heuristic, so when the id
# is readable and unchanged the heuristic must not be allowed to fire.
A="$T/f2"; mkdir -p "$A/kafka/prod/_manifest"
reset_alerts
fixture "topicid JJJJJJJJJJJJJJJJJJJJJJ" "0 0 500" "1 0 500"
run >/dev/null
CKPT_BEFORE=$(cat "$A/kafka/prod/_manifest/$TOPIC.offsets" 2>/dev/null)
fixture "topicid JJJJJJJJJJJJJJJJJJJJJJ" "0 0 0" "1 0 0"        # the bad reading
OUT=$(run)
has  "a zero end with an UNCHANGED TopicId is a failed read" "FAILED offset read" "$OUT"
hasnt "  and is NOT reported as a reset"              "AHEAD of log end" "$OUT"
want "  nothing re-baselined"                     0 "$(runs rebaselined)"
want "  no alert raised"                          0 "$(alerts)"
want "  the checkpoint is left ALONE" "$CKPT_BEFORE" \
     "$(cat "$A/kafka/prod/_manifest/$TOPIC.offsets" 2>/dev/null)"

# ================= 7. TopicId UNAVAILABLE — the offset detector must still work ================
# Older broker, or a kafka-topics.sh that fails. The identity file is then never written and the
# fallback is all there is; without this case the fallback could rot untested behind the id path.
A="$T/g"; mkdir -p "$A/kafka/prod/_manifest"
reset_alerts
cat > "$BIN/kafka-topics.sh" <<'SH'
#!/usr/bin/env bash
exit 1
SH
chmod +x "$BIN/kafka-topics.sh"
fixture "topicid IIIIIIIIIIIIIIIIIIIIII" "0 0 500" "1 0 500"
run >/dev/null
want "  no identity file when the id is unavailable" "absent" \
     "$([ -f "$A/kafka/prod/_manifest/$TOPIC.identity" ] && echo present || echo absent)"
fixture "topicid IIIIIIIIIIIIIIIIIIIIII" "0 0 60" "1 0 60"
OUT=$(run)
has  "offset fallback still detects the reset" "AHEAD of log end" "$OUT"
has  "  and names the offset detector"    "detected by: offset-ahead" "$OUT"
want "  both partitions re-baselined"             2 "$(runs rebaselined)"
want "  one alert"                                1 "$(alerts)"
# restore the working shim
cat > "$BIN/kafka-topics.sh" <<'SH'
#!/usr/bin/env bash
id=$(awk '$1=="topicid"{print $2}' "$OE_FIXTURE")
[ -n "$id" ] && echo "Topic: oe.test.reset	TopicId: $id	PartitionCount: 2	ReplicationFactor: 1"
SH
chmod +x "$BIN/kafka-topics.sh"

# ================= 8. a helper that is READABLE but defines no alert() ========================
# The exact original defect: the file-existence check passed, so no fallback was installed and
# `alert` was an unbound command at the one moment it mattered.
A="$T/h"; mkdir -p "$A/kafka/prod/_manifest"
reset_alerts
DUD="$T/dud"; mkdir -p "$DUD"; cp "$ARCH" "$OE/oe-topics.env" "$DUD/"
echo '# a helper that defines nothing at all' > "$DUD/oe-alert.sh"
dudrun() { env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-reset BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" \
           TOPICS="$TOPIC" ALLOW_NON_NAS=true "$DUD/$(basename "$ARCH")" 2>&1; }
fixture "topicid JJJJJJJJJJJJJJJJJJJJJJ" "0 0 500" "1 0 500"
dudrun >/dev/null
fixture "topicid KKKKKKKKKKKKKKKKKKKKKK" "0 0 60" "1 0 60"
OUT=$(dudrun); RC=$?
want "readable-but-empty helper: run still succeeds" 0 "$RC"
has  "  installs the log-only fallback"  "LOGGED ONLY" "$OUT"
has  "  and logs the undelivered alert"  "ALERT (undelivered" "$OUT"
want "  and archived the new log"               120 "$(runs records)"
want "  delivered nothing"                        0 "$(alerts)"

# ================= 9. interrupted between identity detection and recovery =====================
# The identity must be recorded only AFTER every partition is processed, so a run killed in the
# middle leaves the detector ARMED. Written up front, the next run would compare id==id, skip the
# reset, and resume mid-log — the silent prefix loss this whole change exists to prevent.
A="$T/i"; mkdir -p "$A/kafka/prod/_manifest"
reset_alerts
fixture "topicid LLLLLLLLLLLLLLLLLLLLLL" "0 0 500" "1 0 500"
run >/dev/null
prev_id_file="$A/kafka/prod/_manifest/$TOPIC.identity"
want "  identity recorded after a clean run" "topic_id=LLLLLLLLLLLLLLLLLLLLLL" \
     "$(head -1 "$prev_id_file" 2>/dev/null)"
# the wipe, then a run killed while the consumer is working
fixture "topicid MMMMMMMMMMMMMMMMMMMMMM" "0 0 900" "1 0 900"
cat > "$BIN/kafka-console-consumer.sh" <<'SH'
#!/usr/bin/env bash
echo $$ > "$OE_FIXTURE.consumerpid"
# exec, so the recorded pid IS the process holding the inherited lock fd. Without it `sleep` is a
# child that survives the kill and keeps the lock, and the next run exits clean on a busy lock.
exec sleep 30
SH
chmod +x "$BIN/kafka-console-consumer.sh"
runs_before=$(wc -l < "$A/kafka/prod/_manifest/runs.log" 2>/dev/null || echo 0)
rm -f "$OE_FIXTURE.consumerpid"
timeout 3 env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-reset BOOTSTRAP=shim:9092 \
  KAFKA_BIN="$BIN" TOPICS="$TOPIC" ALLOW_NON_NAS=true "$ARCH" >/dev/null 2>&1
KILLRC=$?
# Prove the interruption happened WHERE it was meant to. Without these, a failure before the
# consumer was ever reached would also leave runs.log and the identity untouched, and the whole
# section would pass having tested nothing.
want "  the run was killed by the timeout, not by an early error" 124 "$KILLRC"
cpid=$(cat "$OE_FIXTURE.consumerpid" 2>/dev/null)
[ -n "${cpid:-}" ] && pass "  the consumer was actually reached (pid $cpid)" \
  || fail "  the consumer was never reached — the interruption did not happen mid-loop"
# The interruption is only meaningful if the run really did NOT finish: a completed run appends a
# line to runs.log, so compare the count rather than asserting against a file that never exists.
runs_after=$(wc -l < "$A/kafka/prod/_manifest/runs.log" 2>/dev/null || echo 0)
want "  the interrupted run wrote no run record" "$runs_before" "$runs_after"
# `timeout` signals only its direct child, so the shim's `sleep` survives holding the INHERITED
# lock fd; the next run would then exit clean on a busy lock and measure nothing. A real crash
# takes the process tree with it. Kill the exact pid the shim recorded — `pkill -f` is both absent
# from a slim container and far too broad on a shared machine.
[ -n "${cpid:-}" ] && kill -9 "$cpid" 2>/dev/null
for _ in $(seq 1 40); do
  [ -n "${cpid:-}" ] && kill -0 "$cpid" 2>/dev/null || break
  sleep 0.25
done
want "  identity NOT advanced by the killed run" "topic_id=LLLLLLLLLLLLLLLLLLLLLL" \
     "$(head -1 "$prev_id_file" 2>/dev/null)"
# restore the consumer and let the next run recover
cat > "$BIN/kafka-console-consumer.sh" <<'SH'
#!/usr/bin/env bash
part=""; off=0; maxm=0
while [ $# -gt 0 ]; do
  case "$1" in
    --partition) part="$2"; shift 2 ;;
    --offset) off="$2"; shift 2 ;;
    --max-messages) maxm="$2"; shift 2 ;;
    *) shift ;;
  esac
done
end=$(awk -v p="$part" '$1==p{print $3}' "$OE_FIXTURE")
avail=$(( end - off )); [ "$avail" -lt 0 ] && avail=0
[ "$avail" -gt "$maxm" ] && avail="$maxm"
i=0
while [ "$i" -lt "$avail" ]; do
  echo -e "CreateTime:1786000000000\t$part\tk$i\t{\"schemaVersion\":1}"
  i=$(( i + 1 ))
done
SH
chmod +x "$BIN/kafka-console-consumer.sh"
OUT=$(run)
has  "the NEXT run still sees the reset" "TopicId changed" "$OUT"
want "  and recovers the whole new log"        1800 "$(runs records)"
want "  identity advanced only now"  "topic_id=MMMMMMMMMMMMMMMMMMMMMM" \
     "$(head -1 "$prev_id_file" 2>/dev/null)"

# ================= 10. the delivered payload says what actually happened ======================
payload=$(cat "$T/alerts.txt" 2>/dev/null)
has  "payload names the environment/job"  "archive prod/test-reset" "$payload"
has  "payload names the detector"          "detected by:" "$payload"
has  "payload scopes the loss honestly"    "after its last successful checkpoint" "$payload"
hasnt "payload does not claim the checkpoint was ahead when it was not" \
      "The checkpoint was ahead" "$payload"

# ================= 11. the es4 selection path is the policy file, end to end ==================
# 2026-08-15: es.futures.cvd.bars shipped through three Codex-gated release gates while the es4
# archive cron carried a pasted topic list that predated it — the set the cron ACTUALLY used was
# defined nowhere reviewable. These cases pin the whole chain: the env file defines OE_ES4_TOPICS,
# the archiver derives exactly that set for ENV=es4, refuses to run without it, and the deployed
# crontab passes no override.
es4_expected=$(. "$OE/oe-topics.env"; printf '%s' "$OE_ES4_TOPICS")
want "oe-topics.env defines a non-empty es4 set" 0 "$([ -n "$es4_expected" ]; echo $?)"
has  "  the es4 set carries the CVD history topic" "es.futures.cvd.bars" "$es4_expected"
es4_derived=$(ARCHIVE_DIR="$T" ENV=es4 PRINT_TOPICS=true KAFKA_BIN="$BIN" bash "$ARCH" 2>/dev/null)
want "ENV=es4 derives exactly OE_ES4_TOPICS" "$es4_expected" "$es4_derived"
# Pinned safety invariant (2026-08-15): es.futures.cvd — the 1 Hz snapshot — STRUCTURALLY starves
# the bounded consumer (compaction makes --max-messages unreachable; a 1 Hz producer means the 60s
# idle exit never fires) and fails EVERY run until the 900s kill. Reintroducing it recreates a
# permanent daily false alarm. Token match, not substring: es.futures.cvd.bars CONTAINS the
# forbidden name and must keep passing.
hasnt "the always-hot snapshot topic es.futures.cvd must NEVER rejoin the es4 archive set" \
      " es.futures.cvd " " $es4_expected "

stripped="$T/topics-no-es4.env"
grep -v '^OE_ES4_TOPICS=' "$OE/oe-topics.env" > "$stripped"
missing_out=$(ARCHIVE_DIR="$T" ENV=es4 PRINT_TOPICS=true KAFKA_BIN="$BIN" OE_TOPICS_ENV="$stripped" bash "$ARCH" 2>&1)
missing_rc=$?
want "a policy file without OE_ES4_TOPICS is refused" 1 "$([ "$missing_rc" -ne 0 ]; echo $((1-$?)))"
has  "  and says why" "OE_ES4_TOPICS" "$missing_out"

crontab_file="$OE/oe-archive.crontab"
want "the repo-managed crontab exists" 0 "$([ -f "$crontab_file" ]; echo $?)"
es4_line=$(grep -c '^1 17 \* \* 1-5 .*ENV=es4 .*oe-archive-kafka\.sh' "$crontab_file")
want "exactly one scheduled es4 archiver entry" 1 "$es4_line"
es4_entry=$(grep '^1 17 \* \* 1-5 .*ENV=es4' "$crontab_file")
hasnt "the es4 entry carries NO TOPICS override (the policy file is the one definition)" \
      "TOPICS=" "$es4_entry"

# ================= 12. COMMITTED-READ capture: es.futures.footprint.strike ======================
# deploy Codex final review, findings 1 and 6. The strike log is transactional. Read committed-only, the
# console consumer can stop below an unresolved transaction and still exit 0, and the archiver then
# checkpointed the high-water mark it queried first — "endoff=1200, got=1099 -> ACCEPT checkpoint=1200",
# skipping every record that committed afterwards. OE_COMMITTED_READ_TOPICS now routes it to
# StrikeArchiveReader.java, which takes its boundary from Kafka metadata. The Java program itself is run
# against a REAL broker outside this suite (the PR record); here a shim with the reader's exact contract
# stands in for it — boundary = the first offset of an OPEN transaction (the LSO) or the log end, capped by
# --max-end; committed records below it are written; every other kind (aborted, open, marker) is not — so
# every case is about what the ARCHIVER does with a reader's answer.
for tool in zcat stat; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' is required by sections 12-14"; exit 1; }
done
STRIKE=es.futures.footprint.strike
SDAY=2026-09-09
CALLS="$OE_FIXTURE.calls"; MARKS="$OE_FIXTURE.marks"
cat > "$BIN/strike-reader" <<'SH'
#!/usr/bin/env bash
# StrikeArchiveReader.java stand-in. Log: "$OE_FIXTURE.strike", one line per offset: <offset> <C|A|O|M> [key value]
# C committed record, A aborted record, O record of a transaction still OPEN, M commit/abort marker.
topic=""; part=""; from=""; maxend=""; out=""; sum=""; mgroup=""; moff=""; dl=""
while [ $# -gt 1 ]; do
  case "$1" in
    --topic) topic="$2" ;; --partition) part="$2" ;; --from) from="$2" ;; --max-end) maxend="$2" ;;
    --out) out="$2" ;; --summary) sum="$2" ;; --mark-group) mgroup="$2" ;; --mark-offset) moff="$2" ;;
    --deadline-ms) dl="$2" ;;
  esac
  shift 2
done
echo "reader $topic p$part from=${from:-} max_end=${maxend:-} mark=${moff:-} group=${mgroup:-} deadline_ms=${dl:-}" >> "$OE_FIXTURE.calls"
# Section 16h: the topic is DELETED and RE-CREATED while this capture runs (a new TopicId from now on).
if [ -z "$mgroup" ] && [ -n "${STRIKE_SHIM_RECREATE:-}" ]; then
  sed "s/^topicid .*/topicid $STRIKE_SHIM_RECREATE/" "$OE_FIXTURE" > "$OE_FIXTURE.new" && mv "$OE_FIXTURE.new" "$OE_FIXTURE"
fi
if [ -n "$mgroup" ]; then
  if [ "${STRIKE_SHIM_MARK:-ok}" = fail ]; then
    echo "STRIKE_ARCHIVE_READER status=FAILED topic=$topic partition=$part reason=broker_down" > "$sum"; exit 2
  fi
  echo "$moff" >> "$OE_FIXTURE.marks"
  echo "STRIKE_ARCHIVE_READER status=COMPLETE topic=$topic partition=$part from=-1 lso=-1 boundary=$moff position=$moff records=0 escaped=0 elapsed_ms=1" > "$sum"
  exit 0
fi
end=$(awk 'NF { e = $1 + 1 } END { print e + 0 }' "$OE_FIXTURE.strike")
lso=$(awk '$2 == "O" { print $1; exit }' "$OE_FIXTURE.strike"); lso="${lso:-$end}"
boundary="$lso"
[ -n "$maxend" ] && [ "$maxend" -lt "$boundary" ] && boundary="$maxend"
awk -v f="$from" -v b="$boundary" -v p="$part" \
  '$2 == "C" && $1 >= f && $1 < b { printf "CreateTime:1786000000000\tPartition:%s\tOffset:%s\t%s\t%s\n", p, $1, $3, $4 }' \
  "$OE_FIXTURE.strike" > "$out"
n=$(wc -l < "$out" | tr -d ' ')
status=COMPLETE; pos="$boundary"; rc=0; rec="$n"; esc=0
[ "$boundary" -lt "$from" ] && pos="$from"
case "${STRIKE_SHIM_MODE:-}" in
  timeout)    status=TIMEOUT; pos=$(( from + 1 )); rc=3 ;;
  lie-short)  pos=$(( boundary - 1 )) ;;                 # says COMPLETE, exits 0, did NOT reach the boundary
  lie-count)  rec=$(( n + 1 )) ;;                        # says it wrote one record more than the file holds
  no-summary) exit 0 ;;                                  # exits 0 and states nothing
  # Section 17: a reader whose FILE does not hold what its summary names — each exits 0, COMPLETE, counts consistent.
  escaped)    esc=1 ;;                                   # it had to escape a TAB/CR/LF in one record
  stray)      printf 'CreateTime:1786000000000\tPartition:%s\tOffset:%s\tstray\t{"stray":1}\n' "$part" "$boundary" >> "$out"
              rec=$(( n + 1 )) ;;                        # a record AT the exclusive boundary, i.e. outside the range
  unordered)  awk '{ l[NR] = $0 } END { for (i = NR; i >= 1; i--) print l[i] }' "$out" > "$out.rev" && cat "$out.rev" > "$out"
              rm -f "$out.rev" ;;                        # the right records, in descending offset order
esac
echo "STRIKE_ARCHIVE_READER status=$status topic=$topic partition=$part from=$from lso=$lso boundary=$boundary position=$pos records=$rec escaped=$esc elapsed_ms=1" > "$sum"
exit "$rc"
SH
# The console consumer again, now also RECORDING each call, so a case can prove which path read a topic.
cat > "$BIN/kafka-console-consumer.sh" <<'SH'
#!/usr/bin/env bash
part=""; off=0; maxm=0; topic=""; props=""
while [ $# -gt 0 ]; do
  case "$1" in
    --partition) part="$2"; shift 2 ;;
    --offset) off="$2"; shift 2 ;;
    --max-messages) maxm="$2"; shift 2 ;;
    --topic) topic="$2"; shift 2 ;;
    --consumer-property) props="$props $2"; shift 2 ;;
    --isolation-level) props="$props isolation-level=$2"; shift 2 ;;
    *) shift ;;
  esac
done
echo "console $topic p$part props=[${props# }]" >> "$OE_FIXTURE.calls"
end=$(awk -v p="$part" '$1==p{print $3}' "$OE_FIXTURE")
avail=$(( end - off )); [ "$avail" -lt 0 ] && avail=0
[ "$avail" -gt "$maxm" ] && avail="$maxm"
i=0
while [ "$i" -lt "$avail" ]; do
  echo -e "CreateTime:1786000000000\t$part\tk$i\t{\"schemaVersion\":1}"
  i=$(( i + 1 ))
done
SH
# kafka-topics in the RECORDED 4.3.0 shapes (cli-fixtures/4.3.0: list, describe-1p, describe-absent, unreach-*), now
# that the committed-read discovery asks it for the topic list and a full description (re-review round 3). The topic
# exists iff the fixture has a "topicid" line, with one partition per fixture partition line. OE_BROKER=down replays
# the recorded unreachable answers; OE_DESCRIBE = fail (recorded unreachable) | noid (a description without a
# TopicId) | zero (the all-zero id) breaks only --describe. The console path reads only "TopicId:" from it, as before.
cat > "$BIN/kafka-topics.sh" <<'SH'
#!/usr/bin/env bash
topic=""; op=""
while [ $# -gt 0 ]; do
  case "$1" in --topic) topic="$2"; shift 2 ;; --list) op=list; shift ;; --describe) op=describe; shift ;; *) shift ;; esac
done
echo "topics $op $topic" >> "$OE_FIXTURE.topics-calls"
replay() { sed "s/@ABSENT@/$topic/" "$OE_CLI/$1.out"; sed "s/@ABSENT@/$topic/" "$OE_CLI/$1.err" >&2; exit "$(cat "$OE_CLI/$1.rc")"; }
id=$(awk '$1=="topicid"{print $2}' "$OE_FIXTURE")
if [ "${OE_BROKER:-up}" = down ]; then [ "$op" = list ] && replay unreach-list; replay unreach-describe; fi
if [ "$op" = list ]; then
  echo __consumer_offsets
  [ -n "$id" ] && printf '%s\n' es.futures.footprint.strike oe.test.reset options.spx.vol-premium.ivrv \
    options.spx.vol-premium.events options.spx.vol-premium.warnings options.spx.vol-premium.current \
    options.spx.vol-premium.dlq options.spx.vol-premium.baseline options.spx.vol-premium.calendar
  exit 0
fi
[ "${OE_DESCRIBE:-ok}" = fail ] && replay unreach-describe
[ -n "$id" ] || replay describe-absent
[ "${OE_DESCRIBE:-ok}" = zero ] && id=AAAAAAAAAAAAAAAAAAAAAA
parts=$(awk '$1 ~ /^[0-9]+$/ { print $1 }' "$OE_FIXTURE")
n=$(printf '%s\n' "$parts" | grep -c .)
if [ "${OE_DESCRIBE:-ok}" = noid ]; then
  printf 'Topic: %s\tPartitionCount: %s\tReplicationFactor: 1\tConfigs: retention.ms=-1\n' "$topic" "$n"
else
  printf 'Topic: %s\tTopicId: %s\tPartitionCount: %s\tReplicationFactor: 1\tConfigs: retention.ms=-1\n' "$topic" "$id" "$n"
fi
for p in $parts; do printf '\tTopic: %s\tPartition: %s\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n' "$topic" "$p"; done
exit 0
SH
chmod +x "$BIN/strike-reader" "$BIN/kafka-console-consumer.sh" "$BIN/kafka-topics.sh"
# The stand-in's healthy description must have the recorded shape the archiver parses: same fields, same TABs.
want "the kafka-topics stand-in describes a topic in the recorded 4.3.0 shape" \
     "$(sed -e 's/@TOPIC1@/T/g' -e 's/TopicId: [^	]*/TopicId: X/' -e 's/Configs: .*/Configs:/' "$OE_CLI/describe-1p.out")" \
     "$(printf 'topicid IIIIIIIIIIIIIIIIIIIIII\n0 0 1\n' > "$OE_FIXTURE"; "$BIN/kafka-topics.sh" --describe --topic T | sed -e 's/TopicId: [^	]*/TopicId: X/' -e 's/Configs: .*/Configs:/')"

strike_log() { # lines of the strike partition's log; the broker fixture's end offset follows from it
  printf '%s\n' "$@" > "$OE_FIXTURE.strike"
  local hwm; hwm=$(awk 'NF { e = $1 + 1 } END { print e + 0 }' "$OE_FIXTURE.strike")
  fixture "topicid ${STRIKE_ID:-SSSSSSSSSSSSSSSSSSSSSS}" "0 0 $hwm${UNTIL_OFF:+ $UNTIL_OFF}"
}
srun() { # $@ = extra env assignments
  env ARCHIVE_DIR="$A" ENV=es4 ARCHIVE_JOB=test-strike BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" \
      TOPICS="$STRIKE" SESSION_DATE="$SDAY" ALLOW_NON_NAS=true STRIKE_READER="$BIN/strike-reader" \
      "$@" "$ARCH" 2>&1
}
fresh() { A="$T/$1"; mkdir -p "$A"; : > "$CALLS"; : > "$MARKS"; }
SDIR() { echo "$A/kafka/es4/$STRIKE/dt=$SDAY"; }
sck() { awk '{split($1,a,"="); if (a[1]=="0") print a[2]}' "$A/kafka/es4/_manifest/$STRIKE.offsets" 2>/dev/null | tail -1; }
sruns() { awk -v k="$1" '{for(i=1;i<=NF;i++){split($i,x,"="); if (x[1]==k) v=x[2]}} END{print v}' "$A/kafka/es4/_manifest/runs.log" 2>/dev/null; }
mlast() { # <field> of the LAST manifest line
  python3 -c 'import json,sys
lines=[l for l in open(sys.argv[1]) if l.strip()]
print(json.loads(lines[-1]).get(sys.argv[2], "<absent>") if lines else "<no-manifest>")' "$(SDIR)/_manifest.jsonl" "$1" 2>/dev/null || echo "<no-manifest>"; }
mlines() { grep -c . "$(SDIR)/_manifest.jsonl" 2>/dev/null || echo 0; }
soffsets() { # every Offset archived for the date, in order, across files
  local f; for f in "$(SDIR)"/*.jsonl.gz; do [ -f "$f" ] && zcat "$f"; done 2>/dev/null \
    | awk -F'\t' '{ sub("Offset:", "", $3); print $3 }' | sort -n | tr '\n' ' ' | sed 's/ $//'; }
sfiles() { ls "$(SDIR)"/*.jsonl.gz 2>/dev/null | wc -l | tr -d ' '; }
lastmark() { tail -1 "$MARKS" 2>/dev/null; }
contiguous() { # the verifier's own continuity rule over this date's manifest ranges
  python3 -c 'import json,sys
r=sorted((int(e["offset_from"]),int(e["offset_to"])) for e in map(json.loads,filter(str.strip,open(sys.argv[1]))))
g=[f"{a[1]}->{b[0]}" for a,b in zip(r,r[1:]) if b[0]>a[1]]
print("gaps:"+",".join(g) if g else "contiguous")' "$(SDIR)/_manifest.jsonl" 2>/dev/null; }

# ---- 12a. committed + aborted, nothing open: the whole log, aborted revisions excluded -------------
fresh s1
strike_log '0 C k0 {"n":0}' '1 C k1 {"n":1}' '2 M' '3 A a3 {"aborted":3}' '4 A a4 {"aborted":4}' '5 M' '6 C k6 {"n":6}' '7 M'
OUT=$(srun); RC=$?
want "12a committed+aborted: run succeeds (rc)"            0 "$RC"
want "  checkpoint = the stable boundary (= log end)"      8 "$(sck)"
want "  exactly the committed offsets are archived"   "0 1 6" "$(soffsets)"
want "  manifest records = committed records"              3 "$(mlast records)"
want "  manifest offset_to = the boundary"                 8 "$(mlast offset_to)"
want "  manifest names the capture"   read_committed_stable_boundary "$(mlast capture)"
want "  no aborted revision in the archive"                0 "$(for f in "$(SDIR)"/*.jsonl.gz; do zcat "$f"; done | grep -c aborted)"
want "  the strike log never reached the console consumer" 0 "$(grep -c "^console $STRIKE" "$CALLS")"
want "  the committed reader read it, from 0"              1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
want "  archive marker recorded at the checkpoint"         8 "$(lastmark)"
has  "  and the run says so" "MARK $STRIKE p0: archived-through 8" "$OUT"
has  "  the checkpoint line is STAMPED as the committed reader's" "0=8 records=3 span=8 dt=$SDAY archived=" "$(tail -1 "$A/kafka/es4/_manifest/$STRIKE.offsets")"
has  "  (stamp token)" " capture=read_committed_stable_boundary" "$(tail -1 "$A/kafka/es4/_manifest/$STRIKE.offsets")"

# ---- 12b. UNRESOLVED transaction, then it commits: the retry captures every withheld record ------------
# The reviewer's reproduction: a transaction open at offset 3 while later offsets exist. The high-water
# mark is 7; the old rule checkpointed it and skipped 3, 4 and 5 for ever.
fresh s2
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 O k3 v3' '4 O k4 v4' '5 C k5 v5' '6 M'
OUT=$(srun); RC=$?
want "12b open transaction: run succeeds (rc)"             0 "$RC"
want "  checkpoint = the LSO, NOT the high-water mark 7"   3 "$(sck)"
want "  manifest offset_to = the LSO"                      3 "$(mlast offset_to)"
want "  only records below the LSO"                  "0 1" "$(soffsets)"
want "  marker = the LSO"                                  3 "$(lastmark)"
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 C k3 v3' '4 C k4 v4' '5 C k5 v5' '6 M' '7 M'
OUT=$(srun); RC=$?
want "  the retry succeeds (rc)"                           0 "$RC"
want "  the retry started at the LSO"                      1 "$(grep -c "^reader $STRIKE p0 from=3 " "$CALLS")"
want "  checkpoint = the new boundary"                     8 "$(sck)"
want "  EVERY committed record, each exactly once" "0 1 3 4 5" "$(soffsets)"
want "  manifest ranges are contiguous"           contiguous "$(contiguous)"

# ---- 12c. marker/aborted-only range: completes with ZERO records and still advances ---------------
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 C k3 v3' '4 C k4 v4' '5 C k5 v5' '6 M' '7 M' '8 A a8 x' '9 M'
OUT=$(srun); RC=$?
want "12c marker-only range: run succeeds (rc)"            0 "$RC"
want "  checkpoint advances over markers + aborted"       10 "$(sck)"
want "  its manifest line says 0 records"                  0 "$(mlast records)"
want "  over offsets [8,10)"                           "8 10" "$(mlast offset_from) $(mlast offset_to)"
want "  an (empty) file backs that line"                   3 "$(sfiles)"
want "  the date's ranges stay contiguous"        contiguous "$(contiguous)"
want "  nothing new archived"                    "0 1 3 4 5" "$(soffsets)"
hasnt "  and it is not a failure"  "failed=1" "$OUT"

# ---- 12d. DELAYED FINALIZE: the open transaction sits AT the checkpoint --------------------------
# The session's finalize is still open when the capture runs: the stable boundary equals the checkpoint.
# Nothing is captured, nothing is skipped, the run is not a failure, and the later run captures it.
fresh s4
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
srun >/dev/null
want "12d session captured before the finalize"           3 "$(sck)"
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 O fin3 v' '4 O fin4 v'
: > "$MARKS"
OUT=$(srun); RC=$?
want "  finalize still OPEN: run succeeds (rc)"            0 "$RC"
has  "  and says an open transaction holds the range" "an open transaction holds the range" "$OUT"
want "  checkpoint unchanged"                              3 "$(sck)"
want "  NO archive marker: checkpoint 3 does not reach the log end 5" "" "$(lastmark)"
# Deploy #1041 review round 2, MAJOR 3: the attempt IS recorded — a manifest line with no file, the empty range at
# the checkpoint and the end this run queried — so a session loader learns that [3,5) is still owed.
want "  the ATTEMPT is recorded: a second manifest line, no file, at the checkpoint, the queried end" \
     "2 no_progress <absent> 3 3 3 5" \
     "$(mlines) $(mlast attempt) $(mlast file) $(mlast offset_from) $(mlast offset_to) $(mlast stable_boundary) $(mlast queried_end)"
want "  naming the source log, with zero records" "SSSSSSSSSSSSSSSSSSSSSS 0" "$(mlast source_topic_id) $(mlast records)"
want "  and no data file was published for it"                1 "$(sfiles)"
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 C fin3 v' '4 C fin4 v' '5 M'
OUT=$(srun); RC=$?
want "  finalize COMMITTED: the next run captures it (rc)" 0 "$RC"
want "  checkpoint past the finalize"                      6 "$(sck)"
want "  the finalize records are archived"         "0 1 3 4" "$(soffsets)"

# ---- 12e. UNTIL_TS with abundant later records: contents bounded by the cutoff (finding 6) ---------
fresh s5
UNTIL_OFF=4 strike_log '0 C a v' '1 C b v' '2 C c v' '3 C d v' '4 C e v' '5 C f v' '6 C g v' '7 C h v' '8 C i v' '9 C j v'
OUT=$(srun UNTIL_TS=1786000000000); RC=$?
want "12e bounded: run succeeds (rc)"                      0 "$RC"
want "  the reader was capped at the time-bounded offset"  1 "$(grep -c "^reader $STRIKE p0 from=0 max_end=4 " "$CALLS")"
want "  only records before the cutoff are archived" "0 1 2 3" "$(soffsets)"
want "  manifest records count ONLY in-range records"      4 "$(mlast records)"
want "  manifest offset_to = the cutoff"                   4 "$(mlast offset_to)"
want "  checkpoint = the cutoff"                           4 "$(sck)"
hasnt "  and bounded mode is not a reset"  "RESET" "$OUT"

# ---- 12f. the reader TIMES OUT: a failed capture, checkpoint unchanged, nothing published -----------
fresh s6
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
OUT=$(srun STRIKE_SHIM_MODE=timeout); RC=$?
want "12f reader timeout: the run FAILS (rc)"              1 "$RC"
has  "  and says the capture failed" "committed-read capture FAILED" "$OUT"
has  "  naming the reader's status" "status=TIMEOUT" "$OUT"
want "  no checkpoint written"                            "" "$(sck)"
want "  no file published"                                 0 "$(sfiles)"
want "  no manifest line"                                  0 "$(mlines)"
want "  no archive marker"                                "" "$(lastmark)"
want "  runs.log records the failure"                      1 "$(sruns failed)"
OUT=$(srun); RC=$?
want "  the next healthy run captures everything"      "0 1" "$(soffsets)"
want "  and checkpoints the boundary"                      3 "$(sck)"

# ---- 12g-i. a reader whose claim does not hold up is refused, whatever its exit status ------------
for mode in lie-short lie-count no-summary; do
  fresh "s7-$mode"
  strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
  OUT=$(srun STRIKE_SHIM_MODE=$mode); RC=$?
  want "12g reader '$mode' (exit 0): the run FAILS (rc)"   1 "$RC"
  want "  checkpoint unchanged"                           "" "$(sck)"
  want "  nothing published"                               0 "$(sfiles)"
done

# ---- 12j-m. processing failures on the committed path leave the checkpoint alone (finding 5) --------
REAL_GZIP=$(command -v gzip); REAL_ZCAT=$(command -v zcat); REAL_AWK=$(command -v awk); REAL_MV=$(command -v mv)
FAULT="$T/fault"; mkdir -p "$FAULT"
fault_bin() { # <name> <body> — a PATH shim that replaces one tool for one run
  rm -rf "$FAULT/$1"; mkdir -p "$FAULT/$1"; printf '#!/usr/bin/env bash\n%s\n' "$2" > "$FAULT/$1/$1"; chmod +x "$FAULT/$1/$1"
}
# compressor: the archiver's `gzip -6` writes a partial stream and dies; decompression elsewhere is real
fault_bin gzip "if [ \"\${1:-}\" = -6 ]; then head -c 20 >/dev/null; printf 'partial'; exit 1; fi; exec $REAL_GZIP \"\$@\""
# decompressor: the statistics pass decompresses everything, then fails
fault_bin zcat "$REAL_ZCAT \"\$@\"; exit 7"
# scanner: the statistics awk prints its numbers, then fails ("scan_archive_file prints statistics, then returns 7")
fault_bin awk "for a in \"\$@\"; do case \"\$a\" in *schemaVersion*) $REAL_AWK \"\$@\"; exit 7 ;; esac; done; exec $REAL_AWK \"\$@\""
# publication: renaming a data file into place fails (a full or stale NAS)
fault_bin mv "for a in \"\$@\"; do last=\"\$a\"; done; case \"\$last\" in *.jsonl.gz) echo 'mv: No space left on device' >&2; exit 1 ;; esac; exec $REAL_MV \"\$@\""
for tool in gzip zcat awk mv; do
  fresh "s8-$tool"
  strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
  OUT=$(srun PATH="$FAULT/$tool:$PATH"); RC=$?
  want "12j committed path, $tool failure: the run FAILS (rc)" 1 "$RC"
  want "  checkpoint unchanged"                              "" "$(sck)"
  want "  no data file published"                             0 "$(sfiles)"
  want "  no archive marker"                                 "" "$(lastmark)"
done

# ---- 12n. the archive marker: es4 only by default, healed on an idle rerun, never fatal -------------
fresh s9
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
srun >/dev/null
: > "$MARKS"
OUT=$(srun); RC=$?
want "12n idle rerun (nothing new): run succeeds (rc)"     0 "$RC"
want "  re-records the marker at the durable checkpoint"   3 "$(lastmark)"
fresh s10
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
OUT=$(srun STRIKE_SHIM_MARK=fail); RC=$?
want "  a marker write that fails does not fail the archive (rc)" 0 "$RC"
want "  the capture and checkpoint stand"                  3 "$(sck)"
has  "  and the run warns that the wipe will stay refused" "cleanup-es4.sh will refuse to wipe" "$OUT"
fresh s11
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-strike BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" TOPICS="$STRIKE" \
    SESSION_DATE="$SDAY" ALLOW_NON_NAS=true STRIKE_READER="$BIN/strike-reader" "$ARCH" >/dev/null 2>&1
want "  ENV=prod writes NO marker to its broker"           0 "$(grep -c 'mark=[0-9]' "$CALLS")"
want "  but still reads it committed-only, through the reader" 1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
want "  and checkpoints the stable boundary"               3 "$(awk '{split($1,a,"="); if (a[1]=="0") print a[2]}' "$A/kafka/prod/_manifest/$STRIKE.offsets" | tail -1)"

# ---- 12o. every OTHER topic keeps the console consumer, its arguments and its manifest line ---------
fresh s12
fixture "topicid OOOOOOOOOOOOOOOOOOOOOO" "0 0 40" "1 0 40"
env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-reset BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" TOPICS="$TOPIC" \
    ALLOW_NON_NAS=true STRIKE_READER="$BIN/strike-reader" "$ARCH" >/dev/null 2>&1
want "12o a non-declared topic never reaches the committed reader" 0 "$(grep -c '^reader ' "$CALLS")"
want "  it is read by the console consumer, per partition" 2 "$(grep -c "^console $TOPIC " "$CALLS")"
want "  with NO consumer property (read_uncommitted as before)" 2 "$(grep -c "^console $TOPIC p[01] props=\[\]$" "$CALLS")"
last_plain=$(tail -1 "$A/kafka/prod/$TOPIC"/dt=*/_manifest.jsonl 2>/dev/null)
hasnt "  its manifest line carries no committed-read fields" '"capture"' "$last_plain"
has  "  and ends exactly as it always did" "\"archiver_version\":\"2026-08-13.1\"}" "$last_plain"
want "  its checkpoint lines are unchanged: '<p>=40 records=40 span=40 dt=<d> archived=<stamp>', no stamp" 2 \
     "$(grep -cE '^[01]=40 records=40 span=40 dt=[0-9-]+ archived=[0-9]{8}T[0-9]{6}Z$' "$A/kafka/prod/_manifest/$TOPIC.offsets")"

# ---- 12p. an UNSTAMPED strike checkpoint proves nothing (re-review round 2, finding 2) -------------------------
# The reviewer's case: an OLDER archiver read committed-only, stopped below a transaction open at 3, and still
# checkpointed the high-water mark 7. That transaction has since committed. Resuming at 7 skips 3, 4 and 5 for
# ever — and the round-1 idle rule then marked 7 on the source, so cleanup-es4.sh would have wiped them. No such
# line exists in production (the strike topic was never archived before this reader), but nothing may trust one.
fresh s14
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 C k3 v3' '4 C k4 v4' '5 C k5 v5' '6 M'
mkdir -p "$A/kafka/es4/_manifest"
printf '0=7 records=2 span=7 dt=2026-09-08 archived=20260908T210100Z\n' > "$A/kafka/es4/_manifest/$STRIKE.offsets"
OUT=$(srun); RC=$?
want "12p legacy checkpoint AT the log end: run succeeds (rc)"   0 "$RC"
has  "  and calls it LEGACY" "LEGACY $STRIKE p0: checkpoint 7 was not written by the committed-read capture" "$OUT"
want "  it did NOT sit idle on it: the reader recaptured from the log start" 1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
want "  the withheld records 3, 4 and 5 are archived now"  "0 1 3 4 5" "$(soffsets)"
want "  the new checkpoint is the boundary"                      7 "$(sck)"
has  "  and it is stamped" " capture=read_committed_stable_boundary" "$(tail -1 "$A/kafka/es4/_manifest/$STRIKE.offsets")"
want "  the manifest records what it recaptured over"            7 "$(mlast recaptured_over_unproven_checkpoint)"
want "  the marker was written once, only AFTER the capture" "reader mark" \
     "$(awk '/^reader .* from=[0-9]/ { print "reader" } /mark=[0-9]/ { print "mark" }' "$CALLS" | tr '\n' ' ' | sed 's/ $//')"
want "  at the captured boundary"                                7 "$(lastmark)"
: > "$MARKS"; : > "$CALLS"
OUT=$(srun); RC=$?
want "  the next run trusts its OWN stamped checkpoint: no recapture" 0 "$(grep -c "^reader $STRIKE p0 from=[0-9]" "$CALLS")"
hasnt "  and says nothing about LEGACY" "LEGACY" "$OUT"
want "  that idle run re-records the marker (stamped, and it reaches the log end)" 7 "$(lastmark)"
fresh s15
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 C k3 v3' '4 C k4 v4' '5 C k5 v5' '6 M'
mkdir -p "$A/kafka/es4/_manifest"
printf '0=4 records=2 span=4 dt=2026-09-08 archived=20260908T210100Z\n' > "$A/kafka/es4/_manifest/$STRIKE.offsets"
OUT=$(srun); RC=$?
want "12p legacy checkpoint BELOW the log end: recaptured from 0, not resumed at 4" 1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
want "  every committed record archived"                 "0 1 3 4 5" "$(soffsets)"
fresh s16
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
mkdir -p "$A/kafka/es4/_manifest"
printf '0=0 records=0 span=0 dt=2026-09-08 archived=20260908T210100Z rebaselined=from-500\n' > "$A/kafka/es4/_manifest/$STRIKE.offsets"
OUT=$(srun); RC=$?
hasnt "12p a re-baseline line (the new log's start) is not LEGACY" "LEGACY" "$OUT"
want "  it is resumed from"                                       1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
fresh s16b
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
mkdir -p "$A/kafka/es4/_manifest"
printf '0=2 records=0 span=0 dt=2026-09-08 archived=20260908T210100Z rebaselined=repair-1\n' > "$A/kafka/es4/_manifest/$STRIKE.offsets"
OUT=$(srun); RC=$?
has  "12p a re-baseline-looking line ABOVE the log start (hand-written) IS legacy" "LEGACY $STRIKE p0: checkpoint 2" "$OUT"
want "  so nothing below it is skipped: recaptured from 0"       "0 1" "$(soffsets)"
fresh s17
UNTIL_OFF=4 strike_log '0 C a v' '1 C b v' '2 C c v' '3 C d v' '4 C e v' '5 C f v' '6 C g v' '7 C h v' '8 C i v' '9 C j v'
srun UNTIL_TS=1786000000000 >/dev/null
want "12p a bounded capture checkpoints the cutoff (stamped)"    4 "$(sck)"
: > "$MARKS"
OUT=$(srun UNTIL_TS=1786000000000); RC=$?
want "  the bounded idle rerun succeeds (rc)"                     0 "$RC"
want "  but records NO marker: checkpoint 4 does not reach the log end 10" "" "$(lastmark)"

# ---- 12q. a FAILED UNTIL_TS lookup fails the run (re-review round 2, finding 5) -----------------------------
# Below, a partition with no line in the time-bounded answer is read to the LOG END: that is what "no record at
# or after the cutoff" looks like. A failed lookup looks the same on stdout. The recorded answers:
want "the recorded 4.3.0 answer to a timestamp after every record: exit 0" 0 "$(cat "$OE_CLI/until-nomatch.rc")"
want "  with NO line for the partition"                  0 "$(grep -c . "$OE_CLI/until-nomatch.out")"
want "  and nothing on stderr"                           0 "$(wc -c < "$OE_CLI/until-nomatch.err" | tr -d ' ')"
want "the recorded answer of an unreachable broker: exit 1" 1 "$(cat "$OE_CLI/unreach-ends.rc")"
want "  ALSO with no line on stdout — the same silence, so only status and stderr can decide" 0 "$(grep -c . "$OE_CLI/unreach-ends.out")"
for mode in fail skip; do
  fresh "s18-$mode"
  UNTIL_OFF=4 strike_log '0 C a v' '1 C b v' '2 C c v' '3 C d v' '4 C e v' '5 C f v' '6 C g v' '7 C h v' '8 C i v' '9 C j v'
  OUT=$(srun UNTIL_TS=1786000000000 OE_TIME_QUERY=$mode); RC=$?
  want "12q strike, UNTIL_TS lookup answers '$mode': the run FAILS (rc)" 1 "$RC"
  has  "  saying the cutoff is unknown" "UNTIL_TS=1786000000000 offset lookup did not succeed" "$OUT"
  want "  the reader never ran"                           0 "$(grep -c '^reader ' "$CALLS")"
  want "  checkpoint unchanged"                          "" "$(sck)"
  want "  nothing published"                              0 "$(sfiles)"
  want "  runs.log records the failure"                   1 "$(sruns failed)"
done
has  "  (the skip case names GetOffsetShell's own diagnostic)" "stderr: Skip getting offsets for topic-partition $STRIKE-0" "$OUT"
# The reviewer's numbers, on the console path: checkpoint 0, intended cutoff 40, log end 100.
A="$T/u1"; mkdir -p "$A/kafka/prod/_manifest"
fixture "topicid UUUUUUUUUUUUUUUUUUUUUU" "0 0 100 40" "1 0 100 40"
OUT=$(run UNTIL_TS=1786000000000 OE_TIME_QUERY=fail); RC=$?
want "12q console path, checkpoint 0 / cutoff 40 / log end 100, lookup fails: the run FAILS (rc)" 1 "$RC"
want "  p0 checkpoint NOT moved to 100"                  "" "$(ck 0)"
want "  no file published" 0 "$(ls "$A/kafka/prod/$TOPIC"/dt=*/*.jsonl.gz 2>/dev/null | wc -l | tr -d ' ')"
OUT=$(run UNTIL_TS=1786000000000); RC=$?
want "  the same run with a working lookup succeeds (rc)" 0 "$RC"
want "  and stops at the cutoff"                         40 "$(ck 0)"
A="$T/u2"; mkdir -p "$A/kafka/prod/_manifest"
fixture "topicid VVVVVVVVVVVVVVVVVVVVVV" "0 0 100" "1 0 100"
OUT=$(run UNTIL_TS=1786000000000); RC=$?
want "12q a SUCCESSFUL lookup with no record after the cutoff (exit 0, no line): run succeeds (rc)" 0 "$RC"
want "  and reads to the log end"                       100 "$(ck 0)"

# ================= 13. processing failures on the CONSOLE path (finding 5, inherited code) ==========
# Each stage's failure used to be invisible: the pipeline kept only the consumer's status, and
# `read <<< "$(scan_archive_file ...)"` hid the statistics helper's. Each must now leave the checkpoint.
grep_real=$(command -v grep)
fault_bin grep "for a in \"\$@\"; do case \"\$a\" in *'Processed a total'*) cat >/dev/null; exit 2 ;; esac; done; exec $grep_real \"\$@\""
for tool in grep gzip zcat awk mv; do
  A="$T/c-$tool"; mkdir -p "$A/kafka/prod/_manifest"
  fixture "topicid PPPPPPPPPPPPPPPPPPPPPP" "0 0 40" "1 0 40"
  OUT=$(run PATH="$FAULT/$tool:$PATH"); RC=$?
  want "13 console path, $tool failure: the run FAILS (rc)"  1 "$RC"
  want "  p0 checkpoint unchanged"                           "" "$(ck 0)"
  want "  no data file published" 0 "$(ls "$A/kafka/prod/$TOPIC"/dt=*/*.jsonl.gz 2>/dev/null | wc -l | tr -d ' ')"
done
A="$T/c-ok"; mkdir -p "$A/kafka/prod/_manifest"
OUT=$(run); RC=$?
want "13 and with every tool healthy the same run archives (rc)" 0 "$RC"
want "  p0 checkpoint"                                      40 "$(ck 0)"
sha_line=$(tail -1 "$A/kafka/prod/$TOPIC"/dt=*/_manifest.jsonl)
f_named=$(printf '%s' "$sha_line" | python3 -c 'import json,sys; e=json.loads(sys.stdin.read()); print(e["file"], e["sha256"])')
want "  the manifest checksum is the published file's" "$(set -- $f_named; echo "$2")" \
     "$(set -- $f_named; sha256sum "$(dirname "$(ls "$A/kafka/prod/$TOPIC"/dt=*/_manifest.jsonl)")/$1" | cut -d' ' -f1)"

# ================= 14. es4 completeness verification (finding 3) ================================
# oe-archive-verify.sh requires OE_ALL_TOPICS_<env>; es4 had none, so its floors — the strike presence
# floor among them — were declared and never checked, and ENV=es4 failed as "no policy". This builds an
# es4 archive tree from the REAL inventory and floors and runs the REAL verifier on it.
VD="$T/vnas"; VDATE=2026-09-09
( . "$OE/oe-topics.env"; printf '%s' "${OE_ALL_TOPICS_es4:-}" > "$T/v.topics"; printf '%s' "$OE_ARCHIVE_MIN_RECORDS" > "$T/v.floors" )
want "oe-topics.env defines the es4 policy as the es4 set" "$(. "$OE/oe-topics.env"; printf '%s' "$OE_ES4_TOPICS")" "$(cat "$T/v.topics")"
vbuild() { # $1 = strike records, or "absent"
  rm -rf "$VD"; mkdir -p "$VD/kafka/es4"
  python3 - "$VD/kafka/es4" "$VDATE" "$1" "$T/v.topics" "$T/v.floors" <<'PY'
import gzip, json, os, sys
root, date, strike, topics_f, floors_f = sys.argv[1:6]
floors = {}
for pair in open(floors_f).read().split():          # the verifier's own parse
    if ":" in pair:
        t, _, v = pair.partition(":")
        try: floors[t] = int(v)
        except ValueError: pass
for t in open(topics_f).read().split():
    if t == "es.futures.footprint.strike":
        if strike == "absent": continue
        n = int(strike)
    else:
        n = max(floors.get(t, 1), 1)
    d = os.path.join(root, t, "dt=" + date); os.makedirs(d)
    name = f"{t}.p0.0-{n}.dt{date.replace('-', '')}.20260909T210100Z.jsonl.gz"
    with gzip.open(os.path.join(d, name), "wb"): pass
    with open(os.path.join(d, "_manifest.jsonl"), "w") as m:
        m.write(json.dumps({"topic": t, "dt": date, "partition": 0, "offset_from": 0, "offset_to": n,
                            "records": n, "file": name, "sha256": "", "archived_at": "20260909T210100Z"}) + "\n")
PY
}
vrun() { env ARCHIVE_DIR="$VD" ENV=es4 FORCE=true VERIFY_CHECKSUMS=none LOG="$T/verify-es4.log" \
             bash "$OE/oe-archive-verify.sh" "$VDATE" 2>&1; }
: > "$T/alerts.txt"
vbuild absent; OUT=$(vrun); RC=$?
want "14 es4, strike folder ABSENT: verifier fails (rc)"   1 "$RC"
hasnt "  it is not refused for a missing policy" "defines no OE_ALL_TOPICS_es4" "$OUT"
has  "  and names the missing strike date" "es.futures.footprint.strike:MISSING" "$OUT"
has  "  and delivers the alert for env=es4" "env=es4 dt=$VDATE" "$(cat "$T/alerts.txt" 2>/dev/null)"
has  "  naming the strike topic"            "es.futures.footprint.strike:MISSING" "$(cat "$T/alerts.txt" 2>/dev/null)"
vbuild 499; OUT=$(vrun); RC=$?
want "  strike at 499 records: verifier fails (rc)"         1 "$RC"
has  "  as PARTIAL" "es.futures.footprint.strike:PARTIAL" "$OUT"
has  "  below the 500 presence floor" "499 records is below the floor of 500" "$OUT"
vbuild 500; OUT=$(vrun); RC=$?
want "  strike at 500 records: presence satisfied (rc)"     0 "$RC"
has  "  the es4 date is COMPLETE by the floors" "VERDICT COMPLETE" "$OUT"
# 500 satisfies the PRESENCE check only: the floor counts records, it does not prove the committed
# capture reached any boundary — that is the reader's COMPLETE and the manifest's stable_boundary.
verify_line=$(grep -c '^5 20 \* \* 1-5 ENV=es4 ARCHIVE_DIR=/mnt/nas/optionsedge .*/oe-archive-verify\.sh' "$OE/oe-archive.crontab")
want "the crontab schedules exactly one es4 verification"  1 "$verify_line"
has  "  and it keeps its own log" "LOG=/home/abhinav/oe-ops/archive-verify-es4.log" "$(grep 'ENV=es4 .*oe-archive-verify' "$OE/oe-archive.crontab")"

# ================= 15. the schedule is New York time (re-review round 2, finding 6) =====================
# The finding assumed Debian's cron, which has no per-entry CRON_TZ. The host is CentOS Stream 9 with cronie
# (checked 2026-09-11), which honours it for every entry BELOW the line. What can break is placement: an entry
# above CRON_TZ runs at the host's Madrid time. So: one CRON_TZ, New York, and every fixed-time entry below it.
tz_n=$(grep -c '^CRON_TZ=' "$crontab_file")
tz_at=$(grep -n '^CRON_TZ=' "$crontab_file" | head -1 | cut -d: -f1)
want "15 exactly one CRON_TZ line"                          1 "$tz_n"
want "  and it is America/New_York" "America/New_York" "$(grep '^CRON_TZ=' "$crontab_file" | head -1 | cut -d= -f2)"
above=$(awk -v tz="$tz_at" 'NR < tz && $1 ~ /^[0-9,]+$/ && $2 ~ /^[0-9,]+$/' "$crontab_file")
want "  no fixed-time entry sits ABOVE it (it would run at Madrid time)" "" "$above"
n_below=$(awk -v tz="$tz_at" 'NR > tz && $1 ~ /^[0-9,]+$/ && $2 ~ /^[0-9,]+$/' "$crontab_file" | grep -c .)
want "  every fixed-time entry (daily 17:10, es4 17:01, verify 20:00 and 20:05, seal, progress x4) is below it" 8 "$n_below"
es4v_at=$(grep -n '^5 20 \* \* 1-5 ENV=es4 ' "$crontab_file" | cut -d: -f1)
want "  the new es4 verification entry in particular" yes "$([ -n "$es4v_at" ] && [ "$es4v_at" -gt "$tz_at" ] && echo yes || echo no)"
has  "  the header names the host's cron, which is what makes CRON_TZ work" "cronie" "$(head -n "$tz_at" "$crontab_file")"

# ================= 16. re-review round 3: source IDENTITY (P1) and VALIDATED discovery (P2) ===============
# P1: an offset names a position in ONE log. The reviewer ran round 2's checkpoint-selection and idle branches: a
# successful archive checkpoints 7, clean-reset re-creates the topic, the new log reaches 7, the TopicId query fails
# — and the idle path marked the NEW log through 7 without capturing it, exit 0, so cleanup-es4.sh would wipe
# records no archive holds. P2: an unreadable initial offset answer (an unreachable broker; "Skip getting offsets"
# at exit 0) became SKIP or a silently missing partition, failed=0, and never reached the UNTIL_TS validation.
# Every broker answer below is RECORDED (cli-fixtures/4.3.0) or the constructed Skip line (constructed/ORIGIN).
OLD7=('0 C k0 v0' '1 C k1 v1' '2 M' '3 C k3 v3' '4 C k4 v4' '5 C k5 v5' '6 M')
NEW7=('0 C n0 w0' '1 C n1 w1' '2 C n2 w2' '3 M' '4 C n4 w4' '5 C n5 w5' '6 M')   # the re-created log, grown back to 7
SID=SSSSSSSSSSSSSSSSSSSSSS; TID=TTTTTTTTTTTTTTTTTTTTTT
OFFS() { cat "$A/kafka/es4/_manifest/$STRIKE.offsets" 2>/dev/null; }
IDF() { cat "$A/kafka/es4/_manifest/$STRIKE.identity" 2>/dev/null | head -1; }
nreaders() { local n; n=$(grep -c "^reader $STRIKE p[0-9]* from=[0-9]" "$CALLS" 2>/dev/null); echo "${n:-0}"; }
nmarks() { local n; n=$(grep -c . "$MARKS" 2>/dev/null); echo "${n:-0}"; }
order() { awk '/^reader .* from=[0-9]/ { print "reader" } /mark=[0-9]/ { print "mark" }' "$CALLS" | tr '\n' ' ' | sed 's/ $//'; }
skeys() { local f; for f in "$(SDIR)"/*.jsonl.gz; do [ -f "$f" ] && zcat "$f"; done 2>/dev/null | awk -F'\t' '{ print $4 }' | sort | tr '\n' ' ' | sed 's/ $//'; }
R2LINE='0=7 records=5 span=7 dt=2026-09-08 archived=20260908T210100Z capture=read_committed_stable_boundary'   # round 2's stamp: no source

# ---- 16a. the reviewer's case: checkpoint 7, topic re-created and back at 7, the TopicId query FAILS -----------------
fresh r1
strike_log "${OLD7[@]}"
OUT=$(srun); RC=$?
want "16a setup: an archive run (rc)"                                        0 "$RC"
want "  checkpoints 7"                                                       7 "$(sck)"
has  "  the stamp names the log it was taken on" " capture=read_committed_stable_boundary topic_id=$SID" "$(OFFS | tail -1)"
want "  and so does the manifest line"                                    "$SID" "$(mlast source_topic_id)"
STRIKE_ID=$TID strike_log "${NEW7[@]}"
CK_BEFORE=$(OFFS); ID_BEFORE=$(IDF); : > "$MARKS"; : > "$CALLS"
OUT=$(srun OE_DESCRIBE=fail); RC=$?
want "16a re-created log at 7, TopicId query fails (recorded unreachable answer): the run FAILS (rc)" 1 "$RC"
has  "  saying the identity cannot be established" "FAIL $STRIKE: its partitions and TopicId cannot be established" "$OUT"
has  "  in the broker's recorded words" "Timed out waiting for a node assignment" "$OUT"
want "  NO archive marker"                                                   0 "$(nmarks)"
want "  the reader never ran"                                                0 "$(grep -c '^reader ' "$CALLS")"
want "  the checkpoint is byte-for-byte unchanged"               "$CK_BEFORE" "$(OFFS)"
want "  the identity file is unchanged"                          "$ID_BEFORE" "$(IDF)"
want "  runs.log failed=1"                                                   1 "$(sruns failed)"
want "  absent=0: not a quiet day"                                           0 "$(sruns absent)"
for mode in noid zero; do
  : > "$MARKS"; : > "$CALLS"
  OUT=$(srun OE_DESCRIBE=$mode); RC=$?
  want "16a the description carries $([ "$mode" = noid ] && echo 'NO TopicId' || echo 'the all-zero TopicId'): the run FAILS (rc)" 1 "$RC"
  want "  NO archive marker, the reader never ran" "0 0" "$(nmarks) $(grep -c '^reader ' "$CALLS")"
  want "  checkpoint unchanged"                                  "$CK_BEFORE" "$(OFFS)"
done
# The reproduction exactly as reported: round 2's own stamped line (no identity) on the re-created log.
fresh r1b
STRIKE_ID=$TID strike_log "${NEW7[@]}"
mkdir -p "$A/kafka/es4/_manifest"
printf '%s\n' "$R2LINE" > "$A/kafka/es4/_manifest/$STRIKE.offsets"
printf 'topic_id=%s\nobserved=20260908T210100Z\n' "$SID" > "$A/kafka/es4/_manifest/$STRIKE.identity"
OUT=$(srun OE_DESCRIBE=fail); RC=$?
want "16a round 2's stamped 7 on the re-created log, TopicId query fails: refused (rc)" 1 "$RC"
want "  NO marker at 7 (round 2 wrote one), the reader never ran" "0 0" "$(nmarks) $(grep -c '^reader ' "$CALLS")"
want "  checkpoint unchanged"                                      "$R2LINE" "$(OFFS)"

# ---- 16b. the same, but a DIFFERENT TopicId is read successfully: a new log, recaptured from its start ------------
fresh r2
strike_log "${OLD7[@]}"; srun >/dev/null
STRIKE_ID=$TID strike_log "${NEW7[@]}"
: > "$MARKS"; : > "$CALLS"; reset_alerts
OUT=$(srun); RC=$?
want "16b re-created log at 7, different TopicId read: run succeeds (rc)"   0 "$RC"
has  "  the reset is seen"                                   "RESET $STRIKE p0" "$OUT"
want "  the new log is recaptured from its start"                           1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
has  "  every record of the new log is archived" "n0 n1 n2 n4 n5" "$(skeys)"
has  "  checkpoint 7, stamped with the NEW TopicId"       "0=7 " "$(OFFS | tail -1)"
has  "  (the new id)"                                       "topic_id=$TID" "$(OFFS | tail -1)"
want "  the marker comes after the capture, once"                "reader mark" "$(order)"
want "  at 7"                                                               7 "$(lastmark)"
want "  rebaselined=1, and alerted"                                     "1 1" "$(sruns rebaselined) $(alerts)"
# With NO identity file, only the checkpoint's own TopicId can tell (the round-2 archiver resumed at 7 and marked).
fresh r3
strike_log "${OLD7[@]}"; srun >/dev/null
rm -f "$A/kafka/es4/_manifest/$STRIKE.identity"
STRIKE_ID=$TID strike_log "${NEW7[@]}"
: > "$MARKS"; : > "$CALLS"; reset_alerts
OUT=$(srun); RC=$?
want "16b the same with NO identity file: run succeeds (rc)"                0 "$RC"
has  "  the checkpoint's own TopicId catches it" "checkpoint 7 was taken on TopicId $SID, the log is now $TID" "$OUT"
has  "  and the alert names that detector"         "detected by: checkpoint-topic-id" "$OUT"
want "  recaptured from the new log's start"                                1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
has  "  every record of the new log is archived" "n0 n1 n2 n4 n5" "$(skeys)"
want "  the marker comes after the capture, once"                "reader mark" "$(order)"
has  "  the new stamp names the new log"                    "topic_id=$TID" "$(OFFS | tail -1)"
: > "$MARKS"; : > "$CALLS"
OUT=$(srun); RC=$?
want "16b the next run on the SAME log trusts its checkpoint: no recapture"  0 "$(nreaders)"
want "  and re-records the marker (stamped, same TopicId, at the log end)"   7 "$(lastmark)"

# ---- 16c. a ROUND-2 stamped line (no identity) with the TopicId readable: treated as unstamped, recaptured ---------
fresh r4
strike_log "${OLD7[@]}"
mkdir -p "$A/kafka/es4/_manifest"
printf '%s\n' "$R2LINE" > "$A/kafka/es4/_manifest/$STRIKE.offsets"
OUT=$(srun); RC=$?
want "16c round 2's stamped 7, TopicId readable: run succeeds (rc)"         0 "$RC"
has  "  it is LEGACY" "LEGACY $STRIKE p0: checkpoint 7 is stamped but names no source log" "$OUT"
want "  recaptured from the log start, not resumed at 7"                    1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
want "  the marker only after that capture"                      "reader mark" "$(order)"
has  "  and the new checkpoint names its log"               "topic_id=$SID" "$(OFFS | tail -1)"

# ---- 16d. a stamped checkpoint on the SAME log whose end reads BELOW it: a failed reading, not a quiet one -------
fresh r5
strike_log '0 C k0 v0' '1 C k1 v1' '2 M' '3 C k3 v3' '4 M'
mkdir -p "$A/kafka/es4/_manifest"
printf '0=9 records=4 span=9 dt=2026-09-08 archived=20260908T210100Z capture=read_committed_stable_boundary topic_id=%s\n' "$SID" \
  > "$A/kafka/es4/_manifest/$STRIKE.offsets"
CK_BEFORE=$(OFFS)
OUT=$(srun); RC=$?
want "16d stamped 9 on the same TopicId, log end reads 5: the run FAILS (rc)" 1 "$RC"
has  "  as a failed reading"                              "FAILED offset read" "$OUT"
want "  checkpoint unchanged, no reader, no marker"  "$CK_BEFORE 0 0" "$(OFFS) $(grep -c '^reader ' "$CALLS") $(nmarks)"

# ---- 16e. P2: the RECORDED unreachable-broker answer on initial discovery ---------------------------------------
fresh r6
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'; srun >/dev/null
CK_BEFORE=$(OFFS); : > "$MARKS"; : > "$CALLS"
OUT=$(srun OE_BROKER=down); RC=$?
want "16e unreachable broker on initial discovery: the run FAILS (rc)"      1 "$RC"
want "  runs.log failed=1"                                                   1 "$(sruns failed)"
want "  absent=0: an unreachable broker is not an absent topic"              0 "$(sruns absent)"
has  "  saying the topic list cannot be read" "FAIL $STRIKE: the broker's topic list cannot be read (kafka-topics --list: exit 1: Error while executing topic command : Timed out waiting for a node assignment" "$OUT"
want "  no reader, no marker, checkpoint unchanged" "0 0 $CK_BEFORE" "$(grep -c '^reader ' "$CALLS") $(nmarks) $(OFFS)"
OUT=$(srun OE_BROKER=down UNTIL_TS=1786000000000); RC=$?
want "16e the same in a backfill (UNTIL_TS): still a failure, the cutoff validation is not bypassed (rc)" 1 "$RC"
want "  failed=1"                                                            1 "$(sruns failed)"
OUT=$(srun OE_ENDS_QUERY=fail); RC=$?
want "16e list and description read, then the log-end query unreachable: the run FAILS (rc)" 1 "$RC"
has  "  naming the query"   "its latest offsets cannot be read (kafka-get-offsets: exit 1" "$OUT"
OUT=$(srun OE_EARLIEST_QUERY=fail); RC=$?
want "16e the log-START query unreachable: the run FAILS (rc)"              1 "$RC"
has  "  naming the query"                "its earliest offsets cannot be read" "$OUT"
want "  after all of them: no reader, no marker, checkpoint unchanged" "0 0 $CK_BEFORE" "$(grep -c '^reader ' "$CALLS") $(nmarks) $(OFFS)"

# ---- 16f. P2: a "Skip getting offsets" partial answer (exit 0), and a partition silently missing -------------------
fresh r7
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
fixture "topicid $SID" "0 0 3" "1 0 3"      # the committed-read topic with TWO partitions
OUT=$(srun OE_ENDS_QUERY=skip OE_SKIP_PART=1); RC=$?
want "16f 'Skip getting offsets' for p1 at exit 0, p0 answered: the run FAILS (rc)" 1 "$RC"
has  "  naming GetOffsetShell's diagnostic" "exit 0 but it reported: Skip getting offsets for topic-partition $STRIKE-1" "$OUT"
want "  not even p0 is read"                                                 0 "$(grep -c '^reader ' "$CALLS")"
want "  failed=1, no checkpoint"                                         "1 " "$(sruns failed) $(sck)"
OUT=$(srun OE_ENDS_QUERY=missing OE_SKIP_PART=1); RC=$?
want "16f p1 silently MISSING (exit 0, no diagnostic): the run FAILS (rc)"  1 "$RC"
has  "  caught by the partition count" "offsets came back for partitions [0] of the 2 the topic has" "$OUT"
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'   # one partition again
OUT=$(srun OE_ENDS_QUERY=skip); RC=$?
want "16f the ONLY partition skipped (stdout empty, exit 0): FAILS, not SKIP (rc)" 1 "$RC"
want "  absent=0, failed=1"                                              "0 1" "$(sruns absent) $(sruns failed)"
OUT=$(srun OE_EARLIEST_QUERY=skip); RC=$?
want "16f a Skip in the log-START answer: the run FAILS (rc)"               1 "$RC"
want "  after all of them: nothing read, nothing archived, no marker" "0 0 0" "$(grep -c '^reader ' "$CALLS") $(sfiles) $(nmarks)"

# ---- 16g. confirmed absence is still SKIP ------------------------------------------------------------------------
fresh r8
: > "$OE_FIXTURE"; : > "$OE_FIXTURE.offsets-calls"
OUT=$(srun); RC=$?
want "16g topic CONFIRMED absent (a topic list read successfully, without it): run succeeds (rc)" 0 "$RC"
has  "  as SKIP"                                             "SKIP $STRIKE (absent" "$OUT"
want "  absent=1, failed=0"                                              "1 0" "$(sruns absent) $(sruns failed)"
want "  the reader never ran"                                                0 "$(grep -c '^reader ' "$CALLS")"
want "  kafka-get-offsets never asked (recorded answer: exit 1 'Could not match')" 0 "$(grep -c . "$OE_FIXTURE.offsets-calls")"

# ---- 16h. the topic is re-created WHILE the capture runs: the marker is not written for a log it did not match ---
fresh r9
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
OUT=$(srun STRIKE_SHIM_RECREATE=$TID); RC=$?
want "16h re-created during the capture: the capture itself stands (rc)"   0 "$RC"
want "  NO archive marker"                                                   0 "$(nmarks)"
has  "  and it says why" "NOT recording archived-through 3 on the source broker — its identity could not be re-confirmed as TopicId $SID" "$OUT"
: > "$MARKS"; : > "$CALLS"
OUT=$(srun); RC=$?
has  "16h the next run sees a different log"                 "RESET $STRIKE p0" "$OUT"
want "  recaptures it from its start"                                        1 "$(grep -c "^reader $STRIKE p0 from=0 " "$CALLS")"
want "  and only then marks it"                                  "reader mark" "$(order)"

# ---- 16i. CONSOLE-consumer topics: discovery deliberately UNCHANGED (the round-3 record says why) ---------------
A="$T/r10"; mkdir -p "$A/kafka/prod/_manifest"; : > "$OE_FIXTURE.topics-calls"
fixture "topicid WWWWWWWWWWWWWWWWWWWWWW" "0 0 40" "1 0 40"
OUT=$(run OE_BROKER=down); RC=$?
want "16i UNCHANGED: a console topic against an unreachable broker is still SKIP (rc)" 0 "$RC"
want "  absent=1, failed=0, as before this round"                        "1 0" "$(runs absent) $(runs failed)"
OUT=$(run); RC=$?
want "16i with the broker up it archives as before (rc)"                    0 "$RC"
want "  p0 checkpoint"                                                      40 "$(ck 0)"
want "  its checkpoint line carries no stamp and no identity" 2 \
     "$(grep -cE '^[01]=40 records=40 span=40 dt=[0-9-]+ archived=[0-9]{8}T[0-9]{6}Z$' "$A/kafka/prod/_manifest/$TOPIC.offsets")"
want "  and its discovery never asked for a topic list"                      0 "$(grep -c '^topics list' "$OE_FIXTURE.topics-calls")"

# ============ 17. the VOL-PREMIUM ledgers: committed-only, with their real offsets preserved ================
# vol-premium-service runs Kafka Streams exactly_once_v2 (it refuses to boot otherwise), so options.spx.vol-premium.ivrv
# and its sibling sinks are written inside transactions. Read by the console consumer (read_uncommitted, its default),
# the records of an ABORTED transaction are archived, and the calibrator (IvRvArchiveLoader) and the activation gate
# (GateArchiveLoader) take them as observations (17z reproduces it). OE_COMMITTED_READ_TOPICS now routes every
# vol-premium ledger to the committed reader, and every such file KEEPS the reader's Offset column (deploy #1041
# review, engine #44): the per-record offset is the only coordinate that can reconcile the overlapping captures an
# ordinary crash re-read leaves behind (publish, die before checkpointing, retry against an ADVANCED stable
# boundary) and the only one that can place a record archived under a LATER dt= against its session's earlier
# records. The archiver proves the column before publishing — five fields, this partition, inside [from, boundary),
# strictly increasing — and says so in the manifest ("offsets_verified":true), which is the provenance the loaders
# admit on. vpread.py models the loader side (options-edge-processing vol-premium-service calibration/ArchiveReader
# and calibration/CommittedLedgerArchive): the file-name grammar, the manifest admission, the offset-layout record
# parse, the range and ordering checks, reconciliation of duplicate/conflicting offsets, coverage and the
# queried_end completeness rule across storage dates — and, since deploy #1041 review round 2 / engine r15, the
# manifest INVENTORY rule (a listed file the disk has lost, an excluded capture's queried end, a no-progress ATTEMPT
# line) and the SOURCE IDENTITY rule (17i-17k); since round 4 / engine r17, the whole-window PREFLIGHT (every line
# validated, coordinates from the line, before any file is opened) and the refusal of a file no manifest line names
# (17h, 17n). The shim reader is the one section 12 uses; the real
# program's read_committed behaviour (markers walked, aborted records withheld, an open transaction bounding the read)
# is proven against a real broker by broker-test/strike-reader-broker-test.sh section 1, not here.
VP=options.spx.vol-premium.ivrv
VDAY=2026-09-09; VDAY2=2026-09-10
cat > "$T/vpread.py" <<'PY'
import gzip, json, os, re, sys
# The LOADER side, modelled: options-edge-processing vol-premium-service calibration/ArchiveReader (the file-name
# grammar LEDGER_NAME, and readOffsetLayoutFile's five-field record parse) and calibration/CommittedLedgerArchive
# (manifest admission: capture, record_layout, offsets_verified, escaped_records; the per-record range and ordering
# checks; reconciliation by real offset — identical duplicate vs SAME_OFFSET_CONFLICT; COVERAGE_GAP; and the
# queried_end rule, SESSION_INCOMPLETE, across storage dates). (Named without their file suffix:
# validate-archive-unit-completeness.sh reads a named source file here as one the unit must ship.)
#   usage: vpread.py <topic> <topic dir> <keys|ranges|check|excluded|identity> <dt> [dt ...]
# Round 2 (deploy #1041 review r2 / engine r15): coverage is proved against the MANIFEST INVENTORY, not the files
# on disk — every line declaring a capture (a file, present or not; a no-progress ATTEMPT with no file) carries
# the end its run queried; a listed file the disk has lost inside the needed range is MISSING_CAPTURE_FILE; an
# excluded capture keeps its queried end; and every capture names its SOURCE LOG (source_topic_id) — two logs in
# one window are SOURCE_IDENTITY_MISMATCH.
# Round 3 (deploy #1041 review r3 / engine r16): identity is part of the obligation — a declared line that names
# NO source log refuses the WINDOW (NO_SOURCE_IDENTITY), because an obligation on an unknown log can be discharged
# by no capture, named or not (a named log's offsets are coordinates on THAT log); the declared LOWER bound is
# owed too — the admitted captures must begin at or before the lowest offset any line of the window declares, or
# the prefix is COVERAGE_GAP (an attempt at checkpoint 3 querying 10, then a capture [10,12): [3,10) is in no
# capture); and a line naming BOTH a file and an attempt is malformed (MANIFEST_MISMATCH), never an attempt.
# Round 4 (deploy #1041 review r4 / engine r17): the WHOLE window is preflighted before any capture file is opened —
# every manifest line of every date parsed and validated (shape, coordinates against the file name, identity), every
# file on disk placed against its line, the window's obligations and coverage decided from the declarations — and
# only then are the admitted files read. Every declaration's coordinates are the VALIDATED line's, admitted or not
# (a line contradicting its file name is MANIFEST_MISMATCH before it can set the required beginning). And a file
# with NO manifest line REFUSES the window (UNCOVERED_EXCLUDED_FILE): it names no log, so no capture's offsets are
# coordinates on it — round 3 let it stay "excluded-and-covered" by offsets, and log B's capture "covered" log A's
# crash residue.
# Round 5 (deploy #1041 review r5): a NONBLANK manifest line that is not a JSON object refuses the window
# (MANIFEST_UNPARSEABLE, naming the date and line) — a run that died mid-append leaves a partial attempt line whose
# queried end nobody can read, and rounds 1-4 skipped it (`except ValueError: continue`), admitting the session at
# the earlier capture's end. Blank lines stay nothing. And, level with engine r18 (13309d86): a committed-read line
# — an attempt included — without queried_end is MANIFEST_MISMATCH, never an obligation of merely its checkpoint;
# a non-integral records / escaped_records / stable_boundary (0.5, "bad") is MANIFEST_MISMATCH in the preflight,
# never an exclusion or a truncation to 0.
topic, root, mode, dates = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
NAME = re.compile(r"p(\d{1,9})\.(\d{1,18})-(\d{1,18})\.dt(\d{8})\.(\d{8}T\d{6}Z)\.jsonl\.gz")
LINE = re.compile(rb"^(?:CreateTime|LogAppendTime):\d+\tPartition:(\d+)\tOffset:(\d+)\t")
CAPTURE, LAYOUT = "read_committed_stable_boundary", "timestamp,partition,offset,key,value"

def die(why):
    print(why); sys.exit(0)

def coords(n, dt):
    m = NAME.fullmatch(n[len(topic) + 1:]) if n.startswith(topic + ".") else None
    if not m or m.group(4) != dt.replace("-", "") or int(m.group(3)) <= int(m.group(2)):
        die("not the archiver's name for this topic and storage date: " + n)
    return int(m.group(1)), int(m.group(2)), int(m.group(3))

def num(e, k, n):
    v = e.get(k)
    if not isinstance(v, int) or isinstance(v, bool):
        die("MANIFEST_MISMATCH: %s: its manifest line has no integral %s" % (n, k))
    return v

def obligation(e, to, n):
    # Every COMMITTED-READ line — a file line or an attempt — records the end it queried; an attempt without one
    # would owe only its checkpoint (engine r18). A console line records none and owes its range's own end.
    if "queried_end" not in e:
        if e.get("capture") == CAPTURE:
            die("MANIFEST_MISMATCH: %s: its manifest line has no integral queried_end" % n)
        return to
    q = e["queried_end"]
    if not isinstance(q, int) or isinstance(q, bool) or q < 0:
        die("MANIFEST_MISMATCH: %s: its manifest line's queried_end %r is not an offset" % (n, q))
    return q

# PHASE 1 — the whole-window PREFLIGHT. No capture file is opened here.
admitted, excluded, unproven, declared = [], {}, [], []
for dt in dates:
    d = os.path.join(root, "dt=" + dt)
    if not os.path.isdir(d):
        continue
    manifest, attempts = {}, []
    mf = os.path.join(d, "_manifest.jsonl")
    if os.path.isfile(mf):
        for i, line in enumerate(open(mf), 1):
            if line.strip():
                try:
                    e = json.loads(line)
                except ValueError:
                    e = None
                if not isinstance(e, dict):
                    # Nonblank and not a JSON object: a run that died mid-append, or a hand edit. What it declared
                    # is unknown, and an unknown declaration may be an obligation — never skipped (round 5).
                    die("MANIFEST_UNPARSEABLE: dt=%s/_manifest.jsonl line %d is not a JSON object: %r — a run that died "
                        "mid-append, or a hand edit; what it declared cannot be read" % (dt, i, line.rstrip("\n")[:80]))
                if e.get("file") is not None and e.get("attempt") is not None:
                    die("MANIFEST_MISMATCH: dt=%s/_manifest.jsonl names both a file (%s) and an attempt (%s) on one "
                        "line: the archiver writes no such line, and it is neither" % (dt, e["file"], e["attempt"]))
                if e.get("file") is not None:
                    manifest[e["file"]] = e
                elif e.get("attempt") is not None:
                    attempts.append(e)
    present = {n for n in os.listdir(d) if n.endswith(".jsonl.gz")}
    # The inventory: every declared capture, on disk or not — its coordinates the LINE's, checked against the name
    # before anything is built from them — and every declaration names its log, or the window is refused: an
    # obligation on an unknown log cannot be discharged by a capture of a known one.
    def identity(e, n):
        i = e.get("source_topic_id")
        if not i:
            die("NO_SOURCE_IDENTITY: dt=%s/_manifest.jsonl declares %s without source_topic_id: its obligation is "
                "on a log no capture can be placed on, so nothing in the window can discharge it" % (dt, n))
        return i
    byname = {}
    for n, e in manifest.items():
        part, f, t = coords(n, dt)
        if part != 0:
            die("%s: partition %d — the ledger this rule reads is partition 0" % (n, part))
        lf, lt = num(e, "offset_from", n), num(e, "offset_to", n)
        if (e.get("topic"), e.get("dt"), e.get("partition"), lf, lt) != (topic, dt, 0, f, t):
            die("MANIFEST_MISMATCH: %s: its manifest line (topic, dt, partition, offset_from, offset_to) does not "
                "describe the file it names, [%d,%d) on dt=%s" % (n, f, t, dt))
        # Every integral field's SHAPE is settled here (engine r18): records on every line; a committed read's
        # stable_boundary and escaped_records too — 0.5 is not a count. The record-count COMPARISON needs the file.
        if num(e, "records", n) < 0:
            die("MANIFEST_MISMATCH: %s: its manifest line claims %r records, not a count" % (n, e.get("records")))
        if e.get("capture") == CAPTURE:
            if num(e, "stable_boundary", n) != lt:
                die("MANIFEST_MISMATCH: %s: its manifest line's stable_boundary %r is not the end of the range it "
                    "names, %d" % (n, e.get("stable_boundary"), lt))
            if num(e, "escaped_records", n) < 0:
                die("MANIFEST_MISMATCH: %s: its manifest line's escaped_records %r is not a count" % (n, e.get("escaped_records")))
        c = {"dt": dt, "n": n, "f": lf, "t": lt, "q": obligation(e, lt, n), "id": identity(e, n),
             "present": n in present, "e": e}
        declared.append(c)
        byname[n] = c
    for a in attempts:
        n = "attempt at %s" % a.get("archived_at", "?")
        f = a.get("offset_from")
        if a.get("attempt") != "no_progress" or a.get("capture") != CAPTURE or a.get("partition") != 0 \
           or not isinstance(f, int) or f < 0 or a.get("offset_to") != f or a.get("stable_boundary") != f:
            die("dt=%s: %s is not a no-progress attempt of this topic at its checkpoint: %r" % (dt, n, a))
        if num(a, "records", n) != 0:
            die("MANIFEST_MISMATCH: %s: a no-progress attempt captured nothing, not %r records" % (n, a.get("records")))
        # obligation() REQUIRES an attempt's queried_end (engine r18): without it the attempt owed its checkpoint.
        declared.append({"dt": dt, "n": n, "f": f, "t": f, "q": obligation(a, f, n),
                         "id": identity(a, n), "present": True, "e": a})
    for n in sorted(present):
        part, f, t = coords(n, dt)
        if part != 0:
            die("%s: partition %d — the ledger this rule reads is partition 0" % (n, part))
        c = byname.get(n)
        if c is None:
            # No line names it: it declares nothing and names no log, so the numbers in its name are not
            # coordinates on any log the window's captures are on, and no admitted capture can cover it.
            excluded["NO_MANIFEST_LINE"] = excluded.get("NO_MANIFEST_LINE", 0) + 1
            die("UNCOVERED_EXCLUDED_FILE: %s [%d,%d) has no manifest line on dt=%s: it declares nothing and names no "
                "source log, so no admitted capture can cover it — crash residue or a console-era file, resolved by "
                "an operator, never by offsets" % (n, f, t, dt))
        e = c["e"]
        why = ("NOT_COMMITTED_READ" if e.get("capture") != CAPTURE else
               "NO_RECORD_OFFSETS" if (e.get("record_layout") != LAYOUT or e.get("offsets_verified") is not True) else
               "ESCAPED_RECORDS" if e.get("escaped_records") != 0 else None)
        if why:
            excluded[why] = excluded.get(why, 0) + 1
            unproven.append(c)
            continue
        admitted.append(c)

admitted.sort(key=lambda c: (c["f"], c["t"], c["dt"], c["n"]))
# PHASE 2 — the window's obligations, from the validated declarations alone; still nothing opened.
# The session's obligation: the end the EARLIEST declaring date's runs meant to reach, admitted or not — and the
# lowest offset ANY line of the window declares, which the admitted captures must begin at or before.
need = need_from = first = None
if declared:
    first = min(c["dt"] for c in declared)
    need = max(c["q"] for c in declared if c["dt"] == first)
    need_from = min(c["f"] for c in declared)
# One source log across the window (every declaration names one, or the window was refused above).
logs = {}
for c in declared:
    logs.setdefault(c["id"], c["n"])
if len(logs) > 1:
    die("SOURCE_IDENTITY_MISMATCH: " + " and ".join("%s from log %s" % (n, i) for i, n in logs.items())
        + " — the topic was re-created between them; offsets on one log say nothing about the other")
# Every capture the manifest claims inside the needed range must be on disk.
for c in declared:
    if not c["present"] and c["t"] > c["f"] and c["f"] < need and c["t"] > need_from:
        die("MISSING_CAPTURE_FILE: %s [%d,%d) is named by dt=%s's manifest but is not on disk" % (c["n"], c["f"], c["t"], c["dt"]))
# Coverage, from the declared ranges: no gap, every unproven (lined) range covered on its log, the prefix and the end.
start, reach = None, None
for c in admitted:
    if reach is None:
        start = c["f"]
    elif c["f"] > reach:
        die("COVERAGE_GAP: offsets [%d,%d) are in no committed capture" % (reach, c["f"]))
    reach = max(reach or 0, c["t"])
for c in unproven:
    if reach is None or c["f"] < start or c["t"] > reach:
        die("UNCOVERED_EXCLUDED_FILE: %s [%d,%d) is not proven and no capture covers it" % (c["n"], c["f"], c["t"]))
# The declared PREFIX is owed too: the window's declarations begin at need_from, and the first admitted capture
# must begin there or below — else [need_from, start) is in no capture, and no later capture will ever hold it.
if need is not None and need_from < need and reach is not None and start > need_from:
    die("COVERAGE_GAP: offsets [%d,%d) are in no committed capture — the window's declarations begin at %d "
        "but the first admitted capture begins at %d" % (need_from, start, need_from, start))
if need is not None and (reach is None or reach < need):
    die("SESSION_INCOMPLETE: dt=%s queried up to %s, committed captures reach only %s"
        % (first, need, reach if reach is not None else "nothing"))
# PHASE 3 — the records. Only now is a capture file opened: the manifest had nothing to refuse.
for c in admitted:
    n, f, t, e = c["n"], c["f"], c["t"], c["e"]
    data = gzip.open(os.path.join(root, "dt=" + c["dt"], n)).read()
    lines = data[:-1].split(b"\n") if data.endswith(b"\n") else (data.split(b"\n") if data else [])
    records, prev = [], None
    for i, l in enumerate(lines):
        h = LINE.match(l)
        parts = l.split(b"\t")
        if not h or len(parts) != 5 or b"\r" in l:
            die("%s: record %d is not <ts>\\tPartition:<p>\\tOffset:<o>\\t<key>\\t<value>: %r" % (n, i, l[:80]))
        if int(h.group(1)) != 0:
            die("%s: record %d is of partition %s" % (n, i, h.group(1).decode()))
        o = int(h.group(2))
        if o < f or o >= t:
            die("%s: record %d is offset %d, outside its range [%d,%d)" % (n, i, o, f, t))
        if prev is not None and o <= prev:
            die("%s: record %d is offset %d, not after %d" % (n, i, o, prev))
        prev = o
        try:
            json.loads(parts[4].decode("utf-8"))
        except Exception:
            die("%s: record %d payload is not a JSON document: %r" % (n, i, parts[4][:80]))
        records.append((o, parts[3].decode("utf-8"), parts[4]))
    if e.get("records") != len(records):
        die("%s: holds %d records, its manifest line claims %s" % (n, len(records), e.get("records")))
    c["records"] = records
by_offset, duplicates = {}, 0
for c in admitted:
    for o, k, v in c["records"]:
        if o not in by_offset:
            by_offset[o] = (k, v, c["n"])
        elif by_offset[o][:2] == (k, v):
            duplicates += 1
        else:
            die("SAME_OFFSET_CONFLICT: %s and %s hold different records at offset %d" % (by_offset[o][2], c["n"], o))
for a in range(len(admitted)):
    for b in range(a + 1, len(admitted)):
        x, y = admitted[a], admitted[b]
        lo, hi = max(x["f"], y["f"]), min(x["t"], y["t"])
        if lo < hi and ([o for o, _, _ in x["records"] if lo <= o < hi]
                        != [o for o, _, _ in y["records"] if lo <= o < hi]):
            die("OVERLAP_PRESENCE_CONFLICT: %s and %s disagree on [%d,%d)" % (x["n"], y["n"], lo, hi))
if mode == "keys":
    print(" ".join(by_offset[o][0] for o in sorted(by_offset)))
elif mode == "ranges":
    print(" ".join("%d-%d" % (c["f"], c["t"]) for c in admitted))
elif mode == "excluded":
    print(" ".join("%s=%d" % (k, v) for k, v in sorted(excluded.items())) or "none")
elif mode == "identity":
    print(" ".join(sorted(logs)) or "none")
else:
    print("ok %d duplicates=%d" % (len(by_offset), duplicates))
PY
vrun() { # $1 = session date; the rest = extra env assignments. ENV=prod: vol-premium runs on dev and production only.
  local day="$1"; shift
  env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-vp BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" TOPICS="${VTOPICS:-$VP}" \
      SESSION_DATE="$day" ALLOW_NON_NAS=true STRIKE_READER="$BIN/strike-reader" "$@" "$ARCH" 2>&1
}
VDIR() { echo "$A/kafka/prod/$VP/dt=${1:-$VDAY}"; }
# The loader model over ONE storage date (its own), and over a SESSION's window of storage dates.
vread() { python3 "$T/vpread.py" "$VP" "$A/kafka/prod/$VP" "$1" "${2:-$VDAY}" 2>&1; }
vsession() { local mode="$1"; shift; python3 "$T/vpread.py" "$VP" "$A/kafka/prod/$VP" "$mode" "$@" 2>&1; }
vz() { local f; for f in "$(VDIR "${1:-$VDAY}")"/*.jsonl.gz; do [ -f "$f" ] && zcat "$f"; done 2>/dev/null; }
vck() { awk '{split($1,a,"="); if (a[1]=="0") print a[2]}' "$A/kafka/prod/_manifest/$VP.offsets" 2>/dev/null | tail -1; }
vm() { # <field> of the LAST manifest line of date $2 (default VDAY)
  python3 -c 'import json,sys
lines=[l for l in open(sys.argv[1]) if l.strip()]
print(json.loads(lines[-1]).get(sys.argv[2], "<absent>") if lines else "<no-manifest>")' "$(VDIR "${2:-$VDAY}")/_manifest.jsonl" "$1" 2>/dev/null || echo "<no-manifest>"; }
vchain() { # every manifest range of the given dates, in order, and the verifier's continuity verdict across them
  local d m=""; for d in "$@"; do m="$m $(VDIR "$d")/_manifest.jsonl"; done
  python3 -c 'import json,sys
r=sorted((int(e["offset_from"]),int(e["offset_to"])) for p in sys.argv[1:] for e in map(json.loads,filter(str.strip,open(p))))
bad=[f"{a[1]}->{b[0]}" for a,b in zip(r,r[1:]) if b[0]!=a[1]]
print(" ".join(f"{a}-{b}" for a,b in r), "broken:"+",".join(bad) if bad else "contiguous")' $m 2>/dev/null; }
vfiles() { ls "$(VDIR "${1:-$VDAY}")"/*.jsonl.gz 2>/dev/null | wc -l | tr -d ' '; }
# The ranges a date's file NAMES claim, and the keys its files hold, read without the loader model.
vnames() { ls "$(VDIR "${1:-$VDAY}")"/*.jsonl.gz 2>/dev/null | sed -E 's/.*\.p0\.([0-9]+-[0-9]+)\..*/\1/' \
             | tr '\n' ' ' | sed 's/ *$//'; }
vkeys() { vz "${1:-$VDAY}" | awk -F'\t' '{printf "%s%s", (NR>1?" ":""), $4}'; }
K1='SPX|2026-09-09|1'; K2='SPX|2026-09-09|2'; K3='SPX|2026-09-09|3'; K4='SPX|2026-09-09|4'
ABORTED_LOG=("0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
             "3 A $K3 {\"frameSeq\":3,\"v\":\"ABORTED\"}" "4 A $K4 {\"frameSeq\":4,\"v\":\"ABORTED\"}" '5 M'
             "6 C $K3 {\"frameSeq\":3,\"v\":\"c\"}" '7 M')

# ---- 17z. THE DEFECT, on main's routing: the console consumer archives the ABORTED frames --------------------------
# A console-consumer stand-in over the same transactional log, with the real tool's semantics: read_uncommitted (its
# default) returns every DATA record — committed, aborted and still-open alike; control markers are never returned;
# --max-messages counts records; then it idles out and exits 0.
BIN2="$T/bin2"; rm -rf "$BIN2"; cp -R "$BIN" "$BIN2"
cat > "$BIN2/kafka-console-consumer.sh" <<'SH'
#!/usr/bin/env bash
part=""; off=0; maxm=0; iso=read_uncommitted
while [ $# -gt 0 ]; do
  case "$1" in --partition) part="$2"; shift 2 ;; --offset) off="$2"; shift 2 ;; --max-messages) maxm="$2"; shift 2 ;;
    --isolation-level) iso="$2"; shift 2 ;; *) shift ;; esac
done
awk -v f="$off" -v m="$maxm" -v p="$part" -v iso="$iso" '
  iso == "read_committed" && $2 == "O" { exit }
  $1 >= f && ($2 == "C" || (iso != "read_committed" && ($2 == "A" || $2 == "O"))) {
    if (n >= m) exit
    printf "CreateTime:1786000000000\tPartition:%s\t%s\t%s\n", p, $3, $4; n++ }' "$OE_FIXTURE.strike"
SH
chmod +x "$BIN2/kafka-console-consumer.sh"
fresh v0
strike_log "${ABORTED_LOG[@]}"
OUT=$(env ARCHIVE_DIR="$A" ENV=prod ARCHIVE_JOB=test-vp BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN2" TOPICS="$VP" \
      SESSION_DATE="$VDAY" ALLOW_NON_NAS=true OE_COMMITTED_READ_TOPICS=es.futures.footprint.strike "$ARCH" 2>&1); RC=$?
want "17z main's routing (console consumer, read_uncommitted): the run 'succeeds' (rc)" 0 "$RC"
want "  and archives BOTH aborted frames as if the engine had published them" 2 "$(vz | grep -c ABORTED)"
want "  so frameSeq 3 appears twice: the aborted revision and the committed one" 2 "$(vz | grep -c '"frameSeq":3,')"

# ---- 17a. an ABORTED batch: never archived; the committed records WITH THEIR OFFSETS --------------------------------
fresh v1
strike_log "${ABORTED_LOG[@]}"
OUT=$(vrun "$VDAY"); RC=$?
want "17a ivrv with an ABORTED batch: the run succeeds (rc)"          0 "$RC"
want "  read by the committed reader, from the log start"              1 "$(grep -c "^reader $VP p0 from=0 " "$CALLS")"
want "  never by the console consumer"                                 0 "$(grep -c "^console $VP " "$CALLS")"
want "  NO aborted record is archived"                                 0 "$(vz | grep -c ABORTED)"
want "  exactly the committed records, in offset order"     "$K1 $K2 $K3" "$(vread keys)"
want "  each record keeps its REAL Kafka offset — 0, 1 and 6, the aborted ones' offsets never reused" \
     "$(printf 'CreateTime:1786000000000\tPartition:0\tOffset:0\t%s\t{"frameSeq":1,"v":"c"}\nCreateTime:1786000000000\tPartition:0\tOffset:1\t%s\t{"frameSeq":2,"v":"c"}\nCreateTime:1786000000000\tPartition:0\tOffset:6\t%s\t{"frameSeq":3,"v":"c"}' "$K1" "$K2" "$K3")" \
     "$(vz)"
want "  the Offset column is on every record"                          3 "$(vz | grep -c $'\tOffset:')"
want "  every record reads as ArchiveReader + CommittedLedgerArchive read it" "ok 3 duplicates=0" "$(vread check)"
want "  one file, named for [0,8): markers included, fewer records than offsets" "0-8" "$(vread ranges)"
want "  manifest: 3 records over 8 offsets"                         "3 8" "$(vm records) $(vm offset_span)"
want "  manifest offset_to = stable boundary = queried end"       "8 8 8" "$(vm offset_to) $(vm stable_boundary) $(vm queried_end)"
want "  manifest names the capture, the layout and the proof" \
     "read_committed_stable_boundary timestamp,partition,offset,key,value True" \
     "$(vm capture) $(vm record_layout) $(vm offsets_verified)"
want "  checkpoint 8, stamped with the source TopicId" 1 \
     "$(grep -c "^0=8 records=3 span=8 dt=$VDAY archived=[0-9TZ]* capture=read_committed_stable_boundary topic_id=SSSSSSSSSSSSSSSSSSSSSS$" "$A/kafka/prod/_manifest/$VP.offsets")"
want "  no archive marker on prod"                                     0 "$(grep -c 'mark=[0-9]' "$CALLS")"
hasnt "  and no withheld-range note: nothing was open" "held by a transaction unresolved" "$OUT"

# ---- 17b. CONTROL MARKERS: a range of markers and aborted records only still completes, and the range is right ------
strike_log "${ABORTED_LOG[@]}" "8 A SPX|2026-09-09|9 {\"frameSeq\":9,\"v\":\"ABORTED\"}" '9 M' '10 M'
OUT=$(vrun "$VDAY"); RC=$?
want "17b a markers/aborted-only range: the run succeeds (rc)"       0 "$RC"
want "  completion reaches the boundary: checkpoint 11"               11 "$(vck)"
want "  published as a ZERO-record file for [8,11)"             "0-8 8-11" "$(vread ranges)"
want "  its manifest line: 0 records over 3 offsets"                "0 3" "$(vm records) $(vm offset_span)"
want "  the loader reads the empty file as zero records"  "ok 3 duplicates=0" "$(vread check)"
want "  the date's ranges chain"                "0-8 8-11 contiguous" "$(vchain "$VDAY")"
hasnt "  and it is not a failure" "failed=1" "$OUT"

# ---- 17c. an OPEN transaction at archive time: bounded, withheld, NOT claimed, and captured once it resolves ---------
fresh v3
OPEN_LOG=("0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
          "3 O $K3 {\"frameSeq\":3,\"v\":\"open\"}" "4 O $K4 {\"frameSeq\":4,\"v\":\"open\"}")
strike_log "${OPEN_LOG[@]}"
OUT=$(vrun "$VDAY"); RC=$?
want "17c OPEN transaction at archive time: the run succeeds (rc)"   0 "$RC"
want "  the read is deadline-bounded (STRIKE_READER_DEADLINE_S), not an idle wait" 1 \
     "$(grep -c "^reader $VP p0 from=0 .* deadline_ms=900000$" "$CALLS")"
want "  checkpoint = the stable boundary 3, NOT the high-water mark 5"  3 "$(vck)"
want "  the file claims [0,3) only"                                "0-3" "$(vnames)"
want "  only the committed records below it"                    "$K1 $K2" "$(vkeys)"
want "  nothing of the open transaction"                               0 "$(vz | grep -c '"open"')"
want "  the manifest says the capture stopped short of the queried end" "3 5" "$(vm stable_boundary) $(vm queried_end)"
has  "  and the run names the withheld offsets" "offsets [3,5) are held by a transaction unresolved at capture time" "$OUT"
has  "  and says where a session loader will find them" "refuses the session until a capture reaches 5" "$OUT"
want "  not a failure"                                                 0 "$(runs failed)"
# THE SESSION, meanwhile, is NOT loadable: offsets [3,5) are this session's and are archived nowhere yet, so a
# loader reading dt=2026-09-09 refuses it rather than calibrating on a session it knows is short (deploy #1041 MAJOR 2).
has  "  a session loader REFUSES this date until they are captured" \
     "SESSION_INCOMPLETE: dt=$VDAY queried up to 5, committed captures reach only 3" "$(vread keys)"
# The verifier says the same thing about the DATE without calling it a fault: the date holds everything it claimed.
VVD="$T/vnas-vp"; rm -rf "$VVD"; mkdir -p "$VVD/kafka/prod"
cp -R "$A/kafka/prod/$VP" "$VVD/kafka/prod/$VP"
printf 'OE_ALL_TOPICS_prod="%s"\nOE_ARCHIVE_MIN_RECORDS=""\n' "$VP" > "$T/vp.topics.env"
OUT_V=$(env ARCHIVE_DIR="$VVD" ENV=prod FORCE=true VERIFY_CHECKSUMS=none LOG="$T/verify-vp.log" \
            OE_TOPICS_ENV="$T/vp.topics.env" bash "$OE/oe-archive-verify.sh" "$VDAY" 2>&1); RC_V=$?
want "  the date itself verifies COMPLETE: it holds every offset it claimed (rc)" 0 "$RC_V"
has  "  and the verifier names the withheld range" "open transaction withheld p0 [3,5)" "$OUT_V"
has  "  saying a session that needs it is refused, not loaded short" "never loaded short" "$OUT_V"
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 C $K3 {\"frameSeq\":3,\"v\":\"c\"}" "4 C $K4 {\"frameSeq\":4,\"v\":\"c\"}" '5 M'
OUT=$(vrun "$VDAY2"); RC=$?
want "  it COMMITS: the next run succeeds (rc)"                        0 "$RC"
want "  starting at the boundary"                                      1 "$(grep -c "^reader $VP p0 from=3 " "$CALLS")"
want "  capturing the withheld records, under ITS date"         "$K3 $K4" "$(vkeys "$VDAY2")"
want "  the two dates chain with no gap and no overlap" "0-3 3-6 contiguous" "$(vchain "$VDAY" "$VDAY2")"
want "  checkpoint past the transaction"                               6 "$(vck)"
# THE WHOLE POINT (deploy #1041 MAJOR 2): the records of session 2026-09-09 that landed under dt=2026-09-10 are
# still that session's. A loader reading the session's window of storage dates finds all four, in offset order,
# and the session is complete — the queried end 5 of its own date's capture is now covered.
want "  the SESSION, read across its storage dates, holds every record — the delayed ones included" \
     "$K1 $K2 $K3 $K4" "$(vsession keys "$VDAY" "$VDAY2")"
want "  its captures chain and are all proven"       "0-3 3-6" "$(vsession ranges "$VDAY" "$VDAY2")"
want "  four committed records, no duplicate"  "ok 4 duplicates=0" "$(vsession check "$VDAY" "$VDAY2")"
want "  nothing excluded"                               "none" "$(vsession excluded "$VDAY" "$VDAY2")"
fresh v3b
strike_log "${OPEN_LOG[@]}"
vrun "$VDAY" >/dev/null
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 A $K3 {\"frameSeq\":3,\"v\":\"open\"}" "4 A $K4 {\"frameSeq\":4,\"v\":\"open\"}" '5 M'
OUT=$(vrun "$VDAY2"); RC=$?
want "  it ABORTS instead: the next run succeeds (rc)"                 0 "$RC"
want "  and archives none of it: its [3,6) file holds zero records" "3-6 ok 0 duplicates=0" \
     "$(vnames "$VDAY2") $(vread check "$VDAY2")"
want "  no record of the aborted transaction anywhere" 0 "$( { vz "$VDAY"; vz "$VDAY2"; } | grep -c '"open"')"
want "  and the session, read across both dates, is complete on its two committed records" \
     "$K1 $K2" "$(vsession keys "$VDAY" "$VDAY2")"

# ---- 17d. a NON-transactional topic: unchanged — console consumer, read_uncommitted, the same bytes and lines ------
fresh v4
fixture "topicid OOOOOOOOOOOOOOOOOOOOOO" "0 0 40" "1 0 40"
OUT=$(run STRIKE_READER="$BIN/strike-reader"); RC=$?
want "17d a non-transactional topic: the run succeeds (rc)"          0 "$RC"
want "  read by the console consumer, both partitions, with no isolation level and no property" 2 \
     "$(grep -c "^console $TOPIC p[01] props=\[\]$" "$CALLS")"
want "  never by the committed reader"                                 0 "$(grep -c '^reader ' "$CALLS")"
last_plain=$(tail -1 "$A/kafka/prod/$TOPIC"/dt=*/_manifest.jsonl 2>/dev/null)
hasnt "  its manifest line has no committed-read field" '"capture"' "$last_plain"
hasnt "  (nor the new ones)" '"queried_end"' "$last_plain"
has  "  and ends exactly as it always did" "\"archiver_version\":\"2026-08-13.1\"}" "$last_plain"
want "  its checkpoint lines are unchanged, no stamp" 2 \
     "$(grep -cE '^[01]=40 records=40 span=40 dt=[0-9-]+ archived=[0-9]{8}T[0-9]{6}Z$' "$A/kafka/prod/_manifest/$TOPIC.offsets")"
want "  its file holds exactly what the console consumer printed" \
     "$("$BIN/kafka-console-consumer.sh" --topic "$TOPIC" --partition 0 --offset 0 --max-messages 40)" \
     "$(zcat "$A/kafka/prod/$TOPIC"/dt=*/"$TOPIC".p0.0-40.*.jsonl.gz)"

# ---- 17e. the layout proof refuses a reader whose file is not the range it names -----------------------------------
for mode in escaped stray unordered; do
  fresh "v5-$mode"
  strike_log "0 C $K1 {\"frameSeq\":1}" "1 C $K2 {\"frameSeq\":2}" '2 M'
  OUT=$(vrun "$VDAY" STRIKE_SHIM_MODE=$mode); RC=$?
  case "$mode" in
    escaped)   why="the reader escaped a TAB/CR/LF in 1 record(s)" ;;
    stray)     why="line 3 is offset 3, outside the captured range [0,3)" ;;
    unordered) why="line 2 is offset 0, not after the previous line (1)" ;;
  esac
  want "17e reader '$mode' (exit 0, COMPLETE) on a PROVEN-layout topic: the run FAILS (rc)" 1 "$RC"
  has  "  refused, saying why" "committed-read capture REFUSED ($why" "$OUT"
  want "  checkpoint unchanged"                                       "" "$(vck)"
  want "  nothing published, no manifest line"                     "0 0" "$(vfiles) $(cat "$(VDIR)/_manifest.jsonl" 2>/dev/null | grep -c .)"
  OUT=$(vrun "$VDAY"); RC=$?
  want "  the next healthy run captures the range"                 "0-3" "$(vread ranges)"
done
fresh v6
strike_log '0 C k0 v0' '1 C k1 v1' '2 M'
OUT=$(srun STRIKE_SHIM_MODE=escaped); RC=$?
want "17e the STRIKE log (filed AS WRITTEN) with an escaped record: accepted as before (rc)" 0 "$RC"
has  "  and noted as before" "record(s) carried a raw TAB/CR/LF, written escaped" "$OUT"
want "  its file keeps the Offset column"                              2 "$(zcat "$(SDIR)"/*.jsonl.gz | grep -c $'\tOffset:')"
want "  its manifest names the layout, and does NOT claim the offsets were proven" \
     "timestamp,partition,offset,key,value False" "$(mlast record_layout) $(mlast offsets_verified)"

# ---- 17g. the CRASH RE-READ (deploy #1041 MAJOR 1): publish, die before checkpointing, retry past an ADVANCED -------
# stable boundary. The two captures OVERLAP, which is exactly what the offsets are for: a loader keys records by
# their real offset, reads the repeated ones once, and places the records the first capture never saw.
fresh v8
strike_log "${OPEN_LOG[@]}"
vrun "$VDAY" > /dev/null
want "17g the first capture published [0,3)"                       "0-3" "$(vnames)"
rm -f "$A/kafka/prod/_manifest/$VP.offsets"     # the CRASH: the file and its manifest line are there, the checkpoint is not
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 C $K3 {\"frameSeq\":3,\"v\":\"c\"}" "4 C $K4 {\"frameSeq\":4,\"v\":\"c\"}" '5 M'
OUT=$(vrun "$VDAY"); RC=$?
want "  the retry re-reads from 0 and succeeds (rc)"                   0 "$RC"
want "  against the ADVANCED boundary: two overlapping captures on the date" "0-3 0-6" "$(vnames)"
want "  the loader reconciles them by offset: every committed record once" \
     "$K1 $K2 $K3 $K4" "$(vread keys)"
want "  the repeated offsets are counted as duplicates, not as records" "ok 4 duplicates=2" "$(vread check)"
want "  nothing excluded"                                         "none" "$(vread excluded)"

# ---- 17h. files a loader must NOT take on trust: no manifest line, a stripped layout, a conflicting record ---------
vfake() { # <name> <dt> <mode: legacy|stripped|conflict> <from> <to> <offset:key:value>...
  local name="$1" dt="$2" fmode="$3" f="$4" t="$5"; shift 5
  python3 - "$(VDIR "$dt")" "$name" "$fmode" "$f" "$t" "$VP" "$dt" "$@" <<'PY'
import gzip, json, os, sys
d, name, mode, f, t, topic, dt = sys.argv[1:8]
os.makedirs(d, exist_ok=True)
recs = [r.split(":", 2) for r in sys.argv[8:]]
with gzip.open(os.path.join(d, name), "wb") as out:
    for o, k, v in recs:
        head = "CreateTime:1786000000000\tPartition:0\t" + ("" if mode == "legacy" else "Offset:%s\t" % o)
        out.write((head + k + "\t" + v + "\n").encode())
if mode != "legacy":
    line = {"topic": topic, "dt": dt, "partition": 0, "offset_from": int(f), "offset_to": int(t),
            "records": len(recs), "capture": "read_committed_stable_boundary", "stable_boundary": int(t),
            "queried_end": int(os.environ.get("VFAKE_QEND", t)), "record_layout": "timestamp,partition,offset,key,value",
            "offsets_verified": mode != "stripped", "escaped_records": 0,
            "source_topic_id": os.environ.get("VFAKE_ID", "SSSSSSSSSSSSSSSSSSSSSS"), "file": name}
    if not line["source_topic_id"]:
        del line["source_topic_id"]
    if mode == "stripped":
        line["record_layout"] = "timestamp,partition,key,value"
    with open(os.path.join(d, "_manifest.jsonl"), "a") as m:
        m.write(json.dumps(line) + "\n")
PY
}
LEG="$VP.p0.0-3.dt20260909.20260909T220000Z.jsonl.gz"
vfake "$LEG" "$VDAY" legacy 0 3 "0:$K1:{\"frameSeq\":1,\"v\":\"legacy\"}" "1:$K2:{\"frameSeq\":2,\"v\":\"legacy\"}"
# Round 4 (deploy #1041 r4 MAJOR 1): a file with NO manifest line REFUSES the window — it names no log, so the
# committed captures' offsets are not coordinates on it and cover nothing of it. Rounds 1-3 read the date on the
# committed captures alone with the file "excluded-and-covered"; 17n has the crash residue that made that wrong.
has  "17h a file with NO manifest line REFUSES the window: it names no log, so no capture's offsets cover it" \
     "UNCOVERED_EXCLUDED_FILE: $LEG [0,3) has no manifest line on dt=$VDAY" "$(vread keys)"
has  "  saying why"                                        "resolved by an operator, never by offsets" "$(vread keys)"
want "  nothing of it in the observations"                             0 "$(vread keys | grep -c legacy)"
rm -f "$(VDIR)/$LEG"
want "  the file removed by an operator: the committed captures alone answer" "$K1 $K2 $K3 $K4" "$(vread keys)"
STRIP="$VP.p0.0-3.dt20260909.20260909T230000Z.jsonl.gz"
vfake "$STRIP" "$VDAY" stripped 0 3 "0:$K1:{\"frameSeq\":1,\"v\":\"stripped\"}"
want "  a committed capture whose offsets were STRIPPED is excluded too" "NO_RECORD_OFFSETS=1" "$(vread excluded)"
rm -f "$(VDIR)/$STRIP"
grep -v -e "$STRIP" "$(VDIR)/_manifest.jsonl" > "$(VDIR)/_manifest.tmp" && mv "$(VDIR)/_manifest.tmp" "$(VDIR)/_manifest.jsonl"
UNCOV="$VP.p0.6-9.dt20260909.20260909T233000Z.jsonl.gz"
vfake "$UNCOV" "$VDAY" legacy 6 9 "6:$K1:{\"frameSeq\":7,\"v\":\"legacy\"}"
has  "  an excluded file NO capture covers refuses the session" "UNCOVERED_EXCLUDED_FILE: $UNCOV" "$(vread keys)"
rm -f "$(VDIR)/$UNCOV"
CONF="$VP.p0.0-6.dt20260909.20260909T234500Z.jsonl.gz"
vfake "$CONF" "$VDAY" ok 0 6 "0:$K1:{\"frameSeq\":1,\"v\":\"OTHER\"}" "1:$K2:{\"frameSeq\":2,\"v\":\"c\"}" \
      "3:$K3:{\"frameSeq\":3,\"v\":\"c\"}" "4:$K4:{\"frameSeq\":4,\"v\":\"c\"}"
has  "  two captures with DIFFERENT records at one offset refuse the session" \
     "SAME_OFFSET_CONFLICT" "$(vread keys)"
has  "  naming the offset"                                    "at offset 0" "$(vread keys)"

# ---- 17i. SOURCE IDENTITY (deploy #1041 r2 / engine r15 MAJOR): every capture names the log it was read from, and a ----
# loader never combines two logs. An offset is a coordinate on ONE log: a topic deleted and re-created is a new log
# under the old name, its offsets restart, and a capture of the new log can neither discharge what the old log
# withheld nor take precedence over the old log's records. The manifest's source_topic_id is Kafka's TopicId, the
# one the validated discovery (section 16) read for THIS capture. What it proves: two captures naming one id are
# coordinates on one log. What it does not prove: what that log held below its earliest retained offset — which is
# why coverage is still proved separately, above.
fresh v9
strike_log "${OPEN_LOG[@]}"
OUT=$(vrun "$VDAY"); RC=$?
want "17i a vol-premium capture names its source log in the manifest (rc, id)" "0 $SID" "$RC $(vm source_topic_id)"
STRIKE_ID=$TID strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"new\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"new\"}" '2 M' \
                          "3 C $K3 {\"frameSeq\":3,\"v\":\"new\"}" "4 C $K4 {\"frameSeq\":4,\"v\":\"new\"}" '5 M'
OUT=$(vrun "$VDAY2"); RC=$?
want "  the topic DELETED and RE-CREATED, the new log grown to 6: the next run succeeds (rc)" 0 "$RC"
has  "  seeing the reset"                                          "RESET $VP p0" "$OUT"
want "  and capturing the new log from ITS start, under its date"           "0-6" "$(vnames "$VDAY2")"
want "  naming the NEW log"                                                 "$TID" "$(vm source_topic_id "$VDAY2")"
has  "  the SESSION read across both dates is REFUSED: two logs" "SOURCE_IDENTITY_MISMATCH" "$(vsession keys "$VDAY" "$VDAY2")"
has  "  naming the old log"                                        "from log $SID" "$(vsession keys "$VDAY" "$VDAY2")"
has  "  and the new"                                               "from log $TID" "$(vsession keys "$VDAY" "$VDAY2")"
want "  the old log's withheld [3,5) is NOT discharged by the new log's offsets: dt=$VDAY alone is still incomplete" \
     "SESSION_INCOMPLETE: dt=$VDAY queried up to 5, committed captures reach only 3" "$(vread keys)"
want "  the new log's own date reads whole on its own"          "$K1 $K2 $K3 $K4" "$(vread keys "$VDAY2")"
# A declaration that names NO source log REFUSES the window (deploy #1041 r3 / engine r16 MAJOR 1). Round 2 excluded
# such a capture and let a named capture's offsets cover its range and discharge its queried end — but a named
# capture's offsets are coordinates on ITS log, and prove nothing about a log nobody can name: the unnamed capture
# may be the previous incarnation's, its withheld range lost with it. Identity is part of the obligation.
vattempt() { # <dt> <checkpoint> <queried_end>: a no-progress ATTEMPT line, naming $VFAKE_ID (unset = $SID; empty = none)
  python3 - "$(VDIR "$1")" "$VP" "$1" "$2" "$3" <<'PY'
import json, os, sys
d, topic, dt, ck, q = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
os.makedirs(d, exist_ok=True)
line = {"topic": topic, "dt": dt, "partition": 0, "offset_from": ck, "offset_to": ck, "records": 0, "offset_span": 0,
        "capture": "read_committed_stable_boundary", "stable_boundary": ck, "queried_end": q,
        "source_topic_id": os.environ.get("VFAKE_ID", "SSSSSSSSSSSSSSSSSSSSSS"), "attempt": "no_progress",
        "archived_at": "20260909T221500Z"}
if not line["source_topic_id"]:
    del line["source_topic_id"]
with open(os.path.join(d, "_manifest.jsonl"), "a") as m:
    m.write(json.dumps(line) + "\n")
PY
}
fresh v9b
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
want "  the loader model reads ONE log across a complete window"          "$SID" "$(vread identity)"
NOID="$VP.p0.0-3.dt20260909.20260909T230000Z.jsonl.gz"
VFAKE_ID= vfake "$NOID" "$VDAY" ok 0 3 "0:$K1:{\"frameSeq\":1,\"v\":\"unnamed\"}"
has  "  a committed capture naming NO source log REFUSES the window (r3): its obligation is on a log nobody can name" \
     "NO_SOURCE_IDENTITY: dt=$VDAY/_manifest.jsonl declares $NOID without source_topic_id" "$(vread keys)"
has  "  saying why"                              "nothing in the window can discharge it" "$(vread keys)"
# Codex r3 UNNAMED_LOG_SATISFIED_BY_NAMED_LOG: dt=D's UNNAMED [0,3) queried 9; dt=D+1 names log $TID's [0,10). Round 2
# excluded the first, took B's [0,10) as covering [0,9), and admitted the session on B's record alone.
fresh v9c
NOIDB="$VP.p0.0-10.dt20260910.20260910T220000Z.jsonl.gz"
VFAKE_QEND=9 VFAKE_ID= vfake "$NOID" "$VDAY" ok 0 3 "0:$K1:{\"frameSeq\":1,\"v\":\"unnamed\"}"
VFAKE_ID=$TID vfake "$NOIDB" "$VDAY2" ok 0 10 "4:$K2:{\"frameSeq\":2,\"v\":\"B\"}"
has  "  Codex r3: an UNNAMED dt=$VDAY capture querying 9, then log $TID's [0,10) on dt=$VDAY2 — REFUSED, B's offsets cover nothing of it" \
     "NO_SOURCE_IDENTITY: dt=$VDAY/_manifest.jsonl declares $NOID" "$(vsession keys "$VDAY" "$VDAY2")"
# Codex r3 UNNAMED_ATTEMPT_SATISFIED_BY_NAMED_LOG: an UNNAMED attempt at 3 querying 9, then a NAMED [3,9). Round 2
# had no exclusion to count and admitted it.
fresh v9d
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
VFAKE_ID= vattempt "$VDAY" 3 9
vfake "$VP.p0.3-9.dt20260910.20260910T220000Z.jsonl.gz" "$VDAY2" ok 3 9 "3:$K3:{\"frameSeq\":3,\"v\":\"c\"}"
has  "  Codex r3: an UNNAMED attempt at 3 querying 9, then a NAMED [3,9) — REFUSED, the attempt's log is unknown" \
     "NO_SOURCE_IDENTITY: dt=$VDAY/_manifest.jsonl declares attempt at 20260909T221500Z without source_topic_id" \
     "$(vsession keys "$VDAY" "$VDAY2")"
# The control: the SAME attempt naming its log, then [3,9) on that log: whole.
fresh v9e
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
vattempt "$VDAY" 3 9
vfake "$VP.p0.3-9.dt20260910.20260910T220000Z.jsonl.gz" "$VDAY2" ok 3 9 "3:$K3:{\"frameSeq\":3,\"v\":\"c\"}"
want "  the control: the same attempt NAMING its log, then [3,9) on that log — whole" "$K1 $K2 $K3" "$(vsession keys "$VDAY" "$VDAY2")"

# ---- 17j. ZERO PROGRESS (deploy #1041 r2 MAJOR 3): a run whose stable boundary sits AT its checkpoint records what ----
# it QUERIED. The reviewer's case: an earlier capture of the date reached 3 and queried 3, so the date's recorded
# obligation was 3. A later run on the date queries 9 while an open transaction holds the LSO at 3: before this round
# it left NOTHING behind, and a session loader admitted the date's earlier observations with [3,9) unresolved.
fresh v10
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
OUT=$(vrun "$VDAY"); RC=$?
want "17j an earlier run on the date captured [0,3) and queried 3 (rc, ranges, queried_end)" "0 0-3 3" "$RC $(vread ranges) $(vm queried_end)"
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 O $K3 {\"frameSeq\":3,\"v\":\"open\"}" "4 O $K4 {\"frameSeq\":4,\"v\":\"open\"}" '5 O x y' '6 O x y' '7 O x y' '8 O x y'
OUT=$(vrun "$VDAY"); RC=$?
want "  a later run: an open transaction AT the checkpoint, queried end 9 — the run succeeds (rc)" 0 "$RC"
has  "  and says the attempt is recorded" "The attempt (queried end 9) is recorded in dt=$VDAY's manifest" "$OUT"
want "  checkpoint unchanged, no new file"                              "3 1" "$(vck) $(vfiles)"
want "  the manifest RECORDS the attempt: no file, an empty range at the checkpoint, the end it queried, zero records" \
     "no_progress <absent> 3 3 3 9 0" \
     "$(vm attempt) $(vm file) $(vm offset_from) $(vm offset_to) $(vm stable_boundary) $(vm queried_end) $(vm records)"
want "  naming the source log"                                         "$SID" "$(vm source_topic_id)"
want "  a session loader REFUSES the date until a capture reaches 9" \
     "SESSION_INCOMPLETE: dt=$VDAY queried up to 9, committed captures reach only 3" "$(vread keys)"
want "  not a failure"                                                     0 "$(runs failed)"
VVD="$T/vnas-vp2"; rm -rf "$VVD"; mkdir -p "$VVD/kafka/prod"
cp -R "$A/kafka/prod/$VP" "$VVD/kafka/prod/$VP"
OUT_V=$(env ARCHIVE_DIR="$VVD" ENV=prod FORCE=true VERIFY_CHECKSUMS=none LOG="$T/verify-vp2.log" \
            OE_TOPICS_ENV="$T/vp.topics.env" bash "$OE/oe-archive-verify.sh" "$VDAY" 2>&1); RC_V=$?
want "  the verifier: the date is COMPLETE for what it claims — an attempt is not a missing file (rc)" 0 "$RC_V"
has  "  and names the withheld range as the attempt's" "open transaction withheld p0 [3,9) — the run at" "$OUT_V"
has  "  (no file published)"                                    "no file published" "$OUT_V"
want "  counting the one file, not the attempt" 1 "$(printf '%s' "$OUT_V" | grep -o 'files=[0-9]*' | head -1 | cut -d= -f2)"
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 C $K3 {\"frameSeq\":3,\"v\":\"c\"}" "4 C $K4 {\"frameSeq\":4,\"v\":\"c\"}" '5 M' '6 A x y' '7 A x y' '8 M'
OUT=$(vrun "$VDAY2"); RC=$?
want "  it resolves: the next run captures [3,9) under ITS date (rc, names)" "0 3-9" "$RC $(vnames "$VDAY2")"
want "  and the SESSION is whole across both dates: the attempt's 9 is reached" "$K1 $K2 $K3 $K4" "$(vsession keys "$VDAY" "$VDAY2")"
want "  the manifest chain reads through the attempt's empty range" "0-3 3-3 3-9 contiguous" "$(vchain "$VDAY" "$VDAY2")"

# ---- 17k. the MANIFEST is the inventory (deploy #1041 r2 / engine r15 MAJOR): a listed file the disk has lost, an -----
# excluded capture's queried end, a negative one. Coverage was proved over the files ON DISK, so a manifest-listed
# capture that went missing took its obligation with it and the session loaded short on the stale prefix; and an
# excluded capture lost its queried_end with its records.
fresh v11
strike_log "${OPEN_LOG[@]}"
vrun "$VDAY" >/dev/null
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 C $K3 {\"frameSeq\":3,\"v\":\"c\"}" "4 C $K4 {\"frameSeq\":4,\"v\":\"c\"}" '5 M'
vrun "$VDAY" >/dev/null
want "17k two captures on the date, contiguous: the session is whole" "0-3 3-6 $K1 $K2 $K3 $K4" "$(vread ranges) $(vread keys)"
TAIL=$(basename "$(ls "$(VDIR)"/*.p0.3-6.*.jsonl.gz)")
rm -f "$(VDIR)/$TAIL"
want "  the second file LOST from disk, its manifest line kept: REFUSED, never read with the stale prefix" \
     "MISSING_CAPTURE_FILE: $TAIL [3,6) is named by dt=$VDAY's manifest but is not on disk" "$(vread keys)"
fresh v12
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
STRIP2="$VP.p0.0-3.dt20260909.20260909T230000Z.jsonl.gz"
VFAKE_QEND=10 vfake "$STRIP2" "$VDAY" stripped 0 3 "0:$K1:{\"frameSeq\":1,\"v\":\"stripped\"}"
want "  an EXCLUDED capture (offsets stripped) whose run queried 10: its records are not read, its obligation STANDS" \
     "SESSION_INCOMPLETE: dt=$VDAY queried up to 10, committed captures reach only 3" "$(vread keys)"
rm -f "$(VDIR)/$STRIP2"
grep -v -e "$STRIP2" "$(VDIR)/_manifest.jsonl" > "$(VDIR)/_manifest.tmp" && mv "$(VDIR)/_manifest.tmp" "$(VDIR)/_manifest.jsonl"
NEG="$VP.p0.3-5.dt20260909.20260909T233000Z.jsonl.gz"
VFAKE_QEND=-1 vfake "$NEG" "$VDAY" ok 3 5 "3:$K3:{\"frameSeq\":3,\"v\":\"c\"}"
has  "  a NEGATIVE queried_end is not an offset: refused, the endpoint check cannot be switched off" \
     "queried_end -1 is not an offset" "$(vread keys)"

# ---- 17l. the ATTEMPT's PREFIX is owed (deploy #1041 r3 MAJOR 2 / engine r16 MAJOR): a no-progress line declares -----
# where its run began (the checkpoint) as well as the end it queried, and round 2 enforced only the end: coverage
# started at whichever admitted capture came first. Codex's ATTEMPT_PREFIX_LOST, reached through the archiver's own
# retention-gap branch: the attempt at checkpoint 3 queried 9; retention then expired [3,6); the next run found the
# log starting at 6, said "3 records LOST", captured [6,9) — and the session loaded on [6,9) as if whole. The
# checkpoint 3 is the PREVIOUS date's capture, so dt=D holds only the attempt, exactly the reviewer's shape.
VDAY0=2026-09-08
fresh v13
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY0" >/dev/null
want "17l the previous date captured [0,3): checkpoint 3"                    "0-3 3" "$(vnames "$VDAY0") $(vck)"
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 O $K3 {\"frameSeq\":3,\"v\":\"open\"}" "4 O $K4 {\"frameSeq\":4,\"v\":\"open\"}" '5 O x y' '6 O x y' '7 O x y' '8 O x y'
OUT=$(vrun "$VDAY"); RC=$?
want "  dt=$VDAY: an attempt at checkpoint 3 querying 9 is its ONLY line (rc, attempt, checkpoint, files)" "0 no_progress 3 0" \
     "$RC $(vm attempt) $(vck) $(vfiles)"
strike_log "6 C $K3 {\"frameSeq\":3,\"v\":\"c\"}" "7 C $K4 {\"frameSeq\":4,\"v\":\"c\"}" '8 M'
fixture "topicid $SID" "0 6 9"              # retention expired [3,6): the log now starts at 6
OUT=$(vrun "$VDAY2"); RC=$?
want "  the next run: checkpoint 3 expired, the log starts at 6 — the archiver's retention-gap branch (rc)" 0 "$RC"
has  "  which says what was lost" "GAP $VP p0: checkpoint 3 expired (log now starts at 6) — 3 records LOST before this run" "$OUT"
want "  and captures [6,9) under its date"                                "6-9" "$(vnames "$VDAY2")"
has  "  the SESSION read across both dates is REFUSED: [3,6) is in no capture, and no later capture will ever hold it" \
     "COVERAGE_GAP: offsets [3,6) are in no committed capture" "$(vsession keys "$VDAY" "$VDAY2")"
has  "  naming the declared beginning and the first capture" \
     "the window's declarations begin at 3 but the first admitted capture begins at 6" "$(vsession keys "$VDAY" "$VDAY2")"
want "  dt=$VDAY alone is still SESSION_INCOMPLETE, as before: nothing is captured there yet" 1 \
     "$(vread keys | grep -c "^SESSION_INCOMPLETE: dt=$VDAY queried up to 9, committed captures reach only nothing")"
# Codex's second shape, in the loader model: the attempt at 3 queried 10 and the next capture is [10,12) — reach 12
# satisfies the end, and the WHOLE required interval [3,10) is unproved. And the first shape at the log start.
fresh v13b
vattempt "$VDAY" 3 10
vfake "$VP.p0.10-12.dt20260910.20260910T220000Z.jsonl.gz" "$VDAY2" ok 10 12 "10:$K4:{\"frameSeq\":4,\"v\":\"c\"}"
has  "  dt=$VDAY holds ONLY an attempt at 3 querying 10, then [10,12): the end is reached and the session is still REFUSED" \
     "COVERAGE_GAP: offsets [3,10) are in no committed capture — the window's declarations begin at 3 but the first admitted capture begins at 10" \
     "$(vsession keys "$VDAY" "$VDAY2")"
fresh v13c
vattempt "$VDAY" 0 10
vfake "$VP.p0.3-10.dt20260910.20260910T220000Z.jsonl.gz" "$VDAY2" ok 3 10 "3:$K3:{\"frameSeq\":3,\"v\":\"c\"}"
has  "  an attempt at 0 querying 10, then [3,10): [0,3) is owed and REFUSED" \
     "COVERAGE_GAP: offsets [0,3) are in no committed capture" "$(vsession keys "$VDAY" "$VDAY2")"
fresh v13d
vattempt "$VDAY" 3 10
vfake "$VP.p0.3-10.dt20260910.20260910T220000Z.jsonl.gz" "$VDAY2" ok 3 10 "3:$K3:{\"frameSeq\":3,\"v\":\"c\"}"
want "  the control: the attempt at 3 querying 10, then [3,10) — whole"    "$K3" "$(vsession keys "$VDAY" "$VDAY2")"

# ---- 17m. a manifest line naming BOTH a file and an attempt (deploy #1041 r3 MINOR): the archiver writes no such ----
# line. The verifier took ANY non-null "attempt" as an attempt line — out of the file-presence and checksum checks —
# so "attempt":"no_progress" pasted onto a missing file's line turned CORRUPT (rc 1) into OK (rc 0). The line is
# malformed: the verifier reports it and still looks for the file; the loader model refuses it (MANIFEST_MISMATCH).
vpaste_attempt() { # <dt> <file>: add "attempt":"no_progress" to the manifest line naming <file>
  python3 - "$(VDIR "$1")/_manifest.jsonl" "$2" <<'PY'
import json, sys
p, name = sys.argv[1], sys.argv[2]
out = []
for l in open(p):
    if l.strip():
        e = json.loads(l)
        if e.get("file") == name:
            e["attempt"] = "no_progress"
        out.append(json.dumps(e) + "\n")
open(p, "w").writelines(out)
PY
}
fresh v14
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
BOTH="$VP.p0.3-6.dt20260909.20260909T230000Z.jsonl.gz"
vfake "$BOTH" "$VDAY" ok 3 6 "3:$K3:{\"frameSeq\":3,\"v\":\"c\"}"
want "17m two captures on the date, whole"                          "0-3 3-6 $K1 $K2 $K3" "$(vread ranges) $(vread keys)"
vverify() { # the verifier over a copy of the VP topic dir; prints "rc=<rc>" then the output
  local vvd="$T/vnas-vp-$1"; rm -rf "$vvd"; mkdir -p "$vvd/kafka/prod"; cp -R "$A/kafka/prod/$VP" "$vvd/kafka/prod/$VP"
  local out rc
  out=$(env ARCHIVE_DIR="$vvd" ENV=prod FORCE=true VERIFY_CHECKSUMS=none LOG="$T/verify-vp-$1.log" \
            OE_TOPICS_ENV="$T/vp.topics.env" bash "$OE/oe-archive-verify.sh" "$VDAY" 2>&1); rc=$?
  printf 'rc=%s\n%s' "$rc" "$out"
}
OUT_V=$(vverify 14a)
want "  the verifier, both files present: COMPLETE (rc), two files"  "rc=0 2" "$(printf '%s' "$OUT_V" | head -1) $(printf '%s' "$OUT_V" | grep -o 'files=[0-9]*' | head -1 | cut -d= -f2)"
vpaste_attempt "$VDAY" "$BOTH"
OUT_V=$(vverify 14b)
want "  \"attempt\":\"no_progress\" pasted onto the second file's line, file PRESENT: the date is not OK (rc)" "rc=1" "$(printf '%s' "$OUT_V" | head -1)"
has  "  the line is reported as malformed"  "1 manifest line(s) name both a file and an attempt (malformed; checked as file lines): ['$BOTH']" "$OUT_V"
has  "  as PARTIAL, not CORRUPT: the file it names is there"           "PARTIAL  $VP" "$OUT_V"
want "  and it is still COUNTED as a file, never as an attempt"           2 "$(printf '%s' "$OUT_V" | grep -o 'files=[0-9]*' | head -1 | cut -d= -f2)"
has  "  the loader model REFUSES the line: it is neither a file line nor an attempt" \
     "MANIFEST_MISMATCH: dt=$VDAY/_manifest.jsonl names both a file ($BOTH) and an attempt (no_progress) on one line" "$(vread keys)"
rm -f "$(VDIR)/$BOTH"
OUT_V=$(vverify 14c)
want "  the file REMOVED, its contradictory line kept — Codex's case: CORRUPT (rc 1), not OK" "rc=1" "$(printf '%s' "$OUT_V" | head -1)"
has  "  the missing file is still seen"      "1 manifest file(s) absent on disk: ['$BOTH']" "$OUT_V"
has  "  as CORRUPT"                                                   "CORRUPT  $VP" "$OUT_V"
has  "  and the loader model still refuses the line, before it could miss the file" "MANIFEST_MISMATCH: dt=$VDAY/_manifest.jsonl names both a file" "$(vread keys)"

# ---- 17n. CRASH RESIDUE across logs (deploy #1041 r4 MAJOR 1): a file published, its manifest line never appended ----
# (the process died between the mv and the append — the window between oe-archive-kafka.sh's gated mv and its
# manifest printf), then the topic re-created and the next run capturing the NEW log. Round 3 excluded the residue
# (NO_MANIFEST_LINE) and let the new log's offsets "cover" its range: Codex r4's ORPHAN_LOSES_DISTINCT_OBSERVATION
# loaded session D on log B's observation alone, log A's distinct observation lost with the line. A file no manifest
# line names now refuses the window: it names no log, so no capture's offsets are coordinates on it. The crash is
# simulated as 17g simulates it — by removing what the crash would not have written: the line, and the checkpoint
# the archiver writes after it (manifest FIRST, checkpoint SECOND).
fresh v15
strike_log "${OPEN_LOG[@]}"
vrun "$VDAY" >/dev/null
RES=$(basename "$(ls "$(VDIR)"/*.p0.0-3.*.jsonl.gz)")
want "17n dt=$VDAY captured log $SID's [0,3), querying 5 (names, queried_end, file)" "0-3 5 $RES" \
     "$(vnames) $(vm queried_end) $(vm file)"
: > "$(VDIR)/_manifest.jsonl"; rm -f "$A/kafka/prod/_manifest/$VP.offsets"
CKN=$(vck)
want "  the CRASH: the file stands, no line names it, no checkpoint (files, lines, checkpoint)" "1 0 none" \
     "$(vfiles) $(grep -c . "$(VDIR)/_manifest.jsonl") ${CKN:-none}"
STRIKE_ID=$TID strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"B\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"B\"}" '2 M' \
                          "3 C $K3 {\"frameSeq\":3,\"v\":\"B\"}" "4 C $K4 {\"frameSeq\":4,\"v\":\"B\"}" '5 M' \
                          '6 C x y' '7 C x y' '8 C x y' '9 M'
OUT=$(vrun "$VDAY2"); RC=$?
want "  the topic re-created, the new log grown to 10: the next run succeeds (rc)" 0 "$RC"
want "  and captures log $TID's [0,10) under its date, naming it"        "0-10 $TID" "$(vnames "$VDAY2") $(vm source_topic_id "$VDAY2")"
has  "  the SESSION across both dates is REFUSED: the residue names no log, and B's [0,10) covers nothing of it" \
     "UNCOVERED_EXCLUDED_FILE: $RES [0,3) has no manifest line on dt=$VDAY" "$(vsession keys "$VDAY" "$VDAY2")"
has  "  saying why"                                        "resolved by an operator, never by offsets" "$(vsession keys "$VDAY" "$VDAY2")"
want "  round 3's answer — the session read on B's records — is never given" 0 \
     "$(vsession keys "$VDAY" "$VDAY2" | grep -c "^SPX|")"
# On the SAME log — 17g's ordinary crash re-read, with the line lost too — the residue refuses just the same:
# nothing names its log, and the rule does not read the numbers in its name as a coordinate on anything.
fresh v15b
strike_log "${OPEN_LOG[@]}"
vrun "$VDAY" >/dev/null
RES=$(basename "$(ls "$(VDIR)"/*.p0.0-3.*.jsonl.gz)")
: > "$(VDIR)/_manifest.jsonl"; rm -f "$A/kafka/prod/_manifest/$VP.offsets"
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 C $K3 {\"frameSeq\":3,\"v\":\"c\"}" "4 C $K4 {\"frameSeq\":4,\"v\":\"c\"}" '5 M'
OUT=$(vrun "$VDAY"); RC=$?
want "  the retry on the SAME log re-reads from 0 against the advanced boundary, beside the residue (rc, names)" "0 0-3 0-6" "$RC $(vnames)"
has  "  and the date still REFUSES: the residue's log is unknown even here" \
     "UNCOVERED_EXCLUDED_FILE: $RES [0,3) has no manifest line on dt=$VDAY" "$(vread keys)"
rm -f "$(VDIR)/$RES"
want "  the residue removed by an operator: the date reads whole on the re-read" "$K1 $K2 $K3 $K4" "$(vread keys)"

# ---- 17o. a run that dies MID-APPEND (deploy #1041 r5 MAJOR R5-2): the archiver appends a manifest line in one --------
# printf; a crash inside it leaves a PARTIAL line. Rounds 1-4's loader model (and the Java loader) skipped a line
# that did not parse, so a truncated no-progress append behind a complete [0,3) admitted the session at 3 with the
# attempt's 9 never asked for — Codex's TRUNCATED_NO_PROGRESS_APPEND. Through the real archiver: 17j's shape (a
# capture querying 3, then an open transaction at the checkpoint querying 9 → the attempt line), the manifest then
# cut mid-line as the crash would leave it. A nonblank line that is not a JSON object refuses the window; the
# verifier reports the date CORRUPT.
fresh v16
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 O $K3 {\"frameSeq\":3,\"v\":\"open\"}" "4 O $K4 {\"frameSeq\":4,\"v\":\"open\"}" '5 O x y' '6 O x y' '7 O x y' '8 O x y'
OUT=$(vrun "$VDAY"); RC=$?
want "17o a capture querying 3, then the attempt querying 9: two lines (rc, lines, attempt, queried_end)" "0 2 no_progress 9" \
     "$RC $(grep -c . "$(VDIR)/_manifest.jsonl") $(vm attempt) $(vm queried_end)"
want "  the complete line: the session is INCOMPLETE until a capture reaches 9" \
     "SESSION_INCOMPLETE: dt=$VDAY queried up to 9, committed captures reach only 3" "$(vread keys)"
# the CRASH mid-append: the manifest is cut inside the attempt line, at the byte a partial write would leave
python3 - "$(VDIR)/_manifest.jsonl" <<'PY'
import sys
p = sys.argv[1]; data = open(p, "rb").read()
lines = data.split(b"\n"); last = lines[-2]          # the attempt line (the file ends with a newline)
cut = last[: len(last) // 2]                          # half of it, no newline
open(p, "wb").write(b"\n".join(lines[:-2]) + b"\n" + cut)
PY
want "  the manifest cut mid-line: the attempt's queried end is gone from what can be read (parseable lines)" 1 \
     "$(python3 -c 'import json,sys
n=0
for l in open(sys.argv[1]):
    try: json.loads(l); n+=1
    except ValueError: pass
print(n)' "$(VDIR)/_manifest.jsonl")"
has  "  the loader model REFUSES the window: a nonblank line that is not a JSON object, named by date and line" \
     "MANIFEST_UNPARSEABLE: dt=$VDAY/_manifest.jsonl line 2 is not a JSON object" "$(vread keys)"
has  "  saying why"                                        "a run that died mid-append" "$(vread keys)"
want "  round 4's answer — the session admitted at 3 — is never given" 0 "$(vread keys | grep -c "^SPX|")"
OUT_V=$(vverify 16a)
want "  the verifier: the date is CORRUPT (rc 1), never OK or PARTIAL over a line it cannot read" "rc=1" "$(printf '%s' "$OUT_V" | head -1)"
has  "  as CORRUPT"                                                   "CORRUPT  $VP" "$OUT_V"
has  "  naming the line"  "1 unparseable manifest line(s) (not a JSON object; a run that died mid-append?) at line(s) [2]" "$OUT_V"
# blank lines are nothing, to the model and the verifier alike
fresh v16b
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
printf '\n   \n\n' >> "$(VDIR)/_manifest.jsonl"
want "  blank lines appended: the date reads whole"                       "$K1 $K2" "$(vread keys)"
OUT_V=$(vverify 16b)
want "  and the verifier is COMPLETE (rc)"                                "rc=0" "$(printf '%s' "$OUT_V" | head -1)"

# ---- 17p. the model level with engine r18 (13309d86): an attempt OWES the end it queried, and every integral field ----
# has the shape of one. The Java loader's rules since r18; the model was behind on both until now. Through the real
# archiver's lines: 17j's attempt with queried_end REMOVED (it owed only its checkpoint: the session admitted at 3
# with [3,9) never asked for), and a real capture's line with escaped_records 0.5 (it truncated to 0 and was
# admitted), records "bad", stable_boundary 2.5.
vedit() { # <dt> <field> <json value | ->: set the field on the LAST manifest line of <dt>; '-' removes it
  python3 - "$(VDIR "$1")/_manifest.jsonl" "$2" "$3" <<'PY'
import json, sys
p, k, v = sys.argv[1], sys.argv[2], sys.argv[3]
lines = [l for l in open(p) if l.strip()]
e = json.loads(lines[-1])
if v == "-": e.pop(k, None)
else: e[k] = json.loads(v)
lines[-1] = json.dumps(e) + "\n"
open(p, "w").writelines(lines)
PY
}
fresh v17
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M' \
           "3 O $K3 {\"frameSeq\":3,\"v\":\"open\"}" "4 O $K4 {\"frameSeq\":4,\"v\":\"open\"}" '5 O x y' '6 O x y' '7 O x y' '8 O x y'
vrun "$VDAY" >/dev/null
want "17p the real attempt line (checkpoint 3, queried 9): the date is INCOMPLETE" \
     "SESSION_INCOMPLETE: dt=$VDAY queried up to 9, committed captures reach only 3" "$(vread keys)"
vedit "$VDAY" queried_end -
has  "  its queried_end REMOVED: the model REFUSES — an attempt owes the end it queried, never merely its checkpoint" \
     "MANIFEST_MISMATCH: attempt at " "$(vread keys)"
has  "  naming the field"                                  "its manifest line has no integral queried_end" "$(vread keys)"
want "  the session admitted at 3 (the model's old answer) is never given" 0 "$(vread keys | grep -c "^SPX|")"
vedit "$VDAY" queried_end 9
vedit "$VDAY" records 1
has  "  an attempt claiming 1 record is not an attempt"    "MANIFEST_MISMATCH: attempt at " "$(vread keys)"
fresh v17b
strike_log "0 C $K1 {\"frameSeq\":1,\"v\":\"c\"}" "1 C $K2 {\"frameSeq\":2,\"v\":\"c\"}" '2 M'
vrun "$VDAY" >/dev/null
want "  a real capture [0,3), whole"                                  "$K1 $K2" "$(vread keys)"
CAP=$(vm file)
vedit "$VDAY" escaped_records 0.5
has  "  escaped_records 0.5 on its line: REFUSED in the preflight (it truncated to 0 and was admitted before)" \
     "MANIFEST_MISMATCH: $CAP: its manifest line has no integral escaped_records" "$(vread keys)"
vedit "$VDAY" escaped_records 0
vedit "$VDAY" records '"bad"'
has  "  records \"bad\": REFUSED before the file is read"  "MANIFEST_MISMATCH: $CAP: its manifest line has no integral records" "$(vread keys)"
vedit "$VDAY" records 2
vedit "$VDAY" stable_boundary 2.5
has  "  stable_boundary 2.5: REFUSED"                      "MANIFEST_MISMATCH: $CAP: its manifest line has no integral stable_boundary" "$(vread keys)"
vedit "$VDAY" stable_boundary 3
vedit "$VDAY" escaped_records 1
want "  the control: escaped_records 1, an integer — an EXCLUSION as before, and uncovered, the date refuses" 1 \
     "$(vread keys | grep -c "^UNCOVERED_EXCLUDED_FILE: $CAP")"
vedit "$VDAY" escaped_records 0
want "  restored: whole"                                              "$K1 $K2" "$(vread keys)"

# ---- 17f. EVERY vol-premium ledger is committed-only by default --------------------------------------------------------
for vt in options.spx.vol-premium.ivrv options.spx.vol-premium.events options.spx.vol-premium.warnings \
          options.spx.vol-premium.current options.spx.vol-premium.dlq options.spx.vol-premium.baseline \
          options.spx.vol-premium.calendar; do
  fresh "v7-$vt"
  strike_log '0 C k {"a":1}' '1 M'
  VTOPICS="$vt" vrun "$VDAY" >/dev/null
  want "17f $vt: read by the committed reader, never by the console consumer" "1 0" \
       "$(grep -c "^reader $vt p0 from=0 " "$CALLS") $(grep -c "^console $vt " "$CALLS")"
done

echo
[ "$FAILED" -eq 0 ] && { echo "test-archive-reset: ALL PASS"; exit 0; }
echo "test-archive-reset: $FAILED FAILURE(S)"; exit 1
