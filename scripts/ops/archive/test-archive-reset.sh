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
# STRIKE_READER seam; the Java program itself is exercised against a real broker, not here.
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
for f in "$OE_CLI/unreach-ends.err" "$OE_CLI/unreach-ends.rc" "$OE_CLI/until-nomatch.rc" "$OE_SKIP"; do
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
case "$time_arg:${OE_TIME_QUERY:-ok}" in
  earliest:*|:*|latest:*|*:ok|*:skip) : ;;
  # the RECORDED answer of kafka-get-offsets 4.3.0 against an unreachable broker: nothing on stdout, exit 1
  *:fail) cat "$OE_CLI/unreach-ends.err" >&2; exit "$(cat "$OE_CLI/unreach-ends.rc")" ;;
esac
while read -r a b c d; do
  [ "$a" = "topicid" ] && continue
  [ -n "$a" ] || continue
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
topic=""; part=""; from=""; maxend=""; out=""; sum=""; mgroup=""; moff=""
while [ $# -gt 1 ]; do
  case "$1" in
    --topic) topic="$2" ;; --partition) part="$2" ;; --from) from="$2" ;; --max-end) maxend="$2" ;;
    --out) out="$2" ;; --summary) sum="$2" ;; --mark-group) mgroup="$2" ;; --mark-offset) moff="$2" ;;
  esac
  shift 2
done
echo "reader $topic p$part from=${from:-} max_end=${maxend:-} mark=${moff:-} group=${mgroup:-}" >> "$OE_FIXTURE.calls"
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
status=COMPLETE; pos="$boundary"; rc=0; rec="$n"
[ "$boundary" -lt "$from" ] && pos="$from"
case "${STRIKE_SHIM_MODE:-}" in
  timeout)    status=TIMEOUT; pos=$(( from + 1 )); rc=3 ;;
  lie-short)  pos=$(( boundary - 1 )) ;;                 # says COMPLETE, exits 0, did NOT reach the boundary
  lie-count)  rec=$(( n + 1 )) ;;                        # says it wrote one record more than the file holds
  no-summary) exit 0 ;;                                  # exits 0 and states nothing
esac
echo "STRIKE_ARCHIVE_READER status=$status topic=$topic partition=$part from=$from lso=$lso boundary=$boundary position=$pos records=$rec escaped=0 elapsed_ms=1" > "$sum"
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
chmod +x "$BIN/strike-reader" "$BIN/kafka-console-consumer.sh"

strike_log() { # lines of the strike partition's log; the broker fixture's end offset follows from it
  printf '%s\n' "$@" > "$OE_FIXTURE.strike"
  local hwm; hwm=$(awk 'NF { e = $1 + 1 } END { print e + 0 }' "$OE_FIXTURE.strike")
  fixture "topicid SSSSSSSSSSSSSSSSSSSSSS" "0 0 $hwm${UNTIL_OFF:+ $UNTIL_OFF}"
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
want "  no manifest line for an empty stable range"        1 "$(mlines)"
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

echo
[ "$FAILED" -eq 0 ] && { echo "test-archive-reset: ALL PASS"; exit 0; }
echo "test-archive-reset: $FAILED FAILURE(S)"; exit 1
