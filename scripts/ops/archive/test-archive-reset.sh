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
# CLIs the archiver calls, so every partition count, offset, id and record count is exact.
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
while read -r a b c d; do
  [ "$a" = "topicid" ] && continue
  [ -n "$a" ] || continue
  case "$time_arg" in
    earliest) echo "$topic:$a:$b" ;;
    ""|latest) echo "$topic:$a:$c" ;;
    *) [ -n "${d:-}" ] && echo "$topic:$a:$d" ;;   # empty answer = no record that new
  esac
done < "$OE_FIXTURE"
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

echo
[ "$FAILED" -eq 0 ] && { echo "test-archive-reset: ALL PASS"; exit 0; }
echo "test-archive-reset: $FAILED FAILURE(S)"; exit 1
