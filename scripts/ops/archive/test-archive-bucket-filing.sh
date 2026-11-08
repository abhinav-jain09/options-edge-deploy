#!/usr/bin/env bash
# test-archive-bucket-filing.sh — the dev book archive files each record under the trade date of its bucketEnd and
# writes the capture provenance into every manifest line (ES Level Absorption Requirements revision 15, section 10
# Capture 3 and 4; A-GAP-05). It runs the real oe-archive-kafka.sh against stand-ins for the Kafka CLIs and for the
# canonical reader (the STRIKE_READER seam, as test-archive-reset.sh does), so every offset, bucketEnd and
# checkpoint is exact.
set -uo pipefail
OE="$(cd "$(dirname "$0")" && pwd)"
ARCH="$OE/oe-archive-kafka.sh"
for tool in flock python3 gzip sha256sum timeout awk; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' is required — refusing to run a suite that would pass vacuously without it"; exit 1; }
done
T=$(mktemp -d); BIN="$T/bin"; mkdir -p "$BIN"
export OE_FIXTURE="$T/fixture"
FAILED=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; FAILED=$(( FAILED + 1 )); }
want() { [ "$2" = "$3" ] && pass "$1" || { fail "$(printf '%s (want %s, got %s)' "$1" "$2" "$3")"; [ -z "${OUT:-}" ] || printf '%s\n' "$OUT" | tail -8 | sed 's/^/      | /'; }; }

cat > "$BIN/kafka-topics.sh" <<'SH'
#!/usr/bin/env bash
topic=""; op=""
while [ $# -gt 0 ]; do case "$1" in --topic) topic="$2"; shift 2 ;; --list) op=list; shift ;; --describe) op=describe; shift ;; *) shift ;; esac; done
if [ "$op" = list ]; then echo __consumer_offsets; cat "$OE_FIXTURE.topics"; exit 0; fi
printf 'Topic: %s\tTopicId: BBBBBBBBBBBBBBBBBBBBBB\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: retention.ms=86400000\n' "$topic"
printf '\tTopic: %s\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n' "$topic"
SH
cat > "$BIN/kafka-get-offsets.sh" <<'SH'
#!/usr/bin/env bash
topic=""; t=""
while [ $# -gt 0 ]; do case "$1" in --topic) topic="$2"; shift 2 ;; --time) t="$2"; shift 2 ;; *) shift ;; esac; done
read -r beg end < "$OE_FIXTURE.range"
if [ "$t" = earliest ]; then echo "$topic:0:$beg"; else echo "$topic:0:$end"; fi
SH
cat > "$BIN/reader" <<'SH'
#!/usr/bin/env bash
# canonical reader stand-in: $OE_FIXTURE.log has one line per offset: <offset> <bucketEnd|-> [<schemaVersion> [<closureKind>]]; writes the five-field layout
topic=""; part=""; from=""; out=""; sum=""
while [ $# -gt 1 ]; do case "$1" in --topic) topic="$2" ;; --partition) part="$2" ;; --from) from="$2" ;; --out) out="$2" ;; --summary) sum="$2" ;; esac; shift 2; done
read -r beg end < "$OE_FIXTURE.range"
awk -v f="$from" -v b="$end" -v p="$part" '$1 >= f && $1 < b {
  ver = ($3 == "" ? 1 : $3); kind = ($4 == "" ? "" : ",\"closureKind\":\"" $4 "\"");
  if ($2 == "-") v = "{\"schemaVersion\":" ver kind "}"; else v = "{\"schemaVersion\":" ver ",\"bucketEnd\":" $2 kind "}";
  printf "CreateTime:1793979056204\tPartition:%s\tOffset:%s\tCME|ESZ6\t%s\n", p, $1, v }' "$OE_FIXTURE.log" > "$out"
n=$(wc -l < "$out" | tr -d ' ')
echo "STRIKE_ARCHIVE_READER status=COMPLETE topic=$topic partition=$part from=$from lso=$end boundary=$end position=$end records=$n escaped=0 elapsed_ms=1" > "$sum"
SH
chmod +x "$BIN"/*

# a registry stand-in: /schemas/ids/<id> answers {"schema": "SCHEMA-<id>"} for the ids the book archive knows, 404 otherwise
REGPORT=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")
python3 - "$REGPORT" <<'PY' &
import sys, http.server, json, re
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        m = re.match(r"^/schemas/ids/(\d+)$", self.path)
        if m and int(m.group(1)) in (3301, 3302, 3401, 3501, 3502, 3504, 3506, 3508, 3509, 3510):
            b = json.dumps({"schema": "SCHEMA-" + m.group(1)}).encode(); self.send_response(200)
        else:
            b = b"{}"; self.send_response(404)
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
REGSRV=$!
trap 'kill $REGSRV 2>/dev/null; chmod -R u+rwx "$T" 2>/dev/null; rm -rf "$T"' EXIT
for _ in $(seq 1 40); do (echo > /dev/tcp/127.0.0.1/$REGPORT) 2>/dev/null && break; sleep 0.25; done
export OE_CANONICAL_REGISTRY_URL="http://127.0.0.1:$REGPORT"
dig() { python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "$1"; }

ms() { python3 - "$@" <<'PY'
import sys, datetime
from zoneinfo import ZoneInfo
y,mo,d,h,mi,s=map(int,sys.argv[1:7])
print(int(datetime.datetime(y,mo,d,h,mi,s,tzinfo=ZoneInfo("America/New_York")).timestamp()*1000))
PY
}
fresh() { A="$T/$1"; mkdir -p "$A"; }
# macOS awk (BSD awk 20200816, Darwin 27 beta) dies with SIGSEGV (rc 139) now and then inside the archiver's proving
# step on this host; found 2026-11-08 and not reproducible outside the archiver. The archiver refuses the capture and
# leaves the checkpoint where it was (a retry next run), which is its contract. A test that wants the archiver's
# OTHER behaviour therefore runs it again, up to five times, when -- and only when -- the output shows that signal
# death; the number of such runs is printed so the flake stays visible. A run that fails any other way is returned as is.
arun() { # <topic> [env assignments]; the archiver, dev, one topic
  local topic="$1" out try=0; shift
  printf '%s\n' "$topic" > "$OE_FIXTURE.topics"
  while :; do
    try=$(( try + 1 ))
    out=$(env ARCHIVE_DIR="$A" ENV=dev ARCHIVE_JOB=test-bucket BOOTSTRAP=shim:9092 KAFKA_BIN="$BIN" TOPICS="$topic" \
      SESSION_DATE=2026-11-10 ALLOW_NON_NAS=true STRIKE_READER="$BIN/reader" "$@" bash "$ARCH" 2>&1); ARC_RC=$?
    case "$out" in *"the layout check could not run"*|*"scan rc=139"*) echo x >> "$T/awk-crashes"; [ "$try" -lt 5 ] && continue ;; esac
    break
  done
  printf '%s\n' "$out"
  return "$ARC_RC"
}
mf() { python3 - "$A/kafka/dev/$1" <<'PY'
import sys, json, glob
for p in sorted(glob.glob(sys.argv[1] + "/dt=*/_manifest.jsonl")):
    for l in open(p):
        if l.strip():
            e = json.loads(l); print(e["dt"], e["offset_from"], e["offset_to"], e["records"])
PY
}
field() { # <topic> <dt> <member> -> member of the first manifest line of that dt
  python3 - "$A/kafka/dev/$1/dt=$2/_manifest.jsonl" "$3" <<'PY'
import sys, json
l = [json.loads(x) for x in open(sys.argv[1]) if x.strip()]
print(l[0].get(sys.argv[2], "<absent>"))
PY
}
ckpt() { awk '{split($1,a,"="); if (a[1]=="0") print a[2]}' "$A/kafka/dev/_manifest/$1.offsets" 2>/dev/null | tail -1; }

TH=es.mbo.health
THU1=$(ms 2026 11 5 17 59 57); THU3=$(ms 2026 11 5 17 59 59); THU4=$(ms 2026 11 5 18 0 0); THU5=$(ms 2026 11 5 18 0 1)
FRI1=$(ms 2026 11 6 17 59 59); FRI2=$(ms 2026 11 6 18 0 0); SAT=$(ms 2026 11 7 12 0 0); SUN=$(ms 2026 11 8 18 0 0)
printf '%s\n' "0 $THU1" "1 $THU1" "2 $THU3" "3 $THU4" "4 $THU5" "5 $FRI1" "6 $FRI2" "7 $SAT" "8 $SUN" > "$OE_FIXTURE.log"

# ---- 1. one capture is cut at every change of trade date ---------------------------------------------------
fresh a1; echo "0 9" > "$OE_FIXTURE.range"
OUT=$(arun $TH); RC=$?
want "1  the run succeeds" 0 "$RC"
want "1  three files, one per trade date, offset-contiguous [0,3) [3,6) [6,9)" \
     "2026-11-05 0 3 3|2026-11-06 3 6 3|2026-11-09 6 9 3" "$(mf $TH | tr '\n' '|' | sed 's/|$//')"
want "1  nothing is filed under the run date (SESSION_DATE 2026-11-10 gets no folder)" no "$([ -d "$A/kafka/dev/$TH/dt=2026-11-10" ] && echo yes || echo no)"
want "1  the checkpoint is the end of the last file" 9 "$(ckpt $TH)"
want "1  the Fri 18:00 record and the Saturday and Sunday 18:00 records are all Monday's" 3 "$(gzip -dc "$A/kafka/dev/$TH/dt=2026-11-09/"*.jsonl.gz | wc -l | tr -d ' ')"
want "1  the scratch folder is gone" no "$([ -d "$A/kafka/dev/$TH/.scratch" ] && echo yes || echo no)"
want "1  bucket bounds of Thursday's file" "$THU1 $THU3 $THU3" "$(field $TH 2026-11-05 first_bucket_end) $(field $TH 2026-11-05 last_bucket_end) $(field $TH 2026-11-05 max_bucket_end)"
want "1  segment 1 of 3" "1 3" "$(field $TH 2026-11-06 segment) $(field $TH 2026-11-06 segments)"
want "1  filed_by" bucketEnd_trade_date "$(field $TH 2026-11-09 filed_by)"
want "1  the first capture has no starting checkpoint (null) and the broker begins at 0" "None 0" "$(field $TH 2026-11-05 capture_start_checkpoint) $(field $TH 2026-11-05 broker_beginning_offset)"
want "1  wall-clock start and end are ISO instants" yes "$(python3 - "$(field $TH 2026-11-05 capture_wall_start)" "$(field $TH 2026-11-05 capture_wall_end)" <<'PY'
import sys, datetime
a, b = [datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") for x in sys.argv[1:3]]
print("yes" if a <= b else "no")
PY
)"
want "1  bytes is the real size (not 0) on this host" yes "$([ "$(field $TH 2026-11-05 bytes)" -gt 0 ] 2>/dev/null && echo yes || echo no)"
want "1  min_event_time is an ISO instant (not unknown) on this host" yes "$(case "$(field $TH 2026-11-05 min_event_time)" in 20*Z) echo yes ;; *) echo no ;; esac)"

# ---- 2. the next run chains from the checkpoint, with the provenance of the chain -------------------------
printf '%s\n' "9 $(ms 2026 11 9 9 30 0)" "10 $(ms 2026 11 9 9 30 1)" >> "$OE_FIXTURE.log"; echo "0 11" > "$OE_FIXTURE.range"
sleep 1
OUT=$(arun $TH); RC=$?
want "2  the second run succeeds" 0 "$RC"
want "2  its single file continues at 9, under Monday" "2026-11-09 9 11 2" "$(mf $TH | tail -1)"
want "2  its starting checkpoint is the previous capture's end" 9 "$(python3 - "$A/kafka/dev/$TH/dt=2026-11-09/_manifest.jsonl" <<'PY'
import sys, json
print([json.loads(x) for x in open(sys.argv[1]) if x.strip()][-1]["capture_start_checkpoint"])
PY
)"
want "2  a run with nothing new is quiet and files nothing" "$(mf $TH | wc -l | tr -d ' ')" "$(arun $TH >/dev/null; mf $TH | wc -l | tr -d ' ')"

# ---- 3. a failure part-way leaves the files before it published and checkpointed ------------------------
fresh a3; echo "0 9" > "$OE_FIXTURE.range"
mkdir -p "$A/kafka/dev/$TH/dt=2026-11-06"; chmod 555 "$A/kafka/dev/$TH/dt=2026-11-06"
OUT=$(arun $TH); RC=$?
want "3  the run fails" 1 "$RC"
want "3  the first file is published" "2026-11-05 0 3 3" "$(mf $TH | head -1)"
want "3  the checkpoint stops at the end of the first file" 3 "$(ckpt $TH)"
chmod 755 "$A/kafka/dev/$TH/dt=2026-11-06"
OUT=$(arun $TH); RC=$?
want "3  the next run completes the rest" 0 "$RC"
want "3  no gap and no duplicate" "2026-11-05 0 3 3|2026-11-06 3 6 3|2026-11-09 6 9 3" "$(mf $TH | tr '\n' '|' | sed 's/|$//')"
want "3  checkpoint 9" 9 "$(ckpt $TH)"

# ---- 4. a record without a bucketEnd is kept and counted ----------------------------------------------------
fresh a4; printf '%s\n' "0 $THU1" "1 -" "2 $THU4" > "$OE_FIXTURE.log"; echo "0 3" > "$OE_FIXTURE.range"
OUT=$(arun $TH); RC=$?
want "4  succeeds" 0 "$RC"
want "4  the keyless record stays in Thursday's file" "2026-11-05 0 2 2|2026-11-06 2 3 1" "$(mf $TH | tr '\n' '|' | sed 's/|$//')"
want "4  and is counted" 1 "$(field $TH 2026-11-05 bucket_end_missing)"

# ---- 5. a topic with no bucketEnd keeps the run-date filing; it still gets the provenance -------------------
fresh a5; printf '%s\n' "0 -" "1 -" > "$OE_FIXTURE.log"; echo "0 2" > "$OE_FIXTURE.range"
DEPTH=es.rithmic.depth
OUT=$(arun $DEPTH); RC=$?
want "5  succeeds" 0 "$RC"
want "5  one file under the run date" "2026-11-10 0 2 2" "$(mf $DEPTH | tr '\n' '|' | sed 's/|$//')"
want "5  filed_by run_date, no bucket members" "run_date <absent>" "$(field $DEPTH 2026-11-10 filed_by) $(field $DEPTH 2026-11-10 first_bucket_end)"
want "5  provenance present" "None 0" "$(field $DEPTH 2026-11-10 capture_start_checkpoint) $(field $DEPTH 2026-11-10 broker_beginning_offset)"

# ---- 6. the same topic on another environment is untouched ------------------------------------------------
fresh a6; printf '%s\n' "0 $THU1" "1 $THU4" > "$OE_FIXTURE.log"; echo "0 2" > "$OE_FIXTURE.range"
OUT=$(arun $TH ENV=prod); RC=$?
want "6  prod: one file under the run date, no new members" "2026-11-10 0 2 2" "$(A="$A" python3 - "$A/kafka/prod/$TH" <<'PY'
import sys, json, glob
for p in glob.glob(sys.argv[1] + "/dt=*/_manifest.jsonl"):
    for l in open(p):
        if l.strip():
            e = json.loads(l); print(e["dt"], e["offset_from"], e["offset_to"], e["records"]); assert "filed_by" not in e and "capture_wall_start" not in e
PY
)"

# ---- 7. what the checker makes of the archiver's own manifests ---------------------------------------------
fresh a7; printf '%s\n' "0 $THU1" "1 $THU4" "2 $FRI2" > "$OE_FIXTURE.log"; echo "0 3" > "$OE_FIXTURE.range"
OUT=$(arun $TH); RC=$?
CH=$(python3 "$OE/oe-archive-session-chain.py" --root "$A" --topics "$TH" --session 2026-11-09 2>&1)
case "$CH" in *LEGACY_CAPTURE*) fail "7  the checker reads the archiver's lines as legacy" ;; *) pass "7  the checker reads the archiver's lines as non-legacy captures" ;; esac
case "$CH" in *NO_CAPTURE_BEFORE_SESSION_START*|*TERMINAL_NOT_REACHED*) pass "7  and says why this one-run archive is not a complete session" ;; *) fail "7  checker output: $CH" ;; esac

# ---- 8. schema ids and digests ---------------------------------------------------------------------------
fresh a8; printf '%s\n' "0 $THU1 1" "1 $THU3 2" "2 $THU4 2" > "$OE_FIXTURE.log"; echo "0 3" > "$OE_FIXTURE.range"
OUT=$(arun $TH); RC=$?
want "8  health, versions 1 and 2 in a file: ids 3504 and 3509 with their digests" \
     "[3504, 3509] $(dig SCHEMA-3504) $(dig SCHEMA-3509) RESOLVED" \
     "$(field $TH 2026-11-05 schema_ids) $(python3 - "$A/kafka/dev/$TH/dt=2026-11-05/_manifest.jsonl" <<'PY'
import sys, json
e = json.loads(open(sys.argv[1]).readline()); print(e["schema_digests"]["3504"], e["schema_digests"]["3509"], e["schema_digest_status"])
PY
)"
want "8  the version-2-only file (Friday) names only 3509" "[3509]" "$(field $TH 2026-11-06 schema_ids)"
TC=es.mbo.closures
fresh a9; printf '%s\n' "0 $THU1 1 FINAL" "1 $THU3 1 BOOK" "2 $THU4 1 BOOK" > "$OE_FIXTURE.log"; echo "0 3" > "$OE_FIXTURE.range"
OUT=$(arun $TC); RC=$?
want "9  closures: BOOK and FINAL in one file are ids 3501 and 3508" "[3501, 3508]" "$(field $TC 2026-11-05 schema_ids)"
want "9  the FINAL bound of Thursday's closures file is its FINAL record's bucketEnd, not the later BOOK one" "$THU1 $THU3" "$(field $TC 2026-11-05 max_final_bucket_end) $(field $TC 2026-11-05 max_bucket_end)"
want "9  and a file with BOOK only is 3501" "[3501]" "$(field $TC 2026-11-06 schema_ids)"
fresh a10; printf '%s\n' "0 $THU1 1" > "$OE_FIXTURE.log"; echo "0 1" > "$OE_FIXTURE.range"
OUT=$(arun $TH OE_CANONICAL_REGISTRY_URL=http://127.0.0.1:1); RC=$?
want "10 an unreachable registry does not lose the capture" 0 "$RC"
want "10 and the manifest says the digest is unresolved" "[3504] UNRESOLVED:3504" "$(field $TH 2026-11-05 schema_ids) $(field $TH 2026-11-05 schema_digest_status)"
fresh a11; printf '%s\n' "0 $THU1 7" > "$OE_FIXTURE.log"; echo "0 1" > "$OE_FIXTURE.range"
OUT=$(arun $TH); RC=$?
want "11 a schemaVersion the table does not know is recorded as unmapped, not guessed" "[] UNMAPPED:version7" "$(field $TH 2026-11-05 schema_ids) $(field $TH 2026-11-05 schema_digest_status)"
fresh a12; printf '%s\n' "0 -" > "$OE_FIXTURE.log"; echo "0 1" > "$OE_FIXTURE.range"
OUT=$(arun $DEPTH); RC=$?
want "12 depth: id 3301 with its digest" "[3301] RESOLVED" "$(field $DEPTH 2026-11-10 schema_ids) $(field $DEPTH 2026-11-10 schema_digest_status)"

echo
[ ! -s "$T/awk-crashes" ] || echo "NOTE  the host awk died with SIGSEGV in $(wc -l < "$T/awk-crashes" | tr -d " ") archiver run(s); each was run again"
echo "test-archive-bucket-filing: $FAILED FAILURE(S)"
[ "$FAILED" -eq 0 ]
