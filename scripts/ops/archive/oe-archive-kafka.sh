#!/usr/bin/env bash
# oe-archive-kafka.sh — incremental, offset-checkpointed export of Kafka topics to durable files.
#
# WHY: Kafka is NOT an archive. Prod expires the heavy option topics at ONE DAY; dev and es4 are
# wiped every single day. Whatever is not copied out is gone forever — there is no backfill.
#
# WHAT IT WRITES
#   $ARCHIVE_DIR/kafka/$ENV/<topic>/dt=<sessionDate>/<topic>.p<part>.<from>-<to>.<archivedAt>.jsonl.gz
#   $ARCHIVE_DIR/kafka/$ENV/<topic>/dt=<sessionDate>/_manifest.jsonl  <- per-file completeness record
#   $ARCHIVE_DIR/kafka/$ENV/_manifest/<topic>.offsets                 <- last archived offset per partition
#   $ARCHIVE_DIR/kafka/$ENV/_manifest/runs.log                        <- one line per run, append-only
#
#   dt=<sessionDate> is the NEW YORK trading date, not the UTC run date (this job runs at
#   17:10 ET, which is already tomorrow in UTC). <archivedAt> is a UTC stamp, YYYYMMDDTHHMMSSZ,
#   so a file name alone states what it holds and when it was captured.
#
# THE TWO MANIFESTS, AND WHY THERE ARE TWO
#   _manifest/<topic>.offsets is the CHECKPOINT: it makes every run incremental and proves there is
#   no gap (archived-to must equal the next run's archived-from). It is written by the archiver, for
#   the archiver.
#   <topic>/dt=<date>/_manifest.jsonl is the COMPLETENESS RECORD: records, event-time bounds, schema
#   identity and a sha256 per file. It is written for whoever has to answer "is this date whole?"
#   later, without a broker and without trusting a log line. oe-archive-verify.sh reads it.
#   Added 2026-08-12: until then, "the run exited 0" was the only evidence a date existed, and that
#   turned out to be evidence of nothing at all (see LOCKING below).
#
# FORMAT: gzipped JSON Lines. Deliberately dependency-free — pandas/polars/duckdb all read it
# directly (duckdb: read_json_auto('.../*.jsonl.gz')). Convert to Parquet later if you want
# columnar speed; you cannot un-lose data you never copied, so capture beats format.
#
# USAGE
#   ARCHIVE_DIR=/mnt/nas/optionsedge ENV=prod ARCHIVE_JOB=daily ./oe-archive-kafka.sh
#   ARCHIVE_DIR=/home/kafka/archive  ENV=prod ALLOW_NON_NAS=true ./oe-archive-kafka.sh
#   TOPICS="a b c" ARCHIVE_DIR=... ./oe-archive-kafka.sh                       # explicit topic list
#
# KNOBS
#   ARCHIVE_JOB     job identity, and half the lock key. daily|spot|dev|es4|adhoc. Default: adhoc.
#   ON_LOCK_BUSY    skip (default) | fail. See LOCKING.
#   SESSION_DATE    override the dt= folder, for backfills.
#   UNTIL_TS        epoch MILLISECONDS. Stop each partition at the first offset at-or-after this
#                   instant instead of at the log end. With SESSION_DATE this reconstructs one
#                   past session exactly as the nightly run would have filed it — see BACKFILL.
#
# BACKFILL (added 2026-08-12, and used in anger the day it was written)
#   These topics carry retention.ms=-1, so a missed night is NOT necessarily lost: the records are
#   still in the log, they simply have no checkpoint pointing at them. Recover a session with
#     SESSION_DATE=2026-08-10 UNTIL_TS=<that day 17:10 ET in ms> ARCHIVE_JOB=backfill ...
#   Run the missed dates in ASCENDING order. Each one starts from the checkpoint the previous one
#   left behind, so the ranges chain with no gap and no overlap, and the final checkpoint lands
#   exactly where a successful nightly run would have left it — the next normal run then continues
#   as if nothing had been missed. Do NOT reach for a hand-rolled consumer here: this path reuses
#   the same verify-then-checkpoint and manifest code as the nightly run, which is the only reason
#   a backfilled folder is trustworthy in the same way a live one is.
#   SCHEMA_REGISTRY default http://localhost:8082 (prod SR is 8082, NOT 8081 — 8081 is a different
#                   service that answers 404 and makes every subject look missing).
#
# SAFETY: refuses to run if ARCHIVE_DIR is missing or not writable, so an unmounted share can
# never look like a successful backup. Reads only — it never deletes or alters a topic.
set -uo pipefail

ARCHIVER_VERSION="2026-08-13.1"

ARCHIVE_DIR="${ARCHIVE_DIR:?set ARCHIVE_DIR (e.g. /mnt/nas/optionsedge, or a local staging dir)}"
ENV_NAME="${ENV:?set ENV to prod|dev|es4}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/current/bin}"
ALLOW_NON_NAS="${ALLOW_NON_NAS:-false}"
ARCHIVE_JOB="${ARCHIVE_JOB:-adhoc}"
ON_LOCK_BUSY="${ON_LOCK_BUSY:-skip}"
SCHEMA_REGISTRY="${SCHEMA_REGISTRY:-http://localhost:8082}"
UNTIL_TS="${UNTIL_TS:-}"
case "$UNTIL_TS" in
  '') : ;;
  *[!0-9]*) echo "FATAL: UNTIL_TS must be epoch milliseconds (digits only), got '$UNTIL_TS'" >&2; exit 2 ;;
esac

case "$ON_LOCK_BUSY" in
  skip|fail) : ;;
  *) echo "FATAL: ON_LOCK_BUSY must be 'skip' or 'fail', got '$ON_LOCK_BUSY'" >&2; exit 2 ;;
esac

# The default set is the TRAINING core: spot (all three tiers, so you can tell which one was
# authoritative), the raw + display option chain, the derived per-strike features, and the scored
# outcomes that become labels. Inputs without outcomes train nothing.
#
# The sets are defined ONCE in oe-topics.env and sourced by every caller, so prod and dev cannot
# drift into archiving different things — two environments with different topic sets produce two
# unrelated datasets that merely look comparable.
OE_TOPICS_ENV="${OE_TOPICS_ENV:-$(dirname "$0")/oe-topics.env}"
# The canonical sets must come FROM THE FILE. An inherited value would otherwise satisfy the checks
# below and let a caller archive a different set behind the canonical file's back — equality by
# luck, not by construction. Require the file, then discard anything inherited.
[ -r "$OE_TOPICS_ENV" ] || { echo "FATAL: '$OE_TOPICS_ENV' missing or unreadable — refusing to archive an unknown evidence set" >&2; exit 1; }
unset DEALER_LEDGER_EVIDENCE OE_SPOT_TOPICS OE_HEAVY_TOPICS_prod OE_ALL_TOPICS_prod OE_ES4_TOPICS
# shellcheck source=/dev/null
. "$OE_TOPICS_ENV"
: "${DEALER_LEDGER_EVIDENCE:?oe-topics.env did not define DEALER_LEDGER_EVIDENCE}"
: "${OE_ALL_TOPICS_prod:?oe-topics.env did not define OE_ALL_TOPICS_prod}"
: "${OE_ES4_TOPICS:?oe-topics.env did not define OE_ES4_TOPICS}"

DEFAULT_TOPICS_prod="$OE_ALL_TOPICS_prod"
# One definition, every caller: the es4 set comes from oe-topics.env, REQUIRED above — there is
# deliberately no fallback (the deploy swaps script and env file as one atomic unit, so a version
# skew between them is a fault to surface, not to paper over), and the 17:01 cron passes no
# TOPICS override (oe-archive.crontab).
DEFAULT_TOPICS_es4="$OE_ES4_TOPICS"
DEFAULT_TOPICS_dev="$DEFAULT_TOPICS_prod"
eval "TOPICS=\"\${TOPICS:-\$DEFAULT_TOPICS_${ENV_NAME}}\""

# Test seam: print the EFFECTIVE topic set and exit, touching nothing. The suite proves with this
# that ENV=es4 derives exactly OE_ES4_TOPICS from oe-topics.env — the selection path the 17:01
# cron depends on — without needing a broker.
if [ "${PRINT_TOPICS:-}" = "true" ]; then printf '%s\n' "$TOPICS"; exit 0; fi

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

# The ONE alert implementation (oe-alert.sh: "One definition, every caller"). Sourced AFTER log()
# so its output lands in this job's logfile. If it is missing the archiver must still run — a
# missing alerter is not a reason to stop archiving — so fall back to a log-only alert() and say
# so, rather than letting `alert` be an unbound command later.
# shellcheck source=/dev/null
[ -r "$(dirname "$0")/oe-alert.sh" ] && . "$(dirname "$0")/oe-alert.sh"
# Verify the CAPABILITY, not the file: unreadable, a sourcing error, and a helper that simply does
# not define alert() are three different faults with one consequence — `alert` unresolved at the
# only moment it matters. This script runs without `set -e`, so that would surface as a
# "command not found" line followed by a perfectly healthy DONE.
if ! declare -F alert >/dev/null 2>&1; then
  log "WARN oe-alert.sh missing or did not define alert() — alerts will be LOGGED ONLY, not delivered"
  alert() { log "ALERT (undelivered, no working oe-alert.sh): $*"; }
fi

# --- fail-loud destination guards (an unmounted NAS must never look like success) ----------
[ -d "$ARCHIVE_DIR" ] || die "ARCHIVE_DIR '$ARCHIVE_DIR' does not exist (NAS not mounted?) — refusing"
probe="$ARCHIVE_DIR/.oe_write_test.$$"
touch "$probe" 2>/dev/null && rm -f "$probe" || die "ARCHIVE_DIR '$ARCHIVE_DIR' is not writable — refusing"
case "$ARCHIVE_DIR" in
  /mnt/nas/*|/Volumes/nas/*) : ;;
  *) [ "$ALLOW_NON_NAS" = "true" ] || die "'$ARCHIVE_DIR' is not a NAS path. Set ALLOW_NON_NAS=true to stage locally on purpose." ;;
esac

# A NAS-LOOKING PATH IS NOT A MOUNTED NAS. If the CIFS mount drops, /mnt/nas/optionsedge collapses
# to an ordinary empty directory on the root filesystem: still present, still writable, still
# matching the path check above — so every guard passes and the archive silently lands on local
# disk while the log says "NAS". The sentinel is written once, ON the NAS, and can only be read
# back through a live mount. 2026-08-08, after Codex flagged exactly this false-mount-success.
SENTINEL="${SENTINEL:-$ARCHIVE_DIR/.oe_nas_sentinel}"
case "$ARCHIVE_DIR" in
  /mnt/nas/*|/Volumes/nas/*)
    [ -s "$SENTINEL" ] || die "'$ARCHIVE_DIR' looks like a NAS path but the sentinel '$SENTINEL' is missing — the share is NOT mounted. Refusing to write to what is really local disk."
    ;;
esac
[ -x "$KAFKA_BIN/kafka-get-offsets.sh" ] || die "kafka CLI not found at $KAFKA_BIN"

# =============================================================================================
# LOCKING — read this before changing it. Getting the SCOPE wrong cost four sessions of prod data.
# =============================================================================================
# WHAT THE LOCK PROTECTS: $ROOT/_manifest/<topic>.offsets. Two concurrent runs would interleave
# appends to one checkpoint file and could each advance past a range the other never wrote — a GAP,
# and gaps are the one failure this whole script exists to prevent.
#
# WHAT WENT WRONG (2026-08-12): the lock was keyed on (env, destination) only. The nightly heavy
# archive and the */10 spot archive both run ENV=prod against the same NAS, so they shared ONE lock
# even though they touch entirely different topics. The 10-minute job held it at 17:10; the daily
# job logged "exiting clean", returned 0, and archived nothing. It did that on 2026-08-10 and
# 2026-08-11 and nothing alerted, because rc=0 was being read as proof of archival. The sessions
# were later recovered only because these topics happen to carry retention.ms=-1 — see the BACKFILL
# note above. close.direction.signal, which carries a 12h override, was NOT recoverable.
#
# THE FIX, in two layers:
#   JOB LOCK    keyed on (env, JOB, destination). Stops a job overlapping ITSELF — the real
#               "two runs interleaving" hazard — without letting a different job block it.
#   TOPIC LOCK  keyed on (env, destination, TOPIC), taken per topic inside the loop. This is the
#               lock whose subject actually matches what it protects: one <topic>.offsets file.
#               Different jobs with disjoint topic sets (the policy in oe-topics.env) never meet;
#               an ad-hoc run that DOES overlap still cannot corrupt a checkpoint, it just yields
#               that topic.
#
# AND, THE PART THAT MATTERS MOST: a busy lock is only "exiting clean" for a job that will retry
# within its own data's retention window — the */10 spot job, which gets 143 more chances today.
# For a once-a-day job there is no next run before the data expires, so ON_LOCK_BUSY=fail makes
# contention a LOUD FAILURE. Exit 0 must never again be the only evidence that archival happened.
_dir_key="$(printf '%s' "$ARCHIVE_DIR" | cksum | cut -d' ' -f1)"
LOCKFILE="${LOCKFILE:-/tmp/oe-archive-kafka.$ENV_NAME.$ARCHIVE_JOB.$_dir_key.lock}"
exec 8>"$LOCKFILE" || die "cannot open lock $LOCKFILE"
if ! flock -n 8; then
  if [ "$ON_LOCK_BUSY" = "fail" ]; then
    log "LOCK BUSY: another env=$ENV_NAME job=$ARCHIVE_JOB run -> $ARCHIVE_DIR holds $LOCKFILE"
    log "FAILURE: this is a scheduled run that cannot be deferred — its data expires before the next one. NOT archived."
    exit 3
  fi
  log "another archive run for env=$ENV_NAME job=$ARCHIVE_JOB -> $ARCHIVE_DIR holds $LOCKFILE — exiting clean (it will cover this range)"
  exit 0
fi

# The folder date must be the TRADING SESSION date in New York, NOT the UTC date of the run.
# This job fires at 17:10 ET, which is already the NEXT day in UTC — using `date -u` filed
# Thursday's session under dt=2026-07-31. Anything reading these folders by date would then
# train on data labelled with the wrong day. SESSION_DATE may be overridden for backfills.
DAY="${SESSION_DATE:-$(TZ=America/New_York date +%Y-%m-%d)}"
# Stamp every file with the instant it was archived, so a file name alone tells you what it
# holds (topic, partition, offset range) AND when it was captured — no manifest lookup needed.
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ROOT="$ARCHIVE_DIR/kafka/$ENV_NAME"
MAN="$ROOT/_manifest"
mkdir -p "$MAN" || die "cannot create $MAN"

# Free-space guard: stop before filling the disk we are archiving onto.
avail_mb=$(df -Pm "$ARCHIVE_DIR" | tail -1 | awk '{print $4}')
[ "${avail_mb:-0}" -gt 5000 ] || die "only ${avail_mb}MB free on $ARCHIVE_DIR — refusing (need >5GB headroom)"
log "archiving env=$ENV_NAME job=$ARCHIVE_JOB bootstrap=$BOOTSTRAP -> $ROOT (free ${avail_mb}MB)"

# =============================================================================================
# COMPLETENESS RECORD helpers
# =============================================================================================
# ONE decompression pass per file yields all three of: the record count (which is what decides
# whether the checkpoint may advance), the event-time bounds, and any JSON schemaVersion present.
# The old code decompressed once just to `wc -l`; these files run to hundreds of megabytes, so
# folding the scan into that same pass costs nothing.
#
# NOTE ON %d: CONVFMT would render a 13-digit epoch as "1.78645e+12" if these were printed with
# %s. Event-time bounds are the field that makes a mislabelled dt= folder detectable, so they have
# to survive intact.
scan_archive_file() {   # $1=path -> "records min_ms max_ms schema_versions_csv"
  LC_ALL=C zcat "$1" 2>/dev/null | LC_ALL=C awk '
    BEGIN { FS="\t"; n=0; mn=""; mx="" }
    {
      n++
      if (substr($1,1,11) == "CreateTime:") {
        ts = substr($1,12) + 0
        if (ts > 0) { if (mn == "" || ts < mn) mn = ts; if (mx == "" || ts > mx) mx = ts }
      }
      if (match($0, /"schemaVersion":[0-9]+/)) {
        v = substr($0, RSTART + 16, RLENGTH - 16)
        if (!(v in sv)) sv[v] = 1
      }
    }
    END {
      s = ""
      for (v in sv) s = (s == "" ? v : s "," v)
      printf "%d %d %d %s\n", n, (mn == "" ? 0 : mn), (mx == "" ? 0 : mx), (s == "" ? "-" : s)
    }'
}

ms_to_iso() {   # $1=epoch ms -> ISO-8601 UTC, or "unknown"
  case "${1:-0}" in
    ''|*[!0-9]*|0) echo "unknown"; return ;;
  esac
  date -u -d "@$(( $1 / 1000 ))" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "unknown"
}

# Confluent-Avro topics archive as text, so their payload is mangled — but the wire header
# (magic 0x00 + 4-byte big-endian schema id) survives whenever all four id bytes were ASCII. A byte
# >= 0x80 became U+FFFD at WRITE time and the id is unrecoverable; report nothing rather than a
# wrong id. Resolved to a real subject+version through the Schema Registry, once per topic per run.
declare -A AVRO_SCHEMA_CACHE
avro_schema_fragment() {   # $1=topic $2=path -> JSON fragment or ""
  local topic="$1" path="$2" id info subject version
  if [ -n "${AVRO_SCHEMA_CACHE[$topic]+set}" ]; then
    printf '%s' "${AVRO_SCHEMA_CACHE[$topic]}"; return
  fi
  id=$(python3 - "$path" <<'PY' 2>/dev/null
import gzip, sys
with gzip.open(sys.argv[1], 'rb') as f:
    line = f.readline()
parts = line.split(b'\t')
if len(parts) < 4:
    raise SystemExit
val = parts[3]
if not val.startswith(b'\x00') or len(val) < 5:
    raise SystemExit
raw = val[1:5]
if any(b >= 0x80 for b in raw):
    raise SystemExit          # id byte was destroyed by the String decode — do not guess
print(int.from_bytes(raw, 'big'))
PY
  )
  local frag=''
  if [ -n "$id" ]; then
    info=$(curl -fsS -m 5 "$SCHEMA_REGISTRY/schemas/ids/$id/versions" 2>/dev/null)
    subject=$(printf '%s' "$info" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["subject"])' 2>/dev/null)
    version=$(printf '%s' "$info" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["version"])' 2>/dev/null)
    if [ -n "$subject" ] && [ -n "$version" ]; then
      frag=$(printf '"schema_source":"confluent-avro","schema_id":%s,"schema_subject":"%s","schema_version":%s' "$id" "$subject" "$version")
    else
      frag=$(printf '"schema_source":"confluent-avro","schema_id":%s,"schema_subject":"unresolved","schema_version":null' "$id")
    fi
  fi
  AVRO_SCHEMA_CACHE[$topic]="$frag"
  printf '%s' "$frag"
}

schema_fragment() {   # $1=topic $2=path $3=schema_versions_csv -> JSON fragment
  if [ "${3:--}" != "-" ]; then
    printf '"schema_source":"payload","schema_versions":"%s"' "$3"
    return
  fi
  local avro; avro="$(avro_schema_fragment "$1" "$2")"
  if [ -n "$avro" ]; then printf '%s' "$avro"
  else printf '"schema_source":"unavailable"'   # honest: this archive cannot state its schema
  fi
}

total_records=0; total_files=0; failed=0; contended=""; absent=0; rebaselined=0; reset_topics=""; reset_detectors=""
for topic in $TOPICS; do
  # end offsets: "topic:partition:endOffset". A topic that does not exist yields nothing.
  ends=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" 2>/dev/null)
  [ -n "$ends" ] || { log "  SKIP $topic (absent or unreadable)"; absent=$(( absent + 1 )); continue; }

  # Per-topic lock: the only lock whose subject matches what it protects. Held for this topic's
  # whole read-verify-checkpoint cycle, released before the next topic.
  topic_lock="/tmp/oe-archive-kafka.$ENV_NAME.$_dir_key.t-$(printf '%s' "$topic" | tr -c 'A-Za-z0-9._-' '_').lock"
  if ! exec {tfd}>"$topic_lock"; then
    log "  WARN $topic: cannot open topic lock $topic_lock — skipping (refusing to write a checkpoint unguarded)"
    failed=$(( failed + 1 )); continue
  fi
  if ! flock -n "$tfd"; then
    exec {tfd}>&-
    log "  CONTENDED $topic: another job holds $topic_lock — not archived by this run"
    contended="$contended $topic"
    continue
  fi

  offfile="$MAN/$topic.offsets"
  touch "$offfile"
  topic_records=0
  skipped_bad_end=0

  # --- SOURCE IDENTITY: the authoritative re-creation detector --------------------------------
  # Offsets alone cannot answer "is this the same log?". They catch a re-created topic only while
  # it is still SHORTER than the old checkpoint: es4 writes ~60k records per partition per session
  # against a checkpoint of ~174k, so a wipe followed by two busy sessions would slip past
  # unnoticed and the archive would resume mid-log, silently missing everything before it.
  #
  # Kafka's TopicId is a UUID minted at creation, so a delete+create produces a different one even
  # when the new log grows past the old offsets. Comparing it makes the detector exact rather than
  # a heuristic. An absent id (older broker, CLI failure) simply falls back to the offset test.
  idfile="$MAN/$topic.identity"
  topic_id=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe \
               --topic "$topic" 2>/dev/null \
             | awk '{for (i=1;i<=NF;i++) if ($i=="TopicId:") {print $(i+1); exit}}')
  # One value per line, and split on '=' AND whitespace: a single `-F=` on a "k=v k2=v2" line
  # yields "v k2" as field 2, which compares unequal to itself on the very next run and fires a
  # reset every single time. The suite catches exactly that.
  prev_id=$(awk -F'[= ]' '$1=="topic_id"{print $2}' "$idfile" 2>/dev/null | tail -1)
  topic_recreated=false
  if [ -n "$topic_id" ] && [ -n "$prev_id" ] && [ "$topic_id" != "$prev_id" ]; then
    topic_recreated=true
    log "  RESET $topic: TopicId changed $prev_id -> $topic_id — the topic was DELETED and RE-CREATED since the last run"
  fi
  # The new identity is NOT written here — see the end of this topic's loop. Writing it up front
  # would disarm the detector on the very next run if this run died in between: the partitions that
  # had not yet been re-baselined would compare equal, and their prefixes would be skipped
  # silently. The identity is the proof that recovery HAPPENED, so it is recorded after the fact.

  # ONE query per topic, not one per partition. Every heavy topic here has 32 partitions and each
  # of these lookups is a fresh JVM (~2s), so asking per partition cost ~60 wasted seconds per
  # topic — about eight minutes across the nightly set once delta-flow and OPB joined it. A nightly
  # job that overruns is not merely slow: it is a job that is still holding its lock when the next
  # scheduled thing wants to run, which is the family of problem this whole change set is about.
  earliest_all=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
                   --topic "$topic" --time earliest 2>/dev/null)
  until_all=""
  [ -n "$UNTIL_TS" ] && until_all=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
                                      --topic "$topic" --time "$UNTIL_TS" 2>/dev/null)

  while IFS=: read -r _t part endoff; do
    [ -n "${part:-}" ] && [ -n "${endoff:-}" ] || continue

    # The UNCLAMPED log end. The reset test below must use THIS, never the time-bounded bound:
    # in UNTIL_TS mode endoff is deliberately pulled back to a past instant, so a checkpoint that
    # has legitimately archived beyond that instant sits above it — which looks exactly like a
    # re-created log and would trigger a spurious re-baseline, re-reading the whole log into the
    # wrong dt= folder. A reset is defined against the real end of the log, not against a bound we
    # chose ourselves.
    log_end="$endoff"

    # Time-bounded mode: end this partition at the first offset at-or-after UNTIL_TS rather than at
    # the log end, so one past session can be reconstructed into its own dt= folder.
    # An EMPTY answer means no record in this partition is that new — every retained record belongs
    # to the range, so the log end is the correct bound. Treating empty as 0 would silently archive
    # nothing and then checkpoint backwards, which is worse than not running at all.
    if [ -n "$UNTIL_TS" ]; then
      until_off=$(printf '%s\n' "$until_all" \
                  | awk -F: -v p="$part" 'NF>=3 && $2==p && $3 != "" {print $3}' | tail -1)
      if [ -n "$until_off" ]; then
        [ "$until_off" -lt "$endoff" ] && endoff="$until_off"
      fi
    fi
    # Manifest line: "<part>=<endOffset> records=<n> span=<n>". Split on whitespace first so the
    # trailing fields cannot leak into the offset, then take the value after '='.
    from=$(awk -v p="$part" '{split($1,a,"="); if (a[1]==p) print a[2]}' "$offfile" | tail -1)
    # Retention may already have deleted the head of the log, so offset 0 often does NOT exist:
    # underlying.spx.index.price starts at 11462, not 0. Always clamp to the real log-start
    # offset — asking for an expired offset makes the consumer return NOTHING, which the old
    # code then archived as an empty file and checkpointed as success.
    earliest=$(printf '%s\n' "$earliest_all" | awk -F: -v p="$part" '$2==p{print $3}' | tail -1)
    earliest="${earliest:-0}"
    ckpt_before="${from:-none}"
    reset_why=""; reset_tag=""
    if [ -n "$from" ]; then
      # Two independent detectors, checked BEFORE the expired-checkpoint branch because a
      # re-created log can leave the old checkpoint on either side of the new log's start.
      if [ "$topic_recreated" = true ]; then
        reset_why="TopicId changed"; reset_tag="topic-id"
      elif [ "$from" -gt "$log_end" ]; then
        # ⚠️ The offset test is a HEURISTIC and the TopicId is the authority — so when the id is
        # readable and UNCHANGED, a checkpoint above the reported end cannot be a re-creation. It
        # is a bad reading of the end offset, and re-baselining on it DESTROYS the checkpoint.
        #
        # 2026-08-14 in prod: "RESET options.databento.gex.strike p0: checkpoint 1882516 is AHEAD
        # of log end 0" — log end ZERO on a topic holding 58 million records, i.e. an empty/failed
        # kafka-get-offsets response. The archiver re-baselined and the day's session was lost;
        # across twelve archived days only two came out with a full session, and the corpus was
        # unusable for any backtest.
        if [ -n "$topic_id" ] && [ -n "$prev_id" ] && [ "$topic_id" = "$prev_id" ]; then
          log "  SKIP $topic p$part: checkpoint $from is above the reported end $log_end, but the" \
              "TopicId is UNCHANGED ($topic_id) — treating this as a FAILED offset read, not a" \
              "re-created log. Nothing archived and the checkpoint is left alone; the next run" \
              "resumes from it."
          skipped_bad_end=$((skipped_bad_end + 1))
          continue
        fi
        reset_why="checkpoint $from is AHEAD of log end $log_end"; reset_tag="offset-ahead"
      fi
    fi
    if [ -z "$from" ]; then
      from="$earliest"
    elif [ -n "$reset_why" ]; then
      # THE LOG WAS REPLACED. es4's clean-reset (Jenkins es4-deploy ACTION=clean-reset) wipes the
      # Kafka data dir and re-creates the topics, so every partition restarts at 0 and the stored
      # offsets name positions in a log that no longer exists.
      #
      # Untreated this is permanent AND silent, which is the whole family of defect this script
      # exists to prevent: count=(endoff-from) goes negative, the `-gt 0` guard below skips the
      # partition, and the run still reports failed=0. It cost the entire 2026-08-11 ES session,
      # and by 2026-08-13 every partition of every live es4 topic was in this state — the job would
      # never have archived another record for that environment, while reporting success daily.
      #
      # Re-baseline to the log start: after a reset every retained record is unarchived. This can
      # re-read data an earlier incarnation already captured, but a duplicate is recoverable and a
      # gap is not — the same trade this script already makes on a mid-run crash.
      log "  RESET $topic p$part: $reset_why — the log was replaced (topic re-created); re-baselining to $earliest"
      from="$earliest"
      rebaselined=$(( rebaselined + 1 ))
      reset_topics=$(printf '%s %s' "$reset_topics" "$topic" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ')
      reset_detectors=$(printf '%s %s' "$reset_detectors" "$reset_tag" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ')

      # Persist the corrected baseline NOW, before deciding whether there is anything to read.
      #
      # Everywhere else this script refuses to advance a checkpoint before the data is safely
      # archived, because advancing early SKIPS records. This write is the opposite direction —
      # it moves the checkpoint BACKWARDS onto the real log start — so its worst case is re-reading,
      # never skipping, and the rule it protects is not violated.
      #
      # It is required, not tidiness: a partition that is EMPTY after the reset (endoff == earliest)
      # hits the `-gt 0` guard below and never reaches the code that appends a checkpoint, so its
      # stale line would survive and the next run would report a RESET again. That is a false alarm
      # every single day, which is how a real alert stops being read.
      printf '%s=%s records=0 span=0 dt=%s archived=%s rebaselined=from-%s\n' \
        "$part" "$earliest" "$DAY" "$STAMP" "$ckpt_before" >> "$offfile"
    elif [ "$from" -lt "$earliest" ]; then
      log "  GAP $topic p$part: checkpoint $from expired (log now starts at $earliest) — $((earliest-from)) records LOST before this run"
      from="$earliest"
    fi
    count=$(( endoff - from ))
    [ "$count" -gt 0 ] || continue

    outdir="$ROOT/$topic/dt=$DAY"; mkdir -p "$outdir"
    # <topic>.p<partition>.<from>-<to>.dt<sessionDate>.<archivedAtUTC>.jsonl.gz
    # The session day is repeated INSIDE the name on purpose: copied to a NAS, attached to a
    # ticket, or dropped into a training bucket, the file still states which trading day it
    # belongs to without its parent folder.
    out="$outdir/$topic.p$part.$from-$endoff.dt${DAY//-/}.$STAMP.jsonl.gz"
    tmp="$out.partial"

    # NOTE: `consumer | gzip` reports GZIP's exit status, so a consumer that emitted zero records
    # still "succeeds". That is how the first version silently archived empty files and advanced
    # its checkpoints. Verify by COUNTING what actually landed, then commit.
    timeout 900 "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
         --topic "$topic" --partition "$part" --offset "$from" --max-messages "$count" \
         --formatter-property print.timestamp=true \
         --formatter-property print.key=true \
         --formatter-property print.partition=true \
         --timeout-ms 60000 2>/dev/null | grep -av '^Processed a total of' | gzip -6 > "$tmp"
    consumer_rc=${PIPESTATUS[0]}

    read -r got min_ms max_ms schema_versions <<< "$(scan_archive_file "$tmp")"
    got="${got:-0}"; min_ms="${min_ms:-0}"; max_ms="${max_ms:-0}"; schema_versions="${schema_versions:--}"
    # got < count is NORMAL on a COMPACTED topic: offsets advance but compaction removes all but
    # the newest record per key, so the readable count is far below (end-from). underlying.spx.price
    # is the extreme case — 642,060 offsets, ~2,000 readable records. Judging by count alone would
    # mark every compacted topic as failed forever. Judge by the CONSUMER's exit status instead,
    # and record both numbers so the compaction ratio is visible in the manifest.
    if [ "$consumer_rc" -eq 0 ] && [ "$got" -gt 0 ]; then
      # Only advance the checkpoint once the file is verified and durably in place. A crash
      # mid-run therefore re-reads the same range next time (duplicates) instead of skipping it —
      # duplicates are recoverable, gaps are not.
      #
      # The mv MUST be gated. This script runs without `set -e`, so an unchecked `mv` that failed —
      # a full NAS, a stale mount, a permissions change — would fall straight through to the
      # checkpoint write below, and the next run would resume past a range whose file does not
      # exist. That is the same silent gap this whole change set exists to close, arrived at from
      # the other direction.
      if ! mv "$tmp" "$out"; then
        rm -f "$tmp"
        log "  WARN $topic p$part [$from,$endoff): could not publish $out — checkpoint NOT advanced, will retry next run"
        failed=$(( failed + 1 ))
        continue
      fi
      printf '%s=%s records=%s span=%s dt=%s archived=%s\n' \
        "$part" "$endoff" "$got" "$count" "$DAY" "$STAMP" >> "$offfile"

      # Completeness record. sha256 is over the gzip stream as committed, so a later bit-rot or a
      # truncated copy is detectable without a broker. Written AFTER the mv so a line in this file
      # always refers to a file that exists.
      sha=$(sha256sum "$out" 2>/dev/null | cut -d' ' -f1)
      bytes=$(stat -c%s "$out" 2>/dev/null || echo 0)
      printf '{"topic":"%s","dt":"%s","partition":%s,"offset_from":%s,"offset_to":%s,"records":%s,"offset_span":%s,"min_event_time_ms":%s,"max_event_time_ms":%s,"min_event_time":"%s","max_event_time":"%s",%s,"sha256":"%s","bytes":%s,"file":"%s","archived_at":"%s","job":"%s","env":"%s","archiver_version":"%s"}\n' \
        "$topic" "$DAY" "$part" "$from" "$endoff" "$got" "$count" \
        "$min_ms" "$max_ms" "$(ms_to_iso "$min_ms")" "$(ms_to_iso "$max_ms")" \
        "$(schema_fragment "$topic" "$out" "$schema_versions")" \
        "${sha:-unknown}" "$bytes" "$(basename "$out")" "$STAMP" "$ARCHIVE_JOB" "$ENV_NAME" "$ARCHIVER_VERSION" \
        >> "$outdir/_manifest.jsonl"

      topic_records=$(( topic_records + got )); total_files=$(( total_files + 1 ))
      if [ "$got" -lt "$count" ]; then
        log "  NOTE $topic p$part: $got readable of $count offsets (compacted topic — history is NOT recoverable from it)"
      fi
    else
      rm -f "$tmp"
      log "  WARN $topic p$part [$from,$endoff): consumer rc=$consumer_rc, got $got — checkpoint NOT advanced, will retry next run"
      failed=$(( failed + 1 ))
    fi
  done <<< "$ends"

  # Record the observed identity ONLY now, with every partition of this topic processed, and
  # atomically so a kill between write and rename cannot leave a half-written id that matches
  # nothing. Ordering is the whole point: written before the loop, a crash in the middle would
  # leave the new id stored against partitions that were never re-baselined, and the next run
  # would see id==id, skip the reset, and resume mid-log — silently missing the prefix. Written
  # after, the worst case is repeating a recovery that already happened, i.e. duplicates.
  if [ -n "$topic_id" ]; then
    printf 'topic_id=%s\nobserved=%s\n' "$topic_id" "$STAMP" > "$idfile.tmp" && mv -f "$idfile.tmp" "$idfile"
  fi

  exec {tfd}>&-
  [ "$topic_records" -gt 0 ] && log "  $topic: +$topic_records records"
  total_records=$(( total_records + topic_records ))
done

# Contention on a topic is a real miss, not a detail: this run did not archive it. A job that can
# retry inside its data's retention window may treat that as fine; a once-a-day job may not.
n_contended=$(printf '%s' "$contended" | wc -w | tr -d ' ')
if [ "$n_contended" -gt 0 ]; then
  log "CONTENDED topics not archived by this run:$contended"
  [ "$ON_LOCK_BUSY" = "fail" ] && failed=$(( failed + n_contended ))
fi

# A re-baseline is a RECOVERY, not a failure: this run did archive the partition. But it is also
# proof that something wiped the log since the last run, so whatever that log held before the wipe
# was never archived and is unrecoverable. That is exactly the news a daily archive must not keep
# to itself, so it is alerted on and carried in runs.log — while the exit status stays governed by
# real failures, so a caller does not retry a run that actually succeeded.
if [ "$rebaselined" -gt 0 ]; then
  reset_topics="${reset_topics% }"; reset_detectors="${reset_detectors% }"
  log "REBASELINED $rebaselined partition(s) after a log reset — topics: $reset_topics (detected by: $reset_detectors)"
  # Report which detector actually fired: the two are not interchangeable, and naming the wrong
  # one sends whoever reads this looking at the wrong thing. And do not overstate the loss — the
  # previous log was archived up to its last good checkpoint; what is unrecoverable is only
  # whatever it accumulated AFTER that, which this job cannot measure once the log is gone.
  alert "$(printf 'archive %s/%s: log RESET on %s partition(s) — topics: %s (detected by: %s)\nThe topics were re-created since the last run. This run re-baselined and archived what the NEW log holds. Whatever the previous log accumulated after its last successful checkpoint was never archived and is unrecoverable; this job cannot tell how much that was. Check whether the pre-wipe session was captured.' \
      "$ENV_NAME" "$ARCHIVE_JOB" "$rebaselined" "$reset_topics" "$reset_detectors")"
fi

printf '%s env=%s job=%s records=%s files=%s failed=%s contended=%s absent=%s rebaselined=%s dt=%s\n' \
  "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$ENV_NAME" "$ARCHIVE_JOB" "$total_records" "$total_files" \
  "$failed" "$n_contended" "$absent" "$rebaselined" "$DAY" >> "$MAN/runs.log"
log "DONE records=$total_records files=$total_files failed=$failed contended=$n_contended absent=$absent rebaselined=$rebaselined"
[ "$failed" -eq 0 ] || exit 1
