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

# A5: topics whose ARCHIVE IS EVIDENCE. For an ordinary topic a quiet day, a compacted short read or
# a recreated log are all normal and the run continues. For these the same events mean the corpus has
# a hole, and a hole that is not reported is worse than one that is: the reader would count a smaller
# population as complete. Every check below that says "strict" is gated on this list and nothing else.
OE_STRICT_TOPICS="${OE_STRICT_TOPICS:-context-tape.direction.ledger}"
is_strict_topic() { case " $OE_STRICT_TOPICS " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
# Topics read COMMITTED-ONLY, through StrikeArchiveReader.java instead of kafka-console-consumer.
# See the capture block below for why this is a list and not the default: every topic NOT named here
# is captured exactly as before, byte for byte; an omission here is today's behaviour, never a new gap.
OE_COMMITTED_READ_TOPICS="${OE_COMMITTED_READ_TOPICS:-es.futures.footprint.strike}"
is_committed_read_topic() { case " $OE_COMMITTED_READ_TOPICS " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
# THE CHECKPOINT STAMP (deploy re-review round 2, finding 2). Every checkpoint line the committed-read
# capture writes ends with this token, and for a committed-read topic ONLY a stamped line is proof of
# archival. An unstamped one was written by something else — in practice the console-consumer archiver
# this reader replaced, which could stop below an open transaction and still checkpoint the high-water
# mark (final review, finding 1: "endoff=1200, got=1099 -> ACCEPT checkpoint=1200"). Resuming from such a
# line would carry that skip forward for ever, and marking it on the source broker would let
# cleanup-es4.sh wipe records no archive holds. So an unstamped line is treated as NO checkpoint: the
# partition is recaptured from the log start (LEGACY below). The recapture duplicates whatever that
# archiver did capture; a duplicate is recoverable and a skipped range is not. Console-consumer topics
# never carry or need the stamp; their checkpoint lines are unchanged byte for byte.
COMMITTED_CKPT_STAMP="capture=read_committed_stable_boundary"
ckpt_is_stamped() { case " $1 " in *" $COMMITTED_CKPT_STAMP "*) return 0 ;; *) return 1 ;; esac; }
# THE SOURCE IDENTITY IN THE STAMP (deploy re-review round 3, finding P1). An offset names a position in ONE log.
# es4's clean-reset re-creates the topic, the new log grows back past the old checkpoint, and "0=7 ... stamped"
# then describes records that no longer exist — while the new log's 0..6 were never read. So every stamped line
# also carries the TopicId of the log it was taken on (" topic_id=<id>"), and it is PROOF only on a log whose
# TopicId this run read successfully AND found equal. A different id is a new log: recaptured from its start.
# A stamped line without an id (written by the round-2 archiver, before this rule) proves nothing and is
# recaptured like an unstamped one. An id this run cannot read fails the topic: nothing is resumed, nothing is
# marked. No cluster id is stamped: es4's KRaft CLUSTER_ID is fixed in infra/es4/docker-compose.yml and survives
# the wipe, so it cannot tell the two logs apart; the TopicId, minted at every creation, can.
ckpt_topic_id() { printf '%s\n' "$1" | tr ' ' '\n' | awk -F= '$1 == "topic_id" { sub(/^topic_id=/, ""); print; exit }'; }
ckpt_is_proven() { # $1=line $2=the TopicId this run read: stamped, and taken on THIS log
  [ -n "$2" ] && ckpt_is_stamped "$1" && [ "$(ckpt_topic_id "$1")" = "$2" ]
}
# The one unstamped line that may still be resumed from: a re-baseline the archiver wrote after a reset,
# which points at the NEW log's start — and only while it is still at or below the retained start, so that
# resuming there cannot skip a record. Anything else unstamped is LEGACY. $1=line $2=its offset $3=earliest
ckpt_is_log_start() { case " $1 " in *" rebaselined=from-"*) [ "$2" -le "$3" ] 2>/dev/null ;; *) return 1 ;; esac; }
# What a Kafka CLI prints on stderr when a read did not (fully) succeed — GetOffsetShell's per-partition
# "Skip getting offsets ... due to error" at exit 0, "Error occurred: ...", the AdminClient's retry WARNs,
# exception names. The same list scripts/es4/strike-archive-interlock.sh refuses on; the real answers are
# recorded in broker-test/cli-fixtures/.
KAFKA_CLI_DIAG='skip|error|exception|fail|timed out|timeout|could not|unable|not available|refused'

# kafka_answer_why <rc> <file-prefix> <stdout-shape-regex or ''> -> why the answer in <prefix>.out / <prefix>.err
# cannot be used, or nothing. The SAME rules as _sai_why in scripts/es4/strike-archive-interlock.sh, whose body this
# is (strike-archive-interlock-test.sh §7 compares the two): a non-zero exit, a diagnostic on stderr, an "Error"
# line on stdout, or a stdout line outside the expected shape. Silence is never read as an answer.
kafka_answer_why() {
  local rc="$1" f="$2" shape="$3" diag bad
  diag=$(grep -m1 -iE "$KAFKA_CLI_DIAG" "$f.err" 2>/dev/null | cut -c1-240)
  if [ "$rc" -ne 0 ]; then
    bad=$(grep -m1 -iE 'error' "$f.out" 2>/dev/null | cut -c1-240)
    printf 'exit %s: %s' "$rc" "${bad:-${diag:-$(tail -1 "$f.err" 2>/dev/null | cut -c1-240)}}"
    return
  fi
  if [ -n "$diag" ]; then printf 'exit 0 but it reported: %s' "$diag"; return; fi
  bad=$(grep -m1 -E '^Error' "$f.out" 2>/dev/null | cut -c1-240)
  if [ -n "$bad" ]; then printf 'exit 0 but it printed: %s' "$bad"; return; fi
  if [ -n "$shape" ]; then
    bad=$(grep -v -E "$shape" "$f.out" 2>/dev/null | grep -m1 . | cut -c1-240)
    [ -z "$bad" ] || printf 'unexpected output line: %s' "$bad"
  fi
}
kafka_seq() { local i=0; while [ "$i" -lt "$1" ]; do printf '%s ' "$i"; i=$((i + 1)); done; }

# read_topic_description <topic> <scratch dir> -> 0 with DESC_N (PartitionCount) and DESC_ID (TopicId), or 1 with
# DESC_WHY. The partition lines must be exactly 0..N-1, as in the interlock, and the TopicId must be a real one:
# 22 base64url characters and not the all-zero id a broker reports for a log that has none.
read_topic_description() {
  local topic="$1" d="$2" rc parts
  DESC_N=""; DESC_ID=""; DESC_WHY=""
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" > "$d/desc.out" 2> "$d/desc.err"
  rc=$?
  DESC_WHY=$(kafka_answer_why "$rc" "$d/desc" '')
  [ -z "$DESC_WHY" ] || return 1
  # "Topic: T<TAB>TopicId: X<TAB>PartitionCount: N<TAB>..." then "<TAB>Topic: T<TAB>Partition: p<TAB>..." (recorded in
  # broker-test/cli-fixtures/<version>/describe-*.out). --topic is a regex in the CLI: other topics' lines are ignored.
  DESC_N=$(awk -F'\t' -v t="Topic: $topic" '$1 == t { for (i = 2; i <= NF; i++) if ($i ~ /^PartitionCount: [0-9]+$/) { sub(/^PartitionCount: /, "", $i); print $i } }' "$d/desc.out")
  DESC_ID=$(awk -F'\t' -v t="Topic: $topic" '$1 == t { for (i = 2; i <= NF; i++) if ($i ~ /^TopicId: /) { sub(/^TopicId: /, "", $i); print $i } }' "$d/desc.out")
  parts=$(awk -F'\t' -v t="Topic: $topic" '$1 == "" && $2 == t && $3 ~ /^Partition: [0-9]+$/ { sub(/^Partition: /, "", $3); print $3 }' "$d/desc.out" | sort -n | tr '\n' ' ')
  case "$DESC_N" in
    ''|*[!0-9]*|0) DESC_WHY="no single PartitionCount for $topic in its description"; return 1 ;;
  esac
  [ "$parts" = "$(kafka_seq "$DESC_N")" ] || { DESC_WHY="PartitionCount $DESC_N, but partition lines [${parts% }]"; return 1; }
  case "$DESC_ID" in
    ''|*[!A-Za-z0-9_-]*) DESC_WHY="no single TopicId for $topic in its description (got '$(printf '%s' "$DESC_ID" | tr '\n' ' ')')"; return 1 ;;
    AAAAAAAAAAAAAAAAAAAAAA) DESC_WHY="the broker reports the all-zero TopicId — a log without an identity"; return 1 ;;
  esac
  [ "${#DESC_ID}" -eq 22 ] || { DESC_WHY="'$DESC_ID' is not a TopicId (22 base64url characters)"; return 1; }
}

# INITIAL DISCOVERY FOR A COMMITTED-READ TOPIC (deploy re-review round 3, finding P2). The console path below takes
# an empty `kafka-get-offsets` answer as "absent", with stderr discarded and the status ignored — so an unreachable
# broker (exit 1, nothing on stdout: recorded in broker-test/cli-fixtures/) or GetOffsetShell's per-partition
# "Skip getting offsets ... due to error" at exit 0 became SKIP or a silently missing partition, the run reported
# failed=0, and the UNTIL_TS validation further down was never reached. For a committed-read topic nothing is
# concluded from silence, by the interlock's rules: (1) presence from a topic list that was READ (absent from it =
# confirmed absent, still SKIP); (2) partition count and TopicId from --describe; (3) log ends and (4) log starts
# from kafka-get-offsets, each exit 0, no diagnostic, only "topic:p:offset" lines and exactly partitions 0..N-1.
# Sets DISC_STATE present|absent|unreadable, DISC_WHY, DISC_ID, DISC_ENDS, DISC_EARLIEST ("topic:p:offset" lines).
committed_discovery() {
  local topic="$1" d rc why got q
  DISC_STATE=unreadable; DISC_WHY=""; DISC_ID=""; DISC_ENDS=""; DISC_EARLIEST=""
  d=$(mktemp -d "${TMPDIR:-/tmp}/oe-archive-disc.XXXXXX" 2>/dev/null) || d=""
  [ -n "$d" ] || { DISC_WHY="could not create a scratch directory for the broker's answers"; return 0; }
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > "$d/list.out" 2> "$d/list.err"
  rc=$?
  why=$(kafka_answer_why "$rc" "$d/list" '^[A-Za-z0-9._-]+$')
  if [ -n "$why" ]; then
    DISC_WHY="the broker's topic list cannot be read (kafka-topics --list: $why)"; rm -rf "$d"; return 0
  fi
  if ! grep -Fxq -- "$topic" "$d/list.out"; then DISC_STATE=absent; rm -rf "$d"; return 0; fi
  if ! read_topic_description "$topic" "$d"; then
    DISC_WHY="its partitions and TopicId cannot be established (kafka-topics --describe: $DESC_WHY)"; rm -rf "$d"; return 0
  fi
  DISC_ID="$DESC_ID"
  for q in latest earliest; do
    if [ "$q" = latest ]; then
      "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" > "$d/$q.out" 2> "$d/$q.err"
    else
      "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" --time earliest > "$d/$q.out" 2> "$d/$q.err"
    fi
    rc=$?
    why=$(kafka_answer_why "$rc" "$d/$q" '^[^:]+:[0-9]+:[0-9]+$')
    if [ -z "$why" ]; then
      got=$(awk -F: -v t="$topic" '$1 == t { print $2 }' "$d/$q.out" | sort -n | tr '\n' ' ')
      [ "$got" = "$(kafka_seq "$DESC_N")" ] \
        || why="offsets came back for partitions [${got% }] of the $DESC_N the topic has — a partition that is not read is not archived"
    fi
    if [ -n "$why" ]; then
      DISC_WHY="its $q offsets cannot be read (kafka-get-offsets: $why)"; rm -rf "$d"; return 0
    fi
  done
  DISC_ENDS=$(awk -F: -v t="$topic" '$1 == t' "$d/latest.out")
  DISC_EARLIEST=$(awk -F: -v t="$topic" '$1 == t' "$d/earliest.out")
  DISC_STATE=present
  rm -rf "$d"
}

# The committed-only reader. A single-file Java program run by the JDK's source launcher against the
# Kafka client jars the broker CLI already ships ($KAFKA_BIN/../libs), so it needs no build step and
# travels in the archive deploy UNIT beside this script. It takes its boundary from Kafka METADATA
# (the read_committed end offset, i.e. the last stable offset), writes only records below it, and
# reports completion only when its position reached it — see the header of StrikeArchiveReader.java.
# STRIKE_READER is the test seam: an executable taking the same arguments (test-archive-reset.sh).
STRIKE_READER="${STRIKE_READER:-}"
STRIKE_READER_SRC="${STRIKE_READER_SRC:-$(cd "$(dirname "$0")" && pwd)/StrikeArchiveReader.java}"
STRIKE_READER_DEADLINE_S="${STRIKE_READER_DEADLINE_S:-900}"
case "$STRIKE_READER_DEADLINE_S" in ''|*[!0-9]*|0) echo "FATAL: STRIKE_READER_DEADLINE_S must be a positive integer, got '$STRIKE_READER_DEADLINE_S'" >&2; exit 2 ;; esac
# $1 = the OUTER wall-clock limit in seconds; the rest are the reader's own arguments. The limit is
# applied in here because `timeout` runs programs, not shell functions (a `timeout N run_strike_reader`
# call site fails with 127 on every run — the suite caught exactly that).
run_strike_reader() {
  local limit="$1"; shift
  if [ -n "$STRIKE_READER" ]; then timeout "$limit" "$STRIKE_READER" "$@"; return; fi
  timeout "$limit" "${JAVA_HOME:+$JAVA_HOME/bin/}java" -Xmx256m -cp "$KAFKA_BIN/../libs/*" "$STRIKE_READER_SRC" "$@"
}
# THE ARCHIVE MARKER. After a committed-read capture is durably published and its checkpoint written,
# the same boundary is committed as the offset of this consumer group ON THE SOURCE broker. It is the
# only archive state a host without the NAS can read: scripts/es4/cleanup-es4.sh compares it against
# the log end before it wipes es4's Kafka data dir, and refuses while any record is unarchived. Only
# es4's source is wiped wholesale, so only ENV=es4 writes it unless told otherwise; the group name is
# a contract with cleanup-es4.sh (ES4_STRIKE_ARCHIVE_GROUP there) — change both or neither.
OE_ARCHIVE_MARK_GROUP="${OE_ARCHIVE_MARK_GROUP:-oe-archive-committed-boundary}"
OE_ARCHIVE_MARK_SOURCE="${OE_ARCHIVE_MARK_SOURCE:-$([ "$ENV_NAME" = es4 ] && echo true || echo false)}"

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

# The reader's one-line summary, field by field ("k=v" words). Empty when absent — every caller
# validates what it gets, because a summary it cannot read is a capture it must not trust.
summary_field() {   # $1=summary line $2=key
  printf '%s\n' "$1" | tr ' ' '\n' | awk -F= -v k="$2" '$1==k { sub(/^[^=]*=/, ""); print; exit }'
}

# Record "archived through <offset>" on the SOURCE broker (see OE_ARCHIVE_MARK_GROUP above). Called
# only once the checkpoint that says the same thing is durably written. A failure here is LOGGED, not
# counted as a failed archive: the capture is safe on the NAS either way, and the only consequence is
# that cleanup-es4.sh keeps refusing to wipe until a later run records the marker — the safe direction.
mark_committed_boundary() {   # $1=topic $2=partition $3=offset $4=the TopicId this run matched
  [ "$OE_ARCHIVE_MARK_SOURCE" = "true" ] || return 0
  local msum="$MAN/.$1.p$2.$STAMP.mark.summary" mlog="$MAN/.$1.p$2.$STAMP.mark.log" mrc mline md
  # The marker certifies a LOG (re-review round 3, finding P1), so it is written only for the log whose TopicId
  # this run matched — and that id is read AGAIN here, just before the write, so a topic re-created while this
  # run was reading it is never certified. Unconfirmed = not written, logged like a failed marker write.
  DESC_WHY=""; DESC_ID=""
  md=$(mktemp -d "${TMPDIR:-/tmp}/oe-archive-mark.XXXXXX" 2>/dev/null) || md=""
  if [ -z "$4" ] || [ -z "$md" ] || ! read_topic_description "$1" "$md" || [ "$DESC_ID" != "$4" ]; then
    log "  WARN $1 p$2: NOT recording archived-through $3 on the source broker — its identity could not be" \
        "re-confirmed as TopicId ${4:-<none>} (${DESC_WHY:-the broker now reports ${DESC_ID:-nothing}}). The archive" \
        "itself is intact; cleanup-es4.sh will refuse to wipe until a later run records it"
    [ -z "$md" ] || rm -rf "$md"
    return 1
  fi
  rm -rf "$md"
  run_strike_reader 180 --bootstrap "$BOOTSTRAP" --topic "$1" --partition "$2" \
      --mark-group "$OE_ARCHIVE_MARK_GROUP" --mark-offset "$3" \
      --mark-metadata "dt=$DAY,archived=$STAMP,job=$ARCHIVE_JOB" --deadline-ms 60000 \
      --summary "$msum" > "$mlog" 2>&1
  mrc=$?
  mline=$(head -1 "$msum" 2>/dev/null)
  rm -f "$msum" "$msum.tmp" "$mlog"
  if [ "$mrc" -eq 0 ] && [ "$(summary_field "$mline" status)" = COMPLETE ] \
     && [ "$(summary_field "$mline" position)" = "$3" ]; then
    log "  MARK $1 p$2: archived-through $3 recorded on the source broker (group $OE_ARCHIVE_MARK_GROUP)"
    return 0
  fi
  log "  WARN $1 p$2: could not record archived-through $3 on the source broker (rc=$mrc: ${mline:-no summary}) —" \
      "the archive itself is intact; cleanup-es4.sh will refuse to wipe until a later run records it"
  return 1
}

total_records=0; total_files=0; failed=0; contended=""; absent=0; rebaselined=0; reset_topics=""; reset_detectors=""
for topic in $TOPICS; do
  if is_committed_read_topic "$topic"; then
    # Validated discovery (committed_discovery above). Unreadable or incomplete = a FAILED topic for this run:
    # nothing read, no marker, checkpoint untouched — never the SKIP an empty answer gets on the console path.
    committed_discovery "$topic"
    case "$DISC_STATE" in
      absent)
        if is_strict_topic "$topic"; then
          log "  FAIL $topic: STRICT topic is absent (not in the broker's topic list) — the corpus cannot be completed for $DAY"
          failed=$(( failed + 1 ))
        else
          log "  SKIP $topic (absent: not in the broker's topic list, which was read successfully)"; absent=$(( absent + 1 ))
        fi
        continue ;;
      present) ends="$DISC_ENDS" ;;
      *)
        log "  FAIL $topic: $DISC_WHY — nothing archived for this topic, no archive marker written, its checkpoint is unchanged"
        failed=$(( failed + 1 ))
        continue ;;
    esac
  else
  # CONSOLE-CONSUMER TOPICS: unchanged (re-review round 3 records why). end offsets: "topic:partition:endOffset".
  # A topic that does not exist yields nothing.
  ends=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" 2>/dev/null)
  fi
  if [ -z "$ends" ]; then
    if is_strict_topic "$topic"; then
      # A5: an absent evidence topic is not a quiet day. Either it was never created or something
      # deleted it, and both mean the corpus is missing a session it can never get back.
      log "  FAIL $topic: STRICT topic is absent or unreadable — the corpus cannot be completed for $DAY"
      failed=$(( failed + 1 ))
    else
      log "  SKIP $topic (absent or unreadable)"; absent=$(( absent + 1 ))
    fi
    continue
  fi

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
  id_write_blocked=0
  if is_committed_read_topic "$topic"; then
    # Read and VALIDATED by committed_discovery: a committed-read topic never gets here without its TopicId, so
    # for it the "absent id falls back to the offset test" sentence above does not apply — it fails instead.
    topic_id="$DISC_ID"
  else
  topic_id=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe \
               --topic "$topic" 2>/dev/null \
             | awk '{for (i=1;i<=NF;i++) if ($i=="TopicId:") {print $(i+1); exit}}')
  fi
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
  if is_committed_read_topic "$topic"; then
    earliest_all="$DISC_EARLIEST"      # validated, every partition (committed_discovery)
  else
  earliest_all=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
                   --topic "$topic" --time earliest 2>/dev/null)
  fi
  until_all=""
  if [ -n "$UNTIL_TS" ]; then
    # THE CUTOFF MUST BE READ, NOT ASSUMED (deploy re-review round 2, finding 5). Below, a partition with
    # no line in this answer is read to the LOG END, because GetOffsetShell drops a partition that has no
    # record at or after the timestamp (it prints nothing for it, at exit 0 — recorded in
    # broker-test/cli-fixtures/<version>/until-nomatch.*). But a FAILED lookup also prints no line: an
    # unreachable broker exits 1 with nothing on stdout, and a per-partition error is "Skip getting
    # offsets ..." on stderr at exit 0. Reading that silence as "no record that new" dropped the cutoff:
    # checkpoint 0, cutoff 40, log end 100 archived all 100 records under the historical dt= and moved the
    # checkpoint to 100, so the later sessions' runs skipped them. So the answer is used only if the
    # command exited 0, reported nothing on stderr and printed nothing but "topic:partition:offset" lines;
    # anything else fails this topic for this run, with its checkpoint untouched.
    until_err=$(mktemp "${TMPDIR:-/tmp}/oe-archive-until.XXXXXX" 2>/dev/null)
    until_why=""
    if [ -z "$until_err" ]; then
      until_why="could not create a scratch file for the lookup's stderr"
    else
      until_all=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
                    --topic "$topic" --time "$UNTIL_TS" 2>"$until_err")
      until_rc=$?
      [ "$until_rc" -eq 0 ] || until_why="exit $until_rc"
      until_diag=$(grep -m1 -iE "$KAFKA_CLI_DIAG" "$until_err" 2>/dev/null | cut -c1-240)
      [ -z "$until_diag" ] || until_why="${until_why:+$until_why, }stderr: $until_diag"
      until_bad=$(printf '%s\n' "$until_all" | grep -vE '^[^:]+:[0-9]+:[0-9]+$' | grep -m1 . | cut -c1-240)
      [ -z "$until_bad" ] || until_why="${until_why:+$until_why, }unexpected output: $until_bad"
      rm -f "$until_err"
    fi
    if [ -n "$until_why" ]; then
      log "  FAIL $topic: the UNTIL_TS=$UNTIL_TS offset lookup did not succeed ($until_why) — the historical cutoff" \
          "is unknown, and reading to the log end would file later sessions under dt=$DAY. Nothing archived for" \
          "this topic; its checkpoint is unchanged."
      failed=$(( failed + 1 ))
      exec {tfd}>&-
      continue
    fi
  fi

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
    # nothing and then checkpoint backwards, which is worse than not running at all. (An empty answer
    # means that ONLY because the lookup itself was validated above; a failed one never reaches here.)
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
    # The same last line, whole: a committed-read topic's checkpoint counts only if it is stamped.
    from_line=$(awk -v p="$part" '{split($1,a,"="); if (a[1]==p) print}' "$offfile" | tail -1)
    # Retention may already have deleted the head of the log, so offset 0 often does NOT exist:
    # underlying.spx.index.price starts at 11462, not 0. Always clamp to the real log-start
    # offset — asking for an expired offset makes the consumer return NOTHING, which the old
    # code then archived as an empty file and checkpointed as success.
    earliest=$(printf '%s\n' "$earliest_all" | awk -F: -v p="$part" '$2==p{print $3}' | tail -1)
    earliest="${earliest:-0}"
    ckpt_before="${from:-none}"
    reset_why=""; reset_tag=""
    # from_proven: `from` is a checkpoint the committed reader itself wrote (stamped), not a log start,
    # a re-baseline or an inherited line. Only such a checkpoint may be re-recorded as the archive marker.
    legacy_from=""; from_proven=false
    # The TopicId a committed-read checkpoint was taken on (empty for every console-consumer topic, and for an
    # unstamped or pre-round-3 line) — see ckpt_is_proven.
    ckpt_id=""
    if is_committed_read_topic "$topic" && ckpt_is_stamped "$from_line"; then ckpt_id=$(ckpt_topic_id "$from_line"); fi
    if [ -n "$from" ]; then
      # Two independent detectors, checked BEFORE the expired-checkpoint branch because a
      # re-created log can leave the old checkpoint on either side of the new log's start.
      if [ "$topic_recreated" = true ]; then
        reset_why="TopicId changed"; reset_tag="topic-id"
      elif [ -n "$ckpt_id" ] && [ "$ckpt_id" != "$topic_id" ]; then
        # Committed read (round 3, P1): the checkpoint itself names the log it was taken on, so a re-created
        # topic is caught even when the identity file cannot tell (missing, or never written) — and even when
        # the new log has grown back past the old offset, where the offset test below sees nothing.
        reset_why="checkpoint $from was taken on TopicId $ckpt_id, the log is now $topic_id"; reset_tag="checkpoint-topic-id"
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
        if { [ -n "$topic_id" ] && [ -n "$prev_id" ] && [ "$topic_id" = "$prev_id" ]; } \
           || { [ -n "$ckpt_id" ] && [ "$ckpt_id" = "$topic_id" ]; }; then
          log "  SKIP $topic p$part: checkpoint $from is above the reported end $log_end, but the" \
              "TopicId is UNCHANGED ($topic_id) — treating this as a FAILED offset read, not a" \
              "re-created log. Nothing archived and the checkpoint is left alone; the next run" \
              "resumes from it."
          skipped_bad_end=$((skipped_bad_end + 1))
          if is_committed_read_topic "$topic"; then
            # A committed-read topic's unreadable end is a failed run, not a quiet one (round 3, P2).
            log "  FAIL $topic p$part: an end offset below its own checkpoint on the same log cannot be a true reading"
            failed=$(( failed + 1 ))
          fi
          continue
        fi
        reset_why="checkpoint $from is AHEAD of log end $log_end"; reset_tag="offset-ahead"
      fi
    fi
    if [ -n "$reset_why" ] && is_strict_topic "$topic"; then
      # A5: rebaselining recovers the NEW log; it is not evidence that the previous session survived.
      # For a strict topic the discontinuity is recorded and the run FAILS, so the reader marks every
      # session the damaged offset range spans NOT_EVALUABLE instead of accepting a fresh suffix.
      log "  FAIL $topic p$part: STRICT topic log RESET ($reset_why) — the pre-reset population is unrecoverable"
      printf '{"topic":"%s","dt":"%s","partition":%s,"discontinuity":"%s","detected_at":"%s","env":"%s"}\n' \
        "$topic" "$DAY" "$part" "$reset_why" "$STAMP" "$ENV_NAME" >> "$MAN/$topic.discontinuities.jsonl"
      # REBASELINE ANYWAY, to the new log's earliest. Failing without rebaselining left the stale
      # checkpoint in place, so the NEXT run compared it against the new log, saw no reset, and
      # resumed past the new log's prefix — losing it permanently and silently. The run still fails,
      # and the discontinuity above is what makes every session it spans NOT_EVALUABLE.
      if ! printf '%s=%s records=0 span=0 dt=%s archived=%s rebaselined=from-%s\n' \
             "$part" "$earliest" "$DAY" "$STAMP" "$ckpt_before" >> "$offfile"; then
        # The append IS the recovery. Unchecked, a temporarily unwritable offsets file let the
        # identity advance anyway, and the next run saw id==id, skipped the reset, and resumed from
        # the stale offset — losing the new log's prefix permanently. Refuse the identity too.
        log "  WARN $topic p$part: could not write the rebaseline — identity NOT advanced, the reset stays visible"
        id_write_blocked=1
      fi
      failed=$(( failed + 1 ))
      continue
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
        "$part" "$earliest" "$DAY" "$STAMP" "$ckpt_before" >> "$offfile" || {
          log "  WARN $topic p$part: could not write the rebaseline — identity NOT advanced, the reset stays visible"
          id_write_blocked=1
        }
    elif is_committed_read_topic "$topic" && ! ckpt_is_proven "$from_line" "$topic_id" \
         && ! ckpt_is_log_start "$from_line" "$from" "$earliest"; then
      # LEGACY (re-review round 2, finding 2): a checkpoint the committed reader did not write. It may be a
      # high-water mark taken past an open transaction whose records the old reader never saw, so it is
      # proof of nothing — resume from it and those records are skipped for ever; mark it on the source
      # and cleanup-es4.sh wipes them. Treat it as ABSENT: recapture from the log start. (A re-baseline
      # line at or below the log start is not legacy: that is exactly where this would begin anyway.)
      # Round 3: a stamped line that names no source log (the round-2 format) is LEGACY too — it cannot say
      # which log its offset belongs to. (A stamped line naming ANOTHER log was already a reset above.)
      if ckpt_is_stamped "$from_line"; then
        log "  LEGACY $topic p$part: checkpoint $from is stamped but names no source log (no 'topic_id=';" \
            "written before the round-3 identity rule), so it cannot be tied to the log now at TopicId $topic_id." \
            "Recapturing from the log start $earliest: records it did archive are archived AGAIN (a duplicate)," \
            "none is skipped."
      else
      log "  LEGACY $topic p$part: checkpoint $from was not written by the committed-read capture (no" \
          "'$COMMITTED_CKPT_STAMP') — an older archiver could checkpoint past a transaction it never read, so" \
          "it proves nothing. Recapturing from the log start $earliest: records it did archive are archived" \
          "AGAIN (a duplicate), none is skipped."
      fi
      legacy_from="$from"
      from="$earliest"
    elif [ "$from" -lt "$earliest" ]; then
      log "  GAP $topic p$part: checkpoint $from expired (log now starts at $earliest) — $((earliest-from)) records LOST before this run"
      if is_strict_topic "$topic"; then
        # A5: for evidence, a gap is the VERDICT, not a note. Continuing would archive the surviving
        # suffix and let it pass the per-date floor, so the corpus would look complete while the
        # session it claims is missing its beginning.
        log "  FAIL $topic p$part: STRICT topic lost $((earliest-from)) records — this session is NOT EVALUABLE"
        failed=$(( failed + 1 ))
        continue
      fi
      from="$earliest"
    else
      # Proven = stamped by the committed reader AND taken on the log whose TopicId this run read (round 3).
      is_committed_read_topic "$topic" && ckpt_is_proven "$from_line" "$topic_id" && from_proven=true
    fi
    count=$(( endoff - from ))
    if [ "$count" -le 0 ]; then
      # Nothing below the bound that the checkpoint has not covered. For a committed-read topic the
      # archive marker is re-recorded at the durable checkpoint anyway: a marker lost with a wiped
      # consumer group, or one whose write failed on an earlier run, would otherwise leave
      # cleanup-es4.sh refusing for ever on a log with nothing left to archive — and an interlock that
      # can never be satisfied is an interlock someone switches off.
      # But ONLY when this run can vouch for it (re-review round 2, finding 2): the checkpoint must be one
      # the committed reader wrote (from_proven — never an inherited, re-baselined or log-start value), and
      # it must already reach the REAL log end, not just a time bound. An idle run endorses nothing it did
      # not itself capture.
      # And (round 3) from_proven now includes the identity: the checkpoint was taken on THIS log.
      if [ "$count" -eq 0 ] && is_committed_read_topic "$topic" && [ "$from_proven" = true ] \
         && [ "$from" -eq "$log_end" ]; then
        mark_committed_boundary "$topic" "$part" "$from" "$topic_id"
      fi
      continue
    fi

    outdir="$ROOT/$topic/dt=$DAY"; mkdir -p "$outdir"
    # <topic>.p<partition>.<from>-<to>.dt<sessionDate>.<archivedAtUTC>.jsonl.gz
    # The session day is repeated INSIDE the name on purpose: copied to a NAS, attached to a
    # ticket, or dropped into a training bucket, the file still states which trading day it
    # belongs to without its parent folder. <to> is cap_to, the end of the range THIS capture proved
    # (below): the queried end for a console-consumer topic, the stable boundary for a committed one.
    manifest_extra=""
    if is_committed_read_topic "$topic"; then
      # ---- COMMITTED-READ CAPTURE (deploy Codex final review, finding 1) ----------------------------
      # es.futures.footprint.strike is written inside Kafka transactions (ES-FOOTPRINT-STRIKE-INTERACTION.md
      # R6). Read with read_committed so an ABORTED revision is never archived (round 1) — and, because
      # a committed-only reader stops below an unresolved transaction, the checkpoint must be the
      # boundary the read PROVABLY reached, never the high-water mark queried above (round 7 and the
      # final review: endoff=1200, a transaction open at 1100, ACCEPT checkpoint=1200 skipped every
      # record that committed afterwards). StrikeArchiveReader.java takes that boundary from Kafka
      # metadata — the read_committed end offset (the last stable offset), capped at the time-bounded
      # offset in UNTIL_TS mode — writes only records below it, and says COMPLETE only when its
      # position reached it. Nothing here parses an offset out of record text.
      #
      # This path is taken ONLY for OE_COMMITTED_READ_TOPICS. Every other topic is captured by the
      # console-consumer branch below exactly as before: same arguments, same bytes, same checkpoint.
      stem="$outdir/.$topic.p$part.$from.$STAMP"
      plain="$stem.records"; sumf="$stem.summary"; rlog="$stem.reader.log"
      tmp="$stem.jsonl.gz.partial"
      bound_args=""
      [ -n "$UNTIL_TS" ] && bound_args="--max-end $endoff"
      # The outer timeout only backs up the reader's own deadline (a JVM that never starts); the
      # reader's deadline is what makes an unfinished capture a FAILURE rather than a short success.
      run_strike_reader $(( STRIKE_READER_DEADLINE_S + 120 )) --bootstrap "$BOOTSTRAP" \
          --topic "$topic" --partition "$part" --from "$from" $bound_args \
          --deadline-ms $(( STRIKE_READER_DEADLINE_S * 1000 )) --out "$plain" --summary "$sumf" \
          > "$rlog" 2>&1
      reader_rc=$?
      sline=$(head -1 "$sumf" 2>/dev/null)
      r_status=$(summary_field "$sline" status);     r_boundary=$(summary_field "$sline" boundary)
      r_position=$(summary_field "$sline" position); r_records=$(summary_field "$sline" records)
      r_escaped=$(summary_field "$sline" escaped)
      why=""
      [ "$reader_rc" -eq 0 ] || why="reader rc=$reader_rc"
      [ "$r_status" = COMPLETE ] || why="$why status=${r_status:-none}"
      for v in "$r_boundary" "$r_position" "$r_records" "$r_escaped"; do
        case "$v" in ''|*[!0-9]*) why="$why unreadable-summary"; break ;; esac
      done
      if [ -z "$why" ]; then
        # Belt and braces over the reader's own exit status: its claim is re-checked, not trusted.
        [ "$r_position" -ge "$r_boundary" ] || why="position $r_position is below boundary $r_boundary"
        if [ -n "$UNTIL_TS" ] && [ "$r_boundary" -gt "$endoff" ]; then
          why="$why boundary $r_boundary is past the time bound $endoff"
        fi
      fi
      if [ -n "$why" ]; then
        log "  WARN $topic p$part [$from,?): committed-read capture FAILED ($why) — checkpoint NOT advanced, will retry next run"
        log "       reader summary: ${sline:-<none>}"
        log "       reader output:  $(tail -1 "$rlog" 2>/dev/null)"
        rm -f "$plain" "$sumf" "$sumf.tmp" "$rlog" "$tmp"
        failed=$(( failed + 1 ))
        continue
      fi
      if [ "$r_boundary" -le "$from" ]; then
        # The stable boundary has not moved past the checkpoint: a transaction that is still open
        # starts at or below it. Nothing is captured and nothing is skipped — the next run reads those
        # records once they resolve. Not a failure of this run. No archive marker either: records of the
        # open transaction sit above `from`, so the checkpoint does not reach the log end, and a marker
        # that cannot cover the log end can only endorse something (re-review round 2, finding 2).
        log "  NOTE $topic p$part: no committed record beyond checkpoint $from yet (stable boundary $r_boundary, log end $log_end) — an open transaction holds the range; the next run captures it once it resolves"
        rm -f "$plain" "$sumf" "$sumf.tmp" "$rlog"
        continue
      fi
      gzip -6 < "$plain" > "$tmp"
      gzip_rc=$?
      scan_out=$(scan_archive_file "$tmp")
      scan_rc=$?
      read -r got min_ms max_ms schema_versions <<< "$scan_out"
      got="${got:-0}"; min_ms="${min_ms:-0}"; max_ms="${max_ms:-0}"; schema_versions="${schema_versions:--}"
      why=""
      [ "$gzip_rc" -eq 0 ] || why="gzip rc=$gzip_rc"
      [ "$scan_rc" -eq 0 ] || why="$why scan rc=$scan_rc"
      [ "$got" = "$r_records" ] || why="$why the file holds $got records but the reader wrote $r_records"
      rm -f "$plain" "$sumf" "$sumf.tmp" "$rlog"
      if [ -n "$why" ]; then
        log "  WARN $topic p$part [$from,$r_boundary): processing FAILED ($why) — checkpoint NOT advanced, will retry next run"
        rm -f "$tmp"
        failed=$(( failed + 1 ))
        continue
      fi
      # A completed range may hold ZERO application records (only commit/abort markers and aborted
      # revisions): it is published — an empty file and its manifest line keep the per-date offset
      # ranges contiguous for the verifier — and the checkpoint advances over it.
      cap_to="$r_boundary"
      count=$(( cap_to - from ))
      manifest_extra=$(printf ',"capture":"read_committed_stable_boundary","stable_boundary":%s,"escaped_records":%s,"source_topic_id":"%s"' "$r_boundary" "$r_escaped" "$topic_id")
      # A recapture over a LEGACY checkpoint says so: the range overlaps whatever the older archiver filed.
      [ -z "$legacy_from" ] || manifest_extra="$manifest_extra$(printf ',"recaptured_over_unproven_checkpoint":%s' "$legacy_from")"
    else
      # ---- CONSOLE-CONSUMER CAPTURE — every topic not in OE_COMMITTED_READ_TOPICS -----------------
      cap_to="$endoff"
      tmp="$outdir/$topic.p$part.$from-$endoff.dt${DAY//-/}.$STAMP.jsonl.gz.partial"
      # NOTE: `consumer | gzip` reports GZIP's exit status, so a consumer that emitted zero records
      # still "succeeds". That is how the first version silently archived empty files and advanced
      # its checkpoints. Verify by COUNTING what actually landed, then commit.
      timeout 900 "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
           --topic "$topic" --partition "$part" --offset "$from" --max-messages "$count" \
           --formatter-property print.timestamp=true \
           --formatter-property print.key=true \
           --formatter-property print.partition=true \
           $(is_strict_topic "$topic" && echo "--formatter-property print.offset=true") \
           --timeout-ms 60000 2>/dev/null | grep -av '^Processed a total of' | gzip -6 > "$tmp"
      # EVERY stage's status, saved at once (final review, finding 5). Keeping only the consumer's let a
      # compressor that died mid-stream, or a failed filter, publish whatever bytes had landed. The
      # filter's 1 means "selected no line", i.e. an empty capture, which the count check refuses anyway.
      pipe_rc=("${PIPESTATUS[@]}")
      consumer_rc=${pipe_rc[0]}; filter_rc=${pipe_rc[1]:-255}; gzip_rc=${pipe_rc[2]:-255}
      # The statistics helper's own status, taken BEFORE its output is parsed: `read <<< "$(helper)"`
      # returned read's status and let a failed decompression (a truncated or corrupt gzip) or a failed
      # awk pass as a verified count.
      scan_out=$(scan_archive_file "$tmp")
      scan_rc=$?
      read -r got min_ms max_ms schema_versions <<< "$scan_out"
      got="${got:-0}"; min_ms="${min_ms:-0}"; max_ms="${max_ms:-0}"; schema_versions="${schema_versions:--}"
      why=""
      [ "$filter_rc" -le 1 ] || why="filter rc=$filter_rc"
      [ "$gzip_rc" -eq 0 ] || why="$why gzip rc=$gzip_rc"
      [ "$scan_rc" -eq 0 ] || why="$why scan rc=$scan_rc"
      if [ -n "$why" ]; then
        log "  WARN $topic p$part [$from,$endoff): processing FAILED ($why) — checkpoint NOT advanced, will retry next run"
        rm -f "$tmp"
        failed=$(( failed + 1 ))
        continue
      fi
      # got < count is NORMAL on a COMPACTED topic: offsets advance but compaction removes all but
      # the newest record per key, so the readable count is far below (end-from). underlying.spx.price
      # is the extreme case — 642,060 offsets, ~2,000 readable records. Judging by count alone would
      # mark every compacted topic as failed forever. Judge by the CONSUMER's exit status instead,
      # and record both numbers so the compaction ratio is visible in the manifest.
      if is_strict_topic "$topic" && [ "$consumer_rc" -eq 0 ] && [ "$got" -ne "$count" ]; then
        # A5: the short read that is NORMAL on a compacted topic is a HOLE on a delete-retained one.
        # Accepting it would archive fewer records than the offset range claims and still advance the
        # checkpoint past them.
        log "  FAIL $topic p$part [$from,$endoff): STRICT topic read $got of $count records — refusing a short read"
        rm -f "$tmp"
        failed=$(( failed + 1 ))
        continue
      fi
      if [ "$consumer_rc" -ne 0 ] || [ "$got" -le 0 ]; then
        rm -f "$tmp"
        log "  WARN $topic p$part [$from,$endoff): consumer rc=$consumer_rc, got $got — checkpoint NOT advanced, will retry next run"
        failed=$(( failed + 1 ))
        continue
      fi
    fi
    out="$outdir/$topic.p$part.$from-$cap_to.dt${DAY//-/}.$STAMP.jsonl.gz"

    # Only advance the checkpoint once the file is verified and durably in place. A crash
    # mid-run therefore re-reads the same range next time (duplicates) instead of skipping it —
    # duplicates are recoverable, gaps are not.
    #
    # Checksum and size are taken over the verified bytes BEFORE they are published (finding 5): the
    # rename does not change them, and a checksum that could not be computed is a failed capture,
    # not a manifest line reading "unknown".
    sha=$(sha256sum "$tmp" 2>/dev/null | cut -d' ' -f1)
    bytes=$(stat -c%s "$tmp" 2>/dev/null || echo 0)
    case "$sha" in
      [0-9a-f]*) [ "${#sha}" -eq 64 ] || sha="" ;;
      *) sha="" ;;
    esac
    if [ -z "$sha" ]; then
      rm -f "$tmp"
      log "  WARN $topic p$part [$from,$cap_to): could not checksum the capture — checkpoint NOT advanced, will retry next run"
      failed=$(( failed + 1 ))
      continue
    fi
    # The mv MUST be gated. This script runs without `set -e`, so an unchecked `mv` that failed —
    # a full NAS, a stale mount, a permissions change — would fall straight through to the
    # checkpoint write below, and the next run would resume past a range whose file does not
    # exist. That is the same silent gap this whole change set exists to close, arrived at from
    # the other direction.
    if ! mv "$tmp" "$out"; then
      rm -f "$tmp"
      log "  WARN $topic p$part [$from,$cap_to): could not publish $out — checkpoint NOT advanced, will retry next run"
      failed=$(( failed + 1 ))
      continue
    fi
    # ORDER MATTERS: manifest FIRST, checkpoint SECOND (A5).
    #
    # The other way round has a window: the checkpoint says "archived through N", the process dies,
    # and the manifest never gets its line. The next run resumes past N, so the range is skipped
    # forever, and the verifier — which reads the manifest — cannot see that anything is missing,
    # because from its point of view that range was never claimed. Writing the manifest first
    # inverts the failure: a crash leaves a manifest line whose range is re-read next run, which is
    # a DUPLICATE. Duplicates are recoverable by content; skipped ranges are not.
    #
    # Completeness record. sha256 is over the gzip stream as committed, so a later bit-rot or a
    # truncated copy is detectable without a broker. Written AFTER the mv so a line in this file
    # always refers to a file that exists. manifest_extra is empty for every console-consumer topic,
    # so their lines are unchanged; a committed-read line also states its capture and boundary.
    printf '{"topic":"%s","dt":"%s","partition":%s,"offset_from":%s,"offset_to":%s,"records":%s,"offset_span":%s,"min_event_time_ms":%s,"max_event_time_ms":%s,"min_event_time":"%s","max_event_time":"%s",%s,"sha256":"%s","bytes":%s,"file":"%s","archived_at":"%s","job":"%s","env":"%s","archiver_version":"%s"%s}\n' \
      "$topic" "$DAY" "$part" "$from" "$cap_to" "$got" "$count" \
      "$min_ms" "$max_ms" "$(ms_to_iso "$min_ms")" "$(ms_to_iso "$max_ms")" \
      "$(schema_fragment "$topic" "$out" "$schema_versions")" \
      "$sha" "$bytes" "$(basename "$out")" "$STAMP" "$ARCHIVE_JOB" "$ENV_NAME" "$ARCHIVER_VERSION" \
      "$manifest_extra" \
      >> "$outdir/_manifest.jsonl" || {
        # The append is the claim; the checkpoint is the promise not to re-read. If the claim could
        # not be written, the promise must not be made — otherwise the range is skipped forever.
        log "  WARN $topic p$part [$from,$cap_to): manifest append FAILED — checkpoint NOT advanced, will re-read next run"
        failed=$(( failed + 1 ))
        continue
      }
    # A committed-read checkpoint is STAMPED (COMMITTED_CKPT_STAMP above) with the TopicId of the log it was
    # taken on (round 3); every other topic's line is exactly what it always was.
    ckpt_stamp=""
    is_committed_read_topic "$topic" && ckpt_stamp=" $COMMITTED_CKPT_STAMP topic_id=$topic_id"
    if ! printf '%s=%s records=%s span=%s dt=%s archived=%s%s\n' \
           "$part" "$cap_to" "$got" "$count" "$DAY" "$STAMP" "$ckpt_stamp" >> "$offfile"; then
      # The file and its claim stand; without the checkpoint the next run re-reads this range, which
      # is a duplicate, never a gap. It is still a failed run: the promise not to re-read was not made.
      log "  WARN $topic p$part [$from,$cap_to): checkpoint append FAILED — the next run re-reads this range (a duplicate, not a gap)"
      failed=$(( failed + 1 ))
      continue
    fi

    topic_records=$(( topic_records + got )); total_files=$(( total_files + 1 ))
    if is_committed_read_topic "$topic"; then
      if [ "$got" -lt "$count" ]; then
        log "  NOTE $topic p$part: $got committed records over $count offsets up to the stable boundary $cap_to (the rest are transaction markers and aborted records)"
      fi
      if [ "$r_escaped" -gt 0 ]; then
        log "  NOTE $topic p$part: $r_escaped record(s) carried a raw TAB/CR/LF, written escaped as \\t \\r \\n (escaped_records in the manifest)"
      fi
      mark_committed_boundary "$topic" "$part" "$cap_to" "$topic_id"
    elif [ "$got" -lt "$count" ]; then
      log "  NOTE $topic p$part: $got readable of $count offsets (compacted topic — history is NOT recoverable from it)"
    fi
  done <<< "$ends"

  # Record the observed identity ONLY now, with every partition of this topic processed, and
  # atomically so a kill between write and rename cannot leave a half-written id that matches
  # nothing. Ordering is the whole point: written before the loop, a crash in the middle would
  # leave the new id stored against partitions that were never re-baselined, and the next run
  # would see id==id, skip the reset, and resume mid-log — silently missing the prefix. Written
  # after, the worst case is repeating a recovery that already happened, i.e. duplicates.
  if [ -n "$topic_id" ] && [ "${id_write_blocked:-0}" -eq 0 ]; then
    printf 'topic_id=%s\nobserved=%s\n' "$topic_id" "$STAMP" > "$idfile.tmp" && mv -f "$idfile.tmp" "$idfile"
  elif [ "${id_write_blocked:-0}" -ne 0 ]; then
    log "  $topic: identity deliberately NOT advanced — a rebaseline could not be recorded, so the next run must still see the reset"
    failed=$(( failed + 1 ))
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
