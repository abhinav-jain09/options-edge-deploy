#!/usr/bin/env bash
# oe-archive-daily.sh — nightly durable archive of one trading day, run from cron at 17:10 ET
# (market close + 1h10m). Wraps oe-archive-kafka.sh with a trading-day gate, a destination
# choice, locks, and assertions about what actually landed.
#
# WHY A WRAPPER: the archiver itself is a plain incremental copier. Everything that makes the
# nightly run trustworthy — is today a trading day, is the NAS actually mounted, did the spot
# topics really produce records, did two runs overlap — belongs here.
#
# ⚠️ 2026-08-12 — THE FAILURE THIS FILE NOW EXISTS TO PREVENT
# This job produced NOTHING for env=prod on 2026-08-10 and 2026-08-11 and reported success both
# times. It shared one per-environment lock with the */10 spot archive; the spot job held it at
# 17:10, this job logged "exiting clean" and returned 0. Nothing alerted, because exit 0 was being
# read as proof of archival.
#
# The two sessions WERE recovered (28.4M records) — but only by luck, not by design: these topics
# turned out to carry retention.ms=-1, so the records were still in the log with no checkpoint
# pointing at them. Do not read that as "a missed night is harmless". close.direction.signal
# carries a 12h override and its 2026-08-10 session is gone for good, gex.strike is compacted so a
# late capture recovers a fraction, and one retention-config change turns the next missed night
# into a total loss. See ARCHIVE-FAIL-SILENT-FIX-20260812.md.
#
# Three things changed, and all three matter:
#   1. This job and the spot job own DISJOINT topic sets (oe-topics.env) and use different lock
#      keys, so neither can starve the other.
#   2. ON_LOCK_BUSY=fail. A once-a-day job has no next run before its data expires, so a busy lock
#      is a failure, never a clean exit.
#   3. Nothing here is trusted as proof of archival any more. oe-archive-verify.sh reads the files
#      on disk from its own cron entry and alerts on a missing or partial date — it would have
#      caught this on the evening of 2026-08-08 even with every bug above still in place.
#
# SCHEDULING: the crontab entry uses CRON_TZ=America/New_York, so 17:10 stays 17:10 in New York
# across both DST switches even though this box runs Europe/Madrid. Do NOT convert the time to
# local and hardcode it — that silently drifts by an hour twice a year, in opposite directions.
#
# DESTINATION: prefers the NAS. Falls back to the local staging disk and says so LOUDLY, because
# a fallback that looks like success is how you discover months later that nothing was archived.
#
# Exit non-zero on any failure so cron surfaces it.
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

OE_DIR="$(cd "$(dirname "$0")" && pwd)"
NAS_DIR="${NAS_DIR:-/mnt/nas/optionsedge}"
STAGING_DIR="${STAGING_DIR:-/home/kafka/archive}"
ARCHIVER="${ARCHIVER:-$OE_DIR/oe-archive-kafka.sh}"
VERIFIER="${VERIFIER:-$OE_DIR/oe-archive-verify.sh}"
CALENDAR_DIR="${CALENDAR_DIR:-/home/abhinav/autostart/jenkins}"
LOG="${LOG:-/home/abhinav/oe-ops/archive-daily.log}"
LOCK="${LOCK:-/tmp/oe-archive-daily.lock}"
ENV_NAME="${ENV:-prod}"
export JAVA_HOME="${JAVA_HOME:-$(ls -d /usr/lib/jvm/*jre-17* 2>/dev/null | head -1)}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# shellcheck source=/dev/null
. "$OE_DIR/oe-alert.sh"
# shellcheck source=/dev/null
. "$OE_DIR/oe-trading-day.sh"

OE_TOPICS_ENV="${OE_TOPICS_ENV:-$OE_DIR/oe-topics.env}"
[ -r "$OE_TOPICS_ENV" ] || { log "FATAL: '$OE_TOPICS_ENV' missing or unreadable — refusing to archive an unknown evidence set"; exit 1; }
unset DEALER_LEDGER_EVIDENCE OE_SPOT_TOPICS OE_HEAVY_TOPICS_prod
# shellcheck source=/dev/null
. "$OE_TOPICS_ENV"
: "${DEALER_LEDGER_EVIDENCE:?oe-topics.env did not define DEALER_LEDGER_EVIDENCE}"
: "${OE_HEAVY_TOPICS_prod:?oe-topics.env did not define OE_HEAVY_TOPICS_prod}"
: "${OE_SPOT_TOPICS:?oe-topics.env did not define OE_SPOT_TOPICS}"

# Spot is the whole point of this archive: an option chain without the underlying at that instant
# cannot train anything. The */10 job OWNS these topics now — this job no longer archives them, it
# ASSERTS them. That is a stronger check than it looks: it makes the daily run fail when the spot
# job has silently died, which the daily run could never notice while it was archiving spot itself.
SPOT_TOPICS="${SPOT_TOPICS:-underlying.spx.index.price underlying.es.price}"

DAY="$(TZ=America/New_York date +%Y-%m-%d)"

# --- single run at a time ---------------------------------------------------------------------
# Overlapping runs would interleave manifest writes. A busy lock here means YESTERDAY'S run is
# still going at 17:10 today — never routine, and never something to exit 0 over.
exec 9>"$LOCK"
if ! flock -n 9; then
  log "FAILURE: another daily archive run holds $LOCK — a previous run is still in flight."
  log "         Today's session is NOT being archived by this invocation. Retention is 1 day."
  alert "🚨 Daily archive did NOT run on $(hostname): $LOCK is held by an earlier run that never finished. Today's session ($DAY) is unarchived and expires in ~24h. There is no backfill."
  exit 1
fi

log "=== daily archive start (ET $(TZ=America/New_York date '+%a %Y-%m-%d %H:%M')) ==="

# --- trading-day gate -------------------------------------------------------------------------
if [ "$(is_trading_day)" != "yes" ]; then
  log "not a trading day in New York — nothing to archive, exiting clean"; exit 0
fi

# --- destination: NAS if genuinely mounted AND writable, else staging (loudly) -----------------
DEST=""
if [ -d "$NAS_DIR" ] && touch "$NAS_DIR/.oe_probe.$$" 2>/dev/null; then
  rm -f "$NAS_DIR/.oe_probe.$$"; DEST="$NAS_DIR"; EXTRA=""
  log "destination: NAS $DEST"
else
  DEST="$STAGING_DIR"; EXTRA="ALLOW_NON_NAS=true"
  log "⚠️  NAS '$NAS_DIR' not mounted/writable — STAGING LOCALLY at $DEST."
  log "⚠️  This data is NOT off-box yet. Mount the NAS and re-run, or copy $DEST across."
  # 2026-08-07 staged locally and nobody noticed until the 08-12 audit; the postgres pass also
  # died that night because it refuses non-NAS paths without ALLOW_NON_NAS.
  alert "⚠️ Daily archive on $(hostname) is STAGING LOCALLY at $DEST — the NAS '$NAS_DIR' is not mounted or not writable. Today's session ($DAY) is one disk failure from gone. Mount it and re-run."
fi
[ -d "$DEST" ] || { log "FATAL: destination '$DEST' does not exist"; exit 1; }

rc=0

# --- prod heavy pass ---------------------------------------------------------------------------
# HEAVY topics only. The spot topics belong to the */10 job (oe-topics.env explains why the two
# sets are disjoint). ON_LOCK_BUSY=fail: if this cannot get its lock there is no later run to
# cover the range, so contention is a failure with an alert, not a clean exit.
env ARCHIVE_DIR="$DEST" ENV="$ENV_NAME" ARCHIVE_JOB=daily ON_LOCK_BUSY=fail \
    TOPICS="$OE_HEAVY_TOPICS_prod" $EXTRA bash "$ARCHIVER" 2>&1 | tee -a "$LOG"
prod_rc=${PIPESTATUS[0]}
if [ "$prod_rc" -eq 3 ]; then
  log "FAILURE: the prod heavy pass could not acquire its archiver lock — NOTHING was archived for env=$ENV_NAME."
  alert "🚨 Daily heavy archive SKIPPED on $(hostname) — env=$ENV_NAME job=daily could not acquire its lock, so dt=$DAY was NOT archived. This is the exact failure that silently destroyed 2026-08-08..08-11. Investigate now; retention is 1 day and there is no backfill."
  rc=1
elif [ "$prod_rc" -ne 0 ]; then
  log "FAILURE: the prod heavy pass exited $prod_rc"
  rc=1
fi

# --- indicator bars (2026-08-19) --------------------------------------------------------------
# The 1h and 4h chains need MULTI-DAY history: EMA21 wants 21 consecutive clean bars, which is
# 2,520 clean 30s children for 1h and 10,080 for 4h. Kafka holds 45 days, but the seeding work
# reads from the ARCHIVE, and until now these topics were archived NOWHERE. Every day not
# captured is a day the seed cannot use. Higher timeframes are ~0.5 MB/session; the 30s bars
# dominate because each carries a ~29.5 KB recoveryCheckpoint.
env ARCHIVE_DIR="$DEST" ENV="$ENV_NAME" ARCHIVE_JOB=daily-indicator-bars \
    TOPICS="options.indicators.bars" $EXTRA bash "$ARCHIVER" 2>&1 | tee -a "$LOG"
[ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
# es4 broker pass. es.drop.*: the 60-session drop-classifier calibration corpus. es.underlying.es.trades
# added 2026-08-08: the raw ES tape (~300-550k records/session, plain JSON, 24h retention) was
# archived NOWHERE — Friday 08-07 tape expired mid-study and had to be re-pulled from Databento.
# es.tape-zones.cells is compacted (FINAL cells persist) but nightly capture preserves the session
# anyway. The PRIMARY capture is the 17:01 ET cron (same archiver, same manifest) firing in the
# 17:00-18:00 ES break so each dt folder holds exactly one session; this 17:10 pass is the backstop
# and is a no-op (0 new records) whenever 17:01 succeeded. Its own job key, and ON_LOCK_BUSY=skip
# on purpose: a backstop that fails because the primary is still running is noise, not signal.
env ARCHIVE_DIR="$DEST" ENV=es4 BOOTSTRAP=192.168.100.4:9092 ARCHIVE_JOB=daily-es4 \
    TOPICS="es.underlying.es.trades es.tape-zones.cells es.options.indicators.bars" \
    bash "$ARCHIVER" 2>&1 | tee -a "$LOG"
[ "${PIPESTATUS[0]}" -eq 0 ] || rc=1

# dev dealer-ledger evidence (2026-08-08). Dev runs the same shadow-live ledger as prod, and
# comparing the two is the whole point of the shadow programme — but dev was archived NOWHERE,
# and dev's dealer-ledger-signal-fired carries a 24h per-topic retention that the service
# RE-STAMPS on every startup, so dev evidence deletes itself daily. This nightly pass is the
# backstop; oe-archive-dev.sh runs the same archiver every 2h so a single missed night cannot
# cost a session. Scope is deliberately the ledger evidence only — dev's market data is a
# synthetic/cascaded feed and duplicating it here would buy volume, not truth.
# The dev broker advertises itself as host.docker.internal (Docker Desktop), which is mapped to
# 192.168.100.102 in /etc/hosts on this box — without that entry the client resolves nothing.
env ARCHIVE_DIR="$DEST" ENV=dev BOOTSTRAP="${DEV_BOOTSTRAP:-192.168.100.102:19092}" \
    ARCHIVE_JOB=daily-dev TOPICS="$DEALER_LEDGER_EVIDENCE" \
    bash "$ARCHIVER" 2>&1 | tee -a "$LOG"
[ "${PIPESTATUS[0]}" -eq 0 ] || rc=1

# drop-classifier corpus moved to the PROD broker 2026-08-12 (es4 wipes nightly
# by design — a week of es.drop.* evaporated). Archive from the prod broker now.
#
# ON_LOCK_BUSY=fail added 2026-08-12: unlike the es4/dev passes above this one is a PRIMARY, not a
# backstop — nothing else archives es.drop.* from the prod broker, so there is no later run to
# cover a skipped range. Defaulting to skip here would be the same shape as the defect that lost
# 2026-08-10/11: a once-a-day job treating "someone else holds the lock" as a clean exit.
env ARCHIVE_DIR="$DEST" ENV=prod ARCHIVE_JOB=daily-drop ON_LOCK_BUSY=fail \
    TOPICS="es.drop.nowcast es.drop.final-summary es.drop.outcome" \
    bash "$ARCHIVER" 2>&1 | tee -a "$LOG"
drop_rc=${PIPESTATUS[0]}
if [ "$drop_rc" -ne 0 ]; then
  [ "$drop_rc" -eq 3 ] && alert "🚨 Drop-classifier corpus NOT archived on $(hostname) for dt=$DAY — job=daily-drop could not acquire its lock. Nothing else archives es.drop.* from the prod broker."
  log "FAILURE: the drop-classifier pass exited $drop_rc"
  rc=1
fi

# Postgres: open interest exists in a readable form ONLY in databento_option_raw_snapshot.
# options.databento.events.raw carries no OI field at all, and options.databento.gex.strike
# does but is Avro (a text archive of it is undecodable bytes). ibkr-raw-retention.sh deletes
# that table hourly at a 16-HOUR horizon; the daily OI baseline lands ~06:30 ET, so at 17:10 ET
# it is ~10.7h old and still present. This run is the only thing between each session's OI and
# permanent loss, and without OI you cannot reconstruct GEX.
PG_ARCHIVER="${PG_ARCHIVER:-$OE_DIR/oe-archive-postgres.sh}"
if [ -x "$PG_ARCHIVER" ]; then
  env ARCHIVE_DIR="$DEST" ENV="$ENV_NAME" $EXTRA bash "$PG_ARCHIVER" 2>&1 | tee -a "$LOG"
  [ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
else
  log "  WARNING: $PG_ARCHIVER missing — OPEN INTEREST IS NOT BEING ARCHIVED (16h retention destroys it nightly)"
  rc=1
fi

# --- spot assertion: prove the underlying was captured TODAY, do not assume --------------------
# A silent spot failure is the worst outcome here: the archive looks full (chain topics are big)
# while every row is untrainable.
#
# ⚠️ This used to sum records=  across the ENTIRE offsets file — every run since the archive began.
# Once any records had ever been archived for a topic the total could never fall back to zero, so
# the assertion could not fail, and it duly reported "spot OK: 274,776 records" on 2026-08-11 while
# that night's heavy archive wrote nothing at all. A cumulative counter cannot answer a question
# about today. Filter on dt= and count only THIS session.
MAN="$DEST/kafka/$ENV_NAME/_manifest"
spot_missing=""
for t in $SPOT_TOPICS; do
  n=$(awk -v d="dt=$DAY" '
        { today=0; for (i=1;i<=NF;i++) if ($i==d) today=1
          if (today) for (i=1;i<=NF;i++) if ($i ~ /^records=/) { split($i,a,"="); s+=a[2] } }
        END { print s+0 }' "$MAN/$t.offsets" 2>/dev/null)
  if [ "${n:-0}" -eq 0 ]; then spot_missing="$spot_missing $t"
  else log "  spot OK: $t dt=$DAY $n records"; fi
done
if [ -n "$spot_missing" ]; then
  log "FAILURE: no spot records archived for dt=$DAY:$spot_missing — today's chain data is NOT trainable"
  alert "🚨 No spot records archived for dt=$DAY on $(hostname):$spot_missing
The */10 spot archive owns these topics — check that its cron is still firing. Without spot at the same instants, today's option chain cannot train anything."
  rc=1
fi

used=$(du -sh "$DEST" 2>/dev/null | cut -f1)
log "=== daily archive done rc=$rc archive_size=$used ==="

# --- completeness check ------------------------------------------------------------------------
# Advisory here: this reads what actually landed and reports it into the same log, so a human
# tailing the nightly output sees the verdict immediately. It is NOT the safety net — the safety
# net is the verifier's own cron entry, which fires whether or not this script ran at all.
if [ -x "$VERIFIER" ]; then
  env ARCHIVE_DIR="$DEST" ENV="$ENV_NAME" bash "$VERIFIER" "$DAY" 2>&1 | tee -a "$LOG"
  [ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
else
  log "  WARNING: $VERIFIER missing — completeness is UNVERIFIED for dt=$DAY"
  rc=1
fi

exit "$rc"
