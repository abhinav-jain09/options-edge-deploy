#!/usr/bin/env bash
# oe-vol-premium-open-capture.sh — one ledger record per session for vol-premium bucket 0.
#
# WHY THIS FILE EXISTS
# vol-premium publishes `NO_BASELINE` on every frame because `ReturnGrid` slot 0 is the session
# open and the index is never observed there: the SPX index series goes silent across the bell, so
# `shapeComplete` is false and no baseline publishes. The only series present through the freeze is
# ES, and whether its per-session offset against the index can be PREDICTED from pre-open
# information is an empirical question that needs >=55 sessions. Those sessions cannot be
# backfilled past what the archive already holds, so they accrue forward, one per trading day.
#
# vol-premium-open-reference-capture.py has been in the repo since 2026-09-21 (#1084) and was
# scheduled NOWHERE. Twelve days, eight archived sessions, zero records: a capture that exists only
# as a file is a study that is not running. This script is what the crontab invokes.
#
# IT READS ONLY THE ARCHIVE. No broker, no consumer group, no live topic. It writes one JSON line
# per session into the ledger and touches nothing else.
set -uo pipefail

OE_DIR="$(cd "$(dirname "$0")" && pwd)"
ARCHIVE_DIR="${ARCHIVE_DIR:-/mnt/nas/optionsedge}"
ENV_NAME="${ENV:-prod}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$ARCHIVE_DIR/kafka/$ENV_NAME}"
LEDGER="${LEDGER:-$ARCHIVE_DIR/vol-premium-open-reference/$ENV_NAME}"
CAPTURE="${CAPTURE:-$OE_DIR/vol-premium-open-reference-capture.py}"
LOG="${LOG:-/home/abhinav/oe-ops/vol-premium-open-capture.log}"
# The archive is complete for a session once a capture run has written files stamped after the
# close. READY_AFTER_MIN is how long after the close that is required to have happened.
READY_AFTER_MIN="${READY_AFTER_MIN:-10}"
INDEX_TOPIC="${INDEX_TOPIC:-underlying.spx.index.price}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# shellcheck source=/dev/null
. "$OE_DIR/oe-alert.sh"
# shellcheck source=/dev/null
. "$OE_DIR/oe-trading-day.sh"

SESSION="${1:-$(TZ=America/New_York date +%Y-%m-%d)}"
case "$SESSION" in
  [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) : ;;
  *) log "FATAL: '$SESSION' is not YYYY-MM-DD"; exit 2 ;;
esac

[ -x "$CAPTURE" ] || [ -r "$CAPTURE" ] || {
  log "FATAL: the capture is not installed at $CAPTURE"
  alert "🚨 vol-premium open-reference capture cannot run on $(hostname): $CAPTURE missing. Bucket 0 is accruing NO sessions."
  exit 2
}

# A non-trading date has no open to reference. The gate is the archiver's own (oe-trading-day.sh):
# two calendars disagreeing about a holiday is how a ledger ends up holding a session that never
# happened, and the >=55 count is taken over this directory.
if [ "$(is_trading_day "$SESSION")" != "yes" ]; then
  log "$SESSION is not a New York trading day — nothing to capture"; exit 0
fi

# THE CLOSE THE COVERAGE RULES ARE FRACTIONS OF. The capture scores coverage against the session it
# is told about, so a half day judged against 16:00 is a sound sample marked incomplete: 210
# minutes of a 385-minute universe is 55%, and one thin quarter would sink it. The calendar already
# knows, and it is the SAME calendar the archiver uses.
CLOSE_ET="$(CALENDAR_DIR="$CALENDAR_DIR" OE_DAY="$SESSION" python3 - <<'PY' 2>/dev/null
import os, sys
from datetime import date
sys.path.insert(0, os.environ.get("CALENDAR_DIR", ""))
day = date.fromisoformat(os.environ["OE_DAY"])
try:
    from market_calendar import MarketCalendar
    print(MarketCalendar().close_time(day).strftime("%H:%M"))
except Exception:
    print("16:00")
PY
)"
case "$CLOSE_ET" in
  [0-9][0-9]:[0-9][0-9]) : ;;
  # FAIL TOWARDS THE FULL SESSION. 16:00 is what every normal day is, and a half day misjudged as a
  # full one is REJECTED for thin coverage — recoverable, visible in the ledger, and not a
  # fabricated acceptance. The reverse, scoring a full session against 13:00, would accept a
  # session on its morning alone.
  *) log "WARNING: no close time for $SESSION from the calendar; using 16:00"; CLOSE_ET=16:00 ;;
esac

# THE SESSION MUST BE OVER BEFORE THE RECORD IS CLAIMED, because the claim is PERMANENT: the
# capture marks a session under .published/ and refuses to republish it, so a run against a
# still-growing archive does not merely report a bad number — it takes the session out of the study
# for good. The spot topics are archived every 10 minutes, so the test is whether a capture run
# stamped after the close has landed.
#
# THIS IS A TIMING GATE, NOT A COMPLETENESS ONE, and the distinction is the honest reading of it: a
# late stamp says the archiver reached past the close for this topic, not that every record of the
# session is on disk. SUFFICIENCY is the capture's own business and it already judges it — the
# coverage, span and per-quarter floors are what reject a session whose archive has a hole, and
# 2026-09-22 is rejected by exactly that. What this gate prevents is the narrower and worse case:
# spending the permanent claim on a session the archiver had not finished writing.
# oe-archive-verify.sh grades the day's completeness separately and alerts on it.
#
# The archive file name carries the stamp:
#   underlying.spx.index.price.p0.31493-31672.dt20260924.20260924T103001Z.jsonl.gz
# Exit 0, not an error: a session that is not ready yet is a session for the retry run, and a
# non-zero exit from cron here would alert every day at 17:30 for a condition that resolves itself.
READY="$(ARCHIVE_ROOT="$ARCHIVE_ROOT" INDEX_TOPIC="$INDEX_TOPIC" OE_DAY="$SESSION" \
         CLOSE_ET="$CLOSE_ET" READY_AFTER_MIN="$READY_AFTER_MIN" python3 - <<'GATE' 2>/dev/null
import datetime as dt, glob, gzip, os, re
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
day = dt.date.fromisoformat(os.environ["OE_DAY"])
hh, mm = (int(part) for part in os.environ["CLOSE_ET"].split(":"))
deadline = (dt.datetime.combine(day, dt.time(hh, mm), ET)
            + dt.timedelta(minutes=int(os.environ["READY_AFTER_MIN"]))).astimezone(dt.timezone.utc)
root = os.environ["ARCHIVE_ROOT"]
index_topic = os.environ["INDEX_TOPIC"]


def files(topic):
    return glob.glob(os.path.join(root, topic, "dt=" + day.isoformat(), "*.jsonl.gz"))


stamps = []
for path in files(index_topic):
    found = re.search(r"\.(\d{8}T\d{6})Z\.jsonl\.gz$", os.path.basename(path))
    if found:
        stamps.append(dt.datetime.strptime(found.group(1), "%Y%m%dT%H%M%S")
                      .replace(tzinfo=dt.timezone.utc))
# NO FILES AT ALL IS NOT READY, and is a different thing from "files, but none late enough". Both
# wait; only the first is worth a word in the log, which the caller prints.
if not stamps or max(stamps) < deadline:
    print("waiting")
    raise SystemExit

# AND EVERY FILE THE CAPTURE WILL READ MUST DECOMPRESS TO ITS END. The spot topics are archived
# every ten minutes, so this runs while the archiver may be writing one; the capture's reader opens
# a .jsonl.gz and iterates it, and a file still being written raises PARTWAY THROUGH, which its
# open-time OSError guard does not catch. That costs a failed run, an alert and a day of delay for
# a condition that resolves itself in minutes.
#
# Reading every byte is the only test of this that is not a guess: a size, an mtime or a successful
# open all pass on a half-written member. It costs one pass over about fifty small files, once a
# day. This is NOT the stamp test above and not a substitute for it - one says the archiver reached
# past the close, the other says what is on disk can be read - and neither is a completeness
# check; the capture's own coverage floors are that, and oe-archive-verify.sh grades the day.
#
# All three topics, because the capture reads all three and a torn ES or basis file fails the run
# just as an index one does.
for topic in (index_topic, "underlying.es.price", "spx.basis.state"):
    for path in files(topic):
        try:
            with gzip.open(path, "rb") as handle:
                while handle.read(1 << 20):
                    pass
        except Exception:
            print("torn")
            raise SystemExit
print("ready")
GATE
)"
case "$READY" in
  ready) : ;;
  torn)
    log "$SESSION: an archive file for this session does not decompress to its end — the archiver is most likely still writing it; leaving the session unclaimed for the retry run"
    exit 0 ;;
  *)
    log "$SESSION: the archive under $ARCHIVE_ROOT/$INDEX_TOPIC holds nothing stamped later than the close + ${READY_AFTER_MIN}m — leaving the session unclaimed for the retry run"
    exit 0 ;;
esac

mkdir -p "$LEDGER" || {
  log "FATAL: cannot create the ledger at $LEDGER"
  alert "🚨 vol-premium open-reference ledger unwritable on $(hostname): $LEDGER. Bucket 0 is accruing NO sessions."
  exit 2
}

log "$SESSION: capturing (close $CLOSE_ET ET, archive $ARCHIVE_ROOT, ledger $LEDGER)"
RECORD="$(python3 "$CAPTURE" --session "$SESSION" --archive-root "$ARCHIVE_ROOT" \
          --close-et "$CLOSE_ET" --out "$LEDGER" 2>&1)"
RC=$?
log "$SESSION: $RECORD"
if [ "$RC" -ne 0 ]; then
  log "$SESSION: the capture exited $RC"
  alert "🚨 vol-premium open-reference capture FAILED for $SESSION on $(hostname) (rc=$RC). Bucket 0 did not gain a session."
  exit "$RC"
fi

# THE COUNT IS THE POINT. accepted/ is what the >=55 bar is taken over; printing it every day is
# how the study's progress is visible without anyone going to look.
ACCEPTED="$(find "$LEDGER/accepted" -maxdepth 1 -name '*.json' 2>/dev/null | grep -c . || true)"
REJECTED="$(find "$LEDGER/rejected" -maxdepth 1 -name '*.json' 2>/dev/null | grep -c . || true)"
log "ledger now holds ${ACCEPTED:-0} accepted and ${REJECTED:-0} rejected sessions (the bar is 55 accepted)"
exit 0
