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
#
# EVERY GATE BELOW PROTECTS AN IRREVERSIBLE ACT. The capture CLAIMS a session by a marker under
# .published/ and refuses to republish it, so a run against a session the archiver has not finished
# with does not report a bad number — it spends the session. That is why this script fails CLOSED:
# anything it cannot positively establish is a reason not to capture, and the only condition that
# exits 0 without capturing is one it has positively established as retryable.
set -uo pipefail

OE_DIR="$(cd "$(dirname "$0")" && pwd)"
ARCHIVE_DIR="${ARCHIVE_DIR:-/mnt/nas/optionsedge}"
ENV_NAME="${ENV:-prod}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$ARCHIVE_DIR/kafka/$ENV_NAME}"
LEDGER="${LEDGER:-$ARCHIVE_DIR/vol-premium-open-reference/$ENV_NAME}"
CAPTURE="${CAPTURE:-$OE_DIR/vol-premium-open-reference-capture.py}"
LOG="${LOG:-/home/abhinav/oe-ops/vol-premium-open-capture.log}"
# oe-archive-verify.sh's own per-session verdict. It is written once the day has been graded, which
# is the authoritative statement that the archiver is DONE with the session — a filename's
# timestamp is not.
COMPLETENESS_DIR="${COMPLETENESS_DIR:-$ARCHIVE_ROOT/_manifest/completeness}"
# The topics the capture reads. All three are gated, because a complete index with an absent ES
# series produces a record that says "no ES reference in the window" about the ARCHIVE rather than
# about the session, and that record is permanent.
CAPTURE_TOPICS="${CAPTURE_TOPICS:-underlying.spx.index.price underlying.es.price spx.basis.state}"
# OPERATOR-ONLY, AND NEVER IN THE CRONTAB. A session whose verdict was never written cannot be
# gated on one, and that is not hypothetical: the verifier did not run the night of 2026-10-01, so
# 2026-10-02 is graded and 2026-10-01 is not, while both are sound sessions by the capture's own
# floors. Waiting costs nothing — an unclaimed session stays claimable forever — so the cron waits,
# and a human backfilling a day the verifier missed sets this, which falls back to the file-stamp
# timing test and says loudly in the log that it did.
ALLOW_UNGRADED="${ALLOW_UNGRADED:-false}"
# How far past the close a file must be stamped for the FALLBACK timing test. Only consulted under
# ALLOW_UNGRADED; the graded path uses each topic's own max_event_time, which is better evidence.
READY_AFTER_MIN="${READY_AFTER_MIN:-10}"

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

# A SESSION THAT HAS NOT HAPPENED CANNOT BE CAPTURED, and the shape of the date does not say that.
# A future weekday with any pre-existing archive directory would otherwise be claimed — permanently
# — from an argument, which is the one thing an operator can get wrong by a keystroke.
TODAY_ET="$(TZ=America/New_York date +%Y-%m-%d)"
if [ "$SESSION" \> "$TODAY_ET" ]; then
  log "FATAL: $SESSION is in the future (today in New York is $TODAY_ET) — refusing to claim it"
  exit 2
fi

[ -r "$CAPTURE" ] || {
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
#
# AND IT FAILS CLOSED. An earlier version swallowed the helper's errors and substituted 16:00,
# calling that "recoverable" — it is not. Publication is permanent, so a half day scored against a
# guessed 16:00 is a sound session rejected FOREVER. If the close cannot be established, nothing is
# claimed and the condition is alerted.
CLOSE_ET="$(CALENDAR_DIR="$CALENDAR_DIR" OE_DAY="$SESSION" python3 - <<'CLOSE'
import os, sys
from datetime import date
# ONE UNIT, ONE CALENDAR — and `sys.path.insert` alone does not say that. A script read from stdin
# gets the CURRENT DIRECTORY on sys.path, so `market_calendar` could resolve from wherever this
# happened to be run instead of from the installed unit, and the close that decides whether a
# session's coverage floors are fractions of 385 minutes or 210 would come from a file nobody
# deployed. Found by a test that pointed CALENDAR_DIR at a directory that does not exist and still
# got an answer. The CURRENT DIRECTORY is removed and CALENDAR_DIR put first; the standard library
# entries stay, because replacing sys.path outright takes zoneinfo with it.
sys.path = [entry for entry in sys.path if entry not in ("", ".", os.getcwd())]
sys.path.insert(0, os.environ.get("CALENDAR_DIR", ""))
from market_calendar import MarketCalendar
print(MarketCalendar().close_time(date.fromisoformat(os.environ["OE_DAY"])).strftime("%H:%M"))
CLOSE
)"
case "$CLOSE_ET" in
  [0-9][0-9]:[0-9][0-9]) : ;;
  *)
    log "FATAL: the calendar did not give a close time for $SESSION (got '$CLOSE_ET') — refusing to claim the session against a guess"
    alert "🚨 vol-premium open-reference capture cannot read the market calendar on $(hostname) for $SESSION. The session is NOT claimed; bucket 0 did not gain one."
    exit 2 ;;
esac

# IS THE ARCHIVER DONE WITH THIS SESSION? The authoritative answer is oe-archive-verify.sh's own
# per-session verdict under _manifest/completeness/, which it writes once it has graded the day.
# Filename recency was the first version of this gate and the reviewer was right to refuse it: it
# proves that one file arrived late, not that the series is there.
#
# WHAT IS REQUIRED, and what deliberately is not:
#   * the verdict for the session EXISTS — the day has been graded at all
#   * every topic the capture reads appears in it, with a status that is not EMPTY
#   * each of those topics' own max_event_time is at or after the close, so the archiver reached
#     past the bell for THAT topic and not merely for the busiest one
#   * NOT status == OK. 2026-10-02 is graded PARTIAL on one offset discontinuity in a topic bucket 0
#     does not read, and it is a sound session: 385 of 385 minutes covered, four full quarters, an ES
#     reference 2 ms old at the open. Requiring OK would refuse good sessions forever, which is the
#     same permanent loss by the opposite mistake. SUFFICIENCY is the capture's own business — its
#     coverage, span and per-quarter floors are fractions of the session and are what reject
#     2026-09-22 for a hole in its third quarter. The status is logged so a PARTIAL is never silent.
#
# The exit codes are distinct on purpose: 0 = positively established as not-ready-yet, so the retry
# run should try again; anything else is a fault and must not be read as "wait".
GATE="$(COMPLETENESS_DIR="$COMPLETENESS_DIR" ARCHIVE_ROOT="$ARCHIVE_ROOT" OE_DAY="$SESSION" \
        CLOSE_ET="$CLOSE_ET" CAPTURE_TOPICS="$CAPTURE_TOPICS" ALLOW_UNGRADED="$ALLOW_UNGRADED" \
        READY_AFTER_MIN="$READY_AFTER_MIN" python3 - <<'GATE'
import datetime as dt, glob, gzip, json, os, re
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
day = dt.date.fromisoformat(os.environ["OE_DAY"])
hh, mm = (int(part) for part in os.environ["CLOSE_ET"].split(":"))
close = dt.datetime.combine(day, dt.time(hh, mm), ET).astimezone(dt.timezone.utc)
root = os.environ["ARCHIVE_ROOT"]
topics = os.environ["CAPTURE_TOPICS"].split()
allow_ungraded = os.environ.get("ALLOW_UNGRADED") == "true"
verdict_path = os.path.join(os.environ["COMPLETENESS_DIR"], day.isoformat() + ".json")


def files(topic):
    return glob.glob(os.path.join(root, topic, "dt=" + day.isoformat(), "*.jsonl.gz"))


def answer(state, why):
    print(state + " " + why)
    raise SystemExit(0)


def from_the_verdict():
    """Each topic's own max_event_time, from oe-archive-verify.sh's grading of the day."""
    try:
        with open(verdict_path) as handle:
            graded = {entry["topic"]: entry for entry in json.load(handle)["topics"]}
    except (OSError, ValueError, KeyError, TypeError) as err:
        # A verdict that cannot be read is a FAULT, not a wait: it will not fix itself, and
        # treating it as "try again this evening" is how a gate becomes a delay nobody notices.
        answer("fault", f"the archive verdict at {verdict_path} cannot be read: {err}")
    told = []
    for topic in topics:
        entry = graded.get(topic)
        if entry is None:
            answer("waiting", f"{topic} is not in the archive verdict for {day}")
        status = entry.get("status")
        if not status or status == "EMPTY":
            answer("waiting", f"{topic} is graded {status or 'ungraded'} in the archive verdict")
        raw = entry.get("max_event_time")
        try:
            latest = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            answer("fault", f"{topic} has no readable max_event_time in the verdict (got {raw!r})")
        if latest.tzinfo is None:
            answer("fault", f"{topic}'s max_event_time {raw!r} carries no offset")
        if latest < close:
            answer("waiting", f"{topic} reaches only {raw}, before the "
                              f"{os.environ['CLOSE_ET']} ET close")
        told.append(f"{topic}={status}/{entry.get('records')}")
    return told


def from_the_file_stamps():
    """THE FALLBACK, weaker on purpose: an archive file name carries the archive RUN's stamp, which
    says one file for one topic arrived late — not that the series is there. Enough for a human
    backfilling a day the verifier missed, not enough for a cron."""
    deadline = close + dt.timedelta(minutes=int(os.environ["READY_AFTER_MIN"]))
    told = []
    for topic in topics:
        stamps = []
        for path in files(topic):
            found = re.search(r"\.(\d{8}T\d{6})Z\.jsonl\.gz$", os.path.basename(path))
            if found:
                stamps.append(dt.datetime.strptime(found.group(1), "%Y%m%dT%H%M%S")
                              .replace(tzinfo=dt.timezone.utc))
        if not stamps:
            answer("waiting", f"{topic} has no archived file for {day}")
        if max(stamps) < deadline:
            answer("waiting", f"{topic}'s latest archive run is "
                              f"{max(stamps).strftime('%Y-%m-%dT%H:%M:%SZ')}, before the close + "
                              f"{os.environ['READY_AFTER_MIN']}m")
        told.append(f"{topic}=UNGRADED")
    return told


if os.path.isfile(verdict_path):
    summary = from_the_verdict()
elif allow_ungraded:
    summary = from_the_file_stamps()
else:
    answer("waiting", "the archive verdict for this session has not been written yet")

# AND EVERY FILE THE CAPTURE WILL READ MUST DECOMPRESS TO ITS END. The verdict is written once and
# the spot topics keep being archived every ten minutes, so a member can be mid-write AFTER the day
# was graded. The capture's reader catches OSError at OPEN and then iterates, and a torn member
# raises partway through — a failed run, an alert, and a day of delay for a condition that resolves
# itself in minutes. Reading every byte is the only test of this that is not a guess: a size, an
# mtime and a successful open all pass on a half-written member.
for topic in topics:
    for path in files(topic):
        try:
            with gzip.open(path, "rb") as handle:
                while handle.read(1 << 20):
                    pass
        except Exception:
            answer("waiting", f"{os.path.basename(path)} does not decompress to its end — "
                              f"the archiver is most likely still writing it")

print("ready " + " ".join(summary))
GATE
)"
GATE_RC=$?
if [ "$GATE_RC" -ne 0 ]; then
  log "FATAL: the readiness gate did not run for $SESSION (rc=$GATE_RC) — the session is NOT claimed"
  alert "🚨 vol-premium open-reference readiness gate FAILED to run on $(hostname) for $SESSION (rc=$GATE_RC). The session is NOT claimed; bucket 0 did not gain one."
  exit 2
fi
GATE_STATE="${GATE%% *}"
GATE_WHY="${GATE#* }"
case "$GATE_STATE" in
  ready)
    case "$GATE_WHY" in
      *UNGRADED*)
        log "$SESSION: NO ARCHIVE VERDICT EXISTS and ALLOW_UNGRADED=true, so this session is claimed on the weaker file-stamp test alone ($GATE_WHY)" ;;
      *)
        log "$SESSION: the archive is graded and every topic reaches past the close ($GATE_WHY)" ;;
    esac ;;
  waiting)
    log "$SESSION: not ready — $GATE_WHY; leaving the session unclaimed for the retry run"
    exit 0 ;;
  *)
    # "fault", or anything this script does not recognise. Both are faults: an unrecognised answer
    # from the gate is exactly the case where carrying on would claim a session on no evidence.
    log "FATAL: the readiness gate for $SESSION reported '$GATE' — the session is NOT claimed"
    alert "🚨 vol-premium open-reference readiness gate reported a fault on $(hostname) for $SESSION: $GATE_WHY. The session is NOT claimed; bucket 0 did not gain one."
    exit 2 ;;
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
