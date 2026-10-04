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
# Where the verified file set is pinned, and what the capture is then pointed at. It holds HARDLINKS
# to archive members, so a pinned member is another NAME for bytes the archive already holds: no file
# data of its own, on the order of a hundred names a day. (On the prod CIFS mount `st_nlink` reads 1
# for both names, so it does not present them as a shared inode the way a local filesystem would —
# what the pin gives is a copy-free second name, which is what matters here, and not a visibly
# shared inode.) Nothing in the pipeline reads the pins
# afterwards; they are kept because they are the record of exactly which files each claim was made
# from, and an operator can prune old sessions from them without touching the archive or the ledger.
# The count is logged on every run so growth is visible rather than inferred.
SNAPSHOT_ROOT="${SNAPSHOT_ROOT:-$LEDGER/.pinned}"
# THE TOPICS ARE NOT CONFIGURABLE HERE, and that is the point. They used to come from an
# environment variable with a three-topic default, which let the gate validate FEWER topics than
# the capture reads while the capture went on reading all three from its own constants — an
# override that could shrink the check and still spend the claim (review round 2). The gate reads
# INDEX, ES and BASIS out of the capture itself, so the gated set cannot drift from the declared
# one, and the names are reported in the log.
#
# IT DOES NOT FOLLOW THAT A FOURTH INPUT WOULD BE GATED — this comment said so once and it was not
# true. Reading three names cannot discover a fourth, whether a new constant or a topic written
# inline at a call site. What holds instead is a REFUSAL: test-archive-reset.sh section 18y fails if
# the capture contains any topic-shaped string literal beyond these three, so a reader that grows an
# input cannot reach production silently — it has to be gated here first.
# THERE IS NO WAY TO CAPTURE A SESSION THE VERIFIER HAS NOT GRADED, and that is deliberate. An
# earlier version had ALLOW_UNGRADED, which fell back to the timing of archive FILE NAMES for a day
# the verifier missed — the night of 2026-10-01 is a real example — and review was right to refuse
# it: the claim it spends is permanent, so an exceptional path that publishes on weaker evidence
# makes the authoritative verdict unusable for that session FOREVER, and an environment variable is
# not an authorisation boundary.
#
# The repair for a missing verdict is to produce the verdict, which costs nothing and is the same
# evidence every other session is judged on:
#
#   ENV=prod ARCHIVE_DIR=/mnt/nas/optionsedge /home/abhinav/oe-ops/oe-archive-verify.sh 2026-10-01
#
# then let the retry run, or invoke this script with that date. Waiting is free: an unclaimed
# session stays claimable, so there is nothing to trade away here.

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
mkdir -p "$LEDGER" || {
  log "FATAL: cannot create the ledger at $LEDGER"
  alert "🚨 vol-premium open-reference ledger unwritable on $(hostname): $LEDGER. Bucket 0 is accruing NO sessions."
  exit 2
}

GATE="$(COMPLETENESS_DIR="$COMPLETENESS_DIR" ARCHIVE_ROOT="$ARCHIVE_ROOT" OE_DAY="$SESSION" \
        CLOSE_ET="$CLOSE_ET" CAPTURE="$CAPTURE" OE_ENV="$ENV_NAME" \
        SNAPSHOT_ROOT="$SNAPSHOT_ROOT" python3 - <<'GATE'
import ast, datetime as dt, glob, gzip, hashlib, importlib.util, json, os, pathlib
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
day = dt.date.fromisoformat(os.environ["OE_DAY"])
hh, mm = (int(part) for part in os.environ["CLOSE_ET"].split(":"))
close = dt.datetime.combine(day, dt.time(hh, mm), ET).astimezone(dt.timezone.utc)
root = os.environ["ARCHIVE_ROOT"]
env_name = os.environ["OE_ENV"]
verdict_path = os.path.join(os.environ["COMPLETENESS_DIR"], day.isoformat() + ".json")

# A GRADE THIS SCRIPT DOES NOT RECOGNISE IS NOT A PASS. oe-archive-verify.sh emits OK, PARTIAL,
# CORRUPT, MISSING, LEGACY and EMPTY, and the first version of this gate refused only EMPTY — so
# CORRUPT (a checksum mismatch, a manifest member absent from disk, an unparseable manifest line)
# would have been accepted as long as the topic reached past the close, and so would any status
# added later. This is an ALLOW-list, which is the difference between a check and a habit.
#
#   OK       — complete as far as the verifier can tell
#   PARTIAL  — the verifier has a reason but not a checksum one; the reasons are LOGGED, and
#              sufficiency is the capture's own business: its coverage, span and per-quarter floors
#              are fractions of the session, so a gap in a topic it reads shows up as missing
#              minutes and is what rejects 2026-09-22. Refusing PARTIAL outright would refuse
#              2026-10-02, whose ES topic is graded PARTIAL on one offset discontinuity and which
#              is a sound session (385 of 385 minutes, four full quarters, an ES reference 2 ms old
#              at the open) — the same permanent loss by the opposite mistake.
#   CORRUPT  — the bytes are wrong. Nothing downstream re-checks a checksum, so this is a FAULT.
#   MISSING / LEGACY / EMPTY / anything else — the series is absent or its completeness cannot be
#              proven. A wait: it may be repairable, and waiting spends nothing.
MAY_PROCEED = ("OK", "PARTIAL")


def answer(state, why):
    print(state + " " + why)
    raise SystemExit(0)


# THE TOPIC SET COMES FROM THE READER, not from this script and not from the environment — and it
# is read WITHOUT RUNNING IT. Importing the capture to reach its constants executes the module, and
# a gate that runs the thing it is about to gate has a side effect nobody asked for: the first
# version of this did exactly that and its own test fixture wrote a spurious "called" line. The
# three names are module-level string assignments, so the source is parsed and they are read off
# the tree.
try:
    tree = ast.parse(pathlib.Path(os.environ["CAPTURE"]).read_text())
except (OSError, SyntaxError, ValueError) as err:
    answer("fault", f"the capture at {os.environ['CAPTURE']} cannot be parsed for its topic set: {err}")
declared = {}
for node in tree.body:
    if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in ("INDEX", "ES", "BASIS"):
                # ASSIGNED TWICE IS NOT DECLARED, for the same reason a topic graded twice is not
                # graded: keeping the last silently resolves a disagreement by file order.
                if target.id in declared:
                    answer("fault", f"the capture assigns {target.id} more than once at module "
                                    f"level, so which topic it reads is not stated")
                declared[target.id] = node.value.value
topics = [declared.get(name) for name in ("INDEX", "ES", "BASIS")]
if not all(isinstance(topic, str) and topic for topic in topics):
    answer("fault", f"the capture does not declare INDEX, ES and BASIS as topic names: {topics!r}")
if len(set(topics)) != len(topics):
    answer("fault", f"the capture declares the same topic twice: {topics!r}")
# WHAT THIS DOES NOT ESTABLISH, stated because the gate must not be read as more than it is: that
# the capture READS only these three. This parses three names; it does not trace the reader. The
# suite is what binds the two together: 18y requires every call that takes a topic to pass one of
# these three, and pins the archive's globbing to _records and input_digest so a new reader cannot
# appear beside them unnoticed. A reader that built a path without globbing would still be outside
# it — that is said here rather than left to be assumed.


def files(topic):
    return glob.glob(os.path.join(root, topic, "dt=" + day.isoformat(), "*.jsonl.gz"))


def from_the_verdict():
    """Each topic's own grade and max_event_time, from oe-archive-verify.sh's grading of the day."""
    try:
        with open(verdict_path) as handle:
            verdict = json.load(handle)
        entries = verdict["topics"]
    except (OSError, ValueError, KeyError, TypeError) as err:
        # A verdict that cannot be read is a FAULT, not a wait: it will not fix itself, and
        # treating it as "try again this evening" is how a gate becomes a delay nobody notices.
        answer("fault", f"the archive verdict at {verdict_path} cannot be read: {err}")

    # THE VERDICT MUST BE ABOUT THIS SESSION, IN THIS ENVIRONMENT. A file is named by whoever put
    # it there: a cached, copied or wrong-day verdict under the right name would otherwise be read
    # as evidence about a day it says nothing about (review round 2).
    if verdict.get("dt") != day.isoformat():
        answer("fault", f"the verdict at {verdict_path} is for dt={verdict.get('dt')!r}, not {day}")
    if verdict.get("env") != env_name:
        answer("fault", f"the verdict at {verdict_path} is for env={verdict.get('env')!r}, "
                        f"not {env_name!r}")

    # A TOPIC GRADED TWICE IS NOT GRADED. Building a dict keeps the last entry silently, so two
    # disagreeing rows would resolve to whichever came second.
    graded = {}
    for entry in entries:
        if not isinstance(entry, dict) or "topic" not in entry:
            answer("fault", f"the verdict at {verdict_path} holds a row that is not a topic entry")
        if entry["topic"] in graded:
            answer("fault", f"the verdict at {verdict_path} grades {entry['topic']} more than once")
        graded[entry["topic"]] = entry

    told = []
    for topic in topics:
        entry = graded.get(topic)
        if entry is None:
            answer("waiting", f"{topic} is not in the archive verdict for {day}")
        status = entry.get("status")
        if status == "CORRUPT":
            answer("fault", f"{topic} is graded CORRUPT for {day} — the bytes on disk do not match "
                            f"the manifest, and nothing downstream re-checks that")
        if status not in MAY_PROCEED:
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
        detail = f"{topic}={status}/{entry.get('records')}"
        if status == "PARTIAL":
            # The reason is the whole content of a PARTIAL. Logging the word alone would hide
            # whether the verifier saw one offset discontinuity or half a session.
            reasons = entry.get("reasons") or []
            detail += "[" + "; ".join(str(reason) for reason in reasons) + "]"
        told.append(detail)
    return told


if not os.path.isfile(verdict_path):
    answer("waiting", "the archive verdict for this session has not been written yet — run "
                      "oe-archive-verify.sh for this date to produce it")
summary = from_the_verdict()

# THE FILES MUST BE THERE NOW, not only in the verdict. A verdict is a statement about a moment
# that has passed; the archive is a mount. With the NAS unmounted, a cached or stale verdict reads
# as evidence while every glob returns nothing, and the capture would publish a permanent record
# saying the session had no ES reference — about the mount, not about the session (review round 2).
for topic in topics:
    if not files(topic):
        answer("waiting", f"{topic} has no archived file on disk for {day}, whatever the verdict "
                          f"says — is {root} mounted?")

# PIN FIRST, THEN VERIFY THE PIN. The order is the whole correctness argument and it was wrong
# once: reading the source and linking it afterwards leaves an interval in which the source can be
# replaced, so the pinned set would hold bytes nothing checked (review round 7). A hardlink is taken
# first, which gives this script a second NAME for those bytes, and the DECOMPRESSION CHECK IS RUN
# AGAINST THE PINNED PATH. Whatever happens to the archive NAME afterwards, the bytes read here are
# the bytes the capture will read, because both names refer to the same file.
#
# On a local filesystem that is a shared inode; on the prod CIFS mount `st_nlink` reads 1 for both
# names, so the mount does not present it as one. What the pinning relies on is the weaker and
# sufficient fact that the second name is not a copy — and what it does NOT give, either way, is
# protection from an in-place rewrite, which is why the reader hashes every member it parses.
#
# HARDLINK OR FAULT — there is no copy fallback. A copy would reintroduce exactly the interval this
# ordering removes (the bytes could change while they are being copied), and it would consume real
# space indefinitely rather than an inode. The ledger and the archive live under the same archive
# root on this host, so a cross-device pin is a misconfiguration worth refusing rather than working
# around.
#
# THE PIN IS WHY A TORN MEMBER IS STILL CAUGHT: the verdict is written once and the spot topics keep
# being archived every ten minutes, so a member can be mid-write after the day was graded. The
# capture's reader catches OSError at OPEN and then iterates, and a torn member raises partway
# through — a failed run, an alert and a day of delay for a condition that resolves itself in
# minutes. Reading every byte is the only test of that which is not a guess: a size, an mtime and a
# successful open all pass on a half-written member.
# THE ARCHIVER'S OWN SHA-256, PER FILE, FROM <topic>/dt=<day>/_manifest.jsonl. The verdict is a
# statement about a moment that has passed: a member replaced under the same name AFTER
# oe-archive-verify.sh graded the day was only decompressed here, never re-hashed, so a mistaken
# restore or sync — or bit-rot — would be pinned, fingerprinted and published as verified bytes
# (review round 11). The fingerprint cannot see it either, since it is name, size and inode, and an
# in-place change keeps all three.
#
# So every member is hashed AS PINNED and compared with the manifest line the archiver wrote when it
# published that file. The two failures are different and are reported differently. A member with NO
# manifest line is one the archiver has not recorded: most often a file it is still writing, and
# sometimes one it published before dying between the rename and the manifest append — an interval
# the archiver's own verifier knows about. Either way it is repairable and is not corruption, so it
# WAITS. A member whose bytes do not match the line it HAS will not fix itself, so it is a FAULT.
def manifest_for(topic):
    path = os.path.join(root, topic, "dt=" + day.isoformat(), "_manifest.jsonl")
    recorded = {}
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except ValueError:
                    # A partial last line is a run that died mid-append; the archiver's own verifier
                    # grades that CORRUPT, and a line this cannot read is not evidence either way.
                    continue
                name, digest = entry.get("file"), entry.get("sha256")
                if isinstance(name, str) and isinstance(digest, str):
                    recorded[os.path.basename(name)] = digest
    except OSError:
        return None
    return recorded


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


snapshot = os.path.join(os.environ["SNAPSHOT_ROOT"], day.isoformat())
expected = set()
members = 0
for topic in topics:
    recorded = manifest_for(topic)
    if recorded is None:
        # No manifest at all is a pre-2026-08-12 date, which oe-archive-verify.sh grades LEGACY and
        # the allow-list already refuses — asserted here rather than assumed, because reaching this
        # with no manifest would mean hashing against nothing and calling it verified.
        answer("fault", f"{topic} has no _manifest.jsonl for {day}, so the bytes on disk cannot be "
                        f"checked against what the archiver recorded")
    folder = os.path.join(snapshot, topic, "dt=" + day.isoformat())
    try:
        os.makedirs(folder, exist_ok=True)
    except OSError as err:
        answer("fault", f"cannot pin the archive for {day} under {snapshot}: {err}")
    for path in files(topic):
        target = os.path.join(folder, os.path.basename(path))
        expected.add(target)
        members += 1
        try:
            os.link(path, target)
        except FileExistsError:
            # A RETRY MAY REUSE ITS OWN PIN, and only its own: os.path.samefile() says the two
            # names are the same file, so re-reading them below costs nothing. A name that resolves
            # to a DIFFERENT file is the case this refuses — the pinned set and the archive have
            # diverged.
            try:
                if not os.path.samefile(path, target):
                    answer("fault", f"{target} is pinned to a different file than the archive now "
                                    f"holds under that name — the pinned set for {day} must be "
                                    f"looked at before this session is claimed")
            except OSError as err:
                answer("fault", f"cannot compare the pin {target} with the archive: {err}")
        except OSError as err:
            answer("fault", f"cannot hardlink {os.path.basename(path)} into the pinned set for "
                            f"{day}: {err}. This script does not fall back to copying — a copy can "
                            f"change while it is made, which is the interval pinning exists to "
                            f"remove")
        # THE ARCHIVER'S CHECKSUM, AGAINST THE PINNED BYTES.
        name = os.path.basename(path)
        if name not in recorded:
            answer("waiting", f"{name} is not in {topic}'s _manifest.jsonl for {day} — the "
                              f"archiver has not recorded it yet, which is what a file still being "
                              f"written looks like")
        try:
            actual = sha256_of(target)
        except OSError as err:
            answer("fault", f"cannot read the pinned {name} to check it against the manifest: {err}")
        if actual != recorded[name]:
            answer("fault", f"{name} does not match the sha256 the archiver recorded for it "
                            f"({actual[:12]} vs {recorded[name][:12]}) — the member on disk is not "
                            f"the one the archive verdict graded, so this session must be looked at "
                            f"before it is claimed")

        # THE CHECK IS ON THE PIN, not on the archive path. Once the link is taken the two names
        # are the same file, so in any state a fixture can set up they read alike — this is about
        # the one state a fixture CANNOT stage, the archive name being replaced in the interval
        # between the link and this read. The pin holds the inode and cannot be re-pointed; the
        # archive name can. So the suite does not distinguish `target` from `path` here, and the
        # reason it is `target` is the race, not a case anyone can show going red.
        try:
            with gzip.open(target, "rb") as handle:
                while handle.read(1 << 20):
                    pass
        except Exception:
            answer("waiting", f"{os.path.basename(path)} does not decompress to its end — "
                              f"the archiver is most likely still writing it")

# AND THE ARCHIVER'S PER-FILE MANIFEST IS PINNED WITH THE MEMBERS, because the reader checks every
# byte it reads against it and is given the pinned set, not the archive. Hardlinked like the
# members: the same file, so what the reader checks against is what this gate checked against.
for topic in topics:
    source = os.path.join(root, topic, "dt=" + day.isoformat(), "_manifest.jsonl")
    target = os.path.join(snapshot, topic, "dt=" + day.isoformat(), "_manifest.jsonl")
    expected.add(target)
    try:
        os.link(source, target)
    except FileExistsError:
        try:
            if not os.path.samefile(source, target):
                answer("fault", f"the pinned _manifest.jsonl for {topic} on {day} is not the one "
                                f"the archive now holds — the pinned set must be looked at")
        except OSError as err:
            answer("fault", f"cannot compare the pinned manifest for {topic} on {day}: {err}")
    except OSError as err:
        answer("fault", f"cannot pin {topic}'s _manifest.jsonl for {day}: {err}")

if not expected:
    # Unreachable while the on-disk check above stands, and asserted rather than assumed: pointing
    # the capture at an empty root would publish a permanent "no ES reference" about nothing.
    answer("fault", f"nothing was pinned for {day}, so there is no verified set to capture from")

# EXACT MEMBERSHIP. The capture globs the pinned directory, so anything else sitting there is an
# input — and a run that died partway through leaves exactly that: members it pinned before the one
# that failed. A later run validates only what the live archive currently holds, so a leftover that
# has since left the archive would reach the record unverified by this run (review round 7). Every
# file under the pinned tree must be one this run just pinned and checked.
present = set()
for topic in topics:
    present |= set(glob.glob(os.path.join(snapshot, topic, "dt=" + day.isoformat(), "*")))
unexpected = sorted(present - expected)
if unexpected:
    answer("fault", f"the pinned set for {day} holds {len(unexpected)} file(s) this run did not "
                    f"pin and check, the first being {os.path.basename(unexpected[0])} — a "
                    f"leftover from a run that died partway through. Look at "
                    f"{snapshot} before this session is claimed")

# THE MARKER IS WRITTEN LAST, AND IT IS WHAT THE CAPTURE REQUIRES BEFORE IT WILL PUBLISH. Running
# the capture by hand against the raw archive used to claim a session with none of these checks
# having run (review round 8). It is written here, into the pin, only once the verdict, the grades,
# the on-disk check, the decompression check and the membership check have all passed.
#
# IT NAMES THE SESSION AND FINGERPRINTS THE INPUT. A marker holding only the session date
# authorised whatever the directory happened to contain, so typing a date into any directory made
# it publishable, and a member added after the gate ran would have been read without complaint
# (review round 9). The digest is the capture's own input_digest() over the files a capture of this
# session will read, and the capture recomputes and compares it.
#
# WHAT IT IS NOT: a security boundary. Anyone who can write this marker can write the ledger
# directly, or edit the capture; on a host where the operator owns the archive no artifact a script
# writes can be beyond them. What it does is make this path the only easy one and every way round it
# a deliberate act — which is the claim, and is less than "publication is bound to verified
# evidence" said.
marker = os.path.join(snapshot, ".vp-open-reference-gate-ok")
try:
    spec = importlib.util.spec_from_file_location("orc_for_digest", os.environ["CAPTURE"])
    reader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reader)
    digest = reader.input_digest(snapshot, day.isoformat())
except Exception as err:
    answer("fault", f"cannot fingerprint the pinned set for {day} with the capture's own "
                    f"input_digest(): {err}")
try:
    with open(marker, "w") as handle:
        handle.write(day.isoformat() + "\n" + digest + "\n")
except OSError as err:
    answer("fault", f"cannot write the gate marker into the pinned set for {day}: {err}")

print(f"ready {members} files pinned and checked; " + " ".join(summary))
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
    log "$SESSION: the archive is graded and every topic reaches past the close ($GATE_WHY)" ;;
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

# THE CAPTURE READS THE PINNED SET, NOT THE LIVE ARCHIVE. That is the whole point of pinning: the
# bytes that produce the permanent record are the bytes the gate verified, and no member that
# arrives afterwards can reach it.
PINNED="$SNAPSHOT_ROOT/$SESSION"

# AND THE PIN IS RE-CHECKED IMMEDIATELY BEFORE THE READER RUNS. The gate verified it and then
# returned; anything that removed the pinned set in between — a prune of the pinned sets, which the
# advice below invites — would leave the reader an empty directory, and a record computed over one
# looks exactly like a session the market could not answer for (review round 8). The capture refuses
# to publish from an input with no records, so the claim cannot be spent either way; this is here so
# the failure says what happened rather than arriving as a mysterious rejection.
#
# NO TEST STAGES THIS, and a case that pretended to was removed rather than left in: the window is
# inside one process and cannot be reached from a fixture. What IS tested is the control that makes
# the window harmless — tests/test_vol_premium_open_reference_capture.py asserts the capture refuses
# to publish from an input holding no records, with or without this check.
PIN_FILES="$(find "$PINNED" -type f -name '*.jsonl.gz' 2>/dev/null | grep -c . || true)"
if [ ! -f "$PINNED/.vp-open-reference-gate-ok" ] || [ "${PIN_FILES:-0}" -eq 0 ]; then
  log "FATAL: the pinned set for $SESSION is gone or incomplete at $PINNED (${PIN_FILES:-0} members, marker $([ -f "$PINNED/.vp-open-reference-gate-ok" ] && echo present || echo absent)) — the session is NOT claimed"
  alert "🚨 vol-premium open-reference pinned set for $SESSION vanished between the gate and the capture on $(hostname). The session is NOT claimed. Something is removing $SNAPSHOT_ROOT while a capture is running."
  exit 2
fi

log "$SESSION: capturing (close $CLOSE_ET ET, pinned set $PINNED with ${PIN_FILES} members, ledger $LEDGER)"
RECORD="$(python3 "$CAPTURE" --session "$SESSION" --archive-root "$PINNED" \
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

# AND SO IS THE COST OF THE PINS, which accumulate by design and were reported as a number per run
# and nothing else — growth nobody would notice until a directory got slow or a filesystem ran out
# of entries. They are hardlinks: the file data and the inodes are the archive's, and what this
# script adds is directory entries, on the order of a hundred a session.
#
# The total is logged every run, and WHILE it is over PIN_LINK_WARN every successful run alerts —
# not once. That is the honest description: there is no state here that remembers having warned, and
# inventing one would mean a marker file that could itself go missing. A repeating alert for a
# condition that only an operator can clear is the right direction for it to fail in.
#
# It is a warning, not a refusal: the pins are evidence, and deciding which sessions no longer need
# theirs is the operator's call, not this script's — removing a pinned directory touches neither the
# archive nor the ledger.
PIN_LINK_WARN="${PIN_LINK_WARN:-200000}"
PINS="$(find "$SNAPSHOT_ROOT" -type f -name '*.jsonl.gz' 2>/dev/null | grep -c . || true)"
PIN_SESSIONS="$(find "$SNAPSHOT_ROOT" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | grep -c . || true)"
log "pinned sets: ${PIN_SESSIONS:-0} sessions, ${PINS:-0} hardlinks under $SNAPSHOT_ROOT (no file data of their own; prune freely)"
if [ "${PINS:-0}" -ge "$PIN_LINK_WARN" ]; then
  log "WARNING: the pinned sets hold ${PINS} hardlinks, at or over the ${PIN_LINK_WARN} mark — this repeats every run until they are pruned"
  alert "⚠️ vol-premium open-reference pinned sets hold ${PINS} hardlinks on $(hostname) (${PIN_SESSIONS} sessions, threshold ${PIN_LINK_WARN}). They are hardlinks — no file data of their own — and old sessions under $SNAPSHOT_ROOT can be pruned without touching the archive or the ledger. This alert repeats every run until they are."
fi
exit 0
