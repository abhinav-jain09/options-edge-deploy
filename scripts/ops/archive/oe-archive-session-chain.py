#!/usr/bin/env python3
"""oe-archive-session-chain.py: the per-session completeness chain of the dev book archive.

ES Level Absorption Requirements revision 15, section 10 "Capture" items 4 to 7 (A-GAP-05). It READS the archive's
manifests (and, only with --scan-legacy, the gzip files of captures that predate the new manifest fields); it
writes nothing.

A session is the trade date D (America/New_York: local date before 18:00, else the next date; Saturday and Sunday
map to Monday). For each of the topics (default: the four of the MBO engine's book,
es.mbo.level-flow, es.mbo.level-flow-final, es.mbo.health, es.mbo.closures) and each partition:

  1. FILE CHAIN. The manifest ranges [offset_from, offset_to) of the files, sorted, leave no hole: each file's
     first offset is the previous file's end (the requirement's "last offset + 1" in inclusive terms). Duplicate
     ranges (a crash re-read) and overlaps are not holes. On es.mbo.closures the BOOK closures (id 3501) sit on a
     compacted key and leave holes between the OFFSETS OF RECORDS; a range chain is blind to record-level holes by
     construction, so the 3501 records are exempt and the chain is the one over the FINAL closures' ranges.
  2. CAPTURE CHAIN. Manifest lines of one run share archived_at: that is one capture. Each capture's
     capture_start_checkpoint equals the previous capture's last offset (its largest offset_to); its
     broker_beginning_offset is at or before its starting checkpoint; it records capture_wall_start and
     capture_wall_end (start not after end). A capture without these members (written before 2026-11-08) cannot
     prove any of this: it makes the sessions it touches BOOK_INCOMPLETE (LEGACY_CAPTURE), never complete.
     A manifest line whose gzip file is gone is a hole too (FILE_MISSING): the manifests alone prove nothing about the data.
  3. THE SESSION'S SPAN. The chain must run from a capture that STARTED (capture_wall_start) at or before the
     session's start instant (18:00 America/New_York of the previous calendar day) through the terminal file: the
     first file whose largest bucketEnd (max_bucket_end) is at or after the next trade date's 09:30
     America/New_York. No capture before the start, or no file reaching the terminal, is BOOK_INCOMPLETE.

A session is BOOK_COMPLETE when every partition of every requested topic passes; otherwise BOOK_INCOMPLETE with the
reasons, and it is excluded from every fold. A trade date the exchange calendar marks closed holds no session
(NO_SESSION). The calendar is market_calendar.py, found in CALENDAR_DIR, beside this script, or in ../../jenkins.
Without it the checker REFUSES to run (exit 2): a weekday-only guess would report exchange holidays as sessions.
--weekday-only is the explicit, printed opt-out for a host that cannot have the calendar.

USAGE
  oe-archive-session-chain.py --root /Volumes/database/optionsedge [--env dev] [--topics "a b"]
        (--session YYYY-MM-DD | --all) [--json] [--strict] [--scan-legacy]
Exit 0 normally; with --strict, 1 when any requested session is BOOK_INCOMPLETE; 2 on a usage error or a missing calendar.
"""
import argparse
import datetime
import glob
import gzip
import json
import os
import re
import sys
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC = datetime.timezone.utc
BOOK_TOPICS = ["es.mbo.level-flow", "es.mbo.level-flow-final", "es.mbo.health", "es.mbo.closures"]
BE = re.compile(rb'"bucketEnd":(\d+)')


def trade_date(ms):
    """The trade date (datetime.date) of an instant in epoch milliseconds. Same rule as bucket_split() in
    oe-archive-kafka.sh; test-archive-session-chain.py binds the two to one table of vectors."""
    local = datetime.datetime.fromtimestamp((ms // 60000) * 60, tz=UTC).astimezone(NY)
    day = local.date()
    if local.hour >= 18:
        day += datetime.timedelta(days=1)
    while day.weekday() >= 5:
        day += datetime.timedelta(days=1)
    return day


def ny_ms(day, hour, minute=0):
    return int(datetime.datetime(day.year, day.month, day.day, hour, minute, tzinfo=NY).timestamp() * 1000)


def load_calendar():
    here = os.path.dirname(os.path.abspath(__file__))
    for d in [os.environ.get("CALENDAR_DIR", ""), here, os.path.join(here, "..", "..", "jenkins")]:
        if d and os.path.isfile(os.path.join(d, "market_calendar.py")):
            sys.path.insert(0, d)
            try:
                from market_calendar import MarketCalendar  # type: ignore
                return MarketCalendar(), "market_calendar.py"
            except Exception:
                pass
            finally:
                sys.path.pop(0)
    return None, None


class CalendarMissing(Exception):
    pass


class Cal:
    def __init__(self, weekday_only=False):
        self.cal, self.name = load_calendar()
        if self.cal is None:
            if not weekday_only:
                raise CalendarMissing("market_calendar.py not found (CALENDAR_DIR, beside this script, or ../../jenkins) or not importable; "
                                      "refusing to guess which days hold a session (use --weekday-only to accept weekdays only)")
            self.name = "weekday-only (explicit)"

    def is_trading_day(self, d):
        if d.weekday() >= 5:
            return False
        return self.cal.is_trading_day(d) if self.cal else True

    def next_trading_day(self, d):
        p = d + datetime.timedelta(days=1)
        for _ in range(30):
            if self.is_trading_day(p):
                return p
            p += datetime.timedelta(days=1)
        raise ValueError("no trading day within 30 days after %s" % d)


def parse_wall(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except Exception:
        return None


def scan_bounds(path):
    """(min, max) bucketEnd over a gzip capture file, or (None, None)."""
    lo = hi = None
    with gzip.open(path, "rb") as f:
        for line in f:
            m = BE.search(line)
            if m:
                v = int(m.group(1))
                lo = v if lo is None or v < lo else lo
                hi = v if hi is None or v > hi else hi
    return lo, hi


def load_topic(root, env, topic, scan_legacy):
    """-> (files_by_partition, problems). A file is a dict of the manifest members plus 'dir'."""
    files, problems = {}, []
    base = os.path.join(root, "kafka", env, topic)
    for mf in sorted(glob.glob(os.path.join(base, "dt=*", "_manifest.jsonl"))):
        dtdir = os.path.dirname(mf)
        with open(mf, "rb") as fh:
            raw = fh.read()
        if raw and not raw.endswith(b"\n"):
            problems.append("MANIFEST_UNTERMINATED:%s" % os.path.basename(dtdir))
        for n, line in enumerate(raw.decode("utf-8", "replace").split("\n"), 1):
            if not line.strip():
                continue
            try:
                e = json.loads(line)
                assert isinstance(e, dict)
            except Exception:
                problems.append("MANIFEST_UNPARSEABLE:%s:line%d" % (os.path.basename(dtdir), n))
                continue
            if e.get("attempt") == "no_progress" or "file" not in e or "offset_from" not in e or "offset_to" not in e:
                continue
            e["dir"] = dtdir
            if "max_bucket_end" not in e and scan_legacy:
                p = os.path.join(dtdir, e["file"])
                if os.path.isfile(p):
                    lo, hi = scan_bounds(p)
                    e["first_bucket_end"], e["last_bucket_end"], e["max_bucket_end"] = lo, hi, hi
                    e["_scanned"] = True
            files.setdefault(int(e.get("partition", 0)), []).append(e)
    return files, problems


def dedupe(entries):
    best = {}
    for e in entries:
        k = (int(e["offset_from"]), int(e["offset_to"]))
        if k not in best or str(e.get("archived_at", "")) >= str(best[k].get("archived_at", "")):
            best[k] = e
    return [best[k] for k in sorted(best)]


def file_holes(entries):
    holes, cur = [], None
    for e in entries:
        a, b = int(e["offset_from"]), int(e["offset_to"])
        if cur is None:
            cur = b
            continue
        if a > cur:
            holes.append((cur, a))
        cur = max(cur, b)
    return holes


def captures_of(entries):
    caps = {}
    for e in entries:
        caps.setdefault(str(e.get("archived_at", "")), []).append(e)
    out = []
    for stamp, es in caps.items():
        starts = {e.get("capture_start_checkpoint", "<absent>") for e in es}
        legacy = any("capture_start_checkpoint" not in e or "broker_beginning_offset" not in e or
                     "capture_wall_start" not in e or "capture_wall_end" not in e for e in es)
        out.append({
            "stamp": stamp, "files": es, "legacy": legacy,
            "start": None if legacy else es[0].get("capture_start_checkpoint"),
            "start_set": starts,
            "begin": None if legacy else es[0].get("broker_beginning_offset"),
            "wall_start": None if legacy else parse_wall(str(min(e["capture_wall_start"] for e in es))),
            "wall_end": None if legacy else parse_wall(str(max(e["capture_wall_end"] for e in es))),
            "first_from": min(int(e["offset_from"]) for e in es),
            "last_to": max(int(e["offset_to"]) for e in es),
        })
    out.sort(key=lambda c: (c["first_from"], c["stamp"]))
    return out


def check_partition(entries, start_ms, terminal_ms):
    """Reasons a (topic, partition) is not complete for a session [start_ms .. terminal_ms]; [] when complete."""
    why = []
    entries = dedupe(entries)
    if not entries:
        return ["NO_FILES"]
    caps = captures_of(entries)
    # the capture that started at or before the session start: the latest such
    c0 = None
    for c in caps:
        if not c["legacy"] and c["wall_start"] is not None and int(c["wall_start"].timestamp() * 1000) <= start_ms:
            c0 = c
    if c0 is None:
        legacy_before = any(c["legacy"] for c in caps)
        return ["NO_CAPTURE_BEFORE_SESSION_START" + ("(only LEGACY_CAPTURE lines exist: written before capture_wall_start was recorded)" if legacy_before else "")]
    # the terminal file: the first (by offset) AT OR AFTER the starting capture whose largest bucketEnd reaches the terminal
    # instant; a file before the starting capture cannot end this session, whatever bucketEnd it holds
    term = None
    for e in entries:
        if int(e["offset_to"]) <= c0["first_from"]:
            continue
        mb = e.get("max_bucket_end")
        if mb is not None and int(mb) >= terminal_ms:
            term = e
            break
    if term is None:
        if any("max_bucket_end" not in e for e in entries):
            why.append("NO_BUCKET_BOUNDS(legacy files lack max_bucket_end; use --scan-legacy to read them)")
        why.append("TERMINAL_NOT_REACHED")
        return why
    lo, hi = c0["first_from"], int(term["offset_to"])
    span = [e for e in entries if int(e["offset_to"]) > lo and int(e["offset_from"]) < hi]
    for a, b in file_holes(span):
        why.append("FILE_HOLE[%d,%d)" % (a, b))
    if span and int(span[0]["offset_from"]) > lo:
        why.append("FILE_HOLE[%d,%d)" % (lo, int(span[0]["offset_from"])))
    for e in span:
        if not os.path.isfile(os.path.join(e["dir"], e["file"])):
            why.append("FILE_MISSING:%s" % e["file"])
    chain = [c for c in caps if c["first_from"] >= c0["first_from"] and c["first_from"] < hi]
    prev = None
    for c in chain:
        tag = c["stamp"]
        if c["legacy"]:
            why.append("LEGACY_CAPTURE:%s" % tag)
            prev = c
            continue
        if len(c["start_set"]) != 1:
            why.append("CAPTURE_START_INCONSISTENT:%s" % tag)
        if c["wall_start"] is None or c["wall_end"] is None or c["wall_start"] > c["wall_end"]:
            why.append("CAPTURE_WALL_CLOCK:%s" % tag)
        if c["start"] is not None and c["begin"] is not None and int(c["begin"]) > int(c["start"]):
            why.append("BROKER_BEGINNING_AFTER_START_CHECKPOINT:%s" % tag)
        if c["start"] is None and any(x is not c and x["first_from"] < c["first_from"] for x in caps):
            # a null starting checkpoint is the FIRST capture of the archive (nothing before it); any later one has lost the continuity proof
            why.append("CAPTURE_START_CHECKPOINT_MISSING:%s" % tag)
        if c["start"] is not None and c["first_from"] != int(c["start"]):
            why.append("CAPTURE_CLAMPED:%s(start checkpoint %s, first offset %s)" % (tag, c["start"], c["first_from"]))
        if prev is not None and not prev["legacy"] and c["start"] is not None and int(c["start"]) != prev["last_to"]:
            why.append("CAPTURE_CHAIN_BREAK:%s(start %s, previous capture ended %s)" % (tag, c["start"], prev["last_to"]))
        if prev is not None and prev["legacy"]:
            pass  # already reported as LEGACY_CAPTURE
        if prev is None and c is not c0:
            pass
        prev = c
    return why


def session_report(root, env, topics, day, cal, scan_legacy):
    if not cal.is_trading_day(day):
        return {"session": day.isoformat(), "status": "NO_SESSION", "reason": "closed trading day", "topics": {}}
    start_ms = ny_ms(day - datetime.timedelta(days=1), 18)
    terminal_ms = ny_ms(cal.next_trading_day(day), 9, 30)
    res, ok = {}, True
    for t in topics:
        files, problems = load_topic(root, env, t, scan_legacy)
        why = list(problems)
        if not files:
            why.append("NO_FILES")
        for p, es in sorted(files.items()):
            for w in check_partition(es, start_ms, terminal_ms):
                why.append("p%d:%s" % (p, w))
        res[t] = why
        ok = ok and not why
    return {"session": day.isoformat(), "status": "BOOK_COMPLETE" if ok else "BOOK_INCOMPLETE", "topics": res,
            "start_instant_ms": start_ms, "terminal_instant_ms": terminal_ms}


def dates_in_archive(root, env, topics):
    ds = set()
    for t in topics:
        for d in glob.glob(os.path.join(root, "kafka", env, t, "dt=*")):
            try:
                ds.add(datetime.date.fromisoformat(os.path.basename(d)[3:]))
            except ValueError:
                pass
    return sorted(ds)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", required=True)
    ap.add_argument("--env", default="dev")
    ap.add_argument("--topics", default=" ".join(BOOK_TOPICS))
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--session")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--scan-legacy", action="store_true")
    ap.add_argument("--weekday-only", action="store_true", help="accept weekdays as the only calendar (printed); default is to refuse without market_calendar.py")
    a = ap.parse_args(argv)
    topics = a.topics.split()
    try:
        cal = Cal(a.weekday_only)
    except CalendarMissing as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    if a.session:
        try:
            days = [datetime.date.fromisoformat(a.session)]
        except ValueError:
            print("usage: --session must be YYYY-MM-DD", file=sys.stderr)
            return 2
    else:
        days = dates_in_archive(a.root, a.env, topics)
    reports = [session_report(a.root, a.env, topics, d, cal, a.scan_legacy) for d in days]
    if a.json:
        print(json.dumps({"calendar": cal.name, "sessions": reports}, indent=1, sort_keys=True))
    else:
        print("calendar=%s topics=%s" % (cal.name, ",".join(topics)))
        for r in reports:
            print("SESSION %s %s" % (r["session"], r["status"]))
            for t, why in sorted(r["topics"].items()):
                if why:
                    print("  %s: %s" % (t, "; ".join(why)))
    bad = any(r["status"] == "BOOK_INCOMPLETE" for r in reports)
    return 1 if (a.strict and bad) else 0


if __name__ == "__main__":
    sys.exit(main())
