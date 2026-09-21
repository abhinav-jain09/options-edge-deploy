#!/usr/bin/env python3
"""One record per session of everything needed to settle vol-premium bucket 0.

WHY THIS EXISTS. `ReturnGrid` slot 0 is the session open, and it is never observed: the SPX index
series goes silent for about 302 s across the bell (measured 0 admissible ticks in
[open-30s, open] on 2026-09-17 and 2026-09-18), so `shapeComplete` is false and no baseline
publishes. Options cannot fill the gap either - SPX options do not quote before 09:30, so put-call
parity has nothing to work with (0 pre-open seconds on 4 sessions of the NAS SPXW corpus). The one
series that IS present through the freeze is ES, which the feed already publishes as
`spxEquivalent` (ES minus `spxBasis`).

WHAT IS NOT YET KNOWN, and what this exists to make knowable: `spxEquivalent` carries a per-session
offset against the index that is only measurable AFTER the index resumes. It was +0.01 bps on
2026-09-17 and -4.71 bps on 2026-09-18 - consecutive sessions, a 4.72 bps swing, against a
first-minute move of -18.60 bps. An offset worth a quarter of the move it would measure, unknown at
the moment it is needed, is not a reference. Whether it can be PREDICTED from pre-open information
is an empirical question, and answering it needs sessions.

WHY IT CANNOT BE BACKFILLED. The prod NAS archive holds 2 sessions of `underlying.spx.index.price`,
3 of `underlying.es.price` and 4 of `spx.basis.state`. There is no history to query: the series was
never captured. So the sessions accrue forward, one per day, from whenever this starts running.

WHAT IT READS. Only the archived topics under ARCHIVE_ROOT/<topic>/dt=<session>/. Nothing live,
nothing on a broker, no consumer group. It writes one JSON line and touches nothing else.
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import gzip
import json
import os
import statistics
import sys
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
INDEX = "underlying.spx.index.price"
ES = "underlying.es.price"
BASIS = "spx.basis.state"
# The admissibility window slot 0 is scored over, and the engine's own MAX_TICK_AGE_MS.
WINDOW_S = 30
# The staleness limit an ES value must meet to stand for a moment in time - the same one the offset
# pairing uses, because a reference and a pair are the same kind of claim.
PAIR_MAX_AGE_S = 2
# A session's offset is a per-session STATISTIC. One coincident pair produces a median and two
# residuals that describe nothing, and admitting it would let a partial archive count toward the
# >=55. RTH at one pair a second is ~23 000; this floor only excludes the degenerate case.
MIN_OFFSET_PAIRS = 600


def _records(root: str, topic: str, day: str):
    pattern = os.path.join(root, topic, f"dt={day}", "*.jsonl.gz")
    for path in sorted(glob.glob(pattern)):
        try:
            handle = gzip.open(path, "rt", errors="replace")
        except OSError:
            continue
        with handle:
            for line in handle:
                brace = line.find("{")
                if brace < 0:
                    continue
                try:
                    yield json.loads(line[brace:])
                except ValueError:
                    continue


def _event_time(record: dict):
    raw = record.get("eventTime")
    if not isinstance(raw, str):
        return None
    try:
        return dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _number(value):
    """A price that is not a number is not a price. Archived values are parsed JSON, not trusted
    types: a string, a null or a list here used to raise inside the arithmetic below and take the
    whole day's capture with it, which biases the forward sample by losing exactly the days whose
    archive was odd."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value != value or value in (float("inf"), float("-inf")):   # NaN / inf
        return None
    return float(value)


def _timed(root: str, topic: str, day: str, session: dt.date):
    """Records under dt=<session>, RESTRICTED to those whose own event time is that session in New
    York. The partition name is a storage decision; it is not evidence about when the record
    happened, and a late or mispartitioned row scored against the wrong 09:30 would fabricate a
    session."""
    out, foreign = [], 0
    for record in _records(root, topic, day):
        stamp = _event_time(record)
        if stamp is None:
            continue
        if stamp.astimezone(ET).date() != session:
            foreign += 1
            continue
        out.append((stamp, record))
    out.sort(key=lambda pair: pair[0])
    return out, foreign


def capture(root: str, day: str) -> dict:
    session = dt.date.fromisoformat(day)
    open_et = dt.datetime.combine(session, dt.time(9, 30), ET)
    window_start = open_et - dt.timedelta(seconds=WINDOW_S)

    index_all, index_foreign = _timed(root, INDEX, day, session)
    es, es_foreign = _timed(root, ES, day, session)

    # THE INDEX SERIES IS THE IBKR_INDEX/LAST ONE, everywhere - not only in the offset. The topic
    # also carries IBKR_OPTION_MODEL rows, a different quantity (bug 368), and counting those as
    # index ticks would report a window that is not empty, a first-print that is not the index's,
    # and a freeze shorter than the one that actually happened.
    index = [(t, r) for t, r in index_all
             if r.get("source") == "IBKR_INDEX" and r.get("priceField") == "LAST"]
    option_model = [(t, r) for t, r in index_all if r.get("source") == "IBKR_OPTION_MODEL"]
    # Basis rows carry `valueReferenceMs`, not `eventTime`, so _timed() would silently score them
    # as zero. Counted raw: this field says the series was archived at all, nothing finer.
    basis = sum(1 for _ in _records(root, BASIS, day))

    # The index, and the freeze itself.
    before = [(t, r) for t, r in index if t < open_et]
    # THE PRE-OPEN SERIES IS NOT THE INDEX. On 2026-09-18 the last record before the bell was an
    # IBKR_OPTION_MODEL row at 09:24:59 and there was no IBKR_INDEX/LAST print before the open at
    # all - which is bug 368, and is invisible if both sources are counted as one series. Both are
    # recorded: the freeze is measured on the true index, and the any-source print is kept beside
    # it with its source named, so a null freeze can be read as "the index never printed" rather
    # than as a gap in the capture.
    before_any = [(t, r) for t, r in index_all if t < open_et]
    after = [(t, r) for t, r in index if t >= open_et]
    admissible = [(t, r) for t, r in index if window_start <= t <= open_et
                  and r.get("quality") == "LIVE"]
    option_model_in_window = [(t, r) for t, r in option_model if window_start <= t <= open_et]

    # ES through the freeze, and the reference it would supply.
    es_window = [(t, r) for t, r in es if window_start <= t <= open_et
                 and _number(r.get("spxEquivalent")) is not None]
    # FRESHNESS AT THE OPEN, held to the same 2 s the offset pairing uses. An ES tick at 09:29:30
    # and nothing after is inside the window but is not a reference FOR THE OPEN, and accepting it
    # would put a 30-second-old price into slot 0 under the name of an observation.
    reference_age_ms = None
    if es_window:
        reference_age_ms = round((open_et - es_window[-1][0]).total_seconds() * 1000, 1)
    reference_fresh = reference_age_ms is not None and reference_age_ms <= PAIR_MAX_AGE_S * 1000
    basis_states = sorted({r.get("spxBasisState") for _, r in es_window if r.get("spxBasisState")})

    # THE OFFSET, measured after the fact. Scored only against genuine IBKR_INDEX/LAST prints: the
    # same topic also carries IBKR_OPTION_MODEL rows, which are a different quantity (bug 368), and
    # scoring against those would measure the wrong thing.
    truth = [(t, r) for t, r in index if t >= open_et + dt.timedelta(minutes=5)]
    errors = []
    cursor = 0
    for stamp, row in truth:
        while cursor + 1 < len(es) and es[cursor + 1][0] <= stamp:
            cursor += 1
        if cursor >= len(es):
            break
        es_stamp, es_row = es[cursor]
        equivalent = _number(es_row.get("spxEquivalent"))
        price = _number(row.get("price"))
        if equivalent is None or price is None:
            continue
        if (stamp - es_stamp).total_seconds() > PAIR_MAX_AGE_S:
            continue
        errors.append(equivalent - price)

    offset = residual_p50 = residual_p95 = None
    if errors:
        offset = statistics.median(errors)
        residuals = sorted(abs(e - offset) for e in errors)
        residual_p50 = residuals[len(residuals) // 2]
        residual_p95 = residuals[max(0, int(0.95 * len(residuals)) - 1)]

    level = next((_number(r.get("price")) for _, r in after
                  if _number(r.get("price")) is not None), None)

    if not es_window:
        rejected = "no ES reference in the window"
    elif not reference_fresh:
        rejected = (f"the ES reference is {reference_age_ms:.0f} ms old at the open, over the "
                    f"{PAIR_MAX_AGE_S * 1000} ms limit")
    elif offset is None:
        rejected = "offset not measurable"
    elif len(errors) < MIN_OFFSET_PAIRS:
        rejected = f"only {len(errors)} offset pairs, under the {MIN_OFFSET_PAIRS} floor"
    else:
        rejected = None
    accepted = rejected is None

    return {
        "schemaVersion": 2,
        "session": day,
        "capturedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        # --- what slot 0 needs and does not get ---
        "indexTicksInWindow": len(admissible),
        "indexLastBeforeOpenEt": before[-1][0].astimezone(ET).isoformat() if before else None,
        "indexFirstAfterOpenEt": after[0][0].astimezone(ET).isoformat() if after else None,
        "freezeSeconds": round((after[0][0] - before[-1][0]).total_seconds(), 3)
        if before and after else None,
        "indexFirstAfterOpen": level,
        "anySourceLastBeforeOpenEt": before_any[-1][0].astimezone(ET).isoformat()
        if before_any else None,
        "anySourceLastBeforeOpenSource": before_any[-1][1].get("source") if before_any else None,
        # --- the candidate reference ---
        "esTicksInWindow": len(es_window),
        "esSpxEquivalentAtOpen": es_window[-1][1]["spxEquivalent"] if es_window else None,
        "esBasisStatesInWindow": basis_states,
        "basisRecords": basis,
        # --- the offset, which is the whole question ---
        "offsetPoints": round(offset, 4) if offset is not None else None,
        "offsetBps": round(offset / level * 1e4, 4) if offset is not None and level else None,
        "offsetResidualP50": round(residual_p50, 4) if residual_p50 is not None else None,
        "offsetResidualP95": round(residual_p95, 4) if residual_p95 is not None else None,
        "offsetPairs": len(errors),
        # A session is ACCEPTED only when it can answer the question: the reference must exist in
        # the window, and the offset must be measurable afterwards. A session that fails either is
        # recorded anyway, with the reason, so the accepted count is never inflated by silence.
        "esReferenceAgeMsAtOpen": reference_age_ms,
        "indexForeignDateRecords": index_foreign,
        "esForeignDateRecords": es_foreign,
        "optionModelTicksInWindow": len(option_model_in_window),
        "accepted": accepted,
        "rejectedBecause": rejected,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--session", required=True, help="session date, YYYY-MM-DD")
    parser.add_argument("--archive-root", required=True,
                        help="archive root holding <topic>/dt=<session>/*.jsonl.gz")
    parser.add_argument("--out", help="append the record to this file (default: stdout only)")
    args = parser.parse_args(argv)

    try:
        dt.date.fromisoformat(args.session)
    except ValueError:
        print(f"vol-premium-open-reference-capture: not a session date: {args.session}",
              file=sys.stderr)
        return 64
    if not os.path.isdir(args.archive_root):
        print(f"vol-premium-open-reference-capture: no archive root at {args.archive_root}",
              file=sys.stderr)
        return 66

    record = capture(args.archive_root, args.session)
    line = json.dumps(record, sort_keys=True)
    print(line)
    if args.out:
        # ONE LINE PER SESSION, and the check and the write are the SAME critical section. A
        # read-then-append let a scheduled run and a manual one both find the session absent and
        # both append it, inflating the denominator the >=55 count is taken over. The lock is held
        # across both, and it is an exclusive lock on the ledger itself, so it also covers a reader
        # on another host sharing the file.
        import fcntl

        with open(args.out, "a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                existing = set()
                for entry in handle:
                    try:
                        existing.add(json.loads(entry).get("session"))
                    except ValueError:
                        continue
                if record["session"] in existing:
                    print(f"vol-premium-open-reference-capture: {record['session']} is already in "
                          f"{args.out}; not appended", file=sys.stderr)
                    return 0
                handle.seek(0, os.SEEK_END)
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
