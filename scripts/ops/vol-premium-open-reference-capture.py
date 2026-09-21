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


def _timed(root: str, topic: str, day: str):
    out = []
    for record in _records(root, topic, day):
        stamp = _event_time(record)
        if stamp is not None:
            out.append((stamp, record))
    out.sort(key=lambda pair: pair[0])
    return out


def capture(root: str, day: str) -> dict:
    session = dt.date.fromisoformat(day)
    open_et = dt.datetime.combine(session, dt.time(9, 30), ET)
    window_start = open_et - dt.timedelta(seconds=WINDOW_S)

    index = _timed(root, INDEX, day)
    es = _timed(root, ES, day)
    # Basis rows carry `valueReferenceMs`, not `eventTime`, so _timed() would silently score them
    # as zero. Counted raw: this field says the series was archived at all, nothing finer.
    basis = sum(1 for _ in _records(root, BASIS, day))

    # The index, and the freeze itself.
    before = [(t, r) for t, r in index if t < open_et]
    after = [(t, r) for t, r in index if t >= open_et]
    admissible = [(t, r) for t, r in index if window_start <= t <= open_et
                  and r.get("quality") == "LIVE"]

    # ES through the freeze, and the reference it would supply.
    es_window = [(t, r) for t, r in es if window_start <= t <= open_et
                 and r.get("spxEquivalent") is not None]
    basis_states = sorted({r.get("spxBasisState") for _, r in es_window if r.get("spxBasisState")})

    # THE OFFSET, measured after the fact. Scored only against genuine IBKR_INDEX/LAST prints: the
    # same topic also carries IBKR_OPTION_MODEL rows, which are a different quantity (bug 368), and
    # scoring against those would measure the wrong thing.
    truth = [(t, r) for t, r in index
             if r.get("source") == "IBKR_INDEX" and r.get("priceField") == "LAST"
             and t >= open_et + dt.timedelta(minutes=5)]
    errors = []
    cursor = 0
    for stamp, row in truth:
        while cursor + 1 < len(es) and es[cursor + 1][0] <= stamp:
            cursor += 1
        if cursor >= len(es):
            break
        es_stamp, es_row = es[cursor]
        equivalent = es_row.get("spxEquivalent")
        if equivalent is None or (stamp - es_stamp).total_seconds() > 2:
            continue
        errors.append(equivalent - row["price"])

    offset = residual_p50 = residual_p95 = None
    if errors:
        offset = statistics.median(errors)
        residuals = sorted(abs(e - offset) for e in errors)
        residual_p50 = residuals[len(residuals) // 2]
        residual_p95 = residuals[max(0, int(0.95 * len(residuals)) - 1)]

    level = next((r.get("price") for _, r in after), None)

    return {
        "schemaVersion": 1,
        "session": day,
        "capturedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "archiveRoot": root,
        # --- what slot 0 needs and does not get ---
        "indexTicksInWindow": len(admissible),
        "indexLastBeforeOpenEt": before[-1][0].astimezone(ET).isoformat() if before else None,
        "indexFirstAfterOpenEt": after[0][0].astimezone(ET).isoformat() if after else None,
        "freezeSeconds": round((after[0][0] - before[-1][0]).total_seconds(), 3)
        if before and after else None,
        "indexFirstAfterOpen": level,
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
        "accepted": bool(es_window) and offset is not None,
        "rejectedBecause": None if (es_window and offset is not None) else
        ("no ES reference in the window" if not es_window else "offset not measurable"),
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
        # Append-only, and one line per session: a capture that ran twice must not silently double
        # the denominator the >=55 count is taken over.
        existing = set()
        if os.path.exists(args.out):
            with open(args.out) as handle:
                for entry in handle:
                    try:
                        existing.add(json.loads(entry).get("session"))
                    except ValueError:
                        continue
        if record["session"] in existing:
            print(f"vol-premium-open-reference-capture: {record['session']} is already in "
                  f"{args.out}; not appended", file=sys.stderr)
            return 0
        with open(args.out, "a") as handle:
            handle.write(line + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
