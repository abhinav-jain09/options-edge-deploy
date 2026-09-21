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
# A session's offset is a per-session STATISTIC, and two things make one: enough DISTINCT
# observations, and observations spread across the session rather than bunched. A raw row count is
# neither - a replayed archive row pairs twice and clears any count floor while describing the same
# instant - so pairs are keyed by the index observation's own timestamp, and the session must be
# COVERED.
#
# RTH is 390 minutes and the index prints many times a second, so a healthy session yields a pair in
# essentially every minute. The floors below are set where a session stops being able to answer the
# question rather than at a statistical threshold nobody can defend: at least half the RTH minutes
# must contain a pair, and there must be at least one distinct pair per covered minute on average.
# A clean low-frequency archive spanning the session passes; 600 pairs inside ten minutes does not.
# Both floors are FRACTIONS of the session actually being scored, not fixed minute counts: a half
# day is 210 minutes, and a fixed 195 would reject a sound sample of one while accepting the first
# 195 consecutive minutes of a normal day. Coverage must also be SPREAD - a sample that stops at
# lunch describes the morning, not the session.
MIN_COVERED_FRACTION = 0.5
MIN_SPAN_FRACTION = 0.8
# The offset is scored from five minutes after the open, so the index has resumed and the first
# prints are not the auction settling.
SCORE_WARMUP_MINUTES = 5


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
        stamp = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    # AN INSTANT WITHOUT AN OFFSET IS NOT AN INSTANT. `2026-09-18T13:35:00` with no Z and no
    # +HH:MM would be read in the HOST's timezone by astimezone(), so the same archive would land
    # in different New York sessions depending on which machine ran the capture - and this ledger
    # is meant to be durable evidence for the >=55 decision, not a function of where it ran.
    if stamp.tzinfo is None or stamp.tzinfo.utcoffset(stamp) is None:
        return None
    return stamp


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
    out, foreign, undated = [], 0, 0
    for record in _records(root, topic, day):
        stamp = _event_time(record)
        if stamp is None:
            # Counted, not silently dropped: a row whose time cannot be established is evidence
            # about the archive, and a session made of them must not look clean.
            undated += 1
            continue
        if stamp.astimezone(ET).date() != session:
            foreign += 1
            continue
        out.append((stamp, record))
    out.sort(key=lambda pair: pair[0])
    return out, foreign, undated


def capture(root: str, day: str, close_et_hhmm: str = "16:00") -> dict:
    session = dt.date.fromisoformat(day)
    open_et = dt.datetime.combine(session, dt.time(9, 30), ET)
    close_hh, close_mm = (int(part) for part in close_et_hhmm.split(":"))
    close_et = dt.datetime.combine(session, dt.time(close_hh, close_mm), ET)
    score_from = open_et + dt.timedelta(minutes=SCORE_WARMUP_MINUTES)
    scoreable_minutes = max(1, int((close_et - score_from).total_seconds() // 60))
    window_start = open_et - dt.timedelta(seconds=WINDOW_S)

    index_all, index_foreign, index_undated = _timed(root, INDEX, day, session)
    es, es_foreign, es_undated = _timed(root, ES, day, session)

    # THE INDEX SERIES IS THE IBKR_INDEX/LAST ONE, everywhere - not only in the offset. The topic
    # also carries IBKR_OPTION_MODEL rows, a different quantity (bug 368), and counting those as
    # index ticks would report a window that is not empty, a first-print that is not the index's,
    # and a freeze shorter than the one that actually happened.
    index = [(t, r) for t, r in index_all
             if r.get("source") == "IBKR_INDEX" and r.get("priceField") == "LAST"]
    option_model = [(t, r) for t, r in index_all if r.get("source") == "IBKR_OPTION_MODEL"]
    # Basis rows carry `valueReferenceMs`, not `eventTime`, so _timed() would silently score them
    # as zero. Counted raw: this field says the series was archived at all, nothing finer.
    # THE BASIS TOPIC CARRIES NO RECORD-TIME FIELD. `valueReferenceMs` is the timestamp of the
    # value the level was anchored to, not of the record - on 2026-09-18 every row referenced
    # 2026-09-17 - so it cannot session-filter anything, and filtering on it would report zero
    # basis records for a session that had thousands. The count is of rows in the partition, named
    # so, and it says only that the series was archived at all.
    basis_rows_in_partition = sum(1 for _ in _records(root, BASIS, day))

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
    # BOUNDED AT BOTH ENDS. Without a close bound a topic that keeps publishing after the bell -
    # or an archive holding only the morning - produced a "session" offset measured over something
    # that is not the session.
    # HALF-OPEN AT THE CLOSE. A tick exactly at 16:00 bucketed into a 386th minute while
    # scoreableMinutes said 385, which could push a borderline session over the coverage floor
    # against a universe the record itself denied.
    # LIVE ONLY, the same bar the window applies. The window demanded quality == "LIVE" while the
    # offset scored every IBKR_INDEX/LAST row, so a session of nothing but stale index prints was
    # accepted into the >=55 ledger with an offset measured against prices that were not live.
    truth = [(t, r) for t, r in index
             if score_from <= t < close_et and r.get("quality") == "LIVE"]
    errors = []
    seen_observations = set()
    covered_minutes = set()
    duplicate_rows = 0
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
        # ONE OBSERVATION, ONE PAIR. A duplicated or replayed archive row describes the same
        # instant twice; counting it twice both clears a floor it should not and weights the
        # median toward whatever was duplicated.
        # IDENTITY IS THE OBSERVATION, not its clock. Two legitimate updates can share a
        # timestamp with different prices; discarding the second as a duplicate would deflate the
        # pair count and bias the median. Only a row that repeats an observation exactly is a
        # duplicate.
        observation = (stamp, price)
        if observation in seen_observations:
            duplicate_rows += 1
            continue
        seen_observations.add(observation)
        covered_minutes.add(stamp.astimezone(ET).replace(second=0, microsecond=0))
        errors.append(equivalent - price)

    covered_fraction = len(covered_minutes) / scoreable_minutes if errors else 0.0
    span_fraction = 0.0
    if seen_observations:
        stamps = sorted(stamp for stamp, _ in seen_observations)
        span_fraction = ((stamps[-1] - stamps[0]).total_seconds() / 60.0) / scoreable_minutes
    offset = residual_p50 = residual_p95 = None
    if errors:
        offset = statistics.median(errors)
        residuals = sorted(abs(e - offset) for e in errors)
        residual_p50 = residuals[len(residuals) // 2]
        residual_p95 = residuals[max(0, int(0.95 * len(residuals)) - 1)]

    level = next((_number(r.get("price")) for _, r in after
                  if r.get("quality") == "LIVE" and _number(r.get("price")) is not None), None)

    if not es_window:
        rejected = "no ES reference in the window"
    elif not reference_fresh:
        rejected = (f"the ES reference is {reference_age_ms:.0f} ms old at the open, over the "
                    f"{PAIR_MAX_AGE_S * 1000} ms limit")
    elif offset is None:
        rejected = "offset not measurable"
    elif covered_fraction < MIN_COVERED_FRACTION:
        rejected = (f"the offset covers {len(covered_minutes)} of {scoreable_minutes} scoreable "
                    f"minutes ({covered_fraction:.0%}), under {MIN_COVERED_FRACTION:.0%}")
    elif span_fraction < MIN_SPAN_FRACTION:
        rejected = (f"the offset spans {span_fraction:.0%} of the session, under "
                    f"{MIN_SPAN_FRACTION:.0%}: it describes part of the day, not the session")
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
        "basisRowsInPartition": basis_rows_in_partition,
        # --- the offset, which is the whole question ---
        "offsetPoints": round(offset, 4) if offset is not None else None,
        "offsetBps": round(offset / level * 1e4, 4) if offset is not None and level else None,
        "offsetResidualP50": round(residual_p50, 4) if residual_p50 is not None else None,
        "offsetResidualP95": round(residual_p95, 4) if residual_p95 is not None else None,
        "offsetPairs": len(errors),
        "sessionCloseEt": close_et.isoformat(),
        "scoreableMinutes": scoreable_minutes,
        "offsetCoveredMinutes": len(covered_minutes),
        "offsetCoveredFraction": round(covered_fraction, 4),
        "offsetSpanFraction": round(span_fraction, 4),
        "offsetDuplicateRowsSkipped": duplicate_rows,
        # A session is ACCEPTED only when it can answer the question: the reference must exist in
        # the window, and the offset must be measurable afterwards. A session that fails either is
        # recorded anyway, with the reason, so the accepted count is never inflated by silence.
        "esReferenceAgeMsAtOpen": reference_age_ms,
        "indexForeignDateRecords": index_foreign,
        "esForeignDateRecords": es_foreign,
        "indexUndatedRecords": index_undated,
        "esUndatedRecords": es_undated,
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
    parser.add_argument("--out", help="ledger directory: accepted/<session>.json holds the "
                                      "sessions the >=55 count is taken over, rejected/ holds the "
                                      "rest with their reason (default: stdout only)")
    parser.add_argument("--close-et", default="16:00",
                        help="session close in New York, HH:MM — 13:00 on a half day (default "
                             "16:00). The coverage rules are fractions of THIS session.")
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

    try:
        hh, mm = (int(part) for part in args.close_et.split(":"))
        dt.time(hh, mm)
    except (ValueError, TypeError):
        print(f"vol-premium-open-reference-capture: not a close time: {args.close_et}",
              file=sys.stderr)
        return 64
    record = capture(args.archive_root, args.session, args.close_et)
    line = json.dumps(record, sort_keys=True)
    print(line)
    if args.out:
        # PUBLICATION IS A LINK, NOT A RESERVATION. The previous version created the destination
        # empty with O_EXCL and filled it afterwards, which review was right to reject: a reader
        # could count an empty file as a captured session, and a crash between the create and the
        # write reserved that session forever - retries refused to repair what they had broken.
        #
        # The record is written COMPLETE to a staging file first, and published with os.link(),
        # which is atomic and fails if the destination exists. A reader therefore sees either
        # nothing or the whole record, a crash leaves only a staging file that the next run
        # replaces, and exactly one of any number of concurrent captures wins - on any filesystem
        # that implements link(), without depending on lock semantics.
        #
        # ACCEPTED AND REJECTED ARE SEPARATE DIRECTORIES. A session that cannot answer the
        # question is still recorded - silence would make the >=55 count look better than the data
        # - but it must not be counted by anything that counts files, so it is not in the same
        # place as the sessions the analysis is taken over.
        verdict = "accepted" if record["accepted"] else "rejected"
        # THE CLAIM IS PER SESSION, NOT PER VERDICT. os.link into accepted/ says nothing about
        # rejected/, so a partial archive could publish a rejection and a later, fuller one an
        # acceptance - two records for one session, both in the ledger, disagreeing. A marker
        # under .published/ is claimed FIRST, so the session is taken exactly once whatever the
        # verdict, and a re-run with better data is refused and says where the record already is.
        published = os.path.join(args.out, verdict)
        staging_dir = os.path.join(args.out, ".staging")
        claims_dir = os.path.join(args.out, ".published")
        # A SYMLINK IS NOT A DIRECTORY THIS SCRIPT OWNS. `.staging -> /shared/other-ledger` would
        # redirect the write outside --out - and so would an ANCESTOR link, which checking only
        # --out and its three children missed entirely: `--out /safe/redirect/ledger` with
        # /safe/redirect -> /other leaves every checked path a real directory while makedirs
        # writes under /other. Comparing the resolved path with the literal one catches a link at
        # ANY component, including one whose target is inside --out, which is still not a
        # directory this script created.
        # THE ROOT IS RESOLVED ONCE, AND EVERYTHING IS WRITTEN UNDER THE RESOLVED PATH. Refusing
        # any symlinked component of --out outright is wrong on a normal machine: macOS resolves
        # /var to /private/var, so every temporary directory would be refused. What the guarantee
        # actually needs is that nothing is written OUTSIDE the directory --out names, and that
        # the ledger's own subdirectories - which this script creates - are not links redirecting
        # a write elsewhere.
        args.out = os.path.realpath(args.out)
        published = os.path.join(args.out, verdict)
        staging_dir = os.path.join(args.out, ".staging")
        claims_dir = os.path.join(args.out, ".published")
        for path in (published, staging_dir, claims_dir):
            if os.path.islink(path):
                print(f"vol-premium-open-reference-capture: {path} is a symlink; refusing to "
                      f"write through it", file=sys.stderr)
                return 73
        os.makedirs(published, exist_ok=True)
        os.makedirs(staging_dir, exist_ok=True)
        os.makedirs(claims_dir, exist_ok=True)
        root_real = os.path.realpath(args.out)
        for path in (published, staging_dir, claims_dir):
            if os.path.commonpath([root_real, os.path.realpath(path)]) != root_real:
                print(f"vol-premium-open-reference-capture: {path} resolves outside {args.out}; "
                      f"refusing", file=sys.stderr)
                return 73

        staging = os.path.join(staging_dir, f"{record['session']}.{os.getpid()}.json")
        with open(staging, "w") as out:
            out.write(line + "\n")
            out.flush()
            os.fsync(out.fileno())
        claim = os.path.join(claims_dir, f"{record['session']}.json")
        final = os.path.join(published, f"{record['session']}.json")
        claimed_now = True
        try:
            os.link(staging, claim)
        except FileExistsError:
            claimed_now = False
        # A CLAIM WITHOUT A RECORD IS A REPAIRABLE STATE, NOT A VERDICT. Killed between the claim
        # and the verdict link, the session had a marker and no record, and every retry reported
        # success - the session could never enter the >=55 set although it was usable. A retry now
        # completes the publication from the EXISTING claim, which is the record that was already
        # decided; only a claim that is already linked into a verdict directory is left alone.
        if not claimed_now:
            for folder in ("accepted", "rejected"):
                if os.path.exists(os.path.join(args.out, folder, f"{record['session']}.json")):
                    print(f"vol-premium-open-reference-capture: {record['session']} is already "
                          f"published; not republished", file=sys.stderr)
                    return 0
            print(f"vol-premium-open-reference-capture: {record['session']} was claimed but never "
                  f"published; completing it from the claim", file=sys.stderr)
            with open(claim) as handle:
                claimed = json.loads(handle.readline())
            final = os.path.join(args.out,
                                 "accepted" if claimed.get("accepted") else "rejected",
                                 f"{record['session']}.json")
            os.makedirs(os.path.dirname(final), exist_ok=True)
        try:
            os.link(claim, final)
        except FileExistsError:
            pass
    return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
