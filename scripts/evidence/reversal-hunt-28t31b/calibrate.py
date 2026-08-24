#!/usr/bin/env python3
"""Notification-volume calibration for the reversal hunt: 40t/60s vs 28t/62s.

Answers exactly one question: under the partial replay described below, how many
VOICE-ELIGIBLE PIVOT CANDIDATES does each threshold pair produce per session?
It does NOT predict production call counts and makes no claim about whether a
candidate was a good call — see "Fidelity" below and the README's limitations.

Usage:
    calibrate.py --manifest <inputs.tsv> --tapes <dir>     # verify, replay, report
    calibrate.py --self-test                               # unit checks only

<dir> holds the .gz files named in the manifest. Every sha256 is verified before
any replay; a mismatch aborts. Output is deterministic and is diffed against
docs/evidence/reversal-hunt-28t31b/expected-output.txt in review.

Fidelity — ported from the DEPLOYED image's source, options-edge-processing
f9ee2aca103f34a2c4117e11f733ef2644569be7:

    HuntPivotTracker.java:123  depthTicks = round(dir * (swingBefore - price) / tickSize)
    HuntPivotTracker.java:127  trendResumed = dir * (confirmationBar.close - swingBefore) > 0
                               (reported, never CALLed)
    HuntEngine.java:957        voiceEligible = !lateRecovery && !lateEmission
    HuntSettings.java:70       REVERSAL_HUNT_LATE_VOICE_FRACTION default 0.5
    EngineConfig.java:91       BUCKET_MS = 2000

with lateRecovery = denom > 0 && recovered/denom >= lateVoiceFraction, where
denom = dir * (counterSwing - anchor) and recovered = dir * (confirmationBar.close
- anchor). The replay has no restore/catch-up path, so lateEmission is false.

NOT modelled, and therefore why these are candidates and not calls: standing-call
suppression, INVALIDATED/CONFIRMED termination, re-arm cadence, and the feed's own
record validation, quarantine, de-duplication and late-event handling. Each of
those changes how many DISTINCT calls a live session reaches, in both directions.
Wall join and translation freshness are NOT a divergence: voiceEligible depends
solely on lateRecovery and lateEmission, so a missing wall or translation leaves
metadata null and cannot suppress a voice.
"""
import argparse
import datetime
import gzip
import hashlib
import json
import os
import sys
import zoneinfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from replay_hunt import Tracker, BUCKET_MS, TICK, O, HI, LO, C, N, OK, CK  # noqa: E402

ET = zoneinfo.ZoneInfo("America/New_York")
LATE_VOICE_FRACTION = 0.5      # HuntSettings.java:70
SETTINGS = (("40t/60s (before)", 40, 30), ("28t/62s (after)", 28, 31))


class TapeError(RuntimeError):
    pass


def et(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, ET).strftime("%H:%M:%S")


# --------------------------------------------------------------------------
# bucket construction — independent of the order records are fed in
# --------------------------------------------------------------------------

def add_trade(buckets, ms, seq, price, source=0, line=0):
    """Fold one trade into its 2 s bucket under a TOTAL, order-independent key.

    The key is (eventTime, sequence, source, line). `sequence` alone is not
    unique: it is the VENUE message sequence, so one aggressive order filling
    against several resting orders yields several trades sharing a sequence at
    the same millisecond — this occurs on the real tapes. `source` is the file's
    position in the manifest and `line` its position within that file, which for
    a per-partition archive is offset order. Together they totally order the
    session, so open/close never depend on the order files are fed in.

    Production is not so lucky: the service folds trades as they arrive across 4
    partitions, so ITS bucket close depends on consumption interleaving. The
    replay therefore fixes one canonical order; see the README's limitations and
    the file-order robustness check.
    """
    b = (ms // BUCKET_MS) * BUCKET_MS
    key = (ms, seq, source, line)
    o = buckets.get(b)
    if o is None:
        buckets[b] = [price, price, price, price, 1, key, key]
        return
    if key == o[OK] or key == o[CK]:
        raise TapeError(f"repeated total key {key} — the manifest lists a file twice")
    o[HI] = max(o[HI], price)
    o[LO] = min(o[LO], price)
    o[N] += 1
    if key < o[OK]:
        o[OK], o[O] = key, price
    if key > o[CK]:
        o[CK], o[C] = key, price


def iter_trades(path):
    """(eventTimeMs, sequence, price) from a gzipped tape, prefixed or bare."""
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            i = line.find("{")
            if i < 0:
                continue
            try:
                t = json.loads(line[i:])
            except Exception:
                continue
            ms = t.get("eventTimeMs")
            if ms is None:
                e = t.get("eventTime")
                if not e:
                    continue
                ms = int(datetime.datetime.fromisoformat(
                    e.replace("Z", "+00:00")).timestamp() * 1000)
            p = t.get("price")
            if p is None:
                continue
            yield ms, (t.get("sequence") or 0), p


def rth_buckets(paths, day):
    """2 s OHLC buckets over the day's RTH session (09:30 <= t < 16:00 ET).

    `paths` is the manifest order; a file's index in it becomes the `source`
    component of the total key, so feeding the SAME list is what makes the
    result reproducible, and feeding a different permutation is the robustness
    check the README describes.
    """
    buckets = {}
    for source, path in enumerate(paths):
        for line, (ms, seq, price) in enumerate(iter_trades(path)):
            lt = datetime.datetime.fromtimestamp(ms / 1000, ET)
            if lt.strftime("%Y-%m-%d") != day:
                continue
            if not ((lt.hour, lt.minute) >= (9, 30) and lt.hour < 16):
                continue
            add_trade(buckets, ms, seq, price, source, line)
    return buckets


# --------------------------------------------------------------------------
# manifest
# --------------------------------------------------------------------------

def read_manifest(path):
    rows = []
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if not line or line.startswith("#"):
            continue
        f = line.split("\t")
        if len(f) != 7:
            raise TapeError(f"manifest row needs 7 tab-separated fields: {line!r}")
        rows.append(dict(session=f[0], order=int(f[1]), dt_dir=f[2], name=f[3],
                         bytes_gz=int(f[4]), sha256_gz=f[5], records=int(f[6])))
    rows.sort(key=lambda r: (r["session"], r["order"]))
    return rows


def verify(rows, tapes_dir):
    """Hash and size every input before any replay. Aborts on the first mismatch."""
    out = {}
    for r in rows:
        p = os.path.join(tapes_dir, r["name"])
        if not os.path.exists(p):
            p = os.path.join(tapes_dir, r["dt_dir"], r["name"])
        if not os.path.exists(p):
            raise TapeError(f"missing input: {r['name']} (looked in {tapes_dir})")
        size = os.path.getsize(p)
        if size != r["bytes_gz"]:
            raise TapeError(f"{r['name']}: size {size} != manifest {r['bytes_gz']}")
        h = hashlib.sha256()
        with open(p, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != r["sha256_gz"]:
            raise TapeError(f"{r['name']}: sha256 {h.hexdigest()} != manifest "
                            f"{r['sha256_gz']}")
        out.setdefault(r["session"], []).append(p)
    return out


# --------------------------------------------------------------------------
# replay
# --------------------------------------------------------------------------

def candidates(buckets, ticks, bks):
    """Voice-eligible pivot candidates per mode, under the partial replay."""
    ordered = sorted(buckets)
    out = {}
    for dirn, mode in ((+1, "FIND_BOTTOM"), (-1, "FIND_TOP")):
        tr = Tracker(buckets, dirn, ticks, bks)
        rows = []
        for now in ordered:
            e = tr.step(now)
            if not e or e["trend_resumed"]:
                continue
            denom = dirn * (e["swing"] - e["anchor"])
            recovered = dirn * (buckets[e["confirm_bucket"]][C] - e["anchor"])
            late = denom > 0 and recovered / denom >= LATE_VOICE_FRACTION
            rows.append(dict(e, voiced=not late))
        out[mode] = rows
    return out


# --------------------------------------------------------------------------
# self-test
# --------------------------------------------------------------------------

def self_test():
    fails = []

    def check(name, got, want):
        if got != want:
            fails.append(f"{name}: got {got!r}, want {want!r}")

    # OHLC independent of the order records arrive in
    trades = [(1000, 5, 10.0, 0, 0), (1001, 9, 12.0, 0, 1),
              (1001, 3, 11.0, 1, 0), (1999, 7, 9.5, 1, 1)]
    a, b = {}, {}
    for t in trades:
        add_trade(a, *t)
    for t in reversed(trades):
        add_trade(b, *t)
    check("OHLC order-independent", a, b)
    check("open by total key", a[0][O], 10.0)
    check("close by total key", a[0][C], 9.5)
    check("high", a[0][HI], 12.0)
    check("low", a[0][LO], 9.5)
    check("count", a[0][N], 4)

    # a venue sequence shared by several trades at the same ms is NORMAL and
    # must resolve deterministically, not raise (this occurs on the real tapes)
    shared = {}
    add_trade(shared, 1000, 5, 10.0, 0, 0)
    add_trade(shared, 1000, 5, 11.0, 0, 1)
    add_trade(shared, 1000, 5, 12.0, 1, 0)
    check("shared venue sequence resolves", (shared[0][O], shared[0][C]), (10.0, 12.0))
    rev = {}
    add_trade(rev, 1000, 5, 12.0, 1, 0)
    add_trade(rev, 1000, 5, 11.0, 0, 1)
    add_trade(rev, 1000, 5, 10.0, 0, 0)
    check("shared sequence order-independent", rev, shared)

    # only a genuinely repeated total key (a file listed twice) is refused
    try:
        add_trade(shared, 1000, 5, 99.0, 0, 0)
        fails.append("repeated total key was accepted")
    except TapeError:
        pass

    # trades land in the right 2 s bucket
    edge = {}
    add_trade(edge, 1999, 1, 5.0)
    add_trade(edge, 2000, 1, 6.0)
    check("bucket boundary", sorted(edge), [0, 2000])

    # ---- POSITIVE CONTROLS: the tracker must actually confirm a pivot ----
    # Without these the whole suite passes against a Tracker stub that always
    # returns None, which proves nothing about the replay.

    def fixture(direction, recovery=0.25):
        """Peak at bucket 15 (100.00), decline 0.25/bucket to a trough at 55
        (90.00), then recovery at `recovery` per bucket to bucket 70.
        depth = 40 ticks, declineBuckets = 40. dir=-1 mirrors about 100."""
        px = {}
        for i in range(0, 15):
            px[i] = 100.0 - (15 - i) * 0.25
        px[15] = 100.0
        for i in range(16, 56):
            px[i] = 100.0 - (i - 15) * 0.25
        for i in range(56, 71):
            px[i] = 90.0 + (i - 55) * recovery
        b = {}
        for i, p in px.items():
            if direction < 0:
                p = 200.0 - p
            b[i * BUCKET_MS] = [p, p, p, p, 1, (i, 0, 0, 0), (i, 0, 0, 0)]
        return b

    def first(buckets, direction, ticks, bks):
        tr = Tracker(buckets, direction, ticks, bks)
        for now in sorted(buckets):
            e = tr.step(now)
            if e:
                return e
        return None

    for direction, name in ((+1, "bottom"), (-1, "top")):
        f = fixture(direction)
        e = first(f, direction, 40, 40)
        if e is None:
            fails.append(f"{name}: the fixture confirmed NO pivot — "
                         f"the replay is not being exercised at all")
            continue
        check(f"{name}: anchor", e["anchor"], 90.0 if direction > 0 else 110.0)
        check(f"{name}: counter-swing", e["swing"], 100.0)
        check(f"{name}: depth ticks", e["depth_ticks"], 40)
        check(f"{name}: decline buckets", e["decline_buckets"], 40)
        check(f"{name}: anchor bucket", e["anchor_bucket"], 55 * BUCKET_MS)
        check(f"{name}: confirm bucket", e["confirm_bucket"], 70 * BUCKET_MS)
        check(f"{name}: trend not resumed", e["trend_resumed"], False)
        # thresholds bite in BOTH dimensions, one tick / one bucket past the edge
        check(f"{name}: depth threshold", first(f, direction, 41, 40), None)
        check(f"{name}: duration threshold", first(f, direction, 40, 41), None)
        # voice guard: 3.75/10 recovered is voiced, 7.50/10 is late
        voiced = candidates(f, 40, 40)["FIND_BOTTOM" if direction > 0 else "FIND_TOP"]
        check(f"{name}: voiced below the late fraction", [c["voiced"] for c in voiced], [True])
        late = candidates(fixture(direction, recovery=0.5), 40, 40)[
            "FIND_BOTTOM" if direction > 0 else "FIND_TOP"]
        check(f"{name}: late recovery suppresses voice", [c["voiced"] for c in late], [False])
        # a single missing wing bucket blocks the SAME confirmation
        gapped = dict(f)
        del gapped[(55 + 3) * BUCKET_MS]
        check(f"{name}: gapped wing blocks", first(gapped, direction, 40, 40), None)

    for f in fails:
        print(f"FAIL  {f}")
    print(f"self-test: {'PASS' if not fails else str(len(fails)) + ' FAILURE(S)'}")
    return 1 if fails else 0


# --------------------------------------------------------------------------

def counts(sessions):
    """Everything the report asserts, as a comparable structure.

    Must cover EVERY number report() prints, voiced or not: the eligible totals
    are reported too, so dropping non-voiced rows here would let a reordering
    move a candidate across the late-recovery line unnoticed.
    """
    return {day: {label: sorted(
                (m, c["confirm_bucket"], c["anchor"], c["depth_ticks"],
                 c["decline_buckets"], c["voiced"])
                for m, rows in candidates(b, ticks, bks).items()
                for c in rows)
            for label, ticks, bks in SETTINGS}
            for day, b in sessions.items()}


def report(sessions):
    print("voice-eligible pivot candidates under the partial replay "
          "(NOT production call counts)")
    print()
    grand = {}
    for day in sorted(sessions):
        buckets = sessions[day]
        print(f"### {day}  RTH 09:30-16:00 ET   {len(buckets):,} valid 2s buckets")
        for label, ticks, bks in SETTINGS:
            res = candidates(buckets, ticks, bks)
            voiced = [(m, c) for m, rows in res.items() for c in rows if c["voiced"]]
            total = sum(len(v) for v in res.values())
            print(f"  {label:<18} eligible {total:>2}  voice-eligible {len(voiced):>2}   "
                  + "  ".join(f"{m.split('_')[1].lower()} {len(v)}"
                              for m, v in res.items()))
            grand.setdefault(label, 0)
            grand[label] += len(voiced)
            for mode, c in sorted(voiced, key=lambda x: x[1]["confirm_bucket"]):
                print(f"      {et(c['confirm_bucket'])} {mode:<11} "
                      f"ES {c['anchor']:>8.2f}  "
                      f"{c['depth_ticks'] * TICK:>5.2f}p / "
                      f"{c['decline_buckets'] * 2:>3}s")
        print()
    n = len(sessions)
    print("### totals")
    print(f"  {'setting':<18} {'voice-eligible':>14} {'per session':>12}")
    for label, _, _ in SETTINGS:
        print(f"  {label:<18} {grand.get(label, 0):>14} "
              f"{grand.get(label, 0) / n:>12.1f}")


def main(argv):
    ap = argparse.ArgumentParser(add_help=True, description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest")
    ap.add_argument("--tapes")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args(argv[1:])

    if a.self_test:
        return self_test()
    if not a.manifest or not a.tapes:
        ap.print_help()
        return 2
    if self_test():
        print("refusing to report numbers from a script that fails its own tests")
        return 1
    print()

    try:
        rows = read_manifest(a.manifest)
        by_session = verify(rows, a.tapes)
        sessions = {day: rth_buckets(paths, day) for day, paths in by_session.items()}
        # Robustness: the canonical file order fixes open/close inside buckets
        # whose last trades share a venue sequence across partitions. Replay the
        # session again with the file order reversed and require the reported
        # counts to be unchanged, so no conclusion here rests on that choice.
        flipped = {day: rth_buckets(list(reversed(paths)), day)
                   for day, paths in by_session.items()}
    except TapeError as e:
        print(f"ABORT: {e}")
        return 1
    for day, b in sessions.items():
        if not b:
            print(f"ABORT: no RTH buckets for {day}")
            return 1
    if counts(sessions) != counts(flipped):
        print("ABORT: reversing the manifest file order changed the counts — the "
              "result depends on an arbitrary choice and must not be reported")
        return 1
    report(sessions)
    print()
    print("file-order sensitivity: replaying with the manifest order reversed "
          "leaves every row above unchanged")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
