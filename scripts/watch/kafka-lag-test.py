#!/usr/bin/env python3
"""Negative controls for kafka-lag.py: every verdict from a synthetic --describe dump and a synthetic state."""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "kafka-lag.py")
NOW = "2026-11-07T12:00:00Z"
HDR = ("GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID")
fails = []


def dump(rows):
    """rows: (group, topic, partition, current, end, lag, consumer)"""
    out = [HDR]
    for g, t, p, cur, end, lag, con in rows:
        out.append("%s %s %s %s %s %s %s /127.0.0.1 client" % (g, t, p, cur, end, lag, con))
    return "\n".join(out) + "\n"


def run(rows, state=None, *args, now=NOW, raw=None):
    d = tempfile.mkdtemp()
    try:
        fx = os.path.join(d, "desc.txt")
        open(fx, "w").write(raw if raw is not None else dump(rows))
        sdir = os.path.join(d, "state")
        os.makedirs(sdir)
        if state is not None:
            open(os.path.join(sdir, "lag-dev.json"), "w").write(json.dumps(state))
        cmd = [sys.executable, CHECK, "--env", "dev", "--state", sdir,
               "--describe-fixture", fx, "--now", now] + list(args)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        after = os.path.join(sdir, "lag-dev.json")
        saved = json.load(open(after)) if os.path.isfile(after) else None
        return r.returncode, r.stdout + r.stderr, saved
    finally:
        shutil.rmtree(d, ignore_errors=True)


def case(name, want_rc, rows, state=None, *args, expect=None, forbid=None, now=NOW, raw=None):
    rc, out, _ = run(rows, state, *args, now=now, raw=raw)
    why = []
    if rc != want_rc:
        why.append("exit %d, wanted %d" % (rc, want_rc))
    if expect and expect not in out:
        why.append("missing %r" % expect)
    if forbid and forbid in out:
        why.append("contains %r" % forbid)
    if why:
        fails.append(name)
        print("  FAIL %-62s %s" % (name, "; ".join(why)))
        print("       " + out.strip().replace("\n", "\n       ")[:500])
    else:
        print("  ok   %-62s exit %d" % (name, rc))


def win(*pairs):
    return {"g1": {"window": [[t, l] for t, l in pairs]}}


OK_ROW = [("g1", "t", "0", "100", "100", "0", "c-1")]
BIG = [("g1", "t", "0", "0", "600000", "600000", "c-1")]

print("kafka-lag.py negative controls")

# --- the floor
case("a group under the threshold is clean", 0, OK_ROW, forbid="LAG_")

# --- R3c first sight
case("a first sample already over the threshold is reported", 1, BIG, None,
     expect="LAG_FIRST_SIGHT g1")
case("a first sample under the threshold is not", 0, OK_ROW, None, forbid="LAG_FIRST_SIGHT")

# --- R3b limb 1: the interval
case("a group whose lag GREW is reported", 1, BIG, win(("2026-11-07T11:45:00Z", 500000)),
     expect="lag grew by 100000")
case("a group draining 10% with a net drop is clean", 0,
     [("g1", "t", "0", "0", "600000", "600000", "c-1")],
     win(("2026-11-07T10:30:00Z", 900000), ("2026-11-07T11:45:00Z", 700000)),
     forbid="LAG_INSUFFICIENT_DRAIN")
case("a group draining far too slowly is reported", 1,
     [("g1", "t", "0", "0", "599000", "599000", "c-1")],
     win(("2026-11-07T10:30:00Z", 620000), ("2026-11-07T11:45:00Z", 600000)),
     expect="LAG_INSUFFICIENT_DRAIN")

# --- R3b limb 2: THE round-2 fix. An oscillating group drains on alternate runs and must still be caught.
case("a group that drained this interval but is NET FLAT is still reported", 1,
     [("g1", "t", "0", "0", "600000", "600000", "c-1")],
     win(("2026-11-07T10:30:00Z", 600000), ("2026-11-07T11:45:00Z", 700000)),
     expect="no lower than")
case("a group that drained this interval and is NET GROWING is still reported", 1,
     [("g1", "t", "0", "0", "700000", "700000", "c-1")],
     win(("2026-11-07T10:30:00Z", 500000), ("2026-11-07T11:45:00Z", 800000)),
     expect="LAG_INSUFFICIENT_DRAIN")

# --- R3b: the time-to-drain limb, and that elapsed time is MEASURED not assumed
# The drop (3000) is under --min-drain-fraction of 54000 either way, so the FRACTION limb fails in both and
# only time-to-drain can accept: 3000 in 2 min clears 51000 in ~34 min (accepted), the same 3000 in 5 min
# needs ~85 min (reported). Same lag, same drop - only the measured elapsed time differs.
case("a drop that clears inside --max-drain-minutes is accepted on the rate", 0,
     [("g1", "t", "0", "0", "51000", "51000", "c-1")],
     win(("2026-11-07T11:58:00Z", 54000)), forbid="LAG_INSUFFICIENT_DRAIN")
case("the SAME drop over a longer interval is reported - the rate is what matters", 1,
     [("g1", "t", "0", "0", "51000", "51000", "c-1")],
     win(("2026-11-07T11:55:00Z", 54000)), expect="LAG_INSUFFICIENT_DRAIN")

# --- R3d
case("an inactive group with a backlog is NO_CONSUMER", 1,
     [("g1", "t", "0", "0", "600000", "600000", "-")], None, expect="NO_CONSUMER g1")
case("an inactive group UNDER the threshold is not NO_CONSUMER", 0,
     [("g1", "t", "0", "100", "100", "0", "-")], None, forbid="NO_CONSUMER")

# --- R3e: `-` is unknown, never 0. 42% of real dev rows are this shape.
case("a group whose every lag is '-' is UNMEASURABLE, not healthy", 0,
     [("g1", "t", "0", "-", "0", "-", "c-1")], None, expect="UNMEASURABLE g1")
case("'-' partitions do not drag a real lag down to clean", 1,
     [("g1", "t", "0", "-", "0", "-", "c-1"), ("g1", "t", "1", "0", "600000", "600000", "c-1")],
     None, expect="LAG_FIRST_SIGHT")

# --- R3g exclusions
d = tempfile.mkdtemp()
exc = os.path.join(d, "exc.json")
open(exc, "w").write(json.dumps([{"group": "g1", "reason": "replay group, drained by hand"}]))
case("an excluded group is skipped and said so", 0, BIG, None, "--exclude", exc,
     expect="excluded by")
open(exc, "w").write(json.dumps([{"group": "g1"}]))
case("an exclusion with no reason is refused", 2, BIG, None, "--exclude", exc,
     expect="needs a group and a reason")

# --- fail closed
case("a dump with no header at all is an error", 2, [], None, raw="nothing here\n",
     expect="no GROUP header before the first row")
case("a header with no groups is an error", 2, [], None, raw=HDR + "\n",
     expect="refusing to read that as 'no lag'")
case("a short row is an error", 2, [], None, raw=HDR + "\ng1 t 0\n", expect="unparsable --describe row")
case("a non-numeric lag is an error", 2, [], None,
     raw=HDR + "\ng1 t 0 0 100 lots c-1 /h cl\n", expect="unparsable LAG")
case("a corrupt state file is an error, not 'first sight' for everyone", 2, BIG,
     {"g1": "not-an-object"}, expect="refusing to treat every group as first sight")
case("a state entry with no window is an error", 2, BIG, {"g1": {}},
     expect="has no window list")
case("an unparsable reading in the state is an error", 2, BIG,
     {"g1": {"window": [["whenever", "lots"]]}}, expect="unparsable reading")
case("a state reading from the FUTURE is an error", 2, BIG,
     win(("2026-11-08T00:00:00Z", 10)), expect="from the future")
case("--max-lag 0 is refused", 2, OK_ROW, None, "--max-lag", "0", expect="--max-lag must be positive")
case("--min-drain-fraction 1 is refused", 2, OK_ROW, None, "--min-drain-fraction", "1",
     expect="between 0 and 1")

# --- R3f: the window is pruned, and state is only written on success
rc, out, saved = run([("g1", "t", "0", "0", "600000", "600000", "c-1")],
                     win(("2026-11-07T06:00:00Z", 10), ("2026-11-07T11:45:00Z", 700000)))
kept = [r[0] for r in saved["g1"]["window"]]
if "2026-11-07T06:00:00Z" in kept:
    fails.append("a reading older than the window is pruned")
    print("  FAIL %-62s kept %s" % ("a reading older than the window is pruned", kept))
else:
    print("  ok   %-62s" % "a reading older than the window is pruned")

rc, out, saved = run([], None, raw=HDR + "\ng1 t 0\n")
if saved is not None:
    fails.append("a failed run writes no state")
    print("  FAIL %-62s state was written" % "a failed run writes no state")
else:
    print("  ok   %-62s" % "a failed run writes no state")

shutil.rmtree(d, ignore_errors=True)
print("\n%d case(s) FAILED" % len(fails) if fails else "\nall cases ok")
sys.exit(1 if fails else 0)
