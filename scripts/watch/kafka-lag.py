#!/usr/bin/env python3
"""Consumer-lag check (FLEET-ALERT-DISCORD-REQUIREMENT R3): a group carrying a backlog it is not draining
fast enough to be safe.

  kafka-lag.py --env dev --state DIR [--bootstrap H:P] [--kafka-bin DIR] [--describe-fixture FILE]

LAG ALONE IS NOT A FAULT. A consumer catching up has lag and is healthy, so a group above --max-lag is
reported only when it is not DRAINING MATERIALLY (R3b):
  * the latest interval must have drained - the lag fell by >= --min-drain-fraction of the earlier reading,
    or the implied time-to-drain at the observed rate is under --max-drain-minutes; AND
  * the lag must be lower than it was at the oldest reading still inside --trend-window-minutes.
Both, because a group that oscillates - or grows net while dropping on alternate runs - would satisfy a
single-interval test every other run and never alert.

THE TWO DRAIN LIMBS OVERLAP AT THIS CADENCE, and the tests say which one is doing the work. Satisfying
time-to-drain while FAILING the fraction needs `dropped < lag/9` together with `lag < 60*dropped/elapsed`,
which solves to `elapsed < ~6.7 min`: so on the 15-minute cron the fraction limb decides almost every
case, and the time-to-drain limb only bites when a run follows soon after another (a hand re-run). Both
are kept because the interval is NOT fixed - see the next paragraph - but neither is presented as
independent of the other.

THE ELAPSED TIME IS MEASURED, NOT ASSUMED. The job's cron is every 15 minutes, but a queued build, a
disableConcurrentBuilds skip or a hand re-run makes the real interval anything at all, so each reading
carries the instant it was taken and the rate is computed from that.

`-` IS NOT ZERO. The CLI prints `-` for the offset and lag of a partition with no committed offset: on dev
1,438 of 3,390 rows (42%) are that shape. Read as 0 they would turn an unmeasurable group into a healthy
one, so they are counted as UNKNOWN and a group with no measurable partition is reported as such, never as
"no lag".

Exit 0 = nothing to report, 1 = at least one finding, 2 = the check could not run.
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True)
ap.add_argument("--state", required=True)
ap.add_argument("--bootstrap")
ap.add_argument("--kafka-bin", default=os.environ.get("KAFKA_BIN", "/home/confluent/confluent-8.2.1/bin"))
ap.add_argument("--manifest", default="schema-manifest/manifest.json")
ap.add_argument("--exclude", default=None, help="JSON list of {group,reason}: groups that may carry a backlog by design (R3g)")
ap.add_argument("--describe-fixture", help="tests only: a --describe --all-groups dump")
ap.add_argument("--max-lag", type=int, default=50000)
ap.add_argument("--min-drain-fraction", type=float, default=0.10)
ap.add_argument("--max-drain-minutes", type=int, default=60)
ap.add_argument("--trend-window-minutes", type=int, default=120)
ap.add_argument("--describe-timeout", type=int, default=120, help="seconds for the one CLI call")
ap.add_argument("--now", default=None, help="ISO instant, for the tests")
a = ap.parse_args()


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def ts(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


now = ts(a.now) if a.now else dt.datetime.now(dt.timezone.utc)
if a.max_lag < 1:
    die("--max-lag must be positive (got %d)" % a.max_lag)
if not 0 < a.min_drain_fraction < 1:
    die("--min-drain-fraction must be between 0 and 1 exclusive (got %r)" % a.min_drain_fraction)
if a.trend_window_minutes < 1:
    die("--trend-window-minutes must be at least 1 (got %d)" % a.trend_window_minutes)

bootstrap = a.bootstrap
if not bootstrap and not a.describe_fixture:
    try:
        bootstrap = json.load(open("schema-manifest/kafka.json"))[a.env]
    except Exception as e:  # noqa: BLE001
        die("no bootstrap server for %s: %s" % (a.env, e))

excluded = {}
if a.exclude:
    try:
        for e in json.load(open(a.exclude)):
            if not e.get("group") or not e.get("reason"):
                raise ValueError("every entry needs a group and a reason: %r" % (e,))
            excluded[e["group"]] = e["reason"]
    except Exception as e:  # noqa: BLE001
        die("cannot read the exclusion list %s: %s" % (a.exclude, e))


def describe():
    """ONE call for every group. 64 groups / 3,390 rows took 1.38 s on dev; the timeout is far above that so
    a slow broker reads as 'could not check' rather than hanging the job."""
    if a.describe_fixture:
        return open(a.describe_fixture).read()
    tool = os.path.join(a.kafka_bin, "kafka-consumer-groups")
    try:
        r = subprocess.run([tool, "--bootstrap-server", bootstrap, "--describe", "--all-groups"],
                           capture_output=True, text=True, timeout=a.describe_timeout)
    except subprocess.TimeoutExpired:
        die("kafka-consumer-groups --describe did not finish within %ds" % a.describe_timeout)
    except OSError as e:
        die("cannot run %s: %s" % (tool, e))
    if r.returncode != 0:
        die("kafka-consumer-groups failed (rc=%d): %s" % (r.returncode, r.stderr.strip()[-300:]))
    return r.stdout


raw = describe()
# GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
groups = {}
saw_header = False
for line in raw.splitlines():
    line = line.rstrip()
    if not line.strip():
        continue
    f = line.split()
    if f[0] == "GROUP":
        saw_header = True
        continue
    if not saw_header:
        # A dump that never had a header is not a dump of this command. Said before the column check, or a
        # wholly unrelated file is reported as a malformed row.
        die("no GROUP header before the first row - this is not a --describe dump: %r" % line[:120])
    if len(f) < 6:
        die("unparsable --describe row (expected at least 6 columns): %r" % line[:200])
    group, topic, lag_s, consumer = f[0], f[1], f[5], (f[6] if len(f) > 6 else "-")
    g = groups.setdefault(group, {"lag": 0, "unknown": 0, "measured": 0, "members": 0, "topics": set()})
    g["topics"].add(topic)
    if consumer != "-":
        g["members"] += 1
    if lag_s == "-":
        g["unknown"] += 1
        continue
    try:
        g["lag"] += int(lag_s)
    except ValueError:
        die("unparsable LAG %r for group %s topic %s" % (lag_s, group, topic))
    g["measured"] += 1

if not saw_header:
    die("no GROUP header in the --describe output - refusing to read it as 'no groups'")
if not groups:
    die("no consumer groups in the --describe output - refusing to read that as 'no lag'")

sp = os.path.join(a.state, "lag-" + a.env + ".json")
try:
    if os.path.lexists(sp) and not os.path.isfile(sp):
        raise ValueError("not a readable regular file (a dangling symlink counts as existing)")
    before = json.load(open(sp)) if os.path.lexists(sp) else {}
    if not isinstance(before, dict):
        raise ValueError("not an object")
    for k, v in before.items():
        if not isinstance(v, dict) or not isinstance(v.get("window"), list):
            raise ValueError("group %s has no window list" % k)
except Exception as e:  # noqa: BLE001
    # A corrupt clock must not read as "first sight" for every group: that would hide a backlog that has
    # already been building. Fail closed, as dlq-growth.py does for the same reason.
    die("the state file %s is unreadable (%s) - refusing to treat every group as first sight; fix or "
        "remove it deliberately" % (sp, e))

cutoff = now - dt.timedelta(minutes=a.trend_window_minutes)
findings, state, unmeasurable, skipped = [], {}, [], 0

for name in sorted(groups):
    g = groups[name]
    window = []
    for item in before.get(name, {}).get("window", []):
        try:
            when, lag = ts(item[0]), int(item[1])
        except Exception:  # noqa: BLE001
            die("the state file %s has an unparsable reading %r for group %s" % (sp, item, name))
        if when > cutoff:
            window.append([item[0], lag])
    total = g["lag"] if g["measured"] else None

    if total is None:
        # Every partition's lag was `-`. Unknown is not zero, and it is not a backlog either: there is
        # nothing to measure, so it is recorded and never judged.
        unmeasurable.append(name)
        state[name] = {"window": window}
        continue

    window.append([now.isoformat().replace("+00:00", "Z"), total])
    state[name] = {"window": window}

    if name in excluded:
        skipped += 1
        continue
    if total <= a.max_lag:
        continue

    if len(window) == 1:
        # R3c: no previous reading, so no rate. A first sample already over the threshold is a real
        # backlog; calling it "baseline only" would hide one that was hours old when the check went in.
        findings.append(("LAG_FIRST_SIGHT", name,
                         "lag %d over %d on first sight (%d partition(s) measured, %d unknown)"
                         % (total, a.max_lag, g["measured"], g["unknown"])))
        continue

    prev_when, prev_lag = ts(window[-2][0]), window[-2][1]
    elapsed_min = (now - prev_when).total_seconds() / 60.0
    if elapsed_min <= 0:
        die("group %s has a previous reading at or after now (%s >= %s) - the state file is from the "
            "future; fix or remove it deliberately" % (name, window[-2][0], now.isoformat()))
    dropped = prev_lag - total
    interval_drained = False
    detail = ""
    if dropped > 0:
        if dropped >= a.min_drain_fraction * prev_lag:
            interval_drained = True
        else:
            rate = dropped / elapsed_min              # records per minute
            ttd = total / rate
            if ttd < a.max_drain_minutes:
                interval_drained = True
            else:
                detail = "draining %d/min, %d min to clear" % (round(rate), round(ttd))
    else:
        detail = "lag grew by %d in %d min" % (-dropped, round(elapsed_min))
    net_drop = window[0][1] > total
    if interval_drained and net_drop:
        continue
    if interval_drained and not net_drop:
        detail = "last interval drained but the lag is no lower than %s ago (%d then, %d now)" % (
            ("%d min" % round((now - ts(window[0][0])).total_seconds() / 60.0)), window[0][1], total)
    findings.append(("LAG_INSUFFICIENT_DRAIN", name,
                     "lag %d over %d; %s" % (total, a.max_lag, detail or "not draining")))

# R3d: an inactive group carrying a backlog. It proves nobody is READING - it says nothing about whether
# anyone is currently writing.
for name in sorted(groups):
    g = groups[name]
    if name in excluded or not g["measured"]:
        continue
    if g["members"] == 0 and g["lag"] > a.max_lag:
        findings.append(("NO_CONSUMER", name,
                         "lag %d over %d with NO active members" % (g["lag"], a.max_lag)))

for cls, name, detail in findings:
    print("%s %s: %s" % (cls, name, detail))
for name in unmeasurable:
    print("UNMEASURABLE %s: every partition's lag is '-' (no committed offsets) - not judged" % name)
if excluded:
    print("excluded by %s: %d group(s) - %s" % (a.exclude, skipped, ", ".join(sorted(excluded))))

# A baseline that cannot be written is "could not check", not a finding: left as exit 1 it would read as a
# lag verdict, and every later run would be a first sight. Exit 2 says what actually happened.
try:
    os.makedirs(a.state, exist_ok=True)
    with open(sp + ".tmp", "w") as fh:
        json.dump(state, fh)
    os.replace(sp + ".tmp", sp)   # atomic, and only reached after every read succeeded (R3f)
except OSError as e:
    die("cannot write the baseline %s (%s) - refusing to report a lag verdict whose baseline was lost" % (sp, e))

print("kafka-lag %s: %d group(s), %d finding(s), %d unmeasurable" % (a.env, len(groups), len(findings), len(unmeasurable)))
sys.exit(1 if findings else 0)
