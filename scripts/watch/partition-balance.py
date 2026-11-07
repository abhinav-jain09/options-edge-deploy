#!/usr/bin/env python3
"""Partition-balance check (FLEET-ALERT-DISCORD-REQUIREMENT R4): a service writing everything into one
partition while the rest sit idle, and a one-partition topic carrying real volume.

  partition-balance.py --env dev --state DIR [--bootstrap H:P] [--kafka-bin DIR] [--offsets-fixture FILE]

Owner's direction: "a service is only writing in one single partition and rest of the partition are free …
there should be alert … so then we can fix the performance in that sense."

TWO FINDINGS, AND THEY ARE DIFFERENT PROBLEMS:
  SKEW                  more than one partition, and the busiest took over --max-share of the interval.
  SINGLE_PARTITION_HOT  exactly one partition, carrying over --single-partition-records. The skew rule can
                        never see this - one partition is trivially 100% of one partition - but it is the
                        same throughput ceiling, so it is reported in its own class (R4h).

GROWTH, NOT CUMULATIVE OFFSETS (R4c). Cumulative offsets describe a topic's whole history, including what
it did before a key or a partition count changed. The direction is about what a service is doing NOW, so
every verdict comes from the delta against the previous run's snapshot.

ONE BOUNDED SNAPSHOT (R4b). `kafka-get-offsets --topic '.*'` returns every partition in one call: measured
1.54 s for 6,579 partitions across 442 topics on dev. A per-topic loop would not fit the job's 10-minute
timeout, and repeated timeouts would stop the baseline ever advancing.

THE SHARE THRESHOLD IS DELIBERATELY LOOSE (R4e). 0.60 on a 32-partition topic is 19.2x an even share, so
this catches grossly broken keying, not the "almost equal" distribution the direction describes as the
goal. Its first finding should always be a real defect.

Exit 0 = balanced, 1 = at least one finding, 2 = the check could not run.
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True)
ap.add_argument("--state", required=True)
ap.add_argument("--bootstrap")
ap.add_argument("--kafka-bin", default=os.environ.get("KAFKA_BIN", "/home/confluent/confluent-8.2.1/bin"))
ap.add_argument("--manifest", default="schema-manifest/manifest.json")
ap.add_argument("--exempt", default=None, help="JSON list of {topic,key,reason}: topics correctly on one partition (R4j)")
ap.add_argument("--offsets-fixture")
ap.add_argument("--min-records", type=int, default=1000)
ap.add_argument("--max-share", type=float, default=0.60)
ap.add_argument("--single-partition-records", type=int, default=10000)
ap.add_argument("--snapshot-timeout", type=int, default=60, help="seconds for the one CLI call (R4b)")
ap.add_argument("--budget", type=int, default=120, help="seconds for the whole check (R4b)")
ap.add_argument("--now", default=None)
a = ap.parse_args()
STARTED = time.monotonic()


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def budget_check(what):
    spent = time.monotonic() - STARTED
    if spent > a.budget:
        die("the check exceeded its %ds budget after %s (%.1fs spent)" % (a.budget, what, spent))


if a.min_records < 1:
    die("--min-records must be positive (got %d): 0 would judge an idle topic" % a.min_records)
if not 0 < a.max_share <= 1:
    die("--max-share must be in (0, 1] (got %r)" % a.max_share)

bootstrap = a.bootstrap
if not bootstrap and not a.offsets_fixture:
    try:
        bootstrap = json.load(open("schema-manifest/kafka.json"))[a.env]
    except Exception as e:  # noqa: BLE001
        die("no bootstrap server for %s: %s" % (a.env, e))

exempt = {}
if a.exempt:
    try:
        for e in json.load(open(a.exempt)):
            if not e.get("topic") or not e.get("reason") or not e.get("key"):
                raise ValueError("every entry needs a topic, a key and a reason: %r" % (e,))
            exempt[e["topic"]] = e["reason"]
    except Exception as e:  # noqa: BLE001
        die("cannot read the exemption list %s: %s" % (a.exempt, e))


def snapshot():
    """{topic: {partition: end_offset}} from ONE call."""
    if a.offsets_fixture:
        raw = open(a.offsets_fixture).read()
    else:
        tool = os.path.join(a.kafka_bin, "kafka-get-offsets")
        try:
            r = subprocess.run([tool, "--bootstrap-server", bootstrap, "--topic", ".*", "--time", "-1"],
                               capture_output=True, text=True, timeout=a.snapshot_timeout)
        except subprocess.TimeoutExpired:
            die("kafka-get-offsets did not finish within %ds" % a.snapshot_timeout)
        except OSError as e:
            die("cannot run %s: %s" % (tool, e))
        if r.returncode != 0:
            die("kafka-get-offsets failed (rc=%d): %s" % (r.returncode, r.stderr.strip()[-300:]))
        raw = r.stdout
    out = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        # topic:partition:offset — a topic name may contain ':' in principle, so split from the RIGHT.
        f = line.rsplit(":", 2)
        if len(f) != 3 or not f[1].isdigit() or not f[2].lstrip("-").isdigit():
            die("unparsable offsets line %r" % line[:200])
        topic, part, end = f[0], int(f[1]), int(f[2])
        if topic.startswith("__"):          # R4g: broker internals are nobody's service to fix
            continue
        out.setdefault(topic, {})[part] = end
    if not out:
        die("no topics in the offsets snapshot - refusing to report 'balanced' on an empty answer")
    return out


# R5: topic -> producing service, from the manifest. Built once.
def producers():
    try:
        man = json.load(open(a.manifest))
    except Exception as e:  # noqa: BLE001
        die("cannot read the manifest %s: %s" % (a.manifest, e))
    by_topic = {}
    for e in man.get("entries", []):
        if a.env not in e.get("environments", []):
            continue
        svcs = {p.get("service") for p in (e.get("producers") or []) if a.env in (p.get("environments") or [])}
        svcs.discard(None)
        if not svcs:
            continue
        for t in e.get("topics", []) or []:
            name = t.get("name")
            # `record-name:` entries name a record, not a topic (R5c).
            if not name or name.startswith("record-name:"):
                continue
            by_topic.setdefault(name, set()).update(svcs)
    return by_topic


# A Kafka Streams internal topic is named "<application.id>-<store>-changelog" (or "-repartition"), and the
# application id is "<service><KAFKA_APPLICATION_ID_SUFFIX>". The manifest never declares these, yet they
# are 113 of dev's 440 topics and the likeliest to skew, because a changelog is co-partitioned with its
# store's key. So the service is DERIVED from the name for these two suffixes only, and the answer says it
# was derived - it is a naming convention, not a declaration.
# The env suffixes include production's "-prod-b": one service's rotation away from a wedged consumer group
# (bcfbf519). The longest suffix is tried first, or "-prod-b-" would be cut at "-prod-".
ENV_SUFFIXES = {"dev": ["-dev-"], "prod": ["-prod-b-", "-prod-"], "production": ["-prod-b-", "-prod-"],
                "es4": ["-es4-"]}


def derive_from_streams_name(topic, env):
    for suffix in ("-changelog", "-repartition"):
        if not topic.endswith(suffix):
            continue
        stem = topic[: -len(suffix)]
        for env_sep in ENV_SUFFIXES.get(env, []):
            head = stem.split(env_sep, 1)[0]
            if head != stem and head:
                return head
        return None
    return None


def attribute(topic, by_topic):
    svcs = sorted(by_topic.get(topic, ()))
    if len(svcs) == 1:
        return svcs[0]
    if len(svcs) > 1:
        return "%s (multiple producers declared)" % ", ".join(svcs)
    derived = derive_from_streams_name(topic, a.env)
    if derived:
        return "%s (derived from the Streams application id)" % derived
    return "UNATTRIBUTED"


now = dt.datetime.fromisoformat(a.now.replace("Z", "+00:00")) if a.now else dt.datetime.now(dt.timezone.utc)
cur = snapshot()
budget_check("the offsets snapshot")
by_topic = producers()
budget_check("the manifest read")

sp = os.path.join(a.state, "partition-" + a.env + ".json")
try:
    if os.path.lexists(sp) and not os.path.isfile(sp):
        raise ValueError("not a readable regular file (a dangling symlink counts as existing)")
    before = json.load(open(sp)) if os.path.lexists(sp) else {}
    if not isinstance(before, dict):
        raise ValueError("not an object")
except Exception as e:  # noqa: BLE001
    die("the baseline file %s is unreadable (%s) - refusing to treat every topic as first sight; fix or "
        "remove it deliberately" % (sp, e))

findings, judged, first_sight, unattributed_cluster = [], 0, 0, 0
state = {}
for topic in sorted(cur):
    parts = cur[topic]
    state[topic] = {str(p): v for p, v in parts.items()}
    if topic not in by_topic:
        unattributed_cluster += 1
    prev = before.get(topic)
    if prev is None:
        first_sight += 1
        continue
    growth = {}
    for p, end in parts.items():
        was = prev.get(str(p))
        if was is None:
            continue                      # a partition added since the last run has no interval yet
        if not isinstance(was, int) or isinstance(was, bool):
            die("the baseline for %s partition %s is not an integer (%r)" % (topic, p, was))
        if end < was:
            # A clean recreated the topic, or retention moved the end backwards. The new value is the
            # baseline; it is never a finding (R4c/R3f family).
            growth = {}
            break
        growth[p] = end - was
    if not growth:
        continue
    total = sum(growth.values())
    if total < a.min_records:
        continue
    judged += 1
    if topic in exempt:
        continue
    who = attribute(topic, by_topic)
    if len(parts) == 1:
        if total >= a.single_partition_records:
            findings.append(("SINGLE_PARTITION_HOT", topic, who,
                             "%d records in the interval on its ONLY partition" % total))
        continue
    busiest = max(growth.values())
    share = busiest / total
    if share > a.max_share:
        used = sum(1 for v in growth.values() if v > 0)
        findings.append(("SKEW", topic, who,
                         "busiest partition took %d of %d records (%.0f%%; %d of %d partitions used)"
                         % (busiest, total, share * 100, used, len(parts))))

budget_check("the comparison")
for cls, topic, who, detail in findings:
    print("%s %s [%s]: %s" % (cls, topic, who, detail))
# R5c: two different counts. The offenders' unattributed share is what an alert carries; the cluster-wide
# count is the size of the manifest gap and is logged every run, never paged.
offender_unattributed = sum(1 for _, _, who, _ in findings if who == "UNATTRIBUTED")
print("unattributed: %d of %d finding(s); %d of %d topic(s) in the cluster have no declared producer for %s"
      % (offender_unattributed, len(findings), unattributed_cluster, len(cur), a.env))
if exempt:
    print("exempt by %s: %d topic(s)" % (a.exempt, len(exempt)))

# As in kafka-lag.py: a lost baseline is "could not check", not "balanced" and not a finding.
try:
    os.makedirs(a.state, exist_ok=True)
    with open(sp + ".tmp", "w") as fh:
        json.dump(state, fh)
    os.replace(sp + ".tmp", sp)      # atomic, only after every read succeeded
except OSError as e:
    die("cannot write the baseline %s (%s) - refusing to report a balance verdict whose baseline was lost" % (sp, e))

print("partition-balance %s: %d topic(s), %d judged, %d first sight, %d finding(s)"
      % (a.env, len(cur), judged, first_sight, len(findings)))
sys.exit(1 if findings else 0)
