#!/usr/bin/env python3
"""Dead-letter growth check (docs/schema-governance R16): a DLQ topic whose end offsets grew since the last run is a failure.

  dlq-growth.py --env prod --state DIR [--bootstrap H:P] [--kafka-bin DIR] [--offsets-fixture FILE --topics-fixture FILE]

DLQ topics = every topic in the cluster whose name ends in `.dlq`, plus every `kind: dlq` topic the manifest names for the environment.
Per topic the SUM of partition end offsets is compared with the previous run's (kept in --state/<env>.json):
  grew            -> FAIL (records were dead-lettered since the last run)
  same / dropped  -> ok (a drop means the topic was recreated by a clean; the new sum becomes the baseline, never an alert)
  first sight     -> baseline recorded, ok
  an unreadable topic list or offsets (Kafka down) -> exit 2, never 'no growth'.
A topic named by the manifest that the cluster does not have is reported (MISSING_DLQ) but is not a failure: a clean legitimately removes it.
State is written only after a fully successful read, so a failed run cannot erase the baseline.
Exit 0 = no growth, 1 = growth, 2 = could not check.
"""
import argparse, json, os, subprocess, sys

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True)
ap.add_argument("--state", required=True)
ap.add_argument("--bootstrap")
ap.add_argument("--kafka-bin", default=os.environ.get("KAFKA_BIN", "/home/confluent/confluent-8.2.1/bin"))
ap.add_argument("--manifest", default="schema-manifest/manifest.json")
ap.add_argument("--topics-fixture")
ap.add_argument("--offsets-fixture")
a = ap.parse_args()
if a.env not in ("dev", "prod", "es4"): print("ERROR: unknown environment %r" % a.env); sys.exit(2)


def die(msg): print("ERROR: " + msg); sys.exit(2)


bootstrap = a.bootstrap
if not bootstrap and not a.topics_fixture:
    try: bootstrap = json.load(open("schema-manifest/kafka.json"))[a.env]
    except Exception as e: die("no bootstrap server for %s: %s" % (a.env, e))


def tool_path(name):
    for cand in (name, name + ".sh"):
        p = os.path.join(a.kafka_bin, cand)
        if os.path.exists(p): return p
    die("no %s in %s (set --kafka-bin or KAFKA_BIN)" % (name, a.kafka_bin))


def kafka(tool, *args):
    r = subprocess.run([tool_path(tool), "--bootstrap-server", bootstrap, *args], capture_output=True, text=True, timeout=120)
    if r.returncode != 0: die("%s failed (rc=%d): %s" % (tool, r.returncode, r.stderr.strip()[-300:]))
    return r.stdout


try:
    all_topics = open(a.topics_fixture).read().split() if a.topics_fixture else kafka("kafka-topics", "--list").split()
except SystemExit: raise
except Exception as e: die("cannot list topics: %s" % e)
if not all_topics: die("the topic list is empty - refusing to read it as 'no dead letters'")
# The manifest DECLARES the DLQ topics of each environment (top-level dlqTopics, plus any MIRRORED entry topic with kind dlq). That declared set is
# authoritative: a cluster topic that ends in .dlq but is not declared is itself a failure (UNDECLARED_DLQ) - a new dead-letter topic must be named in the
# manifest so it is watched on purpose, not by accident.
named = set()
try:
    man = json.load(open(a.manifest))
    named |= set(man.get("dlqTopics", {}).get(a.env, []))
    for e in man["entries"]:
        if e.get("kind") == "MIRRORED" and a.env in e.get("environments", []):
            named |= {t["name"] for t in e.get("topics", []) if t.get("kind") == "dlq" and not t["name"].startswith("record-name:")}
except Exception as e: die("cannot read the manifest: %s" % e)
suffix = {t for t in all_topics if t.endswith(".dlq")}
undeclared = sorted(suffix - named)
dlqs = sorted(suffix | (named & set(all_topics)))
missing = sorted(named - set(all_topics))


def end_sum(topic):
    if a.offsets_fixture:
        return json.load(open(a.offsets_fixture))[topic]
    out = kafka("kafka-get-offsets", "--topic", topic, "--time", "-1")
    parts = [l.strip().split(":") for l in out.splitlines() if l.strip()]
    if not parts or any(len(p) != 3 or not p[2].isdigit() for p in parts): die("unparsable offsets for %s: %r" % (topic, out[:200]))
    return sum(int(p[2]) for p in parts)


now = {t: end_sum(t) for t in dlqs}
os.makedirs(a.state, exist_ok=True)
sp = os.path.join(a.state, a.env + ".json")
try:
    before = json.load(open(sp)) if os.path.exists(sp) else None
    if before is not None and not isinstance(before, dict): raise ValueError("not an object")
except Exception as e: die("the baseline file %s is unreadable (%s) - refusing to treat every DLQ as first sight; fix or remove it deliberately" % (sp, e))
grew = []
for t, n in sorted(now.items()):
    if before is not None and t in before and n > before[t]: grew.append((t, before[t], n))
for t, b, n in grew: print("GROWTH %s: %d -> %d (+%d dead letters since the last run)" % (t, b, n, n - b))
for t in undeclared: print("UNDECLARED_DLQ %s (a .dlq topic the manifest does not declare: add it to dlqTopics.%s)" % (t, a.env))
for t in missing: print("MISSING_DLQ %s (named by the manifest, not in the cluster)" % t)
# MERGE, never overwrite: a topic missing from this (successful) listing keeps its baseline, so if it was only transiently invisible and comes back
# with more records, that growth is still detected instead of being read as "first sight".
merged = dict(before or {}); merged.update(now)
with open(sp + ".tmp", "w") as fh: json.dump(merged, fh)
os.replace(sp + ".tmp", sp)   # atomic; only reached after every read succeeded
print("dlq-growth %s: %d DLQ topic(s)%s, %d grew, %d undeclared" % (a.env, len(dlqs), "" if before is not None else " (first run: baseline recorded)", len(grew), len(undeclared)))
sys.exit(1 if (grew or undeclared) else 0)
