#!/usr/bin/env python3
"""Negative controls for partition-balance.py: every verdict from a synthetic offsets snapshot pair."""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "partition-balance.py")
fails = []

MANIFEST = {"entries": [
    {"kind": "MIRRORED", "subject": "s1", "environments": ["dev"],
     "topics": [{"name": "declared.topic", "kind": "data"},
                {"name": "record-name:com.example.Thing", "kind": "data"}],
     "producers": [{"service": "owner-service", "environments": ["dev"]}]},
    {"kind": "MIRRORED", "subject": "s2", "environments": ["dev"],
     "topics": [{"name": "shared.topic", "kind": "data"}],
     "producers": [{"service": "svc-a", "environments": ["dev"]}]},
    {"kind": "MIRRORED", "subject": "s3", "environments": ["dev"],
     "topics": [{"name": "shared.topic", "kind": "data"}],
     "producers": [{"service": "svc-b", "environments": ["dev"]}]},
    {"kind": "MIRRORED", "subject": "s4", "environments": ["prod"],
     "topics": [{"name": "otherenv.topic", "kind": "data"}],
     "producers": [{"service": "prod-only-service", "environments": ["prod"]}]},
]}


def dump(snap):
    return "".join("%s:%d:%d\n" % (t, p, o) for t, parts in snap.items() for p, o in sorted(parts.items()))


def run(before, after, *args, env="dev", exempt=None):
    d = tempfile.mkdtemp()
    try:
        sdir = os.path.join(d, "state")
        os.makedirs(sdir)
        man = os.path.join(d, "manifest.json")
        json.dump(MANIFEST, open(man, "w"))
        fx = os.path.join(d, "off.txt")
        cmd = [sys.executable, CHECK, "--env", env, "--state", sdir, "--offsets-fixture", fx,
               "--manifest", man]
        if exempt is not None:
            ex = os.path.join(d, "ex.json")
            json.dump(exempt, open(ex, "w"))
            cmd += ["--exempt", ex]
        cmd += list(args)
        if before is not None:
            open(fx, "w").write(dump(before))
            subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        open(fx, "w").write(dump(after))
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return r.returncode, r.stdout + r.stderr
    finally:
        shutil.rmtree(d, ignore_errors=True)


def case(name, want_rc, before, after, *args, expect=None, forbid=None, env="dev", exempt=None):
    rc, out = run(before, after, *args, env=env, exempt=exempt)
    why = []
    if rc != want_rc:
        why.append("exit %d, wanted %d" % (rc, want_rc))
    if expect and expect not in out:
        why.append("missing %r" % expect)
    if forbid and forbid in out:
        why.append("contains %r" % forbid)
    if why:
        fails.append(name)
        print("  FAIL %-64s %s" % (name, "; ".join(why)))
        print("       " + out.strip().replace("\n", "\n       ")[:500])
    else:
        print("  ok   %-64s exit %d" % (name, rc))


def even(topic, parts, each):
    return {topic: {p: each for p in range(parts)}}


print("partition-balance.py negative controls")

# --- the floor: an even spread over 4 partitions
case("an evenly spread topic is balanced", 0,
     even("declared.topic", 4, 0), even("declared.topic", 4, 500), forbid="SKEW")

# --- R4e: the owner's case. Everything into one partition of four.
case("ALL records into one partition of four is SKEW", 1,
     {"declared.topic": {0: 0, 1: 0, 2: 0, 3: 0}},
     {"declared.topic": {0: 2000, 1: 0, 2: 0, 3: 0}},
     expect="SKEW declared.topic [owner-service]")
case("the SKEW line states the share and how many partitions were used", 1,
     {"declared.topic": {0: 0, 1: 0, 2: 0, 3: 0}},
     {"declared.topic": {0: 2000, 1: 0, 2: 0, 3: 0}},
     expect="2000 of 2000 records (100%; 1 of 4 partitions used)")
case("a share just under the threshold is not reported", 0,
     {"declared.topic": {0: 0, 1: 0, 2: 0, 3: 0}},
     {"declared.topic": {0: 1100, 1: 300, 2: 300, 3: 300}},   # 1100/2000 = 55%
     forbid="SKEW")
case("a share just over the threshold is reported", 1,
     {"declared.topic": {0: 0, 1: 0, 2: 0, 3: 0}},
     {"declared.topic": {0: 1300, 1: 240, 2: 230, 3: 230}},   # 1300/2000 = 65%
     expect="SKEW")

# --- R4d: the floor under judgement
case("a topic under --min-records is not judged", 0,
     even("declared.topic", 4, 0), {"declared.topic": {0: 999, 1: 0, 2: 0, 3: 0}},
     forbid="SKEW")
case("first sight is never a finding - there is no interval yet", 0,
     None, {"declared.topic": {0: 99999, 1: 0, 2: 0, 3: 0}},
     expect="first sight")

# --- R4h: one partition is a different problem, and the skew rule cannot see it
case("a ONE-partition topic under the records floor is silent", 0,
     {"declared.topic": {0: 0}}, {"declared.topic": {0: 5000}},
     forbid="SINGLE_PARTITION_HOT")
case("a ONE-partition topic over the floor is SINGLE_PARTITION_HOT, not SKEW", 1,
     {"declared.topic": {0: 0}}, {"declared.topic": {0: 20000}},
     expect="SINGLE_PARTITION_HOT declared.topic", forbid="SKEW declared.topic")

# --- R4g: broker internals are nobody's to fix
case("__consumer_offsets is ignored even when wildly skewed", 0,
     {"__consumer_offsets": {0: 0, 1: 0}, "declared.topic": {0: 0, 1: 0, 2: 0, 3: 0}},
     {"__consumer_offsets": {0: 99999, 1: 0}, "declared.topic": {0: 500, 1: 500, 2: 500, 3: 500}},
     forbid="__consumer_offsets")

# --- R4c: a recreated topic is a new baseline, never a finding
case("a topic whose offsets went BACKWARDS is re-baselined, not reported", 0,
     {"declared.topic": {0: 50000, 1: 50000, 2: 50000, 3: 50000}},
     {"declared.topic": {0: 10, 1: 0, 2: 0, 3: 0}},
     forbid="SKEW")
case("a partition added since the last run has no interval and is skipped", 0,
     {"declared.topic": {0: 0, 1: 0}},
     {"declared.topic": {0: 400, 1: 400, 2: 99999}},
     forbid="SKEW")

# --- R5: attribution
case("several declared producers are all named", 1,
     {"shared.topic": {0: 0, 1: 0, 2: 0, 3: 0}}, {"shared.topic": {0: 2000, 1: 0, 2: 0, 3: 0}},
     expect="[svc-a, svc-b (multiple producers declared)]")
case("a topic the manifest does not name is UNATTRIBUTED", 1,
     {"mystery.topic": {0: 0, 1: 0}}, {"mystery.topic": {0: 2000, 1: 0}},
     expect="[UNATTRIBUTED]")
case("a producer declared for ANOTHER environment does not attribute this one", 1,
     {"otherenv.topic": {0: 0, 1: 0}}, {"otherenv.topic": {0: 2000, 1: 0}},
     expect="[UNATTRIBUTED]")
case("a Streams changelog is attributed from its application id", 1,
     {"indicator-service-dev-indicator-state-changelog": {0: 0, 1: 0, 2: 0, 3: 0}},
     {"indicator-service-dev-indicator-state-changelog": {0: 2000, 1: 0, 2: 0, 3: 0}},
     expect="[indicator-service (derived from the Streams application id)]")
case("the derived name is marked derived, never passed off as declared", 1,
     {"approach-monitor-dev-x-repartition": {0: 0, 1: 0}},
     {"approach-monitor-dev-x-repartition": {0: 2000, 1: 0}},
     expect="derived from the Streams application id")
case("both counts are reported, and they are different numbers", 1,
     {"mystery.topic": {0: 0, 1: 0}, "declared.topic": {0: 0, 1: 0, 2: 0, 3: 0}},
     {"mystery.topic": {0: 2000, 1: 0}, "declared.topic": {0: 500, 1: 500, 2: 500, 3: 500}},
     expect="unattributed: 1 of 1 finding(s); 1 of 2 topic(s)")

# --- R4j exemptions
case("an exempt topic is not reported", 0,
     {"declared.topic": {0: 0, 1: 0, 2: 0, 3: 0}}, {"declared.topic": {0: 2000, 1: 0, 2: 0, 3: 0}},
     exempt=[{"topic": "declared.topic", "key": "SPX", "reason": "one instrument by design"}],
     expect="exempt by")
case("an exemption without a key or reason is refused", 2,
     {"declared.topic": {0: 0, 1: 0}}, {"declared.topic": {0: 2000, 1: 0}},
     exempt=[{"topic": "declared.topic"}], expect="needs a topic, a key and a reason")

# --- fail closed
case("an empty snapshot is an error, not 'balanced'", 2, None, {}, expect="refusing to report 'balanced'")
case("--min-records 0 is refused", 2, None, even("declared.topic", 2, 1), "--min-records", "0",
     expect="--min-records must be positive")
case("--max-share 0 is refused", 2, None, even("declared.topic", 2, 1), "--max-share", "0",
     expect="--max-share must be in")

d = tempfile.mkdtemp()
try:
    sdir = os.path.join(d, "state")
    os.makedirs(sdir)
    man = os.path.join(d, "m.json")
    json.dump(MANIFEST, open(man, "w"))
    fx = os.path.join(d, "o.txt")
    open(fx, "w").write("this is not an offsets dump\n")
    r = subprocess.run([sys.executable, CHECK, "--env", "dev", "--state", sdir,
                        "--offsets-fixture", fx, "--manifest", man], capture_output=True, text=True)
    if r.returncode != 2 or "unparsable offsets line" not in r.stdout + r.stderr:
        fails.append("an unparsable offsets dump is an error")
        print("  FAIL %-64s exit %d" % ("an unparsable offsets dump is an error", r.returncode))
    else:
        print("  ok   %-64s exit 2" % "an unparsable offsets dump is an error")

    # a corrupt baseline must not read as "first sight" for every topic
    open(fx, "w").write(dump(even("declared.topic", 4, 500)))
    open(os.path.join(sdir, "partition-dev.json"), "w").write("[]")
    r = subprocess.run([sys.executable, CHECK, "--env", "dev", "--state", sdir,
                        "--offsets-fixture", fx, "--manifest", man], capture_output=True, text=True)
    if r.returncode != 2 or "refusing to treat every topic as first sight" not in r.stdout + r.stderr:
        fails.append("a corrupt baseline is an error")
        print("  FAIL %-64s exit %d" % ("a corrupt baseline is an error", r.returncode))
    else:
        print("  ok   %-64s exit 2" % "a corrupt baseline is an error")

    # the budget is enforced
    open(fx, "w").write(dump(even("declared.topic", 4, 500)))
    r = subprocess.run([sys.executable, CHECK, "--env", "dev", "--state", sdir + "2",
                        "--offsets-fixture", fx, "--manifest", man, "--budget", "0"],
                       capture_output=True, text=True)
    if r.returncode != 2 or "budget" not in r.stdout + r.stderr:
        fails.append("the wall-clock budget is enforced")
        print("  FAIL %-64s exit %d" % ("the wall-clock budget is enforced", r.returncode))
    else:
        print("  ok   %-64s exit 2" % "the wall-clock budget is enforced")
finally:
    shutil.rmtree(d, ignore_errors=True)

print("\n%d case(s) FAILED" % len(fails) if fails else "\nall cases ok")
sys.exit(1 if fails else 0)
