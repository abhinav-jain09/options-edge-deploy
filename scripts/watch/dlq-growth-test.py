#!/usr/bin/env python3
"""Negative controls for dlq-growth.py using topic/offset fixtures (no Kafka)."""
import json, os, subprocess, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__)); CHECK = os.path.join(HERE, "dlq-growth.py")
T = tempfile.mkdtemp(); fail = 0
MAN = os.path.join(T, "manifest.json"); json.dump({"dlqTopics": {"prod": ["a.dlq", "b.dlq", "c.dlq", "y.dlq"]}, "entries": [{"kind": "MIRRORED", "environments": ["prod"], "topics": [{"name": "named.dlq.topic", "kind": "dlq"}]}]}, open(MAN, "w"))


def run(topics, offs, state="s", env="prod", fixture_ok=True):
    tf, of = os.path.join(T, "t"), os.path.join(T, "o"); open(tf, "w").write(" ".join(topics)); json.dump(offs, open(of, "w"))
    r = subprocess.run([sys.executable, CHECK, "--env", env, "--state", os.path.join(T, state), "--manifest", MAN, "--topics-fixture", tf, "--offsets-fixture", of], capture_output=True, text=True)
    return r.returncode, r.stdout


def expect(name, got, rc, has):
    global fail
    ok = got[0] == rc and has in got[1]; print(("ok     " if ok else "NOT-OK ") + name + ("" if ok else " (rc=%d out=%r)" % got)); fail |= 0 if ok else 1


expect("first run records a baseline and passes", run(["a.dlq", "x"], {"a.dlq": 5}, "s1"), 0, "baseline recorded")
expect("no growth passes", run(["a.dlq", "x"], {"a.dlq": 5}, "s1"), 0, "0 grew")
expect("growth fails and says how much", run(["a.dlq", "x"], {"a.dlq": 9}, "s1"), 1, "+4 dead letters")
expect("the new level is the baseline: the same sum again passes", run(["a.dlq", "x"], {"a.dlq": 9}, "s1"), 0, "0 grew")
expect("a drop (topic recreated by a clean) is not an alert", run(["a.dlq", "x"], {"a.dlq": 0}, "s1"), 0, "0 grew")
expect("growth after a drop is measured from the dropped baseline", run(["a.dlq", "x"], {"a.dlq": 3}, "s1"), 1, "+3")
expect("a topic named by the manifest is watched even without the .dlq suffix", run(["named.dlq.topic"], {"named.dlq.topic": 1}, "s2"), 0, "1 DLQ topic")
expect("a manifest DLQ missing from the cluster is reported, not failed", run(["other"], {}, "s3"), 0, "MISSING_DLQ named.dlq.topic")
expect("a .dlq topic the manifest does not declare is a failure", run(["rogue.dlq", "a.dlq"], {"rogue.dlq": 1, "a.dlq": 0}, "s8"), 1, "UNDECLARED_DLQ rogue.dlq")
expect("an empty topic list is an error, not 'no dead letters'", run([], {}, "s4"), 2, "ERROR")
expect("unknown environment is an error", run(["a.dlq"], {"a.dlq": 1}, "s5", env="staging"), 2, "ERROR")
# a failed read must not erase the baseline
run(["b.dlq"], {"b.dlq": 7}, "s6"); rc, out = run(["b.dlq"], {}, "s6")   # offsets fixture lacks the topic -> KeyError -> non-zero
expect("a failed offsets read exits non-zero", (rc, out), rc, "") if False else None
good = rc != 0; print(("ok     " if good else "NOT-OK ") + "a failed offsets read is not success"); fail |= 0 if good else 1
rc2, out2 = run(["b.dlq"], {"b.dlq": 8}, "s6"); good = rc2 == 1 and "+1" in out2; print(("ok     " if good else "NOT-OK ") + "the baseline survived the failed run"); fail |= 0 if good else 1
# a DLQ that vanishes from one listing and returns bigger must still be caught
run(["c.dlq", "x"], {"c.dlq": 10}, "s7"); expect("a topic absent from one listing does not lose its baseline", run(["x", "y.dlq"], {"y.dlq": 1}, "s7"), 0, "1 DLQ topic")
expect("...and growth after it returns is still detected", run(["c.dlq", "x"], {"c.dlq": 15}, "s7"), 1, "+5 dead letters")
os.makedirs(os.path.join(T, "s9"), exist_ok=True); open(os.path.join(T, "s9", "prod.json"), "w").write('{"a.dlq": 5')
expect("a corrupt baseline fails closed instead of reading every DLQ as first sight", run(["a.dlq", "x"], {"a.dlq": 9}, "s9"), 2, "unreadable")
sys.exit(fail)
