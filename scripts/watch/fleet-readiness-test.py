#!/usr/bin/env python3
"""Negative controls for fleet-readiness.py: every verdict is produced from a synthetic kubectl document."""
import json, os, subprocess, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__)); CHECK = os.path.join(HERE, "fleet-readiness.py")
NOW = "2026-11-03T12:00:00Z"


def dep(name, want, have, since="2026-11-03T10:00:00Z", kind="Deployment", conds=True):
    st = {"availableReplicas": have, "updatedReplicas": want, "observedGeneration": 1} if have is not None else {}
    if conds:
        st["conditions"] = [{"type": "Available", "status": "True" if have and have >= want else "False", "lastTransitionTime": since}]
        if kind == "Deployment" and have and have >= want:
            # what a real, finished rollout carries
            st["conditions"].append({"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable", "lastUpdateTime": since, "lastTransitionTime": since})
    return {"kind": kind, "metadata": {"name": name, "generation": 1, "creationTimestamp": "2026-10-01T00:00:00Z"}, "spec": {"replicas": want}, "status": st}


def run(items, raw=None, minutes=30):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        f.write(raw if raw is not None else json.dumps({"items": items}))
    r = subprocess.run([sys.executable, CHECK, "--input", f.name, "--now", NOW, "--minutes", str(minutes)], capture_output=True, text=True)
    os.unlink(f.name); return r.returncode, r.stdout


fail = 0
def expect(name, rc_want, out_has, items=None, raw=None):
    global fail
    rc, out = run(items, raw)
    good = rc == rc_want and (out_has in out)
    print(("ok     " if good else "NOT-OK ") + name + ("" if good else " (rc=%d, out=%r)" % (rc, out)))
    fail |= 0 if good else 1


expect("all workloads available", 0, "0 stuck", [dep("a", 1, 1), dep("b", 2, 2)])
expect("held at 0 replicas is never reported, however old", 0, "1 held at 0", [dep("a", 1, 1), dep("held", 0, 0, since="2026-01-01T00:00:00Z")])
expect("the 9-hour crash-loop: wants 1, 0 available since 03:00", 1, "STUCK Deployment/eav", [dep("eav", 1, 0, since="2026-11-03T03:00:00Z")])
expect("short for under the threshold is tolerated (rollout in progress)", 0, "0 stuck", [dep("roll", 1, 0, since="2026-11-03T11:45:00Z")])
expect("exactly at the threshold is tolerated, one minute over is not", 1, "STUCK", [dep("edge", 1, 0, since="2026-11-03T11:29:00Z")])
expect("a StatefulSet counts too", 1, "StatefulSet/db", [dep("db", 1, 0, kind="StatefulSet", since="2026-11-03T05:00:00Z")])
expect("partly available (1 of 3) for hours is stuck", 1, "wants 3, 1 available", [dep("p", 3, 1, since="2026-11-03T01:00:00Z")])
expect("no conditions: falls back to creation time, long ago -> stuck", 1, "STUCK", [dep("n", 1, 0, conds=False)])
expect("an empty answer is an error, not 'all healthy'", 2, "ERROR", [])
expect("garbage input is an error", 2, "ERROR", raw="not json")
expect("an object with no status is an error", 2, "ERROR", [{"kind": "Deployment", "metadata": {"name": "x"}, "spec": {"replicas": 1}}])
def rollout(name, want, updated, prog_status, prog_reason, last="2026-11-03T05:00:00Z", gen=3, observed=3):
    d = dep(name, want, want, since="2026-11-03T01:00:00Z")
    d["metadata"]["generation"] = gen
    d["status"].update({"updatedReplicas": updated, "observedGeneration": observed})
    d["status"]["conditions"].append({"type": "Progressing", "status": prog_status, "reason": prog_reason, "lastUpdateTime": last, "lastTransitionTime": last})
    return d


expect("old pods still serving, rollout failed (ProgressDeadlineExceeded): STUCK", 1, "rollout failed", [rollout("bad", 1, 0, "False", "ProgressDeadlineExceeded")])
expect("old pods serving, new generation 0 of 1 updated for hours: STUCK", 1, "rollout not completing", [rollout("slow", 1, 0, "True", "ReplicaSetUpdated")])
expect("a rollout in progress for under the threshold is tolerated", 0, "0 stuck", [rollout("fresh", 1, 0, "True", "ReplicaSetUpdated", last="2026-11-03T11:50:00Z")])
expect("the new generation unobserved for hours is stuck", 1, "rollout not completing", [rollout("unobs", 1, 1, "True", "NewReplicaSetAvailable", gen=4, observed=3)])
expect("a completed rollout (updated == desired, generation observed) is healthy", 0, "1 healthy", [rollout("done", 2, 2, "True", "NewReplicaSetAvailable")])
expect("new pods updated == desired but never Ready, old pods still serving, ReplicaSetUpdated for hours: STUCK", 1, "rollout not completing", [rollout("surge", 2, 2, "True", "ReplicaSetUpdated")])
expect("the same state for under the threshold is a rollout in flight", 0, "0 stuck", [rollout("surge2", 2, 2, "True", "ReplicaSetUpdated", last="2026-11-03T11:55:00Z")])
nop = rollout("noprog", 1, 0, "True", "x"); nop["status"]["conditions"] = [c for c in nop["status"]["conditions"] if c["type"] != "Progressing"]
expect("lagging rollout with no Progressing condition fails closed", 1, "no Progressing timestamp", [nop])
sys.exit(fail)
