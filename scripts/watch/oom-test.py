#!/usr/bin/env python3
"""Negative controls for oom.py: every verdict is produced from a synthetic kubectl document."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "oom.py")
NOW = "2026-11-17T12:00:00Z"
fails = []


def run(items, *args, now=NOW):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump({"items": items}, fh)
        path = fh.name
    try:
        cmd = [sys.executable, CHECK, "--input", path] + (["--now", now] if now else []) + list(args)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return r.returncode, r.stdout + r.stderr
    finally:
        os.unlink(path)


def case(name, want_rc, items, *args, expect=None, forbid=None):
    rc, out = run(items, *args)
    why = []
    if rc != want_rc:
        why.append("exit %d, wanted %d" % (rc, want_rc))
    if expect and expect not in out:
        why.append("output does not contain %r" % expect)
    if forbid and forbid in out:
        why.append("output contains %r and must not" % forbid)
    if why:
        fails.append(name)
        print("  FAIL %-62s %s" % (name, "; ".join(why)))
        print("       " + out.strip().replace("\n", "\n       ")[:500])
    else:
        print("  ok   %-62s exit %d" % (name, rc))


def pod(name, *, phase="Running", statuses=(), init=(), limit="2Gi", extra_status=None):
    def cs(s):
        c = {"name": s["name"], "restartCount": s.get("restarts", 1)}
        c["state"] = s.get("state", {"running": {"startedAt": "2026-11-17T11:00:00Z"}})
        if s.get("oom"):
            c["lastState"] = {"terminated": {"reason": "OOMKilled", "exitCode": 137, "finishedAt": s["oom"]}}
        if s.get("last"):
            c["lastState"] = {"terminated": s["last"]}
        return c
    st = {"phase": phase}
    if statuses:
        st["containerStatuses"] = [cs(s) for s in statuses]
    if init:
        st["initContainerStatuses"] = [cs(s) for s in init]
    st.update(extra_status or {})
    spec = {"containers": [{"name": s["name"], "resources": {"limits": {"memory": limit}} if limit else {}} for s in statuses]}
    return {"metadata": {"name": name, "creationTimestamp": "2026-11-17T01:00:00Z"}, "spec": spec, "status": st}


OK = pod("healthy-1", statuses=[{"name": "app"}])
print("oom.py negative controls")

case("a healthy pod is clean", 0, [OK], forbid="OOM_KILLED")
case("a recent OOMKilled is reported with its memory limit", 1,
     [OK, pod("gex-0", statuses=[{"name": "gex", "oom": "2026-11-17T11:50:00Z", "restarts": 5}], limit="6Gi")],
     expect="OOM_KILLED gex-0: container gex OOMKilled 10 min ago (memory limit 6Gi, restarts 5)")
case("ONE kill is enough - this is not the crash-loop threshold", 1,
     [pod("once", statuses=[{"name": "app", "oom": "2026-11-17T11:45:00Z", "restarts": 1}])],
     expect="OOM_KILLED once")
case("a kill OUTSIDE the window is history", 0,
     [pod("old", statuses=[{"name": "app", "oom": "2026-11-17T08:00:00Z", "restarts": 9}])], forbid="OOM_KILLED")
case("a container with no memory limit says so", 1,
     [pod("nolimit", statuses=[{"name": "app", "oom": "2026-11-17T11:55:00Z"}], limit=None)],
     expect="memory limit none set")
case("a different termination reason is not a memory kill", 0,
     [pod("err", statuses=[{"name": "app", "last": {"reason": "Error", "exitCode": 1, "finishedAt": "2026-11-17T11:55:00Z"}}])],
     forbid="OOM_KILLED")
case("an OOMKilled with no finishedAt cannot be dated, so is history", 0,
     [pod("undated", statuses=[{"name": "app", "last": {"reason": "OOMKilled", "exitCode": 137}}])], forbid="OOM_KILLED")
case("a CURRENT terminated OOMKilled (a Job pod, never restarted) is reported", 1,
     [pod("job-x", phase="Failed", statuses=[{"name": "run", "state": {"terminated": {"reason": "OOMKilled", "finishedAt": "2026-11-17T11:58:00Z"}}}])],
     expect="OOM_KILLED job-x")
case("an INIT container's kill is reported", 1,
     [pod("initoom", init=[{"name": "seed", "oom": "2026-11-17T11:50:00Z"}], statuses=[{"name": "app"}])],
     expect="container seed")
case("a recent eviction is reported", 1,
     [OK, pod("evicted-1", phase="Failed", statuses=[], extra_status={"reason": "Evicted", "message": "The node was low on resource: memory.",
              "conditions": [{"type": "DisruptionTarget", "lastTransitionTime": "2026-11-17T11:40:00Z"}]})],
     expect="EVICTED evicted-1: evicted 20 min ago: The node was low on resource: memory.")
case("an OLD evicted pod still sitting in the list is not news", 0,
     [OK, pod("evicted-old", phase="Failed", statuses=[], extra_status={"reason": "Evicted", "startTime": "2026-11-10T00:00:00Z"})],
     forbid="EVICTED")
case("an eviction with an unparsable time is an error", 2,
     [pod("evicted-bad", phase="Failed", statuses=[], extra_status={"reason": "Evicted", "startTime": "yesterday"})], expect="unparsable eviction time")
case("--window widens what counts", 1,
     [pod("w", statuses=[{"name": "app", "oom": "2026-11-17T09:00:00Z"}])], "--window", "240", expect="OOM_KILLED w")
case("an empty item list is an error, not 'clean'", 2, [], expect="refusing to report 'clean'")
case("a pod with no status is an error", 2, [{"metadata": {"name": "ghost"}}], expect="has no status")
case("containerStatuses that is not a list is an error", 2,
     [{"metadata": {"name": "odd"}, "status": {"phase": "Running", "containerStatuses": "nope"}}], expect="not a list")
case("an unparsable finishedAt is an error", 2,
     [pod("bad", statuses=[{"name": "app", "oom": "soon"}])], expect="unparsable finishedAt")
case("--window 0 is refused", 2, [OK], "--window", "0", expect="--window must be at least 1")

rc = subprocess.run([sys.executable, CHECK, "--input", "/nonexistent/pods.json"], capture_output=True, text=True).returncode
print("  %s %-62s exit %d" % ("ok  " if rc == 2 else "FAIL", "a missing input file is an error", rc))
if rc != 2:
    fails.append("missing input")

rc, out = run([OK, pod("a", statuses=[{"name": "x", "oom": "2026-11-17T11:50:00Z"}])])
if "2 pod(s) checked, 1 memory kill(s) in the last 30 min" not in out:
    fails.append("summary")
    print("  FAIL the summary counts are accurate:", out.strip().splitlines()[-1:])
else:
    print("  ok   the summary counts are accurate")

print("\n%d case(s) FAILED" % len(fails) if fails else "\nall cases ok")
sys.exit(1 if fails else 0)
