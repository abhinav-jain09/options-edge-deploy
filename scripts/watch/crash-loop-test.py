#!/usr/bin/env python3
"""Negative controls for crash-loop.py: every verdict is produced from a synthetic kubectl document."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "crash-loop.py")
NOW = "2026-11-07T12:00:00Z"
fails = []


def run(items, *args, now=NOW):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump({"items": items}, fh)
        path = fh.name
    try:
        cmd = [sys.executable, CHECK, "--input", path]
        if now:
            cmd += ["--now", now]
        cmd += list(args)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return r.returncode, r.stdout + r.stderr
    finally:
        os.unlink(path)


def case(name, want_rc, items, *args, expect=None, forbid=None, now=NOW):
    rc, out = run(items, *args, now=now)
    why = []
    if rc != want_rc:
        why.append("exit %d, wanted %d" % (rc, want_rc))
    if expect and expect not in out:
        why.append("output does not contain %r" % expect)
    if forbid and forbid in out:
        why.append("output contains %r and must not" % forbid)
    if why:
        fails.append(name)
        print("  FAIL %-58s %s" % (name, "; ".join(why)))
        print("       " + out.strip().replace("\n", "\n       ")[:600])
    else:
        print("  ok   %-58s exit %d" % (name, rc))


def pod(name, *, phase="Running", containers=(), init=()):
    def cs(spec):
        c = {"name": spec["name"], "restartCount": spec.get("restarts", 0)}
        if spec.get("waiting"):
            c["state"] = {"waiting": {"reason": spec["waiting"]}}
        else:
            started = spec.get("started") or spec.get("finished") or "2026-11-01T00:00:00Z"
            c["state"] = {"running": {"startedAt": started}}
        if spec.get("finished"):
            c["lastState"] = {"terminated": {"finishedAt": spec["finished"]}}
        if "restartCount" in spec:
            c["restartCount"] = spec["restartCount"]
        return c
    out = {"metadata": {"name": name}, "status": {"phase": phase}}
    if containers:
        out["status"]["containerStatuses"] = [cs(s) for s in containers]
    if init:
        out["status"]["initContainerStatuses"] = [cs(s) for s in init]
    return out


HEALTHY = pod("healthy-1", containers=[{"name": "app", "restarts": 0}])

print("crash-loop.py negative controls")

# --- the happy path, and the floor under every other case
case("a healthy pod is clean", 0, [HEALTHY], forbid="CRASHLOOP")

# --- R2a: the two ways a loop shows up
case("a container in CrashLoopBackOff", 1,
     [HEALTHY, pod("eav", containers=[{"name": "app", "waiting": "CrashLoopBackOff"}])],
     expect="CRASHLOOP eav")
case("restarts at or over the threshold inside the window", 1,
     [HEALTHY, pod("noisy", containers=[{"name": "app", "restarts": 3,
                                         "finished": "2026-11-07T11:50:00Z"}])],
     expect="RESTARTS noisy")
case("restarts under the threshold are not a loop", 0,
     [HEALTHY, pod("twice", containers=[{"name": "app", "restarts": 2,
                                         "finished": "2026-11-07T11:50:00Z"}])],
     forbid="RESTARTS")
case("restarts OUTSIDE the window are history, not a loop", 0,
     [HEALTHY, pod("old", containers=[{"name": "app", "restarts": 9,
                                       "finished": "2026-11-07T06:00:00Z"}])],
     forbid="RESTARTS")
case("a high restart count with NO timestamp is not reported", 0,
     [HEALTHY, pod("undated", containers=[{"name": "app", "restartCount": 9}])],
     forbid="RESTARTS")

# --- R2b: an image that cannot be pulled is the same class
case("ImagePullBackOff is reported as IMAGE_PULL", 1,
     [HEALTHY, pod("badimg", containers=[{"name": "app", "waiting": "ImagePullBackOff"}])],
     expect="IMAGE_PULL badimg")
case("ErrImagePull is reported as IMAGE_PULL", 1,
     [HEALTHY, pod("badimg2", containers=[{"name": "app", "waiting": "ErrImagePull"}])],
     expect="IMAGE_PULL badimg2")
case("an ordinary waiting reason is not a finding", 0,
     [HEALTHY, pod("starting", containers=[{"name": "app", "waiting": "ContainerCreating"}])],
     forbid="CRASHLOOP")

# --- R2c: init containers count. This is the case that would have been missed.
case("an INIT container in CrashLoopBackOff is reported", 1,
     [HEALTHY, pod("initloop", init=[{"name": "wait-for-kafka", "waiting": "CrashLoopBackOff"}],
                   containers=[{"name": "app", "restarts": 0}])],
     expect="init container wait-for-kafka")
case("an INIT container that cannot pull its image is reported", 1,
     [HEALTHY, pod("initpull", init=[{"name": "seed", "waiting": "ImagePullBackOff"}])],
     expect="IMAGE_PULL initpull")

# --- R2f: finished Job pods
case("a Succeeded Job pod is ignored, not reported", 0,
     [HEALTHY, pod("job-ok", phase="Succeeded",
                   containers=[{"name": "run", "restarts": 4, "finished": "2026-11-07T11:55:00Z"}])],
     expect="1 finished Job pod(s) ignored")
case("a Failed pod with nothing restarting is ignored", 0,
     [HEALTHY, pod("job-bad", phase="Failed",
                   containers=[{"name": "run", "restarts": 4, "finished": "2026-11-07T11:55:00Z"}])],
     expect="finished Job pod(s) ignored")
case("a Failed pod that IS crash-looping is still reported", 1,
     [HEALTHY, pod("job-loop", phase="Failed",
                   containers=[{"name": "run", "waiting": "CrashLoopBackOff"}])],
     expect="CRASHLOOP job-loop")

# --- R2d: fail closed. Each of these must be exit 2, never 0.
case("an empty item list is an error, not 'clean'", 2, [], expect="refusing to report 'clean'")
case("a pod with no status is an error", 2,
     [{"metadata": {"name": "ghost"}}], expect="has no status")
case("a non-integer restartCount is an error", 2,
     [pod("weird", containers=[{"name": "app", "restartCount": "many"}])],
     expect="non-integer restartCount")
case("an unparsable restart timestamp is an error", 2,
     [pod("weird2", containers=[{"name": "app", "restarts": 5, "started": "not-a-time"}])],
     expect="unparsable restart timestamp")
case("the restart is dated from lastState when there is no running state", 1,
     [HEALTHY, {"metadata": {"name": "term"}, "status": {"phase": "Running", "containerStatuses": [
         {"name": "app", "restartCount": 4,
          "state": {"terminated": {"exitCode": 1}},
          "lastState": {"terminated": {"finishedAt": "2026-11-07T11:45:00Z"}}}]}}],
     expect="RESTARTS term")
case("containerStatuses that is not a list is an error", 2,
     [{"metadata": {"name": "odd"}, "status": {"phase": "Running", "containerStatuses": "nope"}}],
     expect="not a list")
case("--restarts 0 is refused, it would report every pod", 2, [HEALTHY], "--restarts", "0",
     expect="--restarts must be at least 1")
case("--window 0 is refused", 2, [HEALTHY], "--window", "0", expect="--window must be at least 1")

rc, out = subprocess.run([sys.executable, CHECK, "--input", "/nonexistent/pods.json"],
                         capture_output=True, text=True).returncode, ""
if rc != 2:
    fails.append("a missing input file is an error")
    print("  FAIL %-58s exit %d, wanted 2" % ("a missing input file is an error", rc))
else:
    print("  ok   %-58s exit 2" % "a missing input file is an error")

# --- the count line is the only summary a reader gets, so it must be true
rc, out = run([HEALTHY, pod("x", containers=[{"name": "app", "waiting": "CrashLoopBackOff"}]),
               pod("job", phase="Succeeded", containers=[{"name": "r", "restarts": 0}])])
if "2 pod(s) checked, 1 finished Job pod(s) ignored, 1 crash-looping" not in out:
    fails.append("the summary counts are accurate")
    print("  FAIL %-58s %s" % ("the summary counts are accurate", out.strip().splitlines()[-1:]))
else:
    print("  ok   %-58s" % "the summary counts are accurate")

print("\n%d case(s) FAILED" % len(fails) if fails else "\nall cases ok")
sys.exit(1 if fails else 0)
