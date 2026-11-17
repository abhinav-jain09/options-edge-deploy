#!/usr/bin/env python3
"""Memory-kill check: a container the kernel/kubelet killed for MEMORY inside the window, and a pod evicted inside it.

  oom.py --input kubectl-get-pods-json [--window 30] [--now ISO]

WHY THIS IS NOT crash-loop.py. That check needs `--restarts` (3) restarts inside the window before it says anything, so a service killed for memory once or
twice an hour - gex 5 times, the gateway twice, journal-dispatcher 11 times on 2026-11-17 - never reached it, and the cause (memory) was not in its message
when it did. One OOMKilled is reported here, with the container's memory limit, because the limit is the first thing the reader needs.

Fail closed: unparsable input, a pod without a status, or no pods at all is an error, not 'healthy'.
Exit 0 = nothing killed for memory in the window, 1 = at least one, 2 = the check itself could not run.
"""
import argparse
import datetime as dt
import json
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True, help="kubectl -n <ns> get pods -o json")
ap.add_argument("--window", type=int, default=30, help="minutes; a kill older than this is history")
ap.add_argument("--now", default=None, help="ISO instant, for the tests")
a = ap.parse_args()


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def ts(s):
    t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if t.tzinfo is None:
        raise ValueError("no UTC offset")
    return t


try:
    items = json.load(open(a.input))["items"]
except Exception as e:  # noqa: BLE001
    die("cannot read kubectl output: %s" % e)
if not isinstance(items, list):
    die("`items` is not a list - refusing to report 'clean' on an answer this shape")
if not items:
    die("no pods in the input - refusing to report 'clean' on an empty answer")
if a.window < 1:
    die("--window must be at least 1 minute (got %d)" % a.window)
now = ts(a.now) if a.now else dt.datetime.now(dt.timezone.utc)

findings, checked = [], 0
for pod in items:
    name = (pod.get("metadata") or {}).get("name") or "<unnamed>"
    status = pod.get("status")
    if not isinstance(status, dict):
        die("pod %s has no status - a pod whose state cannot be read is not a healthy pod" % name)
    checked += 1

    # An evicted pod stays in the list as Failed/Evicted for as long as nobody deletes it, so only a RECENT eviction counts. The eviction instant is the
    # DisruptionTarget condition's transition (1.26+), else the pod's start, else its creation.
    if status.get("phase") == "Failed" and status.get("reason") == "Evicted":
        when = None
        for c in status.get("conditions") or []:
            if c.get("type") == "DisruptionTarget" and c.get("lastTransitionTime"):
                when = c["lastTransitionTime"]
        when = when or status.get("startTime") or (pod.get("metadata") or {}).get("creationTimestamp")
        try:
            age = (now - ts(when)).total_seconds() / 60.0
        except Exception as e:  # noqa: BLE001
            die("pod %s has an unparsable eviction time %r (%s)" % (name, when, e))
        if age <= a.window:
            findings.append((name, "EVICTED", "evicted %d min ago: %s" % (round(age), (status.get("message") or "no message")[:120])))
        continue

    limits = {}
    for c in (pod.get("spec") or {}).get("containers") or []:
        limits[c.get("name")] = ((c.get("resources") or {}).get("limits") or {}).get("memory")
    for key in ("initContainerStatuses", "containerStatuses"):
        v = status.get(key)
        if v is None:
            continue
        if not isinstance(v, list):
            die("pod %s has a %s that is not a list" % (name, key))
        for c in v:
            cname = c.get("name") or "<unnamed>"
            # BOTH places a kill shows: the previous run (lastState, the usual case after a restart) and the current state (a container that is not
            # restarted, e.g. a Job pod or restartPolicy Never).
            for where in ("lastState", "state"):
                t = (c.get(where) or {}).get("terminated") or {}
                if t.get("reason") != "OOMKilled":
                    continue
                fin = t.get("finishedAt")
                if not fin:
                    continue        # cannot be dated, so it is history, not news (same rule as crash-loop.py's restart count)
                try:
                    age = (now - ts(fin)).total_seconds() / 60.0
                except Exception as e:  # noqa: BLE001
                    die("pod %s container %s has an unparsable finishedAt %r (%s)" % (name, cname, fin, e))
                if age <= a.window:
                    lim = limits.get(cname)
                    findings.append((name, "OOM_KILLED", "container %s OOMKilled %d min ago (memory limit %s, restarts %s)"
                                     % (cname, round(age), lim or "none set", c.get("restartCount", "?"))))

for name, cls, detail in sorted(findings):
    print("%s %s: %s" % (cls, name, detail))
print("oom: %d pod(s) checked, %d memory kill(s) in the last %d min" % (checked, len(findings), a.window))
sys.exit(1 if findings else 0)
