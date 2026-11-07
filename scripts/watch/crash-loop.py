#!/usr/bin/env python3
"""Crash-loop check (FLEET-ALERT-DISCORD-REQUIREMENT R2): a pod whose container keeps dying, or cannot start
its image, is reported NOW rather than after fleet-readiness.py's 30-minute unavailability threshold.

  crash-loop.py --input kubectl-get-pods-json [--restarts 3] [--window 30] [--now ISO]

Fail closed: unparsable input, an item without a status, or no pods at all is an error, not 'healthy'.
Exit 0 = clean, 1 = at least one crash-looping pod, 2 = the check itself could not run.

WHY THIS IS NOT fleet-readiness.py (R2, §1a of the requirement). That check reports a workload that WANTS
replicas and has had fewer available for longer than --minutes (30). It does catch a crash loop eventually,
and it treats three restarts as not-healthy when deciding an exemption — but a crash loop is visible in the
pod status seconds after the second restart, and the owner asked for an alert on the crash loop itself.
"""
import argparse
import datetime as dt
import json
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True, help="kubectl -n <ns> get pods -o json")
ap.add_argument("--restarts", type=int, default=3, help="restarts inside --window that count as a loop")
ap.add_argument("--window", type=int, default=30, help="minutes; a restart older than this is history")
ap.add_argument("--now", default=None, help="ISO instant, for the tests")
a = ap.parse_args()

# R2b: a pod that cannot pull its image is as dead as one that keeps dying, and the remedy is the same.
WAITING_FATAL = {"CrashLoopBackOff": "CRASHLOOP", "ImagePullBackOff": "IMAGE_PULL", "ErrImagePull": "IMAGE_PULL"}


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def ts(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


try:
    data = json.load(open(a.input))
    items = data["items"]
except Exception as e:  # noqa: BLE001
    die("cannot read kubectl output: %s" % e)
if not isinstance(items, list):
    die("`items` is not a list - refusing to report 'clean' on an answer this shape")
if not items:
    # Same rule as fleet-readiness.py: an empty answer is a failed question, not a healthy fleet.
    die("no pods in the input - refusing to report 'clean' on an empty answer")
now = ts(a.now) if a.now else dt.datetime.now(dt.timezone.utc)
if a.restarts < 1:
    die("--restarts must be at least 1 (got %d): 0 would report every pod" % a.restarts)
if a.window < 1:
    die("--window must be at least 1 minute (got %d)" % a.window)

findings, clean, finished = [], 0, 0
for pod in items:
    name = pod.get("metadata", {}).get("name") or "<unnamed>"
    status = pod.get("status")
    if not isinstance(status, dict):
        die("pod %s has no status - a pod whose state cannot be read is not a healthy pod" % name)
    phase = status.get("phase")
    # R2c: BOTH lists. An init container that crashes or cannot pull keeps the service just as down, and a
    # check that read only containerStatuses would call such a pod healthy.
    statuses = []
    for key in ("initContainerStatuses", "containerStatuses"):
        v = status.get(key)
        if v is None:
            continue
        if not isinstance(v, list):
            die("pod %s has a %s that is not a list" % (name, key))
        statuses += [(key, c) for c in v]

    # R2f: a finished Job pod is not a crash loop. Counted, never reported: a `get pods | wc -l` that
    # counted Completed pods from June once made a wait-for-zero loop run for ten minutes.
    restarting = any((c.get("state") or {}).get("waiting", {}).get("reason") in WAITING_FATAL
                     for _, c in statuses)
    if phase == "Succeeded" or (phase == "Failed" and not restarting):
        finished += 1
        continue

    hits = []
    for key, c in statuses:
        cname = c.get("name") or "<unnamed>"
        where = "init container" if key == "initContainerStatuses" else "container"
        state = c.get("state") or {}
        waiting = state.get("waiting") or {}
        reason = waiting.get("reason")
        if reason in WAITING_FATAL:
            hits.append((WAITING_FATAL[reason], "%s %s is %s" % (where, cname, reason)))
            continue
        rc = c.get("restartCount")
        if rc is None:
            continue
        if not isinstance(rc, int) or isinstance(rc, bool):
            die("pod %s %s %s has a non-integer restartCount %r" % (name, where, cname, rc))
        if rc < a.restarts:
            continue
        # A restart count is cumulative for the life of the pod, so it says nothing on its own about
        # whether the pod is dying NOW. The terminated-at of the last run is what dates it; without that
        # timestamp the count is history and is not reported.
        fin = ((state.get("running") or {}).get("startedAt")
               or ((c.get("lastState") or {}).get("terminated") or {}).get("finishedAt"))
        if not fin:
            continue
        try:
            when = ts(fin)
        except Exception as e:  # noqa: BLE001
            die("pod %s %s %s has an unparsable restart timestamp %r (%s)" % (name, where, cname, fin, e))
        age_min = (now - when).total_seconds() / 60.0
        if age_min <= a.window:
            hits.append(("RESTARTS", "%s %s restarted %d times, last %d min ago"
                         % (where, cname, rc, round(age_min))))
    if hits:
        findings.append((name, hits))
    else:
        clean += 1

for name, hits in sorted(findings):
    for cls, detail in hits:
        print("%s %s: %s" % (cls, name, detail))
print("crash-loop: %d pod(s) checked, %d finished Job pod(s) ignored, %d crash-looping"
      % (clean + len(findings), finished, len(findings)))
sys.exit(1 if findings else 0)
