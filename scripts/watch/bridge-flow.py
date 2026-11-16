#!/usr/bin/env python3
"""Bridge-flow check: inside the ES session the Rithmic depth bridge must hold a fresh, complete book.

  bridge-flow.py --env es4 [--state DIR] [--watch-seconds 150] [--interval 15] [--now ISO] [--probe-cmd CMD] [--restarts-cmd CMD]

WHY (2026-11-16): the Rithmic feed went late, then silent; the bridge book was unready from 14:31Z and the pod crash-looped, so the
liquidity wall had no book for 40+ minutes. The crash-loop check paged only once the kubelet reported a loop; nothing paged on the
condition that matters, "no usable book while the market is open". This check asks the pod's own readiness probe (probe.py: fresh,
complete, connected book) and pages when EVERY sample over --watch-seconds says no.

Exit 0 = ready at least once in the window, or the ES session is closed. Exit 1 = BRIDGE_NOT_READY. Exit 2 = the check could not run
(kubectl failed): never reported as healthy.

Session = CME Globex ES: Sunday 18:00 ET to Friday 17:00 ET, with the daily 17:00-18:00 ET break, and no check in the 5 minutes after any
open (the login and the first snapshot need time). Holidays are listed in CLOSED_DATES (ET dates the market is closed all day; the evening before such a date is closed too).
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
SETTLE_MINUTES = 5
CONTINUITY_MINUTES = 10  # a reminder is only a reminder if the previous run (every 3 minutes) also saw the book down
REMIND_MINUTES = 30  # while the book stays down the page repeats this often, not on every run
# Full-day CME equity closures (ET). Early closes are not listed: the check simply sees a ready book until then.
CLOSED_DATES = {"2026-12-25", "2027-01-01"}
CALENDAR_COVERED_THROUGH = "2027-01-01"  # beyond this the list above must be extended; until then a holiday outside it pages falsely
PROBE_TIMEOUT_S = 25
# The remote shell prints the probe's own verdict, so "the probe said no" is told apart from every kubectl failure by a sentinel that only
# the remote command can emit, not by matching kubectl's error text.
DEFAULT_PROBE = ("kubectl --request-timeout=20s -n options-edge exec deploy/rithmic-depth-bridge -- "
                 "sh -c 'python3 /app/probe.py; echo BRIDGE_PROBE_RC=$?'")
DEFAULT_RESTARTS = ("kubectl -n options-edge get pods -l app.kubernetes.io/name=rithmic-depth-bridge "
                    "-o jsonpath={.items[0].status.containerStatuses[0].restartCount}")
# What kubectl says when the probe ran and said no, or when there is no running container to run it in (a restarting or crash-looping
# pod IS the outage this check exists to page on); anything else (auth, network, a missing kubeconfig) is "could not check".
NOT_READY_MARKS = ("container not found", "cannot exec in a stopped container", "is not running", "no pods", "pod does not exist")


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def session_state(now_utc, holidays=CLOSED_DATES):
    """(open, reason). `now_utc` is an aware datetime."""
    t = now_utc.astimezone(ET)
    wd = t.weekday()  # Mon=0 .. Sun=6
    minutes = t.hour * 60 + t.minute
    if wd == 5:
        return False, "Saturday"
    if wd == 4 and minutes >= 17 * 60:
        return False, "after the Friday 17:00 ET close"
    if wd == 6 and minutes < 18 * 60:
        return False, "before the Sunday 18:00 ET open"
    if 17 * 60 <= minutes < 18 * 60 and wd != 6:
        return False, "daily 17:00-18:00 ET break"
    next_day = (t.date() + dt.timedelta(days=1)).isoformat()
    if t.date().isoformat() in holidays or (next_day in holidays and minutes >= 18 * 60):
        return False, "CME holiday"
    since_open = minutes - 18 * 60 if minutes >= 18 * 60 else None
    if since_open is not None and since_open < SETTLE_MINUTES:
        return False, "within %d minutes of the open" % SETTLE_MINUTES
    return True, "ES session"


def sample(cmd):
    """True = ready, False = the probe itself said not ready; anything else is a check failure."""
    try:
        r = subprocess.run(["/bin/bash", "-c", cmd], capture_output=True, text=True, timeout=PROBE_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        die("the bridge probe did not answer within %d s" % PROBE_TIMEOUT_S)
    for line in r.stdout.splitlines():
        if line.strip() == "BRIDGE_PROBE_RC=0":
            return True
        if line.strip() == "BRIDGE_PROBE_RC=1":
            return False
    if any(mark in r.stderr for mark in NOT_READY_MARKS):
        return False
    die("could not run the bridge probe: %s" % " ".join((r.stderr or r.stdout).split())[:300])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True, choices=["es4"])
    ap.add_argument("--state", default=None, help="directory for the page-reminder baseline")
    ap.add_argument("--watch-seconds", type=int, default=150)
    ap.add_argument("--interval", type=int, default=15)
    ap.add_argument("--now", default=None, help="ISO instant, for the tests")
    ap.add_argument("--probe-cmd", default=DEFAULT_PROBE)
    ap.add_argument("--restarts-cmd", default=DEFAULT_RESTARTS)
    a = ap.parse_args()
    if a.watch_seconds < 1 or a.interval < 0:
        die("--watch-seconds must be at least 1 and --interval not negative")
    now = dt.datetime.fromisoformat(a.now.replace("Z", "+00:00")) if a.now else dt.datetime.now(dt.timezone.utc)
    if now.astimezone(ET).date().isoformat() > CALENDAR_COVERED_THROUGH:
        print("bridge-flow: WARNING the closed-dates calendar ends %s; extend CLOSED_DATES" % CALENDAR_COVERED_THROUGH)
    is_open, reason = session_state(now)
    if not is_open:
        print("bridge-flow: %s (%s); not checked" % (reason, now.astimezone(ET).strftime("%a %H:%M ET")))
        return 0
    state_file = None
    if a.state:
        try:
            os.makedirs(a.state, exist_ok=True)
        except OSError as e:
            die("cannot create the state directory %s: %s" % (a.state, e))
        state_file = os.path.join(a.state, "bridge-flow-%s.json" % a.env)
    waited = 0
    samples = 0
    while True:
        ready = sample(a.probe_cmd)
        samples += 1
        if ready:
            print("bridge-flow: the bridge holds a fresh, complete book (sample %d, %d s in)" % (samples, waited))
            if state_file and os.path.exists(state_file):
                os.unlink(state_file)
            return 0
        if waited >= a.watch_seconds:
            break
        step = min(a.interval, a.watch_seconds - waited) if a.interval else a.watch_seconds - waited
        time.sleep(step if a.interval else 0)
        waited += step
    restarts = ""
    try:
        r = subprocess.run(["/bin/bash", "-c", a.restarts_cmd], capture_output=True, text=True, timeout=60)
        if r.returncode == 0 and r.stdout.strip().isdigit():
            restarts = ", container restarts so far: %s" % r.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    if state_file and os.path.exists(state_file):
        try:
            paged = dt.datetime.fromisoformat(json.load(open(state_file))["pagedAt"])
        except (OSError, ValueError, KeyError):
            paged = None
        try:
            seen = dt.datetime.fromisoformat(json.load(open(state_file))["lastSeenAt"])
        except (OSError, ValueError, KeyError):
            seen = None
        continuous = seen is not None and now - seen < dt.timedelta(minutes=CONTINUITY_MINUTES)
        if paged is not None and continuous and now - paged < dt.timedelta(minutes=REMIND_MINUTES):
            json.dump({"pagedAt": paged.isoformat(), "lastSeenAt": now.isoformat()}, open(state_file, "w"))
            print("bridge-flow: still not ready; paged %d minutes ago, next reminder after %d minutes"
                  % ((now - paged).total_seconds() // 60, REMIND_MINUTES))
            return 0
    if state_file:
        try:
            json.dump({"pagedAt": now.isoformat(), "lastSeenAt": now.isoformat()}, open(state_file, "w"))
        except OSError as e:
            die("cannot keep the page baseline %s: %s" % (state_file, e))
    print("BRIDGE_NOT_READY rithmic-depth-bridge: no fresh, complete book for %d s inside the ES session (%d samples)%s; "
          "the liquidity wall has no book (Rithmic late or silent, or the login is failing)" % (waited, samples, restarts))
    return 1


if __name__ == "__main__":
    sys.exit(main())
