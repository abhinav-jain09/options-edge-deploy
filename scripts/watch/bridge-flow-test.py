#!/usr/bin/env python3
"""Negative controls for bridge-flow.py: the probe is a shell fixture, the clock is --now."""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "bridge-flow.py")
# Mon 2026-11-16 10:00 ET (15:00Z): inside the session. Sat 2026-11-14 is closed.
IN_SESSION = "2026-11-16T15:00:00Z"
fails = []


def run(probe_script, *args, now=IN_SESSION):
    with tempfile.TemporaryDirectory() as d:
        probe = os.path.join(d, "probe.sh")
        counter = os.path.join(d, "n")
        open(probe, "w").write("#!/bin/bash\nn=$(cat %s 2>/dev/null || echo 0); n=$((n+1)); echo $n > %s\n%s\n" % (counter, counter, probe_script))
        os.chmod(probe, 0o755)
        cmd = [sys.executable, CHECK, "--env", "es4", "--now", now, "--interval", "0", "--watch-seconds", "2",
               "--probe-cmd", probe, "--restarts-cmd", "echo 186"] + list(args)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        calls = int(open(counter).read()) if os.path.exists(counter) else 0
        return r.returncode, r.stdout + r.stderr, calls


def case(name, want_rc, script, expect=None, forbid=None, now=IN_SESSION, want_calls=None):
    rc, out, calls = run(script, now=now)
    why = []
    if rc != want_rc:
        why.append("exit %d, wanted %d" % (rc, want_rc))
    if expect and expect not in out:
        why.append("output lacks %r" % expect)
    if forbid and forbid in out:
        why.append("output contains %r" % forbid)
    if want_calls is not None and calls != want_calls:
        why.append("probe called %d times, wanted %d" % (calls, want_calls))
    if why:
        fails.append(name)
        print("  FAIL %-62s %s\n       %s" % (name, "; ".join(why), out.strip()[:400]))
    else:
        print("  ok   %-62s exit %d" % (name, rc))


print("bridge-flow.py negative controls")
NOT_READY = "echo BRIDGE_PROBE_RC=1"
case("a ready book is clean and stops sampling at once", 0, "echo BRIDGE_PROBE_RC=0", expect="fresh, complete book", want_calls=1)
case("unready for the whole window pages with the restart count", 1, NOT_READY, expect="BRIDGE_NOT_READY", want_calls=2)
case("the restart count is in the message", 1, NOT_READY, expect="restarts so far: 186")
case("ready on a later sample is clean (a login that took a while)", 0,
     'n=$(cat "$(dirname "$0")/n"); if [ "$n" -ge 2 ]; then echo BRIDGE_PROBE_RC=0; exit 0; fi; ' + NOT_READY, want_calls=2)
case("a crash-looping pod (no container to exec in) is the outage, not a failed check", 1,
     "echo 'error: unable to upgrade connection: container not found (\"collector\")' >&2; exit 1", expect="BRIDGE_NOT_READY")
case("a stopped container is the outage", 1, "echo 'cannot exec in a stopped container' >&2; exit 1", expect="BRIDGE_NOT_READY")
case("an error message that merely mentions exit code 1 is still a failed check", 2,
     "echo 'Unable to connect: command terminated with exit code 1' >&2; exit 1", expect="ERROR:")
case("a probe that hangs is a failed check (exit 2), not a traceback", 2, "sleep 40", expect="did not answer")
case("kubectl failing is exit 2, never healthy", 2, "echo 'error: You must be logged in to the server' >&2; exit 1", expect="ERROR: could not run the bridge probe")
case("a missing binary is exit 2", 2, "echo 'bash: kubectl: command not found' >&2; exit 127", expect="ERROR:")
case("Saturday is not checked", 0, NOT_READY, now="2026-11-14T15:00:00Z", expect="Saturday", want_calls=0)
case("the 17:00-18:00 ET break is not checked", 0, NOT_READY, now="2026-11-16T22:30:00Z", expect="break", want_calls=0)
case("Sunday before 18:00 ET is not checked", 0, NOT_READY, now="2026-11-15T22:59:00Z", expect="Sunday", want_calls=0)
case("the first 5 minutes after the Sunday open are not checked", 0, NOT_READY, now="2026-11-15T23:03:00Z", expect="within 5 minutes", want_calls=0)
case("10 minutes after the Sunday open IS checked", 1, NOT_READY, now="2026-11-15T23:10:00Z", expect="BRIDGE_NOT_READY")
case("Friday after 17:00 ET is not checked", 0, NOT_READY, now="2026-11-20T22:30:00Z", expect="Friday", want_calls=0)
case("Christmas Day is not checked", 0, NOT_READY, now="2026-12-25T15:00:00Z", expect="holiday", want_calls=0)
case("the evening before Christmas Day is not checked", 0, NOT_READY, now="2026-12-25T00:30:00Z", expect="holiday", want_calls=0)
# --- paging cadence: one page, then a reminder every 30 minutes, cleared when the book returns
with tempfile.TemporaryDirectory() as st:
    def with_state(script, now):
        rc, out, _ = run(script, "--state", st, now=now)
        return rc, out
    rc, out = with_state(NOT_READY, "2026-11-16T15:00:00Z")
    check = rc == 1 and "BRIDGE_NOT_READY" in out
    rc2, out2 = with_state(NOT_READY, "2026-11-16T15:03:00Z")
    check2 = rc2 == 0 and "still not ready; paged 3 minutes ago" in out2 and "BRIDGE_NOT_READY" not in out2
    rc3, out3 = with_state(NOT_READY, "2026-11-16T15:31:00Z")
    check3 = rc3 == 1 and "BRIDGE_NOT_READY" in out3
    rc4, _ = with_state("echo BRIDGE_PROBE_RC=0", "2026-11-16T15:40:00Z")
    rc5, out5 = with_state(NOT_READY, "2026-11-16T15:45:00Z")
    check5 = rc4 == 0 and rc5 == 1 and "BRIDGE_NOT_READY" in out5
    # recovery and re-failure between two runs: the old baseline is stale (last seen 25 minutes ago), so the new outage pages at once
    with_state("echo BRIDGE_PROBE_RC=0", "2026-11-16T16:00:00Z")
    with_state(NOT_READY, "2026-11-16T16:03:00Z")
    rcg, outg = with_state(NOT_READY, "2026-11-16T16:20:00Z")
    check6 = rcg == 1 and "BRIDGE_NOT_READY" in outg
    for name, ok in (("a gap in observation pages again instead of being swallowed as a reminder", check6), ("first outage pages", check), ("repeat inside 30 minutes is a reminder, not a page", check2),
                     ("page repeats after 30 minutes", check3), ("recovery clears the baseline so the next outage pages at once", check5)):
        print("  %s %s" % ("ok  " if ok else "FAIL", name))
        if not ok:
            fails.append(name)
if fails:
    print("\n%d FAILED: %s" % (len(fails), "; ".join(fails)))
    sys.exit(1)
print("\nall bridge-flow negative controls pass")
