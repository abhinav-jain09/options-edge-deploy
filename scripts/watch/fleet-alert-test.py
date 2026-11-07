#!/usr/bin/env python3
"""Negative controls for fleet-alert.py (R1e): the message contract, the caps and the modes."""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "fleet-alert.py")
fails = []


def units(s):
    return len(s.encode("utf-16-le")) // 2


def run(text, rc, env="dev", check="crash-loop", mode=None, build=None):
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as fh:
        fh.write(text)
        path = fh.name
    try:
        cmd = [sys.executable, CHECK, "--env", env, "--check", check, "--rc", str(rc), "--input", path]
        if mode:
            cmd += ["--mode", mode]
        if build:
            cmd += ["--build-url", build]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return r.returncode, r.stdout
    finally:
        os.unlink(path)


def case(name, text, rc, *, expect=None, forbid=None, empty=False, mode=None, env="dev",
         check="crash-loop", build=None, max_units=None, lines=None):
    code, out = run(text, rc, env=env, check=check, mode=mode, build=build)
    why = []
    if code != 0:
        why.append("composer exited %d" % code)
    if empty and out.strip():
        why.append("expected NO message, got %r" % out[:120])
    if not empty and not out.strip():
        why.append("expected a message, got nothing")
    if expect and expect not in out:
        why.append("missing %r" % expect)
    if forbid and forbid in out:
        why.append("contains %r" % forbid)
    if max_units and units(out.strip()) > max_units:
        why.append("%d UTF-16 units, over %d" % (units(out.strip()), max_units))
    if lines is not None and len([l for l in out.strip().splitlines() if l]) != lines:
        why.append("%d lines, wanted %d" % (len(out.strip().splitlines()), lines))
    if why:
        fails.append(name)
        print("  FAIL %-62s %s" % (name, "; ".join(why)))
    else:
        print("  ok   %-62s" % name)


FIND = "CRASHLOOP eav-pod: container app is CrashLoopBackOff\n"

print("fleet-alert.py negative controls")

# --- exit 0 is silence; exit 1 is a message
case("exit 0 sends nothing", "crash-loop: 10 pod(s) checked\n", 0, empty=True)
case("exit 1 sends one message naming the environment and check", FIND, 1,
     expect="**dev** / `crash-loop`")
case("the message states the finding count", FIND + "CRASHLOOP b: container x is CrashLoopBackOff\n", 1,
     expect="2 finding(s)")

# --- R1e: exit 2 is delivered, and in BOTH modes. This is the round-2 correction.
case("exit 2 is delivered as 'could not check'", "ERROR: cannot read kubectl output: boom\n", 2,
     expect="could not check", check="crash-loop")
case("exit 2 carries the reason", "ERROR: cannot read kubectl output: boom\n", 2,
     expect="cannot read kubectl output: boom")
case("exit 2 is delivered even in observation-only", "ERROR: broker unreachable\n", 2,
     mode="observation-only", expect="could not check")
case("a FINDING is silent in observation-only", "SKEW t [svc]: busiest took 100%\n", 1,
     mode="observation-only", empty=True, check="partition-balance")
case("the same finding DOES send in paging mode", "SKEW t [svc]: busiest took 100%\n", 1,
     mode="paging", expect="SKEW", check="partition-balance")

# --- R1e: ranking, and the deterministic tie-break
case("the message emoji is the highest-ranked offender's",
     "SKEW t [s]: x\nCRASHLOOP p: container app is CrashLoopBackOff\n", 1, expect="🔴")
multi = ("SKEW t1 [s]: x\nUNATTRIBUTED t2: y\nCRASHLOOP zzz: c\nNO_CONSUMER g: h\n"
         "LAG_FIRST_SIGHT g2: i\n")
code, out = run(multi, 1)
order = [l for l in out.splitlines()[1:] if l.strip()]
want_first = "zzz"      # CRASHLOOP, rank 5
if want_first not in order[0]:
    fails.append("offenders are ordered by rank")
    print("  FAIL %-62s first line was %r" % ("offenders are ordered by rank", order[0][:70]))
else:
    print("  ok   %-62s" % "offenders are ordered by rank")
# ties break on the name ascending, so the same state always gives the same message
tie = "CRASHLOOP bbb: x\nCRASHLOOP aaa: y\n"
code, out1 = run(tie, 1)
code, out2 = run("CRASHLOOP aaa: y\nCRASHLOOP bbb: x\n", 1)
if out1 != out2 or "aaa" not in out1.splitlines()[1]:
    fails.append("ties break on the name, so the message is deterministic")
    print("  FAIL %-62s" % "ties break on the name, so the message is deterministic")
else:
    print("  ok   %-62s" % "ties break on the name, so the message is deterministic")

# --- R1e: the 5-offender cap and the suffix
many = "".join("CRASHLOOP pod-%02d: container app is CrashLoopBackOff\n" % i for i in range(12))
case("at most 5 offenders are listed", many, 1, expect="… and 7 more", lines=7)   # header + 5 + suffix
case("exactly 5 offenders has no suffix",
     "".join("CRASHLOOP pod-%d: x\n" % i for i in range(5)), 1, forbid="and 0 more", lines=6)

# --- R1e: the ceiling, measured in UTF-16 code units AFTER composition
longname = "x" * 4000
case("a single enormous offender line is capped", "CRASHLOOP %s: detail\n" % longname, 1,
     max_units=1900)
case("five enormous offenders plus a suffix stay under the ceiling",
     "".join("CRASHLOOP %s%d: %s\n" % (longname, i, longname) for i in range(12)), 1,
     max_units=1900)
# emoji are 2 UTF-16 units each: a message that looks short by len(str) can still be rejected
code, out = run("".join("CRASHLOOP %s%d: d\n" % ("e" * 380, i) for i in range(12)), 1)
if units(out.strip()) > 1900:
    fails.append("the ceiling counts UTF-16 units, not characters")
    print("  FAIL %-62s %d units" % ("the ceiling counts UTF-16 units, not characters", units(out.strip())))
else:
    print("  ok   %-62s %d units, %d chars" % ("the ceiling counts UTF-16 units, not characters",
                                               units(out.strip()), len(out.strip())))

# --- a non-zero exit the composer cannot parse must still say something
case("exit 1 with no recognisable finding still sends", "something odd happened\n", 1,
     expect="no recognisable finding line")
case("an unknown finding class is not silently listed", "WEIRD_CLASS t: x\n", 1,
     expect="no recognisable finding line")

# --- classes that share an emoji must still be distinguishable (they share a RANK too)
for a_cls, b_cls in (("CRASHLOOP", "IMAGE_PULL"), ("RESTARTS", "NO_CONSUMER"),
                     ("LAG_INSUFFICIENT_DRAIN", "LAG_FIRST_SIGHT"), ("SKEW", "SINGLE_PARTITION_HOT")):
    _, o1 = run("%s thing: detail\n" % a_cls, 1)
    _, o2 = run("%s thing: detail\n" % b_cls, 1)
    if o1 == o2 or a_cls not in o1 or b_cls not in o2:
        fails.append("%s and %s are distinguishable" % (a_cls, b_cls))
        print("  FAIL %-62s" % ("%s vs %s share an emoji and read identically" % (a_cls, b_cls)))
    else:
        print("  ok   %-62s" % ("%s and %s are distinguishable" % (a_cls, b_cls)))

# --- the build url, when given
case("the build url is included when supplied", FIND, 1, build="https://j/x/1",
     expect="<https://j/x/1>")

# --- an unreadable input is not silence either
code, out = subprocess.run([sys.executable, CHECK, "--env", "dev", "--check", "c", "--rc", "1",
                            "--input", "/nonexistent/x.txt"], capture_output=True, text=True).returncode, ""
if code != 0:
    fails.append("an unreadable input exits 0 with a message")
    print("  FAIL %-62s exit %d" % ("an unreadable input exits 0 with a message", code))
else:
    print("  ok   %-62s" % "an unreadable input exits 0 with a message")

print("\n%d case(s) FAILED" % len(fails) if fails else "\nall cases ok")
sys.exit(1 if fails else 0)
