#!/usr/bin/env python3
"""Negative controls for kafka-broker.py. The controller-log lines are the real ones from the 2026-11-18 06:19 CET incident (dev broker, .102)."""
import json, os, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "kafka-broker.py")
fails = []

CALM = "NOW 2026-11-18 06:25:00\nSWAP total = 7168.00M  used = 5829.06M  free = 1338.94M  (encrypted)\nNCPU 16\nLOAD { 30.25 25.04 21.87 }\n"
INCIDENT = (
    "[2026-11-18 06:19:18,328] ERROR [QuorumController id=1] Exceptionally slow controller event maybeFenceStaleBroker(242602809) took 6352 ms. (org.apache.kafka.controller.EventPerformanceMonitor)\n"
    "[2026-11-18 06:19:39,673] INFO [QuorumController id=1] handleBrokerFenced: changing 6894 partition(s) (org.apache.kafka.controller.ReplicationControlManager)\n"
    "[2026-11-18 06:19:39,718] ERROR [QuorumController id=1] Exceptionally slow controller event maybeFenceStaleBroker(1571782564) took 10774 ms. (org.apache.kafka.controller.EventPerformanceMonitor)\n"
    "[2026-11-18 06:19:41,728] INFO [QuorumController id=1] handleBrokerUnfenced: changing 6894 partition(s) (org.apache.kafka.controller.ReplicationControlManager)\n"
)


def run(host_text, meta=120, extra=()):
    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "f.json")
        json.dump({"host_text": host_text, "metadata_ms": meta}, open(f, "w"))
        r = subprocess.run([sys.executable, CHECK, "--env", "dev", "--fixture", f, *extra], capture_output=True, text=True, timeout=30)
        return r.returncode, r.stdout + r.stderr


def case(name, want_rc, host_text, expect=None, forbid=None, meta=120, extra=()):
    rc, out = run(host_text, meta, extra)
    why = []
    if rc != want_rc: why.append("exit %d wanted %d" % (rc, want_rc))
    if expect and expect not in out: why.append("lacks %r" % expect)
    if forbid and forbid in out: why.append("contains %r" % forbid)
    print(("  ok   " if not why else "  FAIL ") + name + ("" if not why else "  " + "; ".join(why) + "\n" + out[:400]))
    if why: fails.append(name)


print("kafka-broker.py negative controls")
case("a calm broker and host is clean", 0, CALM + "END\n", expect="0 finding(s)")
case("the 11-18 fence 6 minutes ago pages BROKER_FENCED with the partition count and worst event", 1, CALM + INCIDENT + "END\n",
     expect="BROKER_FENCED kafka-dev-broker: the broker fenced itself 1 time(s)")
case("... and names 6894 partitions and 10774 ms", 1, CALM + INCIDENT + "END\n", expect="6894 partition(s) re-elected, worst controller event 10774 ms")
case("the unfence line alone is NOT a fence", 0, CALM + "[2026-11-18 06:19:41,728] INFO [QuorumController id=1] handleBrokerUnfenced: changing 6894 partition(s) (x)\nEND\n", forbid="BROKER_FENCED")
case("the same fence 90 minutes later is outside the window (the incident resolves)", 0, CALM.replace("06:25:00", "07:50:00") + INCIDENT + "END\n", forbid="BROKER_FENCED")
case("a slow controller event with no fence is BROKER_SLOW, not FENCED", 1,
     CALM + "[2026-11-18 06:19:18,328] ERROR [QuorumController id=1] Exceptionally slow controller event maybeFenceStaleBroker(1) took 6352 ms. (x)\nEND\n",
     expect="BROKER_SLOW kafka-dev-broker: 1 controller event(s)", forbid="BROKER_FENCED")
case("a slow event under the limit is ignored", 0, CALM + "[2026-11-18 06:19:18,328] ERROR [QuorumController id=1] Exceptionally slow controller event x(1) took 900 ms. (x)\nEND\n", forbid="BROKER_SLOW")
case("a slow metadata round trip pages", 1, CALM + "END\n", expect="BROKER_SLOW kafka-dev-broker: a metadata round trip took 7000 ms", meta=7000)
case("load of 4.5 per core pages the host", 1, CALM.replace("{ 30.25 25.04 21.87 }", "{ 80.0 75.0 72.0 }") + "END\n", expect="BROKER_HOST_LOAD kafka-dev-host: 15-minute load 72.0 on 16 cores")
case("the 11-18 load (1.4 per core) is NOT paged", 0, CALM + "END\n", forbid="BROKER_HOST_LOAD")
case("30 GiB of swap pages", 1, CALM.replace("used = 5829.06M", "used = 30.5G") + "END\n", expect="BROKER_HOST_SWAP kafka-dev-host: 30.5 GiB")
case("an answer with no END marker is 'could not check', never clean", 2, CALM + INCIDENT, expect="no END marker")
case("a missing load figure is 'could not check'", 2, CALM.replace("LOAD { 30.25 25.04 21.87 }\n", "") + "END\n", expect="missing load15")
case("a missing log directory is 'could not check'", 2, CALM + "NOLOGDIR\nEND\n", expect="log directory does not exist")

# live path: nothing is faked, the dependency itself is made to fail
r = subprocess.run([sys.executable, CHECK, "--env", "dev", "--kcat", "/usr/bin/false", "--bootstrap", "127.0.0.1:1"], capture_output=True, text=True, timeout=60)
ok = r.returncode == 2 and "metadata round trip" in r.stdout
print(("  ok   " if ok else "  FAIL ") + "a failing kcat is exit 2 (could not check)")
if not ok: fails.append("kcat failure"); print(r.stdout[:300], r.stderr[:300])

print("FAILED: %s" % fails if fails else "all kafka-broker controls passed")
sys.exit(1 if fails else 0)
