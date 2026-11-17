#!/usr/bin/env python3
"""Negative controls for host-load.py: every verdict is produced from synthetic kubectl documents."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "host-load.py")
fails = []


def node(name="desktop-control-plane", ready="True", pressure=(), cpu="16", mem="80000000Ki", conds=None):
    c = conds if conds is not None else [{"type": "Ready", "status": ready, "reason": "KubeletReady" if ready == "True" else "KubeletNotReady", "message": "x"}]
    for t in ("MemoryPressure", "DiskPressure", "PIDPressure"):
        c.append({"type": t, "status": "True" if t in pressure else "False", "message": "%s detail" % t, "reason": t})
    return {"metadata": {"name": name}, "status": {"conditions": c, "allocatable": {"cpu": cpu, "memory": mem}}}


def metrics(name, cpu, mem):
    return {"metadata": {"name": name}, "usage": {"cpu": cpu, "memory": mem}}


def run(nodes, *args, metr=None, summ=None, raw=None):
    d = tempfile.mkdtemp(prefix="hl-")
    n = os.path.join(d, "nodes.json")
    open(n, "w").write(raw if raw is not None else json.dumps({"items": nodes}))
    cmd = [sys.executable, CHECK, "--nodes", n]
    if metr is not None:
        m = os.path.join(d, "metrics.json")
        json.dump({"items": metr}, open(m, "w"))
        cmd += ["--metrics", m]
    if summ is not None:
        sd = os.path.join(d, "summary")
        os.mkdir(sd)
        for name, doc in summ.items():
            json.dump(doc, open(os.path.join(sd, name + ".json"), "w"))
        cmd += ["--summary-dir", sd]
    r = subprocess.run(cmd + list(args), capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout + r.stderr


def case(name, want_rc, nodes, *args, expect=None, forbid=None, **kw):
    rc, out = run(nodes, *args, **kw)
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


N = "desktop-control-plane"
print("host-load.py negative controls")

case("a cool, ready node is clean", 0, [node()], metr=[metrics(N, "4", "20000000Ki")], forbid="HOST_", expect="cpu 25% mem 25%")
case("memory over the default 85% is reported (72 of 80 GB)", 1, [node(mem="80000000Ki")],
     metr=[metrics(N, "1", "72000000Ki")], expect="HOST_MEMORY desktop-control-plane: memory at 90% of allocatable")
case("memory just under the threshold is not", 0, [node(mem="80000000Ki")], metr=[metrics(N, "1", "67000000Ki")], forbid="HOST_MEMORY")
case("cpu at or over the threshold is reported (millicores and nanocores parse)", 1, [node(cpu="16")],
     metr=[metrics(N, "15500m", "1Gi")], expect="HOST_CPU desktop-control-plane: cpu at 97%")
case("nanocore cpu quantities parse", 1, [node(cpu="4")], metr=[metrics(N, "3900000000n", "1Gi")], expect="HOST_CPU")
case("a NotReady node is reported even with no usage source", 1, [node(ready="False")], expect="NODE_NOT_READY desktop-control-plane: Ready=False")
case("an Unknown Ready (kubelet stopped posting) is reported", 1, [node(ready="Unknown")], expect="NODE_NOT_READY")
case("MemoryPressure is reported", 1, [node(pressure=("MemoryPressure",))], expect="NODE_PRESSURE desktop-control-plane: MemoryPressure")
case("DiskPressure and PIDPressure are reported", 1, [node(pressure=("DiskPressure", "PIDPressure"))], expect="DiskPressure")
case("with NO usage source it says memory and cpu were NOT measured", 0, [node()], expect="CONDITIONS ONLY")
case("the kubelet stats summary is a usage source too", 1, [node(cpu="16", mem="80000000Ki")],
     summ={N: {"node": {"nodeName": N, "cpu": {"usageNanoCores": 15900000000}, "memory": {"workingSetBytes": 1000000}}}}, expect="HOST_CPU")
case("a node the metrics API does not list is checked on conditions only", 0, [node("other")], metr=[metrics(N, "1", "1Gi")],
     expect="CONDITIONS ONLY")
case("--mem-pct can be tightened", 1, [node(mem="80000000Ki")], "--mem-pct", "50", metr=[metrics(N, "1", "45000000Ki")], expect="HOST_MEMORY")
case("--cpu-pct 0 is refused", 2, [node()], "--cpu-pct", "0", expect="must be in (0, 100]")
case("an empty node list is an error, not 'healthy'", 2, [], expect="refusing to report 'healthy'")
case("a node with no status is an error", 2, [{"metadata": {"name": "ghost"}}], expect="has no status")
case("a node with no Ready condition is an error", 2, [node(conds=[])], expect="no Ready condition")
case("garbage input is an error", 2, [], raw="not json", expect="cannot read kubectl output")
case("an unparsable quantity is an error", 2, [node()], metr=[metrics(N, "lots", "1Gi")], expect="cannot read the usage data")
case("usage for a node with no allocatable is an error", 2,
     [{"metadata": {"name": N}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}],
     metr=[metrics(N, "1", "1Gi")], expect="no usable allocatable")

rc = subprocess.run([sys.executable, CHECK, "--nodes", "/nonexistent/nodes.json"], capture_output=True, text=True).returncode
print("  %s %-62s exit %d" % ("ok  " if rc == 2 else "FAIL", "a missing input file is an error", rc))
if rc != 2:
    fails.append("missing input")

print("\n%d case(s) FAILED" % len(fails) if fails else "\nall cases ok")
sys.exit(1 if fails else 0)
