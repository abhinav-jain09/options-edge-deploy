#!/usr/bin/env python3
"""Host-load check: a node that is NotReady, under kubelet pressure, or running hot on memory or CPU.

  host-load.py --nodes nodes.json [--metrics metrics.json | --summary-dir DIR] [--mem-pct 85] [--cpu-pct 90]

WHY. On 2026-11-17 the dev VM ran at ~64 GB of 77 with the host's 16 cores idle 0.4% and swap nearly full; the kernel killed gex, the gateway and
level-absorption, and NOTHING paged because no check looked at the host. The kills are caught by oom.py after the fact; this check is the cause.

Conditions (Ready, MemoryPressure, DiskPressure, PIDPressure) are ALWAYS read - they need only `get nodes`. Usage needs a source: the metrics API
(--metrics, the JSON of /apis/metrics.k8s.io/v1beta1/nodes) or, for a node the metrics API does not cover, a kubelet stats summary (--summary-dir, one <node>.json per node; the production wrapper does not fetch these - nodes/proxy is not granted). With neither,
the check says so in its output line - it does not claim the host is cool.

Fail closed: unparsable nodes, no nodes at all, or a node without a status is an error. Exit 0 = healthy, 1 = a finding, 2 = could not run.
"""
import argparse
import json
import os
import re
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--nodes", required=True, help="kubectl get nodes -o json")
ap.add_argument("--metrics", default=None, help="kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes")
ap.add_argument("--summary-dir", default=None, help="directory of <node>.json kubelet stats summaries")
ap.add_argument("--mem-pct", type=float, default=85.0, help="working set / allocatable memory at or above this is reported")
ap.add_argument("--cpu-pct", type=float, default=90.0, help="cpu usage / allocatable cpu at or above this is reported")
a = ap.parse_args()


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


UNITS = {"n": 1e-9, "u": 1e-6, "m": 1e-3, "": 1.0, "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15,
         "Ki": 2.0 ** 10, "Mi": 2.0 ** 20, "Gi": 2.0 ** 30, "Ti": 2.0 ** 40, "Pi": 2.0 ** 50}


def qty(v):
    """A Kubernetes quantity ('4', '4000m', '123456n', '77Gi', '8000000Ki') as a float."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]{0,2})", str(v).strip())
    if not m or m.group(2) not in UNITS:
        raise ValueError("unparsable quantity %r" % (v,))
    return float(m.group(1)) * UNITS[m.group(2)]


try:
    nodes = json.load(open(a.nodes))["items"]
except Exception as e:  # noqa: BLE001
    die("cannot read kubectl output: %s" % e)
if not isinstance(nodes, list) or not nodes:
    die("no nodes in the input - refusing to report 'healthy' on an empty answer")
if not (0 < a.mem_pct <= 100 and 0 < a.cpu_pct <= 100):
    die("--mem-pct and --cpu-pct must be in (0, 100]")

usage, source = {}, None          # node -> (cpu cores, memory bytes)
try:
    if a.metrics:
        for it in json.load(open(a.metrics))["items"]:
            usage[it["metadata"]["name"]] = (qty(it["usage"]["cpu"]), qty(it["usage"]["memory"]))
        source = "the metrics API"
    elif a.summary_dir and os.path.isdir(a.summary_dir):
        for fn in sorted(os.listdir(a.summary_dir)):
            if not fn.endswith(".json"):
                continue
            n = json.load(open(os.path.join(a.summary_dir, fn)))["node"]
            usage[n["nodeName"]] = (float(n["cpu"]["usageNanoCores"]) * 1e-9, float(n["memory"]["workingSetBytes"]))
        source = "the kubelet stats summary" if usage else None
except Exception as e:  # noqa: BLE001
    die("cannot read the usage data: %s" % e)

findings, lines, unmeasured = [], [], []
for node in nodes:
    name = (node.get("metadata") or {}).get("name") or "<unnamed>"
    st = node.get("status")
    if not isinstance(st, dict):
        die("node %s has no status" % name)
    conds = {c.get("type"): c for c in st.get("conditions") or []}
    ready = conds.get("Ready")
    if ready is None:
        die("node %s has no Ready condition - cannot judge it" % name)
    if ready.get("status") != "True":
        findings.append((name, "NODE_NOT_READY", "Ready=%s (%s) %s" % (ready.get("status"), ready.get("reason"), (ready.get("message") or "")[:100])))
    for t in ("MemoryPressure", "DiskPressure", "PIDPressure"):
        c = conds.get(t)
        if c and c.get("status") == "True":
            findings.append((name, "NODE_PRESSURE", "%s: %s" % (t, (c.get("message") or c.get("reason") or "")[:120])))
    if name in usage:
        try:
            cpu_alloc, mem_alloc = qty(st["allocatable"]["cpu"]), qty(st["allocatable"]["memory"])
        except Exception as e:  # noqa: BLE001
            die("node %s has no usable allocatable (%s)" % (name, e))
        cpu_pct, mem_pct = 100.0 * usage[name][0] / cpu_alloc, 100.0 * usage[name][1] / mem_alloc
        lines.append("%s cpu %.0f%% mem %.0f%%" % (name, cpu_pct, mem_pct))
        if mem_pct >= a.mem_pct:
            findings.append((name, "HOST_MEMORY", "memory at %.0f%% of allocatable (%.1f of %.1f GiB; threshold %.0f%%)"
                             % (mem_pct, usage[name][1] / 2.0 ** 30, mem_alloc / 2.0 ** 30, a.mem_pct)))
        if cpu_pct >= a.cpu_pct:
            findings.append((name, "HOST_CPU", "cpu at %.0f%% of allocatable (%.1f of %.1f cores; threshold %.0f%%)"
                             % (cpu_pct, usage[name][0], cpu_alloc, a.cpu_pct)))
    else:
        unmeasured.append(name)

for name, cls, detail in sorted(findings):
    print("%s %s: %s" % (cls, name, detail))
if lines:
    print("host-load: %d node(s) checked; usage from %s: %s;%s %d finding(s)"
          % (len(nodes), source, "; ".join(lines),
             (" CONDITIONS ONLY for %s (no usage reported);" % ", ".join(unmeasured)) if unmeasured else "", len(findings)))
else:
    print("host-load: %d node(s) checked on CONDITIONS ONLY - no usage source answered (metrics API and kubelet summary both unavailable), so memory and cpu were NOT measured; %d finding(s)"
          % (len(nodes), len(findings)))
sys.exit(1 if findings else 0)
