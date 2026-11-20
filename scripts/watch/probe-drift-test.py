#!/usr/bin/env python3
"""Negative controls for probe-drift.py. Each case is a manifest directory plus a saved `kubectl get deployments -o json`.

  python3 scripts/watch/probe-drift-test.py

The headline case is the 2026-11-20 one: the manifest says the startup probe is `probe.py --alive` and the live Deployment still runs `probe.py`.
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "probe-drift.py")
fails = []

MANIFEST = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bridge
spec:
  template:
    spec:
      containers:
      - name: collector
        image: PLACEHOLDER
        readinessProbe:
          exec: {command: [python3, /app/probe.py]}
          periodSeconds: 5
        startupProbe:
          exec: {command: [python3, /app/probe.py, --alive]}
          periodSeconds: 5
          failureThreshold: 24
        livenessProbe:
          exec: {command: [python3, /app/probe.py, --alive]}
          periodSeconds: 10
"""


def live_deployment(startup=("python3", "/app/probe.py", "--alive"), liveness=True, readiness=True, extra_probe=None, name="bridge"):
    c = {"name": "collector", "image": "reg/bridge@sha256:abc"}
    if startup is not None:
        c["startupProbe"] = {"exec": {"command": list(startup)}, "periodSeconds": 5, "failureThreshold": 24, "successThreshold": 1, "timeoutSeconds": 1}
    if liveness:
        c["livenessProbe"] = {"exec": {"command": ["python3", "/app/probe.py", "--alive"]}, "periodSeconds": 10, "failureThreshold": 3, "timeoutSeconds": 1}
    if readiness:
        c["readinessProbe"] = {"exec": {"command": ["python3", "/app/probe.py"]}, "periodSeconds": 5, "initialDelaySeconds": 5}
    if extra_probe:
        c.update(extra_probe)
    return {"metadata": {"name": name}, "spec": {"template": {"spec": {"containers": [c]}}}}


def run(items, manifest=MANIFEST, kubectl=None):
    with tempfile.TemporaryDirectory() as d:
        os.makedirs(os.path.join(d, "m"))
        open(os.path.join(d, "m", "bridge.yaml"), "w").write(manifest)
        live = os.path.join(d, "live.json")
        json.dump({"items": items}, open(live, "w"))
        argv = [sys.executable, CHECK, "--env", "es4", "--manifests", os.path.join(d, "m")]
        argv += ["--live", live] if kubectl is None else ["--kubectl", kubectl]
        r = subprocess.run(argv, capture_output=True, text=True)
        return r.returncode, r.stdout


def check(name, cond, detail=""):
    if not cond:
        fails.append("%s %s" % (name, detail))
        print("FAIL", name, detail)
    else:
        print("ok  ", name)


rc, out = run([live_deployment()])
check("a live Deployment that matches the manifest is clean (defaults the manifest does not declare are ignored)", rc == 0 and "PROBE_DRIFT" not in out, out)

rc, out = run([live_deployment(startup=("python3", "/app/probe.py"))])
check("the 11-20 case: the live startup probe lost --alive", rc == 1 and "PROBE_DRIFT Deployment/bridge: container collector startupProbe: exec.command" in out, out)
check("... and says which command is live and which the repository wants", '"/app/probe.py"]' in out and '"--alive"]' in out, out)

rc, out = run([live_deployment(liveness=False)])
check("a probe the manifest declares but the live container lacks", rc == 1 and "livenessProbe" in out and "has none" in out, out)

rc, out = run([live_deployment(extra_probe={"lifecycle": {}})])
check("fields outside the probes are not compared", rc == 0, out)

manifest_without_startup = MANIFEST.replace(
    "        startupProbe:\n          exec: {command: [python3, /app/probe.py, --alive]}\n          periodSeconds: 5\n          failureThreshold: 24\n", "")
rc, out = run([live_deployment()], manifest=manifest_without_startup)
check("a probe added by hand that the manifest does not declare", rc == 1 and "startupProbe" in out and "declares none" in out, out)

live = live_deployment()
live["spec"]["template"]["spec"]["containers"][0]["startupProbe"]["failureThreshold"] = 3
rc, out = run([live])
check("a changed threshold is drift", rc == 1 and "failureThreshold" in out, out)

rc, out = run([])
check("a manifest Deployment that is not in the cluster is not drift", rc == 0, out)

rc, out = run([live_deployment(startup=("python3", "/app/probe.py")), live_deployment(startup=("python3", "/app/probe.py"), name="other")])
check("a live Deployment the manifests do not declare is ignored", out.count("PROBE_DRIFT") == 1, out)

rc, out = run([], kubectl="/nonexistent/kubectl")
check("an unreachable cluster is exit 2 with an ERROR: line, never 'no drift'", rc == 2 and out.startswith("ERROR:"), out)

if fails:
    print("\n%d FAILED" % len(fails))
    sys.exit(1)
print("\nall probe-drift controls passed")
