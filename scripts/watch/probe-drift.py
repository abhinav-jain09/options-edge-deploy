#!/usr/bin/env python3
"""Probe drift (2026-11-20): a live Deployment whose probes differ from the repository manifest that declares them.

  probe-drift.py --env es4 --state DIR [--manifests k8s/es4/services] [--namespace options-edge] [--kubectl kubectl]
                 [--live live.json]   # a saved `kubectl get deployments -o json`, for the tests

WHY THIS EXISTS. rithmic-depth-bridge's manifest declares a PROCESS-ONLY startup probe (`probe.py --alive`: the publisher loop is running, nothing about
the feed), because every restart is another Rithmic login into the same degraded service. The live Deployment carried the FEED-gated probe (`probe.py`)
for two days: release build 1 on 11-18 applied the manifest, its rollout timed out, and the release script's `rollout undo --to-revision` restored the
previous template, old probe included. Nothing noticed, because the manifest was right and last-applied agreed with it. On 11-20 a rollout restart met a
failing first login; the feed-gated startup probe killed the pod after 120 s, each kill was another login, six restarts in 60 minutes and the restart
budget stopped the bridge: depth and MBO silent for an hour. This check is the missing comparison: what the repository says against what is running.

WHAT IS COMPARED. For every Deployment a manifest declares, per container (matched by name), per probe kind (startupProbe, livenessProbe, readinessProbe):
  * the manifest declares the probe: every field the manifest sets (exec.command, httpGet.path/port, periodSeconds, failureThreshold, ...) must equal the
    live value. Fields the manifest leaves out are the cluster's defaults and are not compared;
  * the manifest declares NO such probe but the live container has one: drift (someone added a probe by hand);
  * the manifest declares one and the live container has none: drift.
A manifest Deployment that is not in the cluster is not drift here (it is not deployed, which is another check's business). Images, replicas, env and
everything else are NOT compared: they legitimately move without a manifest commit.

One finding per (Deployment, container, probe kind), so the composer pages once per incident. Exit 0 = no drift, 1 = drift, 2 = the check could not run
(ERROR: line first, so the composer reads the reason).
"""
import argparse
import json
import os
import subprocess
import sys

KINDS = ("startupProbe", "livenessProbe", "readinessProbe")

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True)
ap.add_argument("--state", help="accepted for the stateful-check wrapper; this check keeps no state")
ap.add_argument("--manifests", default=None, help="directory of manifests; default k8s/<env>/services")
ap.add_argument("--namespace", default="options-edge")
ap.add_argument("--kubectl", default="kubectl")
ap.add_argument("--live", default=None, help="saved `kubectl get deployments -o json` instead of asking the cluster")
a = ap.parse_args()

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, "..", ".."))
manifests_dir = a.manifests or os.path.join(REPO, "k8s", a.env, "services")


def die(msg):
    print("ERROR: " + msg)
    sys.exit(2)


try:
    import yaml
except ImportError:
    die("PyYAML is not installed for this python3; the manifests cannot be read")

if not os.path.isdir(manifests_dir):
    die("manifest directory %s does not exist" % manifests_dir)

declared = {}   # Deployment name -> {container name -> {probe kind -> declared probe}}
for fn in sorted(os.listdir(manifests_dir)):
    if not fn.endswith((".yaml", ".yml")):
        continue
    path = os.path.join(manifests_dir, fn)
    try:
        docs = list(yaml.safe_load_all(open(path)))
    except Exception as e:
        die("cannot parse %s: %s" % (path, str(e)[:200]))
    for d in docs:
        if not isinstance(d, dict) or d.get("kind") != "Deployment":
            continue
        name = (d.get("metadata") or {}).get("name")
        containers = (((d.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
        if not name:
            continue
        declared[name] = {c.get("name"): {k: c.get(k) for k in KINDS} for c in containers}

if a.live:
    try:
        live_doc = json.load(open(a.live))
    except Exception as e:
        die("cannot read %s: %s" % (a.live, str(e)[:200]))
else:
    try:
        r = subprocess.run([a.kubectl, "-n", a.namespace, "get", "deployments", "-o", "json"], capture_output=True, text=True)
    except OSError as e:
        die("cannot run %s: %s" % (a.kubectl, str(e)[:200]))
    if r.returncode != 0:
        die("kubectl -n %s get deployments failed: %s" % (a.namespace, " ".join(r.stderr.split())[:300]))
    try:
        live_doc = json.loads(r.stdout)
    except ValueError as e:
        die("kubectl returned unreadable JSON: %s" % str(e)[:200])

live = {}
for d in live_doc.get("items", []):
    cs = (((d.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
    live[d["metadata"]["name"]] = {c.get("name"): {k: c.get(k) for k in KINDS} for c in cs}


def flatten(prefix, value, out):
    if isinstance(value, dict):
        for k, v in value.items():
            flatten(prefix + "." + k if prefix else k, v, out)
    else:
        out[prefix] = value


def drift_of(repo_probe, live_probe):
    """Human lines for every field the repository declares that the live probe does not carry, or a whole-probe difference."""
    if repo_probe is None and live_probe is None:
        return []
    if repo_probe is None:
        return ["the live container has one (%s) and the manifest declares none" % json.dumps(live_probe, sort_keys=True)]
    if live_probe is None:
        return ["the manifest declares one (%s) and the live container has none" % json.dumps(repo_probe, sort_keys=True)]
    want, have = {}, {}
    flatten("", repo_probe, want)
    flatten("", live_probe, have)
    return ["%s live %s, repo %s" % (field, json.dumps(have.get(field)), json.dumps(value))
            for field, value in sorted(want.items()) if have.get(field) != value]


findings = []
for dep, containers in sorted(declared.items()):
    if dep not in live:
        continue
    for cname, probes in sorted(containers.items()):
        live_container = live[dep].get(cname)
        if live_container is None:
            continue
        for kind in KINDS:
            for line in drift_of(probes[kind], live_container[kind]):
                findings.append("PROBE_DRIFT Deployment/%s: container %s %s: %s" % (dep, cname, kind, line))

for f in findings:
    print(f)
print("probe-drift %s: %d manifest Deployment(s), %d in the cluster, %d finding(s)" % (
    a.env, len(declared), sum(1 for d in declared if d in live), len(findings)))
sys.exit(1 if findings else 0)
