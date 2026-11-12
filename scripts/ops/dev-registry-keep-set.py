#!/usr/bin/env python3
"""Which image digests the dev registry prune must KEEP, given the cluster's workloads.

Split out of dev-registry-prune.sh so it can be tested without a cluster, a registry or Docker.

THE DEFECT THIS EXISTS TO CLOSE. The prune used to read

    kubectl get deploy -o jsonpath='{.spec.template.spec.containers[0].image}'

-- the FIRST container of each Deployment. options-edge-web also has an init container,
`publish-public-assets`, pinned to its own digest. That digest was in no keep set, so the prune
deleted its manifest and the garbage collector swept its blobs, and the next time the pod was
recreated it died with ImagePullBackOff on an image the cluster was actively using. The same hole
covers every sidecar and every workload kind that is not a Deployment.

Read every container of every workload: initContainers and containers, Deployments, StatefulSets,
DaemonSets, Jobs and CronJobs alike. A digest that anything references is live.
"""
from __future__ import annotations
import json, re, sys

DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def pod_specs(obj):
    """Every pod spec in a kubectl list, whatever the workload kind.

    CronJob nests one level deeper (spec.jobTemplate.spec.template.spec), which is exactly the kind
    of shape a jsonpath one-liner misses silently.
    """
    for item in (obj or {}).get("items") or []:
        spec = (item or {}).get("spec") or {}
        tmpl = (spec.get("template") or {}).get("spec")
        if tmpl:
            yield tmpl
        job = ((spec.get("jobTemplate") or {}).get("spec") or {}).get("template") or {}
        if job.get("spec"):
            yield job["spec"]


def images(obj):
    """Every image reference in use, from EVERY container -- init containers included."""
    out = []
    for spec in pod_specs(obj):
        for key in ("initContainers", "containers", "ephemeralContainers"):
            for c in spec.get(key) or []:
                img = (c or {}).get("image")
                if img:
                    out.append(img)
    return out


def keep_digests(obj):
    """The digests that must survive a prune. Order-stable for a readable diff."""
    seen, out = set(), []
    for img in images(obj):
        m = DIGEST.search(img)
        if m and m.group(0) not in seen:
            seen.add(m.group(0))
            out.append(m.group(0))
    return out


if __name__ == "__main__":
    data = json.load(sys.stdin)
    for d in keep_digests(data):
        print(d)
