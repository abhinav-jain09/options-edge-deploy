#!/usr/bin/env python3
"""Jenkins-only rollout of the existing single ES feed, with a verified image digest."""
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.request


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def main():
    if os.environ.get("ACTION") != "refresh-es-replay":
        raise SystemExit("refresh-es-replay action required")
    if os.environ.get("DEPLOY_DRY_RUN") != "false":
        raise SystemExit("refresh-es-replay requires DEPLOY_DRY_RUN=false")
    kube = os.environ["ES4_KUBECONFIG"]
    prod = os.environ["PROD_KUBECONFIG"]
    run("bash", "scripts/es4/assert-cluster-identity.sh", kube, "es4", os.environ["ES4_NS_UID"])
    run("bash", "scripts/es4/assert-cluster-identity.sh", prod, "prod", os.environ["PROD_NS_UID"])
    command = ["kubectl", "--kubeconfig", kube, "-n", "options-edge"]
    deployment = json.loads(run(*command, "get", "deployment", "es-feed", "-o", "json"))
    if deployment["spec"]["replicas"] != 1 or deployment["spec"]["strategy"]["type"] != "Recreate":
        raise SystemExit("Only the active singleton Recreate feed may be refreshed")
    run("bash", "scripts/es4/assert-es-feed-exclusive.sh", kube, prod, "es4")
    digest = json.loads(Path(".jenkins-tmp/es-replay-image.json").read_text())["containerimage.digest"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise SystemExit("Missing immutable image digest")
    registry = os.environ["REGISTRY"]
    image = f"{registry}/options-edge-databento-feed@{digest}"
    request = urllib.request.Request(
        f"http://{registry}/v2/options-edge-databento-feed/manifests/{digest}",
        headers={"Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        index = json.load(response)
    children = [m["digest"] for m in index.get("manifests", [])
                if m.get("platform", {}).get("os") == "linux"
                and m.get("platform", {}).get("architecture") == "amd64"]
    if len(children) != 1:
        raise SystemExit("Expected one Linux amd64 image in build result")
    print("Previous image:", deployment["spec"]["template"]["spec"]["containers"][0]["image"], flush=True)
    # This action never applies the parked replicas:0 manifest or changes other workloads.
    run(*command, "apply", "-f", "k8s/es4/es4-feed-config.yaml")
    patch = {"metadata": {"resourceVersion": deployment["metadata"]["resourceVersion"]},
             "spec": {"template": {"spec": {"containers": [{"name": "es-feed", "image": image,
                 "env": [{"name": "DATABENTO_ES_TRADES_REPLAY_START_MINUTES", "value": "60"}]}]}}}}
    run(*command, "patch", "deployment", "es-feed", "--type=strategic", "-p", json.dumps(patch))
    run(*command, "rollout", "status", "deployment/es-feed", "--timeout=600s")
    actual = json.loads(run(*command, "get", "deployment", "es-feed", "-o", "json"))
    if actual["spec"]["template"]["spec"]["containers"][0]["image"] != image:
        raise SystemExit("Deployment image differs from built image")
    pods = json.loads(run(*command, "get", "pods", "-l", "app.kubernetes.io/name=es-feed", "-o", "json"))
    active = [p for p in pods["items"] if not p["metadata"].get("deletionTimestamp")]
    if len(active) != 1:
        raise SystemExit("Expected exactly one active ES feed pod")
    status = next(c for c in active[0]["status"]["containerStatuses"] if c["name"] == "es-feed")
    if not status["ready"] or status["restartCount"] != 0:
        raise SystemExit("New ES feed is not ready with zero restarts")
    if not any(status["imageID"].endswith(d) for d in [digest, *children]):
        raise SystemExit("Running ES feed image differs from verified image index")
    print(json.dumps({"image": image, "pod": active[0]["metadata"]["name"], "ready": True}))


if __name__ == "__main__":
    main()
