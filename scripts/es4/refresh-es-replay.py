#!/usr/bin/env python3
"""Jenkins-only refresh of the existing singleton ES feed, recovering its old template on failure."""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.request


def run(*args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.PIPE,
            timeout=620 if "rollout" in args else 30).strip()
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"{args[0]} failed (exit {exc.returncode}); inspect the target read-only") from None
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{args[0]} command timed out") from None


def registry_json(registry, kind, digest):
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise RuntimeError("Missing immutable image digest")
    request = urllib.request.Request(
        f"http://{registry}/v2/options-edge-databento-feed/{kind}/{digest}",
        headers={"Accept": ", ".join([
            "application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"])},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
    if "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        raise RuntimeError("Registry content differs from requested digest")
    return json.loads(body)


def promote_verified_image(registry, digest):
    """Promote the exact verified manifest bytes only after replay and readiness pass."""
    accept = ', '.join(['application/vnd.oci.image.index.v1+json',
        'application/vnd.docker.distribution.manifest.list.v2+json',
        'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'])
    url = f'http://{registry}/v2/options-edge-databento-feed/manifests/'
    with urllib.request.urlopen(urllib.request.Request(url+digest, headers={'Accept':accept}), timeout=30) as response:
        body = response.read()
        content_type = response.headers['Content-Type']
    if 'sha256:' + hashlib.sha256(body).hexdigest() != digest:
        raise RuntimeError('Promotion manifest digest mismatch')
    request = urllib.request.Request(url+'es-replay', data=body, method='PUT', headers={'Content-Type':content_type})
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 201: raise RuntimeError('Registry promotion failed')
    with urllib.request.urlopen(urllib.request.Request(url+'es-replay', headers={'Accept':accept}), timeout=30) as response:
        if 'sha256:' + hashlib.sha256(response.read()).hexdigest() != digest:
            raise RuntimeError('Registry promotion verification failed')


def replay_result(log):
    reports = re.findall(r'ES replay caught up: (\{[^\n]+\})', log)
    for raw in reports:
        report = json.loads(raw)
        expected, matched = report.get('expected_unique'), report.get('matched_unique')
        if report.get('verified') is not True or type(expected) is not int or type(matched) is not int or expected != matched:
            raise RuntimeError('ES replay identity verification failed')
    return json.loads(reports[-1]) if reports else None


def verify_image(registry, digest, source, architecture="amd64"):
    manifest = registry_json(registry, "manifests", digest)
    accepted = [digest]
    if "manifests" in manifest:
        children = [m["digest"] for m in manifest["manifests"]
                    if m.get("platform", {}).get("os") == "linux"
                    and m.get("platform", {}).get("architecture") == architecture]
        if len(children) != 1:
            raise RuntimeError("Expected one Linux target-platform image in build result")
        accepted.extend(children)
        manifest = registry_json(registry, "manifests", children[0])
    config = registry_json(registry, "blobs", manifest["config"]["digest"])
    if config.get("os") != "linux" or config.get("architecture") != architecture:
        raise RuntimeError(f"Built image must be Linux {architecture}")
    if config.get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision") != source:
        raise RuntimeError("Image source differs from FEED_PERMITTED_SHA")
    return accepted


def container(template, name="es-feed"):
    return next(c for c in template["spec"]["containers"] if c["name"] == name)


def verify_pod(command, image, accepted, name="es-feed", workload="es-feed", selector=None):
    pods = json.loads(run(*command, "get", "pods", "-l", selector or f"app.kubernetes.io/name={workload}", "-o", "json"))
    active = [p for p in pods["items"] if not p["metadata"].get("deletionTimestamp")]
    if len(active) != 1:
        raise RuntimeError("Expected exactly one active ES feed pod")
    pod = active[0]
    status = next(c for c in pod["status"]["containerStatuses"] if c["name"] == name)
    if not status["ready"] or status["restartCount"] != 0:
        raise RuntimeError("New ES feed is not ready with zero restarts")
    if container({"spec": pod["spec"]}, name)["image"] != image or not any(status["imageID"].endswith(d) for d in accepted):
        raise RuntimeError("Running ES feed image differs from verified image")
    env = {e["name"]: e.get("value") for e in container({"spec": pod["spec"]}, name).get("env", [])}
    if env.get("DATABENTO_ES_TRADES_REPLAY_START_MINUTES") != "60":
        raise RuntimeError("Pod does not explicitly set ES replay to 60 minutes")
    return pod["metadata"]["name"]


def downstream_state(command, bootstrap, topic):
    pods = json.loads(run(*command, "get", "pods", "-o", "json"))["items"]
    names = re.compile(r"(indicator|context-tape|close-direction|tape-zones|es-cvd|es-footprint|es-amt|linearizer|es-aggressor)")
    state = {}
    for pod in pods:
        if pod["metadata"].get("deletionTimestamp") or not names.search(pod["metadata"]["name"]): continue
        if not any(o.get("kind")=="ReplicaSet" for o in pod["metadata"].get("ownerReferences",[])): continue
        statuses=pod.get("status",{}).get("containerStatuses",[])
        if not statuses or not all(c.get("ready") for c in statuses): raise RuntimeError("Downstream consumer is not ready")
        state[pod["metadata"]["uid"]] = sum(c["restartCount"] for c in statuses)
    lag_binary = Path('.jenkins-tmp/feed/rust/target/debug/examples/es-replay-lag').resolve()
    lag=json.loads(run(str(lag_binary),bootstrap,topic))
    return {"pods":state,"groups":lag["groups"],"highs":lag["highs"]}


def downstream_healthy(before, after, fence):
    if before["pods"] != after["pods"]: raise RuntimeError("Downstream consumer pod or restart count changed")
    required=set(before["groups"]) | set(after["groups"])
    return all(name in after["groups"] and after["groups"][name]["state"]=="Stable"
               and all(after["groups"][name]["offsets"].get(p) is not None
                       and after["groups"][name]["offsets"][p]>=offset for p,offset in fence.items())
               for name in required)


def preflight_downstream(command, bootstrap, topic):
    before=downstream_state(command,bootstrap,topic)
    fence=dict(before["highs"])
    deadline=time.monotonic()+60
    after=before
    while True:
        if any(g["state"]!="Stable" for g in after["groups"].values()):
            raise RuntimeError("Pre-existing unstable ES consumer; no feed mutation performed")
        if downstream_healthy(before,after,fence):return after
        if time.monotonic()>=deadline:
            raise RuntimeError("Pre-existing ES backlog did not advance within sixty seconds; no feed mutation performed")
        time.sleep(5)
        after=downstream_state(command,bootstrap,topic)


def verify_downstream_fence(before, command, bootstrap, topic, observe=None):
    # Phase B: the feed has passed verification. Never recreate it for a consumer problem.
    after=downstream_state(command,bootstrap,topic)
    fence=dict(after["highs"])
    for attempt in range(40):
        if observe is not None: observe()
        if downstream_healthy(before,after,fence):return fence
        time.sleep(15)
        after=downstream_state(command,bootstrap,topic)
    raise RuntimeError("Downstream ES consumers did not pass the replay fence within ten minutes")


def observe_es_session(command, pod_name, require_complete=False):
    # All evidence from this new pod, bounded. Missing/truncated proof refuses acceptance.
    log=run(*command,"logs",pod_name,"--limit-bytes=4194304")
    subscriptions=re.findall(r"ES trades subscription:",log)
    if len(subscriptions)>1 or re.search(r"Databento ES(?:-only)? live stream error:|ES replay unavailable:",log):
        raise RuntimeError("ES feed reconnected or replay became unavailable during verification")
    subscribed=bool(re.search(r'ES trades subscription: replay_minutes=60 start=Some\("[^\"]+"\)',log))
    report=replay_result(log)
    if require_complete and (len(subscriptions)!=1 or not subscribed or report is None):
        raise RuntimeError("Completed ES session evidence missing during verification")
    return report if subscribed else None


def interrupted(signum, frame):
    raise RuntimeError("Deployment interrupted; recovering previous template")


def main():
    if os.environ.get("ACTION") != "refresh-es-replay":
        raise SystemExit("refresh-es-replay action required")
    if os.environ.get("DEPLOY_DRY_RUN") != "false":
        raise SystemExit("refresh-es-replay requires DEPLOY_DRY_RUN=false")
    source = os.environ["FEED_PERMITTED_SHA"]
    if not re.fullmatch(r"[0-9a-f]{40}", source):
        raise SystemExit("Exact FEED_PERMITTED_SHA required")
    target = os.environ.get("REPLAY_TARGET", "")
    if target == "production":
        kube, role, expected_uid = os.environ["ES4_KUBECONFIG"], "es4", os.environ["ES4_NS_UID"]
        workload, container_name, architecture = "es-feed", "es-feed", "amd64"
    elif target == "dev":
        kube, role, expected_uid = os.environ["DEV_KUBECONFIG"], "dev", os.environ["DEV_NS_UID"]
        workload, container_name, architecture = "options-edge-databento-feed", "databento-feed", "arm64"
    else:
        raise SystemExit("Unknown replay target")
    run("bash", "scripts/es4/assert-cluster-identity.sh", kube, role, expected_uid)
    if target == "production":
        prod = os.environ["PROD_KUBECONFIG"]
        run("bash", "scripts/es4/assert-cluster-identity.sh", prod, "prod", os.environ["PROD_NS_UID"])
        run("bash", "scripts/es4/assert-es-feed-exclusive.sh", kube, prod, "es4")
    command = ["kubectl", "--kubeconfig", kube, "-n", "options-edge"]
    def deployment():
        return json.loads(run(*command, "get", "deployment", workload, "-o", "json"))
    before = deployment()
    if before["spec"]["replicas"] != 1 or before["spec"]["strategy"]["type"] != "Recreate":
        raise SystemExit("Only the active singleton Recreate feed may be refreshed")
    labels=before["spec"]["selector"].get("matchLabels",{})
    if not labels or before["spec"]["selector"].get("matchExpressions"):raise RuntimeError("Unsupported feed selector")
    selector=",".join(f"{key}={value}" for key,value in sorted(labels.items()))
    run("python3","-B","scripts/es4/validate-es-replay-settings.py",str(Path(".jenkins-tmp/feed").resolve()))
    digest = json.loads(Path(".jenkins-tmp/es-replay-image.json").read_text())["containerimage.digest"]
    registry = os.environ.get("REPLAY_REGISTRY",os.environ["REGISTRY"])
    accepted = verify_image(registry, digest, source, architecture)
    image = f"{registry}/options-edge-databento-feed@{digest}"
    old_template = before["spec"]["template"]
    template = copy.deepcopy(old_template)
    selected = container(template, container_name)
    selected["image"] = image
    selected["env"] = [e for e in selected.get("env", []) if e["name"] != "DATABENTO_ES_TRADES_REPLAY_START_MINUTES"]
    selected["env"].append({"name": "DATABENTO_ES_TRADES_REPLAY_START_MINUTES", "value": "60"})
    def patch_template(current, target):
        patch = {"metadata": {"resourceVersion": current["metadata"]["resourceVersion"]}, "spec": {"template": target}}
        run(*command, "patch", "deployment", workload, "--type=merge", "-p", json.dumps(patch))
    print("Previous image:", container(old_template, container_name)["image"], flush=True)
    bootstrap, topic = ("localhost:19092","underlying.es.trades") if target=="dev" else ("192.168.100.4:9092","es.underlying.es.trades")
    downstream_before=preflight_downstream(command,bootstrap,topic)
    previous_signal = signal.signal(signal.SIGTERM, interrupted)
    try:
        # One atomic template update; no ConfigMap, replica or other workload mutation.
        patch_template(before, template)
        run(*command, "rollout", "status", f"deployment/{workload}", "--timeout=600s")
        pod_name = verify_pod(command, image, accepted, container_name, workload, selector)
        replay_report = None
        verified_at = None
        # Require 60 seconds AFTER the first verified completion, within five minutes.
        for attempt in range(10):
            time.sleep(30)
            if verify_pod(command, image, accepted, container_name, workload, selector) != pod_name:
                raise RuntimeError("ES feed pod changed during stability verification")
            report=observe_es_session(command,pod_name,require_complete=replay_report is not None)
            if report is not None:
                replay_report=report
                if verified_at is None: verified_at=time.monotonic()
                if time.monotonic()-verified_at>=60:break
        if replay_report is None:
            raise RuntimeError("No completed, verified 60-minute ES replay observed within five minutes")
        if verified_at is None or time.monotonic()-verified_at<60:
            raise RuntimeError("No sixty-second post-replay stability interval within five minutes")
        actual = deployment()
        if actual["spec"]["template"] != template:
            raise RuntimeError("Deployment template changed during verification")
    except Exception:
        # Recover only this action's template, with optimistic concurrency. Never overwrite another rollout.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        recovered = False
        for attempt in range(3):
            try:
                actual = deployment()
                if actual["metadata"]["uid"] != before["metadata"]["uid"]:
                    raise RuntimeError("Recovery refused: deployment identity changed")
                if actual["spec"]["template"] == old_template:
                    recovered = True
                    break
                if actual["spec"]["template"] != template:
                    raise RuntimeError("Recovery refused: deployment changed concurrently; inspect runtime state")
                patch_template(actual, old_template)
                recovered = True
                break
            except Exception as exc:
                if str(exc).startswith("Recovery refused:") or attempt == 2:
                    print("Inspect read-only: kubectl --kubeconfig", kube, f"-n options-edge get deployment {workload} -o wide", flush=True)
                    raise
                time.sleep(attempt+1)
        if recovered:
            run(*command, "rollout", "status", f"deployment/{workload}", "--timeout=600s")
            if deployment()["spec"]["template"] != old_template:
                raise RuntimeError("Recovery template verification failed")
            print("RECOVERED previous ES feed template; requested replay deployment failed", flush=True)
        raise

    finally:
        signal.signal(signal.SIGTERM, previous_signal)
    try:
        progress_fence=verify_downstream_fence(downstream_before,command,bootstrap,topic,
            observe=lambda:observe_es_session(command,pod_name,require_complete=True))
        if verify_pod(command,image,accepted,container_name,workload,selector)!=pod_name:
            raise RuntimeError("ES feed pod changed during downstream verification")
        if deployment()["spec"]["template"]!=template:raise RuntimeError("Feed template changed during downstream verification")
        observe_es_session(command,pod_name,require_complete=True)
    except Exception as exc:
        raise RuntimeError("Feed passed verification; downstream verification failed; no rollback or tag promotion performed: "+str(exc)) from None
    # A tag transport failure must not undo a verified healthy running feed.
    try:promote_verified_image(registry,digest)
    except Exception:
        raise RuntimeError("Feed verified and running; ES tag was not confirmed promoted. No workload rollback performed") from None
    print(json.dumps({"target":target,"replay":replay_report,"image":image,"pod":pod_name,"ready":True,"replayMinutes":60,"downstreamFence":progress_fence}))



if __name__ == "__main__":
    main()
