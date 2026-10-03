#!/usr/bin/env python3
"""Fixture builder for zerodte-research-migrate-receipt-test.sh: Kubernetes objects as the API would list them, from one-line specs.

  pods  <spec>...        → {"items":[...]}   spec = name:imageID:flag[:opt...]
      imageID   ok (the pinned digest) | ok-pullable (docker-pullable://… form) | ok-bare (a bare sha256:… imageID) | other (another digest)
                | none (no container status yet) | foreign (an image of another repository)
      flag      lit=<v> | none | cmkey=<cm>/<key>[/opt] | seckey=<sec>/<key> | fieldref | dup | expand
                | from-cm=<cm>[/<prefix>] | from-sec=<sec>[/<prefix>] | from-cm-opt=<cm> | lit-and-from-cm=<v>/<cm>
      opt       terminating | phase=<Pending|Succeeded|Failed> | init=<imageID> | sidecar=<imageID> | job=<label> (owned by a Job, labelled)
                | label=<label> (labelled, NOT owned by a Job) | uid=<uid>
  deployments <spec>...  → spec = name:image(ok|other|tag):flag(lit=…|none|from-cm=…):replicas[:rolling|genlag|zero-still-running]
  replicasets <spec>...  → spec = name:owner(<deployment uid>|none)
  jobs <spec>...         → spec = name:label(<app.kubernetes.io/name>|none)
  cronjobs <spec>...     → spec = name
  statefulsets|daemonsets <spec>... → spec = name
  cm <k=v>...            → a ConfigMap's data
"""
import json
import os
import sys

DIGEST = os.environ["FAKE_PINNED_DIGEST"]
OTHER = "sha256:" + "c" * 64
REG = "192.168.100.252:5000/options-edge-vix-option-inteligence"
DEP_UID = "dep-uid-0001"


def image_id(kind):
    return {"ok": REG + "@" + DIGEST, "ok-pullable": "docker-pullable://" + REG + "@" + DIGEST, "ok-bare": DIGEST, "other": REG + "@" + OTHER,
            "foreign": "192.168.100.252:5000/options-edge-other@" + DIGEST, "none": None}[kind]


def spec_image(kind):
    return "192.168.100.252:5000/options-edge-other:x" if kind == "foreign" else REG + ":dev"


def flag_env(flag):
    env, env_from = [], []
    if flag == "none":
        pass
    elif flag.startswith("lit="):
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "value": flag[4:]})
    elif flag.startswith("cmkey="):
        parts = flag[6:].split("/")
        ref = {"name": parts[0], "key": parts[1]}
        if len(parts) > 2 and parts[2] == "opt":
            ref["optional"] = True
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "valueFrom": {"configMapKeyRef": ref}})
    elif flag.startswith("seckey="):
        name, key = flag[7:].split("/")
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "valueFrom": {"secretKeyRef": {"name": name, "key": key}}})
    elif flag == "fieldref":
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}})
    elif flag == "dup":
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "value": "false"})
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "value": "true"})
    elif flag == "expand":
        env.append({"name": "RESEARCH", "value": "true"})
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "value": "$(RESEARCH)"})
    elif flag.startswith("from-cm-opt="):
        env_from.append({"configMapRef": {"name": flag[12:], "optional": True}})
    elif flag.startswith("from-cm="):
        parts = flag[8:].split("/")
        src = {"configMapRef": {"name": parts[0]}}
        if len(parts) > 1:
            src["prefix"] = parts[1]
        env_from.append(src)
    elif flag.startswith("from-sec="):
        parts = flag[9:].split("/")
        src = {"secretRef": {"name": parts[0]}}
        if len(parts) > 1:
            src["prefix"] = parts[1]
        env_from.append(src)
    elif flag.startswith("lit-and-from-cm="):
        v, cm = flag[16:].split("/")
        env.append({"name": "ZERODTE_RESEARCH_ENABLED", "value": v})
        env_from.append({"configMapRef": {"name": cm}})
    else:
        raise SystemExit("unknown flag spec " + flag)
    return env, env_from


def pod(spec):
    parts = spec.split(":")
    name, img, flag, opts = parts[0], parts[1], parts[2], parts[3:]
    env, env_from = flag_env(flag)
    container = {"name": "vix", "image": spec_image(img), "env": env, "envFrom": env_from}
    p = {"metadata": {"name": name, "uid": "uid-" + name, "labels": {"app.kubernetes.io/name": "vix-option-inteligence-service"}},
         "spec": {"containers": [container]}, "status": {"phase": "Running", "containerStatuses": []}}
    if image_id(img) is not None:
        p["status"]["containerStatuses"].append({"name": "vix", "imageID": image_id(img)})
    for o in opts:
        if o == "terminating":
            p["metadata"]["deletionTimestamp"] = "2026-10-04T12:00:00Z"
        elif o.startswith("phase="):
            p["status"]["phase"] = o[6:]
        elif o.startswith("init="):
            p["spec"]["initContainers"] = [{"name": "init", "image": spec_image(o[5:])}]
            if image_id(o[5:]) is not None:
                p["status"]["initContainerStatuses"] = [{"name": "init", "imageID": image_id(o[5:]), "state": {"terminated": {"exitCode": 0}}}]
        elif o.startswith("sidecar="):
            p["spec"]["containers"].append({"name": "side", "image": spec_image(o[8:])})
            if image_id(o[8:]) is not None:
                p["status"]["containerStatuses"].append({"name": "side", "imageID": image_id(o[8:])})
        elif o.startswith("job="):
            p["metadata"]["labels"]["app.kubernetes.io/name"] = o[4:]
            p["metadata"]["ownerReferences"] = [{"kind": "Job", "name": "job-" + name, "uid": "job-uid-" + name}]
        elif o.startswith("label="):
            p["metadata"]["labels"]["app.kubernetes.io/name"] = o[6:]
        elif o.startswith("uid="):
            p["metadata"]["uid"] = o[4:]
        else:
            raise SystemExit("unknown pod option " + o)
    return p


def deployment(spec):
    parts = spec.split(":")
    name, img, flag, replicas, opts = parts[0], parts[1], parts[2], int(parts[3]), parts[4:]
    env, env_from = flag_env(flag)
    image = {"ok": REG + "@" + DIGEST, "other": REG + "@" + OTHER, "tag": REG + ":dev"}[img]
    d = {"metadata": {"name": name, "uid": DEP_UID if name == "vix-option-inteligence-service" else "uid-" + name, "generation": 7},
         "spec": {"replicas": replicas, "template": {"spec": {"containers": [{"name": "vix", "image": image, "env": env, "envFrom": env_from}]}}},
         "status": {"observedGeneration": 7, "replicas": replicas, "updatedReplicas": replicas, "availableReplicas": replicas, "readyReplicas": replicas}}
    for o in opts:
        if o == "rolling":
            d["status"]["updatedReplicas"] = max(replicas - 1, 0)
            d["status"]["unavailableReplicas"] = 1
        elif o == "genlag":
            d["status"]["observedGeneration"] = 6
        elif o == "zero-still-running":
            d["status"]["replicas"] = 1
        else:
            raise SystemExit("unknown deployment option " + o)
    return d


def template(image=None):
    return {"spec": {"containers": [{"name": "vix", "image": image or (REG + "@" + DIGEST)}]}}


def main(argv):
    kind, specs = argv[0], argv[1:]
    items = []
    if kind == "pods":
        items = [pod(s) for s in specs]
    elif kind == "deployments":
        items = [deployment(s) for s in specs]
    elif kind == "replicasets":
        for s in specs:
            name, owner = s.split(":")
            rs = {"metadata": {"name": name, "uid": "uid-" + name}, "spec": {"template": template()}}
            if owner != "none":
                rs["metadata"]["ownerReferences"] = [{"kind": "Deployment", "name": "vix-option-inteligence-service", "uid": owner}]
            items.append(rs)
    elif kind == "jobs":
        for s in specs:
            name, label = s.split(":")
            j = {"metadata": {"name": name, "uid": "uid-" + name, "labels": {}}, "spec": {"template": template()}}
            if label != "none":
                j["metadata"]["labels"]["app.kubernetes.io/name"] = label
            items.append(j)
    elif kind == "cronjobs":
        items = [{"metadata": {"name": s, "uid": "uid-" + s}, "spec": {"jobTemplate": {"spec": {"template": template()}}}} for s in specs]
    elif kind in ("statefulsets", "daemonsets"):
        items = [{"metadata": {"name": s, "uid": "uid-" + s}, "spec": {"template": template()}} for s in specs]
    elif kind == "cm":
        print(json.dumps({"metadata": {"name": "cm"}, "data": dict(s.split("=", 1) for s in specs)}))
        return
    else:
        raise SystemExit("unknown kind " + kind)
    print(json.dumps({"items": items}))


if __name__ == "__main__":
    main(sys.argv[1:])
