#!/usr/bin/env python3
"""Can every image the dev cluster pins actually be PULLED from the registry?

Reads `kubectl get deploy,statefulset,daemonset,cronjob,job -o json` on stdin; talks to $REG.

WHY THIS IS NOT A HEAD CHECK. Three outages in two weeks were each reported healthy by a check that
asked the wrong question: a manifest that RESOLVES is not an image that PULLS. registry:2 with
`storage.cache.blobdescriptor: inmemory` keeps answering HEAD 200 with the full length for a blob whose
file garbage collection already swept, and a digest-pinned image can lose its manifest while its tag
still resolves. So this GETs: the manifest by the EXACT reference the pod pulls (the digest when the
reference carries one), then, for every layer and the config, the FIRST and the LAST byte of the blob.
A swept or truncated file fails at one of those two reads; the cache cannot answer them.

EVERY container is read -- initContainers included -- because options-edge-web's init container kept
a digest nothing else referenced.

Exit 0 only when every pinned image pulls; 1 with one BROKEN line per image otherwise; 2 when it could
not tell (an unreadable or empty workload list is not a clean bill of health).
"""
from __future__ import annotations
import importlib.util, json, os, sys, urllib.request
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "keepset", Path(__file__).resolve().parent / "dev-registry-keep-set.py")
keepset = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(keepset)

ACC = ",".join([
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.index.v1+json"])


def fetch(url, rng=None):
    h = {"Accept": ACC}
    if rng:
        h["Range"] = rng
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=60) as r:
        return r.read()


def blob_readable(reg, repo, desc):
    """First and last byte of the blob file must really come back."""
    size, url = int(desc["size"]), f"{reg}/v2/{repo}/blobs/{desc['digest']}"
    if size <= 0:
        return
    for rng in ("bytes=0-0", f"bytes={size - 1}-{size - 1}"):
        got = fetch(url, rng)
        if len(got) != 1:
            raise RuntimeError(f"blob {desc['digest'][:19]} {rng} returned {len(got)} bytes")


def check_manifest(reg, repo, ref):
    m = json.loads(fetch(f"{reg}/v2/{repo}/manifests/{ref}"))
    if "manifests" in m:  # an index: every runnable sub-manifest must pull (attestations are not pulled)
        for sub in m["manifests"]:
            if (sub.get("platform") or {}).get("architecture") == "unknown":
                continue
            check_manifest(reg, repo, sub["digest"])
        return
    for desc in [m["config"]] + (m.get("layers") or []):
        blob_readable(reg, repo, desc)


def main():
    reg = os.environ.get("REG", "http://localhost:5001").rstrip("/")
    try:
        data = json.load(sys.stdin)
    except Exception as exc:  # noqa: BLE001
        print(f"CANNOT VERIFY: workload list unreadable ({exc})", file=sys.stderr)
        return 2
    if not (data or {}).get("items"):
        print("CANNOT VERIFY: the cluster reported no workloads", file=sys.stderr)
        return 2
    owners = {}
    for item in data["items"]:
        for spec in keepset.pod_specs({"items": [item]}):
            for key in ("initContainers", "containers"):
                for c in spec.get(key) or []:
                    img = (c or {}).get("image") or ""
                    if ":5001/" in img or img.startswith("localhost"):
                        owners.setdefault(img, set()).add(
                            f"{item.get('kind')}/{(item.get('metadata') or {}).get('name')}:{c.get('name')}")
    broken = []
    for img in sorted(owners):
        repo, tag, digest = keepset.split_ref(img)
        try:
            check_manifest(reg, repo, digest or tag or "latest")
        except Exception as exc:  # noqa: BLE001 - any failure to read is a failure to pull
            broken.append(f"BROKEN {repo} @ {(digest or tag or 'latest')[:26]} -> {str(exc)[:70]}   pinned by: "
                          + ", ".join(sorted(owners[img])))
    print(f"verify-pinned: {len(owners)} image(s) pinned, {len(broken)} cannot be pulled")
    for line in broken:
        print("  " + line)
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
