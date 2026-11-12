#!/usr/bin/env python3
"""Can every image the dev cluster pins actually be PULLED from the registry?

Reads `kubectl get deploy,statefulset,daemonset,cronjob,job -o json` on stdin; talks to $REG.

WHY THIS IS NOT A HEAD CHECK. Three outages in two weeks were each reported healthy by a check that
asked the wrong question: a manifest that RESOLVES is not an image that PULLS. registry:2 with
`storage.cache.blobdescriptor: inmemory` keeps answering HEAD 200 with the full length for a blob whose
file garbage collection already swept, and a digest-pinned image can lose its manifest while its tag
still resolves. So this does what a pull does: GET the manifest by the EXACT reference the pod pulls
(the digest when the reference carries one) and check it hashes to that digest, then GET every config
and layer blob IN FULL and check it hashes to its descriptor's digest and size. A swept, truncated or
corrupted file fails there; the descriptor cache cannot answer a GET.

Each distinct blob is read once per run however many images share it (base layers are shared by the
whole fleet), so the cost is the size of the DISTINCT layers, not of 90 images.

EVERY container is read -- initContainers included -- because options-edge-web's init container kept
a digest nothing else referenced.

Exit 0 only when every pinned image pulls; 1 with one BROKEN line per image otherwise; 2 when it could
not tell (an unreadable or empty workload list is not a clean bill of health).
"""
from __future__ import annotations
import hashlib, importlib.util, json, os, sys, urllib.request
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "keepset", Path(__file__).resolve().parent / "dev-registry-keep-set.py")
keepset = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(keepset)

ACC = ",".join([
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.index.v1+json"])
CHUNK = 1 << 20


def _open(url):
    return urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": ACC}), timeout=120)


def _hasher(digest):
    algo, _, _ = digest.partition(":")
    try:
        return hashlib.new(algo)
    except ValueError:
        raise RuntimeError(f"unsupported digest algorithm in {digest[:19]}")


def fetch_verified(url, digest):
    """GET the whole body and require it to hash to `digest`. Returns the bytes for a manifest."""
    h, body = _hasher(digest), []
    with _open(url) as r:
        while True:
            chunk = r.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
            body.append(chunk)
    got = f"{digest.partition(':')[0]}:{h.hexdigest()}"
    if got != digest:
        raise RuntimeError(f"{digest[:19]} reads back as {got[:19]}")
    return b"".join(body)


class Checker:
    def __init__(self, reg):
        self.reg, self.blob_result = reg, {}

    def blob(self, repo, desc):
        d = desc["digest"]
        if d not in self.blob_result:
            try:
                h = _hasher(d)
                with _open(f"{self.reg}/v2/{repo}/blobs/{d}") as r:
                    while True:
                        chunk = r.read(CHUNK)
                        if not chunk:
                            break
                        h.update(chunk)
                if f"{d.partition(':')[0]}:{h.hexdigest()}" != d:
                    raise RuntimeError(f"blob {d[:19]} content does not hash to its digest")
                self.blob_result[d] = None
            except Exception as exc:  # noqa: BLE001
                self.blob_result[d] = str(exc)
        if self.blob_result[d]:
            raise RuntimeError(self.blob_result[d])

    def manifest(self, repo, ref):
        url = f"{self.reg}/v2/{repo}/manifests/{ref}"
        if ":" in ref:                                   # a digest: the body must hash to it
            raw = fetch_verified(url, ref)
        else:
            with _open(url) as r:
                raw = r.read()
        m = json.loads(raw)
        if "manifests" in m:  # an index: every runnable sub-manifest must pull (attestations are not pulled)
            for sub in m["manifests"]:
                if (sub.get("platform") or {}).get("architecture") == "unknown":
                    continue
                self.manifest(repo, sub["digest"])
            return
        for desc in [m["config"]] + (m.get("layers") or []):
            self.blob(repo, desc)


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
    chk, broken = Checker(reg), []
    for img in sorted(owners):
        repo, tag, digest = keepset.split_ref(img)
        try:
            chk.manifest(repo, digest or tag or "latest")
        except Exception as exc:  # noqa: BLE001 - any failure to read is a failure to pull
            broken.append(f"BROKEN {repo} @ {(digest or tag or 'latest')[:26]} -> {str(exc)[:90]}   pinned by: "
                          + ", ".join(sorted(owners[img])))
    print(f"verify-pinned: {len(owners)} image(s) pinned, {len(broken)} cannot be pulled")
    for line in broken:
        print("  " + line)
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
