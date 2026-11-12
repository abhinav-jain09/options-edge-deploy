#!/usr/bin/env python3
"""Drive the REAL dev-registry-prune.sh and dev-registry-verify-pinned.py against a fake registry.

The fake registry reproduces what actually hurt us, not a cartoon of it:
  * garbage collection sweeps blobs no manifest references;
  * its descriptor CACHE keeps answering HEAD 200 for a swept blob until the registry is restarted
    (registry:2 `blobdescriptor: inmemory`, options-edge-web 2026-11-12);
  * a push-in-progress's blobs look unreferenced to a collector that runs against a live registry.
`docker` and `kubectl` are stubs that talk to the fake registry's control endpoints, so the order of
stop -> collect -> start is observable, and a collection that happens while the registry is UP is
recorded as a violation.
"""
from __future__ import annotations
import hashlib, json, os, subprocess, tempfile, threading, unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRUNE = HERE / "dev-registry-prune.sh"
VERIFY_PY = HERE / "dev-registry-verify-pinned.py"
MT = "application/vnd.docker.distribution.manifest.v2+json"


def sha(b):
    return "sha256:" + hashlib.sha256(b).hexdigest()


class Registry:
    def __init__(self):
        self.repos = {}      # repo -> {"tags": {tag: digest}, "manifests": {digest: bytes}}
        self.blobs = {}      # digest -> bytes (the files on disk)
        self.cache = set()   # descriptor cache: digests HEAD claims exist
        self.down = False
        self.calls = []
        self.gc_while_up = False
        self.deleted = []

    def push(self, repo, tag, layers, *, seed=b""):
        descs = []
        for blob in layers + [b"config" + seed + b"".join(layers)]:
            d = sha(blob)
            self.blobs[d] = blob
            self.cache.add(d)
            descs.append({"digest": d, "size": len(blob)})
        m = json.dumps({"schemaVersion": 2, "mediaType": MT, "config": descs[-1], "layers": descs[:-1]},
                       sort_keys=True).encode()
        r = self.repos.setdefault(repo, {"tags": {}, "manifests": {}})
        d = sha(m)
        r["manifests"][d] = m
        if tag:
            r["tags"][tag] = d
        return d

    def gc(self):
        live = set()
        for r in self.repos.values():
            for m in r["manifests"].values():
                j = json.loads(m)
                live.update([j["config"]["digest"]] + [l["digest"] for l in j["layers"]])
        for d in [d for d in self.blobs if d not in live]:
            del self.blobs[d]          # swept; self.cache still remembers it


def handler_for(reg: Registry):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body=b"", headers=None):
            self.send_response(code)
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _route(self):
            p = self.path.split("?")[0]
            if p.startswith("/_control/"):
                op = p.rsplit("/", 1)[1]
                reg.calls.append(op)
                if op == "down":
                    reg.down = True
                elif op == "up":
                    reg.down = False
                    reg.cache = set(reg.blobs)     # a restart empties the descriptor cache
                elif op == "gc":
                    if not reg.down:
                        reg.gc_while_up = True
                    reg.gc()
                return self._send(200, b"ok")
            if reg.down:
                return self._send(503)
            if p == "/v2/":
                return self._send(200, b"{}")
            if p == "/v2/_catalog":
                return self._send(200, json.dumps({"repositories": sorted(reg.repos)}).encode())
            parts = p[len("/v2/"):].split("/")
            kind = parts[-2]
            repo, name = "/".join(parts[:-2]), parts[-1]
            r = reg.repos.get(repo)
            if kind == "tags" and name == "list":
                if r is None:
                    return self._send(404)
                return self._send(200, json.dumps({"name": repo, "tags": sorted(r["tags"])}).encode())
            if kind == "manifests":
                if r is None:
                    return self._send(404)
                d = r["tags"].get(name, name)
                if d not in r["manifests"]:
                    return self._send(404)
                if self.command == "DELETE":
                    del r["manifests"][d]
                    for t in [t for t, x in r["tags"].items() if x == d]:
                        del r["tags"][t]
                    reg.deleted.append((repo, d))
                    return self._send(202)
                return self._send(200, r["manifests"][d], {"Content-Type": MT, "Docker-Content-Digest": d})
            if kind == "blobs":
                if self.command == "HEAD":      # the cache answers HEAD
                    return self._send(200 if name in reg.cache else 404)
                data = reg.blobs.get(name)
                if data is None:
                    return self._send(404)
                rng = self.headers.get("Range")
                if rng:
                    a, b = rng.split("=")[1].split("-")
                    return self._send(206, data[int(a):int(b) + 1])
                return self._send(200, data)
            return self._send(404)

        do_GET = do_HEAD = do_DELETE = do_POST = _route

    return H


def workload(*specs):
    """specs: (init_images, container_images) -> kubectl list."""
    items = []
    for i, (init, main) in enumerate(specs):
        items.append({"kind": "Deployment", "metadata": {"name": f"w{i}"}, "spec": {"template": {"spec": {
            "initContainers": [{"name": f"i{j}", "image": x} for j, x in enumerate(init)],
            "containers": [{"name": f"c{j}", "image": x} for j, x in enumerate(main)]}}}})
    return {"items": items}


class Base(unittest.TestCase):
    def setUp(self):
        self.reg = Registry()
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(self.reg))
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        self.tmp = tempfile.TemporaryDirectory()
        self.bin = Path(self.tmp.name) / "bin"
        self.bin.mkdir()
        self.order = Path(self.tmp.name) / "docker-calls.log"
        self.wl = Path(self.tmp.name) / "workloads.json"
        self.write_stubs()

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()
        self.tmp.cleanup()

    def write_stubs(self):
        ctl = f"curl -s -X POST {self.url}/_control"
        (self.bin / "docker").write_text(f"""#!/bin/bash
echo "$*" >> {self.order}
case "$1" in
  ps) echo local-registry ;;
  inspect) echo registry:2 ;;
  exec) echo 1G ;;
  stop) [ -e {self.tmp.name}/cannot-stop ] && exit 1; {ctl}/down >/dev/null ;;
  start) {ctl}/up >/dev/null ;;
  run) [ -e {self.tmp.name}/gc-fails ] && {{ echo 'gc exploded' >&2; exit 3; }}; {ctl}/gc >/dev/null ;;
esac
""")
        (self.bin / "kubectl").write_text(f"#!/bin/bash\n[ -s {self.wl} ] && cat {self.wl}\n")
        for f in self.bin.iterdir():
            f.chmod(0o755)

    def set_workloads(self, obj):
        self.wl.write_text(json.dumps(obj))

    def run_prune(self):
        env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}", REG=self.url, KCTX="")
        return subprocess.run(["bash", str(PRUNE)], env=env, capture_output=True, text=True, timeout=120)

    def ref(self, repo, *, tag=None, digest=None):
        r = f"host.docker.internal:5001/{repo}"
        if tag:
            r += f":{tag}"
        if digest:
            r += f"@{digest}"
        return r


class Prune(Base):
    def seed(self):
        """web: :dev moved on; its INIT container pins the previous digest (now untagged) -- the 11-12 outage.
        hand: pinned by TAG alone. other: an old build tag nothing uses."""
        reg = self.reg
        self.web_old = reg.push("web", "build-1", [b"w1" * 40, b"shared" * 50])
        del reg.repos["web"]["tags"]["build-1"]                       # untagged, digest-pinned only
        self.web_dev = reg.push("web", "dev", [b"w2" * 40, b"shared" * 50])
        self.hand = reg.push("hand", "2227-abc", [b"h" * 90])         # pinned by tag, no digest
        reg.push("hand", "dev", [b"hd" * 90])
        self.junk = reg.push("other", "build-9", [b"junk" * 70])
        reg.push("other", "dev", [b"od" * 70])
        self.set_workloads(workload(
            ([self.ref("web", tag="dev", digest=self.web_old)], [self.ref("web", tag="dev", digest=self.web_dev)]),
            ([], [self.ref("hand", tag="2227-abc")]),
        ))

    def test_prune_deletes_junk_and_keeps_every_pin_and_everything_still_pulls(self):
        self.seed()
        p = self.run_prune()
        self.assertEqual(0, p.returncode, p.stdout + p.stderr)
        deleted = {d for _, d in self.reg.deleted}
        self.assertIn(self.junk, deleted, "the unused old build tag must be pruned")
        self.assertNotIn(self.web_old, deleted, "the init container's digest is live")
        self.assertNotIn(self.hand, deleted, "a workload pinned by TAG alone is live")
        self.assertIn("0 cannot be pulled", p.stdout)

    def test_a_tag_pinned_image_survives_when_it_is_not_dev(self):
        """Mutation target: drop the (repo, tag) keep and `hand` is deleted."""
        self.seed()
        self.run_prune()
        self.assertIn("2227-abc", self.reg.repos["hand"]["tags"])

    def test_collection_runs_with_the_registry_stopped_and_it_is_started_again(self):
        self.seed()
        p = self.run_prune()
        self.assertEqual(0, p.returncode, p.stdout + p.stderr)
        self.assertFalse(self.reg.gc_while_up, "garbage collection ran against a LIVE registry")
        ops = [c for c in self.reg.calls if c in ("down", "gc", "up")]
        self.assertEqual(["down", "gc", "up"], ops)
        self.assertFalse(self.reg.down, "the registry must be running when the prune ends")

    def test_a_failed_collection_still_starts_the_registry_and_fails_the_prune(self):
        self.seed()
        Path(self.tmp.name, "gc-fails").write_text("")
        p = self.run_prune()
        self.assertEqual(1, p.returncode)
        self.assertFalse(self.reg.down, "a failed GC must not leave the registry stopped")
        self.assertIn("GARBAGE COLLECT FAILED", p.stderr)
        self.assertIn("gc exploded", p.stderr, "the collector's output must be shown, not discarded")

    def test_a_registry_that_cannot_be_stopped_is_never_collected(self):
        self.seed()
        Path(self.tmp.name, "cannot-stop").write_text("")
        p = self.run_prune()
        self.assertEqual(1, p.returncode)
        self.assertNotIn("gc", self.reg.calls)
        self.assertIn("not collecting against a live registry", p.stderr)

    def test_an_empty_cluster_view_refuses_and_deletes_nothing(self):
        self.seed()
        for body in ("", json.dumps({"items": []})):
            self.wl.write_text(body)
            p = self.run_prune()
            self.assertEqual(1, p.returncode, body)
            self.assertIn("REFUSING", p.stderr)
        self.assertEqual([], self.reg.deleted)

    def test_a_prune_that_leaves_a_pinned_layer_missing_fails_even_though_head_says_200(self):
        """The cache lie. A pinned image's layer file is gone but the descriptor cache still answers
        HEAD 200 -- exactly what the old verify (HEAD) reported as healthy."""
        self.seed()
        layer = sha(b"h" * 90)
        del self.reg.blobs[layer]            # swept; self.reg.cache still lists it
        self.assertIn(layer, self.reg.cache)
        p = self.run_prune()
        self.assertEqual(1, p.returncode, p.stdout + p.stderr)
        self.assertIn("BROKEN hand", p.stdout)


class Verify(Base):
    def verify(self, obj):
        env = dict(os.environ, REG=self.url)
        return subprocess.run(["python3", str(VERIFY_PY)], input=json.dumps(obj), env=env,
                              capture_output=True, text=True, timeout=60)

    def test_a_healthy_image_passes(self):
        d = self.reg.push("web", "dev", [b"x" * 100])
        p = self.verify(workload(([], [self.ref("web", tag="dev", digest=d)])))
        self.assertEqual(0, p.returncode, p.stdout)

    def test_an_init_containers_missing_manifest_is_found(self):
        d = self.reg.push("web", "dev", [b"x" * 100])
        gone = sha(b"never pushed")
        p = self.verify(workload(([self.ref("web", tag="dev", digest=gone)], [self.ref("web", tag="dev", digest=d)])))
        self.assertEqual(1, p.returncode)
        self.assertIn("BROKEN web", p.stdout)
        self.assertIn("i0", p.stdout, "the init container must be named as the owner")

    def test_a_truncated_layer_is_found_by_the_last_byte_read(self):
        d = self.reg.push("web", "dev", [b"y" * 100])
        layer = sha(b"y" * 100)
        self.reg.blobs[layer] = b"y" * 40      # file shorter than the descriptor says
        p = self.verify(workload(([], [self.ref("web", tag="dev", digest=d)])))
        self.assertEqual(1, p.returncode, p.stdout)

    def test_an_unreadable_or_empty_workload_list_is_not_a_clean_bill(self):
        for obj in ({"items": []}, {}):
            self.assertEqual(2, self.verify(obj).returncode)


if __name__ == "__main__":
    unittest.main()
