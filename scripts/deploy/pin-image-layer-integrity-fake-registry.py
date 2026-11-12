#!/usr/bin/env python3
"""A registry just real enough to break in the ways the real one broke.

Serves /v2/<repo>/manifests/<ref> and /v2/<repo>/blobs/<digest>. The file <state>/mode selects the
failure, so one server covers every case and the test stays readable. Nothing here is a fixture of
the dev registry: it is the HTTP contract resolve_repo_digest reads, and the breakages are the ones
observed -- a blob that 404s, a blob served short, and an index whose platform manifest is gone.
"""
import hashlib, json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT, STATE = int(sys.argv[1]), sys.argv[2]
LAYER = b"layer-content" * 100
CONFIG = json.dumps({"os": "linux", "architecture": "arm64"}).encode()
def dig(b): return "sha256:" + hashlib.sha256(b).hexdigest()
LAYER_D, CONFIG_D = dig(LAYER), dig(CONFIG)

def mode():
    try:
        with open(STATE + "/mode") as f: return f.read().strip()
    except OSError: return "healthy"

def manifest(m):
    base = {"schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": CONFIG_D, "size": len(CONFIG)},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                        "digest": LAYER_D, "size": len(LAYER)}]}
    if m == "no_blobs":
        base["config"] = {}; base["layers"] = []
    return json.dumps(base).encode()

def index(sub_digest):
    return json.dumps({"schemaVersion": 2,
                       "mediaType": "application/vnd.oci.image.index.v1+json",
                       "manifests": [{"mediaType": "application/vnd.oci.image.manifest.v1+json",
                                      "digest": sub_digest, "size": 999,
                                      "platform": {"os": "linux", "architecture": "arm64"}}]}).encode()

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, body=b"", ctype="application/json", extra=None, head=False):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str((extra or {}).get("len", len(body))))
        for k, v in (extra or {}).items():
            if k != "len": self.send_header(k, v)
        self.end_headers()
        if not head and body: self.wfile.write(body)

    def handle_one(self, head):
        m = mode()
        p = self.path
        if p == "/v2/": return self._send(200, b"{}", head=head)
        if "/manifests/" in p:
            repo = p.split("/v2/", 1)[1].split("/manifests/")[0]
            ref = p.rsplit("/manifests/", 1)[1]
            if repo == "idx" or m.startswith("index"):
                body = manifest("no_blobs" if m == "index_no_blobs" else "healthy")
                sub = dig(body)
                if ref == "dev":
                    if m == "index_gone": sub = "sha256:" + "0" * 64
                    body = index(sub)
                    return self._send(200, body, extra={"Docker-Content-Digest": dig(body)}, head=head)
                if m == "index_gone": return self._send(404, b'{"errors":[]}', head=head)
                return self._send(200, body, extra={"Docker-Content-Digest": dig(body)}, head=head)
            body = manifest(m)
            return self._send(200, body, extra={"Docker-Content-Digest": dig(body)}, head=head)
        if "/blobs/" in p:
            d = p.rsplit("/blobs/", 1)[1]
            if d == CONFIG_D:
                if m == "missing_config": return self._send(404, b'{"errors":[]}', head=head)
                return self._send(200, CONFIG, head=head)
            if d == LAYER_D:
                if m in ("missing_layer", "index_broken"):
                    return self._send(404, b'{"errors":[]}', head=head)
                if m == "not_found_exact_len":
                    # A 404 whose error body is EXACTLY the declared layer size. The length check
                    # alone cannot tell this from a healthy blob; only the status check can.
                    return self._send(404, b"x" * len(LAYER), head=head)
                if m == "short_layer":
                    # 200 with a Content-Length BELOW what the manifest declares: the shape that
                    # pulls as "short read ... unexpected EOF" instead of as a clean 404.
                    return self._send(200, LAYER[:10], extra={"len": 10}, head=head)
                return self._send(200, LAYER, head=head)
            return self._send(404, b'{"errors":[]}', head=head)
        return self._send(404, b'{"errors":[]}', head=head)

    def do_GET(self):  self.handle_one(False)
    def do_HEAD(self): self.handle_one(True)

HTTPServer(("127.0.0.1", PORT), H).serve_forever()
