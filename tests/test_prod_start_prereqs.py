"""prod-start-prereqs.sh is fail-closed: reversal-confirmation stays at 0 unless ReversalTopCandidate sits under id 2901."""
import json, subprocess, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "ops" / "prod-start-prereqs.sh"
SUBJ = "/subjects/com.optionsedge.contracts.reversalcandidate.ReversalTopCandidate/versions/latest"


def _serve(body):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if body is not None and self.path == SUBJ:
                self.send_response(200); self.end_headers(); self.wfile.write(json.dumps(body).encode())
            else:
                self.send_response(404); self.end_headers()
        def log_message(self, *a): pass
    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _run(url):
    r = subprocess.run(["bash", str(SCRIPT)], env={"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "PREREQ_REGISTRY": url},
                       capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout.strip()


def test_registered_under_2901_blocks_nothing():
    srv = _serve({"id": 2901, "version": 1})
    try:
        assert _run(f"http://127.0.0.1:{srv.server_port}") == (0, "")
    finally:
        srv.shutdown()


def test_missing_subject_holds_reversal():
    srv = _serve(None)
    try:
        assert _run(f"http://127.0.0.1:{srv.server_port}") == (0, "reversal-confirmation-service")
    finally:
        srv.shutdown()


def test_wrong_id_holds_reversal():
    srv = _serve({"id": 77, "version": 1})
    try:
        assert _run(f"http://127.0.0.1:{srv.server_port}")[1] == "reversal-confirmation-service"
    finally:
        srv.shutdown()


def test_unreachable_registry_holds_reversal():
    assert _run("http://127.0.0.1:1")[1] == "reversal-confirmation-service"


def test_morning_autostart_uses_it_and_the_job_ships_it():
    assert "prod-start-prereqs.sh" in (ROOT / "scripts/ops/morning-autostart.sh").read_text()
    assert "prod-start-prereqs.sh" in (ROOT / "Jenkinsfile.morning-autostart").read_text()


import unittest


class ProdStartPrereqs(unittest.TestCase):
    def test_registered_under_2901_blocks_nothing(self): test_registered_under_2901_blocks_nothing()
    def test_missing_subject_holds_reversal(self): test_missing_subject_holds_reversal()
    def test_wrong_id_holds_reversal(self): test_wrong_id_holds_reversal()
    def test_unreachable_registry_holds_reversal(self): test_unreachable_registry_holds_reversal()
    def test_wiring(self): test_morning_autostart_uses_it_and_the_job_ships_it()
