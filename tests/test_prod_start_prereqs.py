# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""prod-start-prereqs.sh is fail-closed: reversal-confirmation stays at 0 unless ReversalTopCandidate sits under id 2901."""
import json
import subprocess
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "ops" / "prod-start-prereqs.sh"
SUBJ = "/subjects/com.optionsedge.contracts.reversalcandidate.ReversalTopCandidate/versions/latest"


class ProdStartPrereqs(unittest.TestCase):
    """The bodies live HERE, not in module-level `def test_` functions delegated to from a wrapper.

    unittest collects TestCase methods only, so a module-level test is invisible to it unless
    something calls it. This file used to carry five module-level functions AND a TestCase that
    called each one: every test did run, but a sixth function added beside them would have run
    nowhere, silently — which is the trap scripts/ci/validate-tests-are-collectable.sh bans, and
    why that gate was failing on this file from the day it landed (#1461).
    """

    def _serve(self, body):
        """Start a registry that answers SUBJ with `body`, or 404s everything when body is None."""
        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                if body is not None and self.path == SUBJ:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(json.dumps(body).encode())
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, *a):
                pass

        srv = HTTPServer(("127.0.0.1", 0), H)
        # server_close() is registered BEFORE anything else can raise: shutdown() stops the serve loop
        # but leaves the listening socket open (the old version printed a ResourceWarning per server for
        # exactly that), and if the thread below failed to start there would be nothing else to close it.
        self.addCleanup(srv.server_close)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        # Registered only once serving has actually begun: shutdown() blocks until the serve_forever loop
        # exits, so on a failed start it must NOT run. Cleanups are LIFO, so this one runs first.
        self.addCleanup(srv.shutdown)
        return "http://127.0.0.1:%d" % srv.server_port

    def _run(self, url):
        r = subprocess.run(
            ["bash", str(SCRIPT)],
            env={"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "PREREQ_REGISTRY": url},
            capture_output=True,
            text=True,
            timeout=60,
        )
        return r.returncode, r.stdout.strip()

    def test_registered_under_2901_blocks_nothing(self):
        self.assertEqual((0, ""), self._run(self._serve({"id": 2901, "version": 1})))

    def test_missing_subject_holds_reversal(self):
        self.assertEqual((0, "reversal-confirmation-service"), self._run(self._serve(None)))

    def test_wrong_id_holds_reversal(self):
        self.assertEqual(
            "reversal-confirmation-service", self._run(self._serve({"id": 77, "version": 1}))[1]
        )

    def test_unreachable_registry_holds_reversal(self):
        self.assertEqual("reversal-confirmation-service", self._run("http://127.0.0.1:1")[1])

    def test_morning_autostart_uses_it_and_the_job_ships_it(self):
        for path in ("scripts/ops/morning-autostart.sh", "Jenkinsfile.morning-autostart"):
            self.assertIn(
                "prod-start-prereqs.sh",
                (ROOT / path).read_text(),
                path + " must invoke the prerequisite script",
            )
