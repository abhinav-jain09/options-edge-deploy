"""scripts/kafka/ensure-spot-vol-regime-schemas.sh against the stand-in registry: a fresh registry gets the three canonical
spot-vol-regime subjects under FULL, a re-run changes nothing, a drifted subject-level config is repaired to FULL, a file that is not
the pinned canonical bytes is refused before any call, a newer non-canonical version is refused, and a dry run mutates nothing."""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-spot-vol-regime-schemas.sh"
FIXTURES = ROOT / "tests/fixtures/spotvolregime"
SUBJECTS = [f"com.optionsedge.contracts.spotvolregime.{n}" for n in ("SpotVolRegimeSnapshot", "SpotVolRegimeWindowState", "SpotVolRegimeLastFrameState")]


def run(url: str, directory: str, dry: bool = False):
    env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=directory, DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class EnsureSpotVolRegimeSchemas(unittest.TestCase):
    def test_fresh_registry_gets_every_subject_under_full(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url, str(FIXTURES))
            self.assertEqual(rc, 0, out)
            for s in SUBJECTS:
                self.assertEqual(len(reg.subjects[s]), 1)
                self.assertEqual(reg.configs[s], "FULL")
                self.assertIn("compatibility: " + s + " FULL", out)
        finally:
            server.shutdown()

    def test_rerun_is_idempotent_and_repairs_a_drifted_config(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            self.assertEqual(run(url, str(FIXTURES))[0], 0)
            reg.configs[SUBJECTS[1]] = "BACKWARD"
            rc, out = run(url, str(FIXTURES))
            self.assertEqual(rc, 0, out)
            for s in SUBJECTS:
                self.assertEqual(len(reg.subjects[s]), 1, "no second version")
                self.assertEqual(reg.configs[s], "FULL")
        finally:
            server.shutdown()

    def test_any_file_but_the_pinned_canonical_bytes_is_refused_before_any_call(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            with tempfile.TemporaryDirectory() as d:
                for f in FIXTURES.glob("*.avsc"):
                    shutil.copy(f, d)
                p = Path(d) / "SpotVolRegimeWindowState.avsc"
                p.write_text(p.read_text().replace("state store", "state stOre"))
                rc, out = run(url, d)
            self.assertNotEqual(rc, 0)
            self.assertIn("not the canonical", out)
            self.assertEqual(reg.calls, [])
        finally:
            server.shutdown()

    def test_a_newer_version_after_the_canonical_one_is_refused(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            self.assertEqual(run(url, str(FIXTURES))[0], 0)
            reg.register(SUBJECTS[0], (FIXTURES / "SpotVolRegimeWindowState.avsc").read_text())
            reg.configs[SUBJECTS[0]] = "BACKWARD"
            before = len(reg.calls)
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            self.assertIn("latest version", out)
            self.assertIn("nothing was changed", out)
            # NON-MUTATING: the refused run sent no PUT/POST-version, and the drifted config stays as it was.
            self.assertEqual([c for c in reg.calls[before:] if c.startswith(("PUT", "POST /subjects/" + SUBJECTS[0] + "/versions"))], [])
            self.assertEqual(reg.configs[SUBJECTS[0]], "BACKWARD")
        finally:
            server.shutdown()

    def test_a_bad_later_subject_refuses_before_the_first_one_is_touched(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            reg.register(SUBJECTS[2], (FIXTURES / "SpotVolRegimeWindowState.avsc").read_text())
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            self.assertNotIn(SUBJECTS[0], reg.subjects)
            self.assertEqual(reg.configs, {})
            self.assertEqual([c for c in reg.calls if c.startswith("PUT")], [])
        finally:
            server.shutdown()

    def test_dry_run_mutates_nothing(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url, str(FIXTURES), dry=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("DRY RUN", out)
            self.assertEqual(reg.subjects, {})
            self.assertEqual(reg.configs, {})
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
