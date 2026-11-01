"""scripts/kafka/ensure-delta-flow-schemas.sh against the stand-in registry: a fresh registry gets all seventeen canonical
delta-flow subjects (six public outputs, eleven private state/repartition images) under FULL, a re-run changes nothing, a drifted
subject-level config is repaired to FULL, a file that is not the pinned canonical bytes is refused before any call, a newer
non-canonical version is refused, a bad or unwritable later subject refuses before the first is touched, a failure part-way leaves only
idempotent canonical state that a re-run converges, and a dry run mutates nothing."""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

# collectable as tests.test_ensure_delta_flow_schemas (validate-tests-are-collectable.sh) as well as from inside tests/
sys.path.insert(0, str(Path(__file__).resolve().parent))
from stub_schema_registry import Registry, serve  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-delta-flow-schemas.sh"
FIXTURES = ROOT / "tests/fixtures/deltaflow"
OUTPUTS = ["DeltaFlowEvent", "DeltaFlowSnapshot", "DeltaFlowStrikeSnapshot", "DeltaFlowCorrectionEvent",
           "DeltaFlowPositioningSnapshot", "DeltaFlowCalibrationRecord"]
IMAGES = ["DeltaFlowQuoteState", "DeltaFlowTradeInput", "DeltaFlowIntradayState", "DeltaFlowRollingState",
          "DeltaFlowStrikeStoreValue", "DeltaFlowReversibleContribution", "DeltaFlowAcceptedWatermark", "DeltaFlowIndexMarker",
          "DeltaFlowGreekStoreValue", "DeltaFlowOiState", "DeltaFlowPositioningState"]
NAMES = sorted(OUTPUTS + IMAGES)   # the script registers in this (sorted) order
SUBJECTS = [f"com.optionsedge.contracts.deltaflow.{n}" for n in NAMES]
OTHER = (FIXTURES / "DeltaFlowIndexMarker.avsc").read_text()


def run(url: str, directory: str, dry: bool = False):
    env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=directory, DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class EnsureDeltaFlowSchemas(unittest.TestCase):
    def test_the_fixture_set_is_exactly_the_seventeen_subjects_the_script_pins(self):
        self.assertEqual(len(SUBJECTS), 17)
        self.assertEqual(sorted(p.stem for p in FIXTURES.glob("*.avsc")), NAMES)
        pinned = dict(re.findall(r'"(DeltaFlow\w+)": \("[^"]+", "([0-9a-f]{64})"\)', SCRIPT.read_text()))
        self.assertEqual(sorted(pinned), NAMES)
        for n in NAMES:
            self.assertEqual(hashlib.sha256((FIXTURES / f"{n}.avsc").read_bytes()).hexdigest(), pinned[n], n)

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
                p = Path(d) / "DeltaFlowStrikeSnapshot.avsc"
                p.write_text(p.read_text().replace("Kafka key", "Kafka kEy"))
                rc, out = run(url, d)
            self.assertNotEqual(rc, 0)
            self.assertIn("not the canonical", out)
            self.assertEqual(reg.calls, [])
        finally:
            server.shutdown()

    def test_a_missing_file_is_refused_before_any_call(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            with tempfile.TemporaryDirectory() as d:
                for f in FIXTURES.glob("*.avsc"):
                    if f.stem != "DeltaFlowOiState":
                        shutil.copy(f, d)
                rc, out = run(url, d)
            self.assertNotEqual(rc, 0)
            self.assertEqual(reg.calls, [])
        finally:
            server.shutdown()

    def test_a_newer_version_after_the_canonical_one_is_refused(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            self.assertEqual(run(url, str(FIXTURES))[0], 0)
            reg.register(SUBJECTS[0], OTHER)
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

    def test_a_bad_last_subject_refuses_before_the_first_one_is_touched(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            reg.register(SUBJECTS[-1], OTHER)
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            self.assertNotIn(SUBJECTS[0], reg.subjects)
            self.assertEqual(reg.configs, {})
            self.assertEqual([c for c in reg.calls if c.startswith("PUT")], [])
        finally:
            server.shutdown()

    def test_an_unwritable_last_subject_refuses_before_any_mutation(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            reg.subject_modes[SUBJECTS[-1]] = "READONLY"
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            self.assertIn("not writable", out)
            self.assertEqual(reg.subjects, {})
            self.assertEqual([c for c in reg.calls if c.startswith(("PUT", "POST"))], [])
        finally:
            server.shutdown()

    def test_registration_failing_on_the_2nd_subject_leaves_only_canonical_v1_and_a_rerun_converges(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            reg.fail = [("POST", f"/subjects/{SUBJECTS[1]}/versions")]
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            self.assertIn("re-run", out)
            self.assertEqual(len(reg.subjects[SUBJECTS[0]]), 1)       # canonical v1 only
            for s in SUBJECTS[1:]:
                self.assertNotIn(s, reg.subjects)
            self.assertEqual(reg.configs, {})                          # FULL is set only after every registration
            reg.fail = []
            rc, out = run(url, str(FIXTURES))
            self.assertEqual(rc, 0, out)
            for s in SUBJECTS:
                self.assertEqual(len(reg.subjects[s]), 1)
                self.assertEqual(reg.configs[s], "FULL")
        finally:
            server.shutdown()

    def test_registration_failing_on_the_last_subject_leaves_only_canonical_v1_and_no_full_yet(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            reg.fail = [("POST", f"/subjects/{SUBJECTS[-1]}/versions")]
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            for s in SUBJECTS[:-1]:
                self.assertEqual(len(reg.subjects[s]), 1)
            self.assertNotIn(SUBJECTS[-1], reg.subjects)
            self.assertEqual(reg.configs, {})
            reg.fail = []
            self.assertEqual(run(url, str(FIXTURES))[0], 0)
            for s in SUBJECTS:
                self.assertEqual(reg.configs[s], "FULL")
        finally:
            server.shutdown()

    def test_config_failing_on_the_3rd_subject_leaves_idempotent_state_and_a_rerun_converges(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            reg.fail = [("PUT", f"/config/{SUBJECTS[2]}")]
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            self.assertIn("could not set FULL", out)
            for s in SUBJECTS:
                self.assertEqual(len(reg.subjects[s]), 1)              # all canonical, no wrong content anywhere
            self.assertEqual(reg.configs.get(SUBJECTS[0]), "FULL")
            self.assertEqual(reg.configs.get(SUBJECTS[1]), "FULL")
            self.assertNotIn(SUBJECTS[2], reg.configs)
            reg.fail = []
            self.assertEqual(run(url, str(FIXTURES))[0], 0)
            for s in SUBJECTS:
                self.assertEqual(len(reg.subjects[s]), 1)
                self.assertEqual(reg.configs[s], "FULL")
        finally:
            server.shutdown()

    def test_a_registry_read_failure_changes_nothing(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            reg.fail = [("GET", f"/subjects/{SUBJECTS[5]}/versions/latest")]
            rc, out = run(url, str(FIXTURES))
            self.assertNotEqual(rc, 0)
            self.assertIn("nothing was changed", out)
            self.assertEqual(reg.subjects, {})
            self.assertEqual([c for c in reg.calls if c.startswith(("PUT", "POST"))], [])
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
