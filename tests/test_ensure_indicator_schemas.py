"""scripts/kafka/ensure-indicator-schemas.sh against a stand-in registry (tests/stub_schema_registry.py): the
seven fixed ids are imported under FULL, re-runs are no-ops that still enforce FULL, collisions fail, the mode a
subject had is restored its own way (an override by PUT, an inherited mode by DELETE), and a registry that
forbids subject-level IMPORT is refused by the probe before any subject is touched."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-indicator-schemas.sh"
PARITY = ROOT / "scripts/kafka/assert-indicator-schema-parity.sh"
NAMES = ["IndicatorSnapshot", "IndicatorBar", "BackfillManifest", "IndicatorSignals", "SignalEvent", "SpotTick",
         "IndicatorStateImage"]
IDS = dict(zip(NAMES, range(2001, 2008)))
NS = "com.optionsedge.contracts.indicators.avro."


def schemas_dir() -> Path:
    d = Path(tempfile.mkdtemp())
    for n in NAMES:
        (d / f"{n}.avsc").write_text(json.dumps({"type": "record", "name": n, "namespace": NS[:-1],
                                                 "doc": "fixture", "fields": [{"name": "x", "type": "string"}]}))
    return d


def run(url: str, dry: bool = False, env: dict | None = None):
    e = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=str(schemas_dir()), DEPLOY_DRY_RUN="true" if dry else "false")
    e.update(env or {})
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=e)
    return r.returncode, r.stdout + r.stderr


class EnsureIndicatorSchemas(unittest.TestCase):
    def test_fresh_registry_gets_the_seven_fixed_ids_under_full_and_no_mode_left_behind(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            for n in NAMES:
                s = NS + n
                self.assertEqual(reg.subjects[s][0]["id"], IDS[n], n)
                self.assertEqual(reg.subjects[s][0]["version"], 1)
                self.assertEqual(reg.configs[s], "FULL")
            self.assertEqual(reg.subject_modes, {}, "an inherited mode is restored by DELETE, never by a PUT that leaves an override")
            self.assertNotIn("zz.indicator.importmode.probe", reg.subjects)
            self.assertIn("probe: subject-level IMPORT allowed", out)
        finally:
            server.shutdown()

    def test_rerun_is_a_no_op_that_still_enforces_full(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            self.assertEqual(run(url)[0], 0)
            reg.configs[NS + "IndicatorBar"] = "BACKWARD"   # left at the default by someone else
            calls_before = len(reg.calls)
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.configs[NS + "IndicatorBar"], "FULL")
            self.assertIn("compatibility set to FULL and read back", out)
            self.assertFalse(any(c.startswith("POST /subjects/") and c.endswith("/versions {schema…}") for c in reg.calls[calls_before:]),
                             "nothing was re-imported")
            self.assertEqual(len([s for s in reg.subjects.values() for _ in s]), 7)
        finally:
            server.shutdown()

    def test_fixed_id_held_by_another_schema_fails(self):
        reg = Registry()
        reg.register("some.other.Subject", json.dumps({"type": "string"}), id=2003, version=1)
        server, url = serve(reg)
        try:
            rc, out = run(url)
            self.assertEqual(rc, 1)
            self.assertIn("fixed id 2003 is already held by another schema", out)
            self.assertIn(NS + "IndicatorSnapshot", reg.subjects, "the subjects before the collision were still imported")
            self.assertNotIn(NS + "BackfillManifest", reg.subjects)
        finally:
            server.shutdown()

    def test_subject_holding_a_different_schema_fails(self):
        reg = Registry()
        reg.register(NS + "SpotTick", json.dumps({"type": "record", "name": "SpotTick", "fields": []}), id=77, version=1)
        server, url = serve(reg)
        try:
            rc, out = run(url)
            self.assertEqual(rc, 1)
            self.assertIn("exists with versions [1] but not the canonical schema", out)
        finally:
            server.shutdown()

    def test_an_original_subject_override_is_restored_by_put(self):
        reg = Registry()
        reg.subject_modes[NS + "IndicatorSignals"] = "READONLY"
        server, url = serve(reg)
        try:
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.subject_modes, {NS + "IndicatorSignals": "READONLY"})
            self.assertIn(f'PUT /mode/{NS}IndicatorSignals {{"mode": "READONLY"}}', reg.calls)
        finally:
            server.shutdown()

    def test_registry_without_mode_mutability_is_refused_by_the_probe(self):
        reg = Registry(mutable=False)
        server, url = serve(reg)
        try:
            rc, out = run(url)
            self.assertEqual(rc, 1)
            self.assertIn("subject-level IMPORT is not allowed", out)
            self.assertEqual(reg.subjects, {}, "no subject was touched")
        finally:
            server.shutdown()

    def test_dry_run_mutates_nothing(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url, dry=True)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.subjects, {})
            self.assertEqual(reg.subject_modes, {})
            self.assertFalse(any(c.startswith(("PUT", "POST /subjects/" + NS)) and "/versions" in c for c in reg.calls))
        finally:
            server.shutdown()

    def test_parity_passes_on_identical_ids_and_fails_on_a_differing_one(self):
        a, b = Registry(), Registry()
        sa, ua = serve(a)
        sb, ub = serve(b)
        try:
            self.assertEqual(run(ua)[0], 0)
            self.assertEqual(run(ub)[0], 0)
            r = subprocess.run(["bash", str(PARITY), ua, ub], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn("ids 2001-2007 identical", r.stdout)
            b.by_id[2005] = json.dumps({"type": "record", "name": "SignalEvent", "namespace": NS[:-1], "fields": [{"name": "y", "type": "int"}]})
            r = subprocess.run(["bash", str(PARITY), ua, ub], capture_output=True, text=True)
            self.assertEqual(r.returncode, 1)
            self.assertIn("id 2005 differs", r.stdout)
            del b.by_id[2006]
            r = subprocess.run(["bash", str(PARITY), ua, ub], capture_output=True, text=True)
            self.assertIn("id 2006 absent on target", r.stdout)
        finally:
            sa.shutdown()
            sb.shutdown()


if __name__ == "__main__":
    unittest.main()
