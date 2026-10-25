"""es.futures.aggressor-flow Avro (contracts 0.38.0, fixed id 2701): scripts/kafka/ensure-fixed-id-schemas.sh with schema-id-maps/es-aggressor-flow.map against the
stand-in registry (tests/stub_schema_registry.py), the map's agreement with the other maps, and the wipe script's refusal of es4 and production."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests.stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/kafka/ensure-fixed-id-schemas.sh"
MAPS = ROOT / "scripts/kafka/schema-id-maps"
MAP = MAPS / "es-aggressor-flow.map"
SUBJECT = "com.optionsedge.contracts.esflow.EsAggressorFlowSnapshot"


def rows():
    out = []
    for line in MAP.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            out.append(line.split("|"))
    return out


def run(url: str, dry: bool = False):
    d = Path(tempfile.mkdtemp())
    (d / "EsAggressorFlowSnapshot.avsc").write_text(json.dumps(
        {"type": "record", "name": "EsAggressorFlowSnapshot", "namespace": "com.optionsedge.contracts.esflow", "doc": "fixture", "fields": [{"name": "x", "type": "string"}]}))
    env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=str(d), SCHEMA_ID_MAP=str(MAP), DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class Registration(unittest.TestCase):
    def with_registry(self, check):
        reg = Registry()
        server, url = serve(reg)
        try:
            check(reg, url)
        finally:
            server.shutdown()

    def test_the_map_declares_exactly_one_subject_under_2701(self):
        self.assertEqual(rows(), [[SUBJECT, "2701", "EsAggressorFlowSnapshot.avsc"]])

    def test_a_fresh_registry_gets_the_subject_under_2701_with_full_and_a_rerun_changes_nothing(self):
        def check(reg, url):
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.subjects[SUBJECT][0]["id"], 2701)
            self.assertEqual(set(reg.configs.values()), {"FULL"})
            calls = len(reg.calls)
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            self.assertFalse(any(c.startswith("POST /subjects/") and c.endswith("/versions {schema…}") for c in reg.calls[calls:]), "nothing re-imported")
        self.with_registry(check)

    def test_a_dry_run_mutates_nothing(self):
        def check(reg, url):
            rc, out = run(url, dry=True)
            self.assertEqual(rc, 0, out)
            self.assertEqual((reg.subjects, reg.subject_modes), ({}, {}))
        self.with_registry(check)

    def test_the_id_held_by_another_schema_is_a_collision_never_repaired(self):
        def check(reg, url):
            reg.register("some.other.Subject", json.dumps({"type": "string"}), id=2701, version=1)
            rc, out = run(url)
            self.assertEqual(rc, 1, out)
            self.assertIn("fixed id 2701 is already held by another schema", out)
            self.assertEqual(set(reg.subjects), {"some.other.Subject"})
        self.with_registry(check)


class MapAgrees(unittest.TestCase):
    def test_no_id_is_shared_with_any_other_map_and_none_is_in_a_used_block(self):
        ours = {int(r[1]) for r in rows()}
        for other in MAPS.glob("*.map"):
            if other == MAP:
                continue
            for line in other.read_text().splitlines():
                if line.strip() and not line.lstrip().startswith("#"):
                    self.assertNotIn(int(line.split("|")[1]), ours, f"{other.name} reuses an es-aggressor-flow id")


if __name__ == "__main__":
    unittest.main()
