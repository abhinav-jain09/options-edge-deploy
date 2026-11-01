"""options.spx.vix-option-inteligence-service.current Avro (contracts 0.53.0, fixed id 3001): scripts/kafka/ensure-fixed-id-schemas.sh with schema-id-maps/vix-option-inteligence.map against the
stand-in registry (tests/stub_schema_registry.py), the map's agreement with the other maps, and the wipe script's cluster guard."""
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
MAP = MAPS / "vix-option-inteligence.map"
SUBJECT = "com.optionsedge.contracts.zerodte.ZeroDteIntelligenceSnapshot"


def rows():
    out = []
    for line in MAP.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            out.append(line.split("|"))
    return out


def run(url: str, dry: bool = False):
    d = Path(tempfile.mkdtemp())
    (d / "ZeroDteIntelligenceSnapshot.avsc").write_text(json.dumps(
        {"type": "record", "name": "ZeroDteIntelligenceSnapshot", "namespace": "com.optionsedge.contracts.zerodte", "doc": "fixture", "fields": [{"name": "x", "type": "string"}]}))
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

    def test_the_map_declares_exactly_one_subject_under_3001(self):
        self.assertEqual(rows(), [[SUBJECT, "3001", "ZeroDteIntelligenceSnapshot.avsc"]])

    def test_a_fresh_registry_gets_the_subject_under_3001_with_full_and_a_rerun_changes_nothing(self):
        def check(reg, url):
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.subjects[SUBJECT][0]["id"], 3001)
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
            reg.register("some.other.Subject", json.dumps({"type": "string"}), id=3001, version=1)
            rc, out = run(url)
            self.assertEqual(rc, 1, out)
            self.assertIn("fixed id 3001 is already held by another schema", out)
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
                    self.assertNotIn(int(line.split("|")[1]), ours, f"{other.name} reuses an vix-option-inteligence id")


class WipeScriptGuards(unittest.TestCase):
    """The destructive script is bound to this topic's wire contract and shape (its mock test, roll-forward-vix-option-inteligence-test.sh, runs the behaviours)."""

    SCRIPT = (ROOT / "scripts/kafka/roll-forward-vix-option-inteligence.sh").read_text()

    def test_it_expects_exactly_the_fixed_id_in_hex(self):
        self.assertIn("RF_EXPECT_ID_HEX=00000bb9", self.SCRIPT)  # 3001 = 0x0bb9

    def test_it_recreates_the_topic_at_the_ensure_scripts_shape(self):
        ensure = (ROOT / "scripts/kafka/ensure-vix-option-inteligence-topic.sh").read_text()
        self.assertIn("PARTITIONS=32", ensure)
        self.assertIn("PARTITIONS=32", self.SCRIPT)
        self.assertIn('CONFIGS="cleanup.policy=compact,retention.ms=-1"', self.SCRIPT)
        self.assertIn("cleanup.policy=compact,retention.ms=${RETENTION_MS}", ensure)

    def test_every_cluster_is_bound_to_its_own_broker(self):
        for needle in ("dev:localhost:19092", "es4:192.168.100.4:9092", "production:192.168.100.252:9092"):
            self.assertIn(needle, self.SCRIPT)

    def test_the_mock_test_passes(self):
        r = subprocess.run(["bash", str(ROOT / "scripts/kafka/roll-forward-vix-option-inteligence-test.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])


if __name__ == "__main__":
    unittest.main()
