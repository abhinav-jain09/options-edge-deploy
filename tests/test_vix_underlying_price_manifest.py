"""underlying.vix.price is Avro (VixUnderlyingPrice, FIXED id 3061): the map, the manifest entry, the pinned snapshot, the contracts copy and the
MirrorMaker header must all say the same thing. The topic is byte-mirrored production -> es4, so one id has to name the schema in every registry."""
from __future__ import annotations

import hashlib
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUBJECT = "com.optionsedge.contracts.hpsf.VixUnderlyingPrice"
MANIFEST = json.loads((ROOT / "schema-manifest/manifest.json").read_text())
SNAPSHOT = ROOT / "schema-manifest/schemas" / SUBJECT / "v1.avsc"
FIXTURE = ROOT / "tests/fixtures/vix-underlying-price/VixUnderlyingPrice.avsc"


class VixUnderlyingPriceGovernance(unittest.TestCase):
    def entry(self):
        rows = [e for e in MANIFEST["entries"] if e.get("subject") == SUBJECT]
        self.assertEqual(len(rows), 1)
        return rows[0]

    def test_the_manifest_entry_is_one_fixed_id_in_every_environment(self):
        e = self.entry()
        self.assertEqual((e["kind"], e["id"], e["version"], e["family"]), ("MIRRORED", 3061, 1, "hpsf"))
        self.assertNotIn("idByEnvironment", e, "a byte-mirrored subject has ONE id everywhere")
        self.assertEqual(sorted(e["environments"]), ["dev", "es4", "prod"])
        self.assertIn([3061, 3061], MANIFEST["familyBlocks"]["hpsf"])
        consumers = {c["service"]: c for c in e["consumers"]}
        self.assertEqual(consumers["spot-vol-regime"]["accepts"], [3061])

    def test_the_snapshot_is_the_contracts_file_the_tests_pin(self):
        self.assertEqual(SNAPSHOT.read_bytes(), FIXTURE.read_bytes())
        record = json.loads(SNAPSHOT.read_text())
        self.assertEqual(record["namespace"] + "." + record["name"], SUBJECT)

    def test_the_map_registers_exactly_that_subject_and_id(self):
        rows = [l.split("|") for l in (ROOT / "scripts/kafka/schema-id-maps/vix-underlying-price.map").read_text().splitlines()
                if l.strip() and not l.startswith("#")]
        self.assertEqual(rows, [[SUBJECT, "3061", "VixUnderlyingPrice.avsc"]])

    def test_the_mirror_header_names_the_subject_and_the_generated_header_agrees(self):
        text = (ROOT / "infra/es4/mm2/mm2.properties").read_text()
        self.assertIn("# MIRROR-SUBJECTS: " + SUBJECT, text)
        mirrors = [a for a in MANIFEST["activeMirrors"] if "underlying.vix.price" in a["topics"]]
        self.assertEqual(len(mirrors), 1)
        self.assertEqual(mirrors[0]["subjects"], [SUBJECT])
        self.assertNotIn("unframed", mirrors[0])

    def test_the_feed_role_reads_the_registry_of_its_environment(self):
        base = (ROOT / "k8s/services/databento-vix-feed/base/databento-vix-feed-deployment.yaml").read_text()
        self.assertRegex(base, r"KAFKA_SCHEMA_REGISTRY_URL\s*\n\s*value: \"http://192\.168\.100\.252:8082\"")
        dev = (ROOT / "k8s/overlays/dev/databento-vix-feed-dev-patch.yaml").read_text()
        self.assertRegex(dev, r"KAFKA_SCHEMA_REGISTRY_URL\s*\n\s*value: http://host\.docker\.internal:8082")


if __name__ == "__main__":
    unittest.main()
