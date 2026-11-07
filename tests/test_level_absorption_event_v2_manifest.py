"""Schema manifest wiring of the Level Absorption event version 2 (liquidity requirement A rev 12 section 8, work package K3; contracts 0.88.0).

futures.level-absorption.events-value has TWO versions: version 1 is the archival id 2702 (registered on dev, frozen) and version 2 (the optional book fields) is the new fixed id 2812.
The id 2812 is free in every registry the manifest knows (2703 to 2752 are held on es4 by auto-assigned subjects, so the next integer after 2702 was not usable). The producer is
level-absorption, every reader accepts both ids, and nothing is registered outside dev."""
from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"
SUBJECT = "futures.level-absorption.events-value"
V1, V2 = 2702, 2812


def manifest() -> dict:
    return json.loads((ROOT / "schema-manifest/manifest.json").read_text())


def snapshot_hash(rel: str) -> str:
    out = subprocess.run(["java", "-jar", str(JAR), "hash", str(ROOT / rel)], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
    return out


class EventVersionTwo(unittest.TestCase):
    def setUp(self):
        self.m = manifest()
        self.rows = {e["id"]: e for e in self.m["entries"] if e.get("subject") == SUBJECT}

    def test_the_subject_has_exactly_two_versions_under_the_two_ids(self):
        self.assertEqual(sorted(self.rows), [V1, V2])
        self.assertEqual((self.rows[V1]["version"], self.rows[V2]["version"]), (1, 2))
        for e in self.rows.values():
            self.assertEqual((e["kind"], e["family"], e["environments"], e["topics"]), ("MIRRORED", "futures", ["dev"], [{"name": "futures.level-absorption.events", "kind": "data"}]))
            self.assertTrue(e["retained"])

    def test_each_snapshot_hashes_to_its_entry_and_to_its_contracts_source(self):
        for i, snap in ((V1, "v1.avsc"), (V2, "v2.avsc")):
            e = self.rows[i]
            self.assertEqual(e["schemaFile"], f"{SUBJECT}/{snap}")
            self.assertEqual(snapshot_hash(f"schema-manifest/schemas/{SUBJECT}/{snap}"), e["schemaHash"], f"id {i}")
        self.assertEqual(self.rows[V1]["source"], {"kind": "FILE", "repo": "contracts", "path": "schemas/cvd-related/v1/LevelAbsorptionEvent.avsc"})
        self.assertEqual(self.rows[V2]["source"], {"kind": "FILE", "repo": "contracts", "path": "schemas/cvd-related/LevelAbsorptionEvent.avsc"})

    def test_version_two_is_version_one_plus_the_42_optional_book_fields(self):
        v1 = json.loads((ROOT / f"schema-manifest/schemas/{SUBJECT}/v1.avsc").read_text())
        v2 = json.loads((ROOT / f"schema-manifest/schemas/{SUBJECT}/v2.avsc").read_text())
        n1 = [f["name"] for f in v1["fields"]]
        n2 = [f["name"] for f in v2["fields"]]
        self.assertEqual(len(n1), 70)
        self.assertEqual(n2[:70], n1)
        added = v2["fields"][70:]
        self.assertEqual(len(added), 42)
        self.assertEqual(n2[70], "bookRead")
        self.assertEqual(n2[-1], "bookSource")
        for f in added:
            self.assertTrue(f["name"].startswith("book") or f["name"] == "contractTableDigest", f["name"])
            self.assertEqual((f["type"][0], f["default"]), ("null", None), f["name"])
        self.assertEqual({k: v for k, v in v1.items() if k != "fields"}, {k: v for k, v in v2.items() if k != "fields"})

    def test_the_new_id_is_unique_in_every_registry_the_manifest_knows_and_in_the_family_block(self):
        holders = []
        for e in self.m["entries"]:
            ids = [e.get("id")] + list((e.get("idByEnvironment") or {}).values())
            if V2 in ids:
                holders.append(e["subject"])
        self.assertEqual(holders, [SUBJECT])
        self.assertIn([V2, V2], self.m["familyBlocks"]["futures"])
        # the next integers after the manual line are held by other subjects on es4: that is why the id is not 2704
        held = {e["subject"].split(".")[-1] for e in self.m["entries"] if 2704 in list((e.get("idByEnvironment") or {}).values())}
        self.assertEqual(held, {"DeltaFlowCalibrationRecord"})

    def test_the_producer_is_level_absorption_and_every_reader_accepts_both_ids(self):
        self.assertEqual(self.rows[V2]["producers"], [{"service": "level-absorption", "environments": ["dev"]}])
        self.assertEqual(self.rows[V1]["producers"], [])
        for i in (V1, V2):
            self.assertEqual(self.rows[i]["consumers"], [{"service": "liquidity-validator", "accepts": [V1, V2], "environments": ["dev"]}], f"id {i}")

    def test_the_plan_of_each_bound_service_registers_both_versions_in_order(self):
        for svc in ("level-absorption", "liquidity-validator"):
            with tempfile.TemporaryDirectory() as out:
                run = subprocess.run(["java", "-jar", str(JAR), "plan", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--env", "dev", "--service", svc, "--out", out],
                                     capture_output=True, text=True, check=True)
                rows = [l for l in (Path(out) / "plan.map").read_text().splitlines() if l.startswith(SUBJECT)]
            self.assertEqual(rows, [f"{SUBJECT}|{V1}|{SUBJECT}/v1.avsc|1", f"{SUBJECT}|{V2}|{SUBJECT}/v2.avsc|2"], svc + run.stderr[-200:])


if __name__ == "__main__":
    unittest.main()
