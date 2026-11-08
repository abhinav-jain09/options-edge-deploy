# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
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

    def test_only_the_dev_overlay_gives_the_feed_role_a_registry(self):
        """Production stays shut down with its registry untouched: the base/production render does NOT carry KAFKA_SCHEMA_REGISTRY_URL (the role exits
        without it), the dev overlay carries the dev registry."""
        for rel in ("k8s/services/databento-vix-feed/base/databento-vix-feed-deployment.yaml",
                    "k8s/services/databento-vix-feed/overlays/production/manifest.yaml"):
            text = (ROOT / rel).read_text()
            self.assertNotRegex(text, r"name: KAFKA_SCHEMA_REGISTRY_URL", rel)
        dev = (ROOT / "k8s/overlays/dev/databento-vix-feed-dev-patch.yaml").read_text()
        self.assertRegex(dev, r"KAFKA_SCHEMA_REGISTRY_URL\s*\n\s*value: http://host\.docker\.internal:8082")
        slice_dev = (ROOT / "k8s/services/databento-vix-feed/overlays/dev/manifest.yaml").read_text()
        self.assertRegex(slice_dev, r"KAFKA_SCHEMA_REGISTRY_URL\s*\n\s*value: http://host\.docker\.internal:8082")

    def test_no_ibkr_feed_can_write_the_canonical_topic(self):
        """The shared config still names the canonical topic for the CONSUMERS of KAFKA_IBKR_VIX_PRICE_TOPIC (gateway). The IBKR producer must never
        write it: dev points it at the hop topic, production keeps it at zero replicas, and options-edge-ibkr-feed's FeedConfig refuses the canonical
        name at boot (CANONICAL_VIX_PRICE_TOPIC)."""
        import yaml
        for env, expect in (("dev", "topic"), ("production", "off")):
            docs = [d for d in yaml.safe_load_all((ROOT / f"k8s/services/ibkr-feed/overlays/{env}/manifest.yaml").read_text())
                    if d and d.get("kind") == "Deployment" and d["metadata"]["name"] == "ibkr-feed-service"]
            self.assertEqual(len(docs), 1, env)
            if expect == "off":
                self.assertEqual(docs[0]["spec"]["replicas"], 0, env)
            if True:
                env_map = {e["name"]: e.get("value") for e in docs[0]["spec"]["template"]["spec"]["containers"][0]["env"]}
                self.assertEqual(env_map["KAFKA_IBKR_VIX_PRICE_TOPIC"], "underlying.vix.index.price")
        self.assertFalse(any("ibkr-feed" in str(p) for p in (ROOT / "k8s/es4/services").glob("*")), "no ibkr-feed on es4")


if __name__ == "__main__":
    unittest.main()
