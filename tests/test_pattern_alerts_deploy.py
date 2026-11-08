# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""Deploy wiring of the server-side pattern break alerts (processing market-structure-service/docs/PATTERN-ALERTS-SERVER-SIDE-DESIGN.md sections 3.3, 4, 8; requirement AL-09, AL-10, AL-19).

DEV ONLY: two MIRRORED manifest entries (ids 2224, 2225), two one-partition topics with one week of retention (declared in topics.env, ensured by the market-structure barrier only for
ENVIRONMENT=dev, kept by the dev clean), and the two flags on in the dev overlays. Nothing about the feature may appear in a production or experiment render, the production overlays, or es4."""
from __future__ import annotations

import json
import os
import subprocess
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
REGULAR, ESTIMATE = "options.spx.market-structure.pattern-alerts", "options.spx.market-structure.estimate.pattern-alerts"
WEEK = "604800000"
EVENT = "com.optionsedge.contracts.marketstructure.alerts.PatternAlertEvent"
MARK = "com.optionsedge.contracts.marketstructure.alerts.PatternAlertWatermark"


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def sourced(environment: str | None) -> dict[str, list[str]]:
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    if environment is not None:
        env["ENVIRONMENT"] = environment
    script = (f'source "{ROOT}/scripts/kafka/topics.env"; for v in $(compgen -v OPTIONS_EDGE_); do '
              'case "${!v}" in *pattern-alerts*) echo "$v=$(printf "%s\\n" ${!v} | grep pattern-alerts | tr "\\n" " ")";; esac; done')
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
    return {l.split("=", 1)[0]: l.split("=", 1)[1].split() for l in out.splitlines()}


class Manifest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = json.loads(read("schema-manifest/manifest.json"))
        cls.by = {e["subject"]: e for e in cls.m["entries"] if e["subject"] in (EVENT, MARK)}

    def test_two_dev_only_mirrored_entries_with_the_designed_participants(self):
        self.assertEqual({s: e["id"] for s, e in self.by.items()}, {EVENT: 2224, MARK: 2225})
        for s, e in self.by.items():
            self.assertEqual((e["kind"], e["version"], e["family"], e["environments"]), ("MIRRORED", 1, "marketstructure", ["dev"]), s)
            self.assertNotIn("idByEnvironment", e)
            self.assertNotIn("pending", e, "the contracts files are on main (0.67.0), so pending would be refused as PENDING_LANDED")
            name = s.rsplit(".", 1)[1]
            self.assertEqual(e["source"], {"kind": "FILE", "repo": "contracts", "path": f"schemas-registry/ta/{name}.avsc"})
            self.assertEqual([t["name"] for t in e["topics"]], [REGULAR, ESTIMATE])
            self.assertEqual(e["producers"], [{"service": "market-structure", "environments": ["dev"]}])
            self.assertEqual(e["consumers"], [{"service": "feed-gateway", "accepts": [2224, 2225], "environments": ["dev"]}])
            self.assertTrue((ROOT / "schema-manifest/schemas" / e["schemaFile"]).is_file(), e["schemaFile"])

    def test_the_family_block_gains_exactly_the_two_ids(self):
        ranges = self.m["familyBlocks"]["marketstructure"]
        self.assertIn([2224, 2224], ranges)
        self.assertIn([2225, 2225], ranges)

    def test_the_fixed_id_map_is_untouched_so_a_production_registration_run_cannot_import_them(self):
        self.assertNotIn("PatternAlert", read("scripts/kafka/schema-id-maps/market-structure.map"))


class Topics(unittest.TestCase):
    def test_dev_and_unset_declare_both_topics_one_partition_one_week(self):
        for environment in ("dev", None):
            got = sourced(environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_TOPICS"]), sorted([f"{REGULAR}:1", f"{ESTIMATE}:1"]), environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"]), sorted([REGULAR, ESTIMATE]), environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"]), sorted([f"{REGULAR}={WEEK}", f"{ESTIMATE}={WEEK}"]), environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"]), sorted([REGULAR, ESTIMATE]), environment)
            for compacting in ("OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"):
                self.assertNotIn(compacting, got, "a delete-policy audit topic is in no compact or reset class")

    def test_an_explicit_production_source_declares_nothing_about_them_anywhere(self):
        self.assertEqual(sourced("production"), {})

    def test_only_the_exact_string_production_strips(self):
        self.assertIn("OPTIONS_EDGE_TOPICS", sourced("Production "))
        self.assertIn("OPTIONS_EDGE_TOPICS", sourced("es4"))

    def test_production_keeps_every_other_declaration(self):
        """Production strips the two alert topics from the preserved list and keeps everything else.

        This used to assert `dev_count - prod_count == 2`, a count of ALL dev-only preserved topics, and
        its own comment already said not to do that: "compare the alert delta through the token lists, not
        the totals". It then compared the totals. So it broke the moment a third, unrelated dev-only
        preserved topic was declared - es.rithmic.mbo - and reported `3 != 2`, which reads as "production
        is wrong about the alerts" when nothing about the alerts had changed. That failure is the TA-books
        step of deploy-validation, and because the job is one `bash -e` script it skipped every later step.

        Named, not counted. The two alert topics are the only names this test owns, so it asserts their
        membership directly, plus the real invariant that bounds everything else: production's preserved
        list must be a SUBSET of dev's. A new dev-only topic now passes; a topic preserved on production
        but not dev, or an alert topic leaking into production, still fails.
        """
        def preserved(environment):
            env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
            env["ENVIRONMENT"] = environment
            # env -u BASH_ENV: a non-interactive bash sources BASH_ENV first, which could inject tokens.
            env.pop("BASH_ENV", None)
            out = subprocess.run(
                ["bash", "-c", f'source "{ROOT}/scripts/kafka/topics.env"; '
                               'printf "%s\n" $OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS'],
                capture_output=True, text=True, check=True, env=env).stdout
            return [t for t in out.split() if t]

        dev, prod = preserved("dev"), preserved("production")
        # Non-vacuity first: two empty lists would satisfy every assertion below.
        self.assertTrue(dev, "ENVIRONMENT=dev declared no preserved topics at all")
        self.assertTrue(prod, "ENVIRONMENT=production declared no preserved topics at all")

        for topic in (REGULAR, ESTIMATE):
            self.assertIn(topic, dev, f"{topic} must be preserved by the dev clean")
            self.assertNotIn(topic, prod, f"{topic} must not be preserved on production - it is dev only")

        self.assertEqual([], sorted(set(prod) - set(dev)),
                         "production preserves a topic dev does not; the dev clean would then delete a "
                         "topic production protects")
        # The alert topics must be the only thing their own declaration removes: everything else dev
        # preserves and production does not has to come from some OTHER dev-only block, never from this
        # feature's. Assert that by name rather than by count, so an unrelated dev-only topic cannot
        # break this test again.
        self.assertEqual({REGULAR, ESTIMATE}, (set(dev) - set(prod)) & {REGULAR, ESTIMATE},
                         "both alert topics must be exactly the dev-only part this feature contributes")

    def test_the_barrier_names_both_topics_and_ensures_them_only_for_dev(self):
        text = read("scripts/kafka/ensure-market-structure-topics.sh")
        self.assertIn(f'PATTERN_ALERTS_TOPIC="{REGULAR}"', text)
        self.assertIn(f'PATTERN_ALERTS_ESTIMATE_TOPIC="{ESTIMATE}"', text)
        self.assertIn(f"PATTERN_ALERTS_RETENTION_FLOOR={WEEK}", text)
        self.assertIn('if [ "${ENVIRONMENT:-}" = "dev" ]; then', text)


class Flags(unittest.TestCase):
    def env_of(self, rel: str, container: str) -> dict[str, str]:
        out = {}
        for doc in yaml.safe_load_all(read(rel)):
            if doc and doc.get("kind") == "Deployment":
                for c in doc["spec"]["template"]["spec"]["containers"]:
                    if c["name"] == container:
                        out.update({e["name"]: e.get("value") for e in c.get("env", [])})
        return out

    def test_dev_patches_and_committed_slices_turn_the_flags_on(self):
        for rel in ("k8s/overlays/dev/market-structure-dev-env-patch.yaml", "k8s/services/market-structure/overlays/dev/manifest.yaml"):
            self.assertEqual(self.env_of(rel, "market-structure")["MARKET_STRUCTURE_PATTERN_ALERTS_ENABLED"], "true", rel)
            self.assertEqual(self.env_of(rel, "market-structure")["MARKET_STRUCTURE_GEOMETRY_ENABLED"], "true", "the alerts need the geometry fold")
        for rel in ("k8s/overlays/dev/market-structure-estimate-dev-patch.yaml", "k8s/services/market-structure/overlays/dev/manifest.yaml"):
            self.assertEqual(self.env_of(rel, "market-structure-estimate")["MARKET_STRUCTURE_PATTERN_ALERTS_ENABLED"], "true", rel)
            self.assertEqual(self.env_of(rel, "market-structure-estimate")["MARKET_STRUCTURE_GEOMETRY_ENABLED"], "true", rel)
        for rel in ("k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/services/feed-gateway/overlays/dev/manifest.yaml"):
            names = {}
            for doc in yaml.safe_load_all(read(rel)):
                if doc and doc.get("kind") == "Deployment":
                    for c in doc["spec"]["template"]["spec"]["containers"]:
                        names.update({e["name"]: e.get("value") for e in c.get("env", [])})
            self.assertEqual(names.get("GATEWAY_PATTERN_ALERTS_ENABLED"), "true", rel)

    def test_no_production_experiment_or_es4_file_mentions_the_feature(self):
        needles = ("PATTERN_ALERTS", "pattern-alerts", "PatternAlert")
        for rel in ("k8s/overlays/production", "k8s/overlays/experiment", "k8s/es4", "k8s/base", "k8s/services/market-structure/overlays/production",
                    "k8s/services/feed-gateway/overlays/production", "k8s/services/feed-gateway/overlays/experiment", "image-tags/production.yaml", "scripts/es4"):
            path = ROOT / rel
            if not path.exists():
                continue
            files = [path] if path.is_file() else [p for p in path.rglob("*") if p.is_file()]
            for f in files:
                text = f.read_text(errors="ignore")
                for n in needles:
                    self.assertNotIn(n, text, f"{f.relative_to(ROOT)} must not mention {n}")


if __name__ == "__main__":
    unittest.main()
