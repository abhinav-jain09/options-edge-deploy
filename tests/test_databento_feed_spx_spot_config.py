# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""SPX Spot Price Source (options-edge-databento-feed PR 196): the config that feed image can start with.

The new feed REFUSES TO START while a retired setting is in its environment, so a retired key left in ANY
ConfigMap or Deployment env that reaches the databento-feed image is an outage, not a stale line. This test parses the
YAML (comments cannot hide or fake a key) and fails on the first one.
"""
from __future__ import annotations

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

RETIRED = {
    "DATABENTO_CASCADE_TIER_ORDER",
    "DATABENTO_CASCADE_OFF_HOURS_TIER_ORDER",
    "DATABENTO_ES_ANNUAL_RATE",
    "DATABENTO_ES_ANNUAL_DIV_YIELD",
    "DATABENTO_ES_BASIS_ROLL_BUFFER_DAYS",
    "DATABENTO_ES_BASIS_DTE_OVERRIDE",
}
# Tier 1 is on by default in the feed: these must not be needed to turn it on (the bootstrap and topic default).
DEFAULTED_TIER1 = {"SPX_INDEX_SOURCE_ENABLED", "SPX_INDEX_SOURCE_BOOTSTRAP", "SPX_INDEX_SOURCE_TOPIC"}
CASCADE_SOURCES = {"CASCADED", "NATIVE_SPX_INDEX", "ES_BASIS_DERIVED"}


def documents(path: Path):
    try:
        for doc in yaml.safe_load_all(path.read_text()):
            if isinstance(doc, dict):
                yield doc
    except yaml.YAMLError:
        return        # kustomize patch fragments with templating are not this test's business


def env_of(deployment: dict) -> dict[str, object]:
    out: dict[str, object] = {}
    spec = deployment.get("spec", {}).get("template", {}).get("spec", {})
    for container in (spec.get("containers") or []) + (spec.get("initContainers") or []):
        for item in container.get("env") or []:
            out[item["name"]] = item.get("value", item.get("valueFrom"))
    return out


class RetiredSettingsAreGoneEverywhere(unittest.TestCase):
    def test_no_yaml_under_k8s_sets_a_retired_key(self) -> None:
        offenders = []
        for path in sorted((ROOT / "k8s").rglob("*.yaml")):
            for doc in documents(path):
                keys = set((doc.get("data") or {}).keys()) if doc.get("kind") == "ConfigMap" else set()
                if doc.get("kind") in {"Deployment", "StatefulSet", "CronJob"}:
                    keys |= set(env_of(doc))
                    # a strategic-merge patch fragment has the same shape as a Deployment
                hit = keys & RETIRED
                if hit:
                    offenders.append(f"{path.relative_to(ROOT)}: {sorted(hit)}")
        self.assertEqual([], offenders, "the new feed refuses to start with these keys")

    def test_no_feed_configmap_still_spells_out_the_tier_one_defaults(self) -> None:
        offenders = []
        for rel in ("k8s/infra/base/databento-feed-configmap.yaml",
                    "k8s/infra/overlays/dev/configmap-dev-local-patch.yaml",
                    "k8s/infra/overlays/production/configmap-prod-feed-synthetic-spot-patch.yaml"):
            for doc in documents(ROOT / rel):
                hit = set((doc.get("data") or {}).keys()) & DEFAULTED_TIER1
                if hit:
                    offenders.append(f"{rel}: {sorted(hit)}")
        self.assertEqual([], offenders)

    def test_the_group_ids_stay(self) -> None:
        keys = {}
        for env, rel in (("dev", "k8s/infra/overlays/dev/configmap-dev-local-patch.yaml"),
                         ("prod", "k8s/infra/overlays/production/configmap-prod-feed-synthetic-spot-patch.yaml")):
            for doc in documents(ROOT / rel):
                keys.setdefault(env, {}).update(doc.get("data") or {})
        self.assertEqual("dev-feed-spx-index", keys["dev"]["SPX_INDEX_SOURCE_GROUP_ID"])
        self.assertEqual("prod-feed-spx-index", keys["prod"]["SPX_INDEX_SOURCE_GROUP_ID"])


class CloseBasisDatabaseAndMode(unittest.TestCase):
    def feed_env(self, env_name: str) -> dict[str, object]:
        path = ROOT / "k8s/services/databento-feed/overlays" / env_name / "manifest.yaml"
        for doc in documents(path):
            if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == "options-edge-databento-feed":
                return env_of(doc)
        self.fail(f"no databento-feed Deployment in {path}")

    def test_cascaded_environments_wire_the_close_basis_database_to_the_platform_postgres(self) -> None:
        for env_name in ("dev", "production"):
            with self.subTest(env=env_name):
                env = self.feed_env(env_name)
                self.assertEqual(
                    {"configMapKeyRef": {"key": "POSTGRES_JDBC_URL", "name": "options-edge-config"}},
                    env["SPX_BASIS_PG_URL"])
                self.assertEqual(
                    {"configMapKeyRef": {"key": "POSTGRES_USER", "name": "options-edge-config"}},
                    env["SPX_BASIS_PG_USER"])
                self.assertEqual(
                    {"secretKeyRef": {"key": "POSTGRES_PASSWORD", "name": "options-edge-runtime-secrets"}},
                    env["SPX_BASIS_PG_PASSWORD"])

    def test_the_spot_basis_is_the_one_measured_at_the_close_not_intraday_samples(self) -> None:
        # Tier 2 is ES minus the basis MEASURED AT THE CASH CLOSE (anchor + trading-day projection). Samples would
        # install a different (MEASURED) level. The A1 certification automation may still record PASS records; they
        # no longer switch samples on.
        for env_name in ("dev", "production"):
            with self.subTest(env=env_name):
                self.assertEqual("false", self.feed_env(env_name)["ES_SPX_BASIS_SAMPLES_ENABLED"])

    def test_dev_runs_the_cascade_and_production_too(self) -> None:
        self.assertEqual("CASCADED", self.feed_env("dev")["DATABENTO_SPOT_SOURCE"])
        prod_cm = {}
        for doc in documents(ROOT / "k8s/infra/overlays/production/configmap-prod-feed-synthetic-spot-patch.yaml"):
            prod_cm.update(doc.get("data") or {})
        self.assertEqual("CASCADED", prod_cm["DATABENTO_SPOT_SOURCE"])

    def test_the_experiment_feed_is_not_a_cascade_so_it_needs_no_close_basis_database(self) -> None:
        # If experiment ever moves to the cascade this must fail: it then needs the SPX_BASIS_PG_* refs too.
        base = {}
        for doc in documents(ROOT / "k8s/infra/base/databento-feed-configmap.yaml"):
            base.update(doc.get("data") or {})
        overrides = {}
        for rel in ("k8s/infra/overlays/experiment/configmap-dev-local-patch.yaml",):
            for doc in documents(ROOT / rel):
                overrides.update(doc.get("data") or {})
        source = (overrides.get("DATABENTO_SPOT_SOURCE") or base["DATABENTO_SPOT_SOURCE"]).upper()
        self.assertNotIn(source, CASCADE_SOURCES)
        env = self.feed_env("experiment")
        self.assertNotIn("SPX_BASIS_PG_URL", env)
        self.assertNotIn("DATABENTO_SPOT_SOURCE", env)


if __name__ == "__main__":
    unittest.main()
