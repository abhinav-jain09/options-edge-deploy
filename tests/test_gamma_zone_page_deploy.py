# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The gamma-zone RELAY flag on the feed-gateway, which is what lets the page see anything.

gamma-zone-service writes options.spx.gamma-zone.{current,profile}. The gateway relays both only
when GAMMA_ZONE_ENABLED is set, and the page at /gamma-zone reads both: the measurement, and the
profile that answers "how many contracts traded between these two strikes".

DEV ONLY, and the asymmetry matters in one direction: gamma-zone-service is declared envs: [dev], so
on production the flag would subscribe the gateway to two topics that do not exist there. Production
stays default-off by NOT setting it, which is the same shape as GAMMA_NAVIGATION_ENABLED beside it.
"""
from __future__ import annotations

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEV_PATCH = ROOT / "k8s/overlays/dev/feed-gateway-dev-env-patch.yaml"
DEV_SLICE = ROOT / "k8s/services/feed-gateway/overlays/dev/manifest.yaml"
SERVICES = ROOT / "services.yaml"
FLAG = "GAMMA_ZONE_ENABLED"


def env_of(path: Path) -> dict:
    """The container env of the Deployment in this file.

    safe_load_all, not safe_load: the generated slice is a MULTI-document file (a Service beside the
    Deployment), and safe_load raises on the second document rather than returning the first.
    """
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d and d.get("kind") == "Deployment"]
    assert len(docs) == 1, f"expected exactly one Deployment in {path}, got {len(docs)}"
    containers = docs[0]["spec"]["template"]["spec"]["containers"]
    for container in containers:
        env = {e["name"]: e.get("value") for e in container.get("env", [])}
        if FLAG in env:
            return env
    return {e["name"]: e.get("value") for e in containers[0].get("env", [])}


class RelayFlag(unittest.TestCase):
    def test_the_relay_is_ON_for_dev(self):
        self.assertEqual("true", env_of(DEV_PATCH).get(FLAG))

    def test_the_generated_slice_carries_it_too(self):
        """The patch is the source; the slice is what service-deploy applies. A flag in one and not
        the other is the shape that looks configured and deploys off."""
        self.assertEqual("true", env_of(DEV_SLICE).get(FLAG))

    def test_production_does_NOT_set_it(self):
        """gamma-zone-service is dev-only, so on production the flag would subscribe the gateway to
        two topics that do not exist. Default-off by absence, like GAMMA_NAVIGATION_ENABLED."""
        for path in sorted((ROOT / "k8s/overlays/production").glob("*feed-gateway*.yaml")):
            self.assertNotIn(FLAG, path.read_text(), f"{path.name} must not set {FLAG}")
        prod_slice = ROOT / "k8s/services/feed-gateway/overlays/production/manifest.yaml"
        if prod_slice.is_file():
            self.assertNotIn(FLAG, prod_slice.read_text())

    def test_the_service_it_relays_is_still_dev_only(self):
        """If gamma-zone ever gains a production environment this test should fail, so the decision
        above is re-taken deliberately instead of silently becoming wrong."""
        text = SERVICES.read_text()
        line = [l for l in text.splitlines() if "name: gamma-zone," in l]
        self.assertEqual(1, len(line), "exactly one gamma-zone service entry")
        self.assertIn("envs: [dev]", line[0],
                      "gamma-zone is no longer dev-only: re-take the production relay decision")


if __name__ == "__main__":
    unittest.main()
