# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The What-If SIGNAL reader flag on the feed-gateway (C-250), which is what puts the line on the page.

whatif-map-service publishes the signal layer's line on options.spx.whatif.signal (dev only, behind
OE_WHATIF_SIGNAL_ENABLED). The gateway reads it only when WHATIF_SIGNAL_READER_ENABLED is set, and
GET /api/what-if/line serves it to the What-If page.

DEV ONLY: production stays default-off by NOT setting it, the same shape as GAMMA_NAVIGATION_ENABLED.
"""
from __future__ import annotations

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEV_PATCH = ROOT / "k8s/overlays/dev/feed-gateway-dev-env-patch.yaml"
DEV_SLICE = ROOT / "k8s/services/feed-gateway/overlays/dev/manifest.yaml"
FLAG = "WHATIF_SIGNAL_READER_ENABLED"


def env_of(path: Path) -> dict:
    """The container env of the one Deployment in this file (the slice is multi-document)."""
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d and d.get("kind") == "Deployment"]
    assert len(docs) == 1, f"expected exactly one Deployment in {path}, got {len(docs)}"
    containers = docs[0]["spec"]["template"]["spec"]["containers"]
    for container in containers:
        env = {e["name"]: e.get("value") for e in container.get("env", [])}
        if FLAG in env:
            return env
    return {e["name"]: e.get("value") for e in containers[0].get("env", [])}


class SignalReaderFlag(unittest.TestCase):
    def test_the_reader_is_ON_for_dev(self):
        self.assertEqual("true", env_of(DEV_PATCH).get(FLAG))

    def test_the_generated_slice_carries_it_too(self):
        """The patch is the source; the slice is what service-deploy applies. A flag in one and not
        the other looks configured and deploys off."""
        self.assertEqual("true", env_of(DEV_SLICE).get(FLAG))

    def test_production_does_NOT_set_it(self):
        """The signal layer publishes on dev only; production is out of scope for this change."""
        for path in sorted((ROOT / "k8s/overlays/production").glob("*feed-gateway*.yaml")):
            self.assertNotIn(FLAG, path.read_text(), f"{path.name} must not set {FLAG}")
        prod_slice = ROOT / "k8s/services/feed-gateway/overlays/production/manifest.yaml"
        if prod_slice.is_file():
            self.assertNotIn(FLAG, prod_slice.read_text())

    def test_the_base_does_NOT_set_it(self):
        """A figure in k8s/base reaches production too."""
        for path in sorted((ROOT / "k8s/base").rglob("*.yaml")):
            self.assertNotIn(FLAG, path.read_text(), f"{path} must not set {FLAG}")


if __name__ == "__main__":
    unittest.main()
