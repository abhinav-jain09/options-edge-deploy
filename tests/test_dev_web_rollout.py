# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
#
# Dev has no spare CPU for a surge pod, so the dev web Deployment rolls with maxSurge 0 / maxUnavailable 1.
# Production and experiment keep the base strategy (maxSurge 1 / maxUnavailable 0).
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MONO = ROOT / "k8s" / "overlays" / "dev"
SLICE = ROOT / "k8s" / "services" / "web" / "overlays" / "dev"
PATCH = "web-dev-rollout-patch.yaml"


class DevWebRolloutTest(unittest.TestCase):
    def test_patch_is_identical_in_both_dev_overlays_and_sets_no_surge(self) -> None:
        mono = (MONO / PATCH).read_text()
        self.assertEqual(mono, (SLICE / PATCH).read_text())
        self.assertIn("name: options-edge-web", mono)
        self.assertIn("type: RollingUpdate", mono)
        self.assertIn("maxSurge: 0", mono)
        self.assertIn("maxUnavailable: 1", mono)

    def test_both_dev_kustomizations_apply_it_exactly_once(self) -> None:
        for overlay in (MONO, SLICE):
            text = (overlay / "kustomization.yaml").read_text()
            self.assertEqual(text.count(f"- path: {PATCH}"), 1, overlay)

    def test_rendered_strategy_per_environment_and_overlay(self) -> None:
        import subprocess

        import yaml

        def web_strategy(overlay: Path) -> dict:
            out = subprocess.run(["kubectl", "kustomize", str(overlay)], check=True, capture_output=True, text=True).stdout
            found = [d for d in yaml.safe_load_all(out)
                     if d and d.get("kind") == "Deployment" and d["metadata"]["name"] == "options-edge-web"]
            self.assertEqual(len(found), 1, overlay)
            return found[0]["spec"]["strategy"]

        no_surge = {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1}}
        base = {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}}
        self.assertEqual(web_strategy(MONO), no_surge)
        self.assertEqual(web_strategy(SLICE), no_surge)
        for env in ("production", "experiment"):
            self.assertEqual(web_strategy(ROOT / "k8s" / "overlays" / env), base, env)
            self.assertEqual(web_strategy(ROOT / "k8s" / "services" / "web" / "overlays" / env), base, env)


if __name__ == "__main__":
    unittest.main()
