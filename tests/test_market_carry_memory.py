# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


class MarketCarryMemoryTest(unittest.TestCase):
    def test_production_restore_has_native_memory_headroom(self) -> None:
        rendered = subprocess.run(
            ["kubectl", "kustomize", str(ROOT / "k8s/overlays/production")],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        documents = list(yaml.safe_load_all(rendered))
        deployment = next(
            item
            for item in documents
            if item
            and item.get("kind") == "Deployment"
            and item.get("metadata", {}).get("name") == "market-carry-service"
        )
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        env = {item["name"]: item["value"] for item in container["env"]}

        self.assertEqual(7200, deployment["spec"]["progressDeadlineSeconds"])
        self.assertEqual("-Xms512m -Xmx2048m", env["JAVA_TOOL_OPTIONS"])
        self.assertEqual("5Gi", container["resources"]["limits"]["memory"])
        self.assertEqual("2Gi", container["resources"]["requests"]["memory"])

    def test_production_restore_gets_service_specific_jenkins_timeout(self) -> None:
        script = (ROOT / "scripts/deploy/service-deploy.sh").read_text()

        self.assertIn('[ "$SERVICE" = "market-carry" ]', script)
        self.assertIn('[ "$ENVIRONMENT" = "production" ]', script)
        self.assertIn('ROLLOUT_TIMEOUT="7260s"', script)
        self.assertIn('--timeout="$ROLLOUT_TIMEOUT"', script)

    def test_approach_monitor_rollout_waits_out_its_warm_up(self) -> None:
        script = (ROOT / "scripts/deploy/service-deploy.sh").read_text()

        # Unready while it rebuilds the 15-minute trailing window is TRUE, not a fault; the default
        # 600s wait ended inside that window and failed every deploy of the service.
        self.assertIn('elif [ "$SERVICE" = "approach-monitor" ]; then', script)
        block = script.split('elif [ "$SERVICE" = "approach-monitor" ]; then', 1)[1].split("else", 1)[0]
        self.assertIn('ROLLOUT_TIMEOUT="1800s"', block)
        self.assertGreater(1800, 15 * 60 + 600, "must cover the warm-up plus start-up")
        # and the market-carry production override is untouched
        self.assertIn('ROLLOUT_TIMEOUT="7260s"', script)


if __name__ == "__main__":
    unittest.main()
