# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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


if __name__ == "__main__":
    unittest.main()
