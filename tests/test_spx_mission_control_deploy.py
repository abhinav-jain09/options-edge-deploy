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

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SpxMissionControlDeployTest(unittest.TestCase):
    def test_deployment_and_service_are_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "spx-mission-control-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "spx-mission-control-service.yaml").read_text()
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()

        self.assertIn("name: spx-mission-control-service", deployment)
        self.assertIn("options-edge-spx-mission-control:dev", deployment)
        self.assertIn("containerPort: 8096", deployment)
        self.assertIn("KAFKA_SPX_MISSION_OUTPUT_TOPIC", deployment)
        self.assertIn("options.spx.mission-control.current", deployment)
        self.assertIn("options.databento.pace.mission", deployment)
        self.assertIn("KAFKA_SPX_MISSION_PRESSURE_TOPIC", deployment)
        self.assertIn("options.databento.market-pressure.mission", deployment)
        self.assertIn("KAFKA_SPX_MISSION_SANDWICH_TOPIC", deployment)
        self.assertIn("options.databento.sandwich.mission", deployment)
        self.assertIn("SPX_MISSION_ALLOW_LEGACY_PACE", deployment)
        self.assertIn("SPX_MISSION_ALLOW_LEGACY_PRESSURE", deployment)
        self.assertIn("SPX_MISSION_ALLOW_LEGACY_SANDWICH", deployment)
        self.assertNotIn("options.databento.directional-pressure", deployment)
        self.assertNotIn("display.volume.sandwich.current", deployment)
        self.assertNotIn("display.volume.sandwich.alerts", deployment)
        self.assertNotIn("options.ibkr.pace", deployment)
        self.assertNotIn("options.ibkr.directional-pressure", deployment)
        self.assertIn("path: /health/live", deployment)
        self.assertIn("path: /health/ready", deployment)

        self.assertIn("type: LoadBalancer", service)
        self.assertIn("port: 8096", service)
        self.assertIn("spx-mission-control-deployment.yaml", kustomization)
        self.assertIn("spx-mission-control-service.yaml", kustomization)

    def test_jenkins_rolls_spx_mission_control_image(self) -> None:
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()

        self.assertIn("SPX_MISSION_CONTROL_IMAGE", jenkinsfile)
        self.assertIn("options-edge-spx-mission-control", jenkinsfile)
        self.assertIn("deployment/spx-mission-control-service", jenkinsfile)
        self.assertIn('spx-mission-control="$SPX_MISSION_CONTROL_IMAGE"', jenkinsfile)

    def test_smoke_checks_ui_and_metrics(self) -> None:
        smoke = (ROOT / "scripts" / "smoke" / "check-k8s-services.sh").read_text()

        self.assertIn("check_spx_mission_control", smoke)
        self.assertIn("/mission-control", smoke)
        self.assertIn("/api/mission-control/latest", smoke)
        self.assertIn('service="spx-mission-control-service"', smoke)


if __name__ == "__main__":
    unittest.main()
