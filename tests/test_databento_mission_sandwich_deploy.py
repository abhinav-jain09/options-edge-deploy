from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DatabentoMissionSandwichDeployTest(unittest.TestCase):
    def test_deployment_and_service_are_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "databento-mission-sandwich-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "databento-mission-sandwich-service.yaml").read_text()
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()

        self.assertIn("name: databento-mission-sandwich-service", deployment)
        self.assertIn("options-edge-databento-mission-sandwich:dev", deployment)
        self.assertIn("containerPort: 8099", deployment)
        self.assertIn("APP_MARKET_DATA_SOURCE", deployment)
        self.assertIn("DATABENTO", deployment)
        self.assertIn("KAFKA_MISSION_SANDWICH_VOLUME_TOPIC", deployment)
        self.assertIn("display.volume.current", deployment)
        self.assertIn("KAFKA_MISSION_SANDWICH_PACE_TOPIC", deployment)
        self.assertIn("options.databento.pace.mission", deployment)
        self.assertIn("KAFKA_MISSION_SANDWICH_PRESSURE_TOPIC", deployment)
        self.assertIn("options.databento.market-pressure.mission", deployment)
        self.assertIn("KAFKA_MISSION_SANDWICH_OUTPUT_TOPIC", deployment)
        self.assertIn("options.databento.sandwich.mission", deployment)
        self.assertNotIn("options.ibkr", deployment)

        self.assertIn("port: 8099", service)
        self.assertIn("prometheus.io/scrape", service)
        self.assertIn("databento-mission-sandwich-deployment.yaml", kustomization)
        self.assertIn("databento-mission-sandwich-service.yaml", kustomization)

    def test_jenkins_rolls_databento_mission_sandwich_image(self) -> None:
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()

        self.assertIn("DATABENTO_MISSION_SANDWICH_IMAGE", jenkinsfile)
        self.assertIn("options-edge-databento-mission-sandwich", jenkinsfile)
        self.assertIn("deployment/databento-mission-sandwich-service", jenkinsfile)
        self.assertIn('databento-mission-sandwich="$DATABENTO_MISSION_SANDWICH_IMAGE"', jenkinsfile)
        self.assertIn("rollout status deployment/databento-mission-sandwich-service", jenkinsfile)

    def test_topics_include_mission_sandwich(self) -> None:
        topics = (ROOT / "scripts" / "kafka" / "topics.env").read_text()

        self.assertIn("options.databento.sandwich.mission:4", topics)
        self.assertIn("display.volume.current:4", topics)
        self.assertIn("options.databento.sandwich.mission", topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1])

    def test_smoke_and_monitoring_include_mission_sandwich(self) -> None:
        smoke = (ROOT / "scripts" / "smoke" / "check-k8s-services.sh").read_text()
        monitoring = (ROOT / "scripts" / "monitoring" / "apply-prometheus-scrapes.sh").read_text()

        self.assertIn("check_databento_mission_sandwich", smoke)
        self.assertIn("databento-mission-sandwich-service", smoke)
        self.assertIn("databento-mission-sandwich-service 8099", monitoring)


if __name__ == "__main__":
    unittest.main()
