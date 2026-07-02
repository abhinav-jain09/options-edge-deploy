from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DatabentoMissionPressureDeployTest(unittest.TestCase):
    def test_deployment_and_service_are_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "databento-mission-pressure-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "databento-mission-pressure-service.yaml").read_text()
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()

        self.assertIn("name: databento-mission-pressure-service", deployment)
        self.assertIn("options-edge-databento-mission-pressure:dev", deployment)
        self.assertIn("containerPort: 8098", deployment)
        self.assertIn("APP_MARKET_DATA_SOURCE", deployment)
        self.assertIn("DATABENTO", deployment)
        self.assertIn("KAFKA_MISSION_PRESSURE_RAW_TOPIC", deployment)
        self.assertIn("options.databento.raw", deployment)
        self.assertIn("KAFKA_MISSION_PRESSURE_STRIKE_FLOW_TOPIC", deployment)
        self.assertIn("options.databento.strike-flow", deployment)
        self.assertIn("KAFKA_MISSION_PRESSURE_PACE_TOPIC", deployment)
        self.assertIn("options.databento.pace.mission", deployment)
        self.assertIn("KAFKA_MISSION_PRESSURE_OUTPUT_TOPIC", deployment)
        self.assertIn("options.databento.market-pressure.mission", deployment)
        self.assertNotIn("options.ibkr", deployment)

        self.assertIn("port: 8098", service)
        self.assertIn("prometheus.io/scrape", service)
        self.assertIn("databento-mission-pressure-deployment.yaml", kustomization)
        self.assertIn("databento-mission-pressure-service.yaml", kustomization)

    def test_jenkins_rolls_databento_mission_pressure_image(self) -> None:
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()

        self.assertIn("DATABENTO_MISSION_PRESSURE_IMAGE", jenkinsfile)
        self.assertIn("options-edge-databento-mission-pressure", jenkinsfile)
        self.assertIn("deployment/databento-mission-pressure-service", jenkinsfile)
        self.assertIn('databento-mission-pressure="$DATABENTO_MISSION_PRESSURE_IMAGE"', jenkinsfile)
        self.assertIn("rollout status deployment/databento-mission-pressure-service", jenkinsfile)

    def test_topics_include_mission_pressure_and_mission_pace(self) -> None:
        topics = (ROOT / "scripts" / "kafka" / "topics.env").read_text()

        self.assertIn("options.databento.pace.mission:4", topics)
        self.assertIn("options.databento.market-pressure.mission:4", topics)
        self.assertIn("options.databento.sandwich.mission:4", topics)
        self.assertIn("options.databento.pace.mission", topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1])
        self.assertIn("options.databento.market-pressure.mission", topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1])

    def test_smoke_and_monitoring_include_mission_pressure(self) -> None:
        smoke = (ROOT / "scripts" / "smoke" / "check-k8s-services.sh").read_text()
        monitoring = (ROOT / "scripts" / "monitoring" / "apply-prometheus-scrapes.sh").read_text()

        self.assertIn("check_databento_mission_pressure", smoke)
        self.assertIn("databento-mission-pressure-service", smoke)
        self.assertIn("databento-mission-pressure-service 8098", monitoring)


if __name__ == "__main__":
    unittest.main()
