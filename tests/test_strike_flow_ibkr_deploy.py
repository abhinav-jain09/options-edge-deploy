from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class StrikeFlowIbkrDeployTest(unittest.TestCase):
    @staticmethod
    def env_value(manifest: str, deployment_name: str, env_name: str) -> str:
        start = manifest.index(f"name: {deployment_name}")
        next_doc = manifest.find("\n---", start)
        block = manifest[start:] if next_doc == -1 else manifest[start:next_doc]
        marker = f"- name: {env_name}\n"
        env_start = block.index(marker)
        value_marker = "value:"
        value_start = block.index(value_marker, env_start) + len(value_marker)
        return block[value_start:block.find("\n", value_start)].strip().strip('"')

    def test_two_classifier_deployments_are_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "strike-flow-classifier-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "strike-flow-classifier-service.yaml").read_text()

        for name in ["strike-flow-classifier-databento", "strike-flow-classifier-ibkr"]:
            self.assertIn(f"name: {name}", deployment)
            self.assertIn(f"name: {name}", service)

        self.assertIn("STRIKE_FLOW_SOURCE", deployment)
        self.assertIn("value: DATABENTO", deployment)
        self.assertIn("value: IBKR", deployment)
        self.assertIn("value: options.opra.tcbbo", deployment)
        self.assertIn("value: options.ibkr.raw", deployment)
        self.assertIn("value: options.databento.strike-flow", deployment)
        self.assertIn("value: options.ibkr.strike-flow", deployment)
        self.assertIn("value: options-edge-databento-strike-flow-classifier-v4", deployment)
        self.assertIn("value: options-edge-ibkr-strike-flow-classifier-v1", deployment)

    def test_databento_classifier_uses_opra_tcbbo_json_contract(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "strike-flow-classifier-deployment.yaml").read_text()
        configmap = (ROOT / "k8s" / "infra" / "base" / "configmap.yaml").read_text()

        self.assertEqual(
            "DATABENTO",
            self.env_value(deployment, "strike-flow-classifier-databento", "STRIKE_FLOW_SOURCE"),
        )
        self.assertEqual(
            "options.opra.tcbbo",
            self.env_value(deployment, "strike-flow-classifier-databento", "KAFKA_STRIKE_FLOW_INPUT_TOPIC"),
        )
        self.assertEqual(
            "options.databento.strike-flow",
            self.env_value(deployment, "strike-flow-classifier-databento", "KAFKA_STRIKE_FLOW_OUTPUT_TOPIC"),
        )
        self.assertEqual(
            "options-edge-databento-strike-flow-classifier-v4",
            self.env_value(deployment, "strike-flow-classifier-databento", "KAFKA_STRIKE_FLOW_STREAMS_APP_ID"),
        )
        self.assertIn("KAFKA_STRIKE_FLOW_INPUT_TOPIC: options.opra.tcbbo", configmap)
        self.assertIn(
            "KAFKA_STRIKE_FLOW_STREAMS_APP_ID: options-edge-databento-strike-flow-classifier-v4",
            configmap,
        )
        self.assertNotIn("KAFKA_STRIKE_FLOW_INPUT_TOPIC: options.databento.raw", configmap)
        self.assertNotIn("value: options.databento.raw", deployment)

    def test_ibkr_classifier_remains_snapshot_avro_contract(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "strike-flow-classifier-deployment.yaml").read_text()

        self.assertEqual(
            "IBKR",
            self.env_value(deployment, "strike-flow-classifier-ibkr", "STRIKE_FLOW_SOURCE"),
        )
        self.assertEqual(
            "options.ibkr.raw",
            self.env_value(deployment, "strike-flow-classifier-ibkr", "KAFKA_STRIKE_FLOW_INPUT_TOPIC"),
        )
        self.assertEqual(
            "options.ibkr.strike-flow",
            self.env_value(deployment, "strike-flow-classifier-ibkr", "KAFKA_STRIKE_FLOW_OUTPUT_TOPIC"),
        )
        self.assertEqual(
            "options-edge-ibkr-strike-flow-classifier-v1",
            self.env_value(deployment, "strike-flow-classifier-ibkr", "KAFKA_STRIKE_FLOW_STREAMS_APP_ID"),
        )

    def test_jenkins_rolls_both_classifier_deployments(self) -> None:
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()

        self.assertIn("deployment/strike-flow-classifier-databento", jenkinsfile)
        self.assertIn("deployment/strike-flow-classifier-ibkr", jenkinsfile)
        self.assertIn(
            "delete deployment/strike-flow-classifier-service service/strike-flow-classifier-service --ignore-not-found=true",
            jenkinsfile,
        )
        self.assertNotIn("set image deployment/strike-flow-classifier-service", jenkinsfile)
        self.assertNotIn("rollout restart deployment/strike-flow-classifier-service", jenkinsfile)
        self.assertNotIn("rollout status deployment/strike-flow-classifier-service", jenkinsfile)


if __name__ == "__main__":
    unittest.main()
