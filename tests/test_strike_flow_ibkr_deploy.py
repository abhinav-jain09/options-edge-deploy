from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class StrikeFlowIbkrDeployTest(unittest.TestCase):
    def test_two_classifier_deployments_are_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "strike-flow-classifier-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "strike-flow-classifier-service.yaml").read_text()

        for name in ["strike-flow-classifier-databento", "strike-flow-classifier-ibkr"]:
            self.assertIn(f"name: {name}", deployment)
            self.assertIn(f"name: {name}", service)

        self.assertIn("STRIKE_FLOW_SOURCE", deployment)
        self.assertIn("value: DATABENTO", deployment)
        self.assertIn("value: IBKR", deployment)
        self.assertIn("value: options.databento.raw", deployment)
        self.assertIn("value: options.ibkr.raw", deployment)
        self.assertIn("value: options.databento.strike-flow", deployment)
        self.assertIn("value: options.ibkr.strike-flow", deployment)
        self.assertIn("value: options-edge-databento-strike-flow-classifier-v1", deployment)
        self.assertIn("value: options-edge-ibkr-strike-flow-classifier-v1", deployment)

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
