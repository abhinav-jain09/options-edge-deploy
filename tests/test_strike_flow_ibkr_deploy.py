from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class StrikeFlowDeployTest(unittest.TestCase):
    """Deploy contract for the strike-flow classifier.

    One Service One Identity Rule: the classifier has exactly one deployment
    (strike-flow-classifier-databento) and one stable, UNVERSIONED Streams
    application id. The base manifest MUST pin KAFKA_STRIKE_FLOW_STREAMS_APP_ID
    with a literal equal to the code default: the kafka-cleanup orphan purge
    builds its ACTIVE set from literal *_APP_ID envs (fail-closed), so a
    pin-less service would be purged as orphan (the OLD hazard — a v4 pin overriding the code
    default caused the 2026-07-18 stale-generation incident), and no
    version-suffixed identity may appear anywhere in the manifests.
    """

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

    def test_single_databento_classifier_deployment_is_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "strike-flow-classifier-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "strike-flow-classifier-service.yaml").read_text()

        self.assertIn("name: strike-flow-classifier-databento", deployment)
        self.assertIn("name: strike-flow-classifier-databento", service)
        self.assertEqual(
            "DATABENTO",
            self.env_value(deployment, "strike-flow-classifier-databento", "STRIKE_FLOW_SOURCE"),
        )

    def test_databento_classifier_uses_opra_tcbbo_json_contract(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "strike-flow-classifier-deployment.yaml").read_text()
        configmap = (ROOT / "k8s" / "infra" / "base" / "configmap.yaml").read_text()

        self.assertEqual(
            "options.opra.tcbbo",
            self.env_value(deployment, "strike-flow-classifier-databento", "KAFKA_STRIKE_FLOW_INPUT_TOPIC"),
        )
        self.assertEqual(
            "options.databento.strike-flow",
            self.env_value(deployment, "strike-flow-classifier-databento", "KAFKA_STRIKE_FLOW_OUTPUT_TOPIC"),
        )
        self.assertIn("KAFKA_STRIKE_FLOW_INPUT_TOPIC: options.opra.tcbbo", configmap)
        self.assertNotIn("KAFKA_STRIKE_FLOW_INPUT_TOPIC: options.databento.raw", configmap)
        self.assertNotIn("value: options.databento.raw", deployment)

    def test_app_id_is_pinned_unversioned_everywhere(self) -> None:
        # One Service One Identity Rule + kafka-cleanup contract: the orphan purge builds its
        # ACTIVE set from every live workload's LITERAL *_APP_ID env (fail-closed), so the base
        # deployment MUST pin the app id — with exactly the unversioned identity, matching the
        # code default. A version suffix anywhere under k8s/ violates the rulebook; an absent
        # pin would let an armed purge classify the live changelogs as orphans.
        versioned = re.compile(r"strike-flow-classifier[-a-z]*-v\d")
        base = (ROOT / "k8s" / "base" / "strike-flow-classifier-deployment.yaml").read_text()
        self.assertIn(
            "value: options-edge-databento-strike-flow-classifier\n", base,
            "base must pin the UNVERSIONED app id (kafka-cleanup active-set contract)",
        )
        for path in sorted((ROOT / "k8s").rglob("*.yaml")):
            self.assertIsNone(
                versioned.search(path.read_text()),
                f"{path} must not carry a version-suffixed classifier identity",
            )

    def test_each_overlay_renders_exactly_one_classifier_deployment(self) -> None:
        # One service, one deployment, every environment.
        for env in ["dev", "production", "experiment"]:
            manifest = (
                ROOT / "k8s" / "services" / "strike-flow-classifier" / "overlays" / env / "manifest.yaml"
            ).read_text()
            deployments = re.findall(r"^kind: Deployment$", manifest, flags=re.MULTILINE)
            self.assertEqual(
                1, len(deployments),
                f"{env} overlay must render exactly one classifier Deployment",
            )
            self.assertIn("name: strike-flow-classifier-databento", manifest)
            self.assertNotIn("strike-flow-classifier-ibkr", manifest)

    def test_service_deploy_owns_the_classifier_rollout(self) -> None:
        # Rollouts are driven by services.yaml + scripts/deploy/service-deploy.sh (the service-deploy
        # Jenkins job), not by hardcoded kubectl lines in the Jenkinsfile. The registry entry maps the
        # single classifier deployment and is slice-generated for the per-service deploy path.
        services = (ROOT / "services.yaml").read_text()
        self.assertIn("name: strike-flow-classifier", services)
        self.assertIn("deployments: [strike-flow-classifier-databento]", services)


if __name__ == "__main__":
    unittest.main()
