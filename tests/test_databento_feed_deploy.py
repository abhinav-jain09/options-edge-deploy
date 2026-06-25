from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DatabentoFeedDeployTest(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text()

    def test_databento_feed_manifests_are_part_of_base_kustomization(self) -> None:
        kustomization = self.read("k8s/base/kustomization.yaml")
        for manifest in [
            "databento-feed-configmap.yaml",
            "databento-feed-deployment.yaml",
            "databento-feed-service.yaml",
        ]:
            self.assertIn(manifest, kustomization)

    def test_databento_feed_deployment_uses_jenkins_managed_secret(self) -> None:
        deployment = self.read("k8s/base/databento-feed-deployment.yaml")
        self.assertIn("name: options-edge-databento-feed", deployment)
        self.assertIn("image: 192.168.100.252:5000/options-edge-databento-feed:dev", deployment)
        self.assertIn("name: options-edge-databento-feed-config", deployment)
        self.assertIn("name: options-edge-databento-feed-env", deployment)
        self.assertIn("containerPort: 8010", deployment)
        self.assertNotIn("options-edge-databento-feed-env-dev", deployment)

    def test_databento_feed_config_matches_current_topic_policy(self) -> None:
        config = self.read("k8s/base/databento-feed-configmap.yaml")
        for text in [
            "APP_PROFILE: prod",
            'DATABENTO_EXPIRY: ""',
            "DATABENTO_MARKET_OPEN_ALIGNMENT: \"true\"",
            "KAFKA_BOOTSTRAP_SERVERS: 192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096",
            "KAFKA_DATABENTO_SNAPSHOT_TOPIC: options.databento.raw",
            "KAFKA_DATABENTO_CONTROL_TOPIC: options.databento.control",
            "KAFKA_HPSF_TCBBO_TOPIC: options.opra.tcbbo",
            "KAFKA_HPSF_ES_TRADES_TOPIC: underlying.es.trades",
            "KAFKA_HPSF_SPX_PRICE_TOPIC: underlying.spx.price",
        ]:
            self.assertIn(text, config)
        self.assertNotIn("dev.options.", config)

    def test_dev_overlay_points_databento_feed_to_local_dev(self) -> None:
        overlay = self.read("k8s/overlays/dev/kustomization.yaml")
        patch = self.read("k8s/overlays/dev/configmap-dev-local-patch.yaml")
        self.assertIn("192.168.100.252:5000/options-edge-databento-feed", overlay)
        self.assertIn("host.docker.internal:5001/options-edge-databento-feed", overlay)
        self.assertIn("name: options-edge-databento-feed-config", patch)
        self.assertIn("APP_PROFILE: dev", patch)
        self.assertIn("DATABENTO_MARKET_OPEN_ALIGNMENT: \"true\"", patch)
        self.assertIn("KAFKA_BOOTSTRAP_SERVERS: host.docker.internal:9092", patch)

    def test_jenkins_deploys_databento_feed_with_image_preflight_and_secret(self) -> None:
        pipeline = self.read("Jenkinsfile")
        for text in [
            "DATABENTO_FEED_IMAGE",
            "options-edge-databento-feed:$image_tag",
            "DATABENTO_API_KEY_CREDENTIAL_ID",
            "options-edge-databento-feed-env",
            "set image deployment/options-edge-databento-feed databento-feed",
            "rollout restart deployment/options-edge-databento-feed",
            "rollout status deployment/options-edge-databento-feed",
        ]:
            self.assertIn(text, pipeline)

    def test_prometheus_scrape_includes_databento_feed(self) -> None:
        script = self.read("scripts/monitoring/apply-prometheus-scrapes.sh")
        self.assertIn("add_service_scrape options-edge-databento-feed 8010", script)
        self.assertIn("options-edge-databento-feed databento-volume-aggregator", script)

    def test_kustomize_renders_dev_and_production(self) -> None:
        for overlay in ["dev", "production"]:
            subprocess.run(
                ["kubectl", "kustomize", str(ROOT / "k8s" / "overlays" / overlay)],
                check=True,
                stdout=subprocess.DEVNULL,
            )


if __name__ == "__main__":
    unittest.main()
