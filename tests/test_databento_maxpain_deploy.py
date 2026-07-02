from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DatabentoMaxPainDeployTest(unittest.TestCase):
    def test_deployment_and_service_are_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "databento-maxpain-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "databento-maxpain-service.yaml").read_text()
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()

        self.assertIn("name: databento-maxpain-service", deployment)
        self.assertIn("options-edge-databento-maxpain:dev", deployment)
        self.assertIn("containerPort: 8101", deployment)
        self.assertIn("APP_MARKET_DATA_SOURCE", deployment)
        self.assertIn("DATABENTO", deployment)
        self.assertIn("KAFKA_DATABENTO_RAW_TOPIC", deployment)
        self.assertIn("options.databento.raw", deployment)
        self.assertIn("KAFKA_DATABENTO_MAXPAIN_TOPIC", deployment)
        self.assertIn("options.databento.maxpain", deployment)
        self.assertIn("KAFKA_MAXPAIN_STREAMS_APP_ID", deployment)
        self.assertIn("options-databento-maxpain-streams-v1", deployment)
        self.assertIn("DATABENTO_MAXPAIN_SETTLEMENT_ZONE", deployment)
        self.assertIn("America/New_York", deployment)
        self.assertIn("DATABENTO_MAXPAIN_PM_CLOSE", deployment)
        # Should NOT reference the GEX topic — independence is the headline correctness property.
        self.assertNotIn("options.databento.gex.strike", deployment)

        self.assertIn("port: 8101", service)
        self.assertIn("prometheus.io/scrape", service)
        self.assertIn("databento-maxpain-deployment.yaml", kustomization)
        self.assertIn("databento-maxpain-service.yaml", kustomization)

    def test_jenkins_rolls_databento_maxpain_image(self) -> None:
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()

        # All ten insertion points the deploy job needs for a service must be present —
        # a regression in any of them silently skips the new service. Each assertion pins
        # ONE specific point to a unique substring so the failure points at the missing site.
        # 1. params block
        self.assertIn(
            "string(name: 'DATABENTO_MAXPAIN_IMAGE', defaultValue: ''",
            jenkinsfile,
        )
        # 2. env block default via oeProfile.image
        self.assertIn(
            "oeProfile.image('databento-maxpain', 'production', 'dev')",
            jenkinsfile,
        )
        # 3. first heredoc (registry/tag substitution)
        self.assertIn(
            "DATABENTO_MAXPAIN_IMAGE=$registry/options-edge-databento-maxpain:$image_tag",
            jenkinsfile,
        )
        # 4. second heredoc (env passthrough)
        self.assertIn(
            "DATABENTO_MAXPAIN_IMAGE=$DATABENTO_MAXPAIN_IMAGE",
            jenkinsfile,
        )
        # 5. preflight images list (indented twice)
        self.assertIn(
            "            DATABENTO_MAXPAIN_IMAGE=$DATABENTO_MAXPAIN_IMAGE",
            jenkinsfile,
        )
        # 6. overlay images:-pin loop must include maxpain in the for-list
        self.assertRegex(jenkinsfile, r"for _img_var in [^\n]*DATABENTO_MAXPAIN_IMAGE")
        # 7. kubectl set-image
        self.assertIn(
            'kubectl -n options-edge set image deployment/databento-maxpain-service '
            'databento-maxpain="$DATABENTO_MAXPAIN_IMAGE"',
            jenkinsfile,
        )
        # 8. rollout restart
        self.assertIn(
            "kubectl -n options-edge rollout restart deployment/databento-maxpain-service",
            jenkinsfile,
        )
        # 9. rollout status
        self.assertIn(
            "kubectl -n options-edge rollout status deployment/databento-maxpain-service",
            jenkinsfile,
        )
        # 10. sub-job param propagation
        self.assertIn(
            "string(name: 'DATABENTO_MAXPAIN_IMAGE', value: params.DATABENTO_MAXPAIN_IMAGE)",
            jenkinsfile,
        )

    def test_dev_overlay_remaps_maxpain_image_to_local_registry(self) -> None:
        # Operational parity with every other service: `kubectl apply -k k8s/overlays/dev`
        # must produce the local-mac host.docker.internal:5001 image for maxpain too. Without
        # this entry the dev overlay would still pull from the prod 192.168.100.252:5000.
        overlay = (ROOT / "k8s" / "overlays" / "dev" / "kustomization.yaml").read_text()
        self.assertIn("192.168.100.252:5000/options-edge-databento-maxpain", overlay)
        self.assertIn("host.docker.internal:5001/options-edge-databento-maxpain", overlay)

    def test_topics_include_maxpain_compacted(self) -> None:
        topics = (ROOT / "scripts" / "kafka" / "topics.env").read_text()
        # In the approved-topics whitelist.
        self.assertIn("options.databento.maxpain:4", topics)
        # And in the compacted-topics list (bridge calls ensureCompactedTopic).
        self.assertIn(
            "options.databento.maxpain",
            topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1],
        )
