from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SVC = ROOT / "k8s" / "services" / "databento-maxpain"


class DatabentoMaxPainDeployTest(unittest.TestCase):
    """
    databento-maxpain is a DEV+PROD standalone service (mirrors databento-gex): its Deployment + Service
    live in k8s/base and render in both the dev and production overlays. It is integrated into the
    monolith image pipeline (resolve-images, image-lock, apply.sh pin loop + rollout, image-preflight,
    Jenkinsfile param/env/sub-job) and both image-tags/{dev,production}.yaml.
    """

    def test_service_registered_dev_and_prod(self) -> None:
        services = (ROOT / "services.yaml").read_text()
        line = next(ln for ln in services.splitlines() if "name: databento-maxpain," in ln)
        self.assertIn("envs: [dev, production]", line)
        self.assertIn("image: options-edge-databento-maxpain,", line)
        self.assertIn("deployments: [databento-maxpain-service]", line)
        # Now shipped from the shared base so it renders in dev AND prod overlays.
        base_kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()
        self.assertIn("databento-maxpain-deployment.yaml", base_kustomization)
        self.assertIn("databento-maxpain-service.yaml", base_kustomization)

    def test_deployment_and_service_are_declared(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "databento-maxpain-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "databento-maxpain-service.yaml").read_text()
        self.assertIn("name: databento-maxpain-service", deployment)
        self.assertIn("options-edge-databento-maxpain:dev", deployment)
        self.assertIn("containerPort: 8101", deployment)
        self.assertIn("KAFKA_DATABENTO_RAW_TOPIC", deployment)
        self.assertIn("options.databento.raw", deployment)
        self.assertIn("KAFKA_DATABENTO_MAXPAIN_TOPIC", deployment)
        self.assertIn("options.databento.maxpain", deployment)
        self.assertIn("options-databento-maxpain-streams-v2", deployment)
        self.assertIn("DATABENTO_MAXPAIN_SETTLEMENT_ZONE", deployment)
        # Independence: never references the GEX topic.
        self.assertNotIn("options.databento.gex.strike", deployment)
        self.assertIn("port: 8101", service)
        self.assertIn("prometheus.io/scrape", service)

    def test_integrated_into_monolith_image_pipeline(self) -> None:
        # As a dev+prod service, maxpain is in the MAIN (non-dev-gated) pin/preflight/rollout paths.
        resolve = (ROOT / "scripts" / "deploy" / "resolve-images.sh").read_text()
        lock = (ROOT / "scripts" / "deploy" / "image-lock.sh").read_text()
        apply_sh = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        preflight = (ROOT / "scripts" / "deploy" / "image-preflight.sh").read_text()
        jenkins = (ROOT / "Jenkinsfile").read_text()
        # resolve-images emits it in BOTH the dev heredoc and the prod passthrough.
        self.assertEqual(resolve.count("DATABENTO_MAXPAIN_IMAGE=$registry/options-edge-databento-maxpain"), 1)
        self.assertIn("DATABENTO_MAXPAIN_IMAGE=$DATABENTO_MAXPAIN_IMAGE", resolve)
        # image-lock main var set (NOT the dev-only conditional).
        self.assertIn("\nDATABENTO_MAXPAIN_IMAGE\nEOF", lock)
        # apply.sh: in the main pin loop and the main rollout list.
        self.assertIn("GEX_DELTA_REDIS_WRITER_IMAGE DATABENTO_MAXPAIN_IMAGE; do", apply_sh)
        self.assertIn("rollout status deployment/databento-maxpain-service", apply_sh)
        # It must NOT be in the dev-gated pin loop anymore.
        self.assertNotIn("SHORT_PREMIUM_AGENT_IMAGE DATABENTO_MAXPAIN_IMAGE; do", apply_sh)
        # image-preflight main list.
        self.assertIn("DATABENTO_MAXPAIN_IMAGE=$DATABENTO_MAXPAIN_IMAGE", preflight)
        # Jenkinsfile param + env(oeProfile) + sub-job propagation.
        self.assertIn("string(name: 'DATABENTO_MAXPAIN_IMAGE'", jenkins)
        self.assertIn("oeProfile.image('databento-maxpain', 'production', 'prod')", jenkins)
        self.assertIn("value: params.DATABENTO_MAXPAIN_IMAGE", jenkins)

    def test_image_tags_dev_and_prod(self) -> None:
        dev = (ROOT / "image-tags" / "dev.yaml").read_text()
        prod = (ROOT / "image-tags" / "production.yaml").read_text()
        self.assertIn("databento-maxpain-service:", dev)
        self.assertIn("options-edge-databento-maxpain:dev", dev)
        self.assertIn("databento-maxpain-service:", prod)
        self.assertIn("options-edge-databento-maxpain:prod", prod)

    def test_service_deploy_dropdown_and_slices(self) -> None:
        svc_deploy = (ROOT / "Jenkinsfile.service-deploy").read_text()
        self.assertIn("'databento-maxpain'", svc_deploy)
        # Generated slices exist for both envs.
        self.assertTrue((SVC / "overlays" / "dev" / "manifest.yaml").exists())
        self.assertTrue((SVC / "overlays" / "production" / "manifest.yaml").exists())
        prod_manifest = (SVC / "overlays" / "production" / "manifest.yaml").read_text()
        self.assertIn("name: databento-maxpain-service", prod_manifest)

    def test_not_in_dev_cleanup_disabled_set(self) -> None:
        cleanup = (ROOT / "scripts" / "ops" / "dev-cleanup.sh").read_text()
        disabled_line = next(ln for ln in cleanup.splitlines() if ln.startswith("DISABLED_DEV="))
        self.assertNotIn("databento-maxpain-service", disabled_line)

    def test_topics_include_maxpain_compacted(self) -> None:
        topics = (ROOT / "scripts" / "kafka" / "topics.env").read_text()
        self.assertIn("options.databento.maxpain:4", topics)
        self.assertIn(
            "options.databento.maxpain",
            topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1],
        )


if __name__ == "__main__":
    unittest.main()
