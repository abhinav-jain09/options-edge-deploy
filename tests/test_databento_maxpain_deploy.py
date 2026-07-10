from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SVC = ROOT / "k8s" / "services" / "databento-maxpain"


class DatabentoMaxPainDeployTest(unittest.TestCase):
    """
    databento-maxpain is a DEV-ONLY standalone service (mirrors short-premium-agent): its slice lives
    under k8s/services/databento-maxpain and is referenced ONLY from k8s/overlays/dev, so the
    production/experiment monolith overlays never render it. Deployment happens via the shared
    `kubectl kustomize k8s/overlays/dev | apply` path in the deploy job — no per-service Jenkinsfile
    image plumbing (the current deploy architecture is overlay-apply, not per-image set-image).
    """

    def test_service_registered_dev_only(self) -> None:
        services = (ROOT / "services.yaml").read_text()
        self.assertIn("name: databento-maxpain,", services)
        # Dev-only: declared envs: [dev], image basename matches.
        maxpain_line = next(
            ln for ln in services.splitlines()
            if "name: databento-maxpain," in ln
        )
        self.assertIn("envs: [dev]", maxpain_line)
        self.assertIn("image: options-edge-databento-maxpain,", maxpain_line)
        self.assertIn("deployments: [databento-maxpain-service]", maxpain_line)
        # Must NOT be registered in the shared base (that would render it into prod/experiment).
        base_kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()
        self.assertNotIn("databento-maxpain", base_kustomization)

    def test_deployment_and_service_are_declared(self) -> None:
        deployment = (SVC / "base" / "databento-maxpain-deployment.yaml").read_text()
        service = (SVC / "base" / "databento-maxpain-service.yaml").read_text()
        kustomization = (SVC / "base" / "kustomization.yaml").read_text()

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
        # v2 app id: the refactor bumped the state-store codec (feedEpochId-last); a fresh app id gives
        # a clean state store (the service has no prior deployment).
        self.assertIn("options-databento-maxpain-streams-v2", deployment)
        self.assertIn("DATABENTO_MAXPAIN_SETTLEMENT_ZONE", deployment)
        self.assertIn("America/New_York", deployment)
        self.assertIn("DATABENTO_MAXPAIN_PM_CLOSE", deployment)
        # Independence is the headline correctness property — never reference the GEX topic.
        self.assertNotIn("options.databento.gex.strike", deployment)

        self.assertIn("port: 8101", service)
        self.assertIn("prometheus.io/scrape", service)
        self.assertIn("databento-maxpain-deployment.yaml", kustomization)
        self.assertIn("databento-maxpain-service.yaml", kustomization)

    def test_dev_overlay_references_and_remaps_maxpain(self) -> None:
        overlay = (ROOT / "k8s" / "overlays" / "dev" / "kustomization.yaml").read_text()
        # Referenced dev-only (so it renders in the dev overlay).
        self.assertIn("../../services/databento-maxpain/base", overlay)
        # Image remapped to the local-mac registry, like every other dev service.
        self.assertIn("192.168.100.252:5000/options-edge-databento-maxpain", overlay)
        self.assertIn("host.docker.internal:5001/options-edge-databento-maxpain", overlay)

    def test_standalone_dev_slice_exists(self) -> None:
        # The generated dev slice (mirror of the monolith dev render) must exist for the service.
        manifest = (SVC / "overlays" / "dev" / "manifest.yaml").read_text()
        self.assertIn("name: databento-maxpain-service", manifest)
        self.assertIn("containerPort: 8101", manifest)

    def test_standalone_deploy_wiring(self) -> None:
        # Deployed via the standalone service-deploy job (like short-premium-agent): selectable in the
        # SERVICE dropdown and resolvable from image-tags/dev.yaml.
        svc_deploy = (ROOT / "Jenkinsfile.service-deploy").read_text()
        self.assertIn("'databento-maxpain'", svc_deploy)
        image_tags = (ROOT / "image-tags" / "dev.yaml").read_text()
        self.assertIn("databento-maxpain-service:", image_tags)
        self.assertIn("options-edge-databento-maxpain:dev", image_tags)

    def test_dev_only_image_pinned_only_for_dev(self) -> None:
        # CRITICAL prod-safety invariant: maxpain (dev-only) is pinned in the monolith apply.sh ONLY inside
        # the `ENVIRONMENT=dev` guard, and defined in resolve-images.sh ONLY in the dev branch — so the
        # prod/experiment deploy never attempts to pin a dev-only image (which would fail-close). Mirrors
        # short-premium-agent.
        apply_sh = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        resolve_sh = (ROOT / "scripts" / "deploy" / "resolve-images.sh").read_text()
        # The only apply.sh pin of the dev-only images is the dev-gated loop.
        self.assertIn('if [ "${ENVIRONMENT}" = "dev" ]; then', apply_sh)
        self.assertIn("SHORT_PREMIUM_AGENT_IMAGE DATABENTO_MAXPAIN_IMAGE", apply_sh)
        # Defined exactly once (the dev heredoc), never in the prod passthrough branch.
        self.assertEqual(resolve_sh.count("DATABENTO_MAXPAIN_IMAGE"), 1)

    def test_topics_include_maxpain_compacted(self) -> None:
        topics = (ROOT / "scripts" / "kafka" / "topics.env").read_text()
        # In the approved-topics whitelist at the 4-partition policy count.
        self.assertIn("options.databento.maxpain:4", topics)
        # And in the compacted-topics list (bridge calls ensureCompactedTopic).
        self.assertIn(
            "options.databento.maxpain",
            topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1],
        )


if __name__ == "__main__":
    unittest.main()
