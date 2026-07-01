import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class DeltaFlowDeployTest(unittest.TestCase):
    def test_delta_flow_kubernetes_resources_are_registered(self):
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()
        deployment = (ROOT / "k8s" / "base" / "delta-flow-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "delta-flow-service.yaml").read_text()
        dev_overlay = (ROOT / "k8s" / "overlays" / "dev" / "kustomization.yaml").read_text()

        self.assertIn("delta-flow-deployment.yaml", kustomization)
        self.assertIn("delta-flow-service.yaml", kustomization)
        self.assertIn("name: delta-flow-service", deployment)
        self.assertIn("name: delta-flow\n", deployment)
        self.assertIn("options-edge-delta-flow:dev", deployment)
        self.assertIn("DELTA_FLOW_INPUT_TCBBO_TOPIC", deployment)
        self.assertIn("options.opra.tcbbo", deployment)
        self.assertIn("DELTA_FLOW_INPUT_GEX_TOPIC", deployment)
        self.assertIn("options.databento.gex.strike", deployment)
        self.assertIn("port: 8110", service)
        self.assertIn("host.docker.internal:5001/options-edge-delta-flow", dev_overlay)

    def test_delta_flow_image_is_resolved_and_deployed(self):
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()
        resolve_images = (ROOT / "scripts" / "deploy" / "resolve-images.sh").read_text()
        apply_script = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        preflight = (ROOT / "scripts" / "deploy" / "image-preflight.sh").read_text()
        monitoring = (ROOT / "scripts" / "monitoring" / "apply-prometheus-scrapes.sh").read_text()

        self.assertIn("string(name: 'DELTA_FLOW_IMAGE'", jenkinsfile)
        self.assertIn("DELTA_FLOW_IMAGE = ", jenkinsfile)
        self.assertIn("options-edge-delta-flow:$image_tag", resolve_images)
        self.assertIn("DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE", resolve_images)
        self.assertIn('deployment/delta-flow-service delta-flow="$DELTA_FLOW_IMAGE"', apply_script)
        self.assertIn("rollout restart deployment/delta-flow-service", apply_script)
        self.assertIn("rollout status deployment/delta-flow-service", apply_script)
        self.assertIn("DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE", preflight)
        self.assertIn("add_service_scrape delta-flow-service 8110", monitoring)


if __name__ == "__main__":
    unittest.main()
