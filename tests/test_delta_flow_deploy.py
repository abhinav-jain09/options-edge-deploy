# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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
        self.assertIn("DELTA_FLOW_GREEK_MAX_AGE_MS", deployment)
        self.assertIn('value: "15000"', deployment)
        self.assertIn("port: 8110", service)
        self.assertIn("host.docker.internal:5001/options-edge-delta-flow", dev_overlay)

    def test_delta_flow_image_is_resolved_and_deployed(self):
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()
        resolve_images = (ROOT / "scripts" / "deploy" / "resolve-images.sh").read_text()
        apply_script = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        preflight = (ROOT / "scripts" / "deploy" / "image-preflight.sh").read_text()
        monitoring = (ROOT / "scripts" / "monitoring" / "apply-prometheus-scrapes.sh").read_text()

        self.assertIn("string(name: 'DELTA_FLOW_IMAGE'", jenkinsfile)
        self.assertIn("choice(name: 'DEPLOY_TARGET'", jenkinsfile)
        self.assertIn("DEPLOY_TARGET = ", jenkinsfile)
        self.assertIn("DELTA_FLOW_IMAGE = ", jenkinsfile)
        self.assertIn("options-edge-delta-flow:$image_tag", resolve_images)
        self.assertIn("DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE", resolve_images)
        self.assertIn('deployment/delta-flow-service delta-flow="$DELTA_FLOW_IMAGE"', apply_script)
        self.assertIn("rollout restart deployment/delta-flow-service", apply_script)
        self.assertIn("rollout status deployment/delta-flow-service", apply_script)
        self.assertIn("DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE", preflight)
        self.assertIn('delta-flow-service)', apply_script)
        self.assertIn('kubectl apply -f "$_target_render"', apply_script)
        self.assertIn('DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE', preflight)
        self.assertIn('DEPLOY_TARGET:-all', preflight)
        self.assertIn("add_service_scrape delta-flow-service 8110", monitoring)

    def test_delta_flow_is_not_an_es4_service(self):
        # Removed from es4 2026-11-02 (only ES-related services run there); see k8s/es4/allowed-services.txt.
        self.assertFalse((ROOT / "k8s" / "es4" / "services" / "delta-flow.yaml").exists())

    @staticmethod
    def _delta_flow_container_env(manifest_path, parser):
        """Return {envName: value} for the delta-flow Deployment's first container, from a YAML manifest."""
        for doc in parser.safe_load_all(manifest_path.read_text()):
            if not doc or doc.get("kind") != "Deployment":
                continue
            container = doc["spec"]["template"]["spec"]["containers"][0]
            return {e["name"]: e.get("value") for e in container.get("env", [])}
        raise AssertionError(f"no Deployment found in {manifest_path}")


if __name__ == "__main__":
    unittest.main()
