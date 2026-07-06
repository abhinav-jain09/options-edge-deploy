import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class OptionPriceBehaviorDeployTest(unittest.TestCase):
    def test_option_price_behavior_kubernetes_resources_are_registered(self):
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()
        deployment = (ROOT / "k8s" / "base" / "option-price-behavior-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "option-price-behavior-service.yaml").read_text()
        dev_overlay = (ROOT / "k8s" / "overlays" / "dev" / "kustomization.yaml").read_text()
        experiment_overlay = (ROOT / "k8s" / "overlays" / "experiment" / "kustomization.yaml").read_text()

        self.assertIn("option-price-behavior-deployment.yaml", kustomization)
        self.assertIn("option-price-behavior-service.yaml", kustomization)
        self.assertIn("name: option-price-behavior-service", deployment)
        self.assertIn("name: option-price-behavior\n", deployment)
        self.assertIn("options-edge-option-price-behavior:dev", deployment)
        self.assertIn("OPTION_PRICE_BEHAVIOR_INPUT_TCBBO_TOPIC", deployment)
        self.assertIn("options.opra.tcbbo", deployment)
        self.assertIn("OPTION_PRICE_BEHAVIOR_INPUT_GREEKS_TOPIC", deployment)
        self.assertIn("options.databento.gex.strike", deployment)
        self.assertIn("port: 8080", service)
        self.assertIn("host.docker.internal:5001/options-edge-option-price-behavior", dev_overlay)
        self.assertIn("host.docker.internal:5001/options-edge-option-price-behavior", experiment_overlay)

    def test_option_price_behavior_runs_surface_residual_v2(self):
        # SURFACE_RESIDUAL_V2 is now THE option-price-behavior signal (V1 retired): the single
        # deployment runs the same image with OPB_SERVICE_VARIANT=v2, emits the v2 topics, and carries
        # the calibrated constants. There is NO separate -v2 deployment anymore.
        base = ROOT / "k8s" / "base"
        deployment = (base / "option-price-behavior-deployment.yaml").read_text()
        kustomization = (base / "kustomization.yaml").read_text()

        self.assertIn("OPB_SERVICE_VARIANT", deployment)
        self.assertIn("value: v2", deployment)
        self.assertIn("OPTION_PRICE_BEHAVIOR_V2_BY_OPTION_TOPIC", deployment)
        self.assertIn("option-price-behavior-v2-by-option", deployment)
        self.assertIn("OPTION_PRICE_BEHAVIOR_V2_SESSION_TOPIC", deployment)
        self.assertIn("option-price-behavior-v2-session", deployment)
        # calibrated constants (results ledger §4)
        self.assertIn("OPB_V2_WINSOR_K", deployment)
        self.assertIn("OPB_V2_WEIGHT_CAP", deployment)
        self.assertIn("OPB_V2_Z_THRESHOLD", deployment)
        # V1 is gone: no dashboard topic, no separate v2 deployment file.
        self.assertNotIn("OPTION_PRICE_BEHAVIOR_DASHBOARD_TOPIC", deployment)
        self.assertNotIn("option-price-behavior-v2-deployment.yaml", kustomization)
        self.assertFalse((base / "option-price-behavior-v2-deployment.yaml").exists())

    def test_option_price_behavior_image_is_resolved_and_deployed(self):
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()
        resolve_images = (ROOT / "scripts" / "deploy" / "resolve-images.sh").read_text()
        apply_script = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        preflight = (ROOT / "scripts" / "deploy" / "image-preflight.sh").read_text()
        monitoring = (ROOT / "scripts" / "monitoring" / "apply-prometheus-scrapes.sh").read_text()

        self.assertIn("string(name: 'OPTION_PRICE_BEHAVIOR_IMAGE'", jenkinsfile)
        self.assertIn("OPTION_PRICE_BEHAVIOR_IMAGE = ", jenkinsfile)
        self.assertIn("options-edge-option-price-behavior:$image_tag", resolve_images)
        self.assertIn("OPTION_PRICE_BEHAVIOR_IMAGE=$OPTION_PRICE_BEHAVIOR_IMAGE", resolve_images)
        self.assertIn("OPTION_PRICE_BEHAVIOR_IMAGE=$OPTION_PRICE_BEHAVIOR_IMAGE", preflight)
        self.assertIn("OPTION_PRICE_BEHAVIOR_IMAGE", apply_script)
        self.assertIn(
            'deployment/option-price-behavior-service option-price-behavior="$OPTION_PRICE_BEHAVIOR_IMAGE"',
            apply_script,
        )
        self.assertIn("rollout restart deployment/option-price-behavior-service", apply_script)
        self.assertIn("rollout status deployment/option-price-behavior-service", apply_script)
        self.assertIn("add_service_scrape option-price-behavior-service 8080", monitoring)
        self.assertIn(
            "string(name: 'OPTION_PRICE_BEHAVIOR_IMAGE', value: params.OPTION_PRICE_BEHAVIOR_IMAGE)",
            jenkinsfile,
        )
        # No separate -v2 deployment lines anymore.
        self.assertNotIn("deployment/option-price-behavior-service-v2", apply_script)


if __name__ == "__main__":
    unittest.main()
