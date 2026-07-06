import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class DealerLedgerDeployTest(unittest.TestCase):
    def test_dealer_ledger_kubernetes_resources_are_registered(self):
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()
        deployment = (ROOT / "k8s" / "base" / "dealer-ledger-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "dealer-ledger-service.yaml").read_text()
        dev_overlay = (ROOT / "k8s" / "overlays" / "dev" / "kustomization.yaml").read_text()
        experiment_overlay = (ROOT / "k8s" / "overlays" / "experiment" / "kustomization.yaml").read_text()

        self.assertIn("dealer-ledger-deployment.yaml", kustomization)
        self.assertIn("dealer-ledger-service.yaml", kustomization)
        self.assertIn("name: dealer-ledger-service", deployment)
        self.assertIn("name: dealer-ledger\n", deployment)
        self.assertIn("options-edge-dealer-ledger:dev", deployment)
        self.assertIn("DEALER_LEDGER_INPUT_STRIKE_FLOW_TOPIC", deployment)
        self.assertIn("options.databento.strike-flow", deployment)
        self.assertIn("DEALER_LEDGER_INPUT_GREEKS_TOPIC", deployment)
        self.assertIn("options.databento.gex.strike", deployment)
        self.assertIn("DEALER_LEDGER_OUTPUT_STATE_TOPIC", deployment)
        self.assertIn("dealer-ledger-state", deployment)
        # Ships DARK: runs + emits, but no advisory / not UI-visible / uncalibrated by default.
        self.assertIn('name: DEALER_LEDGER_ADVISORY_ENABLED', deployment)
        self.assertIn('name: DEALER_LEDGER_UI_VISIBLE', deployment)
        self.assertIn("UNCALIBRATED", deployment)
        self.assertIn("port: 8113", service)
        self.assertIn("host.docker.internal:5001/options-edge-dealer-ledger", dev_overlay)
        self.assertIn("host.docker.internal:5001/options-edge-dealer-ledger", experiment_overlay)

    def test_dealer_ledger_image_is_resolved_and_deployed(self):
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()
        resolve_images = (ROOT / "scripts" / "deploy" / "resolve-images.sh").read_text()
        apply_script = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        preflight = (ROOT / "scripts" / "deploy" / "image-preflight.sh").read_text()
        monitoring = (ROOT / "scripts" / "monitoring" / "apply-prometheus-scrapes.sh").read_text()
        dev_tags = (ROOT / "image-tags" / "dev.yaml").read_text()
        prod_tags = (ROOT / "image-tags" / "production.yaml").read_text()

        self.assertIn("string(name: 'DEALER_LEDGER_IMAGE'", jenkinsfile)
        self.assertIn("DEALER_LEDGER_IMAGE = ", jenkinsfile)
        self.assertIn("string(name: 'DEALER_LEDGER_IMAGE', value: params.DEALER_LEDGER_IMAGE)", jenkinsfile)
        self.assertIn("'dealer-ledger-service'", jenkinsfile)  # DEPLOY_TARGET choice
        self.assertIn("options-edge-dealer-ledger:$image_tag", resolve_images)
        self.assertIn("DEALER_LEDGER_IMAGE=$DEALER_LEDGER_IMAGE", resolve_images)
        self.assertIn('deployment/dealer-ledger-service dealer-ledger="$DEALER_LEDGER_IMAGE"', apply_script)
        self.assertIn("rollout restart deployment/dealer-ledger-service", apply_script)
        self.assertIn("rollout status deployment/dealer-ledger-service", apply_script)
        self.assertIn("dealer-ledger-service)", apply_script)  # targeted-deploy case
        self.assertIn("DEALER_LEDGER_IMAGE=$DEALER_LEDGER_IMAGE", preflight)
        self.assertIn("add_service_scrape dealer-ledger-service 8113", monitoring)
        # image-tags consistency guard: mapped in dev MUST be mapped in production too.
        self.assertIn("dealer-ledger-service: 192.168.100.252:5000/options-edge-dealer-ledger:dev", dev_tags)
        self.assertIn("dealer-ledger-service: ghcr.io/abhinav-jain09/options-edge-dealer-ledger:production", prod_tags)


if __name__ == "__main__":
    unittest.main()
