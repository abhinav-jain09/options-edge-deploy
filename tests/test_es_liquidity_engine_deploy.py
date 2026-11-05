"""Deploy wiring of es-liquidity-engine-service (the ES MBO liquidity engine) and the TEMPORARY drop-classifier-ref reference deployment (the G3b parity gate's reference side).

Both are DEV ONLY, single replica, strategy Recreate, health port 8080, drop engine in ES_TRADES mode on the dev broker. The engine writes the default es.drop.* prefix; the reference writes
es.drop.ref.* so the two never share an output topic. Covers the base, the dev overlay slice, the registration everywhere a dev-only service must be, and the absence of any production wiring."""
from __future__ import annotations

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def deployment(rel: str) -> dict:
    return next(d for d in yaml.safe_load_all(read(rel)) if d and d.get("kind") == "Deployment")


def env_of(dep: dict) -> dict:
    return {e["name"]: e.get("value") for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]}


CASES = {
    "es-liquidity-engine": dict(dep="es-liquidity-engine-service", container="es-liquidity-engine", image="options-edge-es-liquidity-engine", var="ES_LIQUIDITY_ENGINE_IMAGE", prefix=None),
    "drop-classifier-ref": dict(dep="drop-classifier-ref-service", container="drop-classifier-ref", image="options-edge-drop-classifier", var="DROP_CLASSIFIER_IMAGE", prefix="es.drop.ref"),
}


class Manifests(unittest.TestCase):
    def test_base_and_committed_slice_are_dev_single_replica_recreate_es_trades(self):
        for svc, c in CASES.items():
            for rel in (f"k8s/services/{svc}/base/{svc}-deployment.yaml", f"k8s/services/{svc}/overlays/dev/manifest.yaml"):
                d = deployment(rel)
                self.assertEqual(d["metadata"]["name"], c["dep"], rel)
                self.assertEqual(d["spec"]["replicas"], 1, rel)
                self.assertEqual(d["spec"]["strategy"]["type"], "Recreate", rel)
                ct = d["spec"]["template"]["spec"]["containers"][0]
                self.assertEqual(ct["name"], c["container"], rel)
                self.assertIn(c["image"], ct["image"], rel)
                e = env_of(d)
                self.assertEqual(e["HEALTH_PORT"], "8080", rel)
                self.assertEqual(e["DROP_UNDERLYING_MODE"], "ES_TRADES", rel)
                for k in ("DROP_UNDERLYING_BOOTSTRAP", "DROP_FLOW_BOOTSTRAP", "DROP_OUTPUT_BOOTSTRAP", "DROP_GAMMA_BOOTSTRAP"):
                    self.assertEqual(e[k], "host.docker.internal:19092", f"{rel} {k}: the dev broker, never a production or es4 address")
                self.assertEqual(e.get("DROP_OUTPUT_PREFIX"), c["prefix"], rel)
                self.assertEqual(ct["livenessProbe"]["httpGet"]["path"], "/health/live", rel)
                self.assertEqual(ct["readinessProbe"]["httpGet"]["path"], "/health/ready", rel)

    def test_the_two_deployments_never_share_an_output_prefix_or_name(self):
        engine, ref = (env_of(deployment(f"k8s/services/{s}/base/{s}-deployment.yaml")) for s in CASES)
        self.assertNotEqual(engine.get("DROP_OUTPUT_PREFIX"), ref.get("DROP_OUTPUT_PREFIX"))
        self.assertTrue(ref["DROP_OUTPUT_PREFIX"].startswith("es.drop."))

    def test_no_service_object_and_no_production_or_experiment_reference(self):
        for svc in CASES:
            self.assertFalse(any(d and d.get("kind") == "Service" for d in yaml.safe_load_all(read(f"k8s/services/{svc}/overlays/dev/manifest.yaml"))), svc)
            for env in ("production", "experiment", "es4"):
                self.assertFalse((ROOT / f"k8s/services/{svc}/overlays/{env}").exists(), f"{svc}: no {env} overlay")
                self.assertNotIn(f"services/{svc}/", read(f"k8s/overlays/{env}/kustomization.yaml") if (ROOT / f"k8s/overlays/{env}/kustomization.yaml").exists() else "", f"{svc} {env}")
        self.assertNotIn("es-liquidity-engine", read("k8s/base/kustomization.yaml"))


class Registration(unittest.TestCase):
    def test_services_yaml_declares_both_dev_only_standalone_slices(self):
        by = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        for svc, c in CASES.items():
            self.assertEqual(by[svc]["envs"], ["dev"], svc)
            self.assertEqual(by[svc]["managedBy"], "standalone")
            self.assertTrue(by[svc]["sliceGenerated"])
            self.assertEqual(by[svc]["deployments"], [c["dep"]])
            self.assertEqual(by[svc]["image"], c["image"])

    def test_dev_overlay_resource_image_remap_and_image_tag(self):
        k = read("k8s/overlays/dev/kustomization.yaml")
        for svc in CASES:
            self.assertIn(f"  - ../../services/{svc}/base\n", k)
        self.assertIn("newName: host.docker.internal:5001/options-edge-es-liquidity-engine", k)
        self.assertIn("es-liquidity-engine-service: host.docker.internal:5001/options-edge-es-liquidity-engine:dev", read("image-tags/dev.yaml"))

    def test_the_engine_image_variable_is_wired_in_every_deploy_script_for_dev_only(self):
        for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/resolve-images.sh", "scripts/deploy/verify-running-images.sh"):
            self.assertIn("ES_LIQUIDITY_ENGINE_IMAGE", read(rel), rel)
        self.assertIn('("drop-classifier-ref-service", "drop-classifier-ref"): "DROP_CLASSIFIER_IMAGE"', read("scripts/deploy/verify-running-images.sh"))

    def test_service_deploy_offers_both_services(self):
        j = read("Jenkinsfile.service-deploy")
        self.assertIn("      'es-liquidity-engine',\n", j)
        self.assertIn("      'drop-classifier-ref',\n", j)


if __name__ == "__main__":
    unittest.main()
