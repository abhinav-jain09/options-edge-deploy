import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class KafkaStreamsStatePvcTest(unittest.TestCase):
    """Streams-state PVCs live in the common-infra layer (standalone-service-deployment).

    storageClassName is IMMUTABLE after creation and differs per environment
    (dev docker-desktop = standard, prod/.2 k3s = local-path). A hard-coded class in the
    base is exactly what failed every prod deploy with "spec: Forbidden: spec is
    immutable" (deploy #490/#502) — so the base must stay class-NEUTRAL and each env
    overlay must pin its own live class.
    """

    def test_base_pvcs_are_storage_class_neutral(self):
        text = (ROOT / "k8s" / "infra" / "base" / "kafka-streams-state-pvcs.yaml").read_text()
        documents = [doc for doc in text.split("---") if "kind: PersistentVolumeClaim" in doc]

        self.assertTrue(documents)
        for document in documents:
            self.assertNotIn("storageClassName", document)

    def test_env_overlays_pin_their_live_storage_class(self):
        expected = {
            "dev": "value: standard",
            "production": "value: local-path",
            "experiment": "value: local-path",
        }
        for env, needle in expected.items():
            overlay = (
                ROOT / "k8s" / "infra" / "overlays" / env / "kustomization.yaml"
            ).read_text()
            self.assertIn("kind: PersistentVolumeClaim", overlay, env)
            self.assertIn("/spec/storageClassName", overlay, env)
            self.assertIn(needle, overlay, env)

    def test_pvcs_are_not_in_the_app_base(self):
        # Per-service deploys must never touch PVCs (§13.5 blast radius): the app base
        # kustomization must not reference the PVC manifest.
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()
        self.assertNotIn("kafka-streams-state-pvcs.yaml", kustomization)


if __name__ == "__main__":
    unittest.main()
