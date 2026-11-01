import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class OptionTruthMemoryTest(unittest.TestCase):
    def test_restore_capacity_is_rendered_in_every_environment(self) -> None:
        manifests = (
            ROOT / "k8s/base/option-truth-engine-deployment.yaml",
            ROOT / "k8s/services/option-truth-engine/overlays/dev/manifest.yaml",
            ROOT / "k8s/services/option-truth-engine/overlays/production/manifest.yaml",
            ROOT / "k8s/services/option-truth-engine/overlays/experiment/manifest.yaml",
            ROOT / "k8s/es4/services/option-truth-engine.yaml",
        )

        for manifest in manifests:
            with self.subTest(manifest=manifest.relative_to(ROOT)):
                rendered = manifest.read_text()
                # The 5g heap is the restore capacity this test exists to pin, so it stays.
                # The LIMIT moved 6Gi -> 7Gi (2026-10-19): at 6Gi the heap was 83% of the
                # container, outside the band every healthy workload in this fleet sits in, and
                # scripts/ci/validate-heap-leaves-container-headroom.sh now enforces that. The
                # headroom came from the container, not from the heap.
                self.assertIn("value: -Xms1g -Xmx5g", rendered)
                self.assertIn("memory: 7Gi", rendered)
                self.assertIn("memory: 2Gi", rendered)


if __name__ == "__main__":
    unittest.main()
