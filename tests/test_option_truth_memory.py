# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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
