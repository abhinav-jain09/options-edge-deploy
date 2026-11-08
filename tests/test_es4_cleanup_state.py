# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RECONCILE = ROOT / "scripts" / "es4" / "reconcile-cleanup-state.awk"
JENKINSFILE = ROOT / "Jenkinsfile.es4-deploy"


class Es4CleanupStateTest(unittest.TestCase):
    def reconcile(self, captured: str, current: str) -> str:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            captured_path = path / "captured"
            current_path = path / "current"
            captured_path.write_text(captured)
            current_path.write_text(current)
            return subprocess.check_output(
                ["awk", "-f", str(RECONCILE), str(captured_path), str(current_path)],
                text=True,
            )

    def test_preserves_original_counts_and_adds_new_deployment(self):
        result = self.reconcile(
            "WIPING\ndatabento-gex-service 1\ndelta-flow-service 1\n",
            "databento-gex-service 0\ndelta-flow-service 0\noption-truth-engine-service 1\n",
        )
        self.assertEqual(
            result,
            "WIPING\ndatabento-gex-service 1\ndelta-flow-service 1\noption-truth-engine-service 1\n",
        )

    def test_drops_deployment_removed_after_capture(self):
        result = self.reconcile(
            "WIPING\nretired-service 1\ndelta-flow-service 1\n",
            "delta-flow-service 0\n",
        )
        self.assertEqual(result, "WIPING\ndelta-flow-service 1\n")

    def test_es4_actions_sync_complete_scripts_tree(self):
        text = JENKINSFILE.read_text()
        scoped_sync = 'rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/'
        self.assertEqual(text.count(scoped_sync), 3)
        self.assertNotIn('rsync -az scripts/es4 ', text)
        self.assertNotIn('rsync -az --delete scripts/es4 ', text)


if __name__ == "__main__":
    unittest.main()
