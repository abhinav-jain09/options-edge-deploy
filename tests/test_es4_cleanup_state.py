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
