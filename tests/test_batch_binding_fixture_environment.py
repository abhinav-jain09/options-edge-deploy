"""The fixture must exercise real binding refusals even inside a caller dry run."""
import os
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/deploy/require-batch-image-binding-test.sh'


def check(test, source, inherited):
    result = subprocess.run(['bash', '-c', source, str(SCRIPT)], cwd=ROOT,
                            env={**os.environ, 'DEPLOY_DRY_RUN': inherited},
                            text=True, capture_output=True, timeout=10)
    test.assertEqual(0, result.returncode, result.stdout + result.stderr)
    test.assertIn('crossed-environment batch bindings rejected', result.stdout)


class BatchBindingFixtureTest(unittest.TestCase):
    def test_actual_harness_passes_under_both_parent_modes(self):
        for mode in ['true', 'false']:
            with self.subTest(mode=mode):
                check(self, SCRIPT.read_text(), mode)

    def test_absent_environment_reset_is_caught_by_the_same_assertion(self):
        old = SCRIPT.read_text().replace('export DEPLOY_DRY_RUN=false\n', '')
        with self.assertRaisesRegex(AssertionError, 'crossed-environment batch image accepted'):
            check(self, old, 'true')
