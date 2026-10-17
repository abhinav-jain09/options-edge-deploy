import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/validation/run-visual-orders-es4.sh'
PROGRAM = SCRIPT.read_text().rsplit("python3 - <<'PY'\n", 1)[1].split('\nPY', 1)[0]


class VisualOrderPerformanceGateTest(unittest.TestCase):
    def run_gate(self, candidate=192, failed_probe=None, omitted=None, samples=5):
        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory) / 'results'
            results.mkdir()
            for name in ['native-callback-baseline', 'native-callback-candidate', 'native-adapter-queue', 'quote-book', 'journal-postgres', 'model-geometry', 'model-history-page']:
                if name == omitted:
                    continue
                for i in range(samples):
                    row = {'probe': 'native-callback' if name.startswith('native-callback-') else name, 'nsPerRecord': 63 if name.endswith('-baseline') else candidate if name.endswith('-candidate') else 100, 'withinBudget': name != failed_probe}
                    (results / (('native-baseline' if name.endswith('-baseline') else 'native-candidate' if name.endswith('-candidate') else name) + '-' + str(i) + '.log')).write_text('VISUAL_PROBE ' + json.dumps(row) + '\n')
            return subprocess.run([sys.executable, '-c', PROGRAM], cwd=directory, capture_output=True, text=True, timeout=10), json.loads((results / 'summary.json').read_text())

    def test_accepted_wrapper_budget_keeps_relative_failure_visible(self):
        process, result = self.run_gate()
        self.assertEqual(0, process.returncode, process.stderr)
        self.assertEqual(1000, result['nativeWrapperBudgetNs'])
        self.assertTrue(result['nativeWrapperBudgetPass'])
        self.assertFalse(result['nativeRelativeBudgetPass'])
        self.assertAlmostEqual(192 / 63 - 1, result['nativeRelativeChange'])

    def test_exact_limit_is_accepted(self):
        process, result = self.run_gate(candidate=1000)
        self.assertEqual(0, process.returncode, process.stderr)
        self.assertTrue(result['nativeWrapperBudgetPass'])

    def test_over_budget_is_refused(self):
        process, result = self.run_gate(candidate=1001)
        self.assertFalse(result['nativeWrapperBudgetPass'])
        self.assertNotEqual(0, process.returncode)
        self.assertIn('Native callback wrapper median exceeded owner-approved 1000 ns budget', process.stderr)

    def test_negative_measurement_is_refused(self):
        process, result = self.run_gate(candidate=-1)
        self.assertFalse(result['nativeWrapperBudgetPass'])
        self.assertNotEqual(0, process.returncode)
        self.assertIn('or was negative', process.stderr)

    def test_other_probe_failure_still_blocks(self):
        process, result = self.run_gate(failed_probe='native-adapter-queue')
        self.assertTrue(result['nativeWrapperBudgetPass'])
        self.assertFalse(result['allNetNewBudgetsPass'])
        self.assertNotEqual(0, process.returncode)
        self.assertIn('A declared net-new budget failed', process.stderr)

    def test_missing_probe_still_blocks(self):
        process, _ = self.run_gate(omitted='quote-book')
        self.assertNotEqual(0, process.returncode)
        self.assertIn('Every declared probe must run', process.stderr)

    def test_five_commands_per_probe_still_required(self):
        process, _ = self.run_gate(samples=4)
        self.assertNotEqual(0, process.returncode)
        self.assertIn('Five repeated commands required for each scenario', process.stderr)


if __name__ == '__main__':
    unittest.main()
