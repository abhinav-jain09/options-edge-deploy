"""Functional qualifier controls only; these synthetic numbers are never performance evidence."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/validation/qualify-responsive-stack.py'


class ResponsiveQualificationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.results = self.root / 'results'
        self.results.mkdir()
        hashes = {}
        for variant in ['baseline', 'candidate']:
            web = self.root / 'web' / variant / 'src/app/web'
            (web / 'assets').mkdir(parents=True)
            (web / 'ContextTapeAssets.java').write_text('static String javascript() {asset("test.js");}private static String asset')
            names = ['test.js', 'context-tape.css', 'context-tape-market-map.css', 'context-tape-orders.css']
            for name in names:
                (web / 'assets' / name).write_bytes(b'test-only')
            hashes[variant] = {name: hashlib.sha256(b'test-only').hexdigest() for name in names}
        for variant in ['baseline', 'candidate']:
            for count in [390, 1439]:
                for repeat in range(1, 6):
                    paths = {name: self.samples() for name in ['full-chart-resize', 'map-snapshot', 'orders-render-health']}
                    if variant == 'candidate':
                        paths.update({name: self.samples(native=name.startswith('floating-')) for name in ['floating-move', 'floating-resize', 'panel-dock-detach']})
                    memory = {'empty': self.memory(100), 'loaded': self.memory(200), 'retainedBytes': 100}
                    if variant == 'candidate':
                        memory.update(beforeCompanion=self.memory(200), companion=self.memory(300), returned=self.memory(200), companionHeapDelta=100, companionPssDelta=100)
                    row = {'status': 'MEASURED', 'variant': variant, 'count': count, 'repeat': repeat,
                           'assetHashes': hashes[variant], 'bookRecords': 4096, 'activePositions': 16, 'linkedPlans': 16,
                           'geometry': {'shades': 16, 'threads': 32, 'labels': 32}, 'paths': paths, 'memory': memory}
                    self.write(f'stack-{variant}-{count}-{repeat}', row)
        for repeat in range(1, 6):
            groups = []
            for probe, variant in [('existing-overlay', 'baseline'), ('existing-overlay', 'candidate'), ('linked-overlay', 'candidate')]:
                groups.append({'probe': probe, 'variant': variant, 'withinBudget': True, 'nativeFrames': 250, 'records': 200,
                               'budgetMs': 16, 'sampleP99Ms': 1, 'elapsedMs': 1, 'distinctFrameTimestamps': True,
                               'frameScheduler': 'native-requestAnimationFrame', 'fixedBindings': True, 'sameCandle': True,
                               'viewportUpdates': 250, 'shades': 16, 'threads': 32, 'labels': 32, 'stateTransitions': 1,
                               'workload': 'fixed-trade-viewport-pan-and-latched-stop'})
            (self.results / f'legacy-{repeat}.log').write_text(''.join('VISUAL_PROBE ' + json.dumps(row) + '\n' for row in groups))

    @staticmethod
    def samples(native=False):
        return {'records': 200, 'samples': [1] * 200, 'meanMs': 1, 'p99Ms': 1,
                **({'nativeFrames': 250, 'distinctFrameTimestamps': True, 'sameSvg': True} if native else {})}

    @staticmethod
    def memory(size):
        return {'heapBytes': size, 'isolates': ['one'], 'pss': {'bytes': size}}

    def write(self, name, row):
        (self.results / f'{name}.log').write_text('RESPONSIVE_PROBE ' + json.dumps(row) + '\n')

    def mutate(self, change, name='stack-candidate-1439-1'):
        row = json.loads((self.results / f'{name}.log').read_text().removeprefix('RESPONSIVE_PROBE '))
        change(row)
        self.write(name, row)

    def qualify(self, expected, command_failed='0', phrase=None):
        child = subprocess.run([sys.executable, str(SCRIPT), str(self.results), command_failed], capture_output=True, text=True)
        self.assertEqual(child.returncode, expected, child.stderr + child.stdout)
        summary = json.loads((self.results / 'responsive-summary.json').read_text())
        if phrase:
            self.assertTrue(any(phrase in error for error in summary['errors']), summary)

    def test_complete_synthetic_controls_pass(self):
        self.qualify(0)

    def test_command_failure_cannot_be_erased(self):
        self.qualify(1, '1', 'commands failed')

    def test_missing_repeat(self):
        (self.results / 'stack-candidate-1439-5.log').unlink()
        self.qualify(1, phrase='missing/invalid report')

    def test_wrong_source(self):
        self.mutate(lambda row: row['assetHashes'].update({'test.js': 'wrong'}))
        self.qualify(1, phrase='source hash mismatch')

    def test_missing_pss(self):
        self.mutate(lambda row: row['memory']['companion']['pss'].clear())
        self.qualify(1, phrase='mandatory deduplicated')

    def test_wrong_warmup_count(self):
        self.mutate(lambda row: row['paths']['floating-move'].update(nativeFrames=249))
        self.qualify(1, phrase='native frames')

    def test_p99_budget_and_summary_recomputation(self):
        self.mutate(lambda row: row['paths']['floating-resize'].update(samples=[17]*200, p99Ms=17, meanMs=17))
        self.qualify(1, phrase='16ms p99')

    def test_forged_summary(self):
        self.mutate(lambda row: row['paths']['map-snapshot'].update(meanMs=0))
        self.qualify(1, phrase='mean recomputation')

    def test_missing_geometry(self):
        self.mutate(lambda row: row['geometry'].update(labels=31))
        self.qualify(1, phrase='full coupled geometry')

    def test_relative_gate(self):
        for repeat in range(1, 6):
            self.mutate(lambda row: row['paths']['map-snapshot'].update(samples=[1.2]*200, meanMs=1.2, p99Ms=1.2), f'stack-candidate-1439-{repeat}')
        self.qualify(1, phrase='more than10percent')

    def test_retained_not_trusted(self):
        self.mutate(lambda row: row['memory'].update(retainedBytes=0))
        self.qualify(1, phrase='retained bytes must recompute')


if __name__ == '__main__':
    unittest.main()
