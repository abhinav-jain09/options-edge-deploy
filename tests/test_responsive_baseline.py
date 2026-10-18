"""Actual Git-object guard controls; no network, browser or performance measurement."""
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/validation/prepare-responsive-stack-bundle.sh'
PREFIX = 'src/app/web/assets/'
MODEL = PREFIX + 'context-tape-model.js'
STACK = PREFIX + 'context-tape-order-stack.js'
LEGACY = [PREFIX + name for name in ['context-tape-orders-model.js', 'context-tape-orders-overlay.js', 'context-tape-orders.css']]


class ResponsiveBaselineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.files = {**{name: 'I1 legacy' for name in LEGACY}, MODEL: 'old model'}
        self.reference = self.commit(self.files)
        self.files[MODEL] = 'shared current model'
        self.shared = self.commit(self.files, [self.reference])
        self.base = self.commit(self.files, [self.reference, self.shared])
        self.candidate_files = {**self.files, STACK: 'I3 stack'}
        self.candidate = self.commit(self.candidate_files, [self.base])

    def git(self, *args, input=None):
        return subprocess.run(['git', '-C', str(self.repo), *args], input=input, text=True,
                              capture_output=True, check=True, timeout=10).stdout.strip()

    def commit(self, files, parents=()):
        self.git('read-tree', '--empty')
        for name, value in files.items():
            self.git('update-index', '--add', '--cacheinfo', '100644', self.git('hash-object', '-w', '--stdin', input=value), name)
        args = ['commit-tree', self.git('write-tree')]
        for parent in parents:
            args.extend(['-p', parent])
        return self.git(*args, input='Synthetic guard fixture\n')

    def verify(self, base=None, candidate=None, script=SCRIPT):
        return subprocess.run(['bash', '-c', 'source "$1"; verify_responsive_baseline "$2" "$3" "$4" "$5" "$6"',
                               'baseline-unit', str(script), str(self.repo), base or self.base,
                               self.reference, self.shared, candidate or self.candidate],
                              capture_output=True, text=True, timeout=10)

    def refuse(self, result, marker):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(marker, result.stderr)

    def test_current_baseline_and_candidate_pass(self):
        self.assertEqual(self.verify().returncode, 0)

    def test_main_alone_is_not_the_approved_two_parent_baseline(self):
        self.refuse(self.verify(base=self.shared), 'BASELINE_PARENT_MISMATCH')

    def test_baseline_must_be_candidate_ancestor(self):
        self.refuse(self.verify(candidate=self.commit(self.candidate_files, [self.shared])), 'BASELINE_NOT_ANCESTOR')

    def test_legacy_mismatch_is_rejected_and_removing_guard_fools_only_the_broken_checker(self):
        bad = self.commit({**self.files, LEGACY[0]: 'wrong legacy'}, [self.reference, self.shared])
        candidate = self.commit(self.candidate_files, [self.candidate, bad])
        self.assertNotEqual(self.git('rev-parse', bad + ':' + LEGACY[0]), self.git('rev-parse', self.reference + ':' + LEGACY[0]))
        self.refuse(self.verify(bad, candidate), 'LEGACY_BASELINE_ASSET_MISMATCH: context-tape-orders-model.js')
        source = SCRIPT.read_text()
        guard = '[ "$actual" = "$expected" ] || { echo "LEGACY_BASELINE_ASSET_MISMATCH: $asset" >&2; return 1; }'
        self.assertEqual(source.count(guard), 1)
        broken = self.repo / 'removed-legacy-guard.sh'
        broken.write_text(source.replace(guard, 'true'))
        result = self.verify(bad, candidate, broken)
        self.assertEqual(result.returncode, 0, result.stderr)
        with self.assertRaises(AssertionError):
            self.refuse(result, 'LEGACY_BASELINE_ASSET_MISMATCH: context-tape-orders-model.js')

    def test_baseline_cannot_silently_restore_the_old_shared_model(self):
        bad = self.commit({**self.files, MODEL: 'old model'}, [self.reference, self.shared])
        self.refuse(self.verify(bad, self.commit(self.candidate_files, [self.candidate, bad])), 'SHARED_CANONICAL_MODEL_MISMATCH')

    def test_candidate_canonical_mismatch(self):
        self.refuse(self.verify(candidate=self.commit({**self.candidate_files, MODEL: 'other model'}, [self.candidate])), 'CANONICAL_TAPE_MODEL_MISMATCH')

    def test_baseline_cannot_include_the_stack_module(self):
        bad = self.commit(self.candidate_files, [self.reference, self.shared])
        self.refuse(self.verify(bad, self.commit(self.candidate_files, [self.candidate, bad])), 'BASELINE_CONTAINS_I3_STACK_MODULE')

    def test_missing_candidate_model_fails_explicitly(self):
        self.refuse(self.verify(candidate=self.commit({name: value for name, value in self.candidate_files.items() if name != MODEL}, [self.candidate])), 'CANDIDATE_CANONICAL_MODEL_REQUIRED')

    def test_invalid_revision_cannot_be_git_options(self):
        self.refuse(self.verify(candidate='--help'), 'BASELINE_SHA_FORMAT')


if __name__ == '__main__':
    unittest.main()
