import importlib.util
from pathlib import Path
import tempfile
import os
import subprocess
import sys
from unittest.mock import patch
import unittest

spec = importlib.util.spec_from_file_location('cache', Path(__file__).resolve().parents[1] / 'scripts/jenkins/quarantine-cvd-proof-bytecode.py')
p = importlib.util.module_from_spec(spec)
previous_bytecode = sys.dont_write_bytecode
try:
    sys.dont_write_bytecode = True
    spec.loader.exec_module(p)
finally:
    sys.dont_write_bytecode = previous_bytecode
NAME = 'cvd-component-source-proof.cpython-314.pyc'


class BytecodeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cvd-cache-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cache = self.root / 'scripts/jenkins/__pycache__'
        self.cache.mkdir(parents=True)

    def test_preserves_only_named_legacy_bytes_and_leaves_foreign_file_for_guard(self):
        (self.cache / NAME).write_bytes(b'exact legacy cache')
        (self.cache / 'foreign.pyc').write_bytes(b'must remain refused')
        moved = p.quarantine(self.root, '803')
        self.assertEqual([str(self.root / '.jenkins-tmp/cvd-proof-bytecode-803' / NAME)], moved)
        self.assertEqual(b'exact legacy cache', Path(moved[0]).read_bytes())
        self.assertFalse((self.cache / NAME).exists())
        self.assertEqual(b'must remain refused', (self.cache / 'foreign.pyc').read_bytes())
        self.assertEqual([], p.quarantine(self.root, '804'))

    def test_empty_domain_does_not_create_quarantine(self):
        self.assertEqual([], p.quarantine(self.root, '1'))
        self.assertFalse((self.root / '.jenkins-tmp').exists())

    def test_invalid_build_cannot_move_legacy_cache(self):
        (self.cache / NAME).write_bytes(b'cache')
        for build in ['0', '../other', '1/2', '9' * 11]:
            with self.subTest(build=build), self.assertRaisesRegex(ValueError, 'build number'):
                p.quarantine(self.root, build)
        self.assertTrue((self.cache / NAME).exists())

    def test_symbolic_file_and_parent_refuse_without_following(self):
        sentinel = self.root / 'sentinel'
        sentinel.write_bytes(b'preserve')
        (self.cache / NAME).symlink_to(sentinel)
        with self.assertRaisesRegex(ValueError, 'Regular bounded'):
            p.quarantine(self.root, '1')
        self.assertEqual(b'preserve', sentinel.read_bytes())
        (self.cache / NAME).unlink()
        (self.cache / NAME).write_bytes(b'cache')
        (self.root / '.jenkins-tmp').symlink_to(self.cache, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'Symbolic quarantine'):
            p.quarantine(self.root, '1')
        self.assertTrue((self.cache / NAME).exists())

    def test_source_cache_symlink_is_refused(self):
        self.cache.rmdir()
        self.cache.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'Symbolic source'):
            p.quarantine(self.root, '1')

    def test_count_size_and_reused_quarantine_refuse_before_move(self):
        file = self.cache / NAME
        file.write_bytes(b'x' * ((1 << 20) + 1))
        with self.assertRaisesRegex(ValueError, 'Regular bounded'):
            p.quarantine(self.root, '1')
        file.write_bytes(b'cache')
        parent = self.root / '.jenkins-tmp'
        parent.mkdir()
        (parent / 'cvd-proof-bytecode-1').mkdir()
        with self.assertRaises(FileExistsError):
            p.quarantine(self.root, '1')
        self.assertTrue(file.exists())
        for n in range(8):
            (self.cache / ('cvd-component-source-proof.cpython-' + str(200 + n) + '.pyc')).write_bytes(b'x')
        with self.assertRaisesRegex(ValueError, 'count exceeds'):
            p.quarantine(self.root, '2')

    def test_quarantine_count_is_bounded(self):
        (self.cache / NAME).write_bytes(b'cache')
        parent = self.root / '.jenkins-tmp'
        parent.mkdir()
        for n in range(128):
            (parent / ('cvd-proof-bytecode-' + str(n))).mkdir()
        with self.assertRaisesRegex(ValueError, 'count is full'):
            p.quarantine(self.root, '803')
        self.assertTrue((self.cache / NAME).exists())

    def test_main_refuses_without_all_guarded_jenkins_inputs(self):
        valid = {'JENKINS_URL': 'http://fixture', 'PERMITTED_SHA_GUARD': 'PASSED', 'ACTION': 'update-image', 'BUILD_NUMBER': '1'}
        for missing in ['JENKINS_URL', 'PERMITTED_SHA_GUARD', 'ACTION']:
            invalid = {k: v for k, v in valid.items() if k != missing}
            with self.subTest(missing=missing), patch.dict(os.environ, invalid, clear=True), self.assertRaisesRegex(ValueError, 'Guarded Jenkins'):
                p.main()

    def test_isolated_entry_point_cannot_load_a_shadow_module_or_write_bytecode(self):
        script = self.cache.parent / 'quarantine-cvd-proof-bytecode.py'
        script.write_text(Path(p.__file__).read_text())
        (self.cache.parent / 're.py').write_text("raise RuntimeError('shadow module executed')")
        (self.cache / NAME).write_bytes(b'old cache')
        env = {**os.environ, 'JENKINS_URL': 'fixture', 'PERMITTED_SHA_GUARD': 'PASSED', 'ACTION': 'update-image', 'BUILD_NUMBER': '1'}
        result = subprocess.run([sys.executable, '-I', '-B', str(script)], cwd=self.root, env=env, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual([], list(self.cache.iterdir()))
        self.assertEqual(b'old cache', (self.root / '.jenkins-tmp/cvd-proof-bytecode-1' / NAME).read_bytes())

    def test_jenkins_wiring_verifies_source_then_isolates_quarantine_then_checks_strictly(self):
        text = (Path(__file__).resolve().parents[1] / 'Jenkinsfile.es4-deploy').read_text()
        self.assertIn("PYTHONDONTWRITEBYTECODE = '1'", text)
        stage = text.split("stage('Update selected pinned image') {", 1)[1].split("stage('Deploy to es4')", 1)[0]
        initial = stage.index('--allow-ignored scripts/jenkins/__pycache__')
        quarantine = stage.index("sh 'python3 -I -B scripts/jenkins/quarantine-cvd-proof-bytecode.py'")
        strict = stage.index("--dir . --allow-ignored .jenkins-tmp'", quarantine)
        effect = stage.index("sh 'python3 scripts/es4/pinned-image-rollforward.py'")
        self.assertLess(initial, quarantine)
        self.assertLess(quarantine, strict)
        self.assertLess(strict, effect)
        self.assertIn('tests.test_cvd_proof_bytecode', stage)

    def test_exact_file_count_and_byte_boundary_are_accepted(self):
        for n in range(8):
            (self.cache / ('cvd-component-source-proof.cpython-' + str(200 + n) + '.pyc')).write_bytes(b'x' * (1 << 20))
        self.assertEqual(8, len(p.quarantine(self.root, '1')))
