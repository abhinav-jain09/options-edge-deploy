from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/validation/prepare-visual-orders-bundle.sh'
RESET = SCRIPT.read_text().split('reset_generated_dir(){', 1)[1].split('\n}\n', 1)[0]


class ValidationWorkspaceTest(unittest.TestCase):
    def reset(self, root, directory):
        return subprocess.run(['bash', '-c', 'set -euo pipefail\nreset_generated_dir(){' + RESET + '\n}\nreset_generated_dir "$1"', 'fixture', directory], cwd=root, capture_output=True, text=True, timeout=5)

    def test_previous_bundle_and_cache_are_emptied(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ['bundle', '.validation-m2']:
                path = root / name / 'nested'
                path.mkdir(parents=True)
                (path / 'obsolete.jar').write_text('obsolete')
                result = self.reset(root, name)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual([], list((root / name).iterdir()))

    def test_root_symlink_is_refused_without_touching_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'outside').mkdir()
            (root / 'outside' / 'preserve.jar').write_text('preserve')
            (root / 'bundle').symlink_to(root / 'outside', target_is_directory=True)
            result = self.reset(root, 'bundle')
            self.assertNotEqual(0, result.returncode)
            self.assertIn('Invalid validation output root', result.stderr)
            self.assertEqual('preserve', (root / 'outside' / 'preserve.jar').read_text())

    def test_nested_symlink_is_unlinked_without_following_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'bundle').mkdir()
            (root / 'outside').mkdir()
            (root / 'outside' / 'preserve.jar').write_text('preserve')
            (root / 'bundle' / 'link').symlink_to(root / 'outside', target_is_directory=True)
            result = self.reset(root, 'bundle')
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual([], list((root / 'bundle').iterdir()))
            self.assertEqual('preserve', (root / 'outside' / 'preserve.jar').read_text())


if __name__ == '__main__':
    unittest.main()
