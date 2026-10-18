"""Execute real collection/cleanup code with deny-by-construction local SSH/rsync fakes."""
import os
from pathlib import Path
import shutil
import secrets
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/validation/responsive-evidence.sh'


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.local_temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.local_temp.cleanup)
        self.local = Path(self.local_temp.name)
        while True:
            self.remote = Path('/tmp/responsive-validation.' + secrets.token_hex(4))
            try:
                self.remote.mkdir(mode=0o700)
                break
            except FileExistsError:
                pass
        self.addCleanup(lambda: shutil.rmtree(self.remote, ignore_errors=True))
        (self.remote / 'results').mkdir()
        (self.remote / 'results/sample.log').write_text('failed sample must be retained')
        (self.remote / 'source-identities.json').write_text('{"testOnly":true}')
        (self.remote / 'manifest.sha256').write_text('test-only source manifest')
        (self.remote / 'ephemeral-source').mkdir()
        (self.remote / 'ephemeral-source/code.js').write_text('test-only ephemeral source')
        binary = self.local / 'bin'
        binary.mkdir()
        ssh = binary / 'ssh'
        ssh.write_text('#!' + sys.executable + '''
import subprocess,sys
args=sys.argv[1:];index=args.index('abhinav@192.168.100.4')
assert args[:index]==['-o','BatchMode=yes','-o','ConnectTimeout=10']
# Only the source-controlled command on this test's own temp directory runs.
raise SystemExit(subprocess.run(args[index+1:],input=sys.stdin.buffer.read()).returncode)
''')
        transfer = binary / 'rsync'
        transfer.write_text('#!' + sys.executable + '''
import os,pathlib,shutil,sys
source,destination=sys.argv[-2:];prefix='abhinav@192.168.100.4:'
assert source.startswith(prefix+'/tmp/responsive-validation.')
source=pathlib.Path(source.removeprefix(prefix));destination=pathlib.Path(destination)
mode=os.environ.get('TEST_TRANSFER','')
if source.is_dir():
    if mode=='partial':
        shutil.copy2(source/'sample.log',destination/'sample.log')
        raise SystemExit(1)
    shutil.copytree(source,destination,dirs_exist_ok=True,symlinks=True)
    if mode=='corrupt':(destination/'sample.log').write_text('corrupt')
    if mode=='extra':(destination/'extra.log').write_text('not in manifest')
else:shutil.copy2(source,destination)
''')
        ssh.chmod(0o700)
        transfer.chmod(0o700)
        self.env = {**os.environ, 'PATH': str(binary) + os.pathsep + os.environ['PATH']}

    def invoke(self, action, argument, expected):
        child = subprocess.run(['bash', str(SCRIPT), action, str(self.remote), argument], cwd=self.local,
                               env=self.env, capture_output=True, text=True)
        self.assertEqual(child.returncode, expected, child.stderr + child.stdout)
        return child.stdout + child.stderr

    def test_successful_verified_collection_then_delete(self):
        self.invoke('collect', 'results', 0)
        self.assertEqual((self.local / 'results/sample.log').read_text(), 'failed sample must be retained')
        self.invoke('cleanup', '1', 0)
        self.assertFalse(self.remote.exists())

    def test_partial_transfer_retains_originals_and_cleans_sources(self):
        self.env['TEST_TRANSFER'] = 'partial'
        self.invoke('collect', 'results', 1)
        message = self.invoke('cleanup', '0', 0)
        self.assertIn(str(self.remote), message)
        self.assertEqual((self.remote / 'evidence/sample.log').read_text(), 'failed sample must be retained')
        self.assertFalse((self.remote / 'ephemeral-source').exists())
        self.assertTrue((self.remote / 'source-identities.json').exists(), 'Keep immutable provenance with retained evidence')

    def test_hash_mismatch_never_deletes_original(self):
        self.env['TEST_TRANSFER'] = 'corrupt'
        self.invoke('collect', 'results', 1)
        self.invoke('cleanup', '0', 0)
        self.assertEqual((self.remote / 'evidence/sample.log').read_text(), 'failed sample must be retained')

    def test_unmanifested_file_refused(self):
        self.env['TEST_TRANSFER'] = 'extra'
        self.invoke('collect', 'results', 1)
        self.invoke('cleanup', '0', 0)
        self.assertTrue((self.remote / 'evidence/sample.log').exists())

    def test_active_supervisor_prevents_collection_and_cleanup(self):
        (self.remote / 'supervisor-active.json').write_text('{}')
        self.invoke('collect', 'results', 1)
        self.invoke('cleanup', '0', 1)
        self.assertTrue((self.remote / 'source-identities.json').exists())
        self.assertTrue((self.remote / 'results/sample.log').exists())

    def test_evidence_symlink_refused_without_following(self):
        target = self.local / 'untouched'
        target.write_text('untouched')
        (self.remote / 'results/link').symlink_to(target)
        self.invoke('collect', 'results', 1)
        self.invoke('cleanup', '0', 0)
        self.assertEqual(target.read_text(), 'untouched')
        self.assertTrue((self.remote / 'evidence/link').is_symlink())


if __name__ == '__main__':
    unittest.main()
