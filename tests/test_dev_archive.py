"""Safety and completion contracts for the native .102 archive worker."""
import datetime
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('dev_archive', ROOT / 'scripts/ops/archive/dev-archive.py')
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)


class DevArchiveTest(unittest.TestCase):
    def test_rejects_wrong_hostname_or_ip(self):
        for host, interfaces in [('MacBook-Pro.local', 'inet 192.168.100.102 '),
                                 ('MacBook-Pro-2.local', 'inet 192.168.100.99 ')]:
            with self.subTest(host=host), patch.object(archive.subprocess, 'check_output',
                                                     side_effect=[host, interfaces]):
                with self.assertRaises(RuntimeError):
                    archive.verify_target()

    def test_production_overrides_are_not_inherited(self):
        with patch.dict(os.environ, {'BOOTSTRAP': 'production:9092', 'ENV': 'prod',
                                     'ARCHIVE_DIR': '/wrong', 'OE_TOPICS_ENV': '/wrong',
                                     'STRIKE_READER': '/wrong'}, clear=True):
            env = archive.environment()
        self.assertEqual(env['BOOTSTRAP'], 'localhost:19092')
        self.assertEqual(env['ENV'], 'dev')
        self.assertNotIn('OE_TOPICS_ENV', env)
        self.assertNotIn('STRIKE_READER', env)

    def test_weekend_heavy_capture_does_not_run(self):
        now = datetime.datetime(2026, 10, 31, 18, 0, tzinfo=ZoneInfo('America/New_York'))
        with patch.object(archive.dt, 'datetime') as clock, patch.object(archive.subprocess, 'run') as run:
            clock.now.return_value = now
            self.assertEqual(archive.main('daily'), 0)
            run.assert_not_called()

    def test_failed_capture_remains_retryable_and_success_runs_once(self):
        now = datetime.datetime(2026, 10, 30, 18, 0, tzinfo=ZoneInfo('America/New_York'))
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / 'runtime'
            runtime.mkdir()
            nas = Path(temporary) / 'nas'
            nas.mkdir()
            (nas / '.oe_nas_sentinel').write_text('test')
            with patch.object(archive, 'ROOT', runtime), patch.object(archive, 'NAS', nas), \
                 patch.object(archive.dt, 'datetime') as clock, \
                 patch.object(archive, 'topics', return_value='allowed.topic'), \
                 patch.object(archive.subprocess, 'check_output', return_value='server on /Volumes/database (smbfs)'), \
                 patch.object(archive.subprocess, 'run', return_value=Mock(returncode=2)) as run:
                clock.now.return_value = now
                marker = runtime / 'state/daily-2026-10-30.ok'
                self.assertEqual(archive.main('daily'), 2)
                self.assertFalse(marker.exists())
                run.return_value = Mock(returncode=0)
                self.assertEqual(archive.main('daily'), 0)
                self.assertTrue(marker.exists())
                self.assertEqual(archive.main('daily'), 0)
                self.assertEqual(run.call_count, 2)


if __name__ == '__main__':
    unittest.main()
