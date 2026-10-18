"""Functional boundary/owned-process checks; synthetic times are not benchmark evidence."""
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'scripts/validation/responsive-supervisor.py'
spec = importlib.util.spec_from_file_location('supervisor', SOURCE)
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class BoundaryTests(unittest.TestCase):
    def budget(self, text):
        return supervisor.deadline_seconds(datetime.fromisoformat(text).replace(tzinfo=timezone.utc))

    def test_rth_refused(self):
        for text in ['2026-10-19T13:30:00', '2026-10-19T19:59:59']:
            with self.assertRaisesRegex(RuntimeError, 'OUTSIDE_RTH'):
                self.budget(text)

    def test_clearance_refused(self):
        with self.assertRaisesRegex(RuntimeError, 'CLEARANCE'):
            self.budget('2026-10-19T13:29:50')

    def test_deadline_precedes_open(self):
        seconds, boundary = self.budget('2026-10-19T13:29:00')
        self.assertEqual(seconds, 45)
        self.assertEqual(boundary.isoformat(), '2026-10-19T13:30:00+00:00')

    def test_weekend_and_dst(self):
        seconds, boundary = self.budget('2026-10-18T10:00:00')
        self.assertEqual(seconds, 2100)
        self.assertEqual(boundary.isoformat(), '2026-10-19T13:30:00+00:00')
        self.assertEqual(self.budget('2026-11-01T10:00:00')[1].isoformat(), '2026-11-02T14:30:00+00:00')

    def test_after_close_next_day(self):
        self.assertEqual(self.budget('2026-10-19T20:00:00')[1].isoformat(), '2026-10-20T13:30:00+00:00')


@unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'pidfd_open'), 'Owned Linux pidfd lifecycle is verified on es4, never inferred from Mac')
class OwnedProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_nonzero_runner_is_not_reaped_as_success(self):
        result = supervisor.run([sys.executable, '-c', 'raise SystemExit(7)'], 5, self.root)
        row = json.loads((self.root / 'supervisor.json').read_text())
        self.assertEqual(result, 1)
        self.assertEqual(row['runnerExit'], 7)
        self.assertEqual(row['reason'], 'COMMAND_FAILED')

    def test_success(self):
        self.assertEqual(supervisor.run([sys.executable, '-c', 'pass'], 5, self.root), 0)

    def test_deadline_kills_detached_browser_like_child(self):
        pidfile = self.root / 'descendant.pid'
        script = "import subprocess,sys,time;from pathlib import Path;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True);Path(sys.argv[1]).write_text(str(p.pid));time.sleep(60)"
        result = supervisor.run([sys.executable, '-c', script, str(pidfile)], 1, self.root)
        row = json.loads((self.root / 'supervisor.json').read_text())
        self.assertEqual(result, 1)
        self.assertEqual(row['reason'], 'GUARDED_DEADLINE')
        self.assertEqual(row['remainingOwned'], [])
        self.assertTrue(pidfile.exists(), 'Functional child must actually have been launched')
        self.assertFalse(Path('/proc', pidfile.read_text()).exists(), 'Detached orphan must be killed and reaped')


if __name__ == '__main__':
    unittest.main()
