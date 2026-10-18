"""Functional boundary/owned-process checks; synthetic times are not benchmark evidence."""
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

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


class MarkerTests(unittest.TestCase):
    def test_marker_survives_known_writers_and_failed_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'results').mkdir()
            marker = root / 'supervisor-active.json'
            for report in [{'cleanupComplete': False, 'remainingOwned': [123]},
                           {'cleanupComplete': False, 'remainingOwned': []},
                           {'cleanupComplete': True, 'remainingOwned': [123]}]:
                marker.write_text('{}')
                (root / 'results/supervisor.json').write_text(json.dumps(report))
                supervisor.release_marker(root)
                self.assertTrue(marker.exists(), report)
            (root / 'results/supervisor.json').write_text(json.dumps({'cleanupComplete': True, 'remainingOwned': []}))
            supervisor.release_marker(root)
            self.assertFalse(marker.exists())


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

    def test_process_capacity_terminates_overflow_descendants(self):
        original = supervisor.OwnedTree
        pidfile = self.root / 'overflow.json'
        class SmallTree(original):
            def __init__(self):
                super().__init__()
                self.limit = 2
            def collect(self, terminating=False):
                if not self.entries and not terminating:
                    end = supervisor.time.monotonic() + 2
                    while not pidfile.exists() and supervisor.time.monotonic() < end:
                        supervisor.time.sleep(.01)
                return super().collect(terminating)
        script = "import subprocess,sys,time,json;from pathlib import Path;ps=[subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True) for _ in range(5)];Path(sys.argv[1]).write_text(json.dumps([p.pid for p in ps]));time.sleep(60)"
        with patch.object(supervisor, 'OwnedTree', SmallTree):
            self.assertEqual(supervisor.run([sys.executable, '-c', script, str(pidfile)], 5, self.root), 1)
        row = json.loads((self.root / 'supervisor.json').read_text())
        self.assertEqual(row['reason'], 'OWNED_PROCESS_CAPACITY')
        self.assertTrue(row['cleanupComplete'])
        self.assertEqual(row['remainingOwned'], [])
        for pid in json.loads(pidfile.read_text()):
            self.assertFalse(Path('/proc', str(pid)).exists(), 'Overflow must be killed AND reaped')

    def test_discovery_failure_still_terminates_tracked_processes(self):
        original = supervisor.OwnedTree
        owned = []
        class FailingTree(original):
            def collect(self, terminating=False):
                if self.entries:
                    raise RuntimeError('INJECTED_DISCOVERY_FAILURE')
                result = super().collect(terminating)
                owned.extend(result)
                return result
        with patch.object(supervisor, 'OwnedTree', FailingTree):
            self.assertEqual(supervisor.run([sys.executable, '-c', 'import time;time.sleep(60)'], 5, self.root), 1)
        row = json.loads((self.root / 'supervisor.json').read_text())
        self.assertIn('INJECTED_DISCOVERY_FAILURE', row['reason'])
        self.assertFalse(row['cleanupComplete'])
        self.assertTrue(row['cleanupErrors'])
        self.assertTrue(owned, 'Must acquire an actual writer before discovery fails')
        for pid in owned:
            state = supervisor.proc(pid)
            self.assertTrue(state is None or Path('/proc', str(pid), 'stat').read_text().rpartition(')')[2].split()[0] == 'Z', 'Known writer must no longer execute')
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass

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
