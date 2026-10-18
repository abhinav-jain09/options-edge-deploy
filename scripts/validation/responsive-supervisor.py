#!/usr/bin/env python3
"""es4-only owner of a bounded measurement tree; stop before RTH, retain partial evidence."""
import ctypes
from datetime import datetime, time as day_time, timedelta, timezone
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

NY = ZoneInfo('America/New_York')
MAX_SECONDS = 35 * 60
RTH_CLEARANCE_SECONDS = 15


def deadline_seconds(now):
    local = now.astimezone(NY)
    if local.weekday() < 5 and day_time(9, 30) <= local.time() < day_time(16):
        raise RuntimeError('OUTSIDE_RTH_REQUIRED')
    date = local.date()
    while date.weekday() >= 5 or datetime.combine(date, day_time(9, 30), NY) <= local:
        date += timedelta(days=1)
    boundary = datetime.combine(date, day_time(9, 30), NY).astimezone(timezone.utc)
    remaining = (boundary - now).total_seconds() - RTH_CLEARANCE_SECONDS
    if remaining <= 0:
        raise RuntimeError('RTH_CLEARANCE_REQUIRED')
    return min(MAX_SECONDS, remaining), boundary


def proc(pid):
    try:
        root = Path('/proc') / str(pid)
        fields = (root / 'stat').read_text().rpartition(')')[2].split()
        uid = int(re.search(r'^Uid:\s*(\d+)', (root / 'status').read_text(), re.M)[1])
        return {'parent': int(fields[1]), 'start': int(fields[19]), 'uid': uid}
    except (FileNotFoundError, ProcessLookupError):
        return None


class OwnedTree:
    def __init__(self):
        self.entries = {}
        self.runner = None

    def collect(self):
        for pid, row in list(self.entries.items()):
            if select.select([row['fd']], [], [], 0)[0]:
                os.close(row['fd'])
                del self.entries[pid]
                if pid != self.runner:  # Popen must retain the real runner exit status.
                    try:
                        os.waitpid(pid, os.WNOHANG)
                    except ChildProcessError:
                        pass
        rows = {}
        for name in os.listdir('/proc'):
            if name.isdigit():
                try:
                    row = proc(int(name))
                    if row and row['uid'] == os.getuid():
                        rows[int(name)] = row
                except PermissionError:
                    pass  # Other users are never candidates for ownership or signals.
        changed = True
        while changed:
            changed = False
            for pid, row in rows.items():
                if pid in self.entries:
                    continue
                parent = rows.get(row['parent'])
                owned_parent = self.entries.get(row['parent'])
                if row['parent'] != os.getpid() and not (parent and owned_parent and parent['start'] == owned_parent['start']):
                    continue
                if len(self.entries) >= 512:
                    raise RuntimeError('OWNED_PROCESS_CAPACITY')
                try:
                    fd = os.pidfd_open(pid)
                    current = proc(pid)
                    if current != row:
                        os.close(fd)
                        continue
                    self.entries[pid] = {**row, 'fd': fd}
                    changed = True
                except ProcessLookupError:
                    pass
        return self.live()

    def live(self):
        return [pid for pid, row in self.entries.items() if not select.select([row['fd']], [], [], 0)[0]]

    def send(self, sig):
        for row in self.entries.values():
            try:
                # pidfds bind the signal to the acquired process, never a reused PID.
                signal.pidfd_send_signal(row['fd'], sig)
            except ProcessLookupError:
                pass

    def close(self):
        for row in self.entries.values():
            os.close(row['fd'])


def run(command, seconds, root):
    stopped = [False]
    for value in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(value, lambda number, frame: stopped.__setitem__(0, True))
    parent = os.getppid()
    libc = ctypes.CDLL(None, use_errno=True)
    for option, value in [(36, 1), (1, signal.SIGTERM)]:  # SUBREAPER, PARENT-DEATH signal.
        if libc.prctl(option, value, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), 'Cannot establish owned-process lifecycle')
    if os.getppid() != parent or stopped[0]:
        raise RuntimeError('SUPERVISOR_PARENT_LOST')
    tree = OwnedTree()
    child = None
    reason = None
    cutoff = time.monotonic() + seconds
    try:
        child = subprocess.Popen(command, start_new_session=True)
        tree.runner = child.pid
        while True:
            live = tree.collect()
            if stopped[0]:
                reason = 'INTERRUPTED'
                break
            try:
                remaining_budget, _ = deadline_seconds(datetime.now(timezone.utc))
                cutoff = min(cutoff, time.monotonic() + remaining_budget)
            except RuntimeError:
                reason = 'RTH_BOUNDARY'
                break
            if time.monotonic() >= cutoff:
                reason = 'GUARDED_DEADLINE'
                break
            if child.poll() is not None:
                reason = 'ORPHANS_AFTER_RUN' if live else 'COMPLETE' if child.returncode == 0 else 'COMMAND_FAILED'
                break
            time.sleep(min(.1, max(0, cutoff - time.monotonic())))
    finally:
        if child:
            # Stop the runner first so it cannot launch another probe during cleanup.
            if child.poll() is None:
                child.terminate()
            tree.collect()
            tree.send(signal.SIGTERM)
            grace = time.monotonic() + 3
            while tree.live() and time.monotonic() < grace:
                time.sleep(.05)
                tree.collect()  # Detached orphan browsers reparent to this subreaper.
            tree.collect()
            tree.send(signal.SIGKILL)
            child.wait(timeout=3)
            end = time.monotonic() + 2
            while tree.live() and time.monotonic() < end:
                tree.send(signal.SIGKILL)
                time.sleep(.05)
                tree.collect()
            remaining = tree.live()
            (root / 'supervisor.json').write_text(json.dumps({'reason': reason, 'runnerExit': child.returncode, 'remainingOwned': remaining, 'status': 'PASS' if reason == 'COMPLETE' and not remaining else 'FAIL'}, indent=2) + '\n')
        tree.close()
    return 0 if reason == 'COMPLETE' and not remaining else 1


def main():
    if sys.platform != 'linux' or not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise RuntimeError('LINUX_PIDFD_REQUIRED')
    if '192.168.100.4' not in subprocess.check_output(['hostname', '-I'], text=True).split():
        raise RuntimeError('ES4_ONLY')
    fd = os.pidfd_open(os.getpid())
    try:
        signal.pidfd_send_signal(fd, 0)  # Verify kernel support before starting any child.
    finally:
        os.close(fd)
    root = Path(__file__).resolve().parents[2]
    if not re.fullmatch(r'/tmp/responsive-validation\.[A-Za-z0-9]{8}', str(root)):
        raise RuntimeError('OWNED_EPHEMERAL_ROOT_REQUIRED')
    os.chdir(root)
    os.sched_setaffinity(0, {3})  # Supervision stays off measured CPU2; probes explicitly taskset2.
    seconds, boundary = deadline_seconds(datetime.now(timezone.utc))
    (root / 'results').mkdir(exist_ok=True)
    (root / 'results' / 'deadline.json').write_text(json.dumps({'nextRthUtc': boundary.isoformat(), 'admittedSeconds': seconds, 'clearanceSeconds': RTH_CLEARANCE_SECONDS}) + '\n')
    marker = root / 'supervisor-active.json'
    marker.write_text(json.dumps({'pid': os.getpid(), 'start': proc(os.getpid())['start']}) + '\n')
    result = run(['bash', str(root / 'scripts/validation/run-responsive-stack-es4.sh')], seconds, root / 'results')
    marker.unlink()
    return result


if __name__ == '__main__':
    sys.exit(main())
