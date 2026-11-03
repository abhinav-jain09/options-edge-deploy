#!/usr/bin/env python3
"""Install the archive on verified .102 with one launchd service per worker."""
import os
from pathlib import Path
import plistlib
import shlex
import shutil
import signal
import subprocess
import time

SOURCE = Path(__file__).resolve().parent
DEST = Path.home() / 'Library/Application Support/OptionsEdge/dev-nas-archive'
FILES = ('dev-archive.py', 'oe-archive-kafka.sh', 'oe-archive-verify.sh',
         'oe-alert.sh', 'oe-topics.env', 'oe-trading-day.sh', 'market_calendar.py',
         'StrikeArchiveReader.java', 'SpotArchiveReader.java', 'IndicatorArchiveReader.java',
         'CanonicalArchiveDecoder.java', 'options-edge-contracts.jar', 'canonical-archive-reader.jar')


def verify_target():
    host = subprocess.check_output(['/bin/hostname'], text=True).strip()
    interfaces = subprocess.check_output(['/sbin/ifconfig'], text=True)
    if host != 'MacBook-Pro-2.local' or 'inet 192.168.100.102 ' not in interfaces:
        raise RuntimeError('Refusing archive operation: target must be MacBook-Pro-2.local / 192.168.100.102')


def main():
    verify_target()
    sources = {name: SOURCE / name for name in FILES}
    sources['market_calendar.py'] = SOURCE.parents[1] / 'jenkins/market_calendar.py'
    for name, source in sources.items():
        if not source.is_file():
            raise RuntimeError('Missing dependency: ' + str(source))
    domain = 'gui/' + str(os.getuid())
    labels = ['com.optionsedge.dev-nas-archive'] + [
        'com.optionsedge.dev-nas-archive-' + mode for mode in ('spot', 'daily', 'verify')]
    for label in labels:
        subprocess.run(['launchctl', 'bootout', domain + '/' + label], capture_output=True)
    # Clean up only this runtime's legacy processes, before replacing any executable.
    rows = {}
    for line in subprocess.check_output(['ps', '-axo', 'pid=,ppid=,args='], text=True).splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3:
            rows[int(parts[0])] = (int(parts[1]), parts[2])
    owned = {pid for pid, (_, command) in rows.items() if str(DEST) + '/' in command}
    ancestors = {os.getpid()}
    pid = os.getpid()
    while pid in rows and rows[pid][0] not in ancestors:
        pid = rows[pid][0]
        ancestors.add(pid)
    owned -= ancestors
    while True:
        extra = {pid for pid, (parent, _) in rows.items() if parent in owned} - owned
        if not extra:
            break
        owned |= extra
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for pid in sorted(owned, reverse=True):
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass
        time.sleep(1)
    DEST.mkdir(parents=True, exist_ok=True)
    for name, source in sources.items():
        temporary = DEST / (name + '.installing')
        shutil.copy2(source, temporary)
        os.replace(temporary, DEST / name)
    helpers = DEST / 'bin'
    helpers.mkdir(exist_ok=True)
    timeout = helpers / 'timeout'
    timeout.write_text('#!/bin/sh\nexec /opt/homebrew/opt/coreutils/libexec/gnubin/timeout --foreground "$@"\n')
    timeout.chmod(0o755)
    kbin = DEST / 'kafka/bin'
    libs = DEST / 'kafka/libs'
    kbin.mkdir(parents=True, exist_ok=True)
    libs.mkdir(parents=True, exist_ok=True)
    for name in ('kafka-get-offsets', 'kafka-topics', 'kafka-console-consumer', 'kafka-consumer-groups'):
        target = Path('/Users/abhinav/development/confluent-7.3.1/bin') / name
        if not target.is_file():
            raise RuntimeError('Missing Kafka CLI: ' + str(target))
        wrapper = kbin / (name + '.sh')
        wrapper.write_text('#!/opt/homebrew/bin/bash\nargs=()\nfor arg in "$@"; do\n'
                           '  if [ "$arg" = --formatter-property ]; then arg=--property; fi\n'
                           '  args+=("$arg")\ndone\nexec "' + str(target) + '" "$' + '{args[@]}"\n')
        wrapper.chmod(0o755)
    temporary = libs / 'canonical-archive-reader.jar.installing'
    shutil.copy2(DEST / 'canonical-archive-reader.jar', temporary)
    os.replace(temporary, libs / 'canonical-archive-reader.jar')
    legacy = Path.home() / 'Library/LaunchAgents/com.optionsedge.dev-nas-archive.plist'
    legacy.unlink(missing_ok=True)
    for mode in ('spot', 'daily', 'verify'):
        label = 'com.optionsedge.dev-nas-archive-' + mode
        # Keep the already-authorized Bash as responsible parent; exec into Python changes
        # the macOS privacy identity. All capture children remain in launchd's process group.
        command = (
            "trap 'kill -TERM \"$worker_pid\" 2>/dev/null; wait \"$worker_pid\"; exit 143' TERM INT\n"
            '/opt/homebrew/bin/python3 ' + shlex.quote(str(DEST / 'dev-archive.py')) +
            ' ' + mode + ' &\nworker_pid=$!\nwait "$worker_pid"\nstatus=$?\nexit "$status"')
        body = dict(Label=label, ProgramArguments=['/opt/homebrew/bin/bash', '-c', command],
                    RunAtLoad=True, StartInterval=600,
                    KeepAlive={'SuccessfulExit': False}, ThrottleInterval=30,
                    ExitTimeOut=15, AbandonProcessGroup=False,
                    StandardOutPath=str(DEST / (mode + '.log')),
                    StandardErrorPath=str(DEST / (mode + '.log')))
        plist = Path.home() / 'Library/LaunchAgents' / (label + '.plist')
        with plist.open('wb') as stream:
            plistlib.dump(body, stream)
        subprocess.run(['launchctl', 'bootstrap', domain, str(plist)], check=True)
        subprocess.run(['launchctl', 'print', domain + '/' + label], check=True, stdout=subprocess.DEVNULL)
        print('Installed restart-safe .102 worker:', label, flush=True)


if __name__ == '__main__':
    main()
