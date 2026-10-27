#!/usr/bin/env python3
"""Install the reviewed archive unit and start its direct dev scheduler."""
import os
from pathlib import Path
import shutil
import signal
import plistlib
import shlex
import subprocess
import sys
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
    for name in FILES:
        if not ((SOURCE / name) if name != 'market_calendar.py' else SOURCE.parents[1] / 'jenkins/market_calendar.py').is_file():
            raise RuntimeError('Missing archive dependency: ' + name)
    DEST.mkdir(parents=True, exist_ok=True)
    # GNU timeout creates its own process group. Stop the complete existing worker tree,
    # including those groups, before swapping executable files or starting replacements.
    listing = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,args='], text=True)
    processes = {}
    for line in listing.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3:
            processes[int(parts[0])] = (int(parts[1]), parts[2])
    owned = {pid for pid, (_, command) in processes.items()
             if str(DEST) + '/' in command and pid != os.getpid()}
    while True:
        descendants = {pid for pid, (parent, _) in processes.items() if parent in owned}
        if descendants <= owned:
            break
        owned |= descendants
    for pid in sorted(owned, reverse=True):
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    if owned:
        time.sleep(1)
    for name in FILES:
        temporary = DEST / (name + ".installing")
        source = SOURCE / name if name != 'market_calendar.py' else SOURCE.parents[1] / 'jenkins/market_calendar.py'
        shutil.copy2(source, temporary)
        os.replace(temporary, DEST / name)
    kbin = DEST / 'kafka/bin'
    libs = DEST / 'kafka/libs'
    kbin.mkdir(parents=True, exist_ok=True)
    libs.mkdir(parents=True, exist_ok=True)
    for name in ('kafka-get-offsets', 'kafka-topics', 'kafka-console-consumer', 'kafka-consumer-groups'):
        target = Path('/Users/abhinav/development/confluent-7.3.1/bin') / name
        if not target.is_file():
            raise RuntimeError('Missing Kafka CLI: ' + str(target))
        wrapper = kbin / (name + '.sh')
        # The Mac's Confluent 7 CLI calls formatter settings --property; the Linux archive CLI
        # calls the same settings --formatter-property. Preserve every other argument exactly.
        wrapper.write_text('#!/opt/homebrew/bin/bash\nargs=()\nfor arg in "$@"; do\n'
                           '  if [ "$arg" = --formatter-property ]; then arg=--property; fi\n'
                           '  args+=("$arg")\ndone\nexec "' + str(target) + '" "${args[@]}"\n')
        wrapper.chmod(0o755)
    temporary = libs / 'canonical-archive-reader.jar.installing'
    shutil.copy2(DEST / 'canonical-archive-reader.jar', temporary)
    os.replace(temporary, libs / 'canonical-archive-reader.jar')
    # The shaded helper contains the released Kafka/Avro/registry dependencies used by the readers.
    domain = 'gui/' + str(os.getuid())
    for mode in ('spot', 'daily', 'verify'):
        label = 'com.optionsedge.dev-nas-archive-' + mode
        plist = Path.home() / 'Library/LaunchAgents' / (label + '.plist')
        subprocess.run(['launchctl', 'bootout', domain + '/' + label], capture_output=True)
        if plist.exists():
            plist.unlink()
    pidfile = DEST / 'scheduler.pid'
    label = 'com.optionsedge.dev-nas-archive'
    plist = Path.home() / 'Library/LaunchAgents' / (label + '.plist')
    subprocess.run(['launchctl', 'bootout', domain + '/' + label], capture_output=True)
    if '--direct' in sys.argv[1:]:
        with (DEST / 'scheduler.log').open('a') as log:
            child = subprocess.Popen(['/opt/homebrew/bin/python3', str(DEST / 'dev-archive.py'), '--serve'],
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        pidfile.write_text(str(child.pid) + '\n')
        print('Temporary direct scheduler started:', child.pid, '(login/reboot recovery unverified)')
        return
    command = 'exec /opt/homebrew/bin/python3 ' + shlex.quote(str(DEST / 'dev-archive.py')) + ' --serve'
    body = dict(Label=label, ProgramArguments=['/bin/bash', '-lc', command],
                RunAtLoad=True, KeepAlive=True, ThrottleInterval=30,
                StandardOutPath=str(DEST / 'scheduler.log'), StandardErrorPath=str(DEST / 'scheduler.log'))
    with plist.open('wb') as stream:
        plistlib.dump(body, stream)
    subprocess.run(['launchctl', 'bootstrap', domain, str(plist)], check=True)
    subprocess.run(['launchctl', 'print', domain + '/' + label], check=True, stdout=subprocess.DEVNULL)
    print('Installed restart-safe dev archive supervisor:', label)


if __name__ == '__main__':
    verify_target()
    main()
