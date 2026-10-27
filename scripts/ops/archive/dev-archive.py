#!/usr/bin/env python3
"""Run the existing offset-checkpointed archiver on the dev Mac only."""
import datetime as dt
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
NAS = Path('/Volumes/database/optionsedge')
BASH = '/opt/homebrew/bin/bash'



def verify_target():
    host = subprocess.check_output(['/bin/hostname'], text=True).strip()
    interfaces = subprocess.check_output(['/sbin/ifconfig'], text=True)
    if host != 'MacBook-Pro-2.local' or 'inet 192.168.100.102 ' not in interfaces:
        raise RuntimeError('Refusing archive operation: target must be MacBook-Pro-2.local / 192.168.100.102')

def environment():
    env = os.environ.copy()
    # Do not inherit production broker, topic, reader, or output overrides.
    for key in list(env):
        if key.startswith(('OE_', 'STRIKE_', 'INDICATOR_', 'SPOT_', 'CANONICAL_')) or key in (
                'BOOTSTRAP', 'TOPICS', 'ENV', 'ARCHIVE_DIR', 'SCHEMA_REGISTRY', 'UNTIL_TS',
                'SESSION_DATE', 'ALLOW_NON_NAS', 'LOCKFILE', 'SENTINEL', 'KAFKA_BIN'):
            del env[key]
    env.update(ENV='dev', BOOTSTRAP='localhost:19092',
               SCHEMA_REGISTRY='http://192.168.100.102:8082', ARCHIVE_DIR=str(NAS),
               OE_SPOT_REGISTRY_URL='http://192.168.100.102:8082',
               OE_CANONICAL_REGISTRY_URL='http://192.168.100.102:8082',
               OE_INDICATOR_REGISTRY_URL='http://192.168.100.102:8082',
               KAFKA_BIN=str(ROOT / 'kafka/bin'),
               CONFLUENT_HOME='/Users/abhinav/development/confluent-7.3.1',
               JAVA_HOME='/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home',
               PATH='/opt/homebrew/opt/coreutils/libexec/gnubin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin')
    return env


def topics(mode):
    key = 'OE_SPOT_TOPICS' if mode == 'spot' else 'OE_HEAVY_TOPICS_prod'
    return subprocess.check_output([BASH, '-c',
        '. "$1"; printf "%s" "${' + key + '}"', 'archive-policy', str(ROOT / 'oe-topics.env')], text=True)


def main(mode):
    if mode not in ('spot', 'daily', 'verify'):
        raise ValueError('mode must be spot, daily, or verify')
    now = dt.datetime.now(ZoneInfo('America/New_York'))
    if mode != 'spot' and (now.weekday() >= 5 or (now.hour, now.minute) <
                           ((20, 0) if mode == 'verify' else (17, 10))):
        return 0
    state = ROOT / 'state'
    state.mkdir(exist_ok=True)
    with (state / (mode + '.lock')).open('w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        marker = state / (mode + '-' + now.date().isoformat() + '.ok')
        if mode != 'spot' and marker.exists():
            return 0
        mount = subprocess.check_output(['/sbin/mount'], text=True)
        if not any(' on /Volumes/database (' in line and 'smbfs' in line for line in mount.splitlines()):
            raise RuntimeError('NAS SMB mount is absent; refusing local-disk fallback')
        if not (NAS / '.oe_nas_sentinel').is_file():
            raise RuntimeError('NAS sentinel is absent')
        env = environment()
        if mode == 'verify':
            env['LOG'] = str(ROOT / 'verify.log')
            command = [BASH, str(ROOT / 'oe-archive-verify.sh')]
        else:
            env.update(TOPICS=topics(mode), ARCHIVE_JOB=mode, ON_LOCK_BUSY='fail')
            if mode == 'spot':
                env['OE_SPOT_AVRO_TOPICS'] = 'underlying.spx.price'
            command = [BASH, str(ROOT / 'oe-archive-kafka.sh')]
        rc = subprocess.run(command, env=env).returncode
        if rc == 0 and mode != 'spot':
            marker.write_text(now.isoformat() + '\n')
        return rc


def serve():
    running = {}
    due = {}
    while True:
        now = time.monotonic()
        for mode in ('spot', 'daily', 'verify'):
            child = running.get(mode)
            if child is not None and child.poll() is not None:
                due[mode] = now + (30 if child.returncode else 600)
                del running[mode]
                child = None
            if child is None and now >= due.get(mode, 0):
                log = (ROOT / (mode + '.log')).open('a')
                try:
                    running[mode] = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), mode],
                                                      stdout=log, stderr=log)
                finally:
                    log.close()
        time.sleep(5)


if __name__ == '__main__':
    verify_target()
    if sys.argv[1] == '--serve':
        serve()
    else:
        sys.exit(main(sys.argv[1]))
