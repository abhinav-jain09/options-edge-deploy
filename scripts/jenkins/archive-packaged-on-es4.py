#!/usr/bin/env python3
"""Bounded packaged-codec check on es4. No broker or production-host access."""
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import zipfile

HOST = 'abhinav@192.168.100.4'
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=2']
ROOT = '/home/abhinav/ci/archive-packaged'


def sdk_parity(sdk, fat):
    if sdk.stat().st_size > 32 << 20 or fat.stat().st_size > 128 << 20:
        raise ValueError('archive package size bound')
    with zipfile.ZipFile(sdk) as original, zipfile.ZipFile(fat) as bundled:
        for archive in [original, bundled]:
            entries = archive.infolist()
            if len(entries) > 65536 or sum(e.file_size for e in entries) > 512 << 20:
                raise ValueError('archive ZIP expansion bound')
            names = [e.filename for e in entries]
            if len(set(names)) != len(names):
                raise ValueError('duplicate archive member')
        count = 0
        for entry in original.infolist():
            if entry.is_dir() or entry.filename.startswith('META-INF/'):
                continue  # Shade merges service metadata; actual serializer behavior is tested below.
            if entry.file_size > 16 << 20 or original.read(entry) != bundled.read(entry.filename):
                raise ValueError('bundled SDK member differs: ' + entry.filename)
            count += 1
        if count < 1:
            raise ValueError('empty SDK parity check')
    return count


def check_window():
    now = datetime.now(ZoneInfo('America/New_York'))
    if now.weekday() < 5 and 825 <= now.hour * 100 + now.minute < 1600:
        raise ValueError('es4 packaged check refused during cash-window exclusion')


def main():
    if not os.environ.get('JENKINS_URL'):
        raise ValueError('Jenkins only')
    for name in ['PERMITTED_SHA', 'CONTRACTS_PERMITTED_SHA']:
        if not re.fullmatch('[0-9a-f]{40}', os.environ.get(name, '')):
            raise ValueError('exact main provenance required')
    check_window()
    build = os.environ['BUILD_NUMBER']
    if not re.fullmatch('[0-9]+', build):
        raise ValueError('invalid build')
    out = Path('.jenkins-tmp/archive-packaged-' + build)
    out.mkdir()  # Never reuse a prior attempt's result.
    helper = Path('.jenkins-tmp/canonical-archive-reader.jar')
    sdk = Path('.jenkins-tmp/contracts/target/options-edge-contracts-archive.jar')
    count = sdk_parity(sdk, helper)
    inputs = {
        'reader.jar': helper,
        'CanonicalArchiveDecoderTest.java': Path('tests/archive-canonical/CanonicalArchiveDecoderTest.java'),
        'fixtures/src/test/resources/cvd/producer-23866b17.json': Path('.jenkins-tmp/contracts/src/test/resources/cvd/producer-23866b17.json'),
        'fixtures/src/test/resources/cvd/direction-output-renderer-models.json': Path('.jenkins-tmp/contracts/src/test/resources/cvd/direction-output-renderer-models.json'),
    }
    hashes = {}
    with tarfile.open(out / 'input.tar.gz', 'w:gz') as archive:
        for name, path in inputs.items():
            if path.is_symlink() or path.stat().st_size > 128 << 20:
                raise ValueError('invalid packaged check input')
            data = path.read_bytes()
            hashes[name] = hashlib.sha256(data).hexdigest()
            member = tarfile.TarInfo(name); member.size = len(data); member.mode = 0o600
            archive.addfile(member, io.BytesIO(data))
        data = json.dumps(hashes).encode()
        member = tarfile.TarInfo('hashes.json'); member.size = len(data); member.mode = 0o600
        archive.addfile(member, io.BytesIO(data))
    if (out / 'input.tar.gz').stat().st_size > 128 << 20:
        raise ValueError('packaged check bundle exceeds128MiB')
    init = '''set -eu
umask 077
mkdir -p /home/abhinav/ci/archive-packaged
[ ! -L /home/abhinav/ci/archive-packaged ]
[ "$(du -sk /home/abhinav/ci/archive-packaged | cut -f1)" -lt 1048576 ]
command -v systemd-run >/dev/null
[ -x /usr/lib/jvm/java-21-openjdk/bin/java ]
mktemp -d /home/abhinav/ci/archive-packaged/run.XXXXXXXX
'''
    remote = subprocess.run(SSH + [HOST, 'bash', '-s'], input=init, text=True, capture_output=True, timeout=30, check=True).stdout.strip()
    if not re.fullmatch(re.escape(ROOT) + r'/run\.[A-Za-z0-9]{8}', remote):
        raise ValueError('unexpected private es4 directory')
    try:
        subprocess.run(['scp', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=10', str(out / 'input.tar.gz'), HOST + ':' + remote + '/input.tar.gz'], timeout=60, check=True)
        check_window()
        command = r'''set -euo pipefail
cd "$1"
[ "$(stat -c%s input.tar.gz)" -le 134217728 ]
tar -xzf input.tar.gz
python3 - <<'VERIFY'
import hashlib,json
from pathlib import Path
for name,want in json.loads(Path('hashes.json').read_text()).items():
 assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==want, name
VERIFY
cpus=$(python3 -c 'import os; a=sorted(os.sched_getaffinity(0)); assert len(a)>=2; print(str(a[0])+","+str(a[1]))')
unit="oe-archive-packaged-${PWD##*/}"
systemd-run --user --quiet --wait --pipe --collect --unit="$unit" \
 --working-directory="$PWD" --property=MemoryMax=1G --property=MemorySwapMax=0 --property=TasksMax=128 \
 --property=RuntimeMaxSec=60 --property=TimeoutStopSec=5 --property=KillMode=control-group \
 taskset --cpu-list "$cpus" env -i PATH=/usr/lib/jvm/java-21-openjdk/bin:/usr/bin:/bin \
 JAVA_HOME=/usr/lib/jvm/java-21-openjdk bash -c '
 set -euo pipefail
 group=$(sed -n "s/^0:://p" /proc/self/cgroup)
 [ "$(cat /sys/fs/cgroup$group/memory.max)" = 1073741824 ]
 [ "$(cat /sys/fs/cgroup$group/memory.swap.max)" = 0 ]
 [ "$(cat /sys/fs/cgroup$group/pids.max)" = 128 ]
 java -version
 mkdir classes
 javac -J-Xmx256m --release 21 -cp reader.jar -d classes CanonicalArchiveDecoderTest.java
 java -Xmx512m -cp "reader.jar:classes" CanonicalArchiveDecoderTest fixtures reader.jar
 '
'''
        with (out / 'test.log').open('wb') as log:
            subprocess.run(SSH + [HOST, 'bash', '-s', '--', remote], input=command.encode(), stdout=log, stderr=subprocess.STDOUT, timeout=90, check=True)
        log = (out / 'test.log').read_text()
        if 'Canonical archive finite codec/layout assertions PASS 56' not in log or 'Canonical archive helper identity PASS' not in log:
            raise ValueError('missing packaged-codec execution evidence')
        proof = {'source': os.environ['PERMITTED_SHA'], 'contractsSource': os.environ['CONTRACTS_PERMITTED_SHA'],
                 'helperSha256': hashes['reader.jar'], 'sdkSha256': hashlib.sha256(sdk.read_bytes()).hexdigest(),
                 'sdkMembersMatched': count, 'os': 'Linux es4', 'codecAssertions': 56, 'heapMiB': 512,
                 'cgroupMiB': 1024, 'swapping': False, 'brokerAccess': False, 'productionAccess': False,
                 'scope': 'packaged codec and class identity only; live source-domain/throughput/restore remains open'}
        (out / 'receipt.json').write_text(json.dumps(proof, indent=2) + '\n')
        print(json.dumps(proof))
    finally:
        # Stop only this private transient unit, then remove only its private directory.
        cleanup = 'systemctl --user stop oe-archive-packaged-${1##*/} >/dev/null 2>&1 || true\nrm -rf -- "$1"\n'
        subprocess.run(SSH + [HOST, 'bash', '-s', '--', remote], input=cleanup, text=True, timeout=30, check=True)


if __name__ == '__main__':
    main()
