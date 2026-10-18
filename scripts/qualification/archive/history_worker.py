"""ES4 private retained-history qualification; no production archive path or offset marker."""
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import time

MAX_OUTPUT = 2 << 30
MAX_LOG = 4 << 20
MAX_SPAN = 2_000_000
JAVA = '/usr/lib/jvm/java-21-openjdk/bin/java'

def digest(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as source:
        while block:=source.read(1 << 20):value.update(block)
    return value.hexdigest()
BINDINGS = {**{t: ('192.168.100.4:9092', 'http://192.168.100.4:8081') for t in (
    'es.futures.cvd.bars', 'es.futures.footprint.bars', 'es.futures.footprint.outcomes', 'es.futures.footprint.strike')},
    'context-tape.direction.ledger': ('192.168.100.102:19092', 'http://192.168.100.102:8082')}


def validate_inventory(rows):
    if not isinstance(rows, list) or not 5 <= len(rows) <= 80:
        raise ValueError('bounded five-topic inventory required')
    seen = set()
    span = 0
    for row in rows:
        if not isinstance(row,dict): raise ValueError('inventory row must be an object')
        if (row.get('bootstrap'), row.get('registry')) != BINDINGS.get(row.get('topic')):
            raise ValueError('only declared ES4/dev archive sources are admitted')
        if any(type(row.get(k)) is not int for k in ['partition', 'from', 'until']):
            raise ValueError('integer coordinates required')
        if not 0 <= row['partition'] < 16 or not 0 <= row['from'] <= row['until']:
            raise ValueError('invalid coordinates')
        key = row['topic'], row['partition']
        if key in seen:
            raise ValueError('duplicate partition')
        seen.add(key)
        span += row['until'] - row['from']
    if {t for t, _ in seen} != set(BINDINGS) or span > MAX_SPAN:
        raise ValueError('incomplete inventory or retained offset-span budget exceeded')
    return rows


def capture_command(row, output, summary, hosts):
    # This path deliberately has no --mark-group/--mark-offset or application checkpoint argument.
    return [JAVA, '-Djdk.net.hosts.file='+str(hosts), '-Xms32m', '-Xmx512m', '-XX:MaxDirectMemorySize=64m', '-XX:MaxMetaspaceSize=64m', '-XX:ReservedCodeCacheSize=32m', '-Xss512k', '-XX:ActiveProcessorCount=2', '-cp', 'reader.jar',
            'StrikeArchiveReader', '--bootstrap', row['bootstrap'], '--canonical-registry', row['registry'],
            '--topic', row['topic'], '--partition', str(row['partition']), '--from', str(row['from']),
            '--max-end', str(row['until']), '--deadline-ms', '900000', '--poll-ms', '200',
            '--out', str(output), '--summary', str(summary)]


def parse_summary(text, row):
    fields = dict(re.findall(r'(\w+)=([^\s]+)', text))
    if fields.get('status') != 'COMPLETE' or fields.get('topic') != row['topic']:
        raise ValueError('capture did not complete its declared topic')
    numbers = {k: int(fields[k]) for k in ['partition', 'from', 'lso', 'boundary', 'position', 'records', 'elapsed_ms']}
    if numbers['partition'] != row['partition'] or numbers['from'] != row['from'] or numbers['boundary'] != row['until'] or numbers['lso'] < numbers['boundary'] or numbers['position'] < numbers['boundary'] or numbers['records'] < 0 or not 0 <= numbers['elapsed_ms'] <= 900000:
        raise ValueError('capture boundary/count/deadline proof differs from inventory')
    return numbers


def usage_kib(pid):
    try:
        text = Path('/proc/' + str(pid) + '/status').read_text()
        return int(re.search(r'^VmHWM:\s+(\d+) kB$', text, re.M)[1])
    except (OSError, TypeError):
        return 0


def supervise(command, logpath, remaining):
    def limits(): resource.setrlimit(resource.RLIMIT_FSIZE, (remaining, remaining))
    with logpath.open('xb') as log:
        start = time.monotonic(); process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, preexec_fn=limits)
        peak = 0
        try:
            while process.poll() is None:
                peak = max(peak, usage_kib(process.pid))
                if time.monotonic()-start > 900 or peak > 768*1024 or logpath.stat().st_size > MAX_LOG:
                    raise ValueError('capture wall/RSS/log budget exceeded')
                time.sleep(.1)
            wall = time.monotonic()-start
            if process.returncode or wall > 900 or peak <= 0: raise ValueError('native capture failed or exceeded complete-process deadline')
            return wall, peak
        finally:
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired: process.kill(); process.wait(timeout=5)


def verify_lines(path, row, expected):
    count=0; maximum=0; previous=row['from']-1; value=hashlib.sha256()
    limit=(16 << 20)+8192
    with path.open('rb') as stream:
        while line:=stream.readline(limit+1):
            if len(line)>limit or not line.endswith(b'\n'):raise ValueError('archive physical line bound/completeness')
            fields=line.split(b'\t',4)
            if len(fields)!=5 or fields[1]!=('Partition:'+str(row['partition'])).encode():raise ValueError('archive partition/layout differs')
            match=re.fullmatch(rb'Offset:([0-9]{1,19})',fields[2])
            if not match:raise ValueError('archive offset field missing')
            offset=int(match[1])
            if not row['from']<=offset<row['until'] or offset<=previous:raise ValueError('archive offsets outside snapshot or not strictly increasing')
            previous=offset;count+=1;maximum=max(maximum,len(line));value.update(line)
    if count!=expected:raise ValueError('physical record count differs')
    return count,maximum,value.hexdigest()


def save_result(result):
    temporary=Path('result.json.new')
    with temporary.open('w') as out:json.dump(result,out,indent=2);out.write('\n');out.flush();os.fsync(out.fileno())
    os.replace(temporary,'result.json')


def verify_inputs(manifest):
    files=manifest['files']
    if set(files)!={'reader.jar','ArchiveHistoryInventory.java','history_worker.py'}:
        raise ValueError('qualification input inventory differs')
    for name, expected_digest in files.items():
        if Path(name).is_symlink() or digest(name)!=expected_digest:
            raise ValueError('qualification input bytes differ')


def validate_cpu_affinity(available):
    cpus=sorted(available)
    if len(cpus)!=2 or any(type(cpu)is not int or not 0<=cpu<=4095 for cpu in cpus):
        raise ValueError('exactly two inherited qualification CPUs required')
    return cpus


def main():
    root = Path.cwd().resolve()
    if not re.fullmatch(r'/home/abhinav/ci/cvd-archive-history/run\.[0-9]+\.[a-z0-9_]{8}', str(root)):
        raise ValueError('private build-owned directory required')
    group = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    cgroup = Path('/sys/fs/cgroup')/group.lstrip('/')
    if (cgroup/'memory.max').read_text().strip() != str(768 << 20) or (cgroup/'memory.swap.max').read_text().strip() != '0' or (cgroup/'pids.max').read_text().strip() != '128':
        raise ValueError('actual archive qualification cgroup differs from fixed budgets')
    cpus=validate_cpu_affinity(os.sched_getaffinity(0))
    manifest = json.loads(Path('inputs.json').read_text())
    verify_inputs(manifest)
    Path('classes').mkdir(); Path('captures').mkdir()
    hosts=root/'qualification.hosts'
    hosts.write_text('127.0.0.1 localhost\n192.168.100.102 host.docker.internal\n')
    subprocess.run(['/usr/lib/jvm/java-21-openjdk/bin/javac', '-J-Xmx128m', '--release', '21', '-cp', 'reader.jar', '-d', 'classes', 'ArchiveHistoryInventory.java'], check=True, timeout=60)
    subprocess.run([JAVA, '-Djdk.net.hosts.file='+str(hosts), '-Xmx128m', '-cp', 'reader.jar:classes', 'ArchiveHistoryInventory', str(root/'reader.jar'), 'inventory.json'], check=True, timeout=120)
    rows = validate_inventory(json.loads(Path('inventory.json').read_text()))
    result = {'status': 'RUNNING', 'source': manifest['source'], 'sdkSource': manifest['sdkSource'],
              'helperSha256': manifest['files']['reader.jar'], 'installedBuild': manifest['installedBuild'],
              'scope': 'actual retained-history capture only; cold/mixed/transaction controls and other-service performance gates remain separate',
              'productionAccess': False, 'qualificationOnlyResolver':{'host.docker.internal':'192.168.100.102'}, 'archiveHostDevDnsReadinessClaim':False, 'groupOrOffsetWrites': False, 'heapMiB':512, 'totalCgroupMiB':768, 'swapping':False, 'cpuAffinity':cpus, 'cpuBudget':'exactly two inherited CPUs, no CPU-controller dependency', 'rows': []}
    def save(): save_result(result)
    save(); total = 0
    try:
        for index, row in enumerate(rows):
            remaining = MAX_OUTPUT-total
            if remaining < 8 << 20: raise ValueError('aggregate capture budget exhausted')
            output = root/'captures'/f'{index}.log'; summary = root/'captures'/f'{index}.summary'
            logpath = Path(f'capture-{index}.stderr')
            wall, peak = supervise(capture_command(row, output, summary, hosts), logpath, remaining)
            proof = parse_summary(summary.read_text(), row)
            count, maximum, checksum = verify_lines(output,row,proof['records'])
            size = output.stat().st_size; total += size
            if count != proof['records'] or total > MAX_OUTPUT: raise ValueError('physical record count or byte budget differs')
            proof.update(row, captureBytes=size, captureSha256=checksum, maximumLineBytes=maximum,
                         peakObservedHwmKiB=peak, completeProcessWallMillis=int(wall*1000), measuredRecordsPerSecond=count/max(.001,wall), emptyDomain=count==0)
            result['rows'].append(proof); save(); print(json.dumps({'topic':row['topic'],'partition':row['partition'],'records':count,'status':'COMPLETE'}), flush=True)
        if not any(r['records'] for r in result['rows']): raise ValueError('no actual retained source records were captured')
        per_topic={topic:sum(r['records'] for r in result['rows'] if r['topic']==topic) for topic in BINDINGS}
        if any(int(v) for k,v in (line.split() for line in (cgroup/'memory.events').read_text().splitlines()) if k=='oom_kill'):raise ValueError('qualification cgroup OOM kill observed')
        result.update(topicRecordCounts=per_topic, qualifiedNonemptyTopics=[t for t,n in per_topic.items() if n], emptyTopics=[t for t,n in per_topic.items() if not n])
        result.update(status='PASS_OBSERVED_RETAINED_HISTORY', captureBytes=total, cgroupMemoryPeakBytes=int((cgroup/'memory.peak').read_text()) if (cgroup/'memory.peak').exists() else None, memoryEvents=(cgroup/'memory.events').read_text(), measurementScope='JDK21 retained history, whole-unit hard 768MiB/no-swap limit and sampled process HWM; actual archive-host JDK26/cold/mixed/transaction controls remain separate gates'); save()
    except BaseException as error:
        result.update(status='FAILED', failure=type(error).__name__+': '+str(error)); save(); raise

if __name__ == '__main__': main()
