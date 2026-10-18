#!/usr/bin/env python3
"""Jenkins-only ES4 qualification of the installed archive JAR, without deployment or marking offsets."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tarfile
import urllib.parse
import urllib.request

BASE = 'http://localhost:8085'
HOST = 'abhinav@192.168.100.4'
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=2']
ROOT = '/home/abhinav/ci/cvd-archive-history'
COMPONENTS = ['scripts/ops/archive', 'tools/canonical-archive-reader', 'Jenkinsfile.archive-scripts-deploy', 'scripts/jenkins/archive-packaged-on-es4.py']


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=kwargs.pop('timeout', 30), **kwargs).stdout.strip()


def get(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        body = response.read((2 << 20)+1)
    if len(body)>2 << 20: raise ValueError('bounded Jenkins metadata required')
    return json.loads(body)


def main_tip(repo):
    value=run('git','ls-remote','git@github.com:abhinav-jain09/'+repo+'.git','refs/heads/main')
    match=re.fullmatch(r'([0-9a-f]{40})\s+refs/heads/main',value)
    if not match: raise ValueError('exact main branch required')
    return match[1]


def check_window(now=None):
    now=now or datetime.now(ZoneInfo('America/New_York'))
    window_start=now.replace(hour=8,minute=25,second=0,microsecond=0)
    window_end=now.replace(hour=16,minute=15,second=0,microsecond=0)
    if now.weekday()<5 and now<window_end and now+timedelta(seconds=1920)>=window_start:
        raise ValueError('source-broker qualification refused during cash-window exclusion')


def latest_installed(builds):
    if not isinstance(builds,list) or len(builds)>64: raise ValueError('bounded archive job inventory required')
    candidates=[]
    for row in builds:
        if type(row.get('number')) is not int or row['number']<=0: raise ValueError('positive build number required')
        parameters={p['name']:p.get('value') for a in row.get('actions',[]) for p in a.get('parameters',[])}
        if parameters.get('RUN_TESTS_ONLY') is False: candidates.append((row,parameters))
    if not candidates: raise ValueError('no recorded archive install')
    row,parameters=max(candidates,key=lambda pair:pair[0]['number'])
    if row.get('building') is not False or row.get('result')!='SUCCESS': raise ValueError('latest install is not a completed success; no fallback')
    for field in ['PERMITTED_SHA','CONTRACTS_PERMITTED_SHA']:
        if not re.fullmatch('[0-9a-f]{40}',parameters.get(field,'')): raise ValueError('archive source/SDK provenance missing')
    if parameters.get('PROD_HOST')!='abhinav@192.168.100.252' or parameters.get('INSTALL_DIR')!='/home/abhinav/oe-ops':
        raise ValueError('unexpected installed archive location')
    return row['number'],parameters


def package_proof(source,sdk,number,proof):
    if proof.get('source')!=source or proof.get('contractsSource')!=sdk or proof.get('codecAssertions')!=56 or proof.get('sdkMembersMatched',0)<1:
        raise ValueError('archive qualification receipt does not match installed source/SDK')
    digest=proof.get('helperSha256','')
    if not re.fullmatch('[0-9a-f]{64}',digest): raise ValueError('archive helper hash missing')
    run('git','cat-file','-e',source+'^{commit}')
    run('git','merge-base','--is-ancestor',source,'HEAD')
    if run('git','diff','--name-only',source,'HEAD','--',*COMPONENTS):
        raise ValueError('installed archive component differs from current main; roll forward before qualification')
    return digest


def main():
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD')!='PASSED': raise ValueError('guarded Jenkins execution required')
    source=os.environ.get('PERMITTED_SHA','');build=os.environ.get('BUILD_NUMBER','')
    if not re.fullmatch('[0-9a-f]{40}',source) or not re.fullmatch('[0-9]+',build): raise ValueError('source/build required')
    if run('git','rev-parse','HEAD')!=source or main_tip('options-edge-deploy')!=source: raise ValueError('current guarded main required')
    def interrupted(signum,frame):raise InterruptedError('qualification interrupted by signal '+str(signum))
    for signum in [signal.SIGTERM,signal.SIGINT,signal.SIGHUP]:signal.signal(signum,interrupted)
    check_window()
    tree='builds[number,result,building,actions[parameters[name,value]]]{0,64}'
    number,parameters=latest_installed(get(BASE+'/job/archive-scripts-deploy/api/json?'+urllib.parse.urlencode({'tree':tree}))['builds'])
    sdk=main_tip('options-edge-contracts')
    if parameters['CONTRACTS_PERMITTED_SHA']!=sdk: raise ValueError('installed helper does not match current contracts main')
    proof=get(BASE+'/job/archive-scripts-deploy/'+str(number)+'/artifact/.jenkins-tmp/archive-packaged-'+str(number)+'/receipt.json')
    expected=package_proof(parameters['PERMITTED_SHA'],sdk,number,proof)
    out=Path('.jenkins-tmp/cvd-archive-history-'+build)
    if out.parent.is_symlink(): raise ValueError('symbolic qualification root refused')
    out.parent.mkdir(mode=0o700,exist_ok=True);out.mkdir(mode=0o700)
    helper=out/'reader.jar'
    # Read only the already-installed artifact. No mutable foreign workspace, runtime build or production capture.
    installed=run(*SSH,'abhinav@192.168.100.252','test -f /home/abhinav/oe-ops/canonical-archive-reader.jar && test ! -L /home/abhinav/oe-ops/canonical-archive-reader.jar && test "$(stat -c%s /home/abhinav/oe-ops/canonical-archive-reader.jar)" -le 134217728 && timeout 15s sha256sum /home/abhinav/oe-ops/canonical-archive-reader.jar')
    if installed.split()[0]!=expected: raise ValueError('installed archive bytes drifted')
    subprocess.run(['scp','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10','abhinav@192.168.100.252:/home/abhinav/oe-ops/canonical-archive-reader.jar',str(helper)],check=True,timeout=90)
    if helper.is_symlink() or not helper.is_file() or helper.stat().st_size>128<<20 or hashlib.file_digest(helper.open('rb'),'sha256').hexdigest()!=expected:raise ValueError('downloaded installed package differs')
    inputs={'reader.jar':helper,'ArchiveHistoryInventory.java':Path('scripts/qualification/archive/ArchiveHistoryInventory.java'),'history_worker.py':Path('scripts/qualification/archive/history_worker.py')}
    metadata={'source':source,'sdkSource':sdk,'installedBuild':number,'installedSource':parameters['PERMITTED_SHA'],'files':{name:hashlib.file_digest(path.open('rb'),'sha256').hexdigest() for name,path in inputs.items()}}
    (out/'inputs.json').write_text(json.dumps(metadata,indent=2)+'\n');inputs['inputs.json']=out/'inputs.json'
    with tarfile.open(out/'inputs.tar.gz','w:gz') as bundle:
        for name,path in inputs.items():
            if path.is_symlink() or path.stat().st_size>128<<20: raise ValueError('qualification input bound')
            bundle.add(path,arcname=name,recursive=False)
    if (out/'inputs.tar.gz').stat().st_size>128<<20: raise ValueError('bundle byte budget')
    checksum=hashlib.file_digest((out/'inputs.tar.gz').open('rb'),'sha256').hexdigest()
    init=r'''import os,pathlib,tempfile,shutil,sys
if sys.version_info<(3,9):raise ValueError('Python3.9+ required for zoneinfo')
for tool in ['/usr/lib/jvm/java-21-openjdk/bin/java','/usr/lib/jvm/java-21-openjdk/bin/javac','/usr/bin/systemd-run']:
 if not os.access(tool,os.X_OK):raise ValueError('missing native qualification tool')
root=pathlib.Path('/home/abhinav/ci/cvd-archive-history');root.mkdir(mode=0o700,exist_ok=True)
if root.is_symlink() or root.stat().st_uid!=os.getuid():raise ValueError('private root identity')
files=0;size=0
for path in root.rglob('*'):
 files+=1
 if path.is_symlink():raise ValueError('private root symlink')
 if path.is_file():size+=path.stat().st_size
 if files>8192 or size>8<<30:raise ValueError('preserved qualification budget full')
if len(list(root.iterdir()))>=4 or shutil.disk_usage(root).free<3<<30:raise ValueError('private disk/run budget full')
print(tempfile.mkdtemp(prefix='run.'+sys.argv[1]+'.',dir=root))
'''
    remote=run(*SSH,HOST,'python3 - '+build,input=init)
    if not re.fullmatch(re.escape(ROOT)+r'/run\.'+build+r'\.[a-z0-9_]{8}',remote): raise ValueError('private run path')
    unit='oe-cvd-archive-'+remote.rsplit('/',1)[-1].replace('.','-')
    controller={'status':'PREPARING','source':source,'sdkSource':sdk,'installedBuild':number,'helperSha256':expected,'remote':remote,'scope':'retained-history qualification only; no archive deployment, groups, commits or production capture'}
    def save(): (out/'controller-receipt.json').write_text(json.dumps(controller,indent=2)+'\n')
    save()
    try:
        subprocess.run(['scp','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10',str(out/'inputs.tar.gz'),HOST+':'+remote+'/inputs.tar.gz'],check=True,timeout=90)
        check_window()
        command=r'''set -euo pipefail
cd "$1"
python3 - <<'WINDOW'
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
now=datetime.now(ZoneInfo('America/New_York'))
start=now.replace(hour=8,minute=25,second=0,microsecond=0)
end=now.replace(hour=16,minute=15,second=0,microsecond=0)
if now.weekday()<5 and now<end and now+timedelta(seconds=1920)>=start:raise ValueError('source host cash-window exclusion')
WINDOW
[ "$(sha256sum inputs.tar.gz | cut -d' ' -f1)" = "$2" ]
python3 - <<'EXTRACT'
import tarfile,pathlib
allowed={'reader.jar','ArchiveHistoryInventory.java','history_worker.py','inputs.json'}
with tarfile.open('inputs.tar.gz') as bundle:
 members=bundle.getmembers()
 if {m.name for m in members}!=allowed or len(members)!=4 or sum(m.size for m in members)>134217728:raise ValueError('bundle inventory/budget')
 for m in members:
  if not m.isfile() or m.size>134217728:raise ValueError('bundle member type')
  with pathlib.Path(m.name).open('xb') as out,bundle.extractfile(m) as source:
   while data:=source.read(1048576):out.write(data)
EXTRACT
systemd-run --user --quiet --wait --pipe --collect --unit="$3" --working-directory="$PWD" \
 --property=MemoryMax=768M --property=MemorySwapMax=0 --property=TasksMax=128 --property=CPUQuota=200% \
 --property=RuntimeMaxSec=1800 --property=TimeoutStopSec=10 --property=KillMode=control-group \
 env -i PATH=/usr/lib/jvm/java-21-openjdk/bin:/usr/bin:/bin PYTHONDONTWRITEBYTECODE=1 python3 history_worker.py
'''
        with (out/'history.log').open('wb') as log:
            subprocess.run(SSH+[HOST,'bash','-s','--',remote,checksum,unit],input=command.encode(),stdout=log,stderr=subprocess.STDOUT,check=True,timeout=1850)
        subprocess.run(['scp','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10',HOST+':'+remote+'/result.json',str(out/'result.json')],check=True,timeout=30)
        result=json.loads((out/'result.json').read_text())
        if result.get('status')!='PASS_OBSERVED_RETAINED_HISTORY' or result.get('helperSha256')!=expected: raise ValueError('native retained-history qualification did not pass')
        installed_after=run(*SSH,'abhinav@192.168.100.252','sha256sum /home/abhinav/oe-ops/canonical-archive-reader.jar')
        if installed_after.split()[0]!=expected: raise ValueError('archive install changed during qualification')
        controller.update(status='PASS_OBSERVED_RETAINED_HISTORY',records=sum(r['records'] for r in result['rows']),qualification=result);save();print(json.dumps({'status':controller['status'],'records':controller['records']}))
    except BaseException as error:
        controller.update(status='FAILED',failure=type(error).__name__+': '+str(error));save();raise
    finally:
        # Stop only this owned transient unit. Preserve all private inputs/captures for audit.
        try:
            stopped=subprocess.run(SSH+[HOST,'systemctl --user stop '+unit],capture_output=True,timeout=20)
            controller['cleanupStopRequested']=True
        except (OSError,subprocess.SubprocessError) as cleanup:
            controller['cleanupWarning']=str(cleanup)
        save()

if __name__=='__main__':main()
