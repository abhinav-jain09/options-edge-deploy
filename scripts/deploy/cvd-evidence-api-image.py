#!/usr/bin/env python3
"""Build the existing API from latest main, or update its existing stopped workload image."""
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys

JOB='cvd-evidence-api-images'
PROCESSING='git@github.com:abhinav-jain09/options-edge-processing.git'
REPO='options-edge-trade-evidence-api'
GUARD='2f6fc2ab7aa51eb8a9e8aa350f2d774cd2fb7a253de2591e43645a19a9886b24'

def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def command(*args,timeout=30):
    return subprocess.check_output(list(args),text=True,stderr=subprocess.PIPE,timeout=timeout)

def bounded_step(args,root,name,timeout,env=None):
    """Run one build step with a finite deadline and an 8 MiB log. The log is bounded by this reader, not by RLIMIT_FSIZE: that limit is inherited
    by the child and made pip fail with EFBIG while writing a >8 MiB wheel."""
    import threading
    log=root/(name+'.log');limit=8<<20;state={'over':False}
    with log.open('xb') as output:
        process=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
        def pump():
            written=0
            for chunk in iter(lambda:process.stdout.read(65536),b''):
                output.write(chunk[:limit-written]);written+=len(chunk)
                if written>=limit:
                    state['over']=True;process.kill();break
        reader=threading.Thread(target=pump,daemon=True);reader.start()
        try:returncode=process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill();process.wait();reader.join(10);raise
        reader.join(30)
    if state['over'] or log.stat().st_size>=limit:raise ValueError('Bounded build step log budget exceeded')
    with log.open('rb') as output:
        output.seek(max(0,log.stat().st_size-4096));tail=output.read(4096).decode(errors='replace')
    print(tail)
    if returncode:raise ValueError('Bounded build step '+name+' failed with status '+str(returncode))


def current():
    value=command('git','ls-remote',PROCESSING,'refs/heads/main')
    match=re.fullmatch(r'([0-9a-f]{40})\s+refs/heads/main\s*',value)
    if not match:raise ValueError('Exact current processing main required')
    return match[1]

def staging(build):
    parent=Path('.jenkins-tmp')
    if parent.is_symlink():raise ValueError('Symbolic staging refused')
    parent.mkdir(exist_ok=True);old=list(parent.glob('evidence-api-*'))
    if len(old)>=64:raise ValueError('Preserved API staging count full')
    count=0;size=0
    for root in old:
        if root.is_symlink() or not root.is_dir():raise ValueError('Regular API staging directory required')
        for path in root.rglob('*'):
            count+=1
            if path.is_symlink():raise ValueError('Symbolic API staging entry')
            if path.is_file():size+=path.stat().st_size
            if count>131072 or size>3<<30:raise ValueError('Preserved API staging budget full')
    root=parent/('evidence-api-'+build);root.mkdir();return root

def prepare_source(root,source):
    checkout=root/'processing'
    command('git','clone','--no-checkout','--depth=1','--single-branch','--branch','main',PROCESSING,str(checkout),timeout=90)
    if command('git','-C',str(checkout),'rev-parse','HEAD').strip()!=source:raise ValueError('Processing main moved during source capture')
    command('git','-C',str(checkout),'sparse-checkout','set','--no-cone','/trade-evidence-api/',timeout=30)
    command('git','-C',str(checkout),'checkout','main',timeout=30)
    if any(line.startswith('120000 ') for line in command('git','-C',str(checkout),'ls-tree','-r','HEAD','--','trade-evidence-api').splitlines()):raise ValueError('Symbolic API build inputs refused')
    command('git','-C',str(checkout),'diff','--exit-code','HEAD','--',timeout=15)
    if command('git','-C',str(checkout),'status','--porcelain','--untracked-files=all',timeout=15).strip():raise ValueError('Fresh API source is dirty')
    return checkout


def tree_digest(path):
    """Content digest of a staged tree (relative names, modes and bytes): the tested sources must be exactly what is published."""
    import hashlib
    h=hashlib.sha256()
    for item in sorted(Path(path).rglob('*')):
        if item.is_symlink():raise ValueError('Symbolic staged entry refused')
        if item.is_file():
            h.update(item.relative_to(path).as_posix().encode()+b'\0'+oct(item.stat().st_mode&0o777).encode()+b'\0')
            with item.open('rb') as stream:
                for block in iter(lambda:stream.read(1<<20),b''):h.update(block)
    return h.hexdigest()


def tool_environment():
    return {**os.environ,'PATH':'/opt/homebrew/bin:/usr/local/bin:'+os.environ.get('PATH','/usr/bin:/bin'),'PYTHONDONTWRITEBYTECODE':'1'}


def build_image(environment,build,reader):
    target=reader.TARGETS[environment];root=staging(build);source=current()
    receipt={'status':'PREPARING','deploymentSource':os.environ['PERMITTED_SHA'],'processingSource':source,'environment':environment,'build':int(build),'runtimeDeployed':False}
    def save():(root/'build-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    save()
    try:
        checkout=prepare_source(root,source)
        sdk=load('api_sdk',Path(__file__).parents[1]/'jenkins/fetch-canonical-contracts.py')
        contracts=root/'contracts';contracts.mkdir();sdk.latest(os.environ['JENKINS_URL'],root/'contracts-source',contracts/'contracts.jar')
        release=json.loads((contracts/'contracts.release.json').read_text())
        if release['source']!=reader.sdk_main():raise ValueError('SDK main changed during source capture')
        tag=('dev' if environment=='dev' else 'prod')+'-'+build+'-'+source[:12]
        push_registry='192.168.100.102:5001' if environment=='dev' else '192.168.100.252:5000'
        image=push_registry+'/'+REPO+':'+tag
        # Docker-free: the suite runs on the host from the same hash-pinned lock and the very sources that are staged next; Jib pushes.
        helper=checkout/'trade-evidence-api/deploy/image_stage.py'
        pom=checkout/'trade-evidence-api/ci/jib-image/pom.xml'
        if not helper.is_file() or not pom.is_file():raise ValueError('Same-source Docker-free image recipe required')
        env=tool_environment()
        # The venv lives OUTSIDE the workspace tree: python3.14 names a venv launcher with a non-ASCII filename, which the permitted-tree verifier
        # cannot declare, so a venv left in the workspace would refuse every later build.
        import shutil,tempfile
        venv=tempfile.mkdtemp(prefix='cvd-api-test-venv-')
        try:bounded_step(['python3.14','-B',str(helper),'test','--venv',str(Path(venv)/'venv'),'--contracts',str(contracts)],root,'tests',600,env=env)
        finally:shutil.rmtree(venv,ignore_errors=True)
        receipt['status']='TESTS_PASSED';save()
        stage=root/'image';rootfs=stage/'rootfs'
        bounded_step(['python3.14','-B',str(helper),'stage','--dest',str(rootfs),'--platform',target['platform'],'--contracts',str(contracts)],root,'stage',900,env=env)
        (stage/'target/classes').mkdir(parents=True);(stage/'pom.xml').write_bytes(pom.read_bytes())
        tested_tree=tree_digest(rootfs)
        if current()!=source or reader.sdk_main()!=release['source']:raise ValueError('Source main changed before publication')
        container_env=','.join(['PYTHONUNBUFFERED=1','PYTHONDONTWRITEBYTECODE=1','TRADE_API_RUNTIME_DIR=/data','TRADE_API_AUTH_REQUIRED=true',
            'PYTHONPATH=/app:/app/site-packages:/app/contracts.jar','OPTIONS_EDGE_CONTRACTS_JAR=/app/contracts.jar',
            'OPTIONS_EDGE_CONTRACTS_VERSION='+release['version'],'OPTIONS_EDGE_CONTRACTS_SHA256='+release['sha256'],'TRADE_API_REVISION='+source])
        bounded_step(['mvn','-B','-q','-f',str(stage/'pom.xml'),'package','-Pjib-image','-Djib.extraDirectories.paths='+str(rootfs.resolve()),
            '-Djib.from.platforms='+target['platform'],'-Djib.to.image='+image,'-Djib.container.environment='+container_env,
            '-Djib.container.labels=org.opencontainers.image.source=https://github.com/abhinav-jain09/options-edge-processing,org.opencontainers.image.revision='+source],
            root,'publish',900,env=env)
        digest=(stage/'target/jib-image.digest').read_text().strip();pushed_config=(stage/'target/jib-image.id').read_text().strip()
        if tree_digest(rootfs)!=tested_tree:raise ValueError('Published image differs from the tested image')
        if not re.fullmatch('sha256:[0-9a-f]{64}',digest) or not re.fullmatch('sha256:[0-9a-f]{64}',pushed_config):raise ValueError('Jib reported no exact image digest')
        tested=pushed_config
        receipt.update(status='PASS',format=1,platform=target['platform'],image=target['registry']+'/'+REPO+':'+tag+'@'+digest,
                       contractsSource=release['source'],contractsJarSha256=release['sha256'],testedConfigDigest=tested,guardVersion=GUARD)
        save();(root/'release.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
    except Exception as error:receipt['failureType']=type(error).__name__;save();raise


def select_release(environment,rows,read,reader):
    candidates=[]
    if not isinstance(rows,list) or len(rows)>64:raise ValueError('Bounded API build inventory required')
    for row in rows:
        params={p['name']:p.get('value') for a in row.get('actions',[]) for p in a.get('parameters',[])}
        if params.get('ACTION')=='build' and params.get('ENVIRONMENT')==environment:
            if type(row.get('number')) is not int or row['number']<=0:raise ValueError('Positive API build required')
            candidates.append((row,params))
    if not candidates:raise ValueError('No API image build for selected environment')
    row,params=max(candidates,key=lambda item:item[0]['number'])
    if row.get('building') is not False or row.get('result')!='SUCCESS':raise ValueError('Latest API image build is not SUCCESS; never fall back')
    base=os.environ['JENKINS_URL'].rstrip('/')+'/job/'+JOB+'/'+str(row['number'])+'/artifact/.jenkins-tmp/evidence-api-'+str(row['number'])+'/release.json'
    proof=json.loads(read(base,65536));source=proof.get('processingSource','');target=reader.TARGETS[environment]
    for value in [source,proof.get('contractsSource',''),proof.get('deploymentSource','')]:
        if not re.fullmatch('[0-9a-f]{40}',value):raise ValueError('Exact API release source provenance required')
    if proof.get('format')!=1 or proof.get('status')!='PASS' or proof.get('runtimeDeployed') is not False or proof.get('build')!=row['number'] or proof.get('deploymentSource')!=params.get('PERMITTED_SHA') or proof.get('guardVersion')!=GUARD:
        raise ValueError('API release build/guard provenance differs')
    if proof.get('environment')!=environment or proof.get('platform')!=target['platform'] or proof.get('contractsSource')!=reader.sdk_main():raise ValueError('API release target or current SDK differs')
    if not re.fullmatch('[0-9a-f]{64}',proof.get('contractsJarSha256','')) or not re.fullmatch('sha256:[0-9a-f]{64}',proof.get('testedConfigDigest','')):raise ValueError('API byte/config digest provenance missing')
    prefix='dev' if environment=='dev' else 'prod';expected=target['registry']+'/'+REPO+':'+prefix+'-'+str(row['number'])+'-'+source[:12]+'@sha256:'
    image=proof.get('image','')
    if not image.startswith(expected) or not re.fullmatch('[0-9a-f]{64}',image[len(expected):]):raise ValueError('API image is not bound to this release')
    return image,{'processingBuild':row['number'],'processingSource':source,'contractsSource':proof['contractsSource'],'contractsJarSha256':proof['contractsJarSha256'],'evidenceApiBuildSource':proof['deploymentSource']}


def roll_image(environment,reader):
    core=load('api_image_core',Path(__file__).parents[1]/'es4/pinned-image-rollforward.py')
    reader.configure(core,environment,'context-tape');core.SERVICES={'trade-evidence-api':('trade-evidence-api','api',REPO)}
    read=core.read_url
    def api_read(url,limit):return read(url.replace('/job/options-edge-processing/','/job/'+JOB+'/'),limit)
    core.read_url=api_read
    core.qualified_image=lambda service,rows,fetch:select_release(environment,rows,read,reader)
    patch=core.patch_for
    def stopped_patch(service,live,image):
        if live.get('spec',{}).get('replicas')!=0:raise ValueError('API image updater requires the existing stopped hold')
        return patch(service,live,image)
    core.patch_for=stopped_patch;os.environ['SERVICE']='trade-evidence-api';core.main()


def main():
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD')!='PASSED':raise ValueError('Guarded Jenkins required')
    if sys.platform!='darwin' or not re.search(r'\binet 192\.168\.100\.(102|201)\b',command('/sbin/ifconfig','-a')):raise ValueError('Primary build Mac only (the Mac running the local Jenkins agent: .102 or .201)')
    if not re.fullmatch('[0-9a-f]{40}',os.environ.get('PERMITTED_SHA','')):raise ValueError('Exact deploy main required')
    environment=os.environ['ENVIRONMENT'];build=os.environ['BUILD_NUMBER'];action=os.environ['ACTION']
    if environment not in ['dev','production'] or not re.fullmatch('[1-9][0-9]{0,9}',build) or action not in ['build','roll-forward']:raise ValueError('Derived build/environment/action required')
    reader=load('api_reader_targets',Path(__file__).with_name('cvd-image-rollforward.py'))
    if action=='build':build_image(environment,build,reader)
    else:roll_image(environment,reader)

if __name__=='__main__':main()
