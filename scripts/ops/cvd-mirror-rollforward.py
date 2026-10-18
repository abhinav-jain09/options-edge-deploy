#!/usr/bin/env python3
"""Replace one existing primary-Mac mirror with the latest qualified registry-aware JAR."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import plistlib
import re
import shlex
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import zipfile

OWNER=Path('/Users/abhinav');JAVA=Path('/opt/homebrew/opt/openjdk@21/bin/java')
TOPICS={'es.futures.cvd':'cvd/CvdSnapshot.avsc','es.futures.cvd.bars':'cvd/CvdBarEnvelope.avsc',
'es.futures.cvd.levels':'cvd/CvdLevelsSnapshot.avsc','es.futures.footprint':'cvd/FootprintSnapshot.avsc',
'es.futures.footprint.bars':'cvd/FootprintBarEnvelope.avsc','es.futures.footprint.outcomes':'cvd/FootprintOutcome.avsc',
'es.futures.footprint.evidence':'cvd/FootprintEvidenceSnapshot.avsc','es.futures.footprint.strike':'cvd-related/FootprintStrikeEnvelope.avsc'}
TARGETS={('127.0.0.1','19092'):('dev','http://127.0.0.1:8082'),('192.168.100.252','9092'):('prod','http://192.168.100.252:8082')}
MAIN='com.optionsedge.processing.mirror.CanonicalTopicMirror'


def run(*args,timeout=20,**kwargs):
    return subprocess.check_output(list(args),timeout=timeout,text=True,stderr=subprocess.PIPE,**kwargs)


def diagnostic(error):
    detail=getattr(error,'stderr',None) or str(error)
    if isinstance(detail,bytes):detail=detail.decode(errors='replace')
    return type(error).__name__+': '+detail[-2048:]


def bounded(path,maximum):
    if path.is_symlink() or not path.is_file() or path.stat().st_size>maximum:raise ValueError('Regular bounded file required')
    return path.read_bytes()


def digest(path,maximum=128<<20):return hashlib.sha256(bounded(path,maximum)).hexdigest()


def read(url,maximum=2<<20):
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url,timeout=20) as response:value=response.read(maximum+1)
    if len(value)>maximum:raise ValueError('Artifact/metadata byte bound exceeded')
    return value


def main_ref(repository):
    text=run('git','ls-remote','git@github.com:abhinav-jain09/'+repository+'.git','refs/heads/main',timeout=30)
    match=re.fullmatch(r'([0-9a-f]{40})\s+refs/heads/main\s*',text)
    if not match:raise ValueError('One exact current main branch required')
    return match[1]


def latest_build(rows):
    if not isinstance(rows,list) or len(rows)>64:raise ValueError('Bounded build inventory required')
    found=[]
    for row in rows:
        params={p['name']:p.get('value') for a in row.get('actions',[]) for p in a.get('parameters',[])}
        if params.get('BUILD_TARGET')=='cvd-avro-related' and params.get('ENVIRONMENT')=='production':
            if type(row.get('number')) is not int or row['number']<=0:raise ValueError('Positive build identity required')
            found.append((row,params))
    if not found:raise ValueError('No full related qualification in recent builds')
    row,params=max(found,key=lambda pair:pair[0]['number'])
    if row.get('building') is not False or row.get('result')!='SUCCESS' or params.get('PUSH_IMAGES') is not True:
        raise ValueError('Latest full related qualification is not successfully published; never fall back')
    return row['number'],params


def release(root):
    module=Path(__file__).parents[1]/'jenkins/fetch-canonical-contracts.py'
    spec=importlib.util.spec_from_file_location('mirror_sdk_release',module);sdk=importlib.util.module_from_spec(spec);spec.loader.exec_module(sdk)
    sdk.latest(os.environ['JENKINS_URL'],root/'contracts-source',root/'contracts.jar')
    sdk_proof=json.loads((root/'contracts.release.json').read_text())
    base=os.environ['JENKINS_URL'].rstrip('/')+'/job/options-edge-processing/'
    query='api/json?'+urllib.parse.urlencode({'tree':'builds[number,result,building,actions[parameters[name,value]]]{0,64}'})
    number,params=latest_build(json.loads(read(base+query))['builds'])
    if params.get('CONTRACTS_PERMITTED_SHA')!=sdk_proof['source']:
        raise ValueError('Latest full qualification is not current SDK main')
    current=main_ref('options-edge-processing');source_proof=None
    if params.get('PERMITTED_SHA')!=current:
        source_spec=importlib.util.spec_from_file_location('mirror_source_proof',Path(__file__).parents[1]/'jenkins/cvd-component-source-proof.py')
        source_module=importlib.util.module_from_spec(source_spec);source_spec.loader.exec_module(source_module)
        source_proof=source_module.prove(params['PERMITTED_SHA'],current)
    prefix=base+str(number)+'/artifact/.jenkins-tmp/'
    raw=read(prefix+'mirror-release-'+str(number)+'/release.json');proof=json.loads(raw)
    receipt=read(prefix+'es4-tests-'+str(number)+'/receipt.json',128<<10)
    if proof.get('source')!=params['PERMITTED_SHA'] or proof.get('contracts_source')!=sdk_proof['source'] or proof.get('build_number')!=str(number):
        raise ValueError('Mirror release source/build differs from successful qualification')
    if proof.get('main_class')!=MAIN or proof.get('runtime_deployed') is not False or proof.get('qualification_receipt_sha256')!=hashlib.sha256(receipt).hexdigest():
        raise ValueError('Mirror qualification receipt/entrypoint differs')
    claim=json.loads(receipt)
    if claim.get('status')!='PASS' or claim.get('source')!=proof['source'] or claim.get('contracts_source')!=proof['contracts_source'] or claim.get('contracts_jar_sha256')!=proof.get('contracts_jar_sha256'):
        raise ValueError('Mirror qualification provenance missing')
    package=read(prefix+'mirror-release-'+str(number)+'/canonical-cvd-mirror.jar',128<<20)
    if hashlib.sha256(package).hexdigest()!=proof.get('jar_sha256'):raise ValueError('Published mirror bytes changed')
    with zipfile.ZipFile(io.BytesIO(package)) as jar,zipfile.ZipFile(root/'contracts.jar') as official:
        entries=jar.infolist()
        if len(entries)>65536 or len({e.filename for e in entries})!=len(entries) or sum(e.file_size for e in entries)>512<<20:
            raise ValueError('Mirror ZIP directory/expansion bound exceeded')
        manifest=jar.read('META-INF/MANIFEST.MF').decode().replace('\r\n','\n').replace('\n ','').split('\n\n',1)[0]
        if [line.split(':',1)[1].strip() for line in manifest.splitlines() if line.split(':',1)[0].lower()=='main-class']!=[MAIN]:
            raise ValueError('Wrong mirror entrypoint')
        checked=0
        for entry in official.infolist():
            name=entry.filename
            if entry.is_dir() or name in ['META-INF/MANIFEST.MF','META-INF/INDEX.LIST'] or name.startswith('META-INF/maven/'):continue
            if entry.file_size>16<<20 or jar.getinfo(name).file_size>16<<20 or official.read(name)!=jar.read(name):raise ValueError('Mirror differs from released SDK member')
            checked+=1
        if checked<=0 or checked!=proof.get('sdk_members_verified'):raise ValueError('Released SDK population differs')
    (root/'canonical-cvd-mirror.jar').write_bytes(package);(root/'release.json').write_bytes(raw)
    if source_proof is not None:proof['componentSourceProof']=source_proof
    return proof


def properties(path):
    result={}
    for line in bounded(path,65536).decode().splitlines():
        line=line.strip()
        if not line or line.startswith(('#','!')):continue
        if '=' not in line or '\\' in line:raise ValueError('Unsupported existing Java properties spelling')
        key,value=line.split('=',1)
        if not key.strip() or key.strip() in result:raise ValueError('Duplicate property refused')
        result[key.strip()]=value.strip()
    return result


def offsets(text,group,topic):
    result={}
    for line in text.splitlines():
        fields=line.split()
        if len(fields)>=6 and fields[:2]==[group,topic]:
            if not all(re.fullmatch('[0-9]+',fields[i]) for i in [2,3,4,5]):raise ValueError('Existing committed mirror offsets required')
            part,current,end,lag=map(int,fields[2:6])
            if part in result or current>end or end-current!=lag:raise ValueError('Invalid committed mirror coordinates')
            result[part]=current
    if not result:raise ValueError('No existing committed source offsets; never bootstrap/reset a group')
    return result


def unit_state(label):
    p=subprocess.run(['launchctl','print','gui/'+str(os.getuid())+'/'+label],text=True,capture_output=True,timeout=10)
    if p.returncode:
        if ('Could not find service "'+label+'" in domain') in p.stderr:return {'loaded':False,'pid':None}
        raise ValueError('Unable to inspect existing launchd domain')
    match=re.search(r'^\s*pid = ([1-9][0-9]*)\s*$',p.stdout,re.M)
    return {'loaded':True,'pid':int(match[1]) if match else None}


def preflight(topic,target,port):
    if topic not in TOPICS or (target,port) not in TARGETS:raise ValueError('Exact existing source/target/topic required')
    slug=re.sub('[^A-Za-z0-9-]','-',target+'-'+port+'-'+topic);label='com.optionsedge.es-cvd-mirror-'+slug
    directory=OWNER/('oe-ops/es-cvd-mirror-'+slug);plist=OWNER/('Library/LaunchAgents/'+label+'.plist')
    raw=bounded(plist,65536);definition=plistlib.loads(raw);args=definition.get('ProgramArguments')
    old_runner=directory/'run-mirror.sh'
    if args not in [[str(old_runner)],['/bin/bash',str(old_runner)]]:
        if not isinstance(args,list) or len(args)!=2 or args[0]!='/bin/bash':raise ValueError('Unmanaged mirror launcher')
        previous=Path(args[1]);base=OWNER/'oe-ops/cvd-mirror-releases'
        if previous.is_symlink() or previous.name!='run-mirror.sh' or previous.parent.parent!=base:raise ValueError('Unmanaged mirror launcher')
        process_path=str(previous.parent)
        marker=json.loads(bounded(previous.parent/'managed.json',65536))
        if marker.get('label')!=label or marker.get('runnerSha256')!=digest(previous):raise ValueError('Managed mirror launcher drift')
    else:
        process_path=str(directory)
        if b'kafka-mirror-maker' not in bounded(old_runner,65536):raise ValueError('Original legacy mirror launcher required')
    if set(definition.get('EnvironmentVariables',{}))-{'PATH'}:raise ValueError('Unexpected existing mirror environment requires reconciliation')
    if definition.get('Label')!=label or definition.get('KeepAlive') is not True:raise ValueError('Existing KeepAlive mirror required')
    cp=properties(directory/'consumer.properties');pp=properties(directory/'producer.properties')
    group='es-cvd-mirror-'+target+'-'+topic
    if cp.get('bootstrap.servers')!='192.168.100.4:9092' or cp.get('group.id')!=group or pp.get('bootstrap.servers')!=target+':'+port:
        raise ValueError('Existing source/target/group identity differs')
    state=unit_state(label)
    if state['loaded'] and not state['pid']:raise ValueError('Loaded mirror without PID requires reconciliation')
    if state['pid'] and process_path not in run('ps','-ww','-p',str(state['pid']),'-o','command=',timeout=5):raise ValueError('Existing mirror process identity differs')
    return {'state':state,'label':label,'plist':plist,'raw':raw,'definition':definition,'directory':directory,'group':group,'pid':state['pid'],
            'consumerSha256':digest(directory/'consumer.properties'),'producerSha256':digest(directory/'producer.properties')}


def committed_offsets(group,topic):
    env={**os.environ,'KAFKA_HEAP_OPTS':'-Xms32m -Xmx128m','JAVA_HOME':'/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home'}
    cli=OWNER/'development/confluent-7.3.1/bin/kafka-consumer-groups'
    return offsets(run(str(cli),'--bootstrap-server','192.168.100.4:9092','--describe','--group',group,timeout=35,env=env),group,topic)

def verify_inputs(root,topic,target,port,group):
    sys.path.insert(0,str((root/'contracts.jar').resolve()))
    spec=importlib.util.spec_from_file_location('mirror_schemas',Path(__file__).parents[1]/'kafka/register-cvd-schemas.py');schemas=importlib.util.module_from_spec(spec);spec.loader.exec_module(schemas)
    with zipfile.ZipFile(root/'contracts.jar') as jar:text=jar.read(TOPICS[topic]).decode()
    for url in ['http://192.168.100.4:8081',TARGETS[(target,port)][1]]:
        actual=json.loads(read(url+'/subjects/'+urllib.parse.quote(topic+'-value',safe='')+'/versions/latest'))
        schemas.verify_schema(actual,text)
    coordinates=committed_offsets(group,topic)
    for bootstrap in ['192.168.100.4:9092',target+':'+port]:
        metadata=json.loads(run('kcat','-L','-J','-b',bootstrap,'-t',topic,'-m','8',timeout=12));rows=[r for r in metadata['topics'] if r['topic']==topic]
        if len(rows)!=1 or rows[0].get('error') or {r['partition'] for r in rows[0]['partitions']}!=set(coordinates):raise ValueError('Matching existing source/target partition set required')
    return coordinates

def linux_probe(root,topic,registry):
    """Run the actual packaged decoder against bounded retained tails on CI es4 only."""
    if registry=='http://127.0.0.1:8082':registry='http://192.168.100.102:8082'
    build=os.environ['BUILD_NUMBER'];ssh=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','abhinav@192.168.100.4']
    code="""from pathlib import Path
import os,json
base=Path.home()/'ci/cvd-mirror-preflight'
if base.is_symlink():raise ValueError('symbolic staging')
base.mkdir(parents=True,exist_ok=True)
count=0;size=0
for path in base.rglob('*'):
 count+=1
 if path.is_symlink():raise ValueError('symbolic staging entry')
 if path.is_file():size+=path.stat().st_size
 if count>8192 or size>2<<30:raise ValueError('preserved staging full')
work=base/('run-'+BUILD)
work.mkdir(mode=0o700)
print(json.dumps({'directory':str(work),'uid':os.getuid(),'gid':os.getgid()}))
""".replace('BUILD',repr(build))
    remote=json.loads(run(*ssh,'python3 -c '+shlex.quote(code),timeout=20));directory=remote['directory']
    if directory!='/home/abhinav/ci/cvd-mirror-preflight/run-'+build or type(remote['uid']) is not int or type(remote['gid']) is not int:raise ValueError('Linux CI staging identity differs')
    source=Path(__file__).with_name('CvdMirrorPreflight.java');jar=root/'canonical-cvd-mirror.jar'
    expected={jar.name:digest(jar),source.name:digest(source,65536)}
    run('scp','-q','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes',str(jar),str(source),'abhinav@192.168.100.4:'+directory+'/',timeout=60)
    target_id=json.loads(read(registry+'/subjects/'+urllib.parse.quote(topic+'-value',safe='')+'/versions/latest'))['id']
    check="""import hashlib,pathlib,subprocess
root=pathlib.Path(DIRECTORY)
for name,expected in EXPECTED.items():
 path=root/name
 if path.is_symlink() or not path.is_file() or path.stat().st_size>128<<20:raise ValueError('artifact bound')
 hasher=hashlib.sha256()
 with path.open('rb') as source:
  while True:
   chunk=source.read(1<<20)
   if not chunk:break
   hasher.update(chunk)
 actual=hasher.hexdigest()
 if actual!=expected:raise ValueError('transported qualification bytes differ')
subprocess.run(JAVA,check=True,timeout=65)
""".replace('DIRECTORY',repr(directory)).replace('EXPECTED',repr(expected))
    command=['java','-Xmx256m','-XX:MaxDirectMemorySize=64m','--class-path',directory+'/canonical-cvd-mirror.jar','--source','21',directory+'/CvdMirrorPreflight.java',
        '192.168.100.4:9092','http://192.168.100.4:8081',registry,topic,str(target_id)]
    if type(target_id) is not int or target_id<=0:raise ValueError('Positive existing destination schema ID required')
    check=check.replace('JAVA',repr(command))
    args=['sudo','systemd-run','--quiet','--wait','--pipe','--collect','--unit=oe-cvd-mirror-preflight-'+build,'--uid='+str(remote['uid']),'--gid='+str(remote['gid']),
        '-p','MemoryMax=512M','-p','MemorySwapMax=0','-p','CPUQuota=100%','-p','TasksMax=128','-p','RuntimeMaxSec=90','python3','-c',check]
    try:result=run(*ssh,' '.join(shlex.quote(value) for value in args),timeout=100)
    except (OSError,subprocess.SubprocessError) as error:raise ValueError('Linux retained-tail preflight refused: '+diagnostic(error)) from error
    matches=re.findall(r'^CANONICAL_MIRROR_PREFLIGHT_PASS records=([0-9]+) bytes=([0-9]+)$',result,re.M)
    if len(matches)!=1:raise ValueError('Actual Linux retained-tail qualification missing')
    return {'host':'Linux CI es4','records':int(matches[0][0]),'bytes':int(matches[0][1]),'sourceSha256':expected[source.name],
        'scope':'actual released decoder and destination registry serialization; manual bounded tail only, no group/commits/Kafka writes; zero sampled records is not historical coverage'}


def main():
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD')!='PASSED':raise ValueError('Guarded Jenkins required')
    if sys.platform!='darwin' or Path.home()!=OWNER or not re.search(r'\binet 192\.168\.100\.102\b',run('/sbin/ifconfig','-a')):
        raise ValueError('Primary Mac only; secondary hosts are out of scope')
    if os.environ.get('SOURCE_ES_CLUSTER_IP')!='192.168.100.4' or os.environ.get('SOURCE_PORT')!='9092':raise ValueError('Existing es4 source only')
    if not re.fullmatch('[0-9a-f]{40}',os.environ.get('PERMITTED_SHA','')):raise ValueError('Exact deploy main provenance required')
    topic=os.environ['TOPIC'];target=os.environ['TARGET_SPX_CLUSTER_IP'];port=os.environ['TARGET_PORT'];dry=os.environ.get('DEPLOY_DRY_RUN')
    if dry not in ['true','false']:raise ValueError('Explicit dry-run state required')
    build=os.environ['BUILD_NUMBER']
    if not re.fullmatch('[1-9][0-9]{0,9}',build):raise ValueError('Derived Jenkins build required')
    before=preflight(topic,target,port)
    if dry=='false' and os.environ.get('CVD_TARGET_READERS_READY')!='true':raise ValueError('All active destination readers must be compatible before mirror output becomes Avro')
    root=Path('.jenkins-tmp')/('cvd-mirror-'+build)
    if root.parent.is_symlink():raise ValueError('Symbolic staging refused')
    root.parent.mkdir(exist_ok=True)
    staged=0;files=0;old=list(root.parent.glob('cvd-mirror-*'))
    if len(old)>=128:raise ValueError('Preserved mirror staging count full')
    for directory in old:
        if directory.is_symlink():raise ValueError('Symbolic mirror staging refused')
        for path in directory.rglob('*'):
            files+=1
            if path.is_symlink():raise ValueError('Symbolic mirror staging entry refused')
            if path.is_file():staged+=path.stat().st_size
            if files>131072 or staged>4<<30:raise ValueError('Preserved mirror staging byte/file budget full')
    root.mkdir()
    proof={'source':os.environ['PERMITTED_SHA'],'topic':topic,'group':before['group'],'label':before['label'],'dryRun':dry=='true','phase':'preparing','installed':False,'runtimeReady':False,'heldStopped':before['pid'] is None}
    def save():(root/'receipt.json').write_text(json.dumps(proof,indent=2)+'\n')
    save()
    try:
        qualified=release(root);proof['release']=qualified
        coordinates=verify_inputs(root,topic,target,port,before['group'])
        proof['retainedTail']=linux_probe(root,topic,TARGETS[(target,port)][1])
        if dry=='false' and proof['retainedTail']['records']<=0:raise ValueError('Nonempty actual retained-tail qualification required before launcher upgrade')
        proof.update(phase='preflight-verified',committedOffsets=coordinates);save()
        if dry=='true':return
        base=OWNER/'oe-ops/cvd-mirror-releases'
        if base.is_symlink():raise ValueError('Symbolic release directory refused')
        base.mkdir(exist_ok=True)
        count=0;total=0;inodes=set()
        for path in base.rglob('*'):
            count+=1
            if path.is_symlink():raise ValueError('Symbolic mirror release entry refused')
            if path.is_file():
                info=path.stat();identity=(info.st_dev,info.st_ino)
                if identity not in inodes:total+=info.st_size;inodes.add(identity)
            if count>8192 or total>1024<<20:raise ValueError('Preserved mirror quarantine full')
        if len(list(base.iterdir()))>=256 or shutil.disk_usage(base).free<512<<20:raise ValueError('Preserved mirror release/disk budget exceeded')
        destination=base/('build-'+build);destination.mkdir(mode=0o700)
        cache=base/'artifacts';cache.mkdir(exist_ok=True)
        cached=cache/(qualified['jar_sha256']+'.jar')
        if not cached.exists():
            pending=cached.with_name(cached.name+'.'+build+'.pending')
            with pending.open('xb') as file:file.write(bounded(root/'canonical-cvd-mirror.jar',128<<20))
            pending.chmod(0o400)
            if digest(pending)!=qualified['jar_sha256']:raise ValueError('Prepared cached mirror differs')
            os.replace(pending,cached)
        if digest(cached)!=qualified['jar_sha256']:raise ValueError('Cached mirror bytes changed')
        os.link(cached,destination/'canonical-cvd-mirror.jar');shutil.copyfile(root/'release.json',destination/'release.json')
        if digest(destination/'canonical-cvd-mirror.jar')!=qualified['jar_sha256']:raise ValueError('Prepared mirror bytes changed')
        for name,key in [('consumer.properties','consumerSha256'),('producer.properties','producerSha256')]:
            shutil.copyfile(before['directory']/name,destination/name)
            if digest(destination/name)!=before[key]:raise ValueError('Prepared mirror properties changed')
        probe=destination/'CvdMirrorReadiness.java';shutil.copyfile(Path(__file__).with_name('CvdMirrorReadiness.java'),probe)
        profile,registry=TARGETS[(target,port)]
        exports={'SOURCE_APP_PROFILE':'prod','SOURCE_KAFKA_BOOTSTRAP_SERVERS':'192.168.100.4:9092','SOURCE_SCHEMA_REGISTRY_URL':'http://192.168.100.4:8081',
            'TARGET_APP_PROFILE':profile,'TARGET_KAFKA_BOOTSTRAP_SERVERS':target+':'+port,'TARGET_SCHEMA_REGISTRY_URL':registry}
        script='#!/bin/bash\nset -euo pipefail\n'+''.join('export '+k+'='+shlex.quote(v)+'\n' for k,v in exports.items())
        script+='exec '+shlex.quote(str(JAVA))+' -Xms32m -Xmx256m -XX:MaxDirectMemorySize=64m -jar '+shlex.quote(str(destination/'canonical-cvd-mirror.jar'))+' '+shlex.quote(str(destination/'consumer.properties'))+' '+shlex.quote(str(destination/'producer.properties'))+' '+shlex.quote(topic)+'\n'
        runner=destination/'run-mirror.sh';runner.write_text(script);runner.chmod(0o700)
        (destination/'managed.json').write_text(json.dumps({'label':before['label'],'runnerSha256':digest(runner)})+'\n')
        smoke=subprocess.run([str(JAVA),'-Xmx256m','-jar',str(destination/'canonical-cvd-mirror.jar')],capture_output=True,text=True,timeout=15)
        if smoke.returncode==0 or 'Usage: CanonicalTopicMirror' not in smoke.stderr:raise ValueError('Prepared JAR/JDK smoke failed before stopping old mirror')
        if bounded(before['plist'],65536)!=before['raw'] or unit_state(before['label'])!=before['state'] or digest(before['directory']/'consumer.properties')!=before['consumerSha256'] or digest(before['directory']/'producer.properties')!=before['producerSha256']:
            raise ValueError('Existing unit changed during preparation')
        proof['phase']='prepared';save()
        if before['pid']:
            run('launchctl','bootout','gui/'+str(os.getuid()),str(before['plist']));end=time.monotonic()+15
            while time.monotonic()<end:
                try:os.kill(before['pid'],0)
                except ProcessLookupError:break
                time.sleep(.2)
            else:raise TimeoutError('Old mirror did not stop; no second process started')
        definition=dict(before['definition']);definition['ProgramArguments']=['/bin/bash',str(runner)]
        (destination/'previous.plist').write_bytes(before['raw'])
        temp=before['plist'].with_name(before['plist'].name+'.'+build+'.new')
        with temp.open('xb') as file:file.write(plistlib.dumps(definition))
        temp.chmod(0o600);os.replace(temp,before['plist']);proof['phase']='launcher-updated';save()
        if before['pid']:
            run('launchctl','bootstrap','gui/'+str(os.getuid()),str(before['plist']));end=time.monotonic()+60;last_readiness='no new PID'
            while time.monotonic()<end:
                current=unit_state(before['label'])['pid']
                if current and current!=before['pid']:
                    try:command=run('ps','-ww','-p',str(current),'-o','command=',timeout=5)
                    except (OSError,subprocess.SubprocessError) as error:last_readiness=diagnostic(error);time.sleep(.5);continue
                    if str(destination/'canonical-cvd-mirror.jar') not in command:raise ValueError('Unexpected new process')
                    try:
                        status=run(str(JAVA),'-Xmx64m','--add-modules','jdk.attach',str(probe),str(current),topic,before['group'],timeout=12)
                        if status.startswith('CANONICAL_MIRROR_READY '):break
                    except (OSError,ValueError,subprocess.SubprocessError) as error:
                        last_readiness=diagnostic(error);proof['readinessLastFailure']=last_readiness;save()
                time.sleep(.5)
            else:raise TimeoutError('Canonical mirror process/readiness verification failed; last='+last_readiness)
            after=committed_offsets(before['group'],topic)
            if set(after)!=set(coordinates) or any(after[part]<offset for part,offset in coordinates.items()):raise ValueError('Committed mirror offsets decreased after handoff')
            proof.update(newPid=current,runtimeReady=True,committedOffsetsAfter=after)
        if plistlib.loads(bounded(before['plist'],65536))!=definition:raise ValueError('Non-launcher plist drift')
        if not before['state']['loaded'] and unit_state(before['label'])['loaded']:raise ValueError('Stopped mirror hold changed')
        if digest(before['directory']/'consumer.properties')!=before['consumerSha256'] or digest(before['directory']/'producer.properties')!=before['producerSha256']:raise ValueError('Original properties changed')
        proof.update(phase='verified',installed=True);save()
    except Exception as error:proof.update(failureType=type(error).__name__,failureMessage=str(error)[-2048:]);save();raise
    finally:save()


if __name__=='__main__':main()
