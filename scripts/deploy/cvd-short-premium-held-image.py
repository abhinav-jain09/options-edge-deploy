"""Image-only upgrade of the existing stopped two-agent workload; native agents are untouched."""
import copy
from decimal import Decimal
import json
import os
import re
import subprocess

JOB='short-premium-agent'
REPO='options-edge-short-premium-agent'


def source_main():
    value=subprocess.check_output(['git','ls-remote','git@github.com:abhinav-jain09/short-premium-agent.git','refs/heads/main'],text=True,timeout=30)
    match=re.fullmatch(r'([0-9a-f]{40})\s+refs/heads/main\s*',value)
    if not match:raise ValueError('One exact Short Premium main required')
    return match[1]


def parameters(row):return {p['name']:p.get('value') for a in row.get('actions',[]) for p in a.get('parameters',[])}


def latest(rows,predicate):
    if not isinstance(rows,list) or len(rows)>64:raise ValueError('Bounded Short Premium build inventory required')
    selected=[]
    for row in rows:
        params=parameters(row)
        if predicate(params):
            if type(row.get('number')) is not int or row['number']<=0:raise ValueError('Positive Short Premium build required')
            selected.append((row,params))
    if not selected:raise ValueError('No relevant Short Premium build')
    row,params=max(selected,key=lambda x:x[0]['number'])
    if row.get('building') is not False or row.get('result')!='SUCCESS':raise ValueError('Latest relevant Short Premium build not SUCCESS; never fall back')
    return row,params


def artifact(read,number,path):
    return json.loads(read(os.environ['JENKINS_URL'].rstrip('/')+'/job/'+JOB+'/'+str(number)+'/artifact/'+path,262144))


def qualification(rows,lock,read):
    row,params=latest(rows,lambda p:p.get('QUALIFICATION_ONLY') is True and p.get('ENVIRONMENT')=='production')
    receipt=artifact(read,row['number'],'.ci-contracts/qualification/receipt.json')
    if params.get('PERMITTED_SHA')!=lock['source'] or receipt.get('source')!=lock['source'] or receipt.get('sdkSource')!=lock['contractsSource'] or receipt.get('sdkSha256')!=lock['contractsSha256']:
        raise ValueError('Private Linux qualification source/SDK bytes differ')
    if receipt.get('host')!='Linux es4' or any(receipt.get(key) is not False for key in ['installed','agentAChanged','agentBChanged']):raise ValueError('Private Linux qualification scope differs')
    perf=artifact(read,row['number'],'.ci-contracts/qualification/performance.json')
    required={'realPayloadParity':60,'repetitionsPerPath':5,'emissionsPerRepetition':12,'regressionThresholdPercent':10,'cadenceSeconds':1,'memoryAbsoluteLimit':230686720,'childCpuIncluded':True,'sameBenchmarkInputAndHost':True}
    if any(perf.get(key)!=value for key,value in required.items()):raise ValueError('Private qualification coverage/budgets differ')
    for metric in ['cpuUsec','memoryPeak','latencyMedian']:
        baseline=Decimal(str(perf['results']['baseline'][metric]));candidate=Decimal(str(perf['results']['candidate'][metric]))
        if not baseline.is_finite() or not candidate.is_finite() or baseline<=0 or candidate<=0 or candidate>baseline*Decimal('1.10'):raise ValueError('Private qualification performance gate failed')
    if Decimal(str(perf['results']['candidate']['memoryPeak']))>230686720:raise ValueError('Private qualification absolute memory gate failed')
    return row['number']


def resolve(environment,rows,read,reader):
    row,params=latest(rows,lambda p:p.get('QUALIFICATION_ONLY') is False and p.get('ENVIRONMENT')==environment)
    target=reader.TARGETS[environment]
    if params.get('PUSH_IMAGE') is not True or params.get('DEPLOY_NATIVE_A') is not False or params.get('BUILD_PLATFORM')!=target['platform'] or params.get('IMAGE_REGISTRY') or params.get('IMAGE_TAG') or params.get('DEV_IMAGE_TAG'):
        raise ValueError('Published fixed-profile container-only build required')
    lock=artifact(read,row['number'],'.jenkins-image-lock.json');source=lock.get('source','');sdk=lock.get('contractsSource','');sha=lock.get('contractsSha256','')
    if not re.fullmatch('[0-9a-f]{40}',source) or source!=params.get('PERMITTED_SHA') or source!=source_main() or sdk!=reader.sdk_main() or not re.fullmatch('[0-9a-f]{40}',sdk) or not re.fullmatch('[0-9a-f]{64}',sha) or lock.get('pushed') is not True:
        raise ValueError('Current Short Premium main and SDK publication proof required')
    prefix=target['registry']+'/'+REPO+':'+str(row['number'])+'-'+source[:12]+'@sha256:';image=lock.get('image','')
    if not image.startswith(prefix) or not re.fullmatch('[0-9a-f]{64}',image[len(prefix):]):raise ValueError('Short Premium image is not bound to successful source/build')
    qualified=qualification(rows,lock,read)
    return image,{'processingBuild':row['number'],'processingSource':source,'contractsSource':sdk,'contractsJarSha256':sha,'privateLinuxQualificationBuild':qualified,'nativeAgentChanges':False,'imageBuildJob':JOB,'qualificationScope':'same-source private Linux gate, not stopped-workload runtime readiness'}


def configure(core,environment,reader):
    core.SERVICES={'short-premium-agent':('short-premium-agent-service','agent-a',REPO)}
    read=core.read_url
    core.read_url=lambda url,limit:read(url.replace('/job/options-edge-processing/','/job/'+JOB+'/'),limit)
    core.qualified_image=lambda service,rows,fetch:resolve(environment,rows,read,reader)
    core.processing_main=source_main
    def changed(*args):raise ValueError('Short Premium main changed; fresh image and private qualification required')
    core.component_source_proof=changed
    original_patch=core.patch_for;original_verify=core.verify
    def containers(live):
        values=live.get('spec',{}).get('template',{}).get('spec',{}).get('containers',[])
        if live.get('spec',{}).get('replicas')!=0 or len(values)!=2 or [c.get('name') for c in values]!=['agent-a','agent-b'] or any(not isinstance(c.get('image'),str) or not c['image'] for c in values):raise ValueError('Existing stopped agent-a/agent-b pair required')
        return values
    def pair_patch(service,live,image):
        values=containers(live);single=copy.deepcopy(live);single['spec']['template']['spec']['containers']=single['spec']['template']['spec']['containers'][:1]
        ops=original_patch(service,single,image)
        return ops[:-1]+[{'op':'test','path':'/spec/template/spec/containers/1/name','value':'agent-b'},{'op':'test','path':'/spec/template/spec/containers/1/image','value':values[1]['image']}]+ops[-1:]+[{'op':'replace','path':'/spec/template/spec/containers/1/image','value':image}]
    def pair_verify(service,before,after,image):
        old=containers(before);new=containers(after)
        if new[1]['image']!=image:raise ValueError('Agent B image was not updated')
        view=copy.deepcopy(after);view['spec']['template']['spec']['containers'][1]['image']=old[1]['image']
        original_verify(service,before,view,image)
    core.patch_for=pair_patch;core.verify=pair_verify
