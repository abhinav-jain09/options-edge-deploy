#!/usr/bin/env python3
"""Register exact released schemas for existing CVD-related topics; never changes Kafka."""
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

ENDPOINTS = [('es4','192.168.100.4:9092','http://192.168.100.4:8081'),
             ('dev','192.168.100.102:19092','http://192.168.100.102:8082'),
             ('production','192.168.100.252:9092','http://192.168.100.252:8082')]
MAX_METADATA_BYTES=8<<20


def selected_endpoints(spec):
    """CVD_SCHEMA_ENVIRONMENTS: a comma list of es4, dev, production (default: all three, as before). An unknown, repeated or empty name is refused."""
    names=[n.strip() for n in spec.split(',')]
    known=[e[0] for e in ENDPOINTS]
    if not names or any(n not in known for n in names) or len(set(names))!=len(names):
        raise ValueError('CVD_SCHEMA_ENVIRONMENTS must be a comma list of distinct names from '+','.join(known))
    return [e for e in ENDPOINTS if e[0] in names]

BINDINGS = {
    'futures.cvd':'cvd/CvdSnapshot.avsc', 'futures.cvd.bars':'cvd/CvdBarEnvelope.avsc',
    'futures.cvd.levels':'cvd/CvdLevelsSnapshot.avsc', 'futures.footprint':'cvd/FootprintSnapshot.avsc',
    'futures.footprint.bars':'cvd/FootprintBarEnvelope.avsc', 'futures.footprint.outcomes':'cvd/FootprintOutcome.avsc',
    'futures.footprint.evidence':'cvd/FootprintEvidenceSnapshot.avsc',
    'futures.footprint.strike':'cvd-related/FootprintStrikeEnvelope.avsc',
    'es.open-direction.forecast':'cvd-related/OpenDirectionForecastEnvelope.avsc',
    'es.open-direction.outcome':'cvd-related/OpenDirectionOutcomeEnvelope.avsc',
    'es.open-direction.status':'cvd-related/OpenDirectionStatus.avsc',
    'options.es-cvd-spx-levels':'cvd-related/AlignedCvdLevelsEnvelope.avsc',
    'es.futures.compression-expansion.current':'cvd-related/EscxCurrent.avsc',
    'es.futures.compression-expansion.events':'cvd-related/EscxEvent.avsc',
    'futures.level-absorption.events':'cvd-related/LevelAbsorptionEvent.avsc',
    'futures.level-absorption.voice':'cvd-related/LevelAbsorptionVoice.avsc',
    'futures.level-absorption.closures':'cvd-related/LevelAbsorptionClosure.avsc',
    'futures.level-absorption.manual-line':'cvd-related/LevelAbsorptionManualLine.avsc',
    'options.es-gex-spx-aligned':'cvd-related/AlignedGexBookExtended.avsc',
    'options.es-strike-intel-spx-aligned':'cvd-related/AlignedStrikeIntelligence.avsc',
    'options.short-premium.recommendation':'cvd-related/ShortPremiumRecommendation.avsc',
    'early-auction.verdicts.shadow':'cvd-related/EarlyAuctionVerdictLedger.avsc',
    **{'context-tape.direction.'+topic:'cvd-related/Direction'+name+'.avsc' for topic,name in
       [('commissioning','Commissioning'),('checkpoint','Checkpoint'),('current','Current'),('push','Push'),
        ('alert','Alert'),('scorecard','Scorecard'),('ledger','Ledger')]},
    **{'context-tape.compression.'+topic:'cvd-related/'+name+'.avsc' for topic,name in
       [('current','RegimeCurrent'),('history','RegimeStatePoint'),('checkpoint','RegimeCheckpoint')]},
}


def binding(topic):
    # Exact defaults or ONE existing es4 prefix. No alternate/new topic is created.
    if topic.startswith('es.es.'):return None
    return BINDINGS.get(topic) or BINDINGS.get(topic.removeprefix('es.'))


def metadata(bootstrap, directory):
    """Bound output on disk while the one owned child executes; no consumer group."""
    stdout=directory/'metadata.json';stderr=directory/'metadata.err'
    with stdout.open('xb') as out, stderr.open('xb') as err:
        child=subprocess.Popen(['kcat','-L','-J','-b',bootstrap,'-m','8'],stdout=out,stderr=err,start_new_session=True)
        end=time.monotonic()+15
        try:
            while child.poll() is None:
                if time.monotonic()>end or stdout.stat().st_size+stderr.stat().st_size>MAX_METADATA_BYTES:
                    raise TimeoutError('Kafka metadata deadline/byte budget exceeded')
                time.sleep(.05)
            if child.returncode!=0:raise RuntimeError('Kafka metadata command refused')
        finally:
            if child.poll() is None:
                try:os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                child.wait(timeout=5)
    if stdout.stat().st_size+stderr.stat().st_size>MAX_METADATA_BYTES:raise ValueError('Kafka metadata byte budget exceeded')
    result=json.loads(stdout.read_bytes())
    topics=result['topics']
    if not isinstance(topics,list) or len(topics)>8192:raise ValueError('Kafka topic inventory budget exceeded')
    names=[]
    for row in topics:
        topic=row['topic']
        if binding(topic):
            if row.get('error') or not row.get('partitions'):raise ValueError('Bound topic metadata unavailable')
            names.append(topic)
    if not names or len(names)>64 or len(set(names))!=len(names):raise ValueError('Expected1..64 distinct existing bound topics')
    return sorted(names)


def http(url, path, body=None, missing=False):
    encoded=None if body is None else json.dumps(body).encode()
    request=urllib.request.Request(url+path,data=encoded,headers={'Content-Type':'application/vnd.schemaregistry.v1+json'})
    try:
        with urllib.request.urlopen(request,timeout=10) as response:raw=response.read((2<<20)+1)
    except urllib.error.HTTPError as error:
        if missing and error.code==404:
            raw=error.read((2<<20)+1)
            if len(raw)>2<<20:raise ValueError('Registry error response budget exceeded')
            if json.loads(raw).get('error_code') in (40401,40403):return None
        raise
    if len(raw)>2<<20:raise ValueError('Registry response budget exceeded')
    return json.loads(raw)


def verify_schema(response, text):
    rules=response.get('ruleSet') or {}
    if response.get('schemaType','AVRO')!='AVRO' or response.get('references') or response.get('metadata') or any(rules.values()):
        raise ValueError('Unexpected registry type/references/metadata/rules')
    from optionsedge_contracts.model import CanonicalModelCodec
    expected=CanonicalModelCodec(text).schema_fingerprint()
    actual=CanonicalModelCodec(response['schema']).schema_fingerprint()
    if actual!=expected:raise ValueError('Registered schema graph differs from released contract')


def reconcile(url, topic, text, apply, call=http):
    subject=urllib.parse.quote(topic+'-value',safe='');body={'schemaType':'AVRO','schema':text}
    lookup=call(url,'/subjects/'+subject,body,missing=True)
    created=False
    if lookup is None:
        versions=call(url,'/subjects/'+subject+'/versions?deleted=true',missing=True)
        if versions is not None and versions!=[]:raise ValueError('Existing subject history conflicts with the released schema')
        if not apply:return {'topic':topic,'registered':False,'action':'missing; dry run only'}
        registered=call(url,'/subjects/'+subject+'/versions',body)
        expected_id=registered['id'];created=True
        lookup=call(url,'/subjects/'+subject,body)
        if lookup['id']!=expected_id:raise ValueError('Registration and subject lookup disagree')
    schema_id=lookup['id']
    if type(schema_id) is not int or schema_id<=0:raise ValueError('Positive schema ID required')
    verify_schema(lookup,text)
    verify_schema(call(url,'/schemas/ids/'+str(schema_id)),text)
    return {'topic':topic,'registered':True,'schemaId':schema_id,'created':created,
            'schemaSha256':hashlib.sha256(text.encode()).hexdigest()}


def main():
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD')!='PASSED':raise ValueError('Guarded Jenkins required')
    dry=os.environ.get('DEPLOY_DRY_RUN')
    if dry not in ['true','false']:raise ValueError('Explicit dry-run mode required')
    build=os.environ['BUILD_NUMBER']
    if not re.fullmatch(r'[1-9][0-9]{0,9}',build):raise ValueError('Build number required')
    parent=Path('.jenkins-tmp')
    if parent.is_symlink():raise ValueError('Refuse symbolic staging directory')
    parent.mkdir(exist_ok=True)
    root=parent/('cvd-schemas-'+build);root.mkdir()  # Refuse stale output.
    spec=importlib.util.spec_from_file_location('released_contracts',Path(__file__).parents[1]/'jenkins/fetch-canonical-contracts.py')
    release=importlib.util.module_from_spec(spec);spec.loader.exec_module(release)
    release.latest(os.environ['JENKINS_URL'],root/'contracts-source',root/'contracts.jar')
    proof=json.loads((root/'contracts.release.json').read_text())
    # The tests use this verified SDK's actual schema identity, not a substitute parser.
    subprocess.run([sys.executable,'-B','-m','unittest','tests.test_register_cvd_schemas','tests.test_canonical_contracts_release'],
        env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str((root/'contracts.jar').resolve()),
             'CANONICAL_PACKAGED_ARTIFACT':str((root/'contracts.jar').resolve()),
             'OPTIONS_EDGE_CONTRACTS_VERSION':proof['version']},check=True,timeout=120)
    with zipfile.ZipFile(root/'contracts.jar') as jar:
        schemas={name:jar.read(name).decode() for name in sorted(set(BINDINGS.values()))}
    for text in schemas.values():
        parsed=json.loads(text)
        if not isinstance(parsed,dict) or parsed.get('type')!='record':raise ValueError('Released named record required')
    # Preflight ALL clusters and schema resources before the first registration.
    inventory=[]
    for environment,bootstrap,url in selected_endpoints(os.environ.get('CVD_SCHEMA_ENVIRONMENTS','es4,dev,production')):
        directory=root/environment;directory.mkdir()
        inventory.append((environment,url,metadata(bootstrap,directory)))
    receipt={'source':os.environ['PERMITTED_SHA'],'contracts':proof,'dryRun':dry=='true','clusters':[],
             'scope':'Exact released external schemas for existing topics only; no Kafka/group/offset/config/mode changes; inactive internal state schemas are outside this action'}
    file=root/'receipt.json'
    def save():file.write_text(json.dumps(receipt,indent=2)+'\n')
    try:
        # Preview every existing subject before any version can be registered.
        receipt['phase']='preflight-registry'
        for environment,url,topics in inventory:
            row={'environment':environment,'registry':url,'plan':[],'subjects':[]};receipt['clusters'].append(row)
            for topic in topics:
                receipt['currentSubject']={'environment':environment,'topic':topic};save()
                row['plan'].append(reconcile(url,topic,schemas[binding(topic)],False));save()
        receipt['phase']='register' if dry=='false' else 'dry-run'
        for row,(_,url,topics) in zip(receipt['clusters'],inventory):
            for topic in topics:
                receipt['currentSubject']={'environment':row['environment'],'topic':topic};save()
                # Recheck history at the write boundary; a changed subject fails closed.
                row['subjects'].append(reconcile(url,topic,schemas[binding(topic)],dry=='false'));save()
        receipt.update(complete=True,phase='completed',currentSubject=None)
    except Exception as error:
        receipt.update(complete=False,failureType=type(error).__name__);raise
    finally:file.write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt))


if __name__=='__main__':main()
