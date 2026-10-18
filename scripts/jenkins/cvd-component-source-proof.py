#!/usr/bin/env python3
"""Prove the latest qualified CVD implementation is unchanged by unrelated main commits."""
import hashlib
import json
import os
from pathlib import Path,PurePosixPath
import re
import subprocess
import time
import xml.etree.ElementTree as ET

COMPONENTS=('processing-common','canonical-topic-mirror','es-cvd-service','es-footprint-strike-service',
'context-tape-service','gamma-migration-service','early-auction-verdict-service','amt-order-bridge',
'es-open-direction-service','es-open-direction-postgres-writer','es-spx-align-service','es-compression-expansion-service',
'level-absorption-service','close-direction-service')
EXTRA=('trade-evidence-api',)
NON_RUNTIME={'docs/market-read/MARKET-READ-REQUIREMENT.md','docs/indicator-service/INDICATOR-AVRO-CONVERSION-PLAN.md','tests/test_processing_image_lock_coverage.py'}
NS='{http://maven.apache.org/POM/4.0.0}'
DEADLINE=None

def git(repo,*args):
    remaining=30 if DEADLINE is None else min(30,DEADLINE-time.monotonic())
    if remaining<=0:raise ValueError('Source proof deadline exceeded')
    result=subprocess.run(['git','-C',str(repo),*args],text=True,capture_output=True,timeout=remaining)
    if result.returncode:raise ValueError('Cannot prove source identity from Git')
    if len(result.stdout.encode())>8<<20:raise ValueError('Git proof output bound exceeded')
    return result.stdout

def blob(repo,commit,path):
    value=git(repo,'show',commit+':'+path)
    if len(value.encode())>2<<20:raise ValueError('Build model byte bound exceeded')
    return value

def xml(text):
    if '<!DOCTYPE' in text or '<!ENTITY' in text:raise ValueError('Unmodelled XML declarations')
    root=ET.fromstring(text)
    if root.tag!=NS+'project':raise ValueError('Known Maven model namespace required')
    return root

def modules(root):
    nodes=root.findall(NS+'modules')
    if len(nodes)!=1 or nodes[0].attrib:raise ValueError('One literal reactor module list required')
    result=[]
    for child in nodes[0]:
        name=(child.text or '').strip();path=PurePosixPath(name)
        if child.tag!=NS+'module' or child.attrib or len(child) or not re.fullmatch('[a-z0-9][a-z0-9-]*',name) or path.is_absolute():
            raise ValueError('Unmodelled reactor module path')
        result.append(name)
    if len(result)>256 or len(set(result))!=len(result):raise ValueError('Bounded unique reactor modules required')
    return result

def parent_model(root):
    node=root.find(NS+'modules')
    for child in list(node):node.remove(child)
    node.text=None
    return ET.tostring(root)

def artifact_ids(root):
    values=set()
    for node in root.iter(NS+'artifactId'):
        value=(node.text or '').strip()
        if not re.fullmatch('[A-Za-z0-9_.-]+',value):raise ValueError('Unmodelled dynamic artifact coordinate')
        values.add(value)
    return values

def compare(repo,qualified,current):
    if not all(re.fullmatch('[0-9a-f]{40}',value) for value in [qualified,current]):raise ValueError('Exact source commits required')
    git(repo,'merge-base','--is-ancestor',qualified,current)
    old=xml(blob(repo,qualified,'pom.xml'));new=xml(blob(repo,current,'pom.xml'));prior=modules(old);latest=modules(new)
    if latest[:len(prior)]!=prior:raise ValueError('Reactor modules removed/reordered; fresh qualification required')
    if old.findtext(NS+'packaging')!='pom' or parent_model(old)!=parent_model(new):raise ValueError('Shared parent build model changed')
    if not set(COMPONENTS)<=set(prior):raise ValueError('CVD dependency scope is incomplete')
    models={};references=artifact_ids(old);project_ids=set()
    for name in prior:
        text=blob(repo,qualified,name+'/pom.xml')
        if text!=blob(repo,current,name+'/pom.xml'):raise ValueError('An existing reactor build model changed')
        model=xml(text);identity=model.findtext(NS+'artifactId')
        if not identity or identity in project_ids:raise ValueError('Unique literal reactor identities required')
        project_ids.add(identity);models[name]=model;references|=artifact_ids(model)
    by_id={model.findtext(NS+'artifactId'):name for name,model in models.items()}
    closed_modules=list(COMPONENTS)
    permitted_external={old.findtext(NS+'artifactId'),'options-edge-contracts'}
    for name in closed_modules:
        if any(node.tag in {NS+'directory',NS+'sourceDirectory',NS+'testSourceDirectory'} for node in models[name].iter()):raise ValueError('Unmodelled explicit source/resource build directory')
        for value in artifact_ids(models[name]):
            if value in by_id and by_id[value] not in closed_modules:closed_modules.append(by_id[value])
            elif value.startswith('options-edge-') and value not in by_id and value not in permitted_external:
                raise ValueError('Unmodelled internal CVD dependency; fresh qualification required')
    added=latest[len(prior):];new_ids=set()
    for name in added:
        if git(repo,'ls-tree','--name-only',qualified,'--',name).strip():raise ValueError('New module reclassifies an existing shared path')
        model=xml(blob(repo,current,name+'/pom.xml'))
        if any(child.tag not in {NS+x for x in ['modelVersion','parent','artifactId','name','description','dependencies','packaging']} for child in model):
            raise ValueError('New module has unmodelled build/profile/repository effects')
        if model.findtext(NS+'packaging','jar')!='jar':raise ValueError('New module is not an ordinary unselected JAR')
        identity=model.findtext(NS+'artifactId')
        if not identity or not re.fullmatch('[A-Za-z0-9_.-]+',identity) or identity in references or identity in new_ids:
            raise ValueError('New module can shadow an existing coordinate')
        parent=model.find(NS+'parent')
        if parent is None or any(parent.findtext(NS+key)!=old.findtext(NS+key) for key in ['groupId','artifactId','version']) or parent.find(NS+'relativePath') is not None:
            raise ValueError('New module does not inherit the unchanged root model')
        new_ids.add(identity)
        for dependency in model.findall(NS+'dependencies/'+NS+'dependency'):
            dep=dependency.findtext(NS+'artifactId');scope=dependency.findtext(NS+'scope')
            if dep not in references or (scope not in ['test'] and dep not in project_ids):raise ValueError('New module has unmodelled dependency/model effects')
    closed=(*closed_modules,*EXTRA);trees={}
    for name in closed:
        before=git(repo,'rev-parse',qualified+':'+name).strip();after=git(repo,'rev-parse',current+':'+name).strip()
        if before!=after:raise ValueError('CVD implementation/dependency tree changed; fresh qualification required')
        if any(line.startswith('120000 ') for line in git(repo,'ls-tree','-r',current,'--',name).splitlines()):raise ValueError('Symbolic source inputs are unmodelled')
        trees[name]=after
    changed=git(repo,'diff','--name-only','--no-renames',qualified,current).splitlines()
    if len(changed)>512:raise ValueError('Source-difference budget exceeded')
    unrelated=set(latest)-set(closed_modules)
    for path in changed:
        if path=='pom.xml' or path in NON_RUNTIME:continue
        if path.split('/',1)[0] in unrelated:continue
        raise ValueError('Shared or unmodelled build/source input changed: '+path)
    return {'qualifiedSource':qualified,'currentMainSource':current,'componentTrees':trees,
        'parentModelSha256':hashlib.sha256(parent_model(xml(blob(repo,current,'pom.xml')))).hexdigest(),
        'existingPomCount':len(prior),'newUnreferencedModules':added,'unrelatedChangedPaths':changed,
        'scope':'identical CVD implementation/dependency trees and existing build models; only unrelated module sources, named operational files, and simple unreferenced module additions differ'}

def prove(qualified,current):
    global DEADLINE
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD')!='PASSED':raise ValueError('Guarded Jenkins source proof required')
    DEADLINE=time.monotonic()+180
    build=os.environ['BUILD_NUMBER']
    if not re.fullmatch('[1-9][0-9]{0,9}',build):raise ValueError('Derived build identity required')
    parent=Path('.jenkins-tmp')
    if parent.is_symlink():raise ValueError('Symbolic source proof staging refused')
    parent.mkdir(exist_ok=True);old=list(parent.glob('cvd-source-proof-*'))
    if len(old)>=128:raise ValueError('Preserved source proof count full')
    count=0;size=0
    for directory in old:
        if directory.is_symlink():raise ValueError('Symbolic source proof entry')
        for file in directory.rglob('*'):
            count+=1
            if file.is_symlink():raise ValueError('Symbolic source proof entry')
            if file.is_file():size+=file.stat().st_size
            if count>131072 or size>4<<30:raise ValueError('Preserved source proof quarantine full')
    root=parent/('cvd-source-proof-'+build);root.mkdir()
    repo=root/'processing'
    result=subprocess.run(['git','clone','--depth=64','--no-checkout','--single-branch','--branch','main',
        'git@github.com:abhinav-jain09/options-edge-processing.git',str(repo)],capture_output=True,text=True,timeout=90)
    if result.returncode or git(repo,'rev-parse','HEAD').strip()!=current:raise ValueError('Latest processing main changed during source proof')
    proof=compare(repo,qualified,current);(root/'receipt.json').write_text(json.dumps(proof,indent=2)+'\n');return proof
