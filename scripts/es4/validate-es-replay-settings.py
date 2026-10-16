#!/usr/bin/env python3
"""Resolve the selected feed template's environment in memory and validate its Rust outputs."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    source=Path(sys.argv[1]).resolve()
    target=os.environ.get('REPLAY_TARGET','')
    specs={'production':('ES4_KUBECONFIG','ES4_NS_UID','es4','es-feed','es-feed'),
           'dev':('DEV_KUBECONFIG','DEV_NS_UID','dev','options-edge-databento-feed','databento-feed')}
    if target not in specs: raise RuntimeError('Explicit replay target required')
    key,uid,role,workload,container=specs[target];kube=os.environ[key]
    subprocess.run(['bash','scripts/es4/assert-cluster-identity.sh',kube,role,os.environ[uid]],check=True,timeout=30)
    command=['kubectl','--kubeconfig',kube,'-n','options-edge']
    def get(kind,name,optional=False):
        r=subprocess.run(command+['get',kind,name,'-o','json'],text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
        if r.returncode:
            if optional and 'NotFound' in r.stderr:return None
            raise RuntimeError('Runtime environment object is unavailable')
        return json.loads(r.stdout)
    deployment=get('deployment',workload)
    selected=next(c for c in deployment['spec']['template']['spec']['containers'] if c['name']==container)
    values={};redactions=set()
    def data(kind,ref):
        obj=get(kind,ref['name'],ref.get('optional',False))
        if obj is None:return {}
        raw=obj.get('data',{})
        if kind=='secret':
            raw={k:base64.b64decode(v,validate=True).decode() for k,v in raw.items()}
            redactions.update(v for v in raw.values() if len(v)>=6)
        return raw
    for item in selected.get('envFrom',[]):
        kind='configmap' if 'configMapRef' in item else 'secret'
        ref=item.get('configMapRef',item.get('secretRef'))
        values.update({item.get('prefix','')+k:v for k,v in data(kind,ref).items()})
    for item in selected.get('env',[]):
        if 'value' in item:values[item['name']]=item['value'];continue
        ref=item.get('valueFrom',{})
        if 'configMapKeyRef' in ref:kind,detail='configmap',ref['configMapKeyRef']
        elif 'secretKeyRef' in ref:kind,detail='secret',ref['secretKeyRef']
        else:raise RuntimeError('Unsupported dynamic runtime environment reference')
        resolved=data(kind,detail)
        if detail['key'] not in resolved and not detail.get('optional',False):raise RuntimeError('Required runtime environment key missing')
        if detail['key'] in resolved:values[item['name']]=resolved[detail['key']]
    # No environment snapshot or key is written to disk or placed in argv.
    binary=Path(os.environ.get('CARGO_TARGET_DIR',str(source/'rust/target')))/'debug/examples/es-replay-settings'
    child_env=dict(os.environ);child_env.pop('DATABENTO_API_KEY',None)
    r=subprocess.run([str(binary)],input=json.dumps(values),env=child_env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=45)
    output=r.stdout
    for value in sorted(redactions,key=len,reverse=True):output=output.replace(value,'[REDACTED]')
    print(output,flush=True)
    if r.returncode:raise RuntimeError('Prospective Rust settings/output-topic validation failed')

if __name__=='__main__':
    try:main()
    except Exception as exc:
        print(type(exc).__name__+': settings preflight did not pass',file=sys.stderr)
        sys.exit(1)
