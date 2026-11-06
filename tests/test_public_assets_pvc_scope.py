import json
import os
import pathlib
import subprocess
import tempfile
import unittest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
CLAIM = {'apiVersion':'v1','kind':'PersistentVolumeClaim','metadata':{'name':'web-immutable-assets','namespace':'options-edge'},'spec':{'storageClassName':'standard','accessModes':['ReadWriteOnce'],'resources':{'requests':{'storage':'2Gi'}}}}


class PublicAssetsPvcScopeTest(unittest.TestCase):
    def run_script(self, dry=True, existing='', scope='true', environment='dev', claims=None):
        with tempfile.TemporaryDirectory() as folder:
            tmp=pathlib.Path(folder)
            docs=[{'apiVersion':'v1','kind':'Namespace','metadata':{'name':'options-edge'}}, {'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'unrelated-config','namespace':'options-edge'},'data':{'sentinel':'do-not-apply'}}, {'apiVersion':'v1','kind':'Secret','metadata':{'name':'unrelated-secret','namespace':'options-edge'},'data':{'sentinel':'ZmFrZQ=='}}]
            docs += [dict(CLAIM,metadata={'name':'other-claim','namespace':'options-edge'})]
            docs += [CLAIM] if claims is None else claims
            (tmp/'fixture.yaml').write_text(yaml.safe_dump_all(docs))
            tool=tmp/'kubectl'
            tool.write_text('''#!/usr/bin/env python3
import json,os,pathlib,sys
p=pathlib.Path(os.environ['SCOPE_FIXTURE']);a=sys.argv[1:]
body=''
if '-f' in a:
 target=a[a.index('-f')+1];body=sys.stdin.read() if target=='-' else pathlib.Path(target).read_text()
with (p/'calls.jsonl').open('a') as f:f.write(json.dumps({'args':a,'body':body})+'\\n')
if a[0]=='kustomize':print((p/'fixture.yaml').read_text());sys.exit(0)
if 'get' in a and 'pvc' in a:
 value=os.environ.get('EXISTING_CLASS','')
 if value:print(value,end='');sys.exit(0)
 sys.exit(1)
if a[0] in ['diff','apply','create']:sys.exit(0)
sys.exit(9)
''')
            tool.chmod(0o755)
            env=dict(os.environ,PATH=str(tmp)+':'+os.environ['PATH'],SCOPE_FIXTURE=str(tmp),EXISTING_CLASS=existing,ENVIRONMENT=environment,DEPLOY_DRY_RUN=str(dry).lower(),WORK_DIR=str(tmp/'work'))
            if scope is not None:env['WEB_PUBLIC_ASSETS_PVC_ONLY']=scope
            else:env.pop('WEB_PUBLIC_ASSETS_PVC_ONLY',None)
            result=subprocess.run(['bash',str(ROOT/'scripts/deploy/common-infra.sh')],cwd=ROOT,env=env,text=True,capture_output=True,timeout=20)
            calls=[json.loads(line) for line in (tmp/'calls.jsonl').read_text().splitlines()] if (tmp/'calls.jsonl').exists() else []
            return result,calls

    def assert_only_target_creates(self,calls,dry):
        mutations=[c for c in calls if c['args'][0] in ['apply','create','diff']]
        self.assertEqual(1,len(mutations))
        operation=mutations[0]
        self.assertEqual('create',operation['args'][0])
        self.assertEqual(dry,'--dry-run=server' in operation['args'])
        docs=list(yaml.safe_load_all(operation['body']))
        self.assertEqual([CLAIM],docs)

    def test_dryrun_and_apply_touch_only_the_public_claim(self):
        for dry in [True,False]:
            result,calls=self.run_script(dry=dry)
            self.assertEqual(0,result.returncode,result.stdout+result.stderr)
            self.assert_only_target_creates(calls,dry)
            self.assertNotIn('unrelated-secret',result.stdout)

    def test_existing_claim_is_never_patched_and_class_conflicts_refuse(self):
        result,calls=self.run_script(dry=False,existing='standard')
        self.assertEqual(0,result.returncode,result.stderr)
        self.assertFalse(any(c['args'][0] in ['apply','create','diff'] for c in calls))
        result,calls=self.run_script(dry=False,existing='other')
        self.assertNotEqual(0,result.returncode)
        self.assertFalse(any(c['args'][0] in ['apply','create'] for c in calls))

    def test_invalid_scope_and_non_dev_fail_before_cluster_access(self):
        for scope,environment in [('true','production'),('true','experiment'),('invalid','dev')]:
            result,calls=self.run_script(scope=scope,environment=environment)
            self.assertNotEqual(0,result.returncode)
            self.assertEqual([],calls)

    def test_missing_duplicate_or_wrong_namespace_target_refuses(self):
        for claims in [[],[CLAIM,CLAIM],[dict(CLAIM,metadata={'name':'web-immutable-assets','namespace':'other'})]]:
            result,calls=self.run_script(claims=claims)
            self.assertNotEqual(0,result.returncode)
            self.assertFalse(any(c['args'][0] in ['apply','create'] for c in calls))

    def test_omitted_flag_preserves_full_infra_dryrun(self):
        result,calls=self.run_script(scope=None,environment='experiment')
        self.assertEqual(0,result.returncode,result.stdout+result.stderr)
        self.assertTrue(any(c['args'][0]=='diff' for c in calls))
        apply=next(c for c in calls if c['args'][0]=='apply')
        self.assertIn('unrelated-config',apply['body'])
        self.assertIn('--dry-run=server',apply['args'])
        self.assertEqual(2,sum(c['args'][0]=='create' for c in calls))

if __name__=='__main__':unittest.main()
