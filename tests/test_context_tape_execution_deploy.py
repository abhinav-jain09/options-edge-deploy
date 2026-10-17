import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/deploy/ensure-context-tape-execution-auth.sh'

class ExecutionCredentialTest(unittest.TestCase):
    def run_script(self, mode, initial=None, failure=None, guarded=True):
        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder)
            if initial is not None:
                (p / 'state.json').write_text(json.dumps(initial))
            stub = p / 'kubectl'
            stub.write_text('''#!/usr/bin/env python3
import base64,json,os,pathlib,sys
p=pathlib.Path(os.environ['FIXTURE'])
with (p/'calls').open('a') as f: f.write(json.dumps(sys.argv[1:])+'\\n')
if os.environ.get('FAILURE')=='get' and 'get' in sys.argv: sys.exit(1)
if 'get' in sys.argv:
 if (p/'state.json').exists(): print((p/'state.json').read_text())
 elif '--ignore-not-found' not in sys.argv: sys.exit(1)
elif 'create' in sys.argv:
 if os.environ.get('FAILURE')=='create': sys.exit(1)
 arg=next(x for x in sys.argv if x.startswith('--from-file=token='))
 token=pathlib.Path(arg.split('=',2)[2]).read_bytes()
 (p/'state.json').write_text(json.dumps({'data':{'token':base64.b64encode(token).decode()}}))
else: sys.exit('unexpected mutation')
''')
            stub.chmod(0o700)
            env = dict(os.environ, PATH=f'{p}:'+os.environ['PATH'], FIXTURE=str(p), ENVIRONMENT='dev', DEPLOY_DRY_RUN=mode, FAILURE=failure or '', PERMITTED_SHA_GUARD='PASSED' if guarded else '', BUILD_NUMBER='1', JOB_NAME='common-infra-deploy')
            result = subprocess.run(['bash', str(SCRIPT)], env=env, text=True, capture_output=True)
            calls = [json.loads(x) for x in (p/'calls').read_text().splitlines()]
            state = json.loads((p/'state.json').read_text()) if (p/'state.json').exists() else None
            return result, calls, state

    def test_dry_run_never_creates(self):
        result, calls, state = self.run_script('true')
        self.assertEqual(0,result.returncode,result.stderr)
        self.assertIsNone(state)
        self.assertTrue(all('get' in c for c in calls))

    def test_existing_token_preserved_and_not_printed(self):
        token = 'A'*64
        initial = {'data':{'token':base64.b64encode(token.encode()).decode()}}
        result, calls, state = self.run_script('false',initial)
        self.assertEqual(0,result.returncode,result.stderr)
        self.assertEqual(initial,state)
        self.assertNotIn(token,result.stdout+result.stderr)
        self.assertTrue(all('get' in c for c in calls))

    def test_invalid_token_is_not_rotated(self):
        result, calls, state = self.run_script('false',{'data':{'token':'bad'}})
        self.assertNotEqual(0,result.returncode)
        self.assertTrue(all('get' in c for c in calls))

    def test_create_requires_guard_and_never_outputs_secret(self):
        result, calls, state = self.run_script('false',guarded=False)
        self.assertNotEqual(0,result.returncode)
        self.assertIsNone(state)
        result, calls, state = self.run_script('false')
        self.assertEqual(0,result.returncode,result.stderr)
        token = base64.b64decode(state['data']['token']).decode()
        self.assertEqual(64,len(token))
        self.assertNotIn(token,result.stdout+result.stderr+json.dumps(calls))
        path = next(x for c in calls for x in c if x.startswith('--from-file=token='))
        self.assertFalse(Path(path.split('=',2)[2]).exists())

    def test_permission_or_create_failure_stops(self):
        for failure in ['get','create']:
            result, _, state = self.run_script('false',failure=failure)
            self.assertNotEqual(0,result.returncode)
            self.assertIsNone(state)

    def test_rendered_web_and_broker_share_issuer_credential_and_private_port(self):
        for environment in ['dev','production']:
            render = subprocess.check_output(['kubectl','kustomize',str(ROOT/'k8s/overlays'/environment)],text=True)
            def select(expression):
                return json.loads(subprocess.check_output(['yq','-o=json',expression],input=render,text=True))
            broker=select('select(.kind == "Deployment" and .metadata.name == "broker-execution-service") | .spec.template.spec.containers[0]')
            web=select('select(.kind == "Deployment" and .metadata.name == "options-edge-web") | .spec.template.spec.containers[0]')
            broker_env={x['name']:x for x in broker['env']}
            web_env={x['name']:x for x in web['env']}
            self.assertEqual(web_env['VITE_AUTH_ISSUER']['value'],broker_env['AUTH_ISSUER_URI']['value'])
            self.assertEqual(web_env['CONTEXT_TAPE_EXECUTION_TOKEN']['valueFrom'],broker_env['CONTEXT_TAPE_EXECUTION_TOKEN']['valueFrom'])
            self.assertEqual('http://broker-execution-service:8131',web_env['CONTEXT_TAPE_EXECUTION_URL']['value'])
            self.assertEqual('8131',broker_env['SERVER_PORT']['value'])
            self.assertEqual('/health',broker['readinessProbe']['httpGet']['path'])
            service=select('select(.kind == "Service" and .metadata.name == "broker-execution-service") | .spec')
            self.assertEqual('ClusterIP',service['type'])
            self.assertEqual(8131,service['ports'][0]['port'])
            self.assertEqual('http',service['ports'][0]['targetPort'])
            origins=web_env['CONTEXT_TAPE_PUBLIC_ORIGINS']['value'].split(',')
            self.assertIn('http://localhost:8090' if environment=='dev' else 'https://bleadingoptions.com',origins)

    def test_auth_only_keeps_other_infra_stages_disabled(self):
        text = (ROOT/'Jenkinsfile.common-infra').read_text()
        self.assertLess(text.index("stage('Permitted commit guard')"),text.index("stage('Context Tape execution credential only')"))
        for name in ['Public gate (PGL-072, secondary)','Render + Diff (always)','Approve production infra change','Apply','WARN: realm reconciliation skipped','Reconcile public realm']:
            stage=text.split("stage('"+name+"')",1)[1].split('steps {',1)[0]
            self.assertIn('!params.CONTEXT_TAPE_AUTH_ONLY',stage)

if __name__=='__main__': unittest.main()
