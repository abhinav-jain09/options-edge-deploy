"""API publication provenance and stopped-only image handoff, with build/runtime effects stubbed."""
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
m=load('evidence_roll',ROOT/'scripts/deploy/cvd-evidence-api-image.py')
w=load('evidence_targets',ROOT/'scripts/deploy/cvd-image-rollforward.py')
SOURCE='a'*40;SDK='b'*40;DEPLOY='d'*40;CONFIG='sha256:'+'c'*64;DIGEST='sha256:'+'e'*64

class ApiImageTest(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='cvd-api-images-'));self.previous=Path.cwd();os.chdir(self.root);self.addCleanup(os.chdir,self.previous)
        self.env={'JENKINS_URL':'http://jenkins/','PERMITTED_SHA_GUARD':'PASSED','PERMITTED_SHA':DEPLOY,'BUILD_NUMBER':'17','ENVIRONMENT':'dev','ACTION':'build'}
        self.reader=SimpleNamespace(TARGETS=copy.deepcopy(w.TARGETS),sdk_main=lambda:SDK);self.steps=[];self.commands=[];self.cleanups=[]
    def build(self,environment='dev',test_failure=False,publish_drift=False,moved=False,test_timeout=False,test_overflow=False):
        def sdk_latest(base,source,destination):
            destination.write_bytes(b'official SDK fixture');destination.with_suffix('.release.json').write_text(json.dumps({'source':SDK,'sha256':'f'*64,'version':'0.23.0'}))
        def command(*args,**kwargs):
            self.commands.append(args)
            if args[:2]==('git','clone'):Path(args[-1]).mkdir();return ''
            if 'rev-parse' in args:return SOURCE+'\n'
            if args[:3]==('docker','image','inspect'):return CONFIG+'\n'
            return ''
        def docker(args,root,name,timeout):
            self.steps.append((name,args,timeout))
            if name=='tests' and test_failure:raise ValueError('test failure fixture')
            if name=='tests' and test_timeout:raise m.subprocess.TimeoutExpired(['docker'],120)
            if name=='tests' and test_overflow:raise ValueError('Bounded Docker log budget exceeded')
            if name=='publish':Path(args[args.index('--metadata-file')+1]).write_text(json.dumps({'containerimage.digest':DIGEST,'containerimage.config.digest':'sha256:'+'0'*64 if publish_drift else CONFIG}))
        with patch.dict(os.environ,self.env,clear=True),patch.object(m,'current',side_effect=[SOURCE,'0'*40] if moved else lambda:SOURCE),patch.object(m,'command',side_effect=command),patch.object(m,'docker_step',side_effect=docker),patch.object(m,'cleanup_test',side_effect=lambda name,owner:self.cleanups.append((name,owner))),patch.object(m,'load',return_value=SimpleNamespace(latest=sdk_latest)),patch('sys.stdout',new=io.StringIO()):m.build_image(environment,'17',self.reader)
        return json.loads((self.root/'.jenkins-tmp/evidence-api-17/release.json').read_text())
    def row(self,number=17,result='SUCCESS',building=False,environment='dev',action='build'):
        return {'number':number,'result':result,'building':building,'actions':[{'parameters':[{'name':'ENVIRONMENT','value':environment},{'name':'ACTION','value':action},{'name':'PERMITTED_SHA','value':DEPLOY}]}]}
    def select(self,proof,rows=None,environment='dev'):
        with patch.dict(os.environ,self.env,clear=True):return m.select_release(environment,rows or [self.row()],lambda url,limit:json.dumps(proof),self.reader)
    def test_both_architectures_publish_only_after_their_own_tests(self):
        proof=self.build();self.assertEqual(['build','tests','publish'],[x[0] for x in self.steps]);self.assertEqual('PASS',proof['status']);self.assertFalse(proof['runtimeDeployed']);self.assertEqual(SOURCE,proof['processingSource']);self.assertEqual(SDK,proof['contractsSource']);self.assertIn('host.docker.internal:5001/',proof['image']);self.assertEqual('linux/arm64',proof['platform'])
        test=self.steps[1][1];self.assertIn('--memory=512m',test);self.assertIn('--cpus=1',test);self.assertIn('TRADE_API_TEST=1',test);self.assertNotIn('-p',test[:test.index('python')]);self.assertEqual(120,self.steps[1][2])
        self.assertTrue(self.steps[-1][1][self.steps[-1][1].index('-t')+1].startswith('192.168.100.102:5001/'));self.assertIn('--name',test);self.assertIn('timeout=110',test[-1])
        self.assertFalse(any(x[0] in ['kubectl','launchctl','ssh'] for x in self.commands))
        image,meta=self.select(proof);self.assertEqual(proof['image'],image);self.assertEqual(SOURCE,meta['processingSource'])
    def test_production_uses_only_the_production_architecture_and_registry(self):
        proof=self.build('production');self.assertEqual('linux/amd64',proof['platform']);self.assertTrue(proof['image'].startswith('192.168.100.252:5000/'));self.assertEqual(3,len(self.steps))
        for name,args,timeout in self.steps:self.assertEqual('linux/amd64',args[args.index('--platform')+1])
    def test_failed_final_image_tests_cannot_publish_or_create_a_release(self):
        with self.assertRaisesRegex(ValueError,'test failure'):self.build(test_failure=True)
        self.assertEqual(['build','tests'],[x[0] for x in self.steps]);self.assertEqual([('cvd-evidence-api-test-dev-17',DEPLOY+'-dev-17')],self.cleanups);self.assertFalse((self.root/'.jenkins-tmp/evidence-api-17/release.json').exists())
    def test_timed_out_test_cleans_only_the_named_owned_container(self):
        with self.assertRaises(m.subprocess.TimeoutExpired):self.build(test_timeout=True)
        self.assertEqual([('cvd-evidence-api-test-dev-17',DEPLOY+'-dev-17')],self.cleanups);self.assertEqual(['build','tests'],[x[0] for x in self.steps]);self.assertFalse((self.root/'.jenkins-tmp/evidence-api-17/release.json').exists())
    def test_log_limited_test_cleans_only_the_named_owned_container(self):
        with self.assertRaisesRegex(ValueError,'log budget'):self.build(test_overflow=True)
        self.assertEqual([('cvd-evidence-api-test-dev-17',DEPLOY+'-dev-17')],self.cleanups);self.assertEqual(['build','tests'],[x[0] for x in self.steps])
    def test_publication_config_drift_has_no_deployable_receipt(self):
        with self.assertRaisesRegex(ValueError,'differs from the tested'):self.build(publish_drift=True)
        self.assertFalse((self.root/'.jenkins-tmp/evidence-api-17/release.json').exists());receipt=json.loads((self.root/'.jenkins-tmp/evidence-api-17/build-receipt.json').read_text());self.assertEqual('TESTS_PASSED',receipt['status'])
    def test_new_source_before_publication_refuses_without_pushing(self):
        with self.assertRaisesRegex(ValueError,'changed before publication'):self.build(moved=True)
        self.assertEqual(['build','tests'],[x[0] for x in self.steps])
    def test_newest_build_never_falls_back_and_rollout_runs_are_not_image_builds(self):
        proof=self.build()
        for row in [self.row(18,'FAILURE'),self.row(18,None,True)]:
            with patch.dict(os.environ,self.env,clear=True),patch.object(m,'command') as command:
                with self.assertRaisesRegex(ValueError,'never fall back'):m.select_release('dev',[self.row(),row],lambda *args: self.fail('artifact fetched behind failed build'),self.reader)
                command.assert_not_called()
        image,meta=self.select(proof,[self.row(),self.row(18,action='roll-forward')]);self.assertEqual(proof['image'],image)
    def test_foreign_release_identity_platform_sdk_or_bytes_refuse(self):
        proof=self.build()
        for key,value in [('deploymentSource','0'*40),('build',18),('runtimeDeployed',True),('guardVersion','x'),('platform','linux/amd64'),('environment','production'),('contractsSource','0'*40),('contractsJarSha256',''),('testedConfigDigest',''),('image',proof['image'].replace('host.docker.internal:5001','untrusted.registry'))]:
            with self.subTest(key=key),self.assertRaises(ValueError):self.select({**proof,key:value})
    def test_rollout_is_image_only_and_refuses_an_active_workload(self):
        core=load('api_cas_test',ROOT/'scripts/es4/pinned-image-rollforward.py')
        live={'kind':'Deployment','apiVersion':'apps/v1','metadata':{'name':'trade-evidence-api','namespace':'options-edge','uid':'existing','resourceVersion':'9'},'spec':{'replicas':0,'template':{'spec':{'containers':[{'name':'api','image':'old','env':[{'name':'existing','value':'preserved'}]}]}}}}
        image='host.docker.internal:5001/options-edge-trade-evidence-api:dev-17-'+SOURCE[:12]+'@'+DIGEST
        def main():
            ops=core.patch_for('trade-evidence-api',live,image);self.assertEqual(['test']*5+['replace'],[x['op'] for x in ops]);self.assertEqual('/spec/template/spec/containers/0/image',ops[-1]['path'])
            with self.assertRaisesRegex(ValueError,'stopped hold'):core.patch_for('trade-evidence-api',{**live,'spec':{**live['spec'],'replicas':1}},image)
            self.assertEqual(w.TARGETS['dev']['uid'],os.environ['ES4_NS_UID']);self.assertEqual(w.TARGETS['dev']['kubeconfig'],os.environ['ES4_KUBECONFIG'])
        with patch.dict(os.environ,self.env,clear=True),patch.object(m,'load',return_value=core),patch.object(core,'main',side_effect=main):m.roll_image('dev',w)
    def test_cleanup_kills_only_the_owned_container_id_and_treats_absence_as_clean(self):
        identity='1'*64;name='cvd-evidence-api-test-dev-17';owner=DEPLOY+'-dev-17';calls=[]
        def command(*args,**kwargs):
            calls.append(args)
            return identity+'|'+owner if args[1]=='container' else ''
        with patch.object(m,'command',side_effect=command):m.cleanup_test(name,owner)
        self.assertEqual(('docker','kill',identity),calls[-1])
        with patch.object(m,'command',return_value=identity+'|foreign') as call:
            with self.assertRaisesRegex(ValueError,'foreign'):m.cleanup_test(name,owner)
            self.assertEqual(1,call.call_count)
        with patch.object(m,'command',side_effect=m.subprocess.CalledProcessError(1,['docker'],stderr='Error: No such container: '+name)) as call:m.cleanup_test(name,owner);self.assertEqual(1,call.call_count)
    def test_sparse_source_capture_excludes_unrelated_tracked_links(self):
        import subprocess
        source=self.root/'source';source.mkdir()
        def git(*args):return subprocess.check_output(['git','-c','commit.gpgsign=false','-c','core.hooksPath=/dev/null','-C',str(source),*args],text=True,stderr=subprocess.PIPE).strip()
        git('init','-b','main');git('config','user.name','API Fixture');git('config','user.email','api@example.invalid');(source/'trade-evidence-api').mkdir();(source/'trade-evidence-api/app.py').write_text('verified API source');(source/'unrelated-link').symlink_to('/owned-fixture-missing');git('add','.');git('commit','-m','fixture');commit=git('rev-parse','HEAD');root=self.root/'sparse';root.mkdir()
        with patch.object(m,'PROCESSING',str(source)):checkout=m.prepare_source(root,commit)
        self.assertEqual('verified API source',(checkout/'trade-evidence-api/app.py').read_text());self.assertFalse((checkout/'unrelated-link').is_symlink());self.assertFalse(any(p.is_symlink() for p in checkout.rglob('*')))

    def test_log_file_budget_is_enforced_by_a_real_small_child(self):
        import sys
        with patch('sys.stdout',new=io.StringIO()):m.docker_step([sys.executable,'-c','print("bounded log")'],self.root,'small',10)
        self.assertEqual('bounded log\n',(self.root/'small.log').read_text())
        with patch('sys.stdout',new=io.StringIO()),self.assertRaisesRegex(ValueError,'log budget exceeded'):
            m.docker_step([sys.executable,'-c','import os; os.write(1,b"x"*(9<<20))'],self.root,'excess',10)
        self.assertLessEqual((self.root/'excess.log').stat().st_size,8<<20)
    def test_staging_is_fresh_bounded_and_never_cleans_an_old_build(self):
        root=m.staging('17');(root/'owned-proof').write_text('preserved')
        with self.assertRaises(FileExistsError):m.staging('17')
        self.assertEqual('preserved',(root/'owned-proof').read_text())
    def test_without_jenkins_guard_nothing_is_inspected(self):
        with patch.dict(os.environ,{},clear=True),patch.object(m,'command') as command:
            with self.assertRaisesRegex(ValueError,'Guarded Jenkins'):m.main()
            command.assert_not_called()

if __name__=='__main__':unittest.main()
