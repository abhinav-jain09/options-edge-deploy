# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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
    def build(self,environment='dev',test_failure=False,publish_drift=False,moved=False,test_timeout=False,test_overflow=False,missing_recipe=False,bad_digest=False):
        def sdk_latest(base,source,destination):
            destination.write_bytes(b'official SDK fixture');destination.with_suffix('.release.json').write_text(json.dumps({'source':SDK,'sha256':'f'*64,'version':'0.23.0'}))
        def command(*args,**kwargs):
            self.commands.append(args)
            if args[:2]==('git','clone'):
                checkout=Path(args[-1]);(checkout/'trade-evidence-api/deploy').mkdir(parents=True);(checkout/'trade-evidence-api/ci/jib-image').mkdir(parents=True)
                if not missing_recipe:
                    (checkout/'trade-evidence-api/deploy/image_stage.py').write_text('# fixture');(checkout/'trade-evidence-api/ci/jib-image/pom.xml').write_text('<project/>')
                return ''
            if 'rev-parse' in args:return SOURCE+'\n'
            return ''
        def step(args,root,name,timeout,env=None):
            self.steps.append((name,args,timeout))
            if name=='tests' and test_failure:raise ValueError('test failure fixture')
            if name=='tests' and test_timeout:raise m.subprocess.TimeoutExpired(['python3.14'],600)
            if name=='tests' and test_overflow:raise ValueError('Bounded build step log budget exceeded')
            if name=='stage':
                rootfs=Path(args[args.index('--dest')+1]);(rootfs/'app').mkdir(parents=True);(rootfs/'app/app.py').write_text('tested sources')
            if name=='publish':
                target=Path(args[args.index('-f')+1]).parent/'target';target.mkdir(exist_ok=True)
                (target/'jib-image.digest').write_text('not-a-digest' if bad_digest else DIGEST);(target/'jib-image.id').write_text(CONFIG)
                if publish_drift:(Path(args[args.index('-f')+1]).parent/'rootfs/app/app.py').write_text('changed after the tests')
        with patch.dict(os.environ,self.env,clear=True),patch.object(m,'current',side_effect=[SOURCE,'0'*40] if moved else lambda:SOURCE),patch.object(m,'command',side_effect=command),patch.object(m,'bounded_step',side_effect=step),patch.object(m,'load',return_value=SimpleNamespace(latest=sdk_latest)),patch('sys.stdout',new=io.StringIO()):m.build_image(environment,'17',self.reader)
        return json.loads((self.root/'.jenkins-tmp/evidence-api-17/release.json').read_text())
    def row(self,number=17,result='SUCCESS',building=False,environment='dev',action='build'):
        return {'number':number,'result':result,'building':building,'actions':[{'parameters':[{'name':'ENVIRONMENT','value':environment},{'name':'ACTION','value':action},{'name':'PERMITTED_SHA','value':DEPLOY}]}]}
    def select(self,proof,rows=None,environment='dev'):
        with patch.dict(os.environ,self.env,clear=True):return m.select_release(environment,rows or [self.row()],lambda url,limit:json.dumps(proof),self.reader)
    def test_both_architectures_publish_only_after_their_own_tests(self):
        proof=self.build();self.assertEqual(['tests','stage','publish'],[x[0] for x in self.steps]);self.assertEqual('PASS',proof['status']);self.assertFalse(proof['runtimeDeployed']);self.assertEqual(SOURCE,proof['processingSource']);self.assertEqual(SDK,proof['contractsSource']);self.assertIn('host.docker.internal:5001/',proof['image']);self.assertEqual('linux/arm64',proof['platform'])
        stage=self.steps[1][1];self.assertEqual('linux/arm64',stage[stage.index('--platform')+1])
        publish=self.steps[2][1];self.assertEqual('mvn',publish[0]);self.assertIn('-Pjib-image',publish);self.assertIn('-Djib.from.platforms=linux/arm64',publish)
        self.assertTrue(next(a for a in publish if a.startswith('-Djib.to.image=')).startswith('-Djib.to.image=192.168.100.102:5001/'))
        self.assertEqual(CONFIG,proof['testedConfigDigest']);self.assertTrue(proof['image'].endswith('@'+DIGEST))
        self.assertFalse(any(x[0] in ['kubectl','launchctl','ssh','docker'] for x in self.commands));self.assertFalse(any(a[0]=='docker' for _,a,_ in self.steps))
        image,meta=self.select(proof);self.assertEqual(proof['image'],image);self.assertEqual(SOURCE,meta['processingSource'])
    def test_production_uses_only_the_production_architecture_and_registry(self):
        proof=self.build('production');self.assertEqual('linux/amd64',proof['platform']);self.assertTrue(proof['image'].startswith('192.168.100.252:5000/'));self.assertEqual(3,len(self.steps))
        self.assertIn('-Djib.from.platforms=linux/amd64',self.steps[2][1]);self.assertEqual('linux/amd64',self.steps[1][1][self.steps[1][1].index('--platform')+1])
    def test_failed_tests_cannot_stage_publish_or_create_a_release(self):
        with self.assertRaisesRegex(ValueError,'test failure'):self.build(test_failure=True)
        self.assertEqual(['tests'],[x[0] for x in self.steps]);self.assertFalse((self.root/'.jenkins-tmp/evidence-api-17/release.json').exists())
    def test_timed_out_tests_stop_before_staging(self):
        with self.assertRaises(m.subprocess.TimeoutExpired):self.build(test_timeout=True)
        self.assertEqual(['tests'],[x[0] for x in self.steps])
    def test_log_limited_tests_stop_before_staging(self):
        with self.assertRaisesRegex(ValueError,'log budget'):self.build(test_overflow=True)
        self.assertEqual(['tests'],[x[0] for x in self.steps])
    def test_staged_tree_changed_after_the_tests_has_no_deployable_receipt(self):
        with self.assertRaisesRegex(ValueError,'differs from the tested'):self.build(publish_drift=True)
        self.assertFalse((self.root/'.jenkins-tmp/evidence-api-17/release.json').exists());receipt=json.loads((self.root/'.jenkins-tmp/evidence-api-17/build-receipt.json').read_text());self.assertEqual('TESTS_PASSED',receipt['status'])
    def test_push_without_an_exact_digest_has_no_deployable_receipt(self):
        with self.assertRaisesRegex(ValueError,'no exact image digest'):self.build(bad_digest=True)
        self.assertFalse((self.root/'.jenkins-tmp/evidence-api-17/release.json').exists())
    def test_missing_same_source_recipe_refuses_before_any_build_step(self):
        with self.assertRaisesRegex(ValueError,'recipe required'):self.build(missing_recipe=True)
        self.assertEqual([],self.steps)
    def test_new_source_before_publication_refuses_without_pushing(self):
        with self.assertRaisesRegex(ValueError,'changed before publication'):self.build(moved=True)
        self.assertEqual(['tests','stage'],[x[0] for x in self.steps])
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
    def test_sparse_source_capture_excludes_unrelated_tracked_links(self):
        import subprocess
        source=self.root/'source';source.mkdir()
        def git(*args):return subprocess.check_output(['git','-c','commit.gpgsign=false','-c','core.hooksPath=/dev/null','-C',str(source),*args],text=True,stderr=subprocess.PIPE).strip()
        git('init','-b','main');git('config','user.name','API Fixture');git('config','user.email','api@example.invalid');(source/'trade-evidence-api').mkdir();(source/'trade-evidence-api/app.py').write_text('verified API source');(source/'unrelated-link').symlink_to('/owned-fixture-missing');git('add','.');git('commit','-m','fixture');commit=git('rev-parse','HEAD');root=self.root/'sparse';root.mkdir()
        with patch.object(m,'PROCESSING',str(source)):checkout=m.prepare_source(root,commit)
        self.assertEqual('verified API source',(checkout/'trade-evidence-api/app.py').read_text());self.assertFalse((checkout/'unrelated-link').is_symlink());self.assertFalse(any(p.is_symlink() for p in checkout.rglob('*')))

    def test_log_file_budget_is_enforced_by_a_real_small_child(self):
        import sys
        with patch('sys.stdout',new=io.StringIO()):m.bounded_step([sys.executable,'-c','print("bounded log")'],self.root,'small',10)
        self.assertEqual('bounded log\n',(self.root/'small.log').read_text())
        with patch('sys.stdout',new=io.StringIO()),self.assertRaisesRegex(ValueError,'log budget exceeded'):
            m.bounded_step([sys.executable,'-c','import os; os.write(1,b"x"*(9<<20))'],self.root,'excess',10)
        self.assertLessEqual((self.root/'excess.log').stat().st_size,8<<20)
    def test_a_child_may_write_a_large_file_while_its_log_stays_bounded(self):
        import sys
        target=self.root/'wheel.bin'
        with patch('sys.stdout',new=io.StringIO()):m.bounded_step([sys.executable,'-c','open(%r,"wb").write(b"x"*(12<<20)); print("done")'%str(target)],self.root,'large',20)
        self.assertEqual(12<<20,target.stat().st_size)
    def test_staging_is_fresh_bounded_and_never_cleans_an_old_build(self):
        root=m.staging('17');(root/'owned-proof').write_text('preserved')
        with self.assertRaises(FileExistsError):m.staging('17')
        self.assertEqual('preserved',(root/'owned-proof').read_text())
    def test_only_the_local_jenkins_build_macs_are_accepted(self):
        for address,allowed in (('192.168.100.102',True),('192.168.100.201',True),('192.168.100.77',False),('192.168.100.2010',False)):
            with self.subTest(address=address),patch.dict(os.environ,{**self.env,'ACTION':'build'},clear=True),patch.object(m.sys,'platform','darwin'),patch.object(m,'command',return_value='inet '+address+' netmask'),patch.object(m,'load') as load:
                if allowed:
                    load.return_value=SimpleNamespace(TARGETS={}) ;
                    with patch.object(m,'build_image') as build:m.main();build.assert_called_once()
                else:
                    with self.assertRaisesRegex(ValueError,'Primary build Mac'):m.main()
    def test_the_test_venv_is_created_outside_the_workspace_and_removed(self):
        self.build();args=self.steps[0][1];venv=Path(args[args.index('--venv')+1])
        self.assertFalse(str(venv).startswith(str(self.root)));self.assertFalse(venv.parent.exists())
    def test_the_image_path_never_invokes_docker(self):
        import re
        self.assertIsNone(re.search(r"'docker'|buildx|docker\s+(build|run|save|image)",(ROOT/'scripts/deploy/cvd-evidence-api-image.py').read_text()))
    def test_without_jenkins_guard_nothing_is_inspected(self):
        with patch.dict(os.environ,{},clear=True),patch.object(m,'command') as command:
            with self.assertRaisesRegex(ValueError,'Guarded Jenkins'):m.main()
            command.assert_not_called()

if __name__=='__main__':unittest.main()
