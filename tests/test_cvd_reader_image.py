"""Actual production image-selection/CAS core under both fixed environment mappings."""
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from tests.test_es4_pinned_image import build, lock, SOURCE, SDK
ROOT=Path(__file__).resolve().parents[1]
def load(name,file):
    s=importlib.util.spec_from_file_location(name,ROOT/file);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
w=load('reader_wrapper','scripts/deploy/cvd-image-rollforward.py')

class ReaderImageTest(unittest.TestCase):
    def core(self,environment,service='context-tape'):
        p=load('reader_core','scripts/es4/pinned-image-rollforward.py');w.configure(p,environment,service);return p
    def test_resolved_dev_arm_image_is_bound_and_foreign_production_refuses(self):
        with patch.dict(os.environ,{},clear=True),patch.object(w,'sdk_main',return_value=SDK):
            p=self.core('dev');row=build(target='options-edge-context-tape')
            for item in row['actions'][0]['parameters']:
                if item['name']=='ENVIRONMENT':item['value']='dev'
            text=lock().replace('options-edge-es-cvd','options-edge-context-tape').replace('192.168.100.252:5000','host.docker.internal:5001').replace('linux/amd64','linux/arm64').replace('prod-','')
            image,proof=p.qualified_image('context-tape',[row],lambda n:text)
            self.assertTrue(image.startswith('host.docker.internal:5001/options-edge-context-tape:'))
            self.assertEqual('/Users/abhinav/.kube/jenkins-deployer-dev.kubeconfig',os.environ['ES4_KUBECONFIG'])
            self.assertEqual('55dcf4ed-9a04-40c4-aeba-039e998c6241',os.environ['ES4_NS_UID']);self.assertEqual('http://127.0.0.1:5001',p.REGISTRY_HTTP)
            with self.assertRaisesRegex(ValueError,'platform'):p.qualified_image('context-tape',[row],lambda n:text.replace('linux/arm64','linux/amd64'))
            with self.assertRaisesRegex(ValueError,'one exact'):p.qualified_image('context-tape',[row],lambda n:text.replace('host.docker.internal:5001','192.168.100.252:5000'))
            with self.assertRaisesRegex(ValueError,'no recent'):p.qualified_image('context-tape',[build()],lambda n:text)
    def test_latest_running_build_never_falls_back_and_sdk_main_must_match(self):
        with patch.dict(os.environ,{},clear=True),patch.object(w,'sdk_main',return_value=SDK):
            p=self.core('production');rows=[build(target='options-edge-context-tape'),build(20,target='options-edge-context-tape',result=None,building=True)]
            text=lock().replace('options-edge-es-cvd','options-edge-context-tape')
            with self.assertRaisesRegex(ValueError,'latest relevant'):p.qualified_image('context-tape',rows,lambda n:text)
            with patch.object(w,'sdk_main',return_value='f'*40),self.assertRaisesRegex(ValueError,'current SDK'):p.qualified_image('context-tape',rows[:1],lambda n:text)
    def test_actual_existing_reader_inventory_is_fenced_without_starting_holds(self):
        import json
        census=json.loads((ROOT/'tests/fixtures/cvd-image-rollforward/deployments-oct18.json').read_text())
        with patch.dict(os.environ,{},clear=True):
            for environment in census:
                available={row['name']:row for row in environment['deployments']}
                selected=[key for key in w.BINDINGS if environment['environment']=='dev' or key!='level-absorption']
                self.assertEqual(10 if environment['environment']=='dev' else 9,len(selected))
                for service in selected:
                    p=self.core(environment['environment'],service);name,container,repo=p.SERVICES[service]
                    self.assertIn(name,available);row=available[name];self.assertIn(container,row['images'])
                    image=p.REGISTRY+'/'+repo+'@sha256:'+'d'*64
                    live={'kind':'Deployment','apiVersion':'apps/v1','metadata':{'name':row['name'],'namespace':'options-edge','uid':row['uid'],'resourceVersion':'123'},'spec':{'replicas':row['replicas'],'template':{'spec':{'containers':[{'name':container,'image':row['images'][container]}]}}}}
                    operations=p.patch_for(service,live,image)
                    self.assertEqual([{'op':'replace','path':'/spec/template/spec/containers/0/image','value':image}],[x for x in operations if x['op']!='test'])
                    self.assertIn({'op':'test','path':'/spec/replicas','value':row['replicas']},operations)
    def test_unknown_targets_services_and_forged_main_refs_refuse(self):
        for environment,service in [('experiment','context-tape'),('dev','indicator-service'),('production','es-cvd'),('production','level-absorption')]:
            with self.assertRaisesRegex(ValueError,'Known dev/prod'):self.core(environment,service)
        for stdout in ['',SDK+'\trefs/tags/main\n',SDK+'\trefs/heads/main\n'+SOURCE+'\trefs/heads/main\n']:
            with patch.object(w.subprocess,'run',return_value=SimpleNamespace(stdout=stdout)),self.assertRaisesRegex(ValueError,'exact SDK'):w.sdk_main()
    def test_literal_successful_jenkins_locks_cover_real_dev_and_prod_tag_shapes(self):
        import json
        directory=ROOT/'tests/fixtures/cvd-image-rollforward'
        for name,environment,service in [('dev-context-tape','dev','context-tape'),('production-close-direction','production','close-direction')]:
            row=json.loads((directory/(name+'.json')).read_text());text=(directory/(name+'.env')).read_text()
            fields=dict(line.split('=',1) for line in text.splitlines() if line);sdk=fields['OPTIONS_EDGE_IMAGE_LOCK_CONTRACTS_GIT_COMMIT']
            with self.subTest(environment=environment),patch.dict(os.environ,{},clear=True),patch.object(w,'sdk_main',return_value=sdk):
                core=self.core(environment,service);image,proof=core.qualified_image(service,[row],lambda number:text)
                self.assertEqual(row['number'],proof['processingBuild']);self.assertIn(fields['OPTIONS_EDGE_IMAGE_LOCK_TAG']+'@',image)
                if environment=='dev':self.assertEqual('1901-a33fa6618b4c',fields['OPTIONS_EDGE_IMAGE_LOCK_TAG'])

    def test_unguarded_wrapper_cannot_load_or_call_core(self):
        with patch.dict(os.environ,{},clear=True),patch.object(w.importlib.util,'spec_from_file_location') as loader:
            with self.assertRaisesRegex(ValueError,'Guarded Jenkins'):w.main()
            loader.assert_not_called()
