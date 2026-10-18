"""Real Jenkins image/qualification fixtures and two-agent CAS checks; no runtime effects."""
import copy
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];FIXTURES=ROOT/'tests/fixtures/cvd-image-rollforward'
def load(name,file):
    spec=importlib.util.spec_from_file_location(name,ROOT/file);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
m=load('sp_held','scripts/deploy/cvd-short-premium-held-image.py');w=load('sp_targets','scripts/deploy/cvd-image-rollforward.py')
def fixture(name):return json.loads((FIXTURES/name).read_text())
SOURCE=fixture('sp-dev-lock.json')['source'];SDK=fixture('sp-dev-lock.json')['contractsSource']

class HeldImageTest(unittest.TestCase):
    def setUp(self):
        self.rows=[fixture('sp-dev-build.json'),fixture('sp-production-build.json'),fixture('sp-native11-build.json')]
        self.locks={12:fixture('sp-dev-lock.json'),13:fixture('sp-production-lock.json')};self.receipt=fixture('sp-native11-receipt.json');self.perf=fixture('sp-native11-performance.json');self.reads=[]
        self.reader=SimpleNamespace(TARGETS=w.TARGETS,sdk_main=lambda:SDK)
    def read(self,url,limit):
        self.reads.append(url)
        if url.endswith('/receipt.json'):return json.dumps(self.receipt)
        if url.endswith('/performance.json'):return json.dumps(self.perf)
        for number,lock in self.locks.items():
            if '/'+str(number)+'/artifact/.jenkins-image-lock.json' in url:return json.dumps(lock)
        raise AssertionError('Unexpected artifact read')
    def resolve(self,environment='dev'):
        with patch.dict(os.environ,{'JENKINS_URL':'http://jenkins/'},clear=True),patch.object(m,'source_main',return_value=SOURCE):return m.resolve(environment,self.rows,self.read,self.reader)
    def test_real_jenkins_locks_resolve_both_profiles_against_the_real_private_gate(self):
        for env,number in [('dev',12),('production',13)]:
            with self.subTest(environment=env):
                image,proof=self.resolve(env);self.assertEqual(self.locks[number]['image'],image);self.assertEqual(11,proof['privateLinuxQualificationBuild']);self.assertEqual(SOURCE,proof['processingSource']);self.assertFalse(proof['nativeAgentChanges'])
    def test_latest_failed_or_running_image_never_falls_back(self):
        for result,building in [('FAILURE',False),(None,True)]:
            row=copy.deepcopy(self.rows[0]);row.update(number=14,result=result,building=building);self.rows=[*self.rows[:3],row]
            with self.assertRaisesRegex(ValueError,'never fall back'):self.resolve()
            self.assertEqual([],self.reads)
    def test_newer_failed_private_gate_refuses_even_with_a_published_image(self):
        row=copy.deepcopy(self.rows[2]);row.update(number=14,result='FAILURE');self.rows.append(row)
        with self.assertRaisesRegex(ValueError,'never fall back'):self.resolve()
        self.assertTrue(all(not url.endswith('/receipt.json') for url in self.reads))
    def test_foreign_registry_or_source_or_sdk_bytes_refuse(self):
        original=copy.deepcopy(self.locks[12])
        for key,value in [('source','a'*40),('contractsSource','a'*40),('contractsSha256','b'*64),('image',original['image'].replace('host.docker.internal:5001','192.168.100.252:5000')),('pushed',False)]:
            with self.subTest(field=key):
                self.locks[12]={**original,key:value}
                with self.assertRaises(ValueError):self.resolve()
    def test_current_main_change_refuses_before_qualification(self):
        with patch.dict(os.environ,{'JENKINS_URL':'http://jenkins/'},clear=True),patch.object(m,'source_main',return_value='a'*40),self.assertRaisesRegex(ValueError,'Current Short Premium'):m.resolve('dev',self.rows,self.read,self.reader)
        self.assertEqual(1,len(self.reads))
    def test_new_sdk_main_refuses_before_any_private_qualification_read(self):
        self.reader.sdk_main=lambda:'f'*40
        with self.assertRaisesRegex(ValueError,'Current Short Premium'):self.resolve()
        self.assertEqual(1,len(self.reads))

    def test_wrong_profile_architecture_or_native_effect_flags_refuse(self):
        original=copy.deepcopy(self.rows[0])
        for key,value in [('BUILD_PLATFORM','linux/amd64'),('DEPLOY_NATIVE_A',True),('PUSH_IMAGE',False),('IMAGE_REGISTRY','other.registry'),('IMAGE_TAG','old'),('DEV_IMAGE_TAG','dev')]:
            row=copy.deepcopy(original)
            for entry in row['actions'][0]['parameters']:
                if entry['name']==key:entry['value']=value
            self.rows=[row,*self.rows[1:]]
            with self.subTest(parameter=key),self.assertRaisesRegex(ValueError,'fixed-profile'):self.resolve()
    def test_private_qualification_scope_and_regression_cannot_be_loosened(self):
        for key,value in [('host','Mac'),('agentBChanged',True),('sdkSha256','f'*64)]:
            original=copy.deepcopy(self.receipt);self.receipt[key]=value
            with self.subTest(field=key),self.assertRaises(ValueError):self.resolve()
            self.receipt=original
        self.perf['results']['candidate']['cpuUsec']=self.perf['results']['baseline']['cpuUsec']*2
        with self.assertRaisesRegex(ValueError,'performance gate'):self.resolve()
    def core(self,env='dev'):
        core=load('sp_pair_core','scripts/es4/pinned-image-rollforward.py');w.configure(core,env,'context-tape');m.configure(core,env,self.reader);return core
    def live(self):
        return {'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':'short-premium-agent-service','namespace':'options-edge','uid':'stable','resourceVersion':'123'},'spec':{'replicas':0,'template':{'spec':{'containers':[{'name':'agent-a','image':'old-a','env':[{'name':'SP_BACKEND','value':'ollama'}],'volumeMounts':[{'name':'book','mountPath':'/data'}]},{'name':'agent-b','image':'old-b','args':['run_agent_b.py'],'env':[{'name':'SP_ENABLED','value':'1'}]}],'volumes':[{'name':'book','persistentVolumeClaim':{'claimName':'preserved'}}]}}}}
    def test_both_images_update_in_one_cas_without_touching_book_settings_or_backend(self):
        with patch.dict(os.environ,{},clear=True):
            p=self.core();before=self.live();image=self.locks[12]['image'];ops=p.patch_for('short-premium-agent',before,image)
            self.assertEqual(['test']*7+['replace']*2,[o['op'] for o in ops]);self.assertIn({'op':'test','path':'/spec/replicas','value':0},ops)
            after=copy.deepcopy(before)
            for c in after['spec']['template']['spec']['containers']:c['image']=image
            p.verify('short-premium-agent',before,after,image);self.assertEqual('old-a',before['spec']['template']['spec']['containers'][0]['image'])
            for index in [0,1]:
                tampered=copy.deepcopy(after);tampered['spec']['template']['spec']['containers'][index]['env'].append({'name':'foreign','value':'changed'})
                with self.assertRaisesRegex(ValueError,'non-image'):p.verify('short-premium-agent',before,tampered,image)
            after['spec']['template']['spec']['containers'][1]['image']='old-b'
            with self.assertRaisesRegex(ValueError,'Agent B image'):p.verify('short-premium-agent',before,after,image)
    def test_active_or_reordered_agents_missing_b_and_init_containers_refuse(self):
        with patch.dict(os.environ,{},clear=True):
            p=self.core();image=self.locks[12]['image']
            for fault in ['active','order','missing','init']:
                live=self.live();pod=live['spec']['template']['spec']
                if fault=='active':live['spec']['replicas']=1
                if fault=='order':pod['containers'].reverse()
                if fault=='missing':pod['containers'].pop()
                if fault=='init':pod['initContainers']=[{'name':'foreign'}]
                with self.subTest(fault=fault),self.assertRaises(ValueError):p.patch_for('short-premium-agent',live,image)
    def test_wrapper_dispatches_the_special_pair_without_changing_the_maven_allowlist(self):
        original=w.configure;observed=[]
        def configure(core,environment,service):
            original(core,environment,service)
            def main():
                observed.append(core.patch_for('short-premium-agent',self.live(),self.locks[12]['image']))
            core.main=main
        with patch.dict(os.environ,{'JENKINS_URL':'http://jenkins/','PERMITTED_SHA_GUARD':'PASSED','ENVIRONMENT':'dev','SERVICE':'short-premium-agent'},clear=True),patch.object(w,'configure',side_effect=configure):w.main()
        self.assertEqual(9,len(observed[0]));self.assertEqual(10,len(w.BINDINGS));self.assertNotIn('short-premium-agent',w.BINDINGS)

    def test_source_ref_parser_rejects_wrong_ref_or_duplicate_results(self):
        for text in ['',SOURCE+'\trefs/tags/main\n',SOURCE+'\trefs/heads/main\n'+SOURCE+'\trefs/heads/main\n']:
            with patch.object(m.subprocess,'check_output',return_value=text),self.assertRaisesRegex(ValueError,'exact Short Premium'):m.source_main()

if __name__=='__main__':unittest.main()
