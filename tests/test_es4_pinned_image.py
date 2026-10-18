import copy
import hashlib
import importlib.util
import io
import json
import os
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('pinned',Path(__file__).resolve().parents[1]/'scripts/es4/pinned-image-rollforward.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
SOURCE='a'*40;SDK='b'*40;SHA='c'*64
MANIFEST=b'{"schemaVersion":2}'
IMAGE='192.168.100.252:5000/options-edge-es-cvd:prod-17-aaaaaaaaaaaa@sha256:'+hashlib.sha256(MANIFEST).hexdigest()

def build(number=17,target='cvd-avro-related',result='SUCCESS',building=False):
    fields={'BUILD_TARGET':target,'ENVIRONMENT':'production','PUSH_IMAGES':True,'PERMITTED_SHA':SOURCE,'CONTRACTS_PERMITTED_SHA':SDK}
    return {'number':number,'result':result,'building':building,'actions':[{'parameters':[{'name':k,'value':v} for k,v in fields.items()]}]}

def lock(number=17):
    fields={'FORMAT':'1','SOURCE_REPO':'git@github.com:abhinav-jain09/options-edge-processing.git','BUILD_ID':str(number),'PLATFORM':'linux/amd64','GIT_COMMIT':SOURCE,'CONTRACTS_GIT_COMMIT':SDK,'CONTRACTS_JAR_SHA256':SHA,'TAG':'prod-'+str(number)+'-'+SOURCE[:12],'GUARD_VERSION':'2f6fc2ab7aa51eb8a9e8aa350f2d774cd2fb7a253de2591e43645a19a9886b24'}
    return '\n'.join('OPTIONS_EDGE_IMAGE_LOCK_'+k+'='+v for k,v in fields.items())+'\nES_CVD_IMAGE='+IMAGE.replace(':prod-17-',':prod-'+str(number)+'-')+'\nES_CVD_IMAGE_GIT_COMMIT='+SOURCE+'\n'

def live(replicas=0):
    return {'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':'es-cvd-service','namespace':'options-edge','uid':'known-uid','resourceVersion':'123'},'spec':{'replicas':replicas,'template':{'spec':{'containers':[{'name':'es-cvd','image':'registry/old:prod','env':[{'name':'PASSWORD','value':'do-not-leak'}],'volumeMounts':[{'name':'state','mountPath':'/data'}]}],'volumes':[{'name':'state','persistentVolumeClaim':{'claimName':'preserved'}}]}}}}

class ImagePatchTest(unittest.TestCase):
    def test_only_image_is_written_and_zero_or_active_count_is_fenced(self):
        for replicas in [0,1,3]:
            before=live(replicas);saved=copy.deepcopy(before);ops=p.patch_for('es-cvd',before,IMAGE)
            self.assertEqual([o for o in ops if o['op']!='test'],[{'op':'replace','path':'/spec/template/spec/containers/0/image','value':IMAGE}])
            tests={o['path']:o['value'] for o in ops if o['op']=='test'}
            for key in ['/spec/replicas','/metadata/uid','/metadata/resourceVersion']:self.assertIn(key,tests)
            self.assertEqual(tests['/spec/replicas'],replicas);self.assertEqual(tests['/metadata/uid'],'known-uid');self.assertEqual(tests['/metadata/resourceVersion'],'123')
            self.assertEqual(before,saved);self.assertNotIn('do-not-leak',json.dumps(ops))
    def test_wrong_workload_and_ambiguous_or_missing_count_refuse(self):
        for path,value in [('name','other'),('namespace','other'),('uid',''),('resourceVersion','bad')]:
            value_live=live();value_live['metadata'][path]=value
            with self.subTest(path=path),self.assertRaises(ValueError):p.patch_for('es-cvd',value_live,IMAGE)
        for replicas in [None,True,'0',-1]:
            with self.subTest(replicas=replicas),self.assertRaisesRegex(ValueError,'replica count'):p.patch_for('es-cvd',live(replicas),IMAGE)
    def test_unknown_sidecars_and_container_identity_refuse(self):
        for change in ['sidecar','init','name']:
            d=live();pod=d['spec']['template']['spec']
            if change=='sidecar':pod['containers'].append({'name':'extra'})
            elif change=='init':pod['initContainers']=[{'name':'extra'}]
            else:pod['containers'][0]['name']='other'
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'single application'):p.patch_for('es-cvd',d,IMAGE)
    def test_wrong_repository_registry_or_mutable_image_refuse(self):
        for value in [IMAGE.replace('192.168.100.252','elsewhere'),IMAGE.replace('options-edge-es-cvd','options-edge-context-tape'),IMAGE.split('@')[0],IMAGE+'garbage']:
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'repository and digest'):p.image_parts('es-cvd',value)
        with self.assertRaisesRegex(ValueError,'allowlist'):p.image_parts('es-feed',IMAGE)
    def test_post_update_verifies_uid_replica_count_and_exact_image(self):
        before=live();after=copy.deepcopy(before);after['spec']['template']['spec']['containers'][0]['image']=IMAGE
        p.verify('es-cvd',before,after,IMAGE)
        for change in ['uid','replicas','image']:
            bad=copy.deepcopy(after)
            if change=='uid':bad['metadata']['uid']='other'
            elif change=='replicas':bad['spec']['replicas']=1
            else:bad['spec']['template']['spec']['containers'][0]['image']='other'
            with self.subTest(change=change),self.assertRaises(ValueError):p.verify('es-cvd',before,bad,IMAGE)

class LatestQualificationTest(unittest.TestCase):
    def test_latest_relevant_build_ignores_other_services_but_never_falls_back(self):
        rows=[build(19,target='options-edge-other',result='FAILURE'),build(17)]
        image,proof=p.qualified_image('es-cvd',rows,lambda n:lock(n));self.assertEqual(image,IMAGE);self.assertEqual(proof['processingBuild'],17)
        for result,building in [('FAILURE',False),(None,True),('ABORTED',False)]:
            with self.subTest(result=result),self.assertRaisesRegex(ValueError,'latest relevant'):p.qualified_image('es-cvd',[build(18,result=result,building=building),build(17)],lambda n:lock(n))
    def test_wrong_build_platform_source_sdk_guard_and_duplicate_lock_refuse(self):
        base=lock()
        for old,new in [('BUILD_ID=17','BUILD_ID=18'),('PLATFORM=linux/amd64','PLATFORM=linux/arm64'),('GIT_COMMIT='+SOURCE,'GIT_COMMIT='+'d'*40),('CONTRACTS_GIT_COMMIT='+SDK,'CONTRACTS_GIT_COMMIT='+'e'*40),('CONTRACTS_JAR_SHA256='+SHA,'CONTRACTS_JAR_SHA256=x'),('SOURCE_REPO=git@github.com','SOURCE_REPO=git@elsewhere'),('GUARD_VERSION=2f6','GUARD_VERSION=000')]:
            with self.subTest(old=old),self.assertRaises(ValueError):p.qualified_image('es-cvd',[build()],lambda n:base.replace(old,new))
        with self.assertRaisesRegex(ValueError,'duplicate'):p.qualified_image('es-cvd',[build()],lambda n:base+'ES_CVD_IMAGE='+IMAGE+'\n')
    def test_stale_tag_and_foreign_service_entry_source_refuse(self):
        for old,new,message in [('TAG=prod-17-','TAG=prod-16-','build and source tag'),
                                ('ES_CVD_IMAGE_GIT_COMMIT='+SOURCE,'ES_CVD_IMAGE_GIT_COMMIT='+'d'*40,'service image source')]:
            with self.subTest(old=old),self.assertRaisesRegex(ValueError,message):p.qualified_image('es-cvd',[build()],lambda n:lock().replace(old,new))
    def test_build_metadata_count_and_number_are_bounded(self):
        with self.assertRaisesRegex(ValueError,'bounded Jenkins'):p.qualified_image('es-cvd',[build()]*65,lambda n:lock())
        for number in [True,'17',0]:
            row=build();row['number']=number
            with self.subTest(number=number),self.assertRaisesRegex(ValueError,'positive Jenkins'):p.qualified_image('es-cvd',[row],lambda n:lock())
    def test_no_eligible_build_and_unpublished_build_refuse(self):
        for rows in [[],[build(target='options-edge-other')]]:
            with self.assertRaisesRegex(ValueError,'no recent'):p.qualified_image('es-cvd',rows,lambda n:lock())
        row=build();row['actions'][0]['parameters'][2]['value']=False
        with self.assertRaisesRegex(ValueError,'latest relevant'):p.qualified_image('es-cvd',[row],lambda n:lock())
    def test_close_direction_needs_its_own_image_target(self):
        with self.assertRaisesRegex(ValueError,'no recent'):p.qualified_image('close-direction',[build()],lambda n:lock())
    def test_no_or_ambiguous_matching_image_and_bounded_lock_refuse(self):
        for text in [lock().replace('options-edge-es-cvd','options-edge-context-tape'),lock()+'EXTRA_IMAGE='+IMAGE+'\nEXTRA_IMAGE_GIT_COMMIT='+SOURCE+'\n']:
            with self.assertRaisesRegex(ValueError,'one exact'):p.qualified_image('es-cvd',[build()],lambda n:text)
        with self.assertRaisesRegex(ValueError,'256KiB'):p.qualified_image('es-cvd',[build()],lambda n:' '*262145)

class MainFlowTest(unittest.TestCase):
    def run_case(self,dry,replicas=0,namespace='expected-ns',mutate_admission=False,closure=True,
                 manifest=MANIFEST,identity='system:serviceaccount:options-edge:jenkins-deployer',
                 main=SOURCE,fail_rollout=False,extra_env=None):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);previous=Path.cwd();os.chdir(tmp.name);self.addCleanup(os.chdir,previous)
        before=live(replicas);after=copy.deepcopy(before);after['spec']['template']['spec']['containers'][0]['image']=IMAGE;calls=[];reads=0
        def kube(*args,**kwargs):
            nonlocal reads
            calls.append(args)
            if args[:2]==('auth','whoami'):return json.dumps({'status':{'userInfo':{'username':identity}}})
            if args[:2]==('get','namespace'):return json.dumps({'metadata':{'uid':namespace}})
            if args[0]=='get':reads+=1;return json.dumps(before if reads==1 else after)
            if args[0]=='patch':
                if '--dry-run=server' not in args:
                    self.assertEqual(json.loads(self.receipt_path.read_text())['phase'],'apply-requested')
                admitted=copy.deepcopy(after)
                if mutate_admission == 'env':admitted['spec']['template']['spec']['containers'][0]['env'][0]['value']='changed'
                elif mutate_admission:admitted['spec']['replicas']=1
                return json.dumps(admitted)
            if args[0]=='rollout' and fail_rollout:raise subprocess.TimeoutExpired(['kubectl','rollout'],620)
            return 'ok\n'
        def url(url,limit):return lock() if '/artifact/' in url else json.dumps({'builds':[build()]})
        env={'JENKINS_URL':'http://jenkins/','PERMITTED_SHA_GUARD':'PASSED','PERMITTED_SHA':SOURCE,'SERVICE':'es-cvd','DEPLOY_DRY_RUN':str(dry).lower(),'ES4_NS_UID':'expected-ns','BUILD_NUMBER':'90','CVD_READER_CLOSURE_CONFIRMED':str(closure).lower()}
        env.update(extra_env or {})
        self.calls=calls
        self.receipt_path=Path(tmp.name)/'.jenkins-tmp/es4-image-90/receipt.json'
        with patch.dict(os.environ,env,clear=True),patch.object(p,'kube',side_effect=kube),patch.object(p,'read_url',side_effect=url),patch.object(p,'processing_main',return_value=main),patch.object(p,'open_direct',return_value=io.BytesIO(manifest)),patch('sys.stdout',new=io.StringIO()):p.main()
        receipt=json.loads(Path('.jenkins-tmp/es4-image-90/receipt.json').read_text());return calls,receipt
    def test_dry_run_never_writes_or_rolls_a_workload(self):
        calls,receipt=self.run_case(True)
        self.assertEqual(len([c for c in calls if c[0]=='patch']),1);self.assertTrue(all('--dry-run=server' in c for c in calls if c[0]=='patch'));self.assertFalse(any(c[0]=='rollout' for c in calls));self.assertTrue(receipt['dryRun'])
    def test_zero_hold_skips_readiness_and_active_count_waits(self):
        for replicas in [0,2]:
            calls,receipt=self.run_case(False,replicas)
            writes=[c for c in calls if c[0]=='patch' and '--dry-run=server' not in c]
            self.assertEqual(len(writes),1);self.assertIn('--patch',writes[0]);self.assertNotIn('--patch-file',writes[0])
            self.assertEqual(any(c[0]=='rollout' for c in calls),replicas>0);self.assertEqual(receipt['replicas'],replicas);self.assertEqual(receipt['runtimeReadinessVerified'],replicas>0)
    def test_wrong_namespace_refuses_before_any_patch(self):
        with self.assertRaisesRegex(ValueError,'namespace identity'):self.run_case(False,namespace='wrong')
        self.assertFalse(any(c[0]=='patch' for c in self.calls))
    def test_admission_replica_mutation_refuses_before_real_patch(self):
        with self.assertRaisesRegex(ValueError,'replica hold changed'):self.run_case(False,mutate_admission=True)
        self.assertFalse(any(c[0]=='patch' and '--dry-run=server' not in c for c in self.calls))

    def test_active_update_needs_explicit_closure_but_dry_run_and_zero_do_not(self):
        with self.assertRaisesRegex(ValueError,'active workload requires'):self.run_case(False,replicas=1,closure=False)
        self.assertFalse(any(c[0]=='patch' for c in self.calls))
        for dry,replicas in [(True,1),(False,0)]:
            calls,receipt=self.run_case(dry,replicas=replicas,closure=False)
            self.assertFalse(receipt['readerClosureAcknowledged'])
    def test_failed_rollout_preserves_evidence_without_claiming_readiness(self):
        with self.assertRaises(subprocess.TimeoutExpired):self.run_case(False,replicas=1,fail_rollout=True)
        receipt=json.loads(self.receipt_path.read_text())
        self.assertEqual(receipt['phase'],'apply-returned');self.assertEqual(receipt['failureType'],'TimeoutExpired')
        self.assertFalse(receipt['runtimeReadinessVerified'])
        patch_payload=self.receipt_path.with_name('patch.json').read_bytes()
        self.assertEqual(hashlib.sha256(patch_payload).hexdigest(),receipt['patchSha256'])
        self.assertEqual(len([c for c in self.calls if c[0]=='patch' and '--dry-run=server' not in c]),1)
    def test_env_admission_drift_refuses_before_real_patch_and_keeps_receipt(self):
        with self.assertRaisesRegex(ValueError,'non-image workload'):self.run_case(False,mutate_admission='env')
        self.assertFalse(any(c[0]=='patch' and '--dry-run=server' not in c for c in self.calls))
        receipt=json.loads(self.receipt_path.read_text());self.assertEqual(receipt['phase'],'patch-prepared')
    def test_stale_processing_main_refuses_before_cluster_calls(self):
        with patch.object(p,'component_source_proof',side_effect=ValueError('CVD source changed')),self.assertRaisesRegex(ValueError,'CVD source changed'):self.run_case(True,main='f'*40)
        self.assertEqual(self.calls,[])
    def test_unrelated_main_changes_require_explicit_exact_component_source_proof(self):
        proof={'qualifiedSource':SOURCE,'currentMainSource':'f'*40,'componentTrees':{'context-tape':'verified-identical-tree'}}
        with patch.object(p,'component_source_proof',return_value=proof) as verifier:
            calls,receipt=self.run_case(True,main='f'*40)
        verifier.assert_called_once_with(SOURCE,'f'*40);self.assertEqual(proof,receipt['componentSourceProof']);self.assertTrue(all('--dry-run=server' in c for c in calls if c[0]=='patch'))
    def test_manifest_digest_mismatch_or_oversize_refuses_before_patch(self):
        for manifest in [MANIFEST+b' ',b'x'*((2<<20)+1)]:
            with self.subTest(size=len(manifest)),self.assertRaisesRegex(ValueError,'manifest digest mismatch'):self.run_case(True,manifest=manifest)
            self.assertFalse(any(c[0]=='patch' for c in self.calls))
    def test_wrong_identity_or_invalid_dry_state_or_manual_image_refuses(self):
        for kwargs,message in [({'identity':'other'},'deployer identity'),
                                ({'extra_env':{'DEPLOY_DRY_RUN':'yes'}},'dry-run state'),
                                ({'extra_env':{'REQUIRED_IMAGE':IMAGE}},'manual image selectors')]:
            with self.subTest(kwargs=kwargs),self.assertRaisesRegex(ValueError,message):self.run_case(True,**kwargs)
            self.assertFalse(any(c[0]=='patch' for c in self.calls))

class SourceFenceTest(unittest.TestCase):
    def test_processing_main_requires_exact_branch_ref(self):
        for output in [SOURCE+'\trefs/heads/main\n',SOURCE+'\trefs/tags/main\n','',SOURCE+'\trefs/heads/main\n'+SDK+'\trefs/heads/main\n']:
            with patch.object(p.subprocess,'run',return_value=subprocess.CompletedProcess([],0,output,'')):
                if output==SOURCE+'\trefs/heads/main\n':self.assertEqual(p.processing_main(),SOURCE)
                else:
                    with self.assertRaisesRegex(ValueError,'exact processing main'):p.processing_main()
    def test_latest_qualification_uses_build_number_not_array_order(self):
        image,proof=p.qualified_image('es-cvd',[build(17),build(19),build(18)],lambda n:lock(n))
        self.assertEqual(proof['processingBuild'],19);self.assertIn(':prod-19-',image)
    def test_close_direction_uses_its_own_successful_image(self):
        own=lock().replace('options-edge-es-cvd','options-edge-close-direction')
        image,proof=p.qualified_image('close-direction',[build(target='options-edge-close-direction')],lambda n:own)
        self.assertIn('/options-edge-close-direction:',image)

if __name__=='__main__':unittest.main()
