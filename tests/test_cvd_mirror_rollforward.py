"""Bounded mirror source/group identity and real handoff orchestration; effects are stubbed."""
import ast
import shlex
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('mirror_roll',ROOT/'scripts/ops/cvd-mirror-rollforward.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class MirrorTest(unittest.TestCase):
    def setUp(self):
        self.base=Path(tempfile.mkdtemp(prefix='cvd-mirror-handoff-'))  # Preserve owned fixtures; no recursive cleanup.
        self.owner=self.base/'owner';self.owner.mkdir();self.previous=Path.cwd();os.chdir(self.base);self.addCleanup(os.chdir,self.previous)
        self.topic='es.futures.cvd.bars';self.group='es-cvd-mirror-127.0.0.1-'+self.topic
        self.slug='127-0-0-1-19092-es-futures-cvd-bars';self.label='com.optionsedge.es-cvd-mirror-'+self.slug
        self.directory=self.owner/('oe-ops/es-cvd-mirror-'+self.slug);self.directory.mkdir(parents=True)
        (self.directory/'run-mirror.sh').write_text('#!/bin/bash\nexec kafka-mirror-maker fixture\n')
        (self.directory/'consumer.properties').write_text('bootstrap.servers=192.168.100.4:9092\ngroup.id='+self.group+'\nauto.offset.reset=earliest\n')
        (self.directory/'producer.properties').write_text('bootstrap.servers=127.0.0.1:19092\n')
        self.plist=self.owner/('Library/LaunchAgents/'+self.label+'.plist');self.plist.parent.mkdir(parents=True)
        self.definition={'Label':self.label,'ProgramArguments':[str(self.directory/'run-mirror.sh')],'KeepAlive':True,'EnvironmentVariables':{'PATH':'unchanged'},'StandardOutPath':'unchanged.log'}
        self.plist.write_bytes(plistlib.dumps(self.definition));self.raw=self.plist.read_bytes();self.state={'loaded':True,'pid':900001};self.effects=[]
        self.env={'JENKINS_URL':'http://jenkins/','PERMITTED_SHA_GUARD':'PASSED','PERMITTED_SHA':'a'*40,'BUILD_NUMBER':'17','SOURCE_ES_CLUSTER_IP':'192.168.100.4','SOURCE_PORT':'9092','TARGET_SPX_CLUSTER_IP':'127.0.0.1','TARGET_PORT':'19092','TOPIC':self.topic,'DEPLOY_DRY_RUN':'false','CVD_TARGET_READERS_READY':'true'}
    def execute(self,dry=False,held=False,smoke_failure=False,bootstrap_failure=False,tail_records=25,readiness_failure=False,process_commands=None):
        self.env['DEPLOY_DRY_RUN']=str(dry).lower()
        if held:self.state.update(loaded=False,pid=None)
        def release(root):
            (root/'canonical-cvd-mirror.jar').write_bytes(b'qualified fixture bytes');(root/'release.json').write_text('{}')
            return {'jar_sha256':m.digest(root/'canonical-cvd-mirror.jar')}
        def run(*args,**kwargs):
            if args[0]=='/sbin/ifconfig':return 'inet 192.168.100.102 netmask fixture'
            if args[:2]==('launchctl','bootout'):self.effects.append('bootout');self.state.update(loaded=False,pid=None)
            if args[:2]==('launchctl','bootstrap'):
                self.effects.append('bootstrap')
                if bootstrap_failure:raise OSError('bootstrap failed fixture')
                self.state.update(loaded=True,pid=900002)
            if args[0]=='ps' and self.state['pid']==900001:return str(self.directory)
            if args[0]=='ps':
                if process_commands:
                    value=process_commands[0]
                    if len(process_commands)>1:process_commands.pop(0)
                    return value.replace('{runner}',str(self.owner/'oe-ops/cvd-mirror-releases/build-17/run-mirror.sh')).replace('{jar}',str(self.owner/'oe-ops/cvd-mirror-releases/build-17/canonical-cvd-mirror.jar'))
                return str(self.owner/'oe-ops/cvd-mirror-releases/build-17/canonical-cvd-mirror.jar')
            if args[0]==str(m.JAVA):
                if readiness_failure:raise m.subprocess.CalledProcessError(2,list(args),stderr='configured readiness failure')
                return 'CANONICAL_MIRROR_READY records=0\n'
            return ''
        with patch.dict(os.environ,self.env,clear=True),patch.object(m,'OWNER',self.owner),patch.object(m.Path,'home',return_value=self.owner),patch.object(m.sys,'platform','darwin'),patch.object(m,'unit_state',side_effect=lambda label:dict(self.state)),patch.object(m,'release',side_effect=release),patch.object(m,'verify_inputs',return_value={0:17}),patch.object(m,'committed_offsets',return_value={0:19}),patch.object(m,'linux_probe',return_value={'host':'Linux CI es4','records':tail_records}),patch.object(m,'run',side_effect=run),patch.object(m.subprocess,'run',return_value=SimpleNamespace(returncode=1,stderr='other error' if smoke_failure else 'Usage: CanonicalTopicMirror consumer producer topic')),patch.object(m.os,'kill',side_effect=ProcessLookupError):
            m.main()
        return json.loads((self.base/'.jenkins-tmp/cvd-mirror-17/receipt.json').read_text())
    def test_actual_handoff_keeps_group_properties_and_all_other_plist_fields(self):
        a=(self.directory/'consumer.properties').read_bytes();b=(self.directory/'producer.properties').read_bytes();proof=self.execute()
        self.assertEqual(['bootout','bootstrap'],self.effects);self.assertTrue(proof['installed']);self.assertTrue(proof['runtimeReady']);self.assertEqual(self.group,proof['group']);self.assertEqual({'0':17},proof['committedOffsets'])
        after=plistlib.loads(self.plist.read_bytes());expected=dict(self.definition);expected['ProgramArguments']=after['ProgramArguments'];self.assertEqual(expected,after)
        self.assertEqual(a,(self.directory/'consumer.properties').read_bytes());self.assertEqual(b,(self.directory/'producer.properties').read_bytes())
        script=Path(after['ProgramArguments'][1]).read_text();self.assertIn('SOURCE_SCHEMA_REGISTRY_URL=http://192.168.100.4:8081',script);self.assertIn('TARGET_SCHEMA_REGISTRY_URL=http://127.0.0.1:8082',script);self.assertNotIn('192.168.100.201',script)
    def test_exact_managed_shell_can_exec_into_the_verified_java_process(self):
        with patch.object(m.time,'sleep'):
            proof=self.execute(process_commands=['/bin/bash {runner}','{jar}'])
        self.assertTrue(proof['runtimeReady']);self.assertEqual(900002,proof['newPid'])

    def test_managed_shell_that_never_execs_times_out(self):
        clock=[0]
        with patch.object(m.time,'monotonic',side_effect=lambda:clock[0]),patch.object(m.time,'sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+61)):
            with self.assertRaisesRegex(TimeoutError,'managed launcher has not execed Java'):
                self.execute(process_commands=['/bin/bash {runner}'])
        proof=json.loads((self.base/'.jenkins-tmp/cvd-mirror-17/receipt.json').read_text())
        self.assertFalse(proof['runtimeReady']);self.assertEqual('launcher-updated',proof['phase'])

    def test_other_shells_or_extra_launcher_arguments_are_refused(self):
        for command in ['/bin/bash /foreign/run-mirror.sh','/bin/bash {runner} extra']:
            with self.subTest(command=command):
                # Each refusal needs a fresh build-owned fixture directory.
                case=MirrorTest();case.setUp()
                try:
                    with self.assertRaisesRegex(ValueError,'Unexpected new process'):
                        case.execute(process_commands=[command])
                finally:case.doCleanups()

    def test_dry_run_never_changes_launcher_or_runtime(self):
        self.env['CVD_TARGET_READERS_READY']='false'
        proof=self.execute(dry=True);self.assertEqual([],self.effects);self.assertEqual(self.raw,self.plist.read_bytes());self.assertFalse(proof['installed']);self.assertFalse(proof['runtimeReady']);self.assertFalse((self.owner/'oe-ops/cvd-mirror-releases').exists())
    def test_empty_tail_is_reported_in_dry_run_but_refuses_launcher_upgrade(self):
        with self.assertRaisesRegex(ValueError,'Nonempty actual retained-tail'):self.execute(tail_records=0)
        self.assertEqual([],self.effects);self.assertEqual(self.raw,self.plist.read_bytes())

    def test_stopped_mirror_remains_stopped_after_launcher_upgrade(self):
        proof=self.execute(held=True);self.assertEqual([],self.effects);self.assertTrue(proof['heldStopped']);self.assertTrue(proof['installed']);self.assertFalse(proof['runtimeReady']);self.assertIsNone(self.state['pid'])
    def test_smoke_failure_keeps_old_process_and_plist(self):
        with self.assertRaisesRegex(ValueError,'smoke failed'):self.execute(smoke_failure=True)
        self.assertEqual([],self.effects);self.assertEqual(self.raw,self.plist.read_bytes());self.assertEqual(900001,self.state['pid'])
    def test_failed_new_start_never_restores_old_raw_byte_mirror(self):
        with self.assertRaisesRegex(OSError,'bootstrap failed'):self.execute(bootstrap_failure=True)
        self.assertEqual(['bootout','bootstrap'],self.effects);self.assertNotEqual(self.raw,self.plist.read_bytes());proof=json.loads((self.base/'.jenkins-tmp/cvd-mirror-17/receipt.json').read_text());self.assertFalse(proof['installed']);self.assertEqual('launcher-updated',proof['phase'])
    def test_active_mirror_refuses_before_any_output_without_reader_closure(self):
        self.env['CVD_TARGET_READERS_READY']='false'
        with self.assertRaisesRegex(ValueError,'active destination readers'):self.execute()
        self.assertEqual([],self.effects);self.assertEqual(self.raw,self.plist.read_bytes())
    def test_loaded_without_pid_is_not_a_stopped_hold(self):
        self.state.update(loaded=True,pid=None)
        with self.assertRaisesRegex(ValueError,'Loaded mirror without PID'):self.execute()
        self.assertEqual([],self.effects);self.assertEqual(self.raw,self.plist.read_bytes())
    def test_stopped_launcher_upgrade_also_requires_future_reader_compatibility(self):
        self.env['CVD_TARGET_READERS_READY']='false'
        with self.assertRaisesRegex(ValueError,'active destination readers'):self.execute(held=True)
        self.assertEqual([],self.effects);self.assertEqual(self.raw,self.plist.read_bytes())
    def test_launchctl_missing_service_is_distinct_from_waiting_or_inspection_error(self):
        with patch.object(m.subprocess,'run',return_value=SimpleNamespace(returncode=0,stdout='state = waiting\n',stderr='')):self.assertEqual({'loaded':True,'pid':None},m.unit_state(self.label))
        with patch.object(m.subprocess,'run',return_value=SimpleNamespace(returncode=113,stdout='',stderr='Could not find service "'+self.label+'" in domain for user gui: 501')):self.assertEqual({'loaded':False,'pid':None},m.unit_state(self.label))
        with patch.object(m.subprocess,'run',return_value=SimpleNamespace(returncode=1,stdout='',stderr='Could not inspect domain')),self.assertRaisesRegex(ValueError,'inspect existing'):m.unit_state(self.label)
    def test_foreign_existing_group_refuses_before_any_effect(self):
        (self.directory/'consumer.properties').write_text('bootstrap.servers=192.168.100.4:9092\ngroup.id=different\n')
        with self.assertRaisesRegex(ValueError,'group identity'):self.execute()
        self.assertEqual([],self.effects);self.assertEqual(self.raw,self.plist.read_bytes())
    def test_offsets_require_each_real_committed_coordinate(self):
        line=self.group+' '+self.topic+' 0 17 19 2 consumer /192.168.100.102 client'
        self.assertEqual({0:17},m.offsets(line,self.group,self.topic))
        for text in ['',line.replace(' 17 ',' - '),line.replace(' 19 ',' 16 '),line+'\n'+line,line.replace(' 2 consumer',' 9 consumer')]:
            with self.assertRaises(ValueError):m.offsets(text,self.group,self.topic)
    def test_properties_duplicate_and_escaped_spelling_refuse(self):
        path=self.base/'properties'
        for text in ['group.id=a\ngroup.id=b\n','group\\.id=a\n','unsupported\n']:
            path.write_text(text)
            with self.assertRaises(ValueError):m.properties(path)
    def test_latest_success_is_ordered_and_never_falls_back(self):
        def row(number,result='SUCCESS',building=False):
            return {'number':number,'result':result,'building':building,'actions':[{'parameters':[{'name':'BUILD_TARGET','value':'cvd-avro-related'},{'name':'ENVIRONMENT','value':'production'},{'name':'PUSH_IMAGES','value':True}]}]}
        self.assertEqual(19,m.latest_build([row(18),row(19),row(17)])[0])
        for rows in [[],[row(19,'FAILURE'),row(18)],[row(19,None,True),row(18)]]:
            with self.assertRaises(ValueError):m.latest_build(rows)
    def test_linux_probe_uses_only_es4_and_rewrites_dev_registry_for_remote_reachability(self):
        root=self.base/'probe';root.mkdir();(root/'canonical-cvd-mirror.jar').write_bytes(b'qualified bytes');calls=[]
        def run(*args,**kwargs):
            calls.append(args)
            if args[0]=='scp':return ''
            if args[-1].startswith('python3 -c '):return json.dumps({'directory':'/home/abhinav/ci/cvd-mirror-preflight/run-17','uid':1000,'gid':1000})
            return 'CANONICAL_MIRROR_PREFLIGHT_PASS records=25 bytes=4096\n'
        with patch.dict(os.environ,self.env,clear=True),patch.object(m,'run',side_effect=run),patch.object(m,'read',return_value=b'{"id":2012}'):
            proof=m.linux_probe(root,self.topic,'http://127.0.0.1:8082')
            remote = ast.parse(shlex.split(calls[-1][-1])[-1])
            java = ast.literal_eval(remote.body[-1].value.args[0])
            self.assertEqual('/usr/lib/jvm/java-21-openjdk/bin/java', java[0])
            self.assertEqual('21', java[java.index('--source') + 1])
        self.assertEqual('Linux CI es4',proof['host']);self.assertEqual(25,proof['records']);self.assertEqual(4096,proof['bytes'])
        command=calls[-1][-1];self.assertIn('--pipe',command);self.assertIn('MemoryMax=512M',command);self.assertIn('MemorySwapMax=0',command);self.assertIn('RuntimeMaxSec=90',command);self.assertIn('http://192.168.100.102:8082',command)
        for call in calls:self.assertNotIn('192.168.100.201',' '.join(call))

    def test_linux_preflight_refusal_keeps_bounded_specific_diagnostics(self):
        root=self.base/'probe';root.mkdir();(root/'canonical-cvd-mirror.jar').write_bytes(b'qualified bytes')
        marker='CANONICAL_MIRROR_PREFLIGHT_REFUSED InvalidAvro topic=es.futures.cvd.bars partition=0 offset=17'
        def run(*args,**kwargs):
            if args[0]=='scp':return ''
            if args[-1].startswith('python3 -c '):return json.dumps({'directory':'/home/abhinav/ci/cvd-mirror-preflight/run-17','uid':1000,'gid':1000})
            raise m.subprocess.CalledProcessError(2,list(args),stderr='x'*4096+marker)
        with patch.dict(os.environ,self.env,clear=True),patch.object(m,'run',side_effect=run),patch.object(m,'read',return_value=b'{"id":2012}'):
            with self.assertRaisesRegex(ValueError,'InvalidAvro') as failure:m.linux_probe(root,self.topic,'http://127.0.0.1:8082')
        self.assertIn(marker,str(failure.exception));self.assertLess(len(str(failure.exception)),2200)

    def test_failed_readiness_keeps_last_cause_in_receipt_without_rollback(self):
        clock=[0]
        with patch.object(m.time,'monotonic',side_effect=lambda:clock[0]),patch.object(m.time,'sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+61)):
            with self.assertRaisesRegex(TimeoutError,'configured readiness failure'):self.execute(readiness_failure=True)
        proof=json.loads((self.base/'.jenkins-tmp/cvd-mirror-17/receipt.json').read_text())
        self.assertIn('configured readiness failure',proof['readinessLastFailure']);self.assertIn('configured readiness failure',proof['failureMessage']);self.assertEqual(['bootout','bootstrap'],self.effects);self.assertNotEqual(self.raw,self.plist.read_bytes())

    def test_unguarded_run_cannot_start_inspection_or_runtime(self):
        with patch.dict(os.environ,{},clear=True),patch.object(m,'run') as command:
            with self.assertRaisesRegex(ValueError,'Guarded Jenkins'):m.main()
            command.assert_not_called()

if __name__=='__main__':unittest.main()

class ReleaseProofTest(unittest.TestCase):
    def test_exact_sdk_members_manifest_receipt_chain_and_bounds(self):
        import hashlib,io,zipfile,warnings
        import importlib.abc,importlib.machinery
        owner=self
        for fault,message in [(None,None),('member','released SDK member'),('main','entrypoint'),('receipt','receipt/entrypoint'),('duplicate','ZIP directory'),('count','population')]:
            with self.subTest(fault=fault):
                root=Path(tempfile.mkdtemp(prefix='cvd-mirror-release-proof-'))
                def archive(entries):
                    buffer=io.BytesIO()
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore',UserWarning)
                        with zipfile.ZipFile(buffer,'w') as jar:
                            for name,body in entries:jar.writestr(name,body)
                    return buffer.getvalue()
                sdk=archive([('test/Codec.class',b'official SDK bytes')])
                entries=[('META-INF/MANIFEST.MF','Manifest-Version: 1.0\nMain-Class: '+('incorrect.Main' if fault=='main' else m.MAIN)+'\n\n'),('test/Codec.class',b'bad bytes' if fault=='member' else b'official SDK bytes')]
                if fault=='duplicate':entries.append(('test/Codec.class',b'official SDK bytes'))
                package=archive(entries)
                source='a'*40;contracts='b'*40;jar_sha='c'*64
                receipt=json.dumps({'status':'PASS','source':source,'contracts_source':contracts,'contracts_jar_sha256':jar_sha}).encode()
                proof={'source':source,'contracts_source':contracts,'contracts_jar_sha256':jar_sha,'build_number':'19','main_class':m.MAIN,'runtime_deployed':False,'sdk_members_verified':2 if fault=='count' else 1,'jar_sha256':hashlib.sha256(package).hexdigest(),'qualification_receipt_sha256':'f'*64 if fault=='receipt' else hashlib.sha256(receipt).hexdigest()}
                params={'BUILD_TARGET':'cvd-avro-related','ENVIRONMENT':'production','PUSH_IMAGES':True,'PERMITTED_SHA':source,'CONTRACTS_PERMITTED_SHA':contracts}
                row={'number':19,'building':False,'result':'SUCCESS','actions':[{'parameters':[{'name':k,'value':v} for k,v in params.items()]}]}
                def fetch(url,maximum=2<<20):
                    if '/api/json?' in url:return json.dumps({'builds':[row]}).encode()
                    if url.endswith('/receipt.json'):return receipt
                    if url.endswith('/release.json'):return json.dumps(proof).encode()
                    if url.endswith('/canonical-cvd-mirror.jar'):return package
                    raise AssertionError('Unexpected artifact request: '+url)
                def latest(base,source_dir,destination):
                    destination.write_bytes(sdk);destination.with_suffix('.release.json').write_text(json.dumps({'source':contracts}))
                class Loader(importlib.abc.Loader):
                    def create_module(self,spec):return None
                    def exec_module(self,module):module.latest=latest
                spec=importlib.machinery.ModuleSpec('mirror_sdk_release',Loader())
                with patch.dict(os.environ,{'JENKINS_URL':'http://jenkins/'},clear=True),patch.object(m.importlib.util,'spec_from_file_location',return_value=spec),patch.object(m,'read',side_effect=fetch),patch.object(m,'main_ref',return_value=source):
                    if fault:
                        with self.assertRaisesRegex(ValueError,message):m.release(root)
                        self.assertFalse((root/'canonical-cvd-mirror.jar').exists())
                    else:
                        actual=m.release(root);self.assertEqual(proof,actual);self.assertEqual(package,(root/'canonical-cvd-mirror.jar').read_bytes())

class ReadinessHeapTest(unittest.TestCase):
    def test_heap_cap_check_runs_without_attaching_to_a_runtime(self):
        import subprocess
        root=Path(tempfile.mkdtemp(prefix='cvd-readiness-heap-'))
        probe=root/'HeapProbe.java'
        probe.write_text('class HeapProbe { public static void main(String[] args) { CvdMirrorReadiness.checkHeap(new java.lang.management.MemoryUsage(0,134217728,268435456,268435456)); try { CvdMirrorReadiness.checkHeap(new java.lang.management.MemoryUsage(0,134217728,268435456,536870912)); throw new AssertionError("wrong cap accepted"); } catch(IllegalStateException expected) {} System.out.println("HEAP_CAP_PASS"); } }')
        java=m.JAVA;compiler=java.with_name('javac')
        subprocess.run([str(compiler),'-d',str(root),str(ROOT/'scripts/ops/CvdMirrorReadiness.java'),str(probe)],check=True,capture_output=True,text=True,timeout=15)
        result=subprocess.run([str(java),'-Xmx64m','-cp',str(root),'HeapProbe'],check=False,capture_output=True,text=True,timeout=10)
        self.assertEqual(0,result.returncode,result.stderr)
        self.assertEqual('HEAP_CAP_PASS',result.stdout.strip())
