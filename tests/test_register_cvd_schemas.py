"""Registry metadata tests; no real registry, broker, group or runtime mutation."""
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import sys
import unittest
import urllib.error
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
if os.environ.get('CANONICAL_PACKAGED_ARTIFACT'):sys.path.insert(0,str(Path(os.environ['CANONICAL_PACKAGED_ARTIFACT']).resolve()))
spec=importlib.util.spec_from_file_location('cvd_registry',ROOT/'scripts/kafka/register-cvd-schemas.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
TEXT='{"type":"record","name":"Proof","fields":[]}'

class Registry:
    def __init__(self,present=False):self.present=present;self.calls=[];self.schema=json.loads(TEXT)
    def __call__(self,url,path,body=None,missing=False):
        self.calls.append((path,body))
        if path.endswith('/versions?deleted=true'):return [1] if self.present else None
        if path.endswith('/versions'):
            self.present=True;return {'id':42}
        if path.startswith('/schemas/ids/'):
            return {'schemaType':'AVRO','schema':json.dumps(self.schema)}
        return {'id':42,'schemaType':'AVRO','schema':json.dumps(self.schema)} if self.present else None

class CvdSchemasTest(unittest.TestCase):
    def test_frozen_external_inventory_and_single_prefix(self):
        self.assertEqual(32,len(m.BINDINGS));self.assertEqual(32,len(set(m.BINDINGS.values())))
        for topic,resource in m.BINDINGS.items():
            self.assertEqual(resource,m.binding(topic))
            if not topic.startswith('es.'):self.assertEqual(resource,m.binding('es.'+topic))
        for unknown in ['context-tape.direction.progress','es.es.futures.cvd','es.es.open-direction.forecast','options.databento.gex.strike','es.futures.cvd.avro']:
            self.assertIsNone(m.binding(unknown))
    def test_dry_missing_performs_lookup_only(self):
        r=Registry();result=m.reconcile('http://registry','es.futures.cvd',TEXT,False,r)
        self.assertFalse(result['registered']);self.assertEqual([('/subjects/es.futures.cvd-value',{'schemaType':'AVRO','schema':TEXT}),('/subjects/es.futures.cvd-value/versions?deleted=true',None)],r.calls)
    def test_apply_then_idempotent_lookup_exact_subject_and_id(self):
        r=Registry();result=m.reconcile('http://registry','es.futures.cvd',TEXT,True,r)
        self.assertTrue(result['registered']);self.assertTrue(result['created']);self.assertEqual(42,result['schemaId'])
        self.assertEqual(['/subjects/es.futures.cvd-value','/subjects/es.futures.cvd-value/versions?deleted=true','/subjects/es.futures.cvd-value/versions','/subjects/es.futures.cvd-value','/schemas/ids/42'],[p for p,b in r.calls])
        r.calls=[];result=m.reconcile('http://registry','es.futures.cvd',TEXT,True,r)
        self.assertFalse(result['created']);self.assertNotIn('/subjects/es.futures.cvd-value/versions',[p for p,b in r.calls])
    def test_lookup_miss_with_existing_or_deleted_history_never_registers(self):
        for apply in [False,True]:
            calls=[]
            def existing(url,path,body=None,missing=False):
                calls.append(path)
                if path.endswith('/versions'):self.fail('must not register over existing subject history')
                if path.endswith('/versions?deleted=true'):return [1,2]
                return None
            with self.subTest(apply=apply),self.assertRaisesRegex(ValueError,'history conflicts'):
                m.reconcile('http://registry','es.futures.cvd',TEXT,apply,existing)
            self.assertEqual(['/subjects/es.futures.cvd-value','/subjects/es.futures.cvd-value/versions?deleted=true'],calls)

    def test_conflicting_schema_and_behavioral_metadata_fail(self):
        for changes in [{'schema':'{"type":"record","name":"Other","fields":[]}'},{'references':[{'name':'x'}]},{'schemaType':'JSON'},{'metadata':{'properties':{'x':'y'}}},{'ruleSet':{'domainRules':[{'name':'transform'}]}}]:
            with self.subTest(changes=changes),self.assertRaises(ValueError):m.verify_schema({'schema':TEXT,**changes},TEXT)
        m.verify_schema({'schema':TEXT,'ruleSet':{'domainRules':[],'migrationRules':[]}},TEXT)
    def test_server_namespace_normalization_preserves_full_schema_identity(self):
        expected={'type':'record','name':'Proof','namespace':'com.example','fields':[
            {'name':'first','type':{'type':'record','name':'Child','namespace':'com.example','fields':[{'name':'price','type':{'type':'bytes','logicalType':'decimal','precision':38,'scale':18}}]}},
            {'name':'second','type':['null','com.example.Child'],'default':None}]}
        actual=json.loads(json.dumps(expected));actual['fields'][0]['type'].pop('namespace')
        actual['fields'][1]['type'][1]='Child'
        m.verify_schema({'schema':json.dumps(actual)},json.dumps(expected))
        for change in ['namespace','precision','scale','field','reference']:
            changed=json.loads(json.dumps(actual))
            if change=='namespace':changed['namespace']='com.other'
            elif change=='precision':changed['fields'][0]['type']['fields'][0]['type']['precision']=37
            elif change=='scale':changed['fields'][0]['type']['fields'][0]['type']['scale']=17
            elif change=='field':changed['fields'][0]['type']['fields'][0]['name']='different'
            else:changed['fields'][1]['type'][1]='Unknown'
            with self.subTest(change=change),self.assertRaises(ValueError):
                m.verify_schema({'schema':json.dumps(changed)},json.dumps(expected))

    def test_foreign_or_invalid_id_fails(self):
        for schema_id in [0,-1,True,'42']:
            with self.subTest(schema_id=schema_id),self.assertRaises(ValueError):
                m.reconcile('http://registry','x',TEXT,False,lambda *a,**k:{'id':schema_id,'schema':TEXT})
        r=Registry();original=r.__call__
        def changed(url,path,body=None,missing=False):
            response=original(url,path,body,missing)
            if path.endswith('/versions'):return {'id':43}
            return response
        with self.assertRaisesRegex(ValueError,'disagree'):m.reconcile('http://registry','x',TEXT,True,changed)
    def test_only_genuine_missing_schema_codes_allow_registration(self):
        for code in [40401,40403,40499]:
            error=urllib.error.HTTPError('http://registry',404,'missing',{},io.BytesIO(json.dumps({'error_code':code}).encode()))
            with patch.object(m.urllib.request,'urlopen',side_effect=error):
                if code in [40401,40403]:self.assertIsNone(m.http('http://registry','/subjects/x',{},True))
                else:
                    with self.assertRaises(urllib.error.HTTPError):m.http('http://registry','/subjects/x',{},True)
    def test_metadata_is_existing_topics_only_and_bounded(self):
        root=Path(tempfile.mkdtemp(prefix='cvd-schema-metadata-'))  # Preserve owned fixture; no recursive cleanup.
        binary=root/'kcat';directory=root/'out';directory.mkdir()
        def run(body):
            binary.write_text('#!/bin/sh\n'+body);binary.chmod(0o755)
            with patch.dict(os.environ,{'PATH':str(root)+os.pathsep+os.environ['PATH']}):return m.metadata('broker:9092',directory)
        with patch.object(m,'MAX_METADATA_BYTES',256):
            self.assertEqual(['es.futures.cvd'],run("printf '%s' '{\"topics\":[{\"topic\":\"es.futures.cvd\",\"partitions\":[{}]},{\"topic\":\"unrelated\",\"partitions\":[{}]}]}'"))
        # New directory per run, so stale or symbolic output can never be reused.
        directory=root/'oversize';directory.mkdir()
        with patch.object(m,'MAX_METADATA_BYTES',8),self.assertRaises(ValueError):run("printf '%s' '{\"topics\":[{\"topic\":\"es.futures.cvd\",\"partitions\":[{}]}]}'")
        directory=root/'empty';directory.mkdir()
        with self.assertRaises(ValueError):run("printf '%s' '{\"topics\":[]}'")
    def test_unguarded_main_has_no_process_network_or_filesystem_effect(self):
        with patch.dict(os.environ,{},clear=True),patch.object(m.subprocess,'Popen') as child,patch.object(m.Path,'mkdir') as mkdir,patch.object(m.urllib.request,'urlopen') as http:
            with self.assertRaisesRegex(ValueError,'Guarded Jenkins'):m.main()
            child.assert_not_called();mkdir.assert_not_called();http.assert_not_called()


class EnvironmentSelection(unittest.TestCase):
    def test_default_is_all_three_in_order(self):
        self.assertEqual(['es4','dev','production'],[e[0] for e in m.selected_endpoints('es4,dev,production')])

    def test_dev_alone_selects_only_the_dev_registry(self):
        self.assertEqual([('dev','192.168.100.102:19092','http://192.168.100.102:8082')],m.selected_endpoints('dev'))

    def test_unknown_repeated_or_empty_names_are_refused(self):
        for bad in ['','prod','dev,dev','dev,','es4,qa']:
            with self.subTest(bad=bad),self.assertRaises(ValueError):m.selected_endpoints(bad)

    def test_only_the_selected_cluster_is_read_and_it_carries_the_dev_only_bindings(self):
        asked=[]
        def fake(bootstrap,directory):asked.append(bootstrap);return ['futures.level-absorption.events','futures.level-absorption.manual-line']
        with tempfile.TemporaryDirectory() as d:
            inv=m.build_inventory(Path(d),'dev',fake)
        self.assertEqual(['192.168.100.102:19092'],asked)
        self.assertEqual(['dev'],[e[0] for e in inv])
        for topic in inv[0][2]:self.assertIsNotNone(m.binding(topic),topic)
        self.assertEqual('cvd-related/LevelAbsorptionEvent.avsc',m.binding('futures.level-absorption.events'))
        self.assertEqual('cvd-related/LevelAbsorptionManualLine.avsc',m.binding('futures.level-absorption.manual-line'))

    def test_default_reads_all_three_clusters(self):
        asked=[]
        with tempfile.TemporaryDirectory() as d:
            m.build_inventory(Path(d),'es4,dev,production',lambda b,_:asked.append(b) or [])
        self.assertEqual(['192.168.100.4:9092','192.168.100.102:19092','192.168.100.252:9092'],asked)

    def test_the_job_defaults_to_all_three_and_the_script_reads_the_variable(self):
        jf=(ROOT/'Jenkinsfile.es4-deploy').read_text()
        self.assertIn("name: 'CVD_SCHEMA_ENVIRONMENTS', defaultValue: 'es4,dev,production'",jf)
        self.assertIn("os.environ.get('CVD_SCHEMA_ENVIRONMENTS','es4,dev,production')",(ROOT/'scripts/kafka/register-cvd-schemas.py').read_text())

if __name__=='__main__':unittest.main()
