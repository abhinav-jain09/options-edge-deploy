#!/usr/bin/env python3
"""Run the real non-destructive ensure script against local Kafka CLI doubles; never contacts Kafka."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
NATIVE = 'options.indicators.bars.current'
ROW = NATIVE + '|1|compact,delete|604800000'
STUB = r'''#!/usr/bin/env python3
import json,os,re,sys
from pathlib import Path
p=Path(os.environ['OE_DISPLAY_TEST_STATE']);s=json.loads(p.read_text());a=sys.argv[1:]
def value(k):return a[a.index(k)+1]
if '--delete' in a:raise RuntimeError('destructive delete is forbidden')
if '--list' in a:print('\n'.join(s['topics']))
elif '--create' in a:
 t=value('--topic');s['calls'].append(['create',t]);cfg={}
 for i,x in enumerate(a):
  if x=='--config':k,v=a[i+1].split('=',1);cfg[k]=v
 s['topics'][t]={'parts':int(value('--partitions')),'cfg':cfg}
elif '--entity-name' in a:
 t=value('--entity-name')
 if '--alter' in a:
  s['calls'].append(['alter',t])
  for k,v in re.findall(r'([^=,]+)=(\[[^]]*\]|[^,]+)',value('--add-config')):s['topics'][t]['cfg'][k]=v.strip('[]')
 else:print('Dynamic configs for topic '+t+' are: '+' '.join(k+'='+v for k,v in s['topics'][t]['cfg'].items()))
elif '--describe' in a:
 t=value('--topic');print('Topic: '+t+' PartitionCount: '+str(s['topics'][t]['parts'])+' ReplicationFactor: 1')
else:raise RuntimeError('unmodelled CLI invocation: '+repr(a))
p.write_text(json.dumps(s))
'''

class DisplayTopicBarrierTest(unittest.TestCase):
    def run_ensure(self, native=None, dry=False, remove_contract=False):
        source=(HERE/'ensure-indicator-topics.sh').read_text()
        contract=re.search(r'CONTRACT="\n(.*?)\n"',source,re.S).group(1)
        topics={}
        for line in contract.splitlines():
            name,parts,policy,retention=line.split('|')
            if name==NATIVE:continue
            cfg={'cleanup.policy':policy}
            if retention:cfg['retention.ms']=retention
            topics[name]={'parts':int(parts),'cfg':cfg}
        self.assertGreater(len(topics),5,'positive control: real baseline topics are loaded')
        before=json.loads(json.dumps(topics))
        if native:topics[NATIVE]=native
        with tempfile.TemporaryDirectory(prefix='indicator-display-test-') as folder:
            root=Path(folder);state=root/'state.json';state.write_text(json.dumps({'topics':topics,'calls':[]}))
            stub=root/'kafka';stub.write_text(STUB);stub.chmod(0o700)
            script=root/'ensure.sh'
            if remove_contract:
                weakened=source.replace(ROW+'\n','',1);self.assertNotEqual(weakened,source);source=weakened
            script.write_text(source)
            env={**os.environ,'OE_DISPLAY_TEST_STATE':str(state),'KAFKA_TOPICS_CMD':str(stub),'KAFKA_CONFIGS_CMD':str(stub),
                 'KAFKA_BOOTSTRAP_SERVERS':'mock.invalid:9092','KAFKA_TOPIC_REPLICATION_FACTOR':'1','INDICATOR_APP_ID':'indicator-test',
                 'TOPIC_PREFIX':'','DEPLOY_DRY_RUN':str(dry).lower()}
            result=subprocess.run(['bash',str(script)],env=env,text=True,capture_output=True,timeout=30)
            after=json.loads(state.read_text())
            self.assertEqual({k:v for k,v in after['topics'].items() if k!=NATIVE},before,'unrelated topics must not change')
            return result,after
    def assert_created(self, result, state):
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn(NATIVE,state['topics'],'native display topic is actually provisioned')
        self.assertEqual(state['topics'][NATIVE],{'parts':1,'cfg':{'cleanup.policy':'compact,delete','retention.ms':'604800000',
                        'segment.ms':'600000','min.cleanable.dirty.ratio':'0.01'}})
    def test_create_and_reconcile_without_deleting_history(self):
        self.assert_created(*self.run_ensure())
        wrong={'parts':1,'cfg':{'cleanup.policy':'delete','retention.ms':'1','segment.ms':'999','min.cleanable.dirty.ratio':'.5'}}
        result,state=self.run_ensure(wrong);self.assert_created(result,state)
        self.assertNotIn(['create',NATIVE],state['calls'])
    def test_wrong_partition_count_fails_without_recreation(self):
        native={'parts':2,'cfg':{'cleanup.policy':'compact,delete','retention.ms':'604800000'}}
        result,state=self.run_ensure(native);self.assertNotEqual(result.returncode,0)
        self.assertIn('PartitionCount=2',result.stdout);self.assertEqual(state['topics'][NATIVE],native)
        self.assertEqual(state['calls'],[])
    def test_dry_run_does_not_create_or_alter(self):
        result,state=self.run_ensure(dry=True);self.assertEqual(result.returncode,0,result.stdout)
        self.assertNotIn(NATIVE,state['topics']);self.assertEqual(state['calls'],[])
    def test_fleet_create_and_reconcile_keep_the_same_native_shape(self):
        for topic in [NATIVE,'es.'+NATIVE]:
            with self.subTest(topic=topic), tempfile.TemporaryDirectory(prefix='display-fleet-test-') as folder:
                root=Path(folder);state=root/'state.json';state.write_text(json.dumps({'topics':{},'calls':[]}))
                for name in ['kafka-topics','kafka-configs']:
                    stub=root/name;stub.write_text(STUB);stub.chmod(0o700)
                script=root/'run.sh'
                script.write_text('set -euo pipefail\nsource "$TOPIC_DECLARATIONS"\nsource "$TOPIC_LIBRARY"\n'
                    'topic_shape_resolve_declarations\ncreate_topic "$DISPLAY_TOPIC" 1\nalter_topic_config "$DISPLAY_TOPIC" "$(topic_cleanup_policy "$DISPLAY_TOPIC")"\n')
                env={**os.environ,'PATH':str(root)+os.pathsep+os.environ['PATH'],'OE_DISPLAY_TEST_STATE':str(state),
                     'TOPIC_DECLARATIONS':str(HERE/'topics.env'),'TOPIC_LIBRARY':str(HERE/'topic-shape.lib.sh'),
                     'DISPLAY_TOPIC':topic,'KAFKA_BOOTSTRAP_SERVERS':'mock.invalid:9092','REPLICATION_FACTOR':'1',
                     'MIN_ISR':'1','RETENTION_MS':'604800000','CLEANUP_POLICY':'delete','KAFKA_TOPIC_CONFIG_RETRY_ATTEMPTS':'1','ENVIRONMENT':'dev','TOPIC_SET':'es4' if topic.startswith('es.') else ''}
                result=subprocess.run(['bash',str(script)],env=env,text=True,capture_output=True,timeout=15)
                self.assertEqual(result.returncode,0,result.stdout+result.stderr)
                actual=json.loads(state.read_text());self.assertEqual(list(actual['topics']),[topic])
                cfg=actual['topics'][topic]['cfg']
                self.assertEqual(cfg['segment.ms'],'600000');self.assertEqual(cfg['min.cleanable.dirty.ratio'],'0.01')
                self.assertEqual(cfg['cleanup.policy'],'compact,delete');self.assertEqual(cfg['retention.ms'],'604800000')
                # Reconcile a wrong existing config through the SAME fleet function; no recreate is involved.
                actual['topics'][topic]['cfg']['segment.ms']='999';actual['topics'][topic]['cfg']['min.cleanable.dirty.ratio']='.5'
                state.write_text(json.dumps(actual));script.write_text('set -euo pipefail\nsource "$TOPIC_DECLARATIONS"\nsource "$TOPIC_LIBRARY"\ntopic_shape_resolve_declarations\nalter_topic_config "$DISPLAY_TOPIC" "$(topic_cleanup_policy "$DISPLAY_TOPIC")"\n')
                result=subprocess.run(['bash',str(script)],env=env,text=True,capture_output=True,timeout=15)
                self.assertEqual(result.returncode,0,result.stdout+result.stderr)
                actual=json.loads(state.read_text());self.assertEqual(actual['topics'][topic]['cfg']['segment.ms'],'600000')
                self.assertEqual(actual['topics'][topic]['cfg']['min.cleanable.dirty.ratio'],'0.01')
                self.assertEqual(sum(c[0]=='create' for c in actual['calls']),1)
    def test_negative_control_detects_an_ensure_script_missing_the_new_contract(self):
        result,state=self.run_ensure(remove_contract=True)
        with self.assertRaisesRegex(AssertionError,'actually provisioned'):self.assert_created(result,state)
        self.assert_created(*self.run_ensure())

if __name__=='__main__':unittest.main()
