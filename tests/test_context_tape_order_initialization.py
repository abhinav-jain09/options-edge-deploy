import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('order_init', ROOT/'scripts/deploy/initialize-context-tape-orders.py')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
SCHEMAS = {topic: {'type': 'record', 'name': 'Order'+str(index), 'fields': []}
           for index, topic in enumerate(MODULE.TOPICS)}


class Fixture(MODULE.Initializer):
    def __init__(self):
        super().__init__('dev', SCHEMAS)
        self.topics = set()
        self.registered = set()
        self.writes = []
        self.offset = '0'
        self.policy = dict(MODULE.POLICY, **{'max.message.bytes': '1048588'})
        self.desired = 0
        self.actual = 0
        self.pods = []
        self.state = 'Empty'
        self.members = '0'
        self.groups = set()
        self.rf = 1
        self.partition_count = 1
        self.wrong_cluster = False
        self.read_failure = False
        self.preflights = 0
        self.race = False

    def kube(self, *args):
        if self.read_failure:
            raise RuntimeError('read unavailable')
        if args[1] == 'configmap':
            return {'data': {'KAFKA_BOOTSTRAP_SERVERS': 'wrong' if self.wrong_cluster else 'host.docker.internal:19092',
                             'SCHEMA_REGISTRY_URL': 'http://host.docker.internal:8082'}}
        if args[1] == 'pods':
            return {'items': self.pods}
        return {'spec': {'replicas': self.desired, 'template': {'spec': {'containers': []}}},
                'status': {'replicas': self.actual}}

    def kafka(self, tool, *args):
        if tool == 'kafka-consumer-groups':
            if '--list' in args:
                return '\n'.join(self.groups)
            group = args[args.index('--group')+1]
            return 'GROUP COORDINATOR ASSIGNMENT-STRATEGY STATE #MEMBERS\n'+group+' host:9092 range '+self.state+' '+self.members+'\n'
        topic = args[args.index('--topic')+1].replace('\\.', '.').strip('^$') if '--topic' in args else None
        if tool == 'kafka-topics':
            if '--list' in args:
                return '\n'.join(self.topics)
            if '--create' in args:
                self.writes.append(('topic', topic))
                self.topics.add(topic)
                return ''
            return f'Topic: {topic} PartitionCount: {self.partition_count} ReplicationFactor: {self.rf}\n'
        if tool == 'kafka-get-offsets':
            return topic+':0:'+self.offset+'\n'
        if tool == 'kafka-configs':
            if '--alter' in args:
                topic = args[args.index('--entity-name')+1]
                self.writes.append(('retention', topic))
                self.policy['retention.bytes'] = MODULE.POLICY['retention.bytes']
                return ''
            return '\n'.join('  '+key+'='+value+' sensitive=false' for key, value in self.policy.items())
        raise AssertionError(tool)

    def schema(self, topic):
        return topic in self.registered

    def preflight(self):
        self.preflights += 1
        if self.race and self.preflights == 2:
            self.desired = 1
        return super().preflight()


class Response:
    def __init__(self, value):
        self.value = value
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def read(self, *args):
        return json.dumps(self.value).encode()


class InitializationTest(unittest.TestCase):
    def guarded(self):
        return patch.dict(os.environ, PERMITTED_SHA_GUARD='PASSED', CONTRACTS_SHA_GUARD='PASSED', BUILD_NUMBER='73', JOB_NAME='common-infra-deploy')

    def apply(self, fixture):
        def register(request, timeout):
            topic = request.full_url.split('/subjects/')[1].split('-value/')[0]
            fixture.writes.append(('schema', topic))
            fixture.registered.add(topic)
            return Response({'id': 10})
        with self.guarded(), patch.object(MODULE.urllib.request, 'urlopen', side_effect=register):
            fixture.run(True)

    def test_absent_dry_run_changes_nothing(self):
        fixture = Fixture()
        fixture.run(False)
        self.assertEqual([], fixture.writes)
        self.assertFalse(fixture.topics)
        self.assertFalse(fixture.registered)

    def test_create_only_missing_and_preserve_existing(self):
        fixture = Fixture()
        self.apply(fixture)
        self.assertEqual([('topic', MODULE.TOPICS[0]), ('schema', MODULE.TOPICS[0]),
                          ('topic', MODULE.TOPICS[1]), ('schema', MODULE.TOPICS[1])], fixture.writes)
        fixture.writes.clear()
        self.apply(fixture)
        self.assertEqual([], fixture.writes)

    def test_only_empty_unbounded_topics_can_receive_declared_byte_cap(self):
        fixture = Fixture()
        fixture.topics.update(MODULE.TOPICS)
        fixture.policy['retention.bytes'] = '-1'
        self.apply(fixture)
        self.assertIn(('retention', MODULE.TOPICS[0]), fixture.writes)
        self.assertEqual('1073741824', fixture.policy['retention.bytes'])
        fixture = Fixture()
        fixture.topics.update(MODULE.TOPICS)
        fixture.policy['retention.bytes'] = '-1'
        fixture.offset = '1'
        with self.assertRaisesRegex(RuntimeError, 'not empty at offset zero'):
            self.apply(fixture)
        self.assertEqual([], fixture.writes)

    def test_retained_data_or_nonzero_floor_refused_before_any_write(self):
        fixture = Fixture()
        fixture.topics.add(MODULE.TOPICS[1])
        fixture.offset = '1'
        with self.assertRaisesRegex(RuntimeError, 'not empty at offset zero'):
            self.apply(fixture)
        self.assertEqual([], fixture.writes)

    def test_incompatible_shape_or_policy_refused(self):
        for field, value, reason in [('rf', 2, 'shape'), ('partition_count', 2, 'shape')]:
            fixture = Fixture()
            fixture.topics.add(MODULE.TOPICS[1])
            setattr(fixture, field, value)
            with self.assertRaisesRegex(RuntimeError, reason):
                self.apply(fixture)
            self.assertEqual([], fixture.writes)
        for key in MODULE.POLICY:
            fixture = Fixture()
            fixture.topics.add(MODULE.TOPICS[1])
            fixture.policy[key] = 'wrong'
            with self.assertRaisesRegex(RuntimeError, 'retention/policy'):
                self.apply(fixture)
            self.assertEqual([], fixture.writes)

    def test_quiescence_cluster_and_read_failures_refused(self):
        for field, value, reason in [('desired', 1, 'desired replicas'), ('actual', 1, 'still has replicas'),
                                     ('pods', [{}], 'still has pods'), ('wrong_cluster', True, 'cluster bootstrap'),
                                     ('read_failure', True, 'read unavailable')]:
            fixture = Fixture()
            setattr(fixture, field, value)
            with self.assertRaisesRegex(RuntimeError, reason):
                self.apply(fixture)
            self.assertEqual([], fixture.writes)

    def test_active_and_unreadable_consumer_group_refused(self):
        for state, members in [('Stable', '1'), ('Empty', '1'), ('Unknown', '0')]:
            fixture = Fixture()
            fixture.groups.add('broker-execution-service')
            fixture.state, fixture.members = state, members
            with self.assertRaisesRegex(RuntimeError, 'active execution group'):
                self.apply(fixture)
            self.assertEqual([], fixture.writes)
        fixture = Fixture()
        fixture.groups.add('broker-execution-service')
        self.apply(fixture)  # An observed empty group is accepted.
        self.assertEqual(4, len(fixture.writes))

    def test_quiescence_is_rechecked_before_first_write(self):
        fixture = Fixture()
        fixture.race = True
        with self.assertRaisesRegex(RuntimeError, 'desired replicas'):
            self.apply(fixture)
        self.assertEqual([], fixture.writes)

    def test_both_source_guards_and_jenkins_identity_required(self):
        for key in ('PERMITTED_SHA_GUARD', 'CONTRACTS_SHA_GUARD', 'BUILD_NUMBER', 'JOB_NAME'):
            fixture = Fixture()
            with self.guarded(), patch.dict(os.environ, {key: ''}), self.assertRaisesRegex(RuntimeError, 'both exact-source guards'):
                fixture.run(True)
            self.assertEqual([], fixture.writes)

    def test_schema_mismatch_and_registry_failure_are_not_absence(self):
        fixture = Fixture()
        for value in ({'schema': json.dumps(SCHEMAS[MODULE.TOPICS[0]]), 'schemaType': 'JSON'},
                      {'schema': '{}'}, {'schema': '{}', 'references': [{}]}):
            with patch.object(MODULE.urllib.request, 'urlopen', return_value=Response(value)):
                with self.assertRaisesRegex(RuntimeError, 'incompatible schema|differs from canonical'):
                    MODULE.Initializer.schema(fixture, MODULE.TOPICS[0])
        error = MODULE.urllib.error.HTTPError('http://registry', 503, 'unavailable', {}, None)
        with patch.object(MODULE.urllib.request, 'urlopen', side_effect=error):
            with self.assertRaises(MODULE.urllib.error.HTTPError):
                MODULE.Initializer.schema(fixture, MODULE.TOPICS[0])
        with patch.object(MODULE.urllib.request, 'urlopen', return_value=Response({'schema': json.dumps(SCHEMAS[MODULE.TOPICS[0]])})):
            self.assertTrue(MODULE.Initializer.schema(fixture, MODULE.TOPICS[0]))

    def test_init_mode_skips_unrelated_infra_and_guards_contracts(self):
        source = (ROOT/'Jenkinsfile.common-infra').read_text()
        for name in ('Public gate (PGL-072, secondary)', 'Render + Diff (always)', 'Approve production infra change',
                     'Apply', 'WARN: realm reconciliation skipped', 'Reconcile public realm'):
            condition = source.split("stage('"+name+"')", 1)[1].split('steps {', 1)[0]
            self.assertIn('!params.CONTEXT_TAPE_ORDER_INIT_ONLY', condition)
        stage = source.split("stage('Context Tape order initialization only')", 1)[1].split("stage('Context Tape execution credential only')", 1)[0]
        self.assertLess(stage.index('permitted-sha-guard.sh --dir'), stage.index('initialize-context-tape-orders.py'))
        self.assertIn('CONTRACTS_PERMITTED_SHA', stage)
        self.assertIn("if (!params.DEPLOY_DRY_RUN)", stage)
        self.assertIn("params.ENVIRONMENT == 'production'", stage)
        self.assertLess(stage.index('verify-permitted-tree.sh --dir'), stage.index('--apply'))


if __name__ == '__main__':
    unittest.main()
