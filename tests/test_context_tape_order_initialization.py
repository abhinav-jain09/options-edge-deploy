import importlib.util
import io
import re
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('order_init', ROOT/'scripts/deploy/initialize-context-tape-orders.py')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
SCHEMAS = {topic: {'type': 'record', 'name': 'CanonicalOrder'+str(index), 'fields': []}
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
        self.selector = {'runtime': 'actual-executor'}
        self.expressions = []
        self.blank_strategy = False

    def kube(self, *args):
        if self.read_failure:
            raise RuntimeError('read unavailable')
        if args[1] == 'configmap':
            return {'data': {'KAFKA_BOOTSTRAP_SERVERS': 'wrong' if self.wrong_cluster else 'host.docker.internal:19092',
                             'SCHEMA_REGISTRY_URL': 'http://host.docker.internal:8082'}}
        if args[1] == 'pods':
            return {'items': self.pods if args[args.index('-l')+1] == ','.join(key+'='+value for key,value in sorted(self.selector.items())) else []}
        return {'spec': {'replicas': self.desired, 'selector': {'matchLabels': self.selector, 'matchExpressions': self.expressions}, 'template': {'spec': {'containers': []}}},
                'status': {'replicas': self.actual}}

    def kafka(self, tool, *args):
        if tool == 'kafka-consumer-groups':
            if '--list' in args:
                return '\n'.join(self.groups)
            group = args[args.index('--group')+1]
            return 'GROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS\n'+group+' host:9092 (1) '+('' if self.blank_strategy else 'range')+' '+self.state+' '+self.members+'\n'
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
    def setUp(self):
        network = patch.object(MODULE.urllib.request, 'urlopen', side_effect=AssertionError('UNEXPECTED_NETWORK_IN_UNIT_TEST'))
        network.start()
        self.addCleanup(network.stop)
        command = patch.object(MODULE, 'command', side_effect=AssertionError('UNEXPECTED_PROCESS_IN_UNIT_TEST'))
        command.start()
        self.addCleanup(command.stop)

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
        fixture.blank_strategy = True
        self.apply(fixture)  # Real Empty classic groups have no assignment strategy.
        self.assertEqual(4, len(fixture.writes))

    def test_actual_selector_includes_terminating_pods_with_zero_status_replicas(self):
        fixture = Fixture()
        fixture.pods = [{'metadata': {'deletionTimestamp': '2026-10-17T15:00:00Z'}}]
        self.assertEqual(0, fixture.desired)
        self.assertEqual(0, fixture.actual)
        with self.assertRaisesRegex(RuntimeError, 'still has pods'):
            self.apply(fixture)
        self.assertEqual([], fixture.writes)

    def test_missing_or_expression_selector_refused(self):
        for selector, expressions in [({}, []), ({'runtime': 'actual-executor'}, [{'key': 'app', 'operator': 'Exists'}])]:
            fixture = Fixture()
            fixture.selector, fixture.expressions = selector, expressions
            with self.assertRaisesRegex(RuntimeError, 'selector must have explicit matchLabels'):
                self.apply(fixture)
            self.assertEqual([], fixture.writes)

    def test_deleted_schema_history_cannot_be_resurrected(self):
        fixture = Fixture()
        for history in ([1], {'unexpected': []}):
            def registry(url, timeout):
                if '?deleted=true' in url:
                    return Response(history)
                raise MODULE.urllib.error.HTTPError(url, 404, 'missing', {}, io.BytesIO(b'{"error_code":40401}'))
            with patch.object(MODULE.urllib.request, 'urlopen', side_effect=registry):
                with self.assertRaisesRegex(RuntimeError, 'refuses resurrection'):
                    MODULE.Initializer.schema(fixture, MODULE.TOPICS[0])
        for history in ([], None):
            def registry(url, timeout):
                if '?deleted=true' in url and history == []:
                    return Response([])
                raise MODULE.urllib.error.HTTPError(url, 404, 'missing', {}, io.BytesIO(b'{"error_code":40401}'))
            with patch.object(MODULE.urllib.request, 'urlopen', side_effect=registry):
                self.assertFalse(MODULE.Initializer.schema(fixture, MODULE.TOPICS[0]))

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

    def test_registry_namespace_rendering_uses_read_only_lookup_of_latest_identity(self):
        canonical = {'type': 'record', 'name': 'Root', 'namespace': 'example', 'fields': [
            {'name': 'child', 'type': {'type': 'record', 'name': 'Child', 'namespace': 'example', 'fields': []}}]}
        rendered = json.loads(json.dumps(canonical))
        del rendered['fields'][0]['type']['namespace']
        initializer = MODULE.Initializer('dev', {MODULE.TOPICS[0]: canonical})
        latest = {'id': 99, 'version': 2, 'schema': json.dumps(rendered)}
        lookup = {'subject': MODULE.TOPICS[0]+'-value', 'id': 99, 'version': 2}
        calls = []
        def registry(request, timeout):
            if isinstance(request, str):
                return Response(latest)
            calls.append((request.get_method(), request.full_url))
            self.assertEqual(canonical, json.loads(json.loads(request.data)['schema']))
            return Response(lookup)
        with patch.object(MODULE.urllib.request, 'urlopen', side_effect=registry):
            self.assertTrue(initializer.schema(MODULE.TOPICS[0]))
            self.assertEqual([('POST', initializer.registry+'/subjects/'+MODULE.TOPICS[0]+'-value')], calls)
            lookup['id'] = 98
            with self.assertRaisesRegex(RuntimeError, 'differs from canonical'):
                initializer.schema(MODULE.TOPICS[0])
            lookup['id'], lookup['version'] = 99, 1
            with self.assertRaisesRegex(RuntimeError, 'differs from canonical'):
                initializer.schema(MODULE.TOPICS[0])
            lookup['version'], lookup['subject'] = 2, 'different-value'
            with self.assertRaisesRegex(RuntimeError, 'differs from canonical'):
                initializer.schema(MODULE.TOPICS[0])
            latest['id'], latest['version'] = 1, 1
            lookup['id'], lookup['version'], lookup['subject'] = True, True, MODULE.TOPICS[0]+'-value'
            with self.assertRaisesRegex(RuntimeError, 'differs from canonical'):
                initializer.schema(MODULE.TOPICS[0])
        for error in (MODULE.urllib.error.URLError('unavailable'), TimeoutError('timed out'),
                      MODULE.urllib.error.HTTPError('http://registry', 404, 'not found', {}, io.BytesIO(b'{"error_code":40403}'))):
            def unavailable(request, timeout):
                if isinstance(request, str):
                    return Response(latest)
                raise error
            with patch.object(MODULE.urllib.request, 'urlopen', side_effect=unavailable):
                with self.assertRaisesRegex(RuntimeError, 'identity lookup unavailable'):
                    initializer.schema(MODULE.TOPICS[0])

    def test_fixture_recovery_is_exact_dev_only_and_never_permanent(self):
        value = {'subject': MODULE.TOPICS[0]+'-value', 'version': 1, 'id': 32,
                 'schema': json.dumps({'type': 'record', 'name': 'Order0', 'fields': []})}
        with self.assertRaisesRegex(RuntimeError, 'dev-only'):
            MODULE.Initializer('production', SCHEMAS, True)
        initializer = MODULE.Initializer('dev', SCHEMAS, True)
        self.assertTrue(initializer.fixture_value(MODULE.TOPICS[0], value))
        self.assertFalse(MODULE.Initializer('dev', SCHEMAS).fixture_value(MODULE.TOPICS[0], value))
        for key, wrong in [('subject', MODULE.TOPICS[1]+'-value'), ('version', 2), ('id', 33),
                           ('schema', '{"type":"record","name":"Other","fields":[]}'), ('schemaType', 'JSON')]:
            self.assertFalse(initializer.fixture_value(MODULE.TOPICS[0], dict(value, **{key: wrong})))
        self.assertFalse(initializer.fixture_value(MODULE.TOPICS[1], value))
        with patch.object(MODULE.urllib.request, 'urlopen', side_effect=[Response([1]), Response(value)]):
            self.assertTrue(initializer.fixture_history(MODULE.TOPICS[0]))
        with patch.object(MODULE.urllib.request, 'urlopen', return_value=Response([1,2])):
            with self.assertRaisesRegex(RuntimeError, 'exactly historical version 1'):
                initializer.fixture_history(MODULE.TOPICS[0])

    def test_full_fixture_recovery_soft_deletes_only_exact_version_then_registers(self):
        fixture = Fixture()
        fixture.recover_dev_fixture = True
        fixture.topics.add(MODULE.TOPICS[0])
        fixture.schema = lambda topic: MODULE.Initializer.schema(fixture, topic)
        placeholder = {'subject': MODULE.TOPICS[0]+'-value', 'version': 1, 'id': 32,
                       'schema': json.dumps({'type': 'record', 'name': 'Order0', 'fields': []})}
        current = {MODULE.TOPICS[0]: placeholder}
        deleted = False
        mutations = []
        def registry(request, timeout):
            nonlocal deleted
            url = request if isinstance(request, str) else request.full_url
            method = 'GET' if isinstance(request, str) else request.get_method()
            topic = url.split('/subjects/')[1].split('-value/')[0]
            if method == 'DELETE':
                self.assertEqual(MODULE.TOPICS[0], topic)
                self.assertTrue(url.endswith('/versions/1'))
                self.assertNotIn('permanent', url)
                self.assertEqual(placeholder, current.pop(topic))
                deleted = True
                mutations.append(('soft-delete', topic, 1))
                return Response(1)
            if method == 'POST':
                payload = json.loads(request.data)
                self.assertEqual(SCHEMAS[topic], json.loads(payload['schema']))
                current[topic] = {'subject': topic+'-value', 'version': 2 if topic == MODULE.TOPICS[0] else 1,
                                  'id': 33, 'schema': payload['schema']}
                mutations.append(('register', topic))
                return Response({'id': 33})
            if url.endswith('/1?deleted=true'):
                return Response(placeholder)
            if url.endswith('/versions?deleted=true') and topic == MODULE.TOPICS[0]:
                return Response([1])
            if topic in current:
                return Response(current[topic])
            raise MODULE.urllib.error.HTTPError(url, 404, 'missing', {}, io.BytesIO(b'{"error_code":40401}'))
        with self.guarded(), patch.object(MODULE.urllib.request, 'urlopen', side_effect=registry):
            fixture.run(True)
        self.assertTrue(deleted)
        self.assertEqual([('soft-delete', MODULE.TOPICS[0], 1), ('register', MODULE.TOPICS[0]),
                          ('register', MODULE.TOPICS[1])], mutations)
        self.assertEqual({'type': 'record', 'name': 'Order0', 'fields': []}, json.loads(placeholder['schema']))
        self.assertEqual(SCHEMAS[MODULE.TOPICS[0]], json.loads(current[MODULE.TOPICS[0]]['schema']))

    def test_init_mode_skips_unrelated_infra_and_guards_contracts(self):
        source = (ROOT/'Jenkinsfile.common-infra').read_text()
        preparation = {'Permitted commit guard', 'Validate preparation mode', 'Context Tape order initialization only',
                       'Context Tape execution credential only'}
        stages = set(re.findall(r"^\s*stage\('([^']+)'\)", source, re.M))
        self.assertTrue(preparation <= stages)
        for name in stages - preparation:
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
