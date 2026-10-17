#!/usr/bin/env python3
"""First-start initialization only. Never convert, purge or reset offsets; cap byte retention only on empty topics.

Quiescence covers the executor and AMT/EAV bridge deployments and their configured
consumer groups. External producers must remain stopped during this operation;
these repeated observations are not a Kafka-wide exclusive writer lock.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.request

TOPICS = ('trading.order.intent', 'trading.order.status')
FILES = ('broker-order-intent.avsc', 'broker-order-status.avsc')
POLICY = {'cleanup.policy': 'delete', 'retention.ms': '-1',
          'retention.bytes': '1073741824', 'min.insync.replicas': '1'}
TARGETS = {
    'dev': ('192.168.100.102:19092', 'http://192.168.100.102:8082',
            {'host.docker.internal:19092', '192.168.100.102:19092'},
            {'http://host.docker.internal:8082', 'http://192.168.100.102:8082'}),
    'production': ('192.168.100.252:9092', 'http://192.168.100.252:8082',
                   {'192.168.100.252:9092'}, {'http://192.168.100.252:8082'})}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def command(*args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=60)
    require(result.returncode == 0, 'command failed: ' + args[0] + ' ' + result.stderr[-1500:])
    require('Error occurred' not in result.stdout and 'Error:' not in result.stdout,
            'CLI reported an error: ' + result.stdout[-1500:])
    return result.stdout


def kafka_tool(name):
    found = shutil.which(name) or shutil.which(name + '.sh')
    if found:
        return found
    candidate = Path.home() / 'kafka-4.3.0/bin' / (name + '.sh')
    require(candidate.is_file() and os.access(candidate, os.X_OK), 'Kafka CLI missing: ' + name)
    return str(candidate)


class Initializer:
    def __init__(self, environment, schemas, recover_dev_fixture=False):
        self.bootstrap, self.registry, self.allowed_bootstrap, self.allowed_registry = TARGETS[environment]
        self.environment = environment
        self.recover_dev_fixture = recover_dev_fixture
        self.fixture_pending = False
        require(not recover_dev_fixture or environment == 'dev', 'fixture recovery is dev-only')
        self.schemas = schemas
        self.unbounded = set()

    def kafka(self, tool, *args):
        return command(kafka_tool(tool), '--bootstrap-server', self.bootstrap, *args)

    def kube(self, *args):
        return json.loads(command('kubectl', '--request-timeout=15s', '-n', 'options-edge', *args, '-o', 'json'))

    def quiescent(self):
        config = self.kube('get', 'configmap', 'options-edge-config')['data']
        require(config['KAFKA_BOOTSTRAP_SERVERS'] in self.allowed_bootstrap, 'cluster bootstrap mismatch')
        require(config['SCHEMA_REGISTRY_URL'].rstrip('/') in self.allowed_registry, 'cluster registry mismatch')
        groups = {'broker-execution-service', 'amt-order-bridge', 'eav-order-bridge'}
        for name in ('broker-execution-service', 'amt-order-bridge'):
            deployment = self.kube('get', 'deployment', name)
            require(deployment['spec'].get('replicas', 1) == 0, name + ' desired replicas are not zero')
            require(all(deployment.get('status', {}).get(k, 0) == 0
                        for k in ('replicas', 'readyReplicas', 'availableReplicas', 'updatedReplicas')),
                    name + ' still has replicas')
            selector = deployment['spec'].get('selector', {})
            labels = selector.get('matchLabels')
            require(isinstance(labels, dict) and labels and not selector.get('matchExpressions'),
                    'deployment selector must have explicit matchLabels only: ' + name)
            pods = self.kube('get', 'pods', '-l', ','.join(key + '=' + value for key, value in sorted(labels.items())))
            require(not pods['items'], name + ' still has pods (including terminating pods)')
            for container in deployment['spec']['template']['spec']['containers']:
                for entry in container.get('env', []):
                    if entry['name'] in ('BROKER_EXECUTION_CONSUMER_GROUP', 'AMT_BRIDGE_CONSUMER_GROUP', 'EAV_BRIDGE_CONSUMER_GROUP'):
                        require('value' in entry and entry['value'], 'unresolved execution consumer group')
                        groups.add(entry['value'])
        existing = set(self.kafka('kafka-consumer-groups', '--list').splitlines())
        for group in sorted(groups & existing):
            state = self.kafka('kafka-consumer-groups', '--describe', '--group', group, '--state')
            lines = [line.split() for line in state.splitlines() if line.strip()]
            header = next((row for row in lines if row[-2:] == ['STATE', '#MEMBERS']), None)
            rows = [row for row in lines if row and row[0] == group]
            require(header is not None and len(rows) == 1 and len(rows[0]) >= 3, 'unreadable consumer group state: ' + group)
            require(rows[0][-2] in ('Empty', 'Dead') and rows[0][-1] == '0', 'active execution group: ' + group)

    def topic(self, topic):
        existing = set(self.kafka('kafka-topics', '--list').splitlines())
        if topic not in existing:
            return False
        description = self.kafka('kafka-topics', '--describe', '--topic', topic)
        require(re.search(r'\bPartitionCount:\s*1\b', description) and
                re.search(r'\bReplicationFactor:\s*1\b', description), 'incompatible topic shape: ' + topic)
        for time in ('-2', '-1'):
            offsets = self.kafka('kafka-get-offsets', '--topic', '^' + re.escape(topic) + '$', '--time', time).strip()
            require(offsets == topic + ':0:0', 'topic is not empty at offset zero: ' + topic + ' ' + offsets)
        config_text = self.kafka('kafka-configs', '--entity-type', 'topics', '--entity-name', topic, '--describe', '--all')
        config = dict(re.findall(r'^\s*([\w.]+)=([^\s]*)', config_text, re.M))
        require(all(config.get(key) == value for key, value in POLICY.items() if key != 'retention.bytes')
                and config.get('retention.bytes') in ('-1', POLICY['retention.bytes']), 'incompatible topic retention/policy: ' + topic)
        require(int(config.get('max.message.bytes', '0')) >= 1048576, 'topic max.message.bytes is too small: ' + topic)
        self.unbounded.discard(topic)
        if config['retention.bytes'] == '-1':
            self.unbounded.add(topic)
        return True

    def schema(self, topic):
        url = self.registry + '/subjects/' + topic + '-value/versions/latest'
        try:
            with urllib.request.urlopen(url, timeout=15) as response:
                value = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            body = json.loads(error.read())
            require(body.get('error_code') in (40401, 40402), 'unexpected schema-registry 404')
            try:
                with urllib.request.urlopen(self.registry + '/subjects/' + topic + '-value/versions?deleted=true', timeout=15) as response:
                    history = json.load(response)
                require(history == [] or (history == [1] and self.fixture_history(topic)),
                        'subject has deleted/history versions; initialization refuses resurrection: ' + topic)
            except urllib.error.HTTPError as history_error:
                require(history_error.code == 404 and json.loads(history_error.read()).get('error_code') == 40401,
                        'cannot prove subject has no deleted/history versions: ' + topic)
            return False
        require(value.get('schemaType', 'AVRO') == 'AVRO' and not value.get('references'), 'incompatible schema type/references: ' + topic)
        if json.loads(value['schema']) != self.schemas[topic] and self.fixture_value(topic, value):
            require(self.fixture_history(topic), 'fixture recovery history changed')
            self.fixture_pending = True
            return False
        if json.loads(value['schema']) != self.schemas[topic]:
            # Registry Avro rendering can omit a redundant inherited namespace.
            # Use its official read-only lookup; never normalize/reimplement Avro here.
            request = urllib.request.Request(self.registry + '/subjects/' + topic + '-value',
                data=json.dumps({'schemaType': 'AVRO', 'schema': json.dumps(self.schemas[topic])}).encode(),
                headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'}, method='POST')
            try:
                with urllib.request.urlopen(request, timeout=15) as response:
                    canonical = json.load(response)
            except urllib.error.HTTPError as error:
                raise RuntimeError('existing schema differs from canonical contract or identity lookup unavailable: ' + topic) from error
            require(type(value.get('id')) is int and value['id'] > 0 and type(value.get('version')) is int
                    and value['version'] > 0 and canonical.get('subject') == topic + '-value'
                    and canonical.get('id') == value['id'] and canonical.get('version') == value['version'],
                    'existing schema differs from canonical contract: ' + topic)
        return True

    def fixture_value(self, topic, value):
        return (self.recover_dev_fixture and self.environment == 'dev' and topic == TOPICS[0]
                and value.get('subject') == topic + '-value' and value.get('version') == 1 and value.get('id') == 32
                and value.get('schemaType', 'AVRO') == 'AVRO' and not value.get('references')
                and json.loads(value['schema']) == {'type': 'record', 'name': 'Order0', 'fields': []})

    def fixture_history(self, topic):
        if not self.recover_dev_fixture or self.environment != 'dev' or topic != TOPICS[0]:
            return False
        root = self.registry + '/subjects/' + topic + '-value/versions'
        with urllib.request.urlopen(root + '?deleted=true', timeout=15) as response:
            require(json.load(response) == [1], 'fixture recovery requires exactly historical version 1')
        with urllib.request.urlopen(root + '/1?deleted=true', timeout=15) as response:
            require(self.fixture_value(topic, json.load(response)), 'fixture recovery identity changed')
        return True

    def preflight(self):
        self.quiescent()
        return [(topic, self.topic(topic), self.schema(topic)) for topic in TOPICS]

    def run(self, apply):
        plan = self.preflight()  # Check BOTH contracts before the first write.
        for topic, exists, registered in plan:
            print(topic + ': topic=' + ('preserve' if exists else 'create') + ', schema=' + ('preserve' if registered else 'register')
                  + (', RECOVER known dev-only empty fixture id32/version1' if self.fixture_pending and topic == TOPICS[0] else '')
                  + ', retention.bytes=' + ('cap empty topic at 1 GiB' if topic in self.unbounded else 'declared'), flush=True)
        if not apply:
            print('Dry run: no Kafka or schema mutation.')
            return
        require(os.environ.get('PERMITTED_SHA_GUARD') == 'PASSED' and os.environ.get('CONTRACTS_SHA_GUARD') == 'PASSED'
                and os.environ.get('BUILD_NUMBER') and os.environ.get('JOB_NAME') == 'common-infra-deploy',
                'initialization requires both exact-source guards in Jenkins')
        for topic in TOPICS:
            self.preflight()  # Refuse newly active consumers or newly retained data.
            if not self.topic(topic):
                args = ['--create', '--if-not-exists', '--topic', topic, '--partitions', '1', '--replication-factor', '1']
                for key, value in {**POLICY, 'max.message.bytes': '1048588'}.items():
                    args += ['--config', key + '=' + value]
                self.kafka('kafka-topics', *args)
                require(self.topic(topic), 'topic still absent after create: ' + topic)
                print('Created/verified ' + topic, flush=True)
            self.preflight()
            if topic in self.unbounded:
                self.kafka('kafka-configs', '--entity-type', 'topics', '--entity-name', topic, '--alter',
                           '--add-config', 'retention.bytes=' + POLICY['retention.bytes'])
                require(self.topic(topic) and topic not in self.unbounded, 'byte retention not applied: ' + topic)
                print('Capped verified empty topic byte retention: ' + topic, flush=True)
            self.preflight()
            if not self.schema(topic):
                if self.fixture_pending and topic == TOPICS[0]:
                    self.quiescent()
                    require(self.topic(topic), 'fixture recovery requires the empty topic')
                    require(self.fixture_history(topic), 'fixture history unavailable')
                    request = urllib.request.Request(self.registry + '/subjects/' + topic + '-value/versions/1', method='DELETE')
                    with urllib.request.urlopen(request, timeout=15) as response:
                        require(json.load(response) == 1, 'fixture soft-delete did not confirm version 1')
                    self.fixture_pending = False
                    print('Soft-deleted only known empty dev fixture id32/version1; retained schema id/history, no records changed.', flush=True)
                request = urllib.request.Request(self.registry + '/subjects/' + topic + '-value/versions',
                    data=json.dumps({'schemaType': 'AVRO', 'schema': json.dumps(self.schemas[topic])}).encode(),
                    headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'}, method='POST')
                with urllib.request.urlopen(request, timeout=15) as response:
                    require(isinstance(json.load(response).get('id'), int), 'registration did not return a schema id')
                for attempt in range(20):
                    try:
                        ready = self.schema(topic)
                    except (RuntimeError, urllib.error.URLError):
                        ready = False
                    if ready:
                        break
                    if attempt != 19:
                        time.sleep(1)
                else:
                    raise RuntimeError('canonical schema not visible after registration: ' + topic)
                print('Registered/verified ' + topic + '-value', flush=True)
        require(all(exists and registered for _, exists, registered in self.preflight()), 'initialization did not complete')
        print('Context Tape order topics and canonical schemas ready. No records, offsets or account grants changed.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--contracts', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--recover-dev-fixture', action='store_true', help='Explicit recovery of dev-only empty Order0 schema id32/version1; no other schema may be deleted.')
    args = parser.parse_args()
    environment = os.environ.get('ENVIRONMENT')
    require(environment in TARGETS, 'only dev and production are allowed')
    schemas = {topic: json.loads((args.contracts / 'schemas' / file).read_text()) for topic, file in zip(TOPICS, FILES)}
    Initializer(environment, schemas, args.recover_dev_fixture).run(args.apply)


if __name__ == '__main__':
    main()
