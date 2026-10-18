"""Finite shell routing/guard units; no broker, JVM, archive, Docker or host mutation."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'scripts/ops/archive/oe-archive-kafka.sh').read_text()
TOPICS = re.search(r'^OE_CANONICAL_ARCHIVE_TOPICS="([^"]+)"$', SOURCE, re.M).group(1).split()


def function(name):
    match = re.search(r'^' + name + r'\(\) \{[^\n]*', SOURCE, re.M)
    assert match, name
    if match.group(0).rstrip().endswith('}'):
        return match.group(0) + '\n'
    end = SOURCE.index('\n}', match.end()) + 2
    return SOURCE[match.start():end] + '\n'


class CanonicalArchiveRoutingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='archive-routing-')
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.bin = self.dir / 'bin'
        self.bin.mkdir()
        self.capture = self.dir / 'commands.jsonl'
        timeout = self.bin / 'timeout'
        timeout.write_text('#!/bin/bash\nshift\nexec "$@"\n')  # Only immediate sentinels execute; no timeout semantics claimed.
        timeout.chmod(0o755)
        self.java17 = self.java(self.dir / 'jdk17/bin/java', 17)
        self.java26 = self.java(self.dir / 'jdk26/bin/java', 26)
        for name in ['StrikeArchiveReader.java', 'canonical-archive-reader.jar', 'options-edge-contracts.jar']:
            (self.dir / name).touch()
        self.env = {**os.environ, 'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
                    'CAPTURE_JSON': str(self.capture), 'JAVA_HOME': str(self.java17.parent.parent),
                    'CANONICAL_ARCHIVE_JAVA': str(self.java26), 'STRIKE_READER': '',
                    'STRIKE_READER_SRC': str(self.dir / 'StrikeArchiveReader.java'),
                    'INDICATOR_CONTRACTS_JAR': str(self.dir / 'options-edge-contracts.jar'),
                    'KAFKA_BIN': str(self.dir / 'kafka/bin'), 'CONFLUENT_HOME': str(self.dir / 'confluent'),
                    'BOOTSTRAP': '192.168.100.4:9092', 'OE_CANONICAL_REGISTRY_URL': '',
                    'OE_INDICATOR_REGISTRY_URL': 'http://unrelated-indicator-override',
                    'OE_COMMITTED_READ_TOPICS': 'unrelated'}
        names = ['is_canonical_archive_topic', 'is_committed_read_topic', 'archive_registry_url',
                 'indicator_reader_classpath', 'canonical_archive_java', 'run_strike_reader']
        self.functions = 'OE_CANONICAL_ARCHIVE_TOPICS="' + ' '.join(TOPICS) + '"\n' + ''.join(function(n) for n in names)

    def java(self, path, major):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('#!/usr/bin/env python3\nimport json,os,sys\n'
                        'if sys.argv[1:]==["-version"]:\n'
                        f' print(\'openjdk version "{major}.0.1"\',file=sys.stderr)\n'
                        'else:\n'
                        ' with open(os.environ["CAPTURE_JSON"],"a") as out:out.write(json.dumps({"binary":sys.argv[0],"args":sys.argv[1:]})+"\\n")\n')
        path.chmod(0o755)
        return path

    def run_shell(self, command, **env):
        return subprocess.run(['bash', '-c', self.functions + command], env={**self.env, **env},
                              text=True, capture_output=True, timeout=10)

    def commands(self):
        return [json.loads(row) for row in self.capture.read_text().splitlines()] if self.capture.exists() else []

    def test_all_five_bindings_force_committed_read_even_when_the_old_list_is_overridden(self):
        helper = (ROOT / 'scripts/ops/archive/CanonicalArchiveDecoder.java').read_text()
        bindings = re.findall(r'"([a-z][a-z.\-]+)",Cvd', helper)
        self.assertEqual(set(TOPICS), set(bindings))
        self.assertEqual(len(TOPICS), 5)
        for topic in TOPICS:
            self.assertEqual(self.run_shell(f'is_committed_read_topic "{topic}"').returncode, 0, topic)
        self.assertEqual(self.run_shell('is_committed_read_topic unknown').returncode, 1)

    def test_registry_follows_the_read_broker_and_a_distinct_override(self):
        for bootstrap, registry in [('192.168.100.4:9092', 'http://192.168.100.4:8081'),
                                    ('192.168.100.102:19092', 'http://192.168.100.102:8082'),
                                    ('192.168.100.252:9092', 'http://192.168.100.252:8082')]:
            r = self.run_shell('run_strike_reader 10 --topic es.futures.cvd.bars --partition 0', BOOTSTRAP=bootstrap)
            self.assertEqual(r.returncode, 0, r.stderr)
            args = self.commands()[-1]['args']
            self.assertEqual(args[args.index('--canonical-registry') + 1], registry)
            self.assertIn('-Xmx512m', args)
            self.assertIn('canonical-archive-reader.jar', args[args.index('-cp') + 1])
            self.assertEqual(args[args.index('-cp') + 1], str(self.dir / 'canonical-archive-reader.jar'))
            self.assertEqual(args[args.index('-cp') + 2], 'StrikeArchiveReader')
            self.assertNotIn(str(self.dir / 'StrikeArchiveReader.java'), args)
        r = self.run_shell('run_strike_reader 10 --topic context-tape.direction.ledger', OE_CANONICAL_REGISTRY_URL='http://explicit:8081')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.commands()[-1]['args'][-1], 'http://explicit:8081')

    def test_explicit_jdk17_and_missing_sidecars_fail_without_raw_fallback(self):
        r = self.run_shell('run_strike_reader 10 --topic es.futures.footprint.strike', CANONICAL_ARCHIVE_JAVA=str(self.java17))
        self.assertEqual(r.returncode, 2)
        self.assertEqual(self.commands(), [])
        (self.dir / 'canonical-archive-reader.jar').unlink()
        r = self.run_shell('run_strike_reader 10 --topic es.futures.cvd.bars')
        self.assertEqual(r.returncode, 2)
        self.assertIn('SDK/helper missing', r.stderr)
        self.assertEqual(self.commands(), [])

    def test_default_jdk_selection_skips_java_home17(self):
        # Redirect only the platform-specific constant into this controlled platform fixture.
        self.functions = self.functions.replace('/usr/lib/jvm/java-latest-openjdk/bin/java', str(self.java26))
        r = self.run_shell('run_strike_reader 10 --topic es.futures.footprint.bars', CANONICAL_ARCHIVE_JAVA='')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.commands()[-1]['binary'], str(self.java26))

    def test_raw_topics_and_the_existing_test_seam_keep_their_original_path(self):
        r = self.run_shell('run_strike_reader 10 --topic options.spx.vol-premium.events')
        self.assertEqual(r.returncode, 0, r.stderr)
        command = self.commands()[-1]
        self.assertEqual(command['binary'], str(self.java17))
        self.assertIn('-Xmx256m', command['args'])
        self.assertNotIn('--canonical-registry', command['args'])
        (self.dir / 'canonical-archive-reader.jar').unlink()
        r = self.run_shell('run_strike_reader 10 --topic es.futures.cvd.bars', STRIKE_READER=str(self.java17))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.commands()[-1]['args'], ['--topic', 'es.futures.cvd.bars'])

    def test_marker_uses_raw_java17_without_sdk_or_registry(self):
        (self.dir / 'canonical-archive-reader.jar').unlink()
        (self.dir / 'options-edge-contracts.jar').unlink()
        r = self.run_shell('run_strike_reader 10 --topic es.futures.cvd.bars --mark-group archive-marker --mark-offset 7', CANONICAL_ARCHIVE_JAVA='/missing/java')
        self.assertEqual(r.returncode, 0, r.stderr)
        command = self.commands()[-1]
        self.assertEqual(command['binary'], str(self.java17))
        self.assertIn('-Xmx256m', command['args'])
        self.assertNotIn('--canonical-registry', command['args'])
