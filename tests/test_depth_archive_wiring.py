# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""The DEV depth archive, pinned end to end without a broker, JVM, NAS or host mutation.

ES Level Absorption Requirements section 9 (document A, capture items 1-6) and ES Book Liquidity Walls
Requirements rev 11 section 8 (document D) specify: the set OE_DEPTH_TOPICS_dev in oe-topics.env, declared
and validated by oe-archive-kafka.sh exactly as OE_ES4_TOPICS is and read through the dedicated invocation
ENV=dev TOPICS="$OE_DEPTH_TOPICS_dev"; es.rithmic.depth, futures.liquidity-wall.closures and (2026-11-05) es.rithmic.mbo
canonical Avro (CanonicalArchiveDecoder bindings under ids 3301, 3401 and 3302); underlying.es.trades.linearized committed-read
and strict; a launchd agent at 02:00, 08:00, 14:00 and 20:00 ET; dev only.
"""
import os
from pathlib import Path
import plistlib
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = ROOT / 'scripts/ops/archive'
ARCHIVER = ARCHIVE_DIR / 'oe-archive-kafka.sh'
WRAPPER = ARCHIVE_DIR / 'oe-archive-depth.sh'
TOPICS_ENV = ARCHIVE_DIR / 'oe-topics.env'
PLIST = ARCHIVE_DIR / 'launchd/oe-archive-depth.plist'
DECODER = ARCHIVE_DIR / 'CanonicalArchiveDecoder.java'
SOURCE = ARCHIVER.read_text()
DEPTH = 'es.rithmic.depth'
TAPE = 'underlying.es.trades.linearized'
CLOSURES = 'futures.liquidity-wall.closures'
MBO = 'es.rithmic.mbo'
ENGINE_TOPICS = ['es.mbo.level-flow', 'es.mbo.level-flow-final', 'es.mbo.health', 'es.mbo.closures']
SET = f'{DEPTH} {TAPE} {CLOSURES} {MBO} ' + ' '.join(ENGINE_TOPICS)


def shell_value(env_file, name):
    out = subprocess.run(['bash', '-c', f'unset {name}; . "$1"; printf "%s" "${{{name}:-}}"', '_', str(env_file)],
                         text=True, capture_output=True, timeout=10)
    assert out.returncode == 0, out.stderr
    return out.stdout


def function(name):
    match = re.search(r'^' + name + r'\(\) \{[^\n]*', SOURCE, re.M)
    assert match, name
    if match.group(0).rstrip().endswith('}'):
        return match.group(0) + '\n'
    end = SOURCE.index('\n}', match.end()) + 2
    return SOURCE[match.start():end] + '\n'


def declaration(name):
    match = re.search(r'^' + name + r'=.*$', SOURCE, re.M)
    assert match, name
    return match.group(0) + '\n'


class DepthSetPolicyTest(unittest.TestCase):
    """The set, where it is defined, and how the archiver derives it."""

    def test_the_policy_file_defines_exactly_the_eight_members_in_order(self):
        self.assertEqual(shell_value(TOPICS_ENV, 'OE_DEPTH_TOPICS_dev'), SET)
        line = re.search(r'^OE_DEPTH_TOPICS_dev="([^"]*)"$', TOPICS_ENV.read_text(), re.M)
        self.assertIsNotNone(line, 'the set must be a plain double-quoted declaration, one definition')
        self.assertEqual(line.group(1), SET)

    def test_the_archiver_discards_an_inherited_value_and_requires_it_from_the_file(self):
        unset = re.search(r'^unset .*$', SOURCE[SOURCE.index('OE_TOPICS_ENV='):], re.M).group(0)
        self.assertIn('OE_DEPTH_TOPICS_dev', unset.split())
        self.assertIn(': "${OE_DEPTH_TOPICS_dev:?oe-topics.env did not define OE_DEPTH_TOPICS_dev}"', SOURCE)
        # Same shape as the two sets it is declared beside.
        self.assertIn(': "${OE_ES4_TOPICS:?oe-topics.env did not define OE_ES4_TOPICS}"', SOURCE)
        self.assertIn(': "${OE_ALL_TOPICS_prod:?oe-topics.env did not define OE_ALL_TOPICS_prod}"', SOURCE)

    def print_topics(self, **env):
        with tempfile.TemporaryDirectory(prefix='depth-policy-') as temp:
            base = {**os.environ, 'ARCHIVE_DIR': temp, 'PRINT_TOPICS': 'true', 'KAFKA_BIN': temp}
            base.pop('TOPICS', None)
            return subprocess.run(['bash', str(ARCHIVER)], env={**base, **env}, text=True, capture_output=True, timeout=20)

    def test_the_dedicated_invocation_derives_exactly_the_set(self):
        r = self.print_topics(ENV='dev', TOPICS=SET)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), SET)

    def test_env_dev_without_the_override_is_not_the_depth_set(self):
        # The override is load-bearing: ENV=dev keeps the prod default, so a wrapper that forgot TOPICS
        # would archive something else against the dev broker.
        r = self.print_topics(ENV='dev')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotEqual(r.stdout.strip(), SET)
        self.assertEqual(r.stdout.strip(), shell_value(TOPICS_ENV, 'OE_ALL_TOPICS_prod'))

    def test_a_policy_file_without_the_set_is_refused_and_says_why(self):
        with tempfile.TemporaryDirectory(prefix='depth-policy-') as temp:
            stripped = Path(temp) / 'oe-topics.env'
            stripped.write_text(''.join(l for l in TOPICS_ENV.read_text().splitlines(True)
                                        if not l.startswith('OE_DEPTH_TOPICS_dev=')))
            r = self.print_topics(ENV='dev', TOPICS=SET, OE_TOPICS_ENV=str(stripped), OE_DEPTH_TOPICS_dev=SET)
        self.assertNotEqual(r.returncode, 0, 'an inherited value must not satisfy the check')
        self.assertIn('OE_DEPTH_TOPICS_dev', r.stderr)


class DepthClassificationTest(unittest.TestCase):
    """How the archiver routes each member: through its own predicate functions, defaults included."""

    def setUp(self):
        names = ['is_strict_topic', 'is_canonical_archive_topic', 'is_committed_read_topic', 'is_as_written_topic']
        decls = ['OE_STRICT_TOPICS', 'OE_VOL_PREMIUM_TRANSACTIONAL_TOPICS', 'OE_COMMITTED_READ_TOPICS',
                 'OE_CANONICAL_ARCHIVE_TOPICS', 'OE_COMMITTED_READ_AS_WRITTEN_TOPICS']
        self.prelude = ''.join(declaration(d) for d in decls) + ''.join(function(n) for n in names)
        self.env = {k: v for k, v in os.environ.items()
                    if k not in {'OE_STRICT_TOPICS', 'OE_COMMITTED_READ_TOPICS', 'OE_COMMITTED_READ_AS_WRITTEN_TOPICS'}}

    def predicate(self, name, topic):
        return subprocess.run(['bash', '-c', self.prelude + f'{name} "{topic}"'], env=self.env,
                              text=True, capture_output=True, timeout=10).returncode == 0

    def test_the_three_avro_topics_are_canonical_and_therefore_committed_read(self):
        for topic in (DEPTH, CLOSURES, MBO, *ENGINE_TOPICS):
            self.assertTrue(self.predicate('is_canonical_archive_topic', topic), topic)
            self.assertTrue(self.predicate('is_committed_read_topic', topic), topic)
            self.assertFalse(self.predicate('is_as_written_topic', topic), topic)
        canonical = re.search(r'^OE_CANONICAL_ARCHIVE_TOPICS="([^"]+)"$', SOURCE, re.M).group(1).split()
        self.assertEqual(canonical[-7:], [DEPTH, CLOSURES, MBO, *ENGINE_TOPICS], 'appended to the canonical set, the five older members untouched')

    def test_the_tape_is_committed_read_and_strict_but_not_canonical_or_as_written(self):
        self.assertTrue(self.predicate('is_committed_read_topic', TAPE))
        self.assertTrue(self.predicate('is_strict_topic', TAPE))
        self.assertFalse(self.predicate('is_canonical_archive_topic', TAPE))
        self.assertFalse(self.predicate('is_as_written_topic', TAPE), 'the proven five-field layout applies')
        # In the DEFAULTS, so a scheduled run that sets neither variable gets them.
        self.assertIn(TAPE, declaration('OE_STRICT_TOPICS'))
        self.assertIn(TAPE, declaration('OE_COMMITTED_READ_TOPICS'))
        # The older strict member is still there.
        self.assertTrue(self.predicate('is_strict_topic', 'context-tape.direction.ledger'))

    def test_canonical_membership_cannot_be_overridden_from_the_environment(self):
        # The canonical set is hard-set (no ${VAR:-} default), so an inherited empty value cannot route the
        # Avro topics back to the binary-unsafe console path.
        self.assertIsNotNone(re.search(r'^OE_CANONICAL_ARCHIVE_TOPICS="es\.futures', SOURCE, re.M))
        env = {**self.env, 'OE_CANONICAL_ARCHIVE_TOPICS': ''}
        r = subprocess.run(['bash', '-c', self.prelude + f'is_canonical_archive_topic "{DEPTH}"'], env=env,
                           text=True, capture_output=True, timeout=10)
        self.assertEqual(r.returncode, 0)


class DecoderBindingTest(unittest.TestCase):
    """The decoder source binds all three records (depth frame, MBO event, wall closure) under their released schemas and refuses a foreign id."""

    def test_all_three_records_are_bound_beside_the_five_older_bindings(self):
        text = DECODER.read_text()
        bindings = text[text.index('BINDINGS=Map.of('):text.index(');', text.index('BINDINGS=Map.of('))]
        self.assertIn('RithmicSchemas.RAW_TOPIC,RithmicSchemas.FRAME', bindings)
        self.assertIn('"futures.liquidity-wall.closures",LiquidityWallSchemas.CLOSURE.schema()', bindings)
        self.assertIn('RithmicMboSchemas.TOPIC,RithmicMboSchemas.EVENT', bindings)
        for older in ('"es.futures.cvd.bars",CvdSchemas.BAR', '"es.futures.footprint.bars",CvdSchemas.FOOTPRINT_BAR',
                      '"es.futures.footprint.outcomes",CvdSchemas.FOOTPRINT_OUTCOME',
                      '"es.futures.footprint.strike",CvdRelatedSchemas.FOOTPRINT_STRIKE',
                      '"context-tape.direction.ledger",CvdRelatedSchemas.directionLedger()'):
            self.assertIn(older, bindings)
        self.assertIn('import com.optionsedge.contracts.rithmic.RithmicSchemas;', text)
        self.assertIn('import com.optionsedge.contracts.rithmic.RithmicMboSchemas;', text)
        self.assertIn('import com.optionsedge.contracts.liquiditywall.LiquidityWallSchemas;', text)

    def test_the_fixed_ids_are_the_registered_ones_and_checked_before_decoding(self):
        text = DECODER.read_text()
        self.assertIn('RITHMIC_DEPTH_FRAME_ID=3301', text)
        fixed = text[text.index('FIXED_IDS=Map.of('):text.index(');', text.index('FIXED_IDS=Map.of('))]
        self.assertIn('RithmicSchemas.RAW_TOPIC,RITHMIC_DEPTH_FRAME_ID', fixed)
        self.assertIn('"futures.liquidity-wall.closures",LiquidityWallSchemas.CLOSURE.id()', fixed)
        self.assertIn('RithmicMboSchemas.TOPIC,RithmicMboSchemas.SCHEMA_ID', fixed)
        deserialize = text[text.index('public byte[] deserialize(String topic,Headers headers,byte[] value)'):]
        self.assertLess(deserialize.index('framedId(value,fixedId,topic)'), deserialize.index('return values.deserialize(topic,headers,value);'),
                        'the header check runs before the SDK decode, so a foreign id never reaches it')

    def test_the_engine_book_topics_are_bound_to_the_sets_of_ids_they_carry(self):
        text = DECODER.read_text()
        versioned = text[text.index('VERSIONED=Map.of('):text.index(');', text.index('VERSIONED=Map.of('))]
        self.assertIn('import com.optionsedge.contracts.esmbo.EsMboSchemas;', text)
        self.assertIn('"es.mbo.level-flow",Map.of(EsMboSchemas.LEVEL_FLOW.id(),EsMboSchemas.LEVEL_FLOW.schema())', versioned)
        # both registered versions of the final rows and of health: version 1 stays readable for its one-day retention
        for name in ('LEVEL_FLOW_FINAL', 'HEALTH'):
            self.assertIn(f'EsMboSchemas.{name}.priorId(),EsMboSchemas.{name}.priorSchema(),EsMboSchemas.{name}.id(),EsMboSchemas.{name}.schema()', versioned.replace('\n', '').replace('  ', ''))
        # both closure kinds live on the one compacted topic
        self.assertIn('EsMboSchemas.CLOSURE.id(),EsMboSchemas.CLOSURE.schema(),EsMboSchemas.CLOSURE_FINAL.id(),EsMboSchemas.CLOSURE_FINAL.schema()', versioned.replace('\n', '').replace('  ', ''))
        # a versioned topic is decoded by the strict decoder of the frame's own id, and an id outside the set is refused before any decoding
        self.assertIn('framedOneOf(value,versioned.keySet(),topic)', text)
        self.assertIn('expected one of the fixed ids', text)

    def test_the_negative_control_anchors_are_still_unique(self):
        # tests/archive-canonical/negative_controls.py mutates these exact lines; each must still occur once.
        text = DECODER.read_text()
        for anchor in ('Schema schema=BINDINGS.get(topic);',
                       'if(!this.topic.equals(topic))throw new IllegalArgumentException("Archive decoder topic mismatch");',
                       'topic.equals("es.futures.footprint.strike")?Set.of(topic):Set.of()',
                       'return values.deserialize(topic,headers,value);'):
            self.assertEqual(text.count(anchor), 1, anchor)

    def test_the_contracts_checkout_fixes_the_same_ids(self):
        # The contracts repo this deploy builds the helper from states the ids; when it is checked out beside
        # the repo (CI stages it under .jenkins-tmp/contracts) the two must agree. Skipped when it is absent.
        for candidate in (ROOT / '.jenkins-tmp/contracts', ROOT.parent / 'contracts-work'):
            holder = candidate / 'src/main/java/com/optionsedge/contracts/liquiditywall/LiquidityWallSchemas.java'
            readme = candidate / 'schemas/rithmic/README.md'
            mbo = candidate / 'src/main/java/com/optionsedge/contracts/rithmic/RithmicMboSchemas.java'
            if holder.exists() and readme.exists() and mbo.exists():
                self.assertIn('new Subject("LiquidityWallClosure", 3401)', holder.read_text())
                self.assertIn('fixed schema ID 3301', readme.read_text())
                self.assertIn('SCHEMA_ID=3302', mbo.read_text())
                return
        self.skipTest('no contracts checkout beside the repo')


class LaunchdAgentTest(unittest.TestCase):
    """The plist: the schedule the requirement names, the wrapper this repo ships, nothing pinned."""

    def setUp(self):
        self.plist = plistlib.load(PLIST.open('rb'))

    def test_four_captures_a_day_at_the_named_hours(self):
        self.assertEqual([(e['Hour'], e['Minute']) for e in self.plist['StartCalendarInterval']],
                         [(2, 0), (8, 0), (14, 0), (20, 0)])
        self.assertIs(self.plist['RunAtLoad'], False)

    def test_it_runs_the_wrapper_from_the_installers_destination(self):
        interp, script = self.plist['ProgramArguments'][:2]
        self.assertEqual(Path(script).name, 'oe-archive-depth.sh')
        self.assertEqual(Path(script).parent.name, 'oe-ops')
        self.assertTrue(interp.endswith('/bash'))
        self.assertEqual(self.plist['Label'], 'com.optionsedge.oe-archive-depth')

    def test_the_environment_is_explicit_and_pins_neither_root_nor_set(self):
        env = self.plist['EnvironmentVariables']
        self.assertIn('PATH', env)
        self.assertNotIn('ARCHIVE_DIR', env, 'the wrapper resolves the root and proves the sentinel (BZ 360)')
        self.assertNotIn('TOPICS', env, 'the policy file is the one definition of the set')
        self.assertIn('install-dev-mac-depth-archive.sh', PLIST.read_text())


class WrapperInvocationTest(unittest.TestCase):
    """The wrapper, driven against a stub archiver that records the environment it was given."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='depth-wrapper-')
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.nas = self.dir / 'nas'
        self.nas.mkdir()
        (self.nas / '.oe_nas_sentinel').write_text('oe-nas\n')
        self.capture = self.dir / 'archiver.env'
        self.stub = self.dir / 'archiver.sh'
        self.stub.write_text('#!/bin/bash\nenv > "$CAPTURE"\nexit "${STUB_RC:-0}"\n')
        self.stub.chmod(0o755)
        self.log = self.dir / 'depth.log'

    def run_wrapper(self, **env):
        base = {'PATH': os.environ['PATH'], 'HOME': str(self.dir), 'ARCHIVER': str(self.stub),
                'CAPTURE': str(self.capture), 'LOG': str(self.log), 'OE_TOPICS_ENV': str(TOPICS_ENV)}
        return subprocess.run(['bash', str(WRAPPER)], env={**base, **env}, text=True, capture_output=True, timeout=20)

    def archiver_env(self):
        return dict(line.split('=', 1) for line in self.capture.read_text().splitlines() if '=' in line)

    def test_the_dedicated_invocation_reaches_the_archiver(self):
        r = self.run_wrapper(ARCHIVE_DIR=str(self.nas))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        got = self.archiver_env()
        self.assertEqual(got['ENV'], 'dev')
        self.assertEqual(got['TOPICS'], SET)
        self.assertEqual(got['ARCHIVE_JOB'], 'depth')
        self.assertEqual(got['ON_LOCK_BUSY'], 'fail')
        self.assertEqual(got['BOOTSTRAP'], '192.168.100.102:19092')
        self.assertEqual(got['ARCHIVE_DIR'], str(self.nas))
        self.assertEqual(got['ALLOW_NON_NAS'], 'true', 'a non-/mnt/nas root is allowed only after the sentinel was read')
        self.assertNotIn('SESSION_DATE', got)
        self.assertNotIn('UNTIL_TS', got)

    def test_the_root_is_resolved_from_candidates_when_not_set(self):
        r = self.run_wrapper(ARCHIVE_ROOT_CANDIDATES=f'{self.dir}/absent {self.nas}')
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.archiver_env()['ARCHIVE_DIR'], str(self.nas))
        self.assertIn('resolved from ARCHIVE_ROOT_CANDIDATES', r.stdout)

    def test_no_sentinel_means_no_capture(self):
        (self.nas / '.oe_nas_sentinel').unlink()
        r = self.run_wrapper(ARCHIVE_DIR=str(self.nas))
        self.assertNotEqual(r.returncode, 0)
        self.assertFalse(self.capture.exists(), 'the archiver must not even be started')
        self.assertIn('sentinel', r.stdout + r.stderr)

    def test_no_root_at_all_means_no_capture(self):
        r = self.run_wrapper(ARCHIVE_ROOT_CANDIDATES=f'{self.dir}/absent')
        self.assertNotEqual(r.returncode, 0)
        self.assertFalse(self.capture.exists())
        self.assertIn('no archive root', r.stdout + r.stderr)

    def test_a_policy_file_without_the_set_is_refused_even_with_an_inherited_value(self):
        stripped = self.dir / 'oe-topics.env'
        stripped.write_text(''.join(l for l in TOPICS_ENV.read_text().splitlines(True)
                                    if not l.startswith('OE_DEPTH_TOPICS_dev=')))
        r = self.run_wrapper(ARCHIVE_DIR=str(self.nas), OE_TOPICS_ENV=str(stripped), OE_DEPTH_TOPICS_dev=SET)
        self.assertNotEqual(r.returncode, 0)
        self.assertFalse(self.capture.exists())
        self.assertIn('OE_DEPTH_TOPICS_dev', r.stdout + r.stderr)

    def test_an_archiver_failure_is_the_wrappers_failure(self):
        r = self.run_wrapper(ARCHIVE_DIR=str(self.nas), STUB_RC='3')
        self.assertEqual(r.returncode, 1)
        self.assertIn('FAILED', self.log.read_text())
        self.assertIn('24h', self.log.read_text())
        self.assertIn('three more runs', self.log.read_text())


class DevOnlyAndGuardsTest(unittest.TestCase):
    """Nothing for prod or es4; the exemption from the unit check is bound by its own guard; CI runs both."""

    def test_the_eight_topics_are_in_no_prod_or_es4_set(self):
        for name in ('OE_ES4_TOPICS', 'OE_ALL_TOPICS_prod', 'OE_HEAVY_TOPICS_prod', 'OE_SPOT_TOPICS'):
            members = shell_value(TOPICS_ENV, name).split()
            for topic in SET.split():
                self.assertNotIn(topic, members, f'{topic} in {name}')
        self.assertNotIn('oe-archive-depth', (ARCHIVE_DIR / 'oe-archive.crontab').read_text())

    def test_the_unit_completeness_check_exempts_the_dev_mac_files_and_names_the_guard(self):
        text = (ROOT / 'scripts/ci/validate-archive-unit-completeness.sh').read_text()
        exempt = re.search(r'^EXEMPT="([^"]*)"$', text, re.M).group(1).split()
        for f in ('oe-archive-depth.sh', 'install-dev-mac-depth-archive.sh', 'install-dev-mac-depth-archive-test.sh'):
            self.assertIn(f, exempt)
        self.assertIn('scripts/ci/validate-dev-mac-depth-archive.sh', text)
        self.assertTrue((ROOT / 'scripts/ci/validate-dev-mac-depth-archive.sh').exists())

    def test_the_deploy_job_runs_the_guard_and_this_module(self):
        job = (ROOT / 'Jenkinsfile.archive-scripts-deploy').read_text()
        self.assertIn('bash scripts/ci/validate-dev-mac-depth-archive.sh', job)
        self.assertIn('tests.test_depth_archive_wiring', job)
        unit = re.search(r'UNIT\s*=\s*"([^"]*)"', job).group(1).split()
        self.assertNotIn('oe-archive-depth.sh', unit, 'a dev-Mac launchd job is not shipped to .252')

    def test_the_guard_passes_on_the_committed_tree(self):
        r = subprocess.run(['bash', str(ROOT / 'scripts/ci/validate-dev-mac-depth-archive.sh')], cwd=ROOT,
                           text=True, capture_output=True, timeout=120)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn('validate-dev-mac-depth-archive: OK', r.stdout)

    def test_script_syntax(self):
        for script in (ARCHIVER, WRAPPER, ARCHIVE_DIR / 'install-dev-mac-depth-archive.sh',
                       ARCHIVE_DIR / 'install-dev-mac-depth-archive-test.sh', ROOT / 'scripts/ci/validate-dev-mac-depth-archive.sh'):
            self.assertEqual(subprocess.run(['bash', '-n', str(script)], capture_output=True).returncode, 0, script)


if __name__ == '__main__':
    unittest.main()
