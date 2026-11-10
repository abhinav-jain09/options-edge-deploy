# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
#
# The ES tape must survive the nightly dev wipe AND keep the window es-amt-service needs, or the service can
# never reach TRUSTED readiness: EsAmtRuntime.ready() is `ready && !retentionShort && "LIVE".equals(state)`,
# and retentionShort is set whenever the tape does not reach the PRIOR session's 09:30 ET open. On 2026-11-09
# the wipe recreated underlying.es.trades at 18:42 ET and the service sat at NOT_READY:LIVE (503) all day.
#
# The keep list is read through dev-cleanup.sh's OWN parser, not by sourcing: an indented declaration was once
# invisible to the column-0 readers while the "exactly one" check still passed (es.rithmic.mbo, 2026-11-06).
import os, pathlib, re, subprocess, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
TOPICS = ROOT / 'scripts/kafka/topics.env'
TOPIC = 'underlying.es.trades'
RETENTION_MS = 122_400_000          # 34 h
CLEANUP = ROOT / 'scripts/ops/dev-cleanup.sh'


def keep_list_as_dev_cleanup_reads_it():
    """Replicate dev-cleanup.sh's parser: every declaration at any indent, self-reference stripped."""
    lines = re.findall(r'(?m)^[ \t]*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="(.*)"$', TOPICS.read_text())
    joined = ' '.join(lines)
    for ref in ('${OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS}', '$OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS'):
        joined = joined.replace(ref, '')
    return joined.split()


def sourced(var, environment=None):
    env = dict(os.environ)
    env.pop('ENVIRONMENT', None)
    if environment is not None:
        env['ENVIRONMENT'] = environment
    out = subprocess.check_output(
        ['bash', '-c', f'source scripts/kafka/topics.env; printf "%s" "${var}"'],
        cwd=ROOT, text=True, env=env)
    return out.split()


class EsAmtTapeKeepTest(unittest.TestCase):
    def test_the_nightly_wipe_keeps_the_es_tape(self):
        keep = keep_list_as_dev_cleanup_reads_it()
        self.assertEqual(keep.count(TOPIC), 1, f'{TOPIC} must be kept exactly once; got {keep}')
        # the linearized twin is a different topic and must not be what satisfies this
        self.assertNotIn(TOPIC + '.linearized', keep)
        # the parser the clean actually uses must be the one asserted above
        self.assertIn('OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="', CLEANUP.read_text())

    def test_dev_keeps_thirty_four_hours_and_production_is_untouched(self):
        dev = sourced('OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES')
        self.assertEqual(dev.count(f'{TOPIC}={RETENTION_MS}'), 1)
        # SHARED variable: a top-level entry would shrink production's ES tape to 34 h.
        prod = sourced('OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES', environment='production')
        self.assertEqual([t for t in prod if t.startswith(f'{TOPIC}=')], [],
                         'production must inherit its own ES tape retention, not 34 h')

    def test_retention_covers_the_services_own_replay_lookback(self):
        """The number is not free: it is what es-amt-service declares it needs."""
        hours = None
        for manifest in ('k8s/services/es-amt/base/es-amt-deployment.yaml',
                         'k8s/services/es-amt/overlays/dev/manifest.yaml'):
            text = (ROOT / manifest).read_text()
            m = re.search(r'name:\s*ES_AMT_REPLAY_LOOKBACK_HOURS\s*\n\s*value:\s*"(\d+)"', text)
            self.assertIsNotNone(m, f'{manifest} must declare ES_AMT_REPLAY_LOOKBACK_HOURS')
            found = int(m.group(1))
            hours = found if hours is None else hours
            self.assertEqual(found, hours, 'base and dev must agree on the lookback')
        self.assertGreaterEqual(RETENTION_MS, hours * 3_600_000,
                                'retention must cover the replay lookback the service asks for')

    def test_the_topic_is_still_declared_once_with_its_shape(self):
        self.assertEqual(sourced('OPTIONS_EDGE_TOPICS').count(f'{TOPIC}:32'), 1)


if __name__ == '__main__':
    unittest.main()
