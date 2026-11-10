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
RETENTION_MS = 388_800_000          # 108 h
# The window is (prior trade date 09:30 ET) -> (the latest a pod can start on that trade date). The trade date
# rolls at 18:00 ET, so the late edge is 17:59, NOT 16:00 -- which is the error that produced the first value of
# 80 h. Worst case: the Tuesday after a MONDAY HOLIDAY, Friday 09:30 -> Tuesday 17:59 = 104.5 h. An ordinary
# Monday is 80.5 h (81.5 across the autumn clock change), so 80 h was short there too.
WORST_PRIOR_OPEN_HOURS = 105
CLEANUP = ROOT / 'scripts/ops/dev-cleanup.sh'


def keep_list_as_dev_cleanup_reads_it():
    """Replicate dev-cleanup.sh's parser: every declaration at any indent, self-reference stripped."""
    lines = re.findall(r'(?m)^[ \t]*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="(.*)"$', TOPICS.read_text())
    joined = ' '.join(lines)
    for ref in ('${OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS}', '$OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS'):
        joined = joined.replace(ref, '')
    return joined.split()


def es_clean_topics():
    """The topics the 09:17 ET ESDOWN step deletes, read from dev-cleanup.sh itself."""
    m = re.search(r"(?m)^ES_CLEAN_TOPICS='([^']*)'", CLEANUP.read_text())
    assert m is not None, 'ES_CLEAN_TOPICS must be a single-quoted one-line assignment'
    return m.group(1).split()


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

    def test_the_pre_open_es_down_step_does_not_delete_the_tape(self):
        """The 09:17 ET ESDOWN step deleted this topic 13 minutes before the open, which the keep list
        alone does not prevent: it guards the 17:20 WIPE, not this path."""
        self.assertNotIn(TOPIC, es_clean_topics())

    def test_no_kept_topic_is_also_on_the_pre_open_delete_list(self):
        """The invariant, so re-adding any kept topic to either list fails the build instead of silently
        reintroducing a path that deletes it."""
        both = sorted(set(keep_list_as_dev_cleanup_reads_it()) & set(es_clean_topics()))
        self.assertEqual(both, [], f'kept topics must never be on the ESDOWN delete list: {both}')

    def test_dev_keeps_the_declared_window_and_production_is_untouched(self):
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
        # The lookback is not the binding constraint: replayHorizonMs() is the EARLIER of
        # `now - lookback` and the prior trade date's 09:30, so the retained tape is what decides.
        self.assertGreaterEqual(RETENTION_MS, WORST_PRIOR_OPEN_HOURS * 3_600_000,
                                'retention must reach the prior session open on the Tuesday after a '
                                'Monday holiday, the widest scheduled gap')

    def test_the_topic_is_still_declared_once_with_its_shape(self):
        self.assertEqual(sourced('OPTIONS_EDGE_TOPICS').count(f'{TOPIC}:32'), 1)


if __name__ == '__main__':
    unittest.main()
