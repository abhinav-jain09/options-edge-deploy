"""Executes the es-indicator-mirror install's parity gate (the block between the '--- parity gate begin/end ---'
markers of Jenkinsfile.es-indicator-mirror, unescaped the way Groovy hands it to sh) against a scenario-driven
stand-in for kafka-get-offsets and kafka-console-consumer (tests/fake_mirror_parity_cli.py).

Why this exists (2026-10-18): the es4 indicator producer is transactional, so the source end offset counts commit
markers that MirrorMaker never copies; the gate must count committed source records instead, and it must fail
closed on the console consumer, which exits 0 on every error."""
import json, os, pathlib, shutil, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests/fake_mirror_parity_cli.py"
SRC, TGT, TOPIC = "192.168.100.4:9092", "127.0.0.1:19092", "es.options.indicators.snapshot.current"


def gate_block() -> str:
    t = (ROOT / "Jenkinsfile.es-indicator-mirror").read_text()
    b = t[t.index("# --- parity gate begin"):t.index("# --- parity gate end ---")]
    # Groovy '''...''' hands \\n and \\t to the shell as \n and \t
    return b.replace("\\\\n", "\\n").replace("\\\\t", "\\t").replace("\\\\(", "\\(").replace("\\\\)", "\\)").replace("\\\\1", "\\1")


def evens(lo, hi):
    """Committed data-record offsets of a transactional producer: record, marker, record, marker ..."""
    return list(range(lo, hi, 2))


def run(scenario: dict, timeout_s: int | None = None):
    d = pathlib.Path(tempfile.mkdtemp(prefix="parity-gate-"))
    kbin = d / "kbin"; kbin.mkdir()
    for n in ("kafka-get-offsets", "kafka-console-consumer"):
        shutil.copy(FAKE, kbin / n); os.chmod(kbin / n, 0o755)
    (kbin / "sleep").write_text("#!/bin/bash\nexit 0\n"); os.chmod(kbin / "sleep", 0o755)   # the retry pause
    mdir = d / "mdir"; mdir.mkdir()
    (d / "scenario.json").write_text(json.dumps(scenario))
    calls = d / "calls.log"; calls.write_text("")
    script = "set -euo pipefail\n" + gate_block()
    env = dict(os.environ, PATH=f"{kbin}:{os.environ['PATH']}", KBIN=str(kbin), SRC=SRC, TGT=TGT, TOPIC=TOPIC, MDIR=str(mdir),
               FAKE_PARITY_SCENARIO=str(d / "scenario.json"), FAKE_PARITY_STATE=str(d / "state.json"),
               FAKE_PARITY_CALLS=str(calls), FAKE_SRC=SRC)
    if timeout_s is not None:
        env["PARITY_CONSUMER_TIMEOUT_S"] = str(timeout_s)
    r = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text()


def one(begin, end, records, target_ends, **extra):
    s = {"source": {"0": {"begin": begin, "end": end, "records": records}}, "target": {"0": target_ends}}
    s.update(extra)
    return s


class ParityGateTest(unittest.TestCase):
    def test_live_single_partition_parity_passes_on_the_first_attempt(self):
        # 451 records + 451 markers on the source (end 902); the target holds the 451 records
        rc, out, calls = run(one(0, 902, evens(0, 902), [451]))
        self.assertEqual(rc, 0, out)
        self.assertIn("parity attempt 1: p0:targetEnd=451 sourceRecordsBelowEnd=451", out)
        self.assertIn("--max-messages 452", calls, "the count is bounded at TE+1")
        self.assertIn("--offset 0", calls)

    def test_a_trailing_copy_is_re_measured_and_passes_once_caught_up(self):
        rc, out, _ = run(one(0, 902, evens(0, 902), [449, 451]))
        self.assertEqual(rc, 0, out)
        self.assertIn("parity attempt 1: p0:targetEnd=449 sourceRecordsBelowEnd=450", out)
        self.assertIn("parity attempt 2: p0:targetEnd=451 sourceRecordsBelowEnd=451", out)

    def test_a_target_with_more_records_than_the_source_committed_fails_at_once(self):
        rc, out, _ = run(one(0, 902, evens(0, 902), [452]))
        self.assertEqual(rc, 1, out)
        self.assertIn("a duplicate copy or a second writer", out)
        self.assertNotIn("parity attempt 2", out)

    def test_an_empty_source_passes_without_consuming(self):
        rc, out, calls = run(one(0, 0, [], [0]))
        self.assertEqual(rc, 0, out)
        self.assertNotIn("kafka-console-consumer", calls)

    def test_a_non_zero_earliest_offset_is_where_the_count_starts(self):
        rc, out, calls = run(one(100, 300, evens(100, 300), [100]))
        self.assertEqual(rc, 0, out)
        self.assertIn("--offset 100", calls)
        self.assertIn("(earliest=100 end=300)", out)

    def test_the_earliest_offset_is_re_read_on_every_attempt(self):
        # attempt 1 trails (target 99 of 100); retention then advances the earliest offset to 50, leaving 75 records
        # — the second attempt must count from 50 (a stale earliest offset would count 100 and never pass)
        rc, out, calls = run(one(0, 200, evens(0, 200), [99, 75], begin_sequence={"0": [0, 50]}))
        self.assertEqual(rc, 0, out)
        self.assertIn("parity attempt 2: p0:targetEnd=75 sourceRecordsBelowEnd=75 (earliest=50", out)
        self.assertIn("--offset 50", calls)

    def test_eight_partitions_all_at_parity_pass_and_one_lagging_forever_fails(self):
        source = {str(p): {"begin": 0, "end": 2 * (10 + p), "records": evens(0, 2 * (10 + p))} for p in range(8)}
        target = {str(p): [10 + p] for p in range(8)}
        rc, out, _ = run({"source": source, "target": target})
        self.assertEqual(rc, 0, out)
        for p in range(8):
            self.assertIn(f"p{p}:targetEnd={10 + p} sourceRecordsBelowEnd={10 + p}", out)
        target["5"] = [14]   # one record short, forever
        rc, out, _ = run({"source": source, "target": target})
        self.assertEqual(rc, 1, out)
        self.assertIn("parity attempt 20", out)
        self.assertIn("has not caught up", out)

    def test_a_consumer_that_exits_non_zero_fails_the_gate(self):
        rc, out, _ = run(one(0, 902, evens(0, 902), [451], consumer={"exit": 1}))
        self.assertEqual(rc, 1, out)
        self.assertIn("consumer exit status 1", out)

    def test_a_consumer_that_terminates_on_an_error_with_exit_zero_fails_the_gate(self):
        rc, out, _ = run(one(0, 902, evens(0, 902), [451], consumer={"error": "KafkaException: broker gone"}))
        self.assertEqual(rc, 1, out)
        self.assertIn("terminated on an error", out)

    def test_an_error_is_not_excused_by_a_timeout_line_elsewhere_in_stderr(self):
        # a KafkaException whose diagnostics also mention TimeoutException: only the exact idle-timeout event passes
        rc, out, _ = run(one(0, 902, evens(0, 902), [451],
                             consumer={"error": "KafkaException: broker gone\norg.apache.kafka.common.errors.TimeoutException"}))
        self.assertEqual(rc, 1, out)
        self.assertIn("other than the idle timeout", out)

    def test_two_termination_events_fail_even_when_one_is_the_idle_timeout(self):
        rc, out, _ = run(one(0, 902, evens(0, 902), [451],
                             consumer={"error": "TimeoutException\n[ERROR] second event (kafka.tools.ConsoleConsumer$)\norg.apache.kafka.common.errors.TimeoutException"}))
        self.assertEqual(rc, 1, out)
        self.assertIn("error events", out)

    def test_the_idle_timeout_event_alone_is_accepted(self):
        # the stand-in emits the measured idle-timeout signature whenever fewer than max-messages records exist
        rc, out, _ = run(one(0, 20, evens(0, 20), [10]))
        self.assertEqual(rc, 0, out)

    def test_a_duplicated_target_on_a_live_topic_is_judged_once_the_consumer_read_past_the_source_end(self):
        # target holds 451 but the source committed 226 below its end (a re-install copied everything twice);
        # TE+1 = 452 exceeds the committed count, so on a live topic the consumer runs into the wall-time bound —
        # the live record at offset 904 (>= SE 902) proves every committed record below SE was read: N=226 < TE → FAIL
        recs = evens(0, 452) + [904]
        rc, out, _ = run(one(0, 902, recs, [451], consumer={"hang_s": 3}), timeout_s=1)
        self.assertEqual(rc, 1, out)
        self.assertIn("consumer stopped at the wall-time bound after reading past the source end (904 >= 902)", out)
        self.assertIn("a duplicate copy or a second writer", out)

    def test_a_consumer_killed_before_the_source_end_is_inconclusive_and_fails(self):
        rc, out, _ = run(one(0, 902, evens(0, 452), [451], consumer={"hang_s": 3}), timeout_s=1)
        self.assertEqual(rc, 1, out)
        self.assertIn("did not reach the source end offset 902", out)

    def test_a_consumer_whose_total_disagrees_with_its_output_fails_the_gate(self):
        rc, out, _ = run(one(0, 902, evens(0, 902), [451], consumer={"total_off_by": -3}))
        self.assertEqual(rc, 1, out)
        self.assertIn("not trustworthy", out)

    def test_an_earliest_offset_the_broker_no_longer_holds_fails_instead_of_resetting(self):
        # the stand-in answers --time -2 with 0 but refuses to start there (retention advanced in between)
        rc, out, _ = run(one(0, 902, evens(100, 902), [401], begin_sequence={"0": [0]}) | {"source": {"0": {"begin": 100, "end": 902, "records": evens(100, 902)}}})
        self.assertEqual(rc, 1, out)
        self.assertIn("no longer holds the earliest offset 0", out)


if __name__ == "__main__":
    unittest.main()
