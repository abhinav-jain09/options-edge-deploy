"""scripts/kafka/ensure-indicator-topics.sh --recreate against the stand-in Kafka CLIs and the REAL topics.env:
the allow-list is environment-qualified (six on es4, ten on dev and production), every topic comes back empty at the
shape topics.env declares through the shared shape library (apply-topics.sh's functions), a bogus environment and
an undeclared name are refused, and a dry run deletes nothing."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-indicator-topics.sh"
FAKE = ROOT / "tests/fake_kafka_cli.py"
BASE6 = ["options.indicators.snapshot.current", "options.indicators.bars", "options.indicators.bootstrap.control",
         "options.indicators.signals.current", "options.indicators.signals.events", "options.indicators.repartition"]
MIRROR4 = ["es.options.indicators.snapshot.current", "es.options.indicators.bars",
           "es.options.indicators.signals.current", "es.options.indicators.signals.events"]


def run(environment: str, existing: dict, dry: bool = False, extra_env: dict | None = None):
    tmp = Path(tempfile.mkdtemp())
    bin_dir = tmp / "bin"
    bin_dir.mkdir()
    for n in ["kafka-topics", "kafka-configs", "kafka-get-offsets", "kafka-broker-api-versions"]:
        shutil.copy(FAKE, bin_dir / n)
        (bin_dir / n).chmod(0o755)
    state = tmp / "state.json"
    state.write_text(json.dumps({"topics": existing}))
    calls = tmp / "calls"
    calls.touch()
    env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls),
               KAFKA_BOOTSTRAP_SERVERS="b:1", KAFKA_TOPIC_REPLICATION_FACTOR="1", KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS="1",
               KAFKA_TOPIC_RETENTION_MS="86400000", RECREATE_ENVIRONMENT=environment, DEPLOY_DRY_RUN="true" if dry else "false",
               KAFKA_TOPIC_DELETE_WAIT_SECONDS="2", KAFKA_TOPIC_REPAIR_WAIT_SECONDS="2", INDICATOR_APP_ID="indicator-service-x")
    env.update(extra_env or {})
    r = subprocess.run(["bash", str(SCRIPT), "--recreate"], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), json.loads(state.read_text())["topics"]


def retained(names, end=500):
    return {n: {"partitions": 32, "rf": 1, "configs": {"cleanup.policy": "delete", "retention.ms": "-1"}, "end": end} for n in names}


class EnsureIndicatorTopicsRecreate(unittest.TestCase):
    def test_dev_recreates_the_ten_topics_empty_at_the_declared_shapes(self):
        rc, out, calls, topics = run("dev", retained(BASE6 + MIRROR4 + ["unrelated.topic"]))
        self.assertEqual(rc, 0, out)
        deleted = [c.split("--topic ")[1].split()[0] for c in calls if c.startswith("kafka-topics") and "--delete" in c]
        self.assertEqual(sorted(deleted), sorted(BASE6 + MIRROR4), "exactly the allow-list, never another name")
        self.assertIn("unrelated.topic", topics)
        self.assertEqual(topics["unrelated.topic"]["end"], 500)
        for n in BASE6 + MIRROR4:
            self.assertEqual(topics[n]["end"], 0, n)
        # the shapes topics.env declares (dev: the base snapshot is compacted; the es.* mirror targets are plain delete,
        # as topics.env declares them — the mirror job pre-creates them the same way)
        self.assertEqual(topics["options.indicators.snapshot.current"]["partitions"], 1)
        self.assertEqual(topics["options.indicators.bars"]["partitions"], 8)
        self.assertEqual(topics["options.indicators.repartition"]["partitions"], 4)
        self.assertEqual(topics["options.indicators.snapshot.current"]["configs"]["cleanup.policy"], "compact,delete")
        self.assertEqual(topics["options.indicators.snapshot.current"]["configs"]["retention.ms"], "604800000")
        self.assertEqual(topics["options.indicators.bars"]["configs"]["cleanup.policy"], "delete")
        self.assertEqual(topics["options.indicators.bars"]["configs"]["retention.ms"], "3888000000")
        self.assertEqual(topics["options.indicators.bootstrap.control"]["configs"]["retention.ms"], "-1")
        self.assertEqual(topics["es.options.indicators.snapshot.current"]["configs"]["cleanup.policy"], "delete")
        self.assertEqual(topics["es.options.indicators.snapshot.current"]["configs"]["retention.ms"], "604800000")
        self.assertEqual(topics["es.options.indicators.bars"]["configs"]["retention.ms"], "3888000000")
        self.assertIn("recreated empty at the declared shape", out)

    def test_production_drops_compaction_where_topics_env_says_so(self):
        rc, out, calls, topics = run("production", retained(BASE6 + MIRROR4))
        self.assertEqual(rc, 0, out)
        # OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS: served topics are not compacted on prod (archive fidelity)
        self.assertEqual(topics["options.indicators.snapshot.current"]["configs"]["cleanup.policy"], "delete")
        self.assertEqual(topics["es.options.indicators.snapshot.current"]["configs"]["cleanup.policy"], "delete")
        self.assertEqual(topics["es.options.indicators.signals.current"]["configs"]["cleanup.policy"], "delete")
        self.assertEqual(len([c for c in calls if "--delete" in c]), 10)

    def test_es4_recreates_its_six_source_topics_only(self):
        es4 = ["es." + n for n in BASE6]
        rc, out, calls, topics = run("es4", retained(es4 + ["es.other"]))
        self.assertEqual(rc, 0, out)
        deleted = [c.split("--topic ")[1].split()[0] for c in calls if c.startswith("kafka-topics") and "--delete" in c]
        self.assertEqual(sorted(deleted), sorted(es4))
        self.assertEqual(topics["es.options.indicators.bars"]["partitions"], 8)
        self.assertEqual(topics["es.options.indicators.repartition"]["partitions"], 4)
        # es4's own declaration set (OPTIONS_EDGE_ES4_COMPACTED_TOPICS) compacts none of the indicator topics
        self.assertEqual(topics["es.options.indicators.snapshot.current"]["configs"]["cleanup.policy"], "delete")
        self.assertEqual(topics["es.options.indicators.bootstrap.control"]["configs"]["cleanup.policy"], "delete")

    def test_absent_topics_are_created_and_a_bogus_environment_is_refused(self):
        rc, out, calls, topics = run("es4", {})
        self.assertEqual(rc, 0, out)
        self.assertEqual(len([c for c in calls if "--delete" in c]), 0)
        self.assertEqual(len([c for c in calls if "--create" in c]), 6)
        rc, out, calls, _ = run("staging", retained(BASE6))
        self.assertEqual(rc, 1)
        self.assertIn("RECREATE_ENVIRONMENT must be dev, production or es4", out)
        self.assertEqual(calls, [])

    def test_an_end_offset_line_missing_for_one_partition_fails_the_recreate(self):
        rc, out, _, _ = run("es4", retained(BASE6), extra_env={"FAKE_KAFKA_OFFSETS_DROP_PARTITION": "es.options.indicators.bars:3"})
        self.assertEqual(rc, 1, out)
        self.assertIn("FAIL: es.options.indicators.bars after recreate", out)
        self.assertIn("want [", out)

    def test_dry_run_deletes_and_creates_nothing(self):
        rc, out, calls, topics = run("dev", retained(BASE6 + MIRROR4), dry=True)
        self.assertEqual(rc, 0, out)
        self.assertFalse(any("--delete" in c or "--create" in c or "--alter" in c for c in calls))
        self.assertEqual(topics["options.indicators.bars"]["end"], 500)


if __name__ == "__main__":
    unittest.main()
