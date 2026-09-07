from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CREATE_SCRIPT = ROOT / "scripts" / "kafka" / "create-hpsf-topics.sh"
VERIFY_SCRIPT = ROOT / "scripts" / "kafka" / "verify-hpsf-topics.sh"


class HpsfTopicScriptTest(unittest.TestCase):
    def rf1_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["KAFKA_TOPIC_REPLICATION_FACTOR"] = "1"
        env["KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS"] = "1"
        return env

    def test_scripts_parse(self) -> None:
        subprocess.run(["bash", "-n", str(CREATE_SCRIPT)], check=True)
        subprocess.run(["bash", "-n", str(VERIFY_SCRIPT)], check=True)

    def test_dry_run_contains_required_topics_and_rf1_config(self) -> None:
        output = subprocess.check_output([str(CREATE_SCRIPT), "--dry-run"], text=True, cwd=ROOT, env=self.rf1_env())

        for topic in [
            "options.opra.tcbbo",
            "options.opra.trades",
            "options.opra.quotes",
            "underlying.es.trades",
            "underlying.spx.price",
            "options.hpsf.underlying-state",
            "options.hpsf.market-flow",
            "options.hpsf.strike-flow",
            "options.hpsf.strike-score",
            "options.hpsf.latest-signal",
            "options.hpsf.signal",
            "options.hpsf.audit",
            "options.hpsf.dlq",
            "options.hpsf.writer-dlq",
            "options.hpsf.exit-signal",
            "options.databento.strike-flow",
            "options.ibkr.strike-flow",
        ]:
            self.assertIn(topic, output)
        self.assertIn("replication.factor=1", output)
        self.assertIn("min.insync.replicas=1", output)
        self.assertIn("compression.type=lz4", output)
        self.assertIn("options.opra.tcbbo partitions=32", output)
        self.assertIn("options.opra.trades partitions=32", output)
        self.assertIn("options.opra.quotes partitions=32", output)
        self.assertIn("options.hpsf.strike-flow partitions=32", output)
        self.assertIn("retention.ms=172800000", output)
        self.assertIn("options.hpsf.signal partitions=32", output)
        self.assertIn("cleanup.policy=delete", output)
        self.assertIn("options.hpsf.latest-signal partitions=32", output)
        self.assertIn("cleanup.policy=compact,delete", output)
        self.assertIn("options.ibkr.strike-flow partitions=32", output)

    def test_generic_topic_list_does_not_own_strike_flow_topics(self) -> None:
        topics_env = (ROOT / "scripts" / "kafka" / "topics.env").read_text()

        self.assertNotIn("options.databento.strike-flow", topics_env)
        self.assertNotIn("options.ibkr.strike-flow", topics_env)

    def test_generic_topic_list_owns_databento_volume_global_state(self) -> None:
        topics_env = (ROOT / "scripts" / "kafka" / "topics.env").read_text()

        self.assertIn("options.databento.volume.state.compacted:32", topics_env)
        self.assertIn("options.databento.volume.state.compacted", topics_env.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1])

    def test_main_deploy_pipeline_no_longer_runs_the_hpsf_topic_scripts(self) -> None:
        """HPSF is retired from dev and prod (USER, 2026-09-07), so the MAIN deploy must not
        run these scripts any more. They stay in the repo for the standalone Jenkinsfile.hpsf-*
        jobs, and the rest of this file still covers them — but wiring them into every dev and
        production deploy is what let a service nobody runs block production build #642:

            Topic options.hpsf.market-flow exists with partitions=1; expected at least 32
            Refusing topic repair in HPSF topic script.

        scripts/kafka/topics.env created those three topics at 1 partition while
        create-hpsf-topics.sh demanded 32 — two scripts in one pipeline disagreeing about the
        same topics, with no service on either side of them.
        """
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()

        self.assertNotIn("scripts/kafka/create-hpsf-topics.sh", jenkinsfile)
        self.assertNotIn("scripts/kafka/verify-hpsf-topics.sh", jenkinsfile)

    def test_script_rejects_invalid_min_isr(self) -> None:
        env = os.environ.copy()
        env["KAFKA_TOPIC_REPLICATION_FACTOR"] = "1"
        env["KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS"] = "2"
        result = subprocess.run([str(CREATE_SCRIPT), "--dry-run"], cwd=ROOT, env=env, text=True, capture_output=True)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("must be a positive integer <= replication factor 1", result.stderr)

    def test_no_forbidden_kafka_configs_in_scripts(self) -> None:
        combined = CREATE_SCRIPT.read_text() + "\n" + VERIFY_SCRIPT.read_text()

        self.assertNotIn("replication.factor=3", combined)
        self.assertNotIn("min.insync.replicas=2", combined)
        self.assertNotIn("num.standby.replicas=1", combined)


if __name__ == "__main__":
    unittest.main()
