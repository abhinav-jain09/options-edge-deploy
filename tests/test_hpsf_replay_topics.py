from __future__ import annotations

import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CREATE_REPLAY_TOPICS = ROOT / "scripts" / "hpsf" / "create-replay-topics-20260612.sh"


class HpsfReplayTopicScriptTest(unittest.TestCase):
    def test_ReplayTopicCreationRf1Test(self) -> None:
        subprocess.run(["bash", "-n", str(CREATE_REPLAY_TOPICS)], check=True)
        output = subprocess.check_output([str(CREATE_REPLAY_TOPICS), "--dry-run"], text=True, cwd=ROOT)

        self.assertIn("replication.factor=1", output)
        self.assertIn("min.insync.replicas=1", output)
        self.assertIn("compression.type=lz4", output)
        for topic in [
            "options.replay.20260612.opra.tcbbo",
            "underlying.replay.20260612.es.trades",
            "underlying.replay.20260612.spx.price",
            "options.replay.20260612.hpsf.underlying-state",
            "options.replay.20260612.hpsf.strike-flow",
            "options.replay.20260612.hpsf.market-flow",
            "options.replay.20260612.hpsf.strike-score",
            "options.replay.20260612.hpsf.signal",
            "options.replay.20260612.hpsf.latest-signal",
            "options.replay.20260612.hpsf.audit",
            "options.replay.20260612.hpsf.dlq",
        ]:
            self.assertIn(topic, output)

    def test_ReplayTopicCreationCompactDeleteTest(self) -> None:
        output = subprocess.check_output([str(CREATE_REPLAY_TOPICS), "--dry-run"], text=True, cwd=ROOT)

        self.assertIn("options.replay.20260612.opra.tcbbo partitions=12", output)
        self.assertIn("options.replay.20260612.hpsf.strike-flow partitions=12", output)
        self.assertIn("options.replay.20260612.hpsf.strike-score partitions=12", output)
        self.assertIn("options.replay.20260612.hpsf.dlq partitions=12", output)
        self.assertIn("options.replay.20260612.hpsf.latest-signal partitions=1", output)
        self.assertIn("cleanup.policy=compact,delete", output)
        self.assertIn("options.replay.20260612.hpsf.signal partitions=1", output)
        self.assertIn("cleanup.policy=delete", output)
        for replay_input_topic in [
            "options.replay.20260612.opra.tcbbo",
            "underlying.replay.20260612.es.trades",
            "underlying.replay.20260612.spx.price",
        ]:
            self.assertIn(f"DRY RUN topic={replay_input_topic}", output)
        self.assertNotIn("retention.ms=86400000", output)
        self.assertIn("retention.ms=2592000000", output)

    def test_replay_topic_script_allows_partition_override(self) -> None:
        env = os.environ.copy()
        env["REPLAY_TOPIC_PARTITIONS"] = "4"
        output = subprocess.check_output([str(CREATE_REPLAY_TOPICS), "--dry-run"], text=True, cwd=ROOT, env=env)

        self.assertIn("options.replay.20260612.opra.tcbbo partitions=4", output)
        self.assertIn("options.replay.20260612.hpsf.strike-flow partitions=4", output)
        self.assertIn("options.replay.20260612.hpsf.dlq partitions=4", output)
        self.assertIn("underlying.replay.20260612.es.trades partitions=2", output)

    def test_replay_topic_script_rejects_non_rf1(self) -> None:
        env = os.environ.copy()
        env["KAFKA_TOPIC_REPLICATION_FACTOR"] = "3"
        result = subprocess.run([str(CREATE_REPLAY_TOPICS), "--dry-run"], cwd=ROOT, env=env, text=True, capture_output=True)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("RF=1 cluster rule violated", result.stderr)

    def test_replay_topic_script_can_reset_replay_topics(self) -> None:
        env = os.environ.copy()
        env["HPSF_REPLAY_RESET_TOPICS"] = "true"
        output = subprocess.check_output([str(CREATE_REPLAY_TOPICS), "--dry-run"], text=True, cwd=ROOT, env=env)

        self.assertIn("Resetting HPSF replay topics before replay run", output)
        self.assertIn("kafka-topics --bootstrap-server DRY_RUN_BOOTSTRAP --delete --topic options.replay.20260612.hpsf.signal", output)
        self.assertIn("kafka-topics --bootstrap-server DRY_RUN_BOOTSTRAP --delete --topic underlying.replay.20260612.es.trades", output)

    def test_replay_topic_script_uses_replay_date_topic_prefix(self) -> None:
        env = os.environ.copy()
        env["REPLAY_DATE"] = "2026-06-11"
        env["TOPIC_PREFIX"] = "options.replay.20260611"
        output = subprocess.check_output([str(CREATE_REPLAY_TOPICS), "--dry-run"], text=True, cwd=ROOT, env=env)

        self.assertIn("Creating HPSF replay topics for 2026-06-11", output)
        self.assertIn("DRY RUN topic=options.replay.20260611.opra.tcbbo", output)
        self.assertIn("DRY RUN topic=underlying.replay.20260611.es.trades", output)
        self.assertIn("DRY RUN topic=options.replay.20260611.hpsf.signal", output)
        self.assertNotIn("options.replay.20260612", output)
        self.assertNotIn("underlying.replay.20260612", output)

    def test_replay_topic_script_retries_post_alter_metadata_race(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bin_dir = tmp_path / "bin"
            state_dir = tmp_path / "state"
            bin_dir.mkdir()
            state_dir.mkdir()

            kafka_topics = bin_dir / "kafka-topics"
            kafka_topics.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    state="${FAKE_KAFKA_STATE:?}"
                    cmd=""
                    topic=""
                    while [[ $# -gt 0 ]]; do
                      case "$1" in
                        --create|--describe|--delete) cmd="${1#--}"; shift ;;
                        --topic) topic="$2"; shift 2 ;;
                        --bootstrap-server|--partitions|--replication-factor|--config) shift 2 ;;
                        *) shift ;;
                      esac
                    done
                    case "$cmd" in
                      create)
                        touch "$state/topic.$topic"
                        echo "Created topic $topic."
                        ;;
                      describe)
                        if [[ ! -f "$state/topic.$topic" ]]; then
                          echo "Topic '$topic' does not exist as expected" >&2
                          exit 1
                        fi
                        if [[ -f "$state/config.$topic" && ! -f "$state/final-describe-failed.$topic" ]]; then
                          touch "$state/final-describe-failed.$topic"
                          echo "Topic '$topic' does not exist as expected" >&2
                          exit 1
                        fi
                        echo "Topic: $topic PartitionCount: 1 ReplicationFactor: 1 Configs: compression.type=lz4"
                        ;;
                      delete)
                        rm -f "$state/topic.$topic" "$state/config.$topic" "$state/final-describe-failed.$topic"
                        ;;
                      *)
                        echo "unsupported kafka-topics command: $cmd" >&2
                        exit 2
                        ;;
                    esac
                    """
                )
            )
            kafka_topics.chmod(0o755)

            kafka_configs = bin_dir / "kafka-configs"
            kafka_configs.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    state="${FAKE_KAFKA_STATE:?}"
                    cmd=""
                    topic=""
                    while [[ $# -gt 0 ]]; do
                      case "$1" in
                        --alter|--describe) cmd="${1#--}"; shift ;;
                        --entity-name) topic="$2"; shift 2 ;;
                        --bootstrap-server|--entity-type|--add-config) shift 2 ;;
                        *) shift ;;
                      esac
                    done
                    case "$cmd" in
                      alter)
                        touch "$state/config.$topic"
                        echo "Completed updating config for topic $topic."
                        ;;
                      describe)
                        if [[ ! -f "$state/config.$topic" ]]; then
                          echo "Configs for topic '$topic' are not visible yet" >&2
                          exit 1
                        fi
                        echo "Dynamic configs for topic $topic are: compression.type=lz4"
                        ;;
                      *)
                        echo "unsupported kafka-configs command: $cmd" >&2
                        exit 2
                        ;;
                    esac
                    """
                )
            )
            kafka_configs.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}:{env['PATH']}"
            env["FAKE_KAFKA_STATE"] = str(state_dir)
            env["KAFKA_BOOTSTRAP_SERVERS"] = "fake:9092"
            env["HPSF_REPLAY_TOPIC_METADATA_RETRIES"] = "3"
            env["HPSF_REPLAY_TOPIC_METADATA_RETRY_SLEEP_SECONDS"] = "0"

            result = subprocess.run([str(CREATE_REPLAY_TOPICS)], cwd=ROOT, env=env, text=True, capture_output=True)

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("Waiting for Kafka topic metadata for options.replay.20260612.opra.tcbbo", result.stderr)
            self.assertIn("Topic: options.replay.20260612.opra.tcbbo", result.stdout)
            self.assertIn("Dynamic configs for topic options.replay.20260612.opra.tcbbo", result.stdout)


if __name__ == "__main__":
    unittest.main()
