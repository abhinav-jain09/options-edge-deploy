from __future__ import annotations

import os
import subprocess
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

        self.assertIn("options.replay.20260612.hpsf.latest-signal partitions=1", output)
        self.assertIn("cleanup.policy=compact,delete", output)
        self.assertIn("options.replay.20260612.hpsf.signal partitions=1", output)
        self.assertIn("cleanup.policy=delete", output)
        self.assertIn("retention.ms=2592000000", output)

    def test_replay_topic_script_rejects_non_rf1(self) -> None:
        env = os.environ.copy()
        env["KAFKA_TOPIC_REPLICATION_FACTOR"] = "3"
        result = subprocess.run([str(CREATE_REPLAY_TOPICS), "--dry-run"], cwd=ROOT, env=env, text=True, capture_output=True)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("RF=1 cluster rule violated", result.stderr)


if __name__ == "__main__":
    unittest.main()
