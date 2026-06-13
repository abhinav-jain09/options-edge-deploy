from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CREATE_SCRIPT = ROOT / "scripts" / "kafka" / "create-hpsf-topics.sh"
VERIFY_SCRIPT = ROOT / "scripts" / "kafka" / "verify-hpsf-topics.sh"


class HpsfTopicScriptTest(unittest.TestCase):
    def test_scripts_parse(self) -> None:
        subprocess.run(["bash", "-n", str(CREATE_SCRIPT)], check=True)
        subprocess.run(["bash", "-n", str(VERIFY_SCRIPT)], check=True)

    def test_dry_run_contains_required_topics_and_rf1_config(self) -> None:
        output = subprocess.check_output([str(CREATE_SCRIPT), "--dry-run"], text=True, cwd=ROOT)

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
        ]:
            self.assertIn(topic, output)
        self.assertIn("replication.factor=1", output)
        self.assertIn("min.insync.replicas=1", output)
        self.assertIn("compression.type=lz4", output)
        self.assertIn("options.hpsf.strike-flow partitions=6", output)
        self.assertIn("retention.ms=172800000", output)
        self.assertIn("options.hpsf.signal partitions=1", output)
        self.assertIn("cleanup.policy=delete", output)
        self.assertIn("options.hpsf.latest-signal partitions=1", output)
        self.assertIn("cleanup.policy=compact,delete", output)

    def test_script_rejects_non_rf1_config(self) -> None:
        env = os.environ.copy()
        env["KAFKA_TOPIC_REPLICATION_FACTOR"] = "3"
        result = subprocess.run([str(CREATE_SCRIPT), "--dry-run"], cwd=ROOT, env=env, text=True, capture_output=True)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("RF=1 cluster rule violated", result.stderr)

    def test_no_forbidden_kafka_configs_in_scripts(self) -> None:
        combined = CREATE_SCRIPT.read_text() + "\n" + VERIFY_SCRIPT.read_text()

        self.assertNotIn("replication.factor=3", combined)
        self.assertNotIn("min.insync.replicas=2", combined)
        self.assertNotIn("num.standby.replicas=1", combined)


if __name__ == "__main__":
    unittest.main()
