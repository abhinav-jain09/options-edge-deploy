"""scripts/kafka/ensure-partition-only-topics.sh against a fake kafka-topics: creates what is missing at the
declared count, grows a smaller copy, only reports a larger one, and on production skips dev-named app ids."""
import os, subprocess, tempfile, textwrap, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-partition-only-topics.sh"


def run(existing, declared, env=None):
    tmp = Path(tempfile.mkdtemp()); calls = tmp / "calls"; calls.touch()
    describe = "\n".join(f"Topic: {n}\tTopicId: x\tPartitionCount: {c}\tReplicationFactor: 1\tConfigs: " for n, c in existing.items())
    dfile = tmp / "describe.txt"; dfile.write_text(describe + ("\n" if describe else ""))
    kt = tmp / "kafka-topics"; kt.write_text(
        "#!/bin/bash\n"
        'case " $* " in\n'
        f'  *" --describe "*) cat "{dfile}"; exit 0 ;;\n'
        f'  *" --create "*) echo "CREATE $*" >> "{calls}"; exit 0 ;;\n'
        f'  *" --alter "*)  echo "ALTER $*"  >> "{calls}"; exit 0 ;;\n'
        "esac\nexit 1\n"); kt.chmod(0o755)
    tenv = tmp / "topics.env"; tenv.write_text(f'OPTIONS_EDGE_PARTITION_ONLY_TOPICS="{declared}"\n')
    e = dict(os.environ, KAFKA_BOOTSTRAP_SERVERS="b:1", TOPICS_ENV=str(tenv), KAFKA_TOPICS_CMD=str(kt))
    e.pop("ENVIRONMENT", None); e.update(env or {})
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=e)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines()


class EnsurePartitionOnlyTopics(unittest.TestCase):
    def test_creates_missing_at_declared_count_without_configs(self):
        rc, out, calls = run({}, "a.topic:32 b.topic:1")
        self.assertEqual(rc, 0)
        self.assertEqual(sorted(calls), ["CREATE --bootstrap-server b:1 --create --if-not-exists --topic a.topic --partitions 32 --replication-factor 1",
                                         "CREATE --bootstrap-server b:1 --create --if-not-exists --topic b.topic --partitions 1 --replication-factor 1"])
        self.assertNotIn("--config", "".join(calls))
        self.assertIn("created 2, grown 0", out)

    def test_grows_smaller_and_only_reports_larger(self):
        rc, out, calls = run({"a.topic": 4, "big.topic": 64}, "a.topic:32 big.topic:32")
        self.assertEqual(rc, 0)
        self.assertEqual(calls, ["ALTER --bootstrap-server b:1 --alter --topic a\\.topic --partitions 32"])
        self.assertIn("larger than declared", out); self.assertIn("big.topic(64>32)", out)

    def test_production_skips_dev_named_app_topics(self):
        rc, out, calls = run({}, "svc-dev-rekey:32 options.hpsf.dlq:32", env={"ENVIRONMENT": "production"})
        self.assertEqual(rc, 0)
        self.assertEqual(len(calls), 1); self.assertIn("--topic options.hpsf.dlq --partitions 32", calls[0])
        self.assertIn("skipped dev-named 1", out)

    def test_dev_keeps_dev_named_entries(self):
        rc, out, calls = run({}, "svc-dev-rekey:32")
        self.assertEqual(rc, 0); self.assertEqual(len(calls), 1); self.assertIn("skipped dev-named 0", out)

    def test_describe_failure_creates_nothing(self):
        tmp = Path(tempfile.mkdtemp()); kt = tmp / "kafka-topics"; kt.write_text("#!/bin/bash\nexit 1\n"); kt.chmod(0o755)
        tenv = tmp / "topics.env"; tenv.write_text('OPTIONS_EDGE_PARTITION_ONLY_TOPICS="a:1"\n')
        r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True,
                           env=dict(os.environ, KAFKA_BOOTSTRAP_SERVERS="b:1", TOPICS_ENV=str(tenv), KAFKA_TOPICS_CMD=str(kt)))
        self.assertEqual(r.returncode, 1); self.assertIn("nothing created blind", r.stderr)


if __name__ == "__main__":
    unittest.main()
