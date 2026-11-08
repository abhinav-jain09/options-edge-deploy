# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""scripts/kafka/spx-price-mirror/assert-timestamp-type.sh with a stand-in kafka-configs: only CreateTime preserves the source timestamp."""
import os, pathlib, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/spx-price-mirror/assert-timestamp-type.sh"


def run(answer, rc=0):
    d = pathlib.Path(tempfile.mkdtemp())
    stub = d / "kafka-configs"
    stub.write_text(f"#!/bin/bash\ncat <<'EOT'\n{answer}\nEOT\nexit {rc}\n")
    os.chmod(stub, 0o755)
    r = subprocess.run(["bash", str(SCRIPT), "b:9092", "t", str(d)], capture_output=True, text=True)
    return r.returncode, r.stdout + r.stderr


class TimestampType(unittest.TestCase):
    def test_create_time_passes(self):
        self.assertEqual(run("  message.timestamp.type=CreateTime sensitive=false synonyms={DEFAULT_CONFIG:log.message.timestamp.type=CreateTime}")[0], 0)

    def test_log_append_time_fails(self):
        rc, out = run("  message.timestamp.type=LogAppendTime sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:message.timestamp.type=LogAppendTime}")
        self.assertNotEqual(rc, 0)
        self.assertIn("not CreateTime", out)

    def test_unreadable_answer_fails(self):
        self.assertNotEqual(run("nothing useful")[0], 0)
        self.assertNotEqual(run("message.timestamp.type=CreateTime", rc=1)[0], 0)


if __name__ == "__main__":
    unittest.main()
