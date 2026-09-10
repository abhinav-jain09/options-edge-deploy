from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APPLY = ROOT / "scripts" / "kafka" / "apply-topics.sh"
TOPICS_ENV = ROOT / "scripts" / "kafka" / "topics.env"
DEV_CLEANUP = ROOT / "scripts" / "ops" / "dev-cleanup.sh"

# The live shape of each Kafka Streams source on 2026-09-10 (prod .252 and dev docker-desktop) — what
# its owning service already stamps. The declaration exists only to pre-create the topic at its real
# partition count after a wipe, so applying it must reproduce this shape exactly and change nothing.
#   topic: (partitions, dev cleanup.policy, prod cleanup.policy, retention.ms)
LIVE = {
    "options.opra.tcbbo": (32, "delete", "delete", "-1"),
    "options.databento.strike-flow": (32, "compact,delete", "delete", "-1"),
    "options.databento.strike-flow.strike.avro": (32, "delete", "delete", "-1"),
    "options.spx.strike-sr.current": (32, "compact,delete", "delete", "-1"),
}

KAFKA_TOPICS_STUB = r"""#!/usr/bin/env bash
mode=""; topic=""; parts=""; configs=""
while [ $# -gt 0 ]; do
  case "$1" in
    --describe|--create|--list|--alter|--delete) mode="${1#--}" ;;
    --topic) topic="$2"; shift ;;
    --partitions) parts="$2"; shift ;;
    --config) configs="$configs $2"; shift ;;
  esac
  shift
done
case "$mode" in
  describe) awk -v t="$topic" '$1==t {printf "Topic: %s\tTopicId: fake\tPartitionCount: %s\tReplicationFactor: 1\tConfigs:\n", $1, $2}' "$FAKE_KAFKA_STATE" ;;
  create) printf '%s %s%s\n' "$topic" "$parts" "$configs" >> "$FAKE_KAFKA_STATE" ;;
  list) awk '{print $1}' "$FAKE_KAFKA_STATE" ;;
esac
exit 0
"""

KAFKA_CONFIGS_STUB = r"""#!/usr/bin/env bash
name=""; add=""
while [ $# -gt 0 ]; do
  case "$1" in
    --entity-name) name="$2"; shift ;;
    --add-config) add="$2"; shift ;;
  esac
  shift
done
[ -n "$add" ] && printf '%s %s\n' "$name" "$add" >> "$FAKE_KAFKA_ALTERS"
exit 0
"""

BROKER_STUB = '#!/usr/bin/env bash\necho "fake:9092 (id: 1 rack: null) -> ("\n'


def _bash() -> str:
    """apply-topics.sh expands a possibly-empty array under `set -u`, which needs bash >= 4."""
    for candidate in ("/opt/homebrew/bin/bash", "/usr/local/bin/bash", shutil.which("bash")):
        if candidate and os.path.exists(candidate):
            major = subprocess.run([candidate, "-c", "echo ${BASH_VERSINFO[0]}"],
                                   capture_output=True, text=True).stdout.strip()
            if major.isdigit() and int(major) >= 4:
                return candidate
    raise AssertionError("no bash >= 4 found to run scripts/kafka/apply-topics.sh")


def _configs(add_config: str) -> dict[str, str]:
    return {k: v.strip("[]") for k, v in re.findall(r"([a-z.]+)=(\[[^\]]*\]|[^,]*)", add_config)}


def _apply(environment: str) -> tuple[dict[str, tuple[int, dict[str, str]]], dict[str, dict[str, str]]]:
    """Run the REAL apply-topics.sh against an empty fake broker; return what it created and altered."""
    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        stubs = tmp / "bin"
        stubs.mkdir()
        for name, body in (("kafka-topics", KAFKA_TOPICS_STUB), ("kafka-configs", KAFKA_CONFIGS_STUB),
                           ("kafka-broker-api-versions", BROKER_STUB)):
            path = stubs / name
            path.write_text(body)
            path.chmod(0o755)
        state, alters = tmp / "state", tmp / "alters"
        state.write_text("")
        alters.write_text("")
        env = {
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "HOME": str(tmp),
            "KAFKA_BOOTSTRAP_SERVERS": "fake:9092",
            "KAFKA_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TOPIC_RETENTION_MS": "-1",  # k8s/infra/base/configmap.yaml, dev and prod
            "ENVIRONMENT": environment,
            "FAKE_KAFKA_STATE": str(state),
            "FAKE_KAFKA_ALTERS": str(alters),
        }
        run = subprocess.run([_bash(), str(APPLY)], env=env, capture_output=True, text=True, timeout=900)
        if run.returncode != 0:
            raise AssertionError(f"apply-topics.sh rc={run.returncode}\n{run.stdout[-1500:]}\n{run.stderr[-1500:]}")
        created = {}
        for line in state.read_text().splitlines():
            topic, parts, *cfg = line.split()
            created[topic] = (int(parts), dict(c.split("=", 1) for c in cfg))
        altered = {}
        for line in alters.read_text().splitlines():
            topic, add = line.split(" ", 1)
            altered[topic] = _configs(add)
        return created, altered


class CrossServiceStreamsSourcesDeclaredTest(unittest.TestCase):
    """A Kafka Streams source written by another service must be pre-created at its real partition
    count, never left to broker auto-create (num.partitions=1 on dev AND prod). 2026-09-10 (dev): the
    OPRA tape and options.databento.strike-flow were auto-created at 1 partition after a wipe;
    strike-flow-classifier sized its repartition topics from the tape and failed every rebalance with
    "expected: 32; actual: 1" — 0 records for ~8h while READY."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.prod = _apply("production")
        cls.dev = _apply("dev")

    def _assert_shape(self, applied, environment: str) -> None:
        created, altered = applied
        for topic, (parts, dev_policy, prod_policy, retention) in LIVE.items():
            policy = prod_policy if environment == "production" else dev_policy
            with self.subTest(environment=environment, topic=topic):
                self.assertIn(topic, created, f"apply-topics does not pre-create {topic} on {environment}")
                self.assertEqual(created[topic][0], parts)
                self.assertEqual(created[topic][1].get("cleanup.policy"), policy)
                self.assertEqual(created[topic][1].get("retention.ms"), retention)
                # alter_topic_config re-applies policy + retention on every run, so the reconcile of an
                # EXISTING topic must land on the same live shape, not just the create.
                self.assertEqual(altered[topic].get("cleanup.policy"), policy)
                self.assertEqual(altered[topic].get("retention.ms"), retention)

    def test_prod_apply_reproduces_the_live_shape(self) -> None:
        self._assert_shape(self.prod, "production")

    def test_dev_apply_reproduces_the_live_shape(self) -> None:
        self._assert_shape(self.dev, "dev")

    def test_dev_cleanup_recreates_the_same_shape_after_a_wipe(self) -> None:
        # dev-cleanup's ensure_topics: eval the assignment lines, then topic_desired per topic.
        script = r'''
          eval "$(grep -E '^OPTIONS_EDGE_(TOPICS|COMPACTED_TOPICS|PURE_COMPACT_TOPICS|EXACT_PARTITION_TOPICS|TOPIC_RETENTION_OVERRIDES|TOPIC_DELETE_RETENTION_OVERRIDES)=' "$1")"
          eval "$(awk '/^topic_desired\(\)/{f=1} f{print} f&&/^}/{exit}' "$2")"
          shift 2
          for t in "$@"; do
            declared=0
            for spec in $OPTIONS_EDGE_TOPICS; do [ "${spec%%:*}" = "$t" ] && declared=1; done
            topic_desired "$t"
            echo "$t $declared $DPARTS $DPOL ${DRET:--1}"
          done
        '''
        out = subprocess.run([_bash(), "-c", script, "bash", str(TOPICS_ENV), str(DEV_CLEANUP), *LIVE],
                             capture_output=True, text=True, check=True).stdout
        seen = {line.split()[0]: line.split()[1:] for line in out.splitlines()}
        for topic, (parts, dev_policy, _prod, retention) in LIVE.items():
            with self.subTest(topic=topic):
                self.assertEqual(seen[topic], ["1", str(parts), dev_policy, retention])


if __name__ == "__main__":
    unittest.main()
