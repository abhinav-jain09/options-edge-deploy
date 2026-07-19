"""Executable tests for the zero-orphan prune in ensure-vix-option-inteligence-topic.sh.

Runs the REAL script against stubbed kafka-topics / kafka-consumer-groups / kafka-configs
binaries backed by a state directory, so exactness, idempotency, fail-closed listing,
fail-loud deletion, delete verification, and TOPIC_PREFIX awareness are all proven against
the shell implementation itself, not a description of it.
"""

import os
import pathlib
import stat
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-vix-option-inteligence-topic.sh"

NEW_TOPIC = "options.spx.vix-option-inteligence-service.current"
LEGACY_TOPIC = "options.spx.0dte.intelligence.current"

KAFKA_TOPICS_STUB = """#!/usr/bin/env bash
S="$STUB_STATE"
cmd=; topic=; prev=
for a in "$@"; do
  [ "$prev" = "--topic" ] && topic="$a"
  case "$a" in --list|--describe|--create|--alter|--delete) cmd="${a#--}";; esac
  prev="$a"
done
case "$cmd" in
  list)
    [ -f "$S/fail_topics_list" ] && exit 1
    cat "$S/topics.txt" ;;
  describe)
    grep -Fxq "$topic" "$S/topics.txt" || exit 1
    echo "Topic: $topic TopicId: x PartitionCount: 32 ReplicationFactor: 1" ;;
  create) echo "$topic" >> "$S/topics.txt" ;;
  alter) : ;;
  delete)
    [ -f "$S/fail_topic_delete" ] && exit 1
    if [ ! -f "$S/topic_delete_noop" ]; then
      grep -Fxv "$topic" "$S/topics.txt" > "$S/t.tmp" || true
      mv "$S/t.tmp" "$S/topics.txt"
    fi ;;
esac
exit 0
"""

KAFKA_CONSUMER_GROUPS_STUB = """#!/usr/bin/env bash
S="$STUB_STATE"
cmd=; group=; prev=
for a in "$@"; do
  [ "$prev" = "--group" ] && group="$a"
  case "$a" in --list|--delete) cmd="${a#--}";; esac
  prev="$a"
done
case "$cmd" in
  list)
    [ -f "$S/fail_groups_list" ] && exit 1
    cat "$S/groups.txt" ;;
  delete)
    [ -f "$S/fail_group_delete" ] && exit 1
    if [ ! -f "$S/group_delete_noop" ]; then
      grep -Fxv "$group" "$S/groups.txt" > "$S/g.tmp" || true
      mv "$S/g.tmp" "$S/groups.txt"
    fi ;;
esac
exit 0
"""

KAFKA_CONFIGS_STUB = """#!/usr/bin/env bash
case " $* " in
  *" --describe "*) echo "cleanup.policy=compact,retention.ms=-1" ;;
esac
exit 0
"""


class PruneScriptTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = pathlib.Path(self.tmp.name)
        self.bin = base / "bin"
        self.state = base / "state"
        self.bin.mkdir()
        self.state.mkdir()
        for name, body in (
            ("kafka-topics", KAFKA_TOPICS_STUB),
            ("kafka-consumer-groups", KAFKA_CONSUMER_GROUPS_STUB),
            ("kafka-configs", KAFKA_CONFIGS_STUB),
        ):
            path = self.bin / name
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)

    def tearDown(self):
        self.tmp.cleanup()

    def run_script(self, topics, groups, flags=(), extra_env=None):
        (self.state / "topics.txt").write_text("".join(t + "\n" for t in topics))
        (self.state / "groups.txt").write_text("".join(g + "\n" for g in groups))
        for flag in flags:
            (self.state / flag).write_text("")
        env = dict(os.environ)
        env.update({
            "PATH": f"{self.bin}:{env['PATH']}",
            "STUB_STATE": str(self.state),
            "KAFKA_BOOTSTRAP_SERVERS": "stub:9092",
            "KAFKA_TOPIC_DELETE_WAIT_SECONDS": "2",
        })
        env.update(extra_env or {})
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

    def topics(self):
        return (self.state / "topics.txt").read_text().split()

    def groups(self):
        return (self.state / "groups.txt").read_text().split()

    def test_prunes_exact_retired_identity_and_spares_neighbours(self):
        result = self.run_script(
            topics=[NEW_TOPIC, LEGACY_TOPIC, "options.databento.events.raw"],
            groups=[
                "zero-dte-intelligence-service-v1",
                "zero-dte-intelligence-service-v1-prod",
                "zero-dte-intelligence-service-v10",   # near miss: v1 followed by 0
                "zero-dte-intelligence-service-v1x",   # near miss: no separator
                "vix-option-inteligence-service-prod", # active identity
            ],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(LEGACY_TOPIC, self.topics())
        self.assertIn("options.databento.events.raw", self.topics())
        self.assertEqual(
            self.groups(),
            ["zero-dte-intelligence-service-v10",
             "zero-dte-intelligence-service-v1x",
             "vix-option-inteligence-service-prod"])
        self.assertIn("deleted and verified absent", result.stdout)
        self.assertIn("zero-orphan verified", result.stdout)

    def test_second_run_is_idempotent(self):
        result = self.run_script(topics=[NEW_TOPIC], groups=["vix-option-inteligence-service-prod"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("absent (nothing to prune)", result.stdout)
        self.assertIn("no zero-dte-intelligence-service-v1* consumer groups remain", result.stdout)

    def test_topics_list_failure_fails_closed(self):
        result = self.run_script(topics=[NEW_TOPIC], groups=[], flags=["fail_topics_list"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FATAL: could not list topics", result.stderr)

    def test_groups_list_failure_fails_closed(self):
        result = self.run_script(topics=[NEW_TOPIC], groups=[], flags=["fail_groups_list"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FATAL: could not list consumer groups", result.stderr)

    def test_retired_group_delete_failure_is_loud(self):
        result = self.run_script(
            topics=[NEW_TOPIC],
            groups=["zero-dte-intelligence-service-v1"],
            flags=["fail_group_delete"],
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FATAL: failed to delete retired consumer group", result.stderr)

    def test_group_surviving_a_successful_delete_fails_terminal_verification(self):
        # The broker acknowledges the delete but the group persists (eventual consistency /
        # misbehaving broker): the terminal zero-orphan re-list must turn this into FATAL.
        result = self.run_script(
            topics=[NEW_TOPIC],
            groups=["zero-dte-intelligence-service-v1"],
            flags=["group_delete_noop"],
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FATAL: retired groups still present after delete", result.stderr)

    def test_unverified_topic_delete_fails_after_bounded_wait(self):
        result = self.run_script(
            topics=[NEW_TOPIC, LEGACY_TOPIC],
            groups=[],
            flags=["topic_delete_noop"],
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("still present", result.stderr)

    def test_es4_prefix_prunes_only_the_mirrored_topic(self):
        result = self.run_script(
            topics=["es." + NEW_TOPIC, "es." + LEGACY_TOPIC, LEGACY_TOPIC],
            groups=[],
            extra_env={"TOPIC_PREFIX": "es."},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("es." + LEGACY_TOPIC, self.topics())
        # The unprefixed prod topic is NOT this mirror's identity; it must survive here.
        self.assertIn(LEGACY_TOPIC, self.topics())


if __name__ == "__main__":
    unittest.main()
