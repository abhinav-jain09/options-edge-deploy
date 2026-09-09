"""Executable tests for the zero-orphan prune in ensure-vix-option-inteligence-topic.sh.

Runs the REAL script against stubbed kafka-topics / kafka-consumer-groups / kafka-configs
binaries backed by a state directory, so exactness, idempotency, fail-closed listing,
fail-loud deletion, delete verification, and TOPIC_PREFIX awareness are all proven against
the shell implementation itself, not a description of it.

The es4 tests additionally run scripts/es4/create-es-topics.sh, which reaches those stubs the way
the es4 box does: through the REAL kafka-cli-shim, whose `sudo -n docker exec` is intercepted by the
`sudo` stub below — so the shims and the shared applier stay under test rather than being bypassed.
"""

import os
import pathlib
import stat
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-vix-option-inteligence-topic.sh"

# Partition count the stub broker reports for a topic seeded by a test rather than created by the
# script under test: the widest count topics.env declares, so a seeded topic reads as an already-
# wide topic the reconciliation is owed nothing on, never as one it still has to grow.
STUB_TOPIC_PARTITIONS = 32

NEW_TOPIC = "options.spx.vix-option-inteligence-service.current"
LEGACY_TOPIC = "options.spx.0dte.intelligence.current"

KAFKA_TOPICS_STUB = """#!/usr/bin/env bash
# Fake broker state: one `topic partitions` line per existing topic, so --describe reports back the
# shape each topic was CREATED with. A stub that answered a fixed partition count made
# scripts/kafka/apply-topics.sh see an EXACT-partition mismatch on every es4 topic declared at
# fewer partitions and exit 1 long before the prune under test ran.
S="$STUB_STATE"
cmd=; topic=; parts=; prev=
for a in "$@"; do
  case "$prev" in
    --topic) topic="$a" ;;
    --partitions) parts="$a" ;;
  esac
  case "$a" in --list|--describe|--create|--alter|--delete) cmd="${a#--}";; esac
  prev="$a"
done
# Read the state with the shell's own string comparison rather than grep/sed: topic names are full
# of dots, and a name spliced into a pattern stops being a literal — `es.futures.cvd` would also
# match `esXfuturesXcvd`. (It also keeps the whole es4 reconciliation fork-free, which is what makes
# running ~110 topics through these stubs take seconds instead of minutes.)
have=; rest=()
while read -r t p; do
  if [ "$t" = "$topic" ]; then have="$p"; else rest+=("$t $p"); fi
done < "$S/topics.txt"
write_without_topic() {
  : > "$S/topics.txt"
  [ ${#rest[@]} -gt 0 ] && printf '%s\n' "${rest[@]}" >> "$S/topics.txt"
  return 0
}
case "$cmd" in
  list)
    [ -f "$S/fail_topics_list" ] && exit 1
    # `--list --topic X` is the existence probe; a bare --list is the whole catalogue.
    if [ -n "$topic" ]; then
      [ -n "$have" ] && echo "$topic"
    else
      while read -r t p; do echo "$t"; done < "$S/topics.txt"
    fi ;;
  describe)
    [ -n "$have" ] || exit 1
    echo "Topic: $topic TopicId: x PartitionCount: $have ReplicationFactor: 1" ;;
  create) [ -n "$have" ] || echo "$topic ${parts:-1}" >> "$S/topics.txt" ;;
  alter)
    # Kafka can only grow a topic; the broker then reports the NEW count.
    [ -n "$have" ] && [ -n "$parts" ] && {
      write_without_topic
      echo "$topic $parts" >> "$S/topics.txt"
    } ;;
  delete)
    [ -f "$S/fail_topic_delete" ] && exit 1
    [ -f "$S/topic_delete_noop" ] || write_without_topic ;;
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
# Records what `--alter --add-config` writes and reports it back in the broker's own --describe
# shape (`  <key>=<value> sensitive=false synonyms={...}`), so a guard that reads a topic's own
# retention.ms line — scripts/ci/validate-declared-overrides-are-explicit.sh, which
# create-es-topics.sh runs over the es4 declaration after reconciling it — sees what the
# reconciliation actually set. A stub that always printed the same line could not tell a topic left
# on the broker default from one the reconciliation reached, which is the whole distinction that
# guard exists to draw.
S="$STUB_STATE"; C="$S/configs.txt"
cmd=; topic=; add=; prev=
for a in "$@"; do
  case "$prev" in
    --entity-name) topic="$a" ;;
    --add-config) add="$a" ;;
  esac
  case "$a" in --describe|--alter) cmd="${a#--}";; esac
  prev="$a"
done
have=; rest=()
if [ -f "$C" ]; then
  while read -r t v; do
    if [ "$t" = "$topic" ]; then have="$v"; else rest+=("$t $v"); fi
  done < "$C"
fi
case "$cmd" in
  alter)
    : > "$C"
    [ ${#rest[@]} -gt 0 ] && printf '%s\n' "${rest[@]}" >> "$C"
    echo "$topic $add" >> "$C" ;;
  describe)
    echo "Dynamic configs for topic $topic are:"
    # Split on top-level commas only: a list value is written `cleanup.policy=[compact,delete]`, and
    # splitting inside the brackets would report two configs the broker never held.
    out=; depth=0
    for ((i = 0; i < ${#have}; i++)); do
      c="${have:i:1}"
      case "$c" in
        "[") depth=$((depth + 1)); out="$out$c" ;;
        "]") depth=$((depth - 1)); out="$out$c" ;;
        ",") if [ "$depth" -eq 0 ]; then
               [ -n "$out" ] && echo "  $out sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:$out}"
               out=
             else out="$out$c"; fi ;;
        *) out="$out$c" ;;
      esac
    done
    [ -n "$out" ] && echo "  $out sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:$out}" ;;
esac
exit 0
"""

# The es4 CLI shims each run `exec sudo -n docker exec -i "$ES4_KAFKA_CONTAINER" <cli> "$@"`. Faking
# `sudo` intercepts every CLI call the real shims make, so the shims themselves stay under test
# rather than being bypassed — and without it the REAL sudo runs and stops at "a password is
# required", which is exactly what the two es4 tests below used to fail on.
#
# The shim dir must come OFF the PATH before the CLI is exec'd. create-es-topics.sh puts it first,
# so a fake sudo that simply exec'd `kafka-topics` would find the shim again — shim calls sudo calls
# shim, forever. Dropping it is also what the real thing does in spirit: inside the container the
# CLI is the CLI, not a proxy back out.
SUDO_STUB = """#!/usr/bin/env bash
# The prefix is ASSERTED, not skipped over. Consuming `-n`, `docker`, `exec` and any leading option
# independently would let a BROKEN shim through: `sudo -n exec -i es4-kafka kafka-topics ...` has no
# `docker` in it and cannot work on the es4 host, yet it would still reach the fake CLI and the
# suite would go green. This stub stands where the real boundary is, so it holds the shim to the
# exact call the es4 box requires.
expected_container="${ES4_KAFKA_CONTAINER:-es4-kafka}"
die() { echo "sudo stub: the shim did not invoke $* — got: $ORIGINAL" >&2; exit 97; }
ORIGINAL="$*"
[ "$1" = "-n" ]     || die "sudo -n (non-interactive)"; shift
[ "$1" = "docker" ] || die "docker"; shift
[ "$1" = "exec" ]   || die "docker exec"; shift
[ "$1" = "-i" ]     || die "docker exec -i"; shift
[ "$1" = "$expected_container" ] || die "the container $expected_container"; shift
[ $# -gt 0 ] || die "a command inside the container"
kept=
while IFS= read -r -d: dir; do
  case "$dir" in *kafka-cli-shim*) continue ;; esac
  kept="${kept:+$kept:}$dir"
done <<< "$PATH:"
PATH="$kept"
export PATH
exec "$@"
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
            ("sudo", SUDO_STUB),
        ):
            path = self.bin / name
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)

    def tearDown(self):
        self.tmp.cleanup()

    def run_script(self, topics, groups, flags=(), extra_env=None, script=SCRIPT):
        # `topic partitions` per line — the shape the stub broker reports back from --describe.
        (self.state / "topics.txt").write_text(
            "".join(f"{t} {STUB_TOPIC_PARTITIONS}\n" for t in topics))
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
        return subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)

    def topics(self):
        return [line.split()[0]
                for line in (self.state / "topics.txt").read_text().splitlines() if line]

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

    def test_whitespace_bearing_group_is_deleted_as_one_unit(self):
        # Group ids are not guaranteed whitespace-free; word splitting would have turned
        # this into a deletion request against the unrelated group "team".
        result = self.run_script(
            topics=[NEW_TOPIC],
            groups=[
                "zero-dte-intelligence-service-v1-blue team",
                "team",
                "zero-dte-intelligence-service-v10",
            ],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (self.state / "groups.txt").read_text().splitlines(),
            ["team", "zero-dte-intelligence-service-v10"])

    def test_es4_script_executes_the_prune_end_to_end(self):
        # Run the REAL es4 script (docker stub resolves `docker exec es4-kafka <cmd>` back to
        # the kafka stubs): the mirrored legacy topic and retired groups are pruned, the
        # near-miss group and unprefixed prod topic survive.
        es4_script = ROOT / "scripts/es4/create-es-topics.sh"
        result = self.run_script(
            topics=["es." + LEGACY_TOPIC, LEGACY_TOPIC],
            groups=["zero-dte-intelligence-service-v1-prod", "zero-dte-intelligence-service-v10"],
            script=es4_script,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("es." + LEGACY_TOPIC, self.topics())
        self.assertIn(LEGACY_TOPIC, self.topics())
        self.assertEqual(self.groups(), ["zero-dte-intelligence-service-v10"])
        self.assertIn("zero-orphan verified", result.stdout)

    def test_es4_script_fails_closed_when_group_listing_fails(self):
        es4_script = ROOT / "scripts/es4/create-es-topics.sh"
        result = self.run_script(
            topics=[], groups=[], flags=["fail_groups_list"], script=es4_script)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FATAL: could not list consumer groups", result.stderr)


if __name__ == "__main__":
    unittest.main()
