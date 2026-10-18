"""scripts/kafka/reset-indicator-state.sh against stub kafka CLIs: refuses a group with members, resets the input
offsets to 0 and reads them back, deletes the changelog and waits for it to disappear, mutates nothing on a dry run."""
from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/reset-indicator-state.sh"


ZERO2 = "indicator-service-dev underlying.spx.price 0 0 120 120\\nindicator-service-dev underlying.spx.price 1 0 99 99"


def run(state: str, offsets_after: str, changelog_present: bool, env: dict | None = None, describe_rc: int = 0,
        partition_count: str = "2", states: list[str] | None = None):
    tmp = Path(tempfile.mkdtemp())
    calls = tmp / "calls"
    calls.touch()
    listing = tmp / "list"
    listing.write_text("indicator-service-dev-indicator-state-changelog\nother\n" if changelog_present else "other\n")
    kcg = tmp / "kafka-consumer-groups"
    if states:
        # successive --state answers: the n-th describe returns states[n] (the last one repeats)
        seq = tmp / "seq"
        seq.write_text("0")
        arr = " ".join(states)
        state_line = (f'  *" --state "*) n=$(cat "{seq}"); set -- {arr}; i=$(( n < $# ? n : $# - 1 )); shift $i; st=$1; echo $((n + 1)) > "{seq}";'
                      f' printf "GROUP  COORDINATOR  ASSIGNMENT-STRATEGY  STATE  #MEMBERS\\nindicator-service-dev  b:1 (1)  range  $st  0\\n"; exit 0 ;;\n')
    else:
        state_line = (f'  *" --state "*) echo "Consumer group \'indicator-service-dev\' does not exist."; exit 1 ;;\n' if state == "absent" else
                      f'  *" --state "*) printf "GROUP  COORDINATOR  ASSIGNMENT-STRATEGY  STATE  #MEMBERS\\nindicator-service-dev  b:1 (1)  range  {state}  0\\n"; exit {describe_rc} ;;\n')
    kcg.write_text(
        "#!/bin/bash\n"
        f'echo "KCG $*" >> "{calls}"\n'
        'case " $* " in\n'
        + state_line +
        f'  *" --reset-offsets "*) echo "reset"; exit 0 ;;\n'
        f'  *" --offsets "*) [ -n "$FAIL_OFFSETS" ] && {{ echo "Error: broker gone"; exit 1; }}; printf "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\\n{offsets_after}\\n"; exit 0 ;;\n'
        "esac\nexit 1\n")
    kcg.chmod(0o755)
    kt = tmp / "kafka-topics"
    kt.write_text(
        "#!/bin/bash\n"
        f'echo "KT $*" >> "{calls}"\n'
        'case " $* " in\n'
        f'  *" --list "*) grep -v __deleted__ "{listing}"; exit 0 ;;\n'
        f'  *" --describe --topic underlying.spx.price "*) printf "Topic: underlying.spx.price\\tTopicId: x\\tPartitionCount: {partition_count}\\tReplicationFactor: 1\\tConfigs: \\n"; exit 0 ;;\n'
        f'  *" --delete "*) sed -i.bak "s/^indicator-service-dev-indicator-state-changelog$/__deleted__/" "{listing}"; exit 0 ;;\n'
        "esac\nexit 1\n")
    kt.chmod(0o755)
    e = dict(os.environ, KAFKA_BOOTSTRAP_SERVERS="b:1", INDICATOR_APP_ID="indicator-service-dev",
             INDICATOR_INPUT_TOPIC="underlying.spx.price", KAFKA_CONSUMER_GROUPS_CMD=str(kcg), KAFKA_TOPICS_CMD=str(kt),
             KAFKA_TOPIC_DELETE_WAIT_SECONDS="3")
    e.update(env or {})
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=e)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines()


class ResetIndicatorState(unittest.TestCase):
    def test_empty_group_is_reset_to_zero_and_changelog_deleted(self):
        rc, out, calls = run("Empty", ZERO2, True)
        self.assertEqual(rc, 0, out)
        self.assertTrue(any("--reset-offsets --to-earliest --execute" in c and "--topic underlying.spx.price" in c for c in calls), calls)
        self.assertTrue(any("--delete --topic indicator-service-dev-indicator-state-changelog" in c for c in calls), calls)
        self.assertIn("0 on every one of the 2 partitions", out)
        self.assertIn("changelog indicator-service-dev-indicator-state-changelog deleted", out)
        reset_at = next(i for i, c in enumerate(calls) if "--reset-offsets" in c)
        delete_at = next(i for i, c in enumerate(calls) if "--delete" in c)
        self.assertLess(reset_at, delete_at, "offsets reset BEFORE the changelog purge (plan section 1)")

    def test_only_an_absent_or_empty_group_may_be_reset(self):
        # the allow-list is explicit: Stable, a rebalance, Dead, an unparseable answer and a failed describe all refuse
        for state, rc_desc in [("Stable", 0), ("PreparingRebalance", 0), ("CompletingRebalance", 0), ("Dead", 0),
                               ("Garbage", 0), ("Empty", 1)]:
            # a member state is POLLED for GROUP_EMPTY_WAIT_S before the refusal; zero means refuse at once
            rc, out, calls = run(state, "", True, describe_rc=rc_desc, env={"GROUP_EMPTY_WAIT_S": "0"})
            self.assertEqual(rc, 1, (state, rc_desc, out))
            self.assertIn("only an absent or Empty group may be reset", out, state)
            self.assertFalse(any("--reset-offsets" in c or "--delete" in c for c in calls), state)
        rc, out, _ = run("absent", ZERO2, True)
        self.assertEqual(rc, 0, out)
        self.assertIn("group indicator-service-dev: absent", out)

    def test_a_group_whose_last_member_is_still_leaving_is_waited_for(self):
        # es4-deploy #791: the pods were gone but the coordinator still held a member (Stable) for a few seconds,
        # the reset refused and the deployment was left at 0 replicas. A Stable or rebalancing group is polled
        # until it empties (bounded), then the reset proceeds exactly as for an Empty group.
        rc, out, calls = run("Empty", ZERO2, True, states=["Stable", "PreparingRebalance", "Empty"], env={"GROUP_EMPTY_WAIT_S": "60"})
        self.assertEqual(rc, 0, out)
        self.assertIn("Stable — waiting for the last member to leave", out)
        self.assertIn("group indicator-service-dev: Empty", out)
        self.assertEqual(3, sum(1 for c in calls if "--state" in c), calls)
        self.assertTrue(any("--reset-offsets" in c for c in calls))
        # Dead and an unparseable state are never waited for
        for state in ["Dead", "Garbage"]:
            rc, out, calls = run("Empty", ZERO2, True, states=[state, "Empty"], env={"GROUP_EMPTY_WAIT_S": "60"})
            self.assertEqual(rc, 1, (state, out))
            self.assertEqual(1, sum(1 for c in calls if "--state" in c), state)
        # the wait is bounded: a group that never empties is refused after the budget
        rc, out, calls = run("Empty", ZERO2, True, states=["Stable"], env={"GROUP_EMPTY_WAIT_S": "5"})
        self.assertEqual(rc, 1, out)
        self.assertIn("only an absent or Empty group may be reset", out)
        self.assertFalse(any("--reset-offsets" in c for c in calls))

    def test_an_unreadable_partition_count_refuses(self):
        rc, out, calls = run("Empty", ZERO2, True, partition_count="")
        self.assertEqual(rc, 1)
        self.assertIn("cannot read the partition count", out)
        self.assertFalse(any("--reset-offsets" in c or "--delete" in c for c in calls))

    def test_offsets_that_did_not_read_back_zero_on_every_partition_fail(self):
        cases = {
            "one partition not zero": "indicator-service-dev underlying.spx.price 0 0 120 120\\nindicator-service-dev underlying.spx.price 1 57 99 42",
            "a partition row missing": "indicator-service-dev underlying.spx.price 0 0 120 120",
            "an extra partition row": ZERO2 + "\\nindicator-service-dev underlying.spx.price 2 0 5 5",
            "no committed row at all": "",
        }
        for label, rows in cases.items():
            rc, out, calls = run("Empty", rows, True)
            self.assertEqual(rc, 1, label)
            self.assertIn("after the reset are [", out, label)
            self.assertFalse(any("--delete" in c for c in calls), label + ": the changelog is not touched after a failed reset")

    def test_a_failed_offset_read_back_fails(self):
        rc, out, calls = run("Empty", "", True, env={"FAIL_OFFSETS": "1"})
        self.assertEqual(rc, 1)
        self.assertIn("could not read the offsets", out)
        self.assertFalse(any("--delete" in c for c in calls))

    def test_absent_changelog_is_fine(self):
        rc, out, calls = run("Empty", ZERO2, False)
        self.assertEqual(rc, 0, out)
        self.assertIn("changelog indicator-service-dev-indicator-state-changelog absent", out)

    def test_dry_run_mutates_nothing(self):
        rc, out, calls = run("Empty", "", True, env={"DEPLOY_DRY_RUN": "true"})
        self.assertEqual(rc, 0, out)
        self.assertFalse(any("--reset-offsets" in c or "--delete" in c for c in calls))


if __name__ == "__main__":
    unittest.main()
