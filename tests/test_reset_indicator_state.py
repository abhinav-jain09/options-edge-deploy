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


def run(state: str, offsets_after: str, changelog_present: bool, env: dict | None = None):
    tmp = Path(tempfile.mkdtemp())
    calls = tmp / "calls"
    calls.touch()
    listing = tmp / "list"
    listing.write_text("indicator-service-dev-indicator-state-changelog\nother\n" if changelog_present else "other\n")
    kcg = tmp / "kafka-consumer-groups"
    kcg.write_text(
        "#!/bin/bash\n"
        f'echo "KCG $*" >> "{calls}"\n'
        'case " $* " in\n'
        f'  *" --state "*) printf "GROUP  COORDINATOR  ASSIGNMENT-STRATEGY  STATE  #MEMBERS\\nindicator-service-dev  b:1 (1)  range  {state}  0\\n"; exit 0 ;;\n'
        f'  *" --reset-offsets "*) echo "reset"; exit 0 ;;\n'
        f'  *" --offsets "*) printf "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\\n{offsets_after}\\n"; exit 0 ;;\n'
        "esac\nexit 1\n")
    kcg.chmod(0o755)
    kt = tmp / "kafka-topics"
    kt.write_text(
        "#!/bin/bash\n"
        f'echo "KT $*" >> "{calls}"\n'
        'case " $* " in\n'
        f'  *" --list "*) grep -v __deleted__ "{listing}"; exit 0 ;;\n'
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
        rc, out, calls = run("Empty", "indicator-service-dev underlying.spx.price 0 0 120 120", True)
        self.assertEqual(rc, 0, out)
        self.assertTrue(any("--reset-offsets --to-earliest --execute" in c and "--topic underlying.spx.price" in c for c in calls), calls)
        self.assertTrue(any("--delete --topic indicator-service-dev-indicator-state-changelog" in c for c in calls), calls)
        self.assertIn("0 on every partition", out)
        self.assertIn("changelog indicator-service-dev-indicator-state-changelog deleted", out)
        reset_at = next(i for i, c in enumerate(calls) if "--reset-offsets" in c)
        delete_at = next(i for i, c in enumerate(calls) if "--delete" in c)
        self.assertLess(reset_at, delete_at, "offsets reset BEFORE the changelog purge (plan section 1)")

    def test_group_with_members_is_refused_before_anything(self):
        rc, out, calls = run("Stable", "", True)
        self.assertEqual(rc, 1)
        self.assertIn("still has members", out)
        self.assertFalse(any("--reset-offsets" in c or "--delete" in c for c in calls))

    def test_offsets_that_did_not_read_back_zero_fail(self):
        rc, out, calls = run("Empty", "indicator-service-dev underlying.spx.price 0 57 120 63", True)
        self.assertEqual(rc, 1)
        self.assertIn("not 0 after the reset", out)
        self.assertFalse(any("--delete" in c for c in calls), "the changelog is not touched after a failed reset")

    def test_absent_changelog_is_fine(self):
        rc, out, calls = run("Empty", "indicator-service-dev underlying.spx.price 0 0 120 120", False)
        self.assertEqual(rc, 0, out)
        self.assertIn("changelog indicator-service-dev-indicator-state-changelog absent", out)

    def test_dry_run_mutates_nothing(self):
        rc, out, calls = run("Empty", "", True, env={"DEPLOY_DRY_RUN": "true"})
        self.assertEqual(rc, 0, out)
        self.assertFalse(any("--reset-offsets" in c or "--delete" in c for c in calls))


if __name__ == "__main__":
    unittest.main()
