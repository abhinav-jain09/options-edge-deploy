"""Off-hours clean-slate: what it must NOT destroy.

The classification loop is the whole safety contract of this job — a topic lands
in exactly one of PRESERVE / delete-as-state / purge-as-data, and getting that
wrong is silent until someone runs a real wipe. These vectors execute the loop's
actual logic against realistic topic names for dev, prod and es4.
"""

import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "offhours-clean-slate.sh"

# Every indicator changelog the fleet actually creates, one per environment.
INDICATOR_CHANGELOGS = [
    "indicator-service-dev-indicator-state-changelog",
    "indicator-service-prod-indicator-state-changelog",
    "indicator-service-es4-indicator-state-changelog",
]


class OffhoursCleanSlatePreservedTest(unittest.TestCase):
    def classify(self, topics):
        """Run the script's real classification case against `topics`."""
        source = SCRIPT.read_text()
        case_body = re.search(
            r"  case \"\$t\" in\n(.*?)\n  esac\n", source, re.S
        )
        self.assertIsNotNone(case_body, "the classification case must stay findable")
        harness = (
            'STATE_TOPICS=""\nPURGE_TOPICS=""\nPRESERVED=""\nKEEP_DURABLE=0\n'
            "log() { :; }\n"
            'while read -r t; do\n  [ -n "$t" ] || continue\n'
            "  case \"$t\" in\n" + case_body.group(1) + "\n  esac\n"
            "done <<'TOPICS'\n" + "\n".join(topics) + "\nTOPICS\n"
            'echo "STATE:$STATE_TOPICS"\necho "PURGE:$PURGE_TOPICS"\n'
            'echo "KEEP:$KEEP_DURABLE"\n'
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "harness.sh"
            path.write_text(harness)
            out = subprocess.check_output(["bash", str(path)], text=True)
        parsed = {}
        for line in out.splitlines():
            key, _, value = line.partition(":")
            parsed[key] = value.split()
        return parsed

    def test_indicator_changelogs_are_preserved_in_every_environment(self):
        """Indicator warmup is an ACCUMULATION, not a cache.

        A timeframe becomes READY only after ~21 clean closes: 15m needs 5.25 h,
        1h needs 21 h, 4h needs 84 h. One session cannot rebuild that, so deleting
        this changelog does not cost a rebuild — it makes 1h and 4h permanently
        unreachable, because every clean would reset them long before 21 h of
        coverage exists.
        """
        result = self.classify(INDICATOR_CHANGELOGS)
        for topic in INDICATOR_CHANGELOGS:
            self.assertNotIn(topic, result["STATE"], f"{topic} must not be deleted")
            self.assertNotIn(topic, result["PURGE"], f"{topic} must not be purged")
        self.assertEqual(
            len(INDICATOR_CHANGELOGS), int(result["KEEP"][0]),
            "each environment's indicator changelog counts as preserved",
        )

    def test_ordinary_changelogs_are_still_deleted(self):
        """The preserve rule must be narrow — everything else still rebuilds."""
        topics = [
            "hpsf-processing-service-prod-strike-flow-changelog",
            "dealer-ledger-prod-ledger-state-changelog",
            "indicator-service-prod-indicator-state-repartition",
        ]
        result = self.classify(topics)
        for topic in topics:
            self.assertIn(topic, result["STATE"], f"{topic} still rebuilds from inputs")
        self.assertEqual(0, int(result["KEEP"][0]))

    def test_gamma_migration_scorer_stays_preserved(self):
        """The precedent this rule follows must not regress."""
        result = self.classify(["gamma-migration-prod-gamma-migration-scorer-changelog"])
        self.assertEqual([], result["STATE"])
        self.assertEqual(1, int(result["KEEP"][0]))

    def test_wipe_kafka_default_stays_true_because_nothing_else_reclaims_disk(self):
        """The `true` default is LOAD-BEARING, not an opt-in extra.

        Retention is eternal (KAFKA_TOPIC_RETENTION_MS = -1), so this job and
        dev-cleanup.sh are the only things that ever delete data. A stale comment
        in this script described the superseded 12h-TTL world and nearly cost a
        change that flipped the default to false — which would have silently
        turned every operator-run clean-slate into a scale-down.
        """
        source = SCRIPT.read_text()
        self.assertIn('WIPE_KAFKA="${WIPE_KAFKA:-true}"', source)
        self.assertNotIn('WIPE_KAFKA="${WIPE_KAFKA:-false}"', source)

    def invocation_blocks(self, jenkinsfile):
        """Every executable invocation of the script, with the env block feeding it.

        A shell step builds NAME='value' assignments across continuation lines
        and ends at the script path; the block is that whole step.
        """
        blocks = []
        lines = jenkinsfile.splitlines()
        for i, line in enumerate(lines):
            if "offhours-clean-slate.sh" not in line or line.strip().startswith("//"):
                continue
            # Only EXECUTIONS. scp/rsync ship the file; they do not run it, and
            # requiring env on them would be a false positive.
            first = line.strip().split()[0] if line.strip().split() else ""
            if first in ("scp", "cp", "rsync", "mkdir", "chmod"):
                continue
            # Walk BACK to the start of the env assignments feeding this call.
            j = i
            while j > 0 and (
                lines[j - 1].rstrip().endswith("\\")
                or "=" in lines[j - 1] and "'" in lines[j - 1]
            ):
                j -= 1
            blocks.append("\n".join(lines[j:i + 1]))
        return blocks

    def test_known_invocation_shapes_pass_wipe_kafka_or_the_default_stands(self):
        """Regression guard for the invocation shapes that exist today.

        Jenkins exposes DRY_RUN and WIPE_ENABLED only, so the script default is
        what decides whether an enabled run actually cleans. If the parameter is
        ever declared it must reach EVERY invocation — a value that stops at the
        declaration is the trap — and the checked-block count is asserted so an
        empty loop cannot pass.

        LIMIT, stated rather than implied: this recognises literal script-path
        invocations. A withEnv wrapper, a variable holding the path, or a
        shared-library step could add an invocation this never sees. It catches
        the known shapes; it does not prove the contract for all future ones.
        """
        jenkinsfile = (ROOT / "Jenkinsfile.offhours-clean-slate").read_text()
        source = SCRIPT.read_text()
        blocks = self.invocation_blocks(jenkinsfile)
        self.assertGreaterEqual(
            len(blocks), 1, "the Jenkinsfile must still invoke the script"
        )
        declared = re.search(
            r"""(?:booleanParam|string|choice|text)\s*\(\s*name:\s*['"]WIPE_KAFKA['"]""",
            jenkinsfile,
        )
        checked = 0
        for block in blocks:
            if declared:
                self.assertIn(
                    "WIPE_KAFKA='${WIPE_KAFKA}'", block,
                    "a declared WIPE_KAFKA must reach this invocation:\n" + block,
                )
            else:
                self.assertNotIn(
                    "WIPE_KAFKA=", block,
                    "an undeclared WIPE_KAFKA must not be passed:\n" + block,
                )
            checked += 1
        self.assertEqual(len(blocks), checked, "every invocation must be checked")
        if not declared:
            self.assertIn(
                'WIPE_KAFKA="${WIPE_KAFKA:-true}"', source,
                "with no caller override, the script default must stay true",
            )

    # Live guidance must state these; history may only appear inside the block
    # explicitly marked SUPERSEDED.
    FALSE_CONTRACT_CLAIMS = [
        "nothing is spared",          # two changelogs ARE spared
        "exactly the 3",              # more than the 3 system topics are kept
        "close+15",                   # CLOSE_BUFFER_MIN is 30
        "13:15",                      # cron is 13:30 / 16:30
        "log.retention.ms=1d",        # retention is eternal (-1)
        "self-expires",               # nothing self-expires any more
    ]

    def test_known_superseded_claims_do_not_come_back(self):
        """Regression list for the six false statements actually found here.

        The first version scanned for `12h`/`self-expire`, which is why the
        `log.retention.ms=1d` claim and the close+15 schedule slipped past it, and
        it allowlisted any line saying "earlier note". This checks the specific
        claims and confines history to the delimited SUPERSEDED block.

        LIMIT, stated rather than implied: these are exact substrings. Equivalent
        wording ("all changelogs are deleted", "Kafka clears data overnight") would
        pass. This stops the KNOWN mistakes returning; it is not a proof that every
        live line states the contract correctly.
        """
        source = SCRIPT.read_text()
        superseded = re.search(
            r"# 2026-07-08 \(SUPERSEDED.*?(?=\nWIPE_KAFKA=)", source, re.S
        )
        self.assertIsNotNone(superseded, "the superseded history block must stay delimited")
        history = superseded.group(0)
        for line_no, line in enumerate(source.splitlines(), 1):
            if line in history.splitlines():
                continue
            for claim in self.FALSE_CONTRACT_CLAIMS:
                self.assertNotIn(
                    claim, line,
                    f"line {line_no} states a superseded contract ({claim!r}): {line.strip()}",
                )

    def test_retention_is_eternal_so_this_job_owns_deletion(self):
        """The PREMISE behind the true default, read rather than assumed.

        Dropped by accident in an earlier rewrite while the commit message still
        claimed it existed. If retention ever stops being eternal, the reasoning
        for WIPE_KAFKA=true changes and this must be revisited deliberately.
        """
        configmap = (ROOT / "k8s" / "infra" / "base" / "configmap.yaml").read_text()
        self.assertIn('KAFKA_TOPIC_RETENTION_MS: "-1"', configmap)
        self.assertIn('KAFKA_MAX_RETENTION_MS: "-1"', configmap)

    def test_scheduled_runs_stay_log_only(self):
        """Both gates must keep defaulting to harmless."""
        source = SCRIPT.read_text()
        self.assertIn('DRY_RUN="${DRY_RUN:-true}"', source)
        self.assertIn('WIPE_ENABLED="${WIPE_ENABLED:-false}"', source)


if __name__ == "__main__":
    unittest.main()
