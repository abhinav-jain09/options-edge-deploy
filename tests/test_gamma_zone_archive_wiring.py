# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The gamma-zone corpus archive: options.spx.gamma-zone.log copied out of Kafka to a file on the dev Mac.

The log topic IS the corpus - scoring the service means reading months of it back - and Kafka is not
an archive. Two things take it away: the dev wipes (the topic is excluded from all four deletion
paths BY NAME, which is exactly the protection that failed when a cleanup run destroyed ten
keep-list topics it had just printed as kept), and the topic's own 1 GiB cap, which drops the oldest
emissions after about 87 sessions.

It runs on the DEV MAC under launchd, not on .252 under the crontab, because .252's archive entries
are disabled by the owner with "archive moves to dev" written beside them. Installed on dev and
proven there 2026-11-11: 381 records, exit 0, agent registered hourly at minute 0.
"""
from __future__ import annotations

import plistlib
import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR_REPO = ROOT / "scripts/ops/archive"
WRAPPER = ARCHIVE_DIR_REPO / "oe-archive-gamma-zone.sh"
PLIST = ARCHIVE_DIR_REPO / "launchd/oe-archive-gamma-zone.plist"
INSTALLER = ARCHIVE_DIR_REPO / "install-dev-mac-gamma-zone-archive.sh"
TOPICS_ENV = ARCHIVE_DIR_REPO / "oe-topics.env"
ARCHIVER = ARCHIVE_DIR_REPO / "oe-archive-kafka.sh"
DEPLOY_TOPICS = ROOT / "scripts/kafka/topics.env"
CRONTAB = ARCHIVE_DIR_REPO / "oe-archive.crontab"

CORPUS_TOPIC = "options.spx.gamma-zone.log"
LABEL = "com.optionsedge.oe-archive-gamma-zone"


def env_value(name: str) -> str:
    m = re.search(rf'^{name}="([^"]*)"', TOPICS_ENV.read_text(), re.M)
    assert m, f"{name} is not declared in oe-topics.env"
    return m.group(1)


class TopicSet(unittest.TestCase):
    def test_the_corpus_set_is_declared_and_is_ONLY_the_log(self):
        """.current must NOT be archived: it is a compacted latest-value view that the next folded
        bar rebuilds, so archiving it would copy one session over and over and add nothing the log
        does not already hold."""
        self.assertEqual(CORPUS_TOPIC, env_value("GAMMA_ZONE_CORPUS"))
        self.assertNotIn("gamma-zone.current", env_value("GAMMA_ZONE_CORPUS"))

    def test_the_topic_is_the_one_the_SERVICE_actually_writes(self):
        """Pinned against the deploy repo's own declaration, not against this file's idea of the
        name. A rename that only half-landed would otherwise archive nothing while every assertion
        here still passed."""
        self.assertIn(f"{CORPUS_TOPIC}:1", DEPLOY_TOPICS.read_text())

    def test_the_wrapper_takes_the_set_FROM_THE_FILE_and_refuses_an_inherited_one(self):
        """An inherited GAMMA_ZONE_CORPUS would let a caller archive a different set behind the
        canonical file's back - equality by luck. oe-archive-dev.sh makes the same refusal."""
        text = WRAPPER.read_text()
        self.assertIn("unset GAMMA_ZONE_CORPUS", text)
        self.assertIn('${GAMMA_ZONE_CORPUS:?', text)


class Wrapper(unittest.TestCase):
    def test_it_parses(self):
        subprocess.run(["bash", "-n", str(WRAPPER)], check=True, capture_output=True)

    def test_it_runs_against_DEV_with_its_own_lock_key(self):
        """ARCHIVE_JOB is half the archiver's lock key, not a label: two jobs once shared one and the
        daily run exited 0 having archived nothing, every weekday for two weeks."""
        text = WRAPPER.read_text()
        self.assertIn("ENV=dev", text)
        self.assertIn("ARCHIVE_JOB=gamma-zone", text)

    def test_it_passes_ALLOW_NON_NAS_because_local_disk_is_the_POINT(self):
        """This is the one archive wrapper that does not want the NAS sentinel. The NAS is full and
        nothing has archived anywhere since 2026-10-26, so a NAS destination would be an hourly
        failure. The flag is unconditional here and the destination is logged on every run."""
        text = WRAPPER.read_text()
        self.assertIn("ALLOW_NON_NAS=true", text)
        self.assertNotIn("oe_nas_sentinel", text)

    def test_reachability_is_a_KAFKA_HANDSHAKE_not_a_port_check(self):
        """The dev broker advertises host.docker.internal. A client that connects to the port and
        then cannot resolve the advertised listener fails with UnknownHostException, while a plain
        TCP check passes and tells you nothing - the trap oe-archive-dev.sh documents."""
        text = WRAPPER.read_text()
        self.assertIn("kafka-get-offsets.sh", text)
        self.assertNotIn("/dev/tcp/", text)

    def test_the_archiver_is_invoked_through_env_not_a_bare_assignment_prefix(self):
        """${JAVA_HOME:+JAVA_HOME=...} expands to ONE WORD containing '='. Bash decides what is an
        assignment prefix BEFORE expansion, so that word became the command name and the first real
        run died with exit 127. env treats every leading NAME=VALUE word as an assignment, expanded
        or not. Found by running the wrapper, not by reading it."""
        text = WRAPPER.read_text()
        self.assertRegex(text, r"(?m)^env TOPICS=")
        self.assertNotRegex(text, r"(?m)^\$\{JAVA_HOME:\+")

    def test_it_refuses_to_fill_the_dev_machines_disk(self):
        """The destination is the dev Mac's own disk, which also carries the dev cluster: filling it
        takes the environment down, not just the archive."""
        text = WRAPPER.read_text()
        self.assertIn("MIN_FREE_MB", text)
        self.assertIn("df -m", text)

    def test_a_failure_alerts_rather_than_sitting_in_a_log_nobody_reads(self):
        text = WRAPPER.read_text()
        self.assertIn("oe-alert.sh", text)
        self.assertIn("FAILURE: archiver exited", text)


class LaunchdAgent(unittest.TestCase):
    def plist(self) -> dict:
        return plistlib.loads(PLIST.read_bytes())

    def test_it_is_a_valid_plist_with_this_label(self):
        """Parsed by a STRICT parser, not just `plutil -lint`. plutil accepted a comment containing
        '--', which is illegal anywhere inside an XML comment; expat refused it. launchd happened to
        be lenient too, so the only thing that caught it was this test."""
        self.assertEqual(LABEL, self.plist()["Label"])

    def test_it_runs_the_wrapper_the_installer_places(self):
        """A plist naming a different path is the failure that looks like a working install."""
        args = self.plist()["ProgramArguments"]
        self.assertTrue(args[-1].endswith("/oe-ops/oe-archive-gamma-zone.sh"), args)
        # The installer checks the same thing on the host, against its own $DEST, before it loads
        # anything -- this pins that the two agree on the file name at all.
        self.assertIn('grep -q "$DEST/$WRAPPER"', INSTALLER.read_text())
        self.assertIn('WRAPPER="oe-archive-gamma-zone.sh"', INSTALLER.read_text())

    def test_it_fires_hourly_at_minute_zero(self):
        """Minute-only is the same instant in every zone, so daylight saving and a change of the
        host's timezone are both harmless. The checkpoint, not the schedule, decides what a run
        reads."""
        self.assertEqual({"Minute": 0}, self.plist()["StartCalendarInterval"])

    def test_it_does_NOT_pin_ARCHIVE_DIR_so_the_local_default_applies(self):
        """Setting it here would silently override the wrapper's own destination, and the move to
        the NAS has to move the existing tree with it - the offsets checkpoint lives under
        ARCHIVE_DIR, so a fresh directory re-archives from zero and duplicates everything."""
        self.assertNotIn("ARCHIVE_DIR", self.plist().get("EnvironmentVariables", {}))

    def test_it_does_not_run_at_load(self):
        """RunAtLoad would fire an archive during the install, before anyone has checked the host."""
        self.assertFalse(self.plist().get("RunAtLoad", False))


class Installer(unittest.TestCase):
    def test_it_parses(self):
        subprocess.run(["bash", "-n", str(INSTALLER)], check=True, capture_output=True)

    def test_it_places_every_file_the_wrapper_and_archiver_read_beside_themselves(self):
        """A missing one is a job that fails hourly. The wrapper sources oe-topics.env and
        oe-alert.sh and runs oe-archive-kafka.sh; the archiver reads the same two beside itself."""
        m = re.search(r'^FILES="([^"]+)"', INSTALLER.read_text(), re.M)
        self.assertIsNotNone(m)
        placed = set(m.group(1).split())
        self.assertEqual({"$WRAPPER", "oe-archive-kafka.sh", "oe-topics.env", "oe-alert.sh"}, placed)
        for name in ("oe-topics.env", "oe-alert.sh", "oe-archive-kafka.sh"):
            self.assertTrue((ARCHIVE_DIR_REPO / name).exists(), f"{name} is not in the repo")

    def test_it_verifies_the_STAGED_copy_not_the_repo_copy(self):
        """A stale or truncated host copy that still parses would otherwise be found an hour later,
        in a log nobody is reading."""
        text = INSTALLER.read_text()
        self.assertIn("the staged oe-topics.env derives", text)
        self.assertIn("env -i", text)

    def test_it_proves_exactly_one_agent_registered(self):
        self.assertIn("expected exactly one", INSTALLER.read_text())

    def test_it_needs_no_canonical_avro_sidecars(self):
        """The service's outputs are JSON on the wire, so there is no helper jar to place - unlike
        the depth installer, which refuses without one. If the format ever changes this installer
        must grow that refusal: a job scheduled without its reader fails silently and often."""
        self.assertNotIn("SIDECARS", INSTALLER.read_text())
        m = re.search(r'^OE_CANONICAL_ARCHIVE_TOPICS="([^"]*)"', ARCHIVER.read_text(), re.M)
        self.assertIsNotNone(m)
        self.assertNotIn("gamma-zone", m.group(1))


class NotOnTheWrongHost(unittest.TestCase):
    def test_the_252_crontab_does_NOT_schedule_this(self):
        """.252's archive entries are disabled by the owner - "archive moves to dev" - and an entry
        there would be a second, competing capture against a host whose jobs are switched off."""
        self.assertNotIn("gamma-zone", CRONTAB.read_text())


if __name__ == "__main__":
    unittest.main()
