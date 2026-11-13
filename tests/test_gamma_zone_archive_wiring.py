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

import os
import plistlib
import re
import shutil
import subprocess
import tempfile
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
PROFILE_TOPIC = "options.spx.gamma-zone.profile"
CORPUS_SET = (CORPUS_TOPIC, PROFILE_TOPIC)
LABEL = "com.optionsedge.oe-archive-gamma-zone"


def env_value(name: str) -> str:
    m = re.search(rf'^{name}="([^"]*)"', TOPICS_ENV.read_text(), re.M)
    assert m, f"{name} is not declared in oe-topics.env"
    return m.group(1)


class TopicSet(unittest.TestCase):
    def test_the_corpus_set_is_the_log_AND_the_profile(self):
        """MEMBERSHIP, not equality, and not position: an earlier version of this asserted the whole
        variable equalled the log topic, so adding the profile failed here for no reason of
        substance. What matters is that both are present and that nothing else is.

        .current must NOT be archived: it is a compacted latest-value view that the next folded bar
        rebuilds, and every field in it is also in the log, so archiving it would copy one session
        over and over and add nothing. .profile is compacted the same way and is also rebuildable
        within a session -- but its content is NOT in the log. The log carries the extremes and the
        swing; only .profile carries the per-level volume distribution, i.e. the volume per point.
        Keyed sessionDate|symbol|timeframe under cleanup.policy=compact, so the cleaner keeps one
        record per session and the intra-session snapshots -- the ones that show a thin shelf BEFORE
        price falls through it -- are gone once the segment closes. The archive is what keeps them.
        """
        declared = env_value("GAMMA_ZONE_CORPUS").split()
        self.assertEqual(sorted(CORPUS_SET), sorted(declared))
        self.assertNotIn("options.spx.gamma-zone.current", declared)

    def test_every_archived_topic_is_one_the_SERVICE_actually_writes(self):
        """Pinned against the deploy repo's own declaration, not against this file's idea of the
        names. A rename that only half-landed would otherwise archive nothing while every assertion
        here still passed. Checked for EVERY member, so a future addition cannot skip the check."""
        text = DEPLOY_TOPICS.read_text()
        for topic in env_value("GAMMA_ZONE_CORPUS").split():
            with self.subTest(topic=topic):
                self.assertIn(f"{topic}:1", text)

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


class WrapperBehaviour(unittest.TestCase):
    """RUN the wrapper against stubs, rather than reading it.

    Codex round 1: "the tests are almost entirely textual ... they do not execute failure-path
    fixtures for inherited TOPICS, lock contention, arbitrary destination creation, multi-topic
    probing, or actual archiver invocation." Every assertion below drives the real script.
    """

    def run_wrapper(self, *, topics_env="options.spx.gamma-zone.log", archiver_rc=0, **env):
        """Stand up a fake host: a fake HOME, a stub archiver that records its environment, and a
        stub kafka-get-offsets.sh that records every --topic it was asked for."""
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        oe, kbin = tmp / "oe-ops", tmp / "kafka/bin"
        oe.mkdir(parents=True)
        kbin.mkdir(parents=True)
        shutil.copy(WRAPPER, oe / WRAPPER.name)
        (oe / "oe-topics.env").write_text(f'GAMMA_ZONE_CORPUS="{topics_env}"\n')
        (oe / "oe-alert.sh").write_text('alert() { echo "ALERT: $*" >> "$ALERT_SINK"; }\n')
        (oe / "oe-archive-kafka.sh").write_text(
            "#!/usr/bin/env bash\n"
            'printf "TOPICS=%s\\nARCHIVE_DIR=%s\\nALLOW_NON_NAS=%s\\nON_LOCK_BUSY=%s\\nENV=%s\\nARCHIVE_JOB=%s\\nOE_STRICT_TOPICS=%s\\n" '
            '"$TOPICS" "$ARCHIVE_DIR" "$ALLOW_NON_NAS" "$ON_LOCK_BUSY" "$ENV" "$ARCHIVE_JOB" "$OE_STRICT_TOPICS" > "$ARCHIVER_SINK"\n'
            f"exit {archiver_rc}\n")
        (oe / "oe-archive-kafka.sh").chmod(0o755)
        (kbin / "kafka-get-offsets.sh").write_text(
            # One RUN line per INVOCATION, then its topics. Recording only the topics could not
            # tell one call carrying "--topic a --topic b" from two calls carrying one each --
            # the same hole as asserting on .split() (Codex round 2).
            '#!/usr/bin/env bash\necho "RUN" >> "$PROBE_SINK"\nt=""\n'
            'while [ $# -gt 0 ]; do [ "$1" = "--topic" ] && { t="$2"; echo "$2" >> "$PROBE_SINK"; }; shift; done\n'
            '[ -z "${PROBE_NOISE:-}" ] || echo "WARNING: something chatty on stdout"\n'
            '[ -n "${PROBE_SILENT:-}" ] || echo "$t:0:123"\n'
            'exit ${PROBE_RC:-0}\n')
        (kbin / "kafka-get-offsets.sh").chmod(0o755)

        e = dict(os.environ)
        e.pop("TOPICS", None)
        e.update(HOME=str(tmp), KAFKA_BIN=str(kbin), JAVA_HOME="",
                 ARCHIVER_SINK=str(tmp / "archiver.env"), PROBE_SINK=str(tmp / "probes"),
                 ALERT_SINK=str(tmp / "alerts"), LOG=str(tmp / "log"))
        template = env.pop("ARCHIVE_DIR_TEMPLATE", None)
        if template is not None:
            env["ARCHIVE_DIR"] = template.replace("$HOME", str(tmp))
        e.update({k: str(v) for k, v in env.items()})
        proc = subprocess.run(["bash", str(oe / WRAPPER.name)], capture_output=True, text=True, env=e)
        read = lambda f: (tmp / f).read_text() if (tmp / f).exists() else ""
        return proc, read("archiver.env"), read("probes"), read("alerts"), tmp

    def test_an_inherited_TOPICS_does_NOT_reach_the_archiver(self):
        """THE finding. The wrapper and its own comment both claimed the set comes from the file,
        while TOPICS="${TOPICS:-...}" let any caller archive whatever it liked."""
        proc, archiver, _, _, _ = self.run_wrapper(TOPICS="some.other.topic")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("TOPICS=options.spx.gamma-zone.log", archiver)
        self.assertNotIn("some.other.topic", archiver)

    def test_it_asks_the_archiver_to_FAIL_on_lock_contention(self):
        """What this checks and what it does not: it proves the wrapper passes ON_LOCK_BUSY=fail,
        not that the real archiver then exits non-zero. The archiver's own default is "skip", which
        exits 0 having archived nothing; that behaviour is the archiver's to test (Codex round 3
        was right that the previous title claimed more than this)."""
        _, archiver, _, _, _ = self.run_wrapper()
        self.assertIn("ON_LOCK_BUSY=fail", archiver)

    def test_the_corpus_is_STRICT_so_an_absent_topic_is_not_a_quiet_day(self):
        """For an ordinary topic the archiver treats absent-or-unreadable as a quiet day: SKIP,
        absent++, exit 0. For THIS topic that is an hourly green run capturing none of the corpus.
        The preflight probe narrows the window; it cannot close it, because the topic can go between
        the probe and the read. Passed per job rather than added to the archiver's global default,
        which would make prod and es4 fail on a topic that is absent there by design."""
        _, archiver, _, _, _ = self.run_wrapper()
        self.assertIn("OE_STRICT_TOPICS=options.spx.gamma-zone.log", archiver)

    def test_it_archives_DEV_under_its_own_job_name(self):
        _, archiver, _, _, _ = self.run_wrapper()
        self.assertIn("ENV=dev", archiver)
        self.assertIn("ARCHIVE_JOB=gamma-zone", archiver)
        self.assertIn("ALLOW_NON_NAS=true", archiver)

    def test_a_destination_outside_the_approved_root_is_REFUSED(self):
        """A typo must not quietly create a successful archive tree nobody is looking at."""
        proc, archiver, _, alerts, tmp = self.run_wrapper(ARCHIVE_DIR=str(tmp_path := Path(tempfile.mkdtemp()) / "typo"))
        self.addCleanup(shutil.rmtree, tmp_path.parent, True)
        self.assertNotEqual(0, proc.returncode)
        self.assertEqual("", archiver, "the archiver must not have been invoked")
        self.assertFalse(tmp_path.exists(), "and the stray directory must not have been created")
        self.assertIn("outside", alerts)

    def test_a_destination_that_ESCAPES_the_approved_root_is_refused(self):
        """"$HOME"/oe-ops/* matches $HOME/oe-ops/../../somewhere, so a prefix test is not
        containment. Both sides are canonicalised with pwd -P and compared on a directory
        boundary, so this resolves outside the approved root and is refused."""
        escaping = "$HOME/oe-ops/../../escaped"   # expanded by the wrapper's own $HOME
        proc, archiver, _, alerts, tmp = self.run_wrapper(ARCHIVE_DIR_TEMPLATE=escaping)
        self.assertNotEqual(0, proc.returncode)
        self.assertEqual("", archiver)
        self.assertIn("outside the approved root", alerts)

    def test_that_refusal_can_be_MEANT_explicitly(self):
        outside = Path(tempfile.mkdtemp()) / "deliberate"
        self.addCleanup(shutil.rmtree, outside.parent, True)
        proc, archiver, _, _, _ = self.run_wrapper(ARCHIVE_DIR=str(outside),
                                                   ALLOW_ARCHIVE_DIR_OVERRIDE="true")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn(f"ARCHIVE_DIR={outside}", archiver)

    def test_every_topic_is_probed_SEPARATELY(self):
        """One --topic carrying the whole whitespace-joined set asks Kafka for a name with a space
        in it, and rejects a healthy broker the moment the set grows past one."""
        _, _, probes, _, _ = self.run_wrapper(topics_env="options.spx.gamma-zone.log another.topic")
        # LINES, not .split(). The stub records one line per invocation, and a single invocation
        # carrying "a b" splits into the same two words as two invocations carrying "a" and "b" --
        # so .split() passed while the bug was still there. My own mutation caught this.
        self.assertEqual(["RUN", "options.spx.gamma-zone.log", "RUN", "another.topic"],
                         probes.splitlines(),
                         "expected two separate invocations, each probing exactly one topic")

    def test_an_unreachable_broker_stops_it_before_it_archives(self):
        proc, archiver, _, alerts, _ = self.run_wrapper(PROBE_RC="1")
        self.assertNotEqual(0, proc.returncode)
        self.assertEqual("", archiver, "the archiver must not have been invoked")
        self.assertIn("cannot reach the dev broker", alerts)

    def test_a_SILENT_but_successful_probe_is_not_a_healthy_broker(self):
        """An exit code is not an answer. A CLI that exits 0 while printing nothing -- or printing a
        diagnostic -- passed as a handshake, and the archiver downstream treats an absent topic as
        nothing to do and finishes failed=0. The two together are a green run that captured
        nothing."""
        proc, archiver, _, alerts, _ = self.run_wrapper(PROBE_SILENT="1")
        self.assertNotEqual(0, proc.returncode)
        self.assertEqual("", archiver, "the archiver must not have been invoked")
        self.assertIn("no offsets", alerts)

    def test_a_CHATTY_but_healthy_broker_is_accepted(self):
        """A CLI that prints a warning first and the offsets after is a working broker. Matching the
        whole output rather than each line refused it -- fail-closed, but refusing a healthy broker
        is the same class of mistake as probing the whole topic list in one call (Codex round 4)."""
        proc, archiver, _, _, _ = self.run_wrapper(PROBE_NOISE="1")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("TOPICS=options.spx.gamma-zone.log", archiver)

    def test_an_archiver_failure_is_propagated_and_alerted(self):
        """launchd records the wrapper's exit code; swallowing the archiver's would make a job that
        captures nothing look healthy for ever."""
        proc, _, _, alerts, _ = self.run_wrapper(archiver_rc=3)
        self.assertEqual(3, proc.returncode)
        self.assertIn("FAILED", alerts)

    def test_a_missing_topic_set_refuses_rather_than_archiving_everything(self):
        proc, archiver, _, _, _ = self.run_wrapper(topics_env="")
        self.assertNotEqual(0, proc.returncode)
        self.assertEqual("", archiver)


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
        """Minute-only fires at the top of every LOCAL hour. It stays hourly across a daylight
        saving switch and across a change of the host's timezone; which UTC instant each run lands
        on moves, and nothing here depends on that. The checkpoint, not the schedule, decides what
        a run reads."""
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
