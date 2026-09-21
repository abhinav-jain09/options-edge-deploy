"""prod-cleanup.sh must REPORT the Kafka data dir's own filesystem and WARN at >= 80 % — reporting only.

2026-09-16: /home/kafka (the Kafka log dir's own 1.9 TB NVMe) hit 100 % and the broker crash-looped,
while the nightly cleanup's BEFORE/AFTER lines and Discord post only ever showed root(/) and /home.
These tests run the remote script's reporting functions against a fake df/findmnt on PATH.
"""
import os
import re
import stat
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "prod-cleanup.sh"

FAKE_DF = r"""#!/usr/bin/env bash
# fake df: last arg is the path; usage comes from FAKE_PCT_<mount> env (default 31), -h vs -P ignored.
p="${@: -1}"
case "$p" in
  "$FAKE_KAFKA_DATA"|"$FAKE_KAFKA_DATA"/*|/home/kafka|/home/kafka/*) pct="${FAKE_PCT_KAFKA:-31}"; dev=/dev/nvme0n1p1; mnt=/home/kafka; size=1.9T; used=1.6T;;
  /home|/home/*)             pct="${FAKE_PCT_HOME:-40}";  dev=/dev/mapper/rl-home; mnt=/home; size=800G; used=320G;;
  *)                          pct="${FAKE_PCT_ROOT:-55}";  dev=/dev/mapper/rl-root; mnt=/; size=70G; used=38G;;
esac
echo "Filesystem      Size  Used Avail Use% Mounted on"
echo "$dev  $size  $used  0  ${pct}% $mnt"
"""

FAKE_FINDMNT = r"""#!/usr/bin/env bash
# fake findmnt -T PATH -n -o TARGET: anything under /home/kafka is its own mount.
[ "${FAKE_FINDMNT_FAIL:-}" = 1 ] && exit 1
case "$2" in "$FAKE_KAFKA_DATA"|"$FAKE_KAFKA_DATA"/*|/home/kafka|/home/kafka/*) echo /home/kafka;; /home*) echo /home;; *) echo /;; esac
"""


def remote_script_body() -> str:
    text = SCRIPT.read_text()
    match = re.search(r"<<'REMOTE'\n(.*?)\nREMOTE\n", text, re.DOTALL)
    assert match, "prod-cleanup.sh no longer embeds the remote script as a <<'REMOTE' heredoc"
    return match.group(1)


class ProdCleanupReportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.remote = self.dir / "remote.sh"
        self.remote.write_text(remote_script_body())
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        for name, body in (("df", FAKE_DF), ("findmnt", FAKE_FINDMNT)):
            p = self.bin / name
            p.write_text(body)
            p.chmod(p.stat().st_mode | stat.S_IXUSR)
        self.kafka_data = self.dir / "kraft-combined-logs"
        self.kafka_data.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def run_report(self, *calls: str, kafka_pct="31", kafka_data=None, extra_env=None) -> str:
        env = dict(os.environ)
        env["PATH"] = f"{self.bin}:{env['PATH']}"
        env["FAKE_PCT_KAFKA"] = kafka_pct
        env.update(extra_env or {})
        # `source` loads the functions without running the cleanup (guarded on BASH_SOURCE == $0).
        # KAFKA_DATA points at a real temp dir (kafka_mount checks it exists) which the fake df and
        # findmnt classify as the /home/kafka mount via FAKE_KAFKA_DATA.
        data = kafka_data if kafka_data is not None else str(self.kafka_data)
        env["FAKE_KAFKA_DATA"] = str(self.kafka_data)
        script = (
            f'source "{self.remote}"; KAFKA_DATA="{data}"; '
            + "; ".join(calls)
        )
        return subprocess.check_output(["bash", "-c", script], env=env, text=True)

    def test_script_parses_and_never_carries_the_password(self):
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)
        text = SCRIPT.read_text()
        self.assertIn('PW=$(cat "$OPS/.prod-ssh-pw"', text)
        self.assertNotRegex(text, r"^PW='", "the SSH password must come from the 0600 file, not the script")

    def test_sourcing_the_remote_script_runs_no_cleanup(self):
        out = self.run_report("echo sourced-ok", extra_env={"OUT": str(self.dir / "never-written")})
        self.assertEqual(out.strip(), "sourced-ok")
        self.assertFalse((self.dir / "never-written").exists())

    def test_before_after_report_the_kafka_mount_resolved_by_findmnt(self):
        # /home/kafka/... must NOT be reported as /home: the label carries the resolved mount point.
        out = self.run_report("report_disks BEFORE", "report_disks AFTER", kafka_pct="87",
                              extra_env={"FAKE_PCT_HOME": "40", "FAKE_PCT_ROOT": "55"})
        lines = out.splitlines()
        self.assertEqual(len(lines), 2)
        self.assertRegex(lines[0], r"^BEFORE  root\(/\) 38G/70G 55%   home 320G/800G 40%   kafka\(/home/kafka\) 1\.6T/1\.9T 87%$")
        self.assertTrue(lines[1].startswith("AFTER   root(/)"))
        self.assertIn("kafka(/home/kafka) 1.6T/1.9T 87%", lines[1])

    def test_kafka_mount_falls_back_to_df_when_findmnt_fails(self):
        out = self.run_report("kafka_mount; echo", extra_env={"FAKE_FINDMNT_FAIL": "1"})
        self.assertEqual(out.strip(), "/home/kafka")

    def test_warn_below_threshold_is_silent(self):
        self.assertEqual(self.run_report("warn_kafka", kafka_pct="79"), "")

    def test_warn_at_and_above_threshold_is_loud_and_names_the_mount(self):
        for pct in ("80", "87", "100"):
            with self.subTest(pct=pct):
                out = self.run_report("warn_kafka", kafka_pct=pct)
                self.assertRegex(out, rf"^WARN    kafka\(/home/kafka\) {pct}% used >= 80%")
                self.assertIn("NOT cleaned here", out)

    def test_warn_when_kafka_data_dir_is_missing(self):
        out = self.run_report("report_disks BEFORE", "warn_kafka", kafka_data=str(self.dir / "absent"))
        self.assertIn("kafka(?) MISSING", out.splitlines()[0])
        self.assertRegex(out.splitlines()[1], r"^WARN    kafka data dir .*absent is MISSING")

    def test_warn_line_is_a_single_line_the_mac_side_can_grep(self):
        out = self.run_report("warn_kafka", kafka_pct="95")
        self.assertEqual(len(out.splitlines()), 1)
        text = SCRIPT.read_text()
        # Mac side: a WARN always posts (even under NOTIFY=onchange) and becomes an @here mention.
        self.assertIn("WARNS=$(printf '%s\\n' \"$RESULT\" | grep '^WARN')", text)
        self.assertIn('[ "$CHANGED" -eq 0 ] && [ -z "$WARNS" ]', text)
        self.assertIn('CONTENT="@here $WARNS"', text)

    def test_remote_script_never_deletes_kafka_data(self):
        body = remote_script_body()
        # The only rm/find -delete targets are the log4j dirs, the package caches and the journal.
        for line in body.splitlines():
            if "rm -rf" in line or "-delete" in line:
                self.assertNotIn("KAFKA_DATA", line)
                self.assertNotIn("kraft-combined-logs", line)
        self.assertNotIn("kafka-delete-records", body)
        self.assertNotIn("kafka-configs", body)


if __name__ == "__main__":
    unittest.main()


class BuildStagingPruneTest(unittest.TestCase):
    """The image builds' staging directories are the HOST's to prune, and only ever inside its own root.

    Production image builds rsync the whole workspace to $HOME/ci/remote-builds/<name> and no longer
    delete it themselves: from the build side the path is caller-influenced and can be raced by another
    process on the shared host. The host half is here — fixed root, age-based, never following a symlink
    out — and these tests run it against a real temporary HOME with real directories and symlinks.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.home = self.dir / "home"
        (self.home / "ci" / "remote-builds").mkdir(parents=True)
        self.remote = self.dir / "remote.sh"
        self.remote.write_text(remote_script_body())

    def tearDown(self):
        self.tmp.cleanup()

    def prune(self, keep_days="3"):
        env = dict(os.environ)
        env["HOME"] = str(self.home)
        script = (f'source "{self.remote}"; BUILD_STAGING="$HOME/ci/remote-builds"; '
                  f'BUILD_STAGING_KEEP_DAYS={keep_days}; clean_build_staging')
        return subprocess.check_output(["bash", "-c", script], env=env, text=True)

    def _staging(self, name, age_days):
        d = self.home / "ci" / "remote-builds" / name
        d.mkdir()
        (d / "workspace-file").write_text("x")
        when = time.time() - age_days * 86400
        os.utime(d, (when, when))
        return d

    def test_old_directories_go_and_fresh_ones_stay(self):
        old = self._staging("options-edge-processing-1.aaaaaaaa", 10)
        fresh = self._staging("options-edge-processing-2.bbbbbbbb", 0)
        out = self.prune()
        self.assertFalse(old.exists(), out)
        self.assertTrue(fresh.exists(), out)
        self.assertIn("purge  build-staging", out)
        self.assertIn("1 dir(s) older than 3d", out)

    def test_a_symlink_inside_the_root_is_never_followed(self):
        outside = self.dir / "outside"
        outside.mkdir()
        (outside / "keep").write_text("x")
        link = self.home / "ci" / "remote-builds" / "options-edge-processing-3.cccccccc"
        link.symlink_to(outside)
        when = time.time() - 10 * 86400
        os.utime(link, (when, when), follow_symlinks=False)
        self.prune()
        self.assertTrue((outside / "keep").exists(), "a symlinked staging entry was followed out of the root")

    def test_a_staging_root_that_is_a_symlink_is_not_pruned(self):
        elsewhere = self.dir / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / "keep").write_text("x")
        root = self.home / "ci" / "remote-builds"
        for child in root.iterdir():
            child.unlink()
        root.rmdir()
        root.symlink_to(elsewhere)
        out = self.prune()
        self.assertIn("is a symlink", out)
        self.assertTrue((elsewhere / "keep").exists())

    def test_nothing_to_prune_is_quiet_and_harmless(self):
        out = self.prune()
        self.assertIn("0 dir(s)", out)
