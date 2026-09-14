"""dev-cleanup.sh must unload the es4->dev mirror agents around the topic wipe.

2026-09-14: a running kafka-mirror-maker re-created es.options.databento.gex.strike 0.3 s after the
wipe deleted it, at dev's num.partitions=1, so ensure_topics' declared shape never applied. These
tests run the real pause/resume functions against a fake launchctl and fake LaunchAgents plists.
"""
import os
import re
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "dev-cleanup.sh"

FAKE_LAUNCHCTL = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    # Loaded labels live in $FAKE_STATE/loaded, one per line; every call is appended to $FAKE_STATE/calls.
    set -u
    echo "$*" >> "$FAKE_STATE/calls"
    loaded="$FAKE_STATE/loaded"; touch "$loaded"
    case "$1" in
      bootout)
        label="${2##*/}"
        grep -vx "$label" "$loaded" > "$loaded.tmp"; mv "$loaded.tmp" "$loaded" ;;
      bootstrap)
        label="$(basename "$3" .plist)"
        if grep -qx "$label" "$FAKE_STATE/refuse" 2>/dev/null; then exit 5; fi
        grep -qx "$label" "$loaded" && exit 37
        echo "$label" >> "$loaded" ;;
      list)
        grep -qx "$2" "$loaded" ;;
    esac
    """
)


def plist(program: str) -> str:
    return textwrap.dedent(
        f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <plist version="1.0">
        <dict>
            <key>KeepAlive</key>
            <true/>
            <key>ProgramArguments</key>
            <array>
                <string>{program}</string>
            </array>
        </dict>
        </plist>
        """
    )


def helper_block() -> str:
    text = SCRIPT.read_text()
    start = text.index("LAUNCH_AGENTS_DIR=")
    end = text.index("\n}\n", text.index("resume_dev_mirrors() {")) + 3
    return text[start:end]


class DevCleanupPausesDevMirrorsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.agents = base / "LaunchAgents"
        self.ops = base / "oe-ops"
        self.state = base / "state"
        for d in (self.agents, self.ops, self.state):
            d.mkdir()
        self.launchctl = base / "launchctl"
        self.launchctl.write_text(FAKE_LAUNCHCTL)
        self.launchctl.chmod(self.launchctl.stat().st_mode | stat.S_IEXEC)
        self.paused = self.ops / ".dev-mirrors-paused"
        # label -> (program dir, producer bootstrap or None)
        self.make_agent("com.optionsedge.es-cvd-mirror-127-0-0-1-19092-es-futures-cvd", "es-cvd-mirror-127-0-0-1-19092-es-futures-cvd", "127.0.0.1:19092")
        self.make_agent("com.optionsedge.esgex-mirror", "es-gex-mirror", "localhost:19092")
        self.make_agent("com.optionsedge.es-cvd-mirror-192-168-100-252-9092-es-futures-cvd", "es-cvd-mirror-192-168-100-252-9092-es-futures-cvd", "192.168.100.252:9092")
        self.make_agent("com.optionsedge.vixbridge", "vixbridge", None)
        (self.state / "loaded").write_text("\n".join([
            "com.optionsedge.es-cvd-mirror-127-0-0-1-19092-es-futures-cvd",
            "com.optionsedge.esgex-mirror",
            "com.optionsedge.es-cvd-mirror-192-168-100-252-9092-es-futures-cvd",
            "com.optionsedge.vixbridge",
        ]) + "\n")

    def tearDown(self):
        self.tmp.cleanup()

    def make_agent(self, label, dirname, bootstrap):
        d = self.ops / dirname
        d.mkdir()
        (d / "run-mirror.sh").write_text("#!/usr/bin/env bash\n")
        if bootstrap:
            (d / "producer.properties").write_text(f"bootstrap.servers={bootstrap}\nacks=1\n")
            (d / "consumer.properties").write_text("bootstrap.servers=192.168.100.4:9092\n")
        (self.agents / f"{label}.plist").write_text(plist(str(d / "run-mirror.sh")))

    def run_fn(self, fn):
        script = helper_block() + f"\n{fn}\n"
        env = dict(os.environ, LAUNCH_AGENTS_DIR=str(self.agents), LAUNCHCTL=str(self.launchctl),
                   DEV_MIRRORS_PAUSED=str(self.paused), FAKE_STATE=str(self.state))
        return subprocess.run(["bash", "-c", script], env=env, text=True, capture_output=True, check=True).stdout

    def loaded(self):
        return set((self.state / "loaded").read_text().split())

    def test_pause_unloads_only_agents_that_write_to_dev(self):
        out = self.run_fn("pause_dev_mirrors")
        self.assertIn("paused 2 es4->dev mirror agent(s)", out)
        self.assertEqual(self.loaded(), {
            "com.optionsedge.es-cvd-mirror-192-168-100-252-9092-es-futures-cvd",
            "com.optionsedge.vixbridge",
        })
        labels = sorted(line.split()[0] for line in self.paused.read_text().splitlines())
        self.assertEqual(labels, ["com.optionsedge.es-cvd-mirror-127-0-0-1-19092-es-futures-cvd", "com.optionsedge.esgex-mirror"])

    def test_resume_reloads_the_paused_agents_and_clears_the_list(self):
        self.run_fn("pause_dev_mirrors")
        out = self.run_fn("resume_dev_mirrors")
        self.assertIn("resumed 2 es4->dev mirror agent(s)", out)
        self.assertEqual(len(self.loaded()), 4)
        self.assertFalse(self.paused.exists())

    def test_failed_reload_keeps_the_list_for_the_next_start(self):
        self.run_fn("pause_dev_mirrors")
        (self.state / "refuse").write_text("com.optionsedge.esgex-mirror\n")
        out = self.run_fn("resume_dev_mirrors")
        self.assertIn("WARN: mirror agent com.optionsedge.esgex-mirror did not load", out)
        self.assertTrue(self.paused.exists())
        (self.state / "refuse").unlink()
        self.run_fn("resume_dev_mirrors")
        self.assertEqual(len(self.loaded()), 4)
        self.assertFalse(self.paused.exists())

    def test_second_clean_after_an_interrupted_one_still_resumes_everything(self):
        self.run_fn("pause_dev_mirrors")           # clean #1 dies after pausing: both dev agents unloaded
        out = self.run_fn("pause_dev_mirrors")     # clean #2 finds them already unloaded
        self.assertIn("paused 2 es4->dev mirror agent(s)", out)
        self.run_fn("resume_dev_mirrors")
        self.assertEqual(len(self.loaded()), 4)
        self.assertFalse(self.paused.exists())

    def test_agent_that_will_not_unload_is_reported(self):
        stuck = "com.optionsedge.esgex-mirror"
        fake = self.launchctl.read_text().replace('bootout)\n', f'bootout)\n    [ "${{2##*/}}" = "{stuck}" ] && exit 0\n', 1)
        self.launchctl.write_text(fake)
        env_sleep = helper_block().replace("sleep 3", ":")
        script = env_sleep + "\npause_dev_mirrors\n"
        env = dict(os.environ, LAUNCH_AGENTS_DIR=str(self.agents), LAUNCHCTL=str(self.launchctl),
                   DEV_MIRRORS_PAUSED=str(self.paused), FAKE_STATE=str(self.state))
        out = subprocess.run(["bash", "-c", script], env=env, text=True, capture_output=True, check=True).stdout
        self.assertIn(f"WARN: mirror agent {stuck} is still loaded", out)
        self.assertIn("paused 1 es4->dev mirror agent(s)", out)

    def test_resume_without_a_list_is_a_no_op(self):
        self.assertEqual(self.run_fn("resume_dev_mirrors"), "")
        self.assertFalse((self.state / "calls").exists())

    def test_wipe_pauses_before_deleting_and_resumes_after_recreating(self):
        text = SCRIPT.read_text()
        clean = text[text.index("do_clean() {"):text.index("\n}\n", text.index("do_clean() {"))]
        pause = clean.index("pause_dev_mirrors")
        delete = clean.index("--delete --topic")
        ensure = clean.index("ensure_topics", delete)
        resume = clean.index("resume_dev_mirrors")
        self.assertLess(pause, delete)
        self.assertLess(ensure, resume)
        for fn in ("do_start() {", "do_start_overnight() {"):
            body = text[text.index(fn):text.index("\n}\n", text.index(fn))]
            self.assertRegex(body, re.compile(r"ensure_topics\s+resume_dev_mirrors"))


if __name__ == "__main__":
    unittest.main()
