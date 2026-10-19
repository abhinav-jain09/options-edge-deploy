"""dev-cleanup.sh must unload the es4->dev mirror agents around the topic wipe.

2026-09-14: a running kafka-mirror-maker re-created es.options.databento.gex.strike 0.3 s after the
wipe deleted it, at dev's num.partitions=1, so ensure_topics' declared shape never applied. These
tests run the real pause/resume functions against a fake launchctl and fake LaunchAgents plists,
including the one-line plist layout most real mirror agents use.
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
BASH = "/bin/bash" if Path("/bin/bash").exists() else "bash"   # macOS: the 3.2 launchd runs it with

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
        grep -qx "$label" "$FAKE_STATE/stuck" 2>/dev/null && exit 0
        grep -vx "$label" "$loaded" > "$loaded.tmp"; mv "$loaded.tmp" "$loaded" ;;
      bootstrap)
        label="$(basename "$3" .plist)"
        grep -qx "$label" "$FAKE_STATE/refuse" 2>/dev/null && exit 5
        grep -qx "$label" "$loaded" && exit 37
        echo "$label" >> "$loaded" ;;
      list)
        # mirror_agent_pid() runs `launchctl list` with NO label and reads the PID COLUMN, so this
        # fake has to emit the real three-column table (PID Status Label). The previous version only
        # set an exit code for `list <label>` -- the OLD contract, from before the script was
        # changed to require a running pid -- so every lookup came back empty, every resume waited
        # out DEV_MIRROR_WAIT_S and reported FAIL, and five tests errored. The pid states that
        # matter are modelled rather than assumed:
        #   $FAKE_STATE/nopid    loaded but NOT running -> '-' in the pid column, the state
        #                        measured on this Mac (agents registered with no pid at all)
        #   $FAKE_STATE/respawn  a NEW pid on every call -> a KeepAlive restart loop, which is
        #                        exactly what the settle check exists to catch
        if [ "$#" -ge 2 ]; then
          grep -qx "$2" "$loaded"
        else
          n=0
          while read -r label; do
            [ -n "$label" ] || continue
            n=$((n + 1))
            if grep -qx "$label" "$FAKE_STATE/nopid" 2>/dev/null; then
              printf -- '-\t0\t%s\n' "$label"
            elif grep -qx "$label" "$FAKE_STATE/respawn" 2>/dev/null; then
              c=$(cat "$FAKE_STATE/respawn.count" 2>/dev/null || echo 0)
              c=$((c + 1)); echo "$c" > "$FAKE_STATE/respawn.count"
              printf '%s\t0\t%s\n' "$((9000 + c))" "$label"
            else
              printf '%s\t0\t%s\n' "$((1000 + n))" "$label"
            fi
          done < "$loaded"
        fi ;;
    esac
    """
)

DEV_CVD = "com.optionsedge.es-cvd-mirror-127-0-0-1-19092-es-futures-cvd"
DEV_GEX = "com.optionsedge.esgex-mirror"
DEV_BASH = "com.optionsedge.es-bash-launched-mirror-127-0-0-1-19092"
PROD_CVD = "com.optionsedge.es-cvd-mirror-192-168-100-252-9092-es-futures-cvd"
BRIDGE = "com.optionsedge.es-trades-bridge-192-168-100-252-9092"


def one_line_plist(label, args, logs):
    strings = "".join(f"<string>{a}</string>" for a in args)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n<plist version="1.0"><dict>\n'
        f"  <key>Label</key><string>{label}</string>\n"
        f"  <key>ProgramArguments</key><array>{strings}</array>\n"
        "  <key>RunAtLoad</key><true/><key>KeepAlive</key><true/>\n"
        f"  <key>StandardOutPath</key><string>{logs}/out.log</string>\n"
        "</dict></plist>\n"
    )


def multi_line_plist(label, args):
    strings = "\n".join(f"        <string>{a}</string>" for a in args)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n<plist version="1.0">\n<dict>\n'
        f"    <key>Label</key>\n    <string>{label}</string>\n"
        f"    <key>ProgramArguments</key>\n    <array>\n{strings}\n    </array>\n"
        "</dict>\n</plist>\n"
    )


def block(start_marker, fn_header):
    text = SCRIPT.read_text()
    start = text.index(start_marker)
    end = text.index("\n}\n", text.index(fn_header)) + 3
    return text[start:end]


def helper_block():
    return block("LAUNCH_AGENTS_DIR=", "resume_dev_mirrors() {")


class DevCleanupPausesDevMirrorsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.agents, self.ops, self.state, self.logs = (base / d for d in ("LaunchAgents", "oe-ops", "state", "logs"))
        for d in (self.agents, self.ops, self.state, self.logs):
            d.mkdir()
        self.launchctl = base / "launchctl"
        self.launchctl.write_text(FAKE_LAUNCHCTL)
        self.launchctl.chmod(self.launchctl.stat().st_mode | stat.S_IEXEC)
        self.paused = self.ops / ".dev-mirrors-paused"
        # One-line plist, logs elsewhere (the layout of the real es-auction/es-cvd agents).
        self.agent(DEV_CVD, "es-cvd-mirror-127-0-0-1-19092-es-futures-cvd", "127.0.0.1:19092", one_line=True)
        # Multi-line plist, label differs from its directory name, localhost spelling.
        self.agent(DEV_GEX, "es-gex-mirror", "localhost:19092")
        # `/bin/bash run-mirror.sh`: the script is ProgramArguments[1].
        self.agent(DEV_BASH, "es-bash-launched", "127.0.0.1:19092", one_line=True, via_bash=True)
        self.agent(PROD_CVD, "es-cvd-mirror-192-168-100-252-9092-es-futures-cvd", "192.168.100.252:9092", one_line=True)
        self.agent(BRIDGE, "es-trades-bridge-192-168-100-252-9092", None)
        self.set_loaded({DEV_CVD, DEV_GEX, DEV_BASH, PROD_CVD, BRIDGE})

    def tearDown(self):
        self.tmp.cleanup()

    def agent(self, label, dirname, bootstrap, one_line=False, via_bash=False):
        d = self.ops / dirname
        d.mkdir()
        script = d / "run-mirror.sh"
        script.write_text("#!/usr/bin/env bash\n")
        if bootstrap:
            (d / "producer.properties").write_text(f"bootstrap.servers={bootstrap}\nacks=1\n")
            (d / "consumer.properties").write_text("bootstrap.servers=192.168.100.4:9092\n")
        args = ["/bin/bash", str(script)] if via_bash else [str(script)]
        body = one_line_plist(label, args, self.logs) if one_line else multi_line_plist(label, args)
        (self.agents / f"{label}.plist").write_text(body)

    def set_loaded(self, labels):
        (self.state / "loaded").write_text("".join(f"{l}\n" for l in sorted(labels)))

    def loaded(self):
        return set((self.state / "loaded").read_text().split())

    def paused_labels(self):
        return sorted(line.split()[0] for line in self.paused.read_text().splitlines())

    def run_script(self, script, cwd=None, path_prefix=None, expect_rc=0):
        # The real defaults are a 60s wait and a 5s settle PER AGENT, which the production
        # comment justifies for 14 JVMs on a loaded Mac. Both are overridable precisely so this
        # gate does not spend minutes per case.
        env = dict(os.environ, LAUNCH_AGENTS_DIR=str(self.agents), LAUNCHCTL=str(self.launchctl),
                   DEV_MIRRORS_PAUSED=str(self.paused), FAKE_STATE=str(self.state),
                   DEV_MIRROR_WAIT_S="4", DEV_MIRROR_SETTLE_S="0")
        if path_prefix:
            env["PATH"] = f"{path_prefix}:{env['PATH']}"
        r = subprocess.run([BASH, "-c", script], env=env, text=True, capture_output=True, cwd=cwd)
        # The exit code is part of the contract, not noise: resume_dev_mirrors RETURNS its verdict
        # ("this function used to end on an echo, so it ALWAYS returned 0 and a total failure to
        # resume was indistinguishable from success"). check=True turned that verdict into a
        # harness crash, so the expected code is asserted instead.
        self.assertEqual(r.returncode, expect_rc,
                         f"exit {r.returncode}, expected {expect_rc}\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}")
        return r.stdout

    def run_fn(self, fn, **kw):
        return self.run_script(helper_block().replace("sleep 3", ":") + f"\n{fn}\n", **kw)

    def test_relative_program_arguments_never_resolve_against_the_callers_cwd(self):
        # An unrelated agent (`/bin/bash -lc auto`) run by an operator standing inside a dev mirror dir.
        other = "com.optionsedge.prod-cleanup"
        (self.agents / f"{other}.plist").write_text(one_line_plist(other, ["/bin/bash", "-lc", "auto"], self.logs))
        self.set_loaded({DEV_CVD, DEV_GEX, DEV_BASH, PROD_CVD, BRIDGE, other})
        cwd = self.ops / "es-gex-mirror"                      # holds a dev producer.properties
        out = self.run_fn("dev_mirror_agents", cwd=cwd)
        self.assertEqual(sorted(line.split()[0] for line in out.splitlines()), sorted([DEV_CVD, DEV_GEX, DEV_BASH]))
        self.run_fn("pause_dev_mirrors", cwd=cwd)
        self.assertIn(other, self.loaded())

    def test_discovery_failure_is_loud_and_pauses_nothing(self):
        fake_bin = Path(self.tmp.name) / "bin"
        fake_bin.mkdir()
        py = fake_bin / "python3"
        py.write_text("#!/bin/sh\nexit 1\n")
        py.chmod(0o755)
        out = self.run_fn("pause_dev_mirrors", path_prefix=str(fake_bin))
        self.assertIn("ERROR: es4->dev mirror discovery failed", out)
        self.assertEqual(self.loaded(), {DEV_CVD, DEV_GEX, DEV_BASH, PROD_CVD, BRIDGE})
        self.assertFalse(self.paused.exists())

    def test_discovery_reads_the_program_path_from_every_plist_layout(self):
        out = self.run_fn("dev_mirror_agents")
        self.assertEqual(sorted(line.split()[0] for line in out.splitlines()), sorted([DEV_CVD, DEV_GEX, DEV_BASH]))

    def test_pause_unloads_only_agents_that_write_to_dev(self):
        out = self.run_fn("pause_dev_mirrors")
        self.assertIn("paused 3 es4->dev mirror agent(s)", out)
        self.assertNotIn("ERROR", out)
        self.assertEqual(self.loaded(), {PROD_CVD, BRIDGE})
        self.assertEqual(self.paused_labels(), sorted([DEV_CVD, DEV_GEX, DEV_BASH]))

    def test_resume_reloads_the_paused_agents_and_clears_the_list(self):
        self.run_fn("pause_dev_mirrors")
        out = self.run_fn("resume_dev_mirrors")
        self.assertIn("resumed 3 es4->dev mirror agent(s)", out)
        self.assertEqual(self.loaded(), {DEV_CVD, DEV_GEX, DEV_BASH, PROD_CVD, BRIDGE})
        self.assertFalse(self.paused.exists())

    def test_agent_unloaded_on_purpose_is_not_loaded_by_the_wipe(self):
        self.set_loaded({DEV_CVD, DEV_BASH, PROD_CVD, BRIDGE})       # esgex-mirror deliberately off
        self.run_fn("pause_dev_mirrors")
        self.assertEqual(self.paused_labels(), sorted([DEV_CVD, DEV_BASH]))
        self.run_fn("resume_dev_mirrors")
        self.assertNotIn(DEV_GEX, self.loaded())

    def test_second_clean_after_an_interrupted_one_still_resumes_everything(self):
        self.run_fn("pause_dev_mirrors")           # clean #1 dies after pausing: dev agents unloaded
        out = self.run_fn("pause_dev_mirrors")     # clean #2 finds nothing loaded to pause
        self.assertIn("paused 0 es4->dev mirror agent(s)", out)
        self.assertEqual(self.paused_labels(), sorted([DEV_CVD, DEV_GEX, DEV_BASH]))
        self.run_fn("resume_dev_mirrors")
        self.assertEqual(self.loaded(), {DEV_CVD, DEV_GEX, DEV_BASH, PROD_CVD, BRIDGE})
        self.assertFalse(self.paused.exists())

    def test_failed_reload_keeps_the_list_for_the_next_start(self):
        self.run_fn("pause_dev_mirrors")
        (self.state / "refuse").write_text(f"{DEV_GEX}\n")
        # An agent that will not bootstrap never reaches a pid, so the verdict is NOT RUNNING and
        # resume returns non-zero. The old expectation ("WARN: ... did not load") predates the
        # change to pid-based liveness, and nothing caught that because this whole step has not
        # executed in CI since the collectability gate started failing ahead of it.
        out = self.run_fn("resume_dev_mirrors", expect_rc=1)
        self.assertIn(f"FAIL: mirror agent {DEV_GEX} is NOT RUNNING", out)
        self.assertIn("1 NOT RUNNING", out)
        self.assertTrue(self.paused.exists(), "the list must survive for the next start")
        (self.state / "refuse").unlink()
        self.run_fn("resume_dev_mirrors")
        self.assertIn(DEV_GEX, self.loaded())
        self.assertFalse(self.paused.exists())

    def test_an_agent_registered_with_no_pid_is_not_counted_as_resumed(self):
        # The outage this check exists for: `launchctl list <label>` exits 0 for a job that is
        # merely REGISTERED, so a registration-based check deleted the pause list while all 14
        # mirrors were down. A '-' in the pid column is the only thing that distinguishes them.
        self.run_fn("pause_dev_mirrors")
        (self.state / "nopid").write_text(f"{DEV_GEX}\n")
        out = self.run_fn("resume_dev_mirrors", expect_rc=1)
        self.assertIn(f"FAIL: mirror agent {DEV_GEX} is NOT RUNNING", out)
        self.assertIn("resumed 2 es4->dev mirror agent(s), 1 NOT RUNNING", out)
        self.assertTrue(self.paused.exists())
        self.assertIn(DEV_GEX, self.loaded(), "it IS loaded — that is exactly the trap")

    def test_a_respawn_loop_does_not_pass_as_running(self):
        # A JVM that gets a pid and dies in a KeepAlive loop passes a point-in-time check on its
        # brief pid window. Requiring the SAME pid after a settle is what rejects it, because each
        # restart issues a new one.
        self.run_fn("pause_dev_mirrors")
        (self.state / "respawn").write_text(f"{DEV_CVD}\n")
        out = self.run_fn("resume_dev_mirrors", expect_rc=1)
        self.assertIn(f"FAIL: mirror agent {DEV_CVD} is NOT RUNNING stably", out)
        self.assertIn("restart loop", out)
        self.assertTrue(self.paused.exists())

    def test_agent_that_will_not_unload_is_reported(self):
        (self.state / "stuck").write_text(f"{DEV_GEX}\n")
        out = self.run_fn("pause_dev_mirrors")
        self.assertIn(f"WARN: mirror agent {DEV_GEX} is still loaded", out)
        self.assertIn("paused 2 es4->dev mirror agent(s)", out)

    def test_resume_without_a_list_is_a_no_op(self):
        self.assertEqual(self.run_fn("resume_dev_mirrors"), "")
        self.assertFalse((self.state / "calls").exists())

    def test_mirrors_stay_paused_when_topics_env_was_not_applied(self):
        gate = block("resume_dev_mirrors_if_declared() {", "resume_dev_mirrors_if_declared() {")
        self.run_fn("pause_dev_mirrors")
        out = self.run_script(helper_block() + "\n" + gate + "\nresume_dev_mirrors_if_declared 1\n")
        self.assertIn("mirrors stay PAUSED", out)
        self.assertNotIn(DEV_CVD, self.loaded())
        self.assertTrue(self.paused.exists())
        self.run_script(helper_block() + "\n" + gate + "\nresume_dev_mirrors_if_declared 0\n")
        self.assertIn(DEV_CVD, self.loaded())
        self.assertFalse(self.paused.exists())

    def test_ensure_topics_reports_an_unreadable_topics_env(self):
        text = SCRIPT.read_text()
        body = text[text.index("ensure_topics() {"):text.index("\n}\n", text.index("ensure_topics() {"))]
        warning = body.index("could not read deploy topics.env")
        self.assertRegex(body[warning:], r"return 1")

    def test_wipe_pauses_before_deleting_and_resumes_after_recreating(self):
        text = SCRIPT.read_text()
        clean = text[text.index("do_clean() {"):text.index("\n}\n", text.index("do_clean() {"))]
        pause = clean.index("pause_dev_mirrors")
        delete = clean.index("--delete --topic")
        resume = clean.index("ensure_topics; resume_dev_mirrors_if_declared $?")
        self.assertLess(pause, delete)
        self.assertLess(delete, resume)
        for fn in ("do_start() {", "do_start_overnight() {"):
            body = text[text.index(fn):text.index("\n}\n", text.index(fn))]
            self.assertRegex(body, re.compile(r"^\s+ensure_topics; resume_dev_mirrors_if_declared \$\?$", re.M))


if __name__ == "__main__":
    unittest.main()
