"""dev-cleanup.sh must unload the es4->dev mirror agents around the topic wipe.

2026-09-14: a running kafka-mirror-maker re-created es.options.databento.gex.strike 0.3 s after the
wipe deleted it, at dev's num.partitions=1, so ensure_topics' declared shape never applied. These
tests run the real pause/resume functions against a fake launchctl and fake LaunchAgents plists,
including the one-line plist layout most real mirror agents use.

The fake models LIVENESS, not merely registration, because that distinction is the point of the
resume half: `launchctl list <label>` exits 0 for a job that is loaded with nothing running, so only
the PID column of a bare `launchctl list` tells a running mirror from a dead one. See FAKE_LAUNCHCTL
for the three states a test can put a label in, and scripts/ci/dev-mirror-resume-test.sh for the same
invariant checked against the shipped function by a shell gate.
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

# A VIRTUAL sleep, put on PATH ahead of the real one. It advances $FAKE_STATE/clock by the seconds it
# was asked to spend, and FAKE_LAUNCHCTL reads that clock instead of the wall one. Two consequences,
# and the second is the reason it exists:
#   * the suite spends no real time waiting, so the CI gate costs seconds;
#   * the ONLY thing that can move a respawning agent's pid, or age a slow one into running, is the
#     helper actually calling sleep. Against wall time those tests were probabilistic -- delete the
#     helper's `sleep "$DEV_MIRROR_SETTLE_S"` and two back-to-back observations USUALLY land in the
#     same second, but a preempted or slow host could still straddle a boundary and let the broken
#     helper look correct. On a virtual clock, deleted sleep == frozen clock == red, every time.
FAKE_SLEEP = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    set -u
    n=0; { read -r n < "$FAKE_STATE/clock"; } 2>/dev/null
    echo "$(( n + ${1%%.*} ))" > "$FAKE_STATE/clock"
    """
)

FAKE_LAUNCHCTL = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    # Loaded labels live in $FAKE_STATE/loaded, one per line; every call is appended to $FAKE_STATE/calls.
    #
    # TWO FORMS OF `list`, BECAUSE launchd ANSWERS TWO DIFFERENT QUESTIONS AND THE GATE NEEDS BOTH:
    #   `list <label>`  rc only -- is the job REGISTERED? It exits 0 even when nothing is running, which
    #                   is how dev spent a night with all 14 mirrors down while the clean reported OK.
    #   `list`          the PID/Status/Label table. The PID column is the only thing that separates a
    #                   running job from a registered one, so mirror_agent_pid parses this form.
    #
    # LIVENESS IS THEREFORE MODELLED, NOT ASSUMED. A loaded label is in one of four states, and a test
    # chooses it by listing the label in a state file (see DevCleanupPausesDevMirrorsTest.mark):
    #   $FAKE_STATE/dead     '-' in the PID column: loaded, nothing running (the 2026-10-12 outage).
    #   $FAKE_STATE/respawn  a KeepAlive restart loop: a pid at every instant, never the same for long.
    #   $FAKE_STATE/slow     '-' until a second has passed, then a stable pid: a JVM that is starting.
    #   (none of them)       running: a pid that is STABLE across calls. This is the default, so a label
    #                        a test bootstraps or pre-loads is a healthy mirror unless it is marked.
    #
    # respawn AND slow READ THE VIRTUAL CLOCK (see FAKE_SLEEP), NOT THE WALL CLOCK AND NOT A CALL
    # COUNTER. Both of those leave the same hole: a pid that changes whenever it is asked keeps the
    # tests green even if someone deletes the helper's `sleep "$DEV_MIRROR_SETTLE_S"`, while a REAL job
    # that lives one second would then be observed twice inside that second, report one pid, and be
    # accepted. Here the clock only moves when the helper sleeps, so its sleeps are what the tests rest
    # on, deterministically rather than usually.
    #
    # Labels are compared whole and literally -- never as regexes, and delimited by the one character a
    # line-based list cannot contain, a newline. One thing the production awk genuinely cannot do, so
    # neither does this: a label containing WHITESPACE never matches at all ($3 is its first word), and
    # the agent is reported not running. Fail-closed, but not an exact comparison.
    set -u
    echo "$*" >> "$FAKE_STATE/calls"
    loaded="$FAKE_STATE/loaded"; touch "$loaded"
    printf -v NL '\\n'

    # This fake is forked once per launchctl call and the helper makes several per agent, so the state
    # lists are read ONCE per call into "\\na\\nb\\n" strings matched with `case` -- no grep per label, and
    # no subshell per label either (agent_pid assigns PIDCOL rather than printing it).
    # ASSIGNED with printf -v, never echoed through $( ), which strips the trailing newline we match on.
    slurp() {   # slurp <state file> <variable to set>
      local body=""
      [ -f "$FAKE_STATE/$1" ] && body=$(cat "$FAKE_STATE/$1")
      printf -v "$2" '%s' "$NL$body$NL"
    }

    mint_pid() {
      local n=4242
      { read -r n < "$FAKE_STATE/next_pid"; } 2>/dev/null
      echo "$(( n + 1 ))" > "$FAKE_STATE/next_pid"
      echo "$(( n + 1 ))"
    }

    # The PID column for one loaded label -> PIDCOL. Needs $now and the lists slurped by the caller.
    agent_pid() {
      local label="$1" start
      case "$dead"    in *"$NL$label$NL"*) PIDCOL='-'; return ;; esac
      case "$respawn" in *"$NL$label$NL"*) PIDCOL=$(( 9000 + now )); return ;; esac
      case "$slow" in *"$NL$label$NL"*)
        [ -f "$FAKE_STATE/seen.$label" ] || echo "$now" > "$FAKE_STATE/seen.$label"
        read -r start < "$FAKE_STATE/seen.$label"
        [ "$(( now - start ))" -ge 1 ] || { PIDCOL='-'; return; } ;;
      esac
      [ -f "$FAKE_STATE/pid.$label" ] || mint_pid > "$FAKE_STATE/pid.$label"
      read -r PIDCOL < "$FAKE_STATE/pid.$label"
    }

    case "${1:-}" in
      bootout)
        label="${2##*/}"
        grep -Fqx -- "$label" "$FAKE_STATE/stuck" 2>/dev/null && exit 0
        grep -Fvx -- "$label" "$loaded" > "$loaded.tmp"; mv "$loaded.tmp" "$loaded"
        rm -f "$FAKE_STATE/pid.$label" ;;          # the process is gone; a reload gets a new pid
      bootstrap)
        label="$(basename "$3" .plist)"
        grep -Fqx -- "$label" "$FAKE_STATE/refuse" 2>/dev/null && exit 5
        grep -Fqx -- "$label" "$loaded" && exit 37
        echo "$label" >> "$loaded" ;;
      list)
        if [ "$#" -ge 2 ]; then
          grep -Fqx -- "$2" "$loaded"              # REGISTERED? rc only, exactly as launchd answers it
        else
          now=0; { read -r now < "$FAKE_STATE/clock"; } 2>/dev/null
          slurp dead dead; slurp respawn respawn; slurp slow slow
          printf 'PID\\tStatus\\tLabel\\n'
          while read -r label; do
            [ -n "$label" ] || continue
            agent_pid "$label"
            printf '%s\\t0\\t%s\\n' "$PIDCOL" "$label"
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
        self.rc = None                     # exit status of the last run_script, for the failure cases
        self.launchctl = base / "launchctl"
        self.launchctl.write_text(FAKE_LAUNCHCTL)
        self.launchctl.chmod(self.launchctl.stat().st_mode | stat.S_IEXEC)
        self.shims = base / "shims"                # goes on PATH so the helper's `sleep` is virtual
        self.shims.mkdir()
        (self.shims / "sleep").write_text(FAKE_SLEEP)
        (self.shims / "sleep").chmod(0o755)
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

    def mark(self, kind, *labels):
        """Put these labels in a liveness state other than the default (running with a stable pid).

        "dead": loaded with '-' in the PID column -- registered, nothing running. "respawn": a new pid
        on every call -- a KeepAlive restart loop. Both are states `launchctl list <label>` reports as
        a plain success, which is exactly why resume_dev_mirrors may not ask it.
        """
        (self.state / kind).write_text("".join(f"{l}\n" for l in labels))

    def loaded(self):
        return set((self.state / "loaded").read_text().split())

    def paused_labels(self):
        return sorted(line.split()[0] for line in self.paused.read_text().splitlines())

    def run_script(self, script, cwd=None, path_prefix=None, check=True):
        # WAIT_S=4 is TWO passes of the helper's `sleep 2` loop, so the retry is exercised rather than
        # dead code; SETTLE_S=1 so the settle actually elapses. The helper's own comment says both are
        # "overridable so the CI gate does not spend a minute per case" -- which this suite never did,
        # and 60s per agent is where its 842s came from. These are seconds on the VIRTUAL clock (see
        # FAKE_SLEEP), so they cost nothing, and are small only to keep the loops short.
        env = dict(os.environ, LAUNCH_AGENTS_DIR=str(self.agents), LAUNCHCTL=str(self.launchctl),
                   DEV_MIRRORS_PAUSED=str(self.paused), FAKE_STATE=str(self.state),
                   DEV_MIRROR_WAIT_S="4", DEV_MIRROR_SETTLE_S="1")
        env["PATH"] = f"{self.shims}:{env['PATH']}"
        if path_prefix:
            env["PATH"] = f"{path_prefix}:{env['PATH']}"
        proc = subprocess.run([BASH, "-c", script], env=env, text=True, capture_output=True, cwd=cwd)
        self.rc = proc.returncode
        if check:
            self.assertEqual(0, proc.returncode, f"rc={proc.returncode}\n{proc.stdout}{proc.stderr}")
        return proc.stdout

    def run_fn(self, fn, **kw):
        # The helper runs verbatim: its sleeps are real calls, answered by the virtual `sleep` on PATH.
        return self.run_script(helper_block() + f"\n{fn}\n", **kw)

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
        out = self.run_fn("resume_dev_mirrors", check=False)
        # bootstrap's own rc is deliberately ignored by the helper; the verdict is the pid, and an
        # agent that never loaded has none. The function must say so AND return it.
        self.assertEqual(1, self.rc)
        self.assertIn(f"FAIL: mirror agent {DEV_GEX} is NOT RUNNING", out)
        self.assertIn("1 NOT RUNNING", out)
        self.assertTrue(self.paused.exists())
        (self.state / "refuse").unlink()
        self.run_fn("resume_dev_mirrors")
        self.assertIn(DEV_GEX, self.loaded())
        self.assertFalse(self.paused.exists())

    def test_an_agent_that_is_registered_without_a_pid_is_a_failed_resume(self):
        # The 2026-10-12 outage, reproduced: bootstrap succeeds, `launchctl list <label>` exits 0, and
        # the PID column is '-'. A resume that judged by registration called this success, deleted the
        # pause list and let the clean exit 0 while dev read frozen topics all night. This test is also
        # what keeps the FAKE honest -- it is the case the fake's no-arg `list` must be able to express,
        # and it must come out FAILED, not merely unasserted.
        self.run_fn("pause_dev_mirrors")
        self.mark("dead", DEV_GEX)
        out = self.run_fn("resume_dev_mirrors", check=False)
        self.assertEqual(1, self.rc)
        self.assertIn(f"FAIL: mirror agent {DEV_GEX} is NOT RUNNING", out)
        self.assertIn("resumed 2 es4->dev mirror agent(s), 1 NOT RUNNING", out)
        self.assertTrue(self.paused.exists())                       # retained, so the next start retries
        self.assertEqual(self.paused_labels(), sorted([DEV_CVD, DEV_GEX, DEV_BASH]))

    def test_an_agent_in_a_restart_loop_does_not_count_as_resumed(self):
        # A JVM that starts, gets a pid and dies under KeepAlive has a pid at every instant -- just
        # never the same one. Only re-reading the pid after a settle separates it from a healthy mirror.
        self.run_fn("pause_dev_mirrors")
        self.mark("respawn", DEV_CVD)
        # The fake's respawning pid moves only when the virtual clock does, and between these two
        # observations the only thing that moves it is the helper's `sleep "$DEV_MIRROR_SETTLE_S"`.
        # Delete that sleep from the helper and this test goes red -- every time, not most times.
        out = self.run_fn("resume_dev_mirrors", check=False)
        self.assertEqual(1, self.rc)
        self.assertIn(f"FAIL: mirror agent {DEV_CVD} is NOT RUNNING stably", out)
        self.assertIn("(restart loop)", out)
        self.assertIn("resumed 2 es4->dev mirror agent(s), 1 NOT RUNNING", out)
        self.assertTrue(self.paused.exists())

    def test_a_mirror_that_is_only_slow_to_start_is_not_called_failed(self):
        # The inverse outage, and the reason the budget is 60s and not 10s: 14 JVMs starting together on
        # a Mac at load ~24 take longer than one observation, and calling a healthy mirror failed is its
        # own incident. This agent shows '-' on the first look and a stable pid on the retry.
        # The slow agent is the only paused mirror here, so the two observations the helper makes of
        # it are its own -- with other agents ahead of it in the list, their probes would have aged
        # this one into "running" before its turn and the retry would never be exercised.
        self.set_loaded({DEV_GEX, PROD_CVD, BRIDGE})
        self.run_fn("pause_dev_mirrors")
        self.assertEqual(self.paused_labels(), [DEV_GEX])
        self.mark("slow", DEV_GEX)
        out = self.run_fn("resume_dev_mirrors")
        self.assertEqual(0, self.rc)
        self.assertIn("resumed 1 es4->dev mirror agent(s)", out)
        self.assertNotIn("NOT RUNNING", out)
        self.assertFalse(self.paused.exists())

    def test_a_resume_that_did_not_come_back_running_reaches_the_exit_status(self):
        # The wrapper used to end its failure branch on an echo, so it returned 0 and cron, Jenkins and
        # every other caller still saw success while the mirrors were down.
        gate = block("resume_dev_mirrors_if_declared() {", "resume_dev_mirrors_if_declared() {")
        self.run_fn("pause_dev_mirrors")
        self.mark("dead", DEV_CVD)
        out = self.run_script(helper_block() + "\n" + gate + "\nresume_dev_mirrors_if_declared 0\n",
                              check=False)
        self.assertEqual(1, self.rc)
        self.assertIn("ERROR: es4->dev mirrors did NOT all come back RUNNING", out)
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
