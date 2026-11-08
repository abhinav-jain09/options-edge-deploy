# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
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
    # An UNOBSERVABLE launchctl: every call fails, the way a missing binary or an unreachable
    # launchd does. Not the same as "nothing is loaded", which is what the script used to read it as.
    [ -e "$FAKE_STATE/broken" ] && exit 1
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

    def run_script(self, script, cwd=None, path_prefix=None, expect_rc=0, both=False):
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
        # `both` is for messages the script must send to STDERR: inside pause_dev_mirrors' discovery
        # loop stdout is redirected into the paused-list file, so a diagnostic printed there would
        # land in the list instead of the log.
        return (r.stdout + r.stderr) if both else r.stdout

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
        # expect_rc=1: discovery failed, so NOTHING is known to be paused. Saying so only in the log
        # let do_clean delete behind a live mirror; the status is what stops it now.
        out = self.run_fn("pause_dev_mirrors", path_prefix=str(fake_bin), expect_rc=1)
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
        # expect_rc=1: the other two were paused, but ONE mirror is still running, and that is enough
        # to re-create its target at 1 partition the moment the wipe deletes it. Reporting the two it
        # managed is not the same as proving the mirrors are down.
        out = self.run_fn("pause_dev_mirrors", expect_rc=1)
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

    # ---------------------------------------------------------------------------------------------
    # The ORDER is proven by RUNNING do_clean, not by scanning it for text.
    #
    # Two text scans stood here and both lied. The first asserted the literal
    # `ensure_topics; resume_dev_mirrors_if_declared $?`, which stopped existing when that pair
    # became `ensure_topics_or_refuse` (the old form discarded ensure_topics' status), so the test
    # failed on the text while the behaviour got safer — and under `bash -e` in deploy-validation it
    # took every later step of that job with it. The second indexed `pause_dev_mirrors`, which
    # matched the COMMENT above the call, so "pause before delete" was satisfied by prose.
    #
    # The replacement that only stripped whole-line comments was no better, and Codex named the
    # mutation that proves it: a here-doc holding the three anchor lines
    #
    #     : <<'ORDER_TEXT'
    #       pause_dev_mirrors
    #       --delete --topic
    #       ensure_topics_or_refuse
    #     ORDER_TEXT
    #
    # keeps every text assertion green with the real path deleted. No `#`-stripper fixes that;
    # heredocs, quotes and line continuations would need a shell parser. So count EFFECTS instead:
    # run the real do_clean with the real ensure_topics_or_refuse, stub its neighbours as recorders,
    # and assert the sequence they observed. That is rename-proof, because a do_clean that stops
    # pausing records no pause whatever it calls its helpers.
    # ---------------------------------------------------------------------------------------------

    @staticmethod
    def _extract(name):
        """One function, lifted out by name — the pattern scripts/ci/dev-cleanup-topic-recreation-fatal-test.sh uses.

        dev-cleanup.sh dispatches on its arguments at the bottom, so sourcing it would start doing things.

        The span is VALIDATED, not trusted. A regex is not a shell parser: a column-zero `}` inside a
        here-doc, or a missing outer brace, could silently select a fragment, and a test running a
        fragment proves nothing while looking green. `bash -n` on the extracted text turns that into
        a loud failure, and the terminator is required to be the real outer close.
        """
        text = SCRIPT.read_text()
        body = re.search(r"^%s\(\) \{.*?^\}" % re.escape(name), text, re.S | re.M)
        assert body, "could not extract %s from %s" % (name, SCRIPT)
        fn = body.group(0)
        assert fn.endswith("\n}"), "%s: the extracted span does not end at a column-zero close" % name
        check = subprocess.run([BASH, "-n"], input=fn, capture_output=True, text=True)
        assert check.returncode == 0, (
            "%s: the extracted span is not valid shell, so the regex selected the wrong text — "
            "fix the extraction rather than testing a fragment:\n%s" % (name, check.stderr))
        return fn

    def _run(self, entry, ensure_rc, pause_rc=0):
        """Run one dev-cleanup entry point against recorders. Returns the observed effect sequence.

        Everything the assertion is ABOUT is real: the entry point itself and ensure_topics_or_refuse.
        Its neighbours become recorders, so the subject of every claim below is the entry point's own
        behaviour and not a helper's spelling.
        """
        with tempfile.TemporaryDirectory() as d:
            seq = Path(d) / "seq"
            kt = Path(d) / "kafka-topics"
            kubectl = Path(d) / "kubectl"
            # $KT records a delete ONLY for `--delete --topic <nonempty>`, and records WHICH topic.
            # Recording on a bare `--delete` would let a broken xargs pipeline — one that passes no
            # usable topic — still place a DELETE in the sequence, making the order an artefact of
            # this stub rather than of the code under test.
            kt.write_text(textwrap.dedent("""\
                #!/usr/bin/env bash
                del=0; topic=""
                while [ $# -gt 0 ]; do
                  case "$1" in
                    --delete) del=1 ;;
                    --topic)  shift; topic="${1:-}" ;;
                  esac
                  shift
                done
                [ "$del" = 1 ] && [ -n "$topic" ] && echo "DELETE:$topic" >> "%s"
                exit 0
                """) % seq)
            # $K/$KK: one deployment to work on, no pods left running, and every scale RECORDED.
            # Listing a deployment is what makes the scale loops execute at all — with an empty list
            # "nothing was scaled up" is true of every run and the refusal test below proves nothing.
            # `get pod` stays empty so do_clean's wait-for-termination loop exits immediately.
            kubectl.write_text(textwrap.dedent("""\
                #!/usr/bin/env bash
                if [ "${1:-}" = scale ]; then echo "SCALE:${2:-}:${3:-}" >> "%s"; exit 0; fi
                case "$*" in
                  *"get deploy -o name"*) printf 'deployment.apps/indicator-service\ndeployment.apps/disabled-service\n' ;;
                esac
                exit 0
                """) % seq)
            for f in (kt, kubectl):
                f.chmod(f.stat().st_mode | stat.S_IEXEC)

            driver = Path(d) / "drive.sh"
            driver.write_text(textwrap.dedent("""\
                SEQ=%(seq)s
                rec() { echo "$1" >> "$SEQ"; }

                clean_guard()                { return 0; }
                require_kafka_cli()          { :; }
                clean_logs()                 { :; }
                dev_clean_preserved_topics() { echo "keep.one"; }
                reset_preserved_from_main()  { echo "keep.two"; }
                kafka_list_topics()          { printf 'a.topic\\nb.topic\\n'; }
                topic_count()                { echo 0; }
                wait_for_ready()             { return 0; }
                # do_start's own post-scale work. Stubbed so its SUCCESS path is reachable at all:
                # the control test below exists because an unreachable success path would make the
                # refusal test pass for the wrong reason, and it caught exactly that (rc=127).
                apply_internal_topic_configs()  { return 0; }
                verify_start_readiness()        { return 0; }
                run_partition_doctor()          { return 0; }
                pause_dev_mirrors()               { rec PAUSE; return %(pause_rc)s; }
                resume_dev_mirrors_if_declared()  { rec "RESUME:$1"; return 0; }
                ensure_topics()                   { rec RECREATE; return %(ensure_rc)s; }
                sleep() { :; }
                docker() { :; }
                tmutil() { :; }

                WIPE_KAFKA=true
                KEEP='^(keycloak)$'
                DISABLED_DEV='^(disabled-service)$'
                OVERNIGHT_SET="%(overnight_set)s"
                K=%(kubectl)s
                KK=%(kubectl)s
                KT=%(kt)s
                BS=127.0.0.1:9092
                KAFKA_DATA=%(dir)s
                TOPICS_ENV_REF=origin/main:scripts/kafka/topics.env
                TOPICS_ENV_SHA=deadbeef
                DEV_ONLY_ESTIMATE_KEEP=""
                RESET_PRESERVED_TOPICS=""
                CLEAN_TOPICS_FAILED=0

                %(ensure_fn)s
                %(entry_fn)s

                %(entry)s >/dev/null 2>&1
                echo "rc=$?" >> "$SEQ"
                echo "CLEAN_TOPICS_FAILED=$CLEAN_TOPICS_FAILED" >> "$SEQ"
                """) % {
                    "seq": seq, "kubectl": kubectl, "kt": kt, "dir": d,
                    "ensure_rc": ensure_rc, "entry": entry, "pause_rc": pause_rc,
                    "overnight_set": "indicator-service",
                    "ensure_fn": self._extract("ensure_topics_or_refuse"),
                    "entry_fn": self._extract(entry),
                })
            subprocess.run([BASH, str(driver)], capture_output=True, timeout=120)
            return seq.read_text().splitlines() if seq.exists() else []

    def test_wipe_pauses_before_deleting_and_resumes_after_recreating(self):
        seq = self._run("do_clean", ensure_rc=0)
        deletes = [e for e in seq if e.startswith("DELETE:")]
        self.assertEqual({"DELETE:a.topic", "DELETE:b.topic"}, set(deletes),
                         "both listed topics must actually be deleted, each with a usable --topic: %r" % seq)
        self.assertIn("PAUSE", seq, "do_clean never paused the es4->dev mirrors: a running mirror re-creates a "
                                    "deleted topic at dev's num.partitions=1 and the declared shape never applies")
        self.assertIn("RECREATE", seq, "do_clean never recreated the declared topics")
        resume = [i for i, e in enumerate(seq) if e.startswith("RESUME:")]
        self.assertTrue(resume, "the mirrors were never resumed: dev stays wiped with its mirrors held down")

        pause = seq.index("PAUSE")
        first_delete = min(i for i, e in enumerate(seq) if e.startswith("DELETE:"))
        last_delete = max(i for i, e in enumerate(seq) if e.startswith("DELETE:"))
        recreate = seq.index("RECREATE")
        self.assertLess(pause, first_delete, "observed %r: the mirrors must be unloaded BEFORE the delete" % seq)
        self.assertLess(last_delete, recreate, "observed %r: topics must be recreated AFTER the delete" % seq)
        self.assertLess(recreate, resume[0], "observed %r: the mirrors come back only after the recreate" % seq)
        self.assertIn("rc=0", seq, "a clean whose topic pass succeeded must SUCCEED: %r" % seq)

    def test_a_failed_recreation_reaches_the_resume_gate_and_carries_its_status_out(self):
        """What the deleted `$?` literal stood in for, observed rather than regexed.

        The gate is REACHED with the real status; it does not resume. On a non-zero status
        resume_dev_mirrors_if_declared deliberately leaves the mirrors PAUSED — reloading them before
        their targets exist would auto-create those at 1 partition, the exact wedge the pause exists
        to prevent — and that behaviour is asserted against the real function in
        test_mirrors_stay_paused_when_topics_env_was_not_applied. What this test adds is that the
        gate sees ensure_topics' REAL status rather than a hardcoded one, and that do_clean's own
        status carries the failure out: 'Setting a variable nobody reads is the same silence this
        round is about', as the script says at its return. Assert the status, not just the flag, or a
        `return 0` there passes unnoticed.
        """
        seq = self._run("do_clean", ensure_rc=1)
        self.assertIn("RECREATE", seq)
        self.assertIn("RESUME:1", seq,
                      "the resume gate must be reached with ensure_topics' REAL status (it then leaves "
                      "the mirrors paused, by design): %r" % seq)
        self.assertIn("CLEAN_TOPICS_FAILED=1", seq, "a failed recreation must be recorded: %r" % seq)
        self.assertIn("rc=1", seq,
                      "the recorded flag is not enough — do_clean's own status must carry the failure out, "
                      "or the CLEAN slot reads a failed wipe as a success: %r" % seq)

    def test_neither_start_path_scales_anything_when_the_topic_pass_fails(self):
        """The two other callers of the pair, proven by effect instead of by reading their text.

        The old text scan at least checked that do_start and do_start_overnight call the pair. This
        asserts the consequence that matters: a failed topic pass must stop the scale-up, because a
        workload started against a missing topic crash-loops and an auto-created one takes the broker
        DEFAULT shape instead of the declared one.
        """
        for entry in ("do_start", "do_start_overnight"):
            seq = self._run(entry, ensure_rc=1)
            self.assertIn("RECREATE", seq, "%s never ran the topic pass: %r" % (entry, seq))
            self.assertIn("rc=1", seq, "%s must REFUSE when the topic pass fails: %r" % (entry, seq))
            self.assertEqual([], [e for e in seq if e.startswith("SCALE:")],
                             "%s scaled something up after a failed topic pass: %r" % (entry, seq))

    def test_a_paused_list_that_cannot_be_created_is_not_proven(self):
        """`touch` is part of the proof too, and its failure was being ERASED.

        The assignment sat above `local unproven=0`, so a failed touch set the flag and the
        declaration immediately cleared it: the function still returned success. Found by review,
        not by the suite, which is why this test exists. `touch` is stubbed rather than the directory
        made read-only, so only THIS branch can be what fails — and the list file is created FIRST,
        because without it the later `sort` of a missing file fails too and sets the flag on its own:
        a mutation that dropped this branch SURVIVED until the two were separated.
        """
        self.paused.write_text("")
        fake_bin = Path(self.tmp.name) / "notouch"
        fake_bin.mkdir()
        s = fake_bin / "touch"
        s.write_text("#!/bin/sh\nexit 1\n")
        s.chmod(0o755)
        out = self.run_fn("pause_dev_mirrors", path_prefix=str(fake_bin), expect_rc=1, both=True)
        self.assertIn("cannot write the paused-mirror list", out)

    def test_a_plist_that_cannot_be_classified_is_not_proven(self):
        """An unreadable plist is "I cannot tell", not "not a mirror".

        Discovery skipped anything it could not parse and still exited 0, so a dev mirror launchd has
        LOADED but whose plist cannot be read produced no candidate, no launchctl observation and a
        successful pause — and the wipe then deleted its target behind it.
        """
        (self.agents / "com.optionsedge.unreadable.plist").write_text("this is not a plist")
        out = self.run_fn("pause_dev_mirrors", expect_rc=1, both=True)
        self.assertIn("cannot classify", out)
        self.assertIn("com.optionsedge.unreadable.plist", out)
    def test_a_paused_list_that_cannot_be_recorded_is_not_proven(self):
        """The list IS part of the proof, so failing to write it is "not proven", not a detail.

        The resume reloads exactly what this list names. An incomplete one means a later successful
        start brings back only some of the agents this clean took down, and the rest sit unloaded
        with nothing scheduled to notice — dev then reads FROZEN es4 topics while reporting healthy,
        the failure this file complains about elsewhere. A mutation that drops this branch survived
        the suite until this test existed.
        """
        fake_bin = Path(self.tmp.name) / "nosort"
        fake_bin.mkdir()
        s = fake_bin / "sort"
        s.write_text("#!/bin/sh\nexit 1\n")
        s.chmod(0o755)
        out = self.run_fn("pause_dev_mirrors", path_prefix=str(fake_bin), expect_rc=1, both=True)
        self.assertIn("could not record the paused-mirror list", out)
    def test_an_unobservable_launchctl_is_not_read_as_nothing_being_loaded(self):
        """`launchctl list <label>` answering non-zero meant "not loaded" — even when launchctl
        itself could not answer.

        A missing binary or an unreachable launchd made EVERY mirror look absent: nothing was booted
        out, nothing was warned about, the paused list stayed empty, and the function reported
        success. do_clean then deleted topics with every mirror still live, which is the precise
        wedge it pauses them to avoid. "It is gone" and "I could not look" must not be the same
        answer.
        """
        (self.state / "broken").touch()
        out = self.run_fn("pause_dev_mirrors", expect_rc=1, both=True)
        self.assertIn("cannot tell whether", out)
        self.assertIn("NOT proven down", out)

    def test_do_clean_deletes_nothing_when_the_pause_reports_failure(self):
        """The CALLER's half of it: whatever made the pause fail, do_clean must not delete.

        This substitutes a failing pause rather than composing the real unobservable-launchctl
        condition end to end — the launchctl cases are proven against the real function in the tests
        above, and this proves the status is honoured on the deciding path. Said plainly because
        "unobservable launchctl stops the wipe" would claim an end-to-end path this does not run.
        """
        seq = self._run("do_clean", ensure_rc=0, pause_rc=1)
        self.assertEqual([], [e for e in seq if e.startswith("DELETE:")],
                         "a wipe ran while the mirrors were not proven down: %r" % seq)
    def test_a_pause_that_cannot_prove_the_mirrors_are_down_deletes_nothing(self):
        """The hazard this whole module exists for, finally asserted on the deciding path.

        pause_dev_mirrors detects both dangerous cases — discovery failed, or an agent would not
        unload — and used to print ERROR/WARN and return 0, so do_clean deleted anyway: the function
        announced the hazard and then permitted it. A mirror still running re-creates its target
        0.3 s after the delete, at dev's num.partitions=1, so ensure_topics' declared shape never
        applies and nothing afterwards can see that it did not (2026-09-14,
        es.options.databento.gex.strike). The wipe has not begun at that point, so refusing leaves
        dev scaled down but WHOLE.
        """
        seq = self._run("do_clean", ensure_rc=0, pause_rc=1)
        self.assertIn("PAUSE", seq, "the pause must have been attempted: %r" % seq)
        self.assertEqual([], [e for e in seq if e.startswith("DELETE:")],
                         "do_clean deleted topics behind a mirror it could not prove was down: %r" % seq)
        self.assertNotIn("RECREATE", seq, "nothing should have been recreated: the wipe never ran: %r" % seq)
        self.assertIn("rc=1", seq, "a wipe that could not pause the mirrors must REFUSE: %r" % seq)

    def test_start_scales_the_disabled_set_to_zero_and_everything_else_to_one(self):
        """The success control, strong enough to notice do_start scaling the wrong things.

        With one deployment and no disabled set, "something was scaled" passed while do_start could
        have been scaling every deployment to 1 — including the ones deliberately held down.
        """
        seq = self._run("do_start", ensure_rc=0)
        self.assertIn("rc=0", seq, "a start whose topic pass succeeded must succeed: %r" % seq)
        scales = {e.split(":")[1]: e.split(":")[2] for e in seq if e.startswith("SCALE:")}
        self.assertEqual({"deploy/indicator-service": "--replicas=1",
                          "deploy/disabled-service": "--replicas=0"}, scales,
                         "the disabled set must be held at 0 and everything else brought to 1: %r" % seq)

    def test_overnight_start_scales_its_set_up_when_the_topic_pass_succeeds(self):
        """The control for the overnight refusal: with an empty set it could never scale anything,
        so "nothing was scaled up after a failure" was true of every run."""
        seq = self._run("do_start_overnight", ensure_rc=0)
        self.assertIn("rc=0", seq, "an overnight start whose topic pass succeeded must succeed: %r" % seq)
        self.assertIn("SCALE:deploy/indicator-service:--replicas=1", seq,
                      "the overnight set must actually be scaled up: %r" % seq)


if __name__ == "__main__":
    unittest.main()
