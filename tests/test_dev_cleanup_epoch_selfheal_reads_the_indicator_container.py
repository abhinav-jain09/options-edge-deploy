"""The dev-cleanup epoch self-heal must actually recover an indicator that has lost its epoch.

2026-10-22: after a dev clean the indicator-service pod looped on
`EpochFatalException: no epoch recovery copy and INDICATOR_EPOCH_GENESIS is not authorized`, and
dev-cleanup.sh's self-heal - written for exactly this - never ran. The pod has TWO containers
(indicator-estimate, indicator-service); `kubectl logs deploy/indicator-service` defaults to the FIRST, which
never contains the exception. Measured on the live pod: the default container matched the signature 0
times, `-c indicator-service` 3 times.

These tests do not match source text. They EXTRACT the real self-heal block and its helper from
dev-cleanup.sh and RUN them against a fake kubectl that models the two-container pod, then assert what was
done: genesis switched on exactly once and back off afterwards, in that order, on the incident, on a
restart race (signature only in the previous container instance), never on a healthy pod, loudly when the
logs cannot be read, and the flag is reset even when the pod never gets ready.
"""
import os
import re
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "dev-cleanup.sh"
BASH = "/bin/bash" if Path("/bin/bash").exists() else "bash"   # macOS: the 3.2 launchd runs it with

SIGNATURE = (
    "Caused by: com.optionsedge.processing.indicator.engine.EpochMachine$EpochFatalException: "
    "no epoch recovery copy and INDICATOR_EPOCH_GENESIS is not authorized (rev 14 §3.1)"
)

# A fake kubectl for a pod with two containers. Behaviour is selected by files in $FAKE_STATE:
#   only_previous   the signature is only in the --previous instance (a restart race)
#   healthy         the deployment is ready from the start and logs are clean
#   unreadable      `logs -c indicator-service` fails (a renamed/missing container)
#   never_ready     the deployment never becomes ready, even after genesis
FAKE_KUBECTL = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    set -u
    echo "$*" >> "$FAKE_STATE/calls"
    st="$FAKE_STATE"
    case "$*" in
      *"get deploy indicator-service -o jsonpath"*)
        if [ -f "$st/healthy" ]; then echo 1
        elif [ -f "$st/never_ready" ]; then echo 0
        elif [ -f "$st/ever_on" ]; then echo 1
        else echo 0; fi ;;
      *"logs deploy/indicator-service"*)
        if ! echo "$*" | grep -q -- "-c indicator-service"; then
          # a bare call reads the FIRST container: indicator-estimate, which never holds the exception
          echo "[main] INFO consumer clientId=consumer-indicator-estimate-0 SubscriptionState resetting"; exit 0
        fi
        if [ -f "$st/unreadable" ]; then echo 'error: container indicator-service is not valid for pod' >&2; exit 1; fi
        if [ -f "$st/healthy" ]; then echo "[main] INFO started"; exit 0; fi
        if echo "$*" | grep -q -- "--previous"; then
          if [ -f "$st/only_previous" ]; then echo '%(sig)s'; exit 0; fi
          echo 'Error from server (BadRequest): previous terminated container not found' >&2; exit 1
        fi
        if [ -f "$st/only_previous" ]; then echo "[main] INFO starting up"; else echo '%(sig)s'; fi ;;
      *"set env deploy/indicator-service INDICATOR_EPOCH_GENESIS=true"*) touch "$st/ever_on" ;;
      *"set env deploy/indicator-service INDICATOR_EPOCH_GENESIS=false"*) : ;;
    esac
    """
) % {"sig": SIGNATURE}


def extract_pieces():
    text = SCRIPT.read_text()
    helper = re.search(r"^indicator_epoch_fatal_count\(\) \{\n.*?^\}\n", text, re.S | re.M)
    assert helper, "indicator_epoch_fatal_count() is gone from dev-cleanup.sh; update this test deliberately"
    start = text.index('  if ! echo "$DISABLED_DEV" | grep -qx "indicator-service" 2>/dev/null; then')
    end = text.index("\n\n  # Schema self-heal:", start)
    return helper.group(0), text[start:end]


class EpochSelfHeal(unittest.TestCase):
    # dev-cleanup.sh runs with `set -uo pipefail` (no errexit). Tests default to exactly that mode. The key
    # scenarios are ALSO run in the strictest shell: errexit in POSIX mode, where command substitutions INHERIT
    # errexit (macOS bash 3.2 does not inherit it otherwise, so plain `set -e` would test nothing). The helper
    # must not depend on errexit being off.
    SCRIPT_MODE = "set -uo pipefail"
    STRICT_MODE = "set -euo pipefail; set -o posix"

    def run_block(self, *flags, mutate_helper=None, shell_opts=None):
        helper, block = extract_pieces()
        if mutate_helper:
            helper = mutate_helper(helper)
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            fake = tmp / "kubectl"
            fake.write_text(FAKE_KUBECTL)
            fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
            for flag in flags:
                (tmp / flag).touch()
            harness = tmp / "harness.sh"
            harness.write_text(
                f"{shell_opts or self.SCRIPT_MODE}\n"
                "sleep() { :; }\n"          # the real block polls every 5 s; the fake answers instantly
                f'K="{fake}"; KK="{fake}"; DISABLED_DEV=""\n'
                + helper
                + "run_block() {\n" + block + "\n}\nrun_block\n"
            )
            env = dict(os.environ, FAKE_STATE=str(tmp))
            done = subprocess.run([BASH, str(harness)], capture_output=True, text=True, env=env, timeout=60)
            calls = (tmp / "calls").read_text().splitlines() if (tmp / "calls").exists() else []
        return done, calls

    @staticmethod
    def genesis_calls(calls):
        return [c for c in calls if "INDICATOR_EPOCH_GENESIS=" in c and "set env" in c]

    def test_the_incident_runs_genesis_once_and_resets_it(self):
        done, calls = self.run_block()
        self.assertEqual(0, done.returncode, done.stderr)
        gen = self.genesis_calls(calls)
        self.assertEqual(2, len(gen), f"genesis must be set on then off exactly once: {gen}")
        self.assertTrue(gen[0].endswith("INDICATOR_EPOCH_GENESIS=true"), gen)
        self.assertTrue(gen[1].endswith("INDICATOR_EPOCH_GENESIS=false"), gen)
        self.assertIn("epoch self-heal done", done.stdout)

    def test_a_signature_only_in_the_previous_instance_still_triggers_genesis(self):
        # After a restart the exception can be only in the terminated instance's logs.
        done, calls = self.run_block("only_previous")
        gen = self.genesis_calls(calls)
        self.assertEqual(2, len(gen), f"a restart race hid the failure: {gen}\n{done.stdout}")
        self.assertIn("epoch self-heal done", done.stdout)

    def test_a_healthy_indicator_is_never_touched(self):
        done, calls = self.run_block("healthy")
        self.assertEqual([], self.genesis_calls(calls), "genesis must never run on a healthy indicator")

    def test_unreadable_logs_are_reported_not_read_as_no_failure(self):
        done, calls = self.run_block("unreadable")
        self.assertEqual([], self.genesis_calls(calls))
        self.assertIn("cannot read the indicator-service container logs", done.stderr)

    def test_genesis_is_reset_even_when_the_pod_never_becomes_ready(self):
        done, calls = self.run_block("never_ready")
        gen = self.genesis_calls(calls)
        self.assertEqual(2, len(gen), gen)
        self.assertTrue(gen[0].endswith("=true") and gen[1].endswith("=false"), gen)
        self.assertIn("needs manual attention", done.stdout)

    def test_the_key_scenarios_also_hold_under_strict_errexit(self):
        # Expected non-zero outcomes (no previous instance, grep -c with zero matches) must not abort a
        # strict shell. The fatal-current / no-previous case is the incident itself.
        for flags, expect_genesis in (((), True), (("only_previous",), True), (("healthy",), False)):
            with self.subTest(flags=flags):
                done, calls = self.run_block(*flags, shell_opts=self.STRICT_MODE)
                gen = self.genesis_calls(calls)
                self.assertEqual(0, done.returncode, f"aborted under errexit: {done.stderr}\n{done.stdout}")
                self.assertEqual(2 if expect_genesis else 0, len(gen), f"{flags}: {gen}")

    def test_the_strict_mode_test_is_meaningful(self):
        # The pre-fix helper (`out=$(...); rc=$?` and a bare `grep -c`) ABORTS under inherited errexit on the
        # incident, before genesis is ever enabled. If this stops failing, the strict test above proves nothing.
        unsafe = lambda h: h.replace(" && rc=0 || rc=$?", "; rc=$?").replace(" || true)))", ")))")
        done, calls = self.run_block(mutate_helper=unsafe, shell_opts=self.STRICT_MODE)
        self.assertNotEqual(0, done.returncode, "the unsafe helper no longer aborts: the strict test is vacuous")
        self.assertEqual([], self.genesis_calls(calls))

    def test_the_original_bare_log_read_never_bootstraps(self):
        # The 2026-10-22 bug: without -c the default container is read, which never holds the exception.
        done, calls = self.run_block(
            mutate_helper=lambda h: h.replace("-c indicator-service ", "")
        )
        self.assertEqual(
            [], self.genesis_calls(calls),
            "if this fails the fake no longer models the bug; the other tests would prove nothing",
        )


if __name__ == "__main__":
    unittest.main()
