# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""dev-cleanup.sh after indicator-service learned to recover its own epoch (2026-11-02).

The outage: `dev-cleanup.sh now` wiped every dev topic, indicator-service refused to start (no epoch copy), and
the toggle that was meant to heal it (here, plus a launchd job on the dev host) read the wrong container of the
two-container pod and never fired -> ~7 h without bars. The service now mints the epoch itself, so this script
must (1) no longer toggle anything, (2) never read a multi-container deploy's logs without naming the container,
(3) tell the operator `start` is required, (4) make `start` FAIL LOUDLY when indicator-service is not fully
ready, (5) refuse to wipe recovery state: the dev-clean keep list is read fail-closed.

These RUN the real functions extracted from the script against a fake kubectl / a throwaway git repo.
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
TOPICS_ENV = ROOT / "scripts" / "kafka" / "topics.env"
BASH = "/bin/bash" if Path("/bin/bash").exists() else "bash"
TEXT = SCRIPT.read_text()

SHARED_KEEP = """options.indicators.snapshot.current options.indicators.bars options.spx.market-structure.recovery
es.futures.footprint es.futures.footprint.bars es.futures.footprint.strike""".split()
ESTIMATE_KEEP = """options.indicators.estimate.bars options.indicators.estimate.bars.current
options.spx.market-structure.estimate.recovery options.spx.market-structure.estimate.patterns
options.spx.market-structure.estimate.ui options.spx.market-structure.estimate.sequence""".split()


def func(name):
    m = re.search(r"^%s\(\) \{\n.*?^\}\n" % name, TEXT, re.S | re.M)
    assert m, f"{name}() is gone from dev-cleanup.sh; update this test deliberately"
    return m.group(0)


FAKE_KUBECTL = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    # `get pods -l app.kubernetes.io/name=<svc> -o jsonpath=...` -> the ready flags of that pod's containers
    for a in "$@"; do case "$a" in app.kubernetes.io/name=*) svc=${a#app.kubernetes.io/name=};; esac; done
    f="$FAKE_STATE/$svc"; [ -f "$f" ] && cat "$f" || true
    """
)


class Readiness(unittest.TestCase):
    def run_verify(self, indicator, market="true false", disabled=""):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            k = tmp / "kubectl"
            k.write_text(FAKE_KUBECTL)
            k.chmod(k.stat().st_mode | stat.S_IXUSR)
            def lines(spec):   # "true true" or "true true;|false true" (several pods, "ts|flags")
                return "\n".join(x if "|" in x else "|" + x for x in spec.split(";")) + "\n"
            if indicator is not None:
                (tmp / "indicator-service").write_text(lines(indicator))
            (tmp / "market-structure-service").write_text(lines(market))
            h = tmp / "h.sh"
            h.write_text(
                "set -uo pipefail\nsleep() { :; }\n"
                f'KK="{k}"; DISABLED_DEV="{disabled}"; INDICATOR_READY_POLLS=3\n'
                + func("pod_ready_report") + func("verify_start_readiness")
                + "verify_start_readiness\n"
            )
            done = subprocess.run([BASH, str(h)], capture_output=True, text=True,
                                  env=dict(os.environ, FAKE_STATE=str(tmp)), timeout=60)
        return done

    def test_two_of_two_passes_and_reports_market_structure(self):
        d = self.run_verify("true true")
        self.assertEqual(0, d.returncode, d.stderr)
        self.assertIn("indicator-service 2/2 containers ready", d.stdout)
        self.assertIn("market-structure-service 1/2 containers ready", d.stdout)

    def test_one_of_two_fails_loudly(self):
        d = self.run_verify("false true")
        self.assertEqual(1, d.returncode)
        self.assertIn("FAILED: indicator-service did NOT become fully ready", d.stderr)

    def test_no_pod_at_all_fails(self):
        d = self.run_verify(None)
        self.assertEqual(1, d.returncode)

    def test_market_structure_not_ready_is_reported_not_fatal(self):
        d = self.run_verify("true true", market="false false")
        self.assertEqual(0, d.returncode)
        self.assertIn("market-structure-service 0/2 containers ready", d.stdout)

    def test_a_held_down_indicator_is_skipped(self):
        d = self.run_verify(None, disabled="indicator-service")
        self.assertEqual(0, d.returncode)
        self.assertIn("readiness check skipped", d.stdout)

    def test_an_old_ready_pod_cannot_vouch_for_a_new_unready_one(self):
        d = self.run_verify("true true;false true")      # rollout: old pod ready, NEW pod 1/2
        self.assertEqual(1, d.returncode)
        self.assertIn("FAILED", d.stderr)

    def test_a_terminating_unready_pod_is_ignored(self):
        d = self.run_verify("2026-11-02T01:00:00Z|false false;true true")
        self.assertEqual(0, d.returncode, d.stderr)
        self.assertIn("indicator-service 2/2 containers ready (1 pod(s))", d.stdout)

    def test_a_pod_without_container_statuses_is_not_ready(self):
        self.assertEqual(1, self.run_verify("|").returncode)

    def test_do_start_returns_the_verdict(self):
        self.assertRegex(TEXT, r"verify_start_readiness \|\| verify_rc=\$\?")
        self.assertRegex(TEXT, r"return \$verify_rc")


class NoToggleAndContainerNames(unittest.TestCase):
    def test_no_genesis_toggle_remains(self):
        code = "\n".join(l for l in TEXT.splitlines() if not l.lstrip().startswith("#"))
        self.assertNotIn("INDICATOR_EPOCH_GENESIS", code)
        self.assertNotIn("set env deploy/indicator-service", code)
        self.assertNotIn("indicator_epoch_fatal_count", code)

    def test_every_deploy_log_read_names_its_container(self):
        for line in TEXT.splitlines():
            if line.lstrip().startswith("#"):
                continue
            if re.search(r"\blogs deploy/", line):
                self.assertRegex(line, r"logs deploy/\S+ -c \S+", line)

    def test_now_clean_tells_the_operator_start_is_required(self):
        self.assertIn("'dev-cleanup.sh start' is REQUIRED", TEXT)


class KeepList(unittest.TestCase):
    def call(self, topics_env_text):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            repo = tmp / "r"
            (repo / "scripts/kafka").mkdir(parents=True)
            (repo / "scripts/kafka/topics.env").write_text(topics_env_text)
            for c in (["init", "-q", "-b", "main"], ["add", "-A"],
                      ["-c", "user.email=a@b", "-c", "user.name=t", "commit", "-qm", "x"],
                      ["remote", "add", "origin", str(repo)]):     # fetches from itself: the fetch must SUCCEED
                subprocess.run(["git", "-C", str(repo)] + c, check=True, capture_output=True)
            h = tmp / "h.sh"
            h.write_text(
                f'DEPLOY_REPO="{repo}"; TOPICS_ENV_REF="main:scripts/kafka/topics.env"\nsleep() {{ :; }}\n'
                + func("dev_clean_preserved_topics") + "dev_clean_preserved_topics\n"
            )
            return subprocess.run([BASH, str(h)], capture_output=True, text=True, timeout=30)

    def test_the_shipped_declaration_is_read_and_has_the_six_shared_topics(self):
        d = self.call(TOPICS_ENV.read_text())
        self.assertEqual(0, d.returncode, d.stderr)
        self.assertEqual(sorted(SHARED_KEEP), sorted(d.stdout.split()))

    def test_the_six_dev_only_estimate_topics_are_kept_by_the_script_not_the_shared_file(self):
        m = re.search(r'^DEV_ONLY_ESTIMATE_KEEP="([^"]*)"', TEXT, re.M)
        self.assertEqual(sorted(ESTIMATE_KEEP), sorted(m.group(1).split()))
        self.assertNotIn("DEV_ONLY_ESTIMATE_KEEP", TOPICS_ENV.read_text())
        self.assertIn('DEV_KEEP="$DEV_KEEP $DEV_ONLY_ESTIMATE_KEEP"', TEXT)

    def test_a_failed_fetch_refuses_instead_of_trusting_a_stale_ref(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            repo = tmp / "r"
            (repo / "scripts/kafka").mkdir(parents=True)
            (repo / "scripts/kafka/topics.env").write_text(TOPICS_ENV.read_text())
            for c in (["init", "-q", "-b", "main"], ["add", "-A"],
                      ["-c", "user.email=a@b", "-c", "user.name=t", "commit", "-qm", "x"],
                      ["remote", "add", "origin", str(tmp / "does-not-exist")],
                      ["update-ref", "refs/remotes/origin/main", "HEAD"]):
                subprocess.run(["git", "-C", str(repo)] + c, check=True, capture_output=True)
            h = tmp / "h.sh"
            h.write_text(
                f'DEPLOY_REPO="{repo}"; TOPICS_ENV_REF="origin/main:scripts/kafka/topics.env"\nsleep() {{ :; }}\n'
                + func("dev_clean_preserved_topics") + "dev_clean_preserved_topics\n")
            d = subprocess.run([BASH, str(h)], capture_output=True, text=True, timeout=30)
        self.assertNotEqual(0, d.returncode, "a stale origin/main must not be trusted when the fetch fails")
        self.assertEqual("", d.stdout.strip())

    def test_a_successful_fetch_reads_the_NEW_origin_main_not_a_stale_ref(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            origin, clone = tmp / "origin", tmp / "clone"
            def git(repo, *a):
                subprocess.run(["git", "-C", str(repo), "-c", "user.email=a@b", "-c", "user.name=t"] + list(a),
                               check=True, capture_output=True)
            (origin / "scripts/kafka").mkdir(parents=True)
            subprocess.run(["git", "init", "-q", "-b", "main", str(origin)], check=True)
            env = origin / "scripts/kafka/topics.env"
            env.write_text('OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="old.topic"\n')
            git(origin, "add", "-A"); git(origin, "commit", "-qm", "1")
            subprocess.run(["git", "clone", "-q", str(origin), str(clone)], check=True)
            env.write_text('OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="old.topic new.topic"\n')
            git(origin, "commit", "-qam", "2")                       # origin moves on; the clone's origin/main is STALE
            h = tmp / "h.sh"
            h.write_text(f'DEPLOY_REPO="{clone}"; TOPICS_ENV_REF="origin/main:scripts/kafka/topics.env"\nsleep() {{ :; }}\n'
                         + func("dev_clean_preserved_topics") + "dev_clean_preserved_topics\n")
            d = subprocess.run([BASH, str(h)], capture_output=True, text=True, timeout=30)
        self.assertEqual(0, d.returncode, d.stderr)
        self.assertEqual(["old.topic", "new.topic"], d.stdout.split())

    def test_missing_declaration_refuses(self):
        self.assertNotEqual(0, self.call('OTHER="x"\n').returncode)

    def test_duplicate_declaration_refuses(self):
        v = 'OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="a b"\n'
        self.assertNotEqual(0, self.call(v + v).returncode)

    def test_empty_declaration_refuses(self):
        self.assertNotEqual(0, self.call('OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS=" "\n').returncode)

    def test_do_clean_refuses_before_touching_anything_when_the_list_is_unreadable(self):
        i_guard = TEXT.index("local DEV_KEEP=\"\"")
        i_logs = TEXT.index("  clean_logs", i_guard)
        self.assertIn("REFUSING the wipe", TEXT[i_guard:i_logs])
        self.assertIn("return 1", TEXT[i_guard:i_logs])

    def test_the_delete_loop_uses_the_keep_list(self):
        self.assertIn("$RESET_PRESERVED_TOPICS $DEV_KEEP", TEXT)


if __name__ == "__main__":
    unittest.main()
