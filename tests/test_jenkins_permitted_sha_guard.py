"""The Jenkins-side permitted-commit guard (Deployment Permission Rule, options-edge rule.md).

Two things are under test, and each is driven through its refusal cases, not just its happy path:

  * scripts/jenkins/permitted-sha-guard.sh — the script every deploy job runs before its first
    effect. Its bash suite (permitted-sha-guard-test.sh, shared byte-for-byte with the gateway and
    processing repositories) covers: permitted A with checkout A; missing / empty / whitespace /
    short / uppercase / non-SHA values; permitted A with checkout B; the branch advancing between
    queue and checkout; a matching SHA on a forbidden ref; an unreachable origin; a nested
    application checkout.
  * scripts/ci/validate-jenkins-permitted-sha-guard.sh — the CI gate that every Jenkinsfile carries
    the guard BEFORE any effect. Run against the repository (must pass), against the committed
    refused fixture (the apply-before-guard shape from the Codex finding), and against one mutation
    of the good fixture per rule the validator enforces, so a validator that quietly stops checking
    something fails here instead of going green.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "scripts/jenkins/permitted-sha-guard.sh"
GUARD_SUITE = ROOT / "scripts/jenkins/permitted-sha-guard-test.sh"
VALIDATOR = ROOT / "scripts/ci/validate_jenkins_permitted_sha_guard.py"
FIXTURES = ROOT / "tests/fixtures/permitted-sha-guard"


def run_validator(root: Path, manifest: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["python3", str(VALIDATOR), "--root", str(root), "--manifest", str(manifest), "--guard-script", str(GUARD)],
        capture_output=True, text=True,
    )


class PermittedShaGuardScriptTest(unittest.TestCase):
    def test_guard_script_parses(self) -> None:
        subprocess.run(["bash", "-n", str(GUARD)], check=True)

    def test_guard_refuses_every_unpermitted_case_and_permits_the_exact_commit(self) -> None:
        r = subprocess.run(["bash", str(GUARD_SUITE)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("permitted-sha-guard-test: ALL PASS", r.stdout)
        self.assertNotIn("FAIL [", r.stdout)
        # The suite must have exercised the named contract cases, by name.
        for case in [
            "permitted A, branch main, checkout A",
            "PERMITTED_SHA unset",
            "PERMITTED_SHA empty",
            "PERMITTED_SHA whitespace only",
            "short SHA (12)",
            "uppercase SHA",
            "permitted A, checkout B",
            "branch advanced B->D, permitted B",
            "feature commit C, permitted C",
            "main commit but ref reports feature",
            "origin unreachable",
            "nested at B, permitted D",
        ]:
            self.assertIn(f"ok   [{case}]", r.stdout)

    def test_guard_never_substitutes_a_value(self) -> None:
        code = "\n".join(l for l in GUARD.read_text().splitlines() if not l.lstrip().startswith("#"))
        # No fallback of any kind for the permitted value: the only PERMITTED_SHA expansions are the
        # presence test, the raw print and the assignment to p.
        self.assertNotIn("${PERMITTED_SHA:-", code)
        self.assertNotIn("GIT_COMMIT", code)
        for forbidden in ("catchError", "|| true"):
            self.assertNotIn(forbidden, code)


class PermittedShaGuardValidatorTest(unittest.TestCase):
    def test_repository_passes(self) -> None:
        r = subprocess.run(["bash", str(ROOT / "scripts/ci/validate-jenkins-permitted-sha-guard.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("carry the permitted-commit guard before any effect", r.stdout)

    def test_every_jenkinsfile_is_classified(self) -> None:
        manifest = (ROOT / "scripts/ci/jenkins-permitted-sha-scope.txt").read_text()
        listed = {l.split("|")[0].strip() for l in manifest.splitlines() if l.strip() and not l.startswith("#")}
        present = {p.name for p in ROOT.glob("Jenkinsfile*") if p.is_file()}
        self.assertEqual(listed, present)

    def test_good_fixture_passes(self) -> None:
        d = FIXTURES / "good"
        r = run_validator(d, d / "scope.txt")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_committed_refused_fixture_apply_before_guard(self) -> None:
        d = FIXTURES / "refused"
        r = run_validator(d, d / "scope.txt")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("stage 'Deploy' precedes the guard", r.stdout)
        self.assertIn("mutation token before the guard", r.stdout)
        self.assertIn("kubectl", r.stdout)

    # ---- one mutation per enforced rule, built from the good fixture -------------------------------
    def _mutated(self, transform, manifest_line: str | None = None, extra_files: dict[str, str] | None = None):
        good = (FIXTURES / "good/Jenkinsfile.fixture-good").read_text()
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        (tmp / "Jenkinsfile.fixture-good").write_text(transform(good))
        for name, body in (extra_files or {}).items():
            (tmp / name).write_text(body)
        (tmp / "scope.txt").write_text(manifest_line or (FIXTURES / "good/scope.txt").read_text())
        return run_validator(tmp, tmp / "scope.txt")

    def test_mutation_missing_parameter(self) -> None:
        r = self._mutated(lambda s: s.replace("    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')\n", ""))
        self.assertEqual(r.returncode, 1)
        self.assertIn("lacks string(name: 'PERMITTED_SHA'", r.stdout)

    def test_mutation_parameter_without_trim_or_with_default(self) -> None:
        r = self._mutated(lambda s: s.replace("defaultValue: '', trim: true, description: 'the permitted commit'", "defaultValue: 'main', description: 'x'"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("defaultValue: ''", r.stdout)
        self.assertIn("trim: true", r.stdout)

    def test_mutation_restart_from_stage_enabled(self) -> None:
        r = self._mutated(lambda s: s.replace("disableRestartFromStage(); ", ""))
        self.assertEqual(r.returncode, 1)
        self.assertIn("disableRestartFromStage()", r.stdout)

    def test_mutation_guard_stage_missing(self) -> None:
        r = self._mutated(lambda s: s.replace("stage('Permitted commit guard')", "stage('Preflight')"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("no stage named 'Permitted commit guard'", r.stdout)

    def test_mutation_guard_wrapped_in_catch_error(self) -> None:
        r = self._mutated(lambda s: s.replace("          if (rc != 0) {\n            error(", "          catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {\n            error("))
        self.assertEqual(r.returncode, 1)
        self.assertIn("catchError", r.stdout)

    def test_mutation_guard_does_not_error(self) -> None:
        r = self._mutated(lambda s: s.replace("error(\"Permitted commit guard REFUSED this build (rc=${rc})\")", "echo \"guard said ${rc}\""))
        self.assertEqual(r.returncode, 1)
        self.assertIn("must error()", r.stdout)

    def test_mutation_guard_calls_something_else(self) -> None:
        r = self._mutated(lambda s: s.replace("bash scripts/jenkins/permitted-sha-guard.sh", "bash scripts/jenkins/enforce-main-branch.sh"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("does not call scripts/jenkins/permitted-sha-guard.sh", r.stdout)

    def test_mutation_guard_does_not_set_flag(self) -> None:
        r = self._mutated(lambda s: s.replace("          env.PERMITTED_SHA_GUARD = 'PASSED'\n", ""))
        self.assertEqual(r.returncode, 1)
        self.assertIn("PERMITTED_SHA_GUARD = 'PASSED'", r.stdout)

    def test_mutation_unlisted_stage_before_guard(self) -> None:
        r = self._mutated(lambda s: s.replace("  stages {\n", "  stages {\n    stage('Warm up') { steps { echo 'hi' } }\n"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("stage 'Warm up' precedes the guard but is not in the manifest's before= set", r.stdout)

    def test_listed_stage_before_guard_is_allowed_but_its_effects_are_not(self) -> None:
        line = "Jenkinsfile.fixture-good | in | before=Refuse anything but main | fixture\n"
        ok = self._mutated(lambda s: s.replace("  stages {\n", "  stages {\n    stage('Refuse anything but main') { steps { sh 'git fetch origin main' } }\n"), manifest_line=line)
        self.assertEqual(ok.returncode, 0, ok.stdout)
        bad = self._mutated(lambda s: s.replace("  stages {\n", "  stages {\n    stage('Refuse anything but main') { steps { sh 'docker push x' } }\n"), manifest_line=line)
        self.assertEqual(bad.returncode, 1)
        self.assertIn("mutation token before the guard", bad.stdout)

    def test_mutation_downstream_trigger_unbound(self) -> None:
        r = self._mutated(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", "        build job: 'other-job', parameters: [string(name: 'X', value: 'y')]\n"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("build job: forwards no PERMITTED_SHA", r.stdout)
        ok = self._mutated(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", "        build job: 'other-job', parameters: [string(name: 'PERMITTED_SHA', value: params.PERMITTED_SHA)]\n"))
        self.assertEqual(ok.returncode, 0, ok.stdout)
        marked = self._mutated(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", "        // UNBOUND-DOWNSTREAM: other-job has no guard yet\n        build job: 'other-job'\n"))
        self.assertEqual(marked.returncode, 0, marked.stdout)

    def test_mutation_post_mutates_without_gate(self) -> None:
        r = self._mutated(lambda s: s.replace("    always { echo 'fixture done' }", "    always { sh 'kubectl delete job old' }"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("post {} mutates a target but is not gated on PERMITTED_SHA_GUARD", r.stdout)
        ok = self._mutated(lambda s: s.replace("    always { echo 'fixture done' }", "    always { script { if (env.PERMITTED_SHA_GUARD == 'PASSED') { sh 'kubectl delete job old' } } }"))
        self.assertEqual(ok.returncode, 0, ok.stdout)

    def test_mutation_unclassified_jenkinsfile(self) -> None:
        r = self._mutated(lambda s: s, extra_files={"Jenkinsfile.fixture-stray": "pipeline { agent any stages { stage('x') { steps { echo 'x' } } } }\n"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("Jenkinsfile.fixture-stray: not classified", r.stdout)

    def test_mutation_reguard_missing(self) -> None:
        line = "Jenkinsfile.fixture-good | in | reguard=Deploy | fixture\n"
        r = self._mutated(lambda s: s, manifest_line=line)
        self.assertEqual(r.returncode, 1)
        self.assertIn("must be immediately followed by 'Permitted commit guard (deploy workspace)'", r.stdout)

    def test_out_of_scope_needs_a_reason(self) -> None:
        r = self._mutated(lambda s: s, manifest_line="Jenkinsfile.fixture-good | out | |\n")
        self.assertEqual(r.returncode, 1)
        self.assertIn("needs a reason", r.stdout + r.stderr)


class ServiceDeployBindingTest(unittest.TestCase):
    """service-deploy's specifics that the generic validator does not model."""

    def setUp(self) -> None:
        self.text = (ROOT / "Jenkinsfile.service-deploy").read_text()

    def test_intent_ledger_claim_records_the_permitted_and_checked_out_commit(self) -> None:
        self.assertIn('"permittedSha":os.environ.get("PERMITTED_SHA","")', self.text)
        self.assertIn('"checkedOutSha":subprocess.check_output(["git","rev-parse","HEAD"])', self.text)

    def test_image_build_trigger_is_bound_by_its_own_permitted_sha(self) -> None:
        self.assertIn("string(name: 'PROCESSING_PERMITTED_SHA', defaultValue: '', trim: true", self.text)
        self.assertRegex(self.text, r"psha\.matches\('\^\[0-9a-f\]\{40\}\$'\)")
        self.assertIn("string(name: 'PERMITTED_SHA',      value: psha)", self.text)
        # The check precedes the trigger.
        self.assertLess(self.text.index("psha.matches("), self.text.index("build job: 'options-edge-processing'"))

    def test_guard_runs_before_the_intent_claim_and_again_on_the_deploy_host(self) -> None:
        stages = re.findall(r"^\s*stage\('([^']*)'", self.text, re.M)
        self.assertEqual(stages[0], "Permitted commit guard")
        self.assertEqual(stages[1], "Intent dedup")
        self.assertEqual(stages[stages.index("Deploy path") + 1], "Permitted commit guard (deploy workspace)")


if __name__ == "__main__":
    unittest.main()
