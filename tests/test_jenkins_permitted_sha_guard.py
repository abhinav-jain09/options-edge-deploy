"""The Jenkins-side permitted-commit guard (Deployment Permission Rule, options-edge rule.md).

Under test, each driven through its refusal cases, not just its happy path:

  * scripts/jenkins/permitted-sha-guard.sh — the script every deploy job runs before its first
    effect. Its bash suite (permitted-sha-guard-test.sh, shared byte-for-byte with the gateway,
    processing and web repositories) covers: permitted A with checkout A; missing / empty /
    whitespace / short / uppercase / non-SHA values; permitted A with checkout B; the branch
    advancing between queue and checkout; a matching SHA on a forbidden ref — via GIT_BRANCH, via
    BRANCH_NAME, via a contradiction between the two, and via the explicitly selected --ref; an
    unreachable origin; a bare repository / .git directory; a nested application checkout.
  * scripts/jenkins/require-guarded-downstream.sh — EXECUTED against a local HTTP stand-in for the
    controller: refuses a downstream whose live definition lacks PERMITTED_SHA, an unreachable
    controller, a missing JENKINS_URL.
  * scripts/jenkins/fetch-permitted-image-lock.sh — EXECUTED against a served image lock: yields the
    digest for the permitted commit; refuses a lock from another commit, a missing image, an
    unreachable build.
  * scripts/deploy/post-deploy-recovery.sh — EXECUTED with a sentinel kubectl: a build whose deploy
    workspace was not permitted never reaches kubectl even when an earlier build stranded a recovery
    marker (the exact Codex I1 sequence); a permitted build does.
  * scripts/deploy/bind-required-image.sh — EXECUTED with a stub registry resolver: the permitted
    build's digest replaces a moved mutable tag; a wrong repository, an authoritative render that
    disagrees, and a digest the registry does not serve are refused.
  * scripts/ci/validate-jenkins-permitted-sha-guard.sh — run against the repository (must pass),
    against the committed refused fixture (apply-before-guard, the Codex round-2 shape), and against
    one mutation of the good fixture per rule it enforces — including the counterexamples Codex
    round 1 found accepted: `rc == 0`, a post that merely echoes the flag's name, an
    UNBOUND-DOWNSTREAM annotation, a `git pull` after the guard.
"""
from __future__ import annotations

import http.server
import json
import os
import re
import shutil
import socketserver
import stat
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "scripts/jenkins/permitted-sha-guard.sh"
GUARD_SUITE = ROOT / "scripts/jenkins/permitted-sha-guard-test.sh"
COMPAT = ROOT / "scripts/jenkins/require-guarded-downstream.sh"
FETCH_LOCK = ROOT / "scripts/jenkins/fetch-permitted-image-lock.sh"
RECOVERY = ROOT / "scripts/deploy/post-deploy-recovery.sh"
BIND = ROOT / "scripts/deploy/bind-required-image.sh"
VALIDATOR = ROOT / "scripts/ci/validate_jenkins_permitted_sha_guard.py"
FIXTURES = ROOT / "tests/fixtures/permitted-sha-guard"
SHA_A = "a" * 40
SHA_B = "b" * 40
DIGEST = "sha256:" + "c" * 64


def run_validator(root: Path, manifest: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["python3", str(VALIDATOR), "--root", str(root), "--manifest", str(manifest), "--guard-script", str(GUARD)],
        capture_output=True, text=True,
    )


class _Server:
    """A local HTTP stand-in for the Jenkins controller: path -> (status, body)."""

    def __init__(self, routes: dict[str, tuple[int, str]]):
        routes_local = routes

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                status, body = routes_local.get(self.path, (404, "not found"))
                data = body.encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):  # silence
                pass

        self.httpd = socketserver.TCPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        self.t = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.t.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def sh(script: Path, args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
    full = {"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/tmp")}
    full.update(env)
    return subprocess.run(["bash", str(script), *args], capture_output=True, text=True, env=full)


class PermittedShaGuardScriptTest(unittest.TestCase):
    def test_guard_script_parses(self) -> None:
        subprocess.run(["bash", "-n", str(GUARD)], check=True)

    def test_guard_refuses_every_unpermitted_case_and_permits_the_exact_commit(self) -> None:
        r = subprocess.run(["bash", str(GUARD_SUITE)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("permitted-sha-guard-test: ALL PASS", r.stdout)
        self.assertNotIn("FAIL [", r.stdout)
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
            "main commit but GIT_BRANCH reports feature",
            "BRANCH_NAME=main does NOT mask GIT_BRANCH=origin/feature",
            "--ref feature at a merged commit",
            ".git metadata dir refused",
            "bare repository refused",
            "origin unreachable",
            "nested at B, permitted D",
            "nested --ref feature at a merged commit",
        ]:
            self.assertIn(f"ok   [{case}]", r.stdout)

    def test_guard_never_substitutes_a_value(self) -> None:
        code = "\n".join(l for l in GUARD.read_text().splitlines() if not l.lstrip().startswith("#"))
        self.assertNotIn("${PERMITTED_SHA:-", code)
        self.assertNotIn("GIT_COMMIT", code)
        for forbidden in ("catchError", "|| true"):
            self.assertNotIn(forbidden, code)
        # The worktree predicate judges the printed answer, never the exit status.
        self.assertIn('[ "$inside" = "true" ]', code)


class RequireGuardedDownstreamTest(unittest.TestCase):
    def _defs(self, names: list[str]) -> str:
        return json.dumps({"property": [{"_class": "x"}, {"parameterDefinitions": [{"name": n} for n in names]}]})

    def test_guarded_downstream_is_accepted(self) -> None:
        s = _Server({"/job/child/api/json?tree=property[parameterDefinitions[name]]": (200, self._defs(["PERMITTED_SHA", "X"]))})
        self.addCleanup(s.close)
        r = sh(COMPAT, ["child"], {"JENKINS_URL": s.url + "/"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("declares PERMITTED_SHA", r.stdout)

    def test_unguarded_downstream_is_refused(self) -> None:
        s = _Server({"/job/child/api/json?tree=property[parameterDefinitions[name]]": (200, self._defs(["ENVIRONMENT"]))})
        self.addCleanup(s.close)
        r = sh(COMPAT, ["child"], {"JENKINS_URL": s.url})
        self.assertEqual(r.returncode, 1)
        self.assertIn("does not declare a PERMITTED_SHA parameter", r.stderr)

    def test_unreachable_or_unknown_job_is_refused(self) -> None:
        s = _Server({})
        self.addCleanup(s.close)
        r = sh(COMPAT, ["child"], {"JENKINS_URL": s.url})
        self.assertEqual(r.returncode, 1)
        self.assertIn("could not read the definition", r.stderr)

    def test_missing_jenkins_url_is_refused(self) -> None:
        r = sh(COMPAT, ["child"], {})
        self.assertEqual(r.returncode, 1)
        self.assertIn("JENKINS_URL is not set", r.stderr)


class FetchPermittedImageLockTest(unittest.TestCase):
    LOCK = (
        "OPTIONS_EDGE_IMAGE_LOCK_FORMAT=1\n"
        f"OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT={SHA_A}\n"
        f"ES_CVD_IMAGE=192.168.100.252:5000/options-edge-es-cvd:prod-7-aaaaaaaaaaaa@{DIGEST}\n"
        f"ES_CVD_IMAGE_GIT_COMMIT={SHA_A}\n"
        f"INDICATOR_SERVICE_IMAGE=192.168.100.252:5000/options-edge-indicator-service:prod-7-aaaaaaaaaaaa@{DIGEST}\n"
        f"INDICATOR_SERVICE_IMAGE_GIT_COMMIT={SHA_B}\n"
    )
    PATH = "/job/options-edge-processing/7/artifact/.jenkins-tmp/options-edge-image-lock.env"

    def _serve(self, body=None, status=200):
        s = _Server({self.PATH: (status, body if body is not None else self.LOCK)})
        self.addCleanup(s.close)
        return s.url

    def test_permitted_commit_yields_the_digest_pinned_image(self) -> None:
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd"], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), f"192.168.100.252:5000/options-edge-es-cvd:prod-7-aaaaaaaaaaaa@{DIGEST}")

    def test_lock_from_another_commit_is_refused(self) -> None:
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_B, "options-edge-es-cvd"], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 1)
        self.assertIn(f"was built from {SHA_A}, not the permitted {SHA_B}", r.stderr)
        self.assertEqual(r.stdout, "")

    def test_per_image_commit_mismatch_is_refused(self) -> None:
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-indicator-service"], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 1)
        self.assertIn("INDICATOR_SERVICE_IMAGE was built from", r.stderr)

    def test_missing_image_and_unreachable_build_are_refused(self) -> None:
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-vol-premium"], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 1)
        self.assertIn("has 0 digest-pinned entries", r.stderr)
        r = sh(FETCH_LOCK, ["options-edge-processing", "8", SHA_A, "options-edge-es-cvd"], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 1)
        self.assertIn("could not read the image lock", r.stderr)

    def test_malformed_inputs_are_refused(self) -> None:
        url = self._serve()
        for args, msg in [
            (["options-edge-processing", "x", SHA_A, "options-edge-es-cvd"], "is not a number"),
            (["options-edge-processing", "7", SHA_A[:12], "options-edge-es-cvd"], "not a full commit id"),
            (["options-edge-processing", "7", SHA_A, "options-edge-es-cvd; rm"], "not a plain image name"),
        ]:
            r = sh(FETCH_LOCK, args, {"JENKINS_URL": url})
            self.assertEqual(r.returncode, 1, args)
            self.assertIn(msg, r.stderr)


class PostDeployRecoveryTest(unittest.TestCase):
    """Codex I1: an earlier build stranded a recovery marker; this build passes the FIRST guard, enters
    Deploy path and fails the SECOND guard. post{always} must not scale anything."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.sentinel = self.tmp / "kubectl-was-called"
        fake = self.tmp / "bin" / "kubectl"
        fake.parent.mkdir()
        fake.write_text(f"#!/usr/bin/env bash\necho called >> '{self.sentinel}'\nexit 0\n")
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        self.marker = self.tmp / "vix-paused-replicas"
        self.marker.write_text("3\n")   # stranded by an earlier run

    def _run(self, env: dict[str, str]) -> subprocess.CompletedProcess:
        e = {"PATH": f"{self.tmp / 'bin'}:{os.environ['PATH']}", "PERMITTED_SHA_GUARD": "PASSED"}  # first guard DID pass
        e.update(env)
        return subprocess.run(["bash", str(RECOVERY), str(self.marker)], capture_output=True, text=True, env=e)

    def test_second_guard_rejected_never_touches_the_cluster_and_keeps_the_marker(self) -> None:
        r = self._run({})
        self.assertEqual(r.returncode, 0)
        self.assertFalse(self.sentinel.exists(), "kubectl was invoked after a deploy-workspace refusal")
        self.assertTrue(self.marker.exists(), "the stranded marker must be preserved")
        self.assertIn("HUMAN REQUIRED", r.stderr)
        self.assertIn("replicas=3", r.stderr)

    def test_flag_value_must_be_exactly_passed(self) -> None:
        for bad in ("passed", "PASSED ", "true", "1"):
            r = self._run({"DEPLOY_WORKSPACE_PERMITTED": bad})
            self.assertFalse(self.sentinel.exists(), bad)
            self.assertEqual(r.returncode, 0)

    def test_permitted_deploy_workspace_runs_the_recovery(self) -> None:
        r = self._run({"DEPLOY_WORKSPACE_PERMITTED": "PASSED"})
        self.assertTrue(self.sentinel.exists(), r.stdout + r.stderr)

    def test_no_marker_nothing_to_do(self) -> None:
        self.marker.unlink()
        r = self._run({})
        self.assertEqual(r.returncode, 0)
        self.assertFalse(self.sentinel.exists())
        self.assertIn("nothing touched", r.stdout)


class BindRequiredImageTest(unittest.TestCase):
    """The permitted child build's digest wins over a mutable tag that moved (Codex I4)."""

    OTHER = "sha256:" + "d" * 64

    def _run(self, env: dict[str, str], served: str | None) -> subprocess.CompletedProcess:
        serve = f"printf '%s\\n' '{served}'" if served else "return 1"
        script = f"""
          resolve_repo_digest() {{ echo "resolve $1 $2 $3" >&2; {serve}; }}
          . '{BIND}'
          bind_required_image
        """
        e = {"PATH": os.environ["PATH"], "PINNED_IMAGE": f"192.168.100.252:5000/options-edge-es-cvd:prod@{self.OTHER}",
             "MUTABLE_IMAGE": "192.168.100.252:5000/options-edge-es-cvd:prod", "PIN_IS_AUTHORITATIVE": "false",
             "DEPLOY_PLATFORM": "linux/amd64"}
        e.update(env)
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=e)

    def test_no_required_image_keeps_the_tag_pin(self) -> None:
        r = self._run({}, DIGEST)
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout.strip(), f"192.168.100.252:5000/options-edge-es-cvd:prod@{self.OTHER}")

    def test_moved_tag_is_replaced_by_the_permitted_digest(self) -> None:
        r = self._run({"REQUIRED_IMAGE": f"192.168.100.252:5000/options-edge-es-cvd:prod-7-aaaa@{DIGEST}"}, DIGEST)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), f"192.168.100.252:5000/options-edge-es-cvd:prod@{DIGEST}")
        self.assertIn("the tag moved after the permitted build", r.stderr)
        self.assertIn(f"resolve 192.168.100.252:5000 options-edge-es-cvd {DIGEST}", r.stderr)

    def test_wrong_repository_is_refused(self) -> None:
        r = self._run({"REQUIRED_IMAGE": f"192.168.100.252:5000/options-edge-vol-premium:prod-7@{DIGEST}"}, DIGEST)
        self.assertEqual(r.returncode, 1)
        self.assertIn("is not this service's image", r.stderr)

    def test_authoritative_render_that_disagrees_is_refused(self) -> None:
        r = self._run({"REQUIRED_IMAGE": f"x/options-edge-es-cvd:t@{DIGEST}", "PIN_IS_AUTHORITATIVE": "true"}, DIGEST)
        self.assertEqual(r.returncode, 1)
        self.assertIn("the render pins", r.stderr)

    def test_digest_the_registry_does_not_serve_is_refused(self) -> None:
        r = self._run({"REQUIRED_IMAGE": f"x/options-edge-es-cvd:t@{DIGEST}"}, None)
        self.assertEqual(r.returncode, 1)
        self.assertIn("does not serve", r.stderr)
        r = self._run({"REQUIRED_IMAGE": f"x/options-edge-es-cvd:t@{DIGEST}"}, self.OTHER)
        self.assertEqual(r.returncode, 1)
        self.assertIn("the registry answered", r.stderr)

    def test_malformed_digest_is_refused(self) -> None:
        r = self._run({"REQUIRED_IMAGE": "x/options-edge-es-cvd:t@sha256:short"}, DIGEST)
        self.assertEqual(r.returncode, 1)
        self.assertIn("no valid digest", r.stderr)


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
        mutated = transform(good)
        self.assertNotEqual(mutated, good, "the mutation did not apply")
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        (tmp / "Jenkinsfile.fixture-good").write_text(mutated)
        for name, body in (extra_files or {}).items():
            (tmp / name).write_text(body)
        (tmp / "scope.txt").write_text(manifest_line or (FIXTURES / "good/scope.txt").read_text())
        return run_validator(tmp, tmp / "scope.txt")

    def _refused(self, transform, message: str, manifest_line: str | None = None) -> None:
        r = self._mutated(transform, manifest_line=manifest_line)
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn(message, r.stdout + r.stderr)

    def test_mutation_missing_parameter(self) -> None:
        self._refused(lambda s: s.replace("    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')\n", ""),
                      "lacks string(name: 'PERMITTED_SHA'")

    def test_mutation_parameter_without_trim_or_with_default(self) -> None:
        r = self._mutated(lambda s: s.replace("defaultValue: '', trim: true, description: 'the permitted commit'", "defaultValue: 'main', description: 'x'"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("defaultValue: ''", r.stdout)
        self.assertIn("trim: true", r.stdout)

    def test_mutation_restart_from_stage_enabled(self) -> None:
        self._refused(lambda s: s.replace("disableRestartFromStage(); ", ""), "disableRestartFromStage()")

    def test_mutation_guard_stage_missing(self) -> None:
        self._refused(lambda s: s.replace("stage('Permitted commit guard')", "stage('Preflight')"), "no stage named 'Permitted commit guard'")

    def test_mutation_inverted_predicate_rc_equals_zero(self) -> None:
        # Codex round-1 counterexample: accepted by the old validator; a refused SHA would proceed.
        self._refused(lambda s: s.replace("if (rc != 0) {", "if (rc == 0) {"), "not the canonical guard")

    def test_mutation_guard_wrapped_in_catch_error(self) -> None:
        self._refused(lambda s: s.replace("          if (rc != 0) {\n            error(", "          catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {\n            error("),
                      "not the canonical guard")

    def test_mutation_guard_does_not_error(self) -> None:
        self._refused(lambda s: s.replace("error(\"Permitted commit guard REFUSED this build (rc=${rc})\")", "echo \"guard said ${rc}\""), "not the canonical guard")

    def test_mutation_guard_calls_something_else(self) -> None:
        self._refused(lambda s: s.replace("bash scripts/jenkins/permitted-sha-guard.sh", "bash scripts/jenkins/enforce-main-branch.sh"), "not the canonical guard")

    def test_mutation_guard_does_not_set_flag_or_sets_the_wrong_one(self) -> None:
        self._refused(lambda s: s.replace("          env.PERMITTED_SHA_GUARD = 'PASSED'\n", ""), "not the canonical guard")
        self._refused(lambda s: s.replace("env.PERMITTED_SHA_GUARD = 'PASSED'", "env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'"), "must set env.PERMITTED_SHA_GUARD")

    def test_mutation_extra_statement_inside_the_guard(self) -> None:
        # A statement smuggled into the guard stage (e.g. a deploy before the check) is refused.
        self._refused(lambda s: s.replace("          def rc = sh(returnStatus", "          sh 'kubectl apply -f early.yaml'\n          def rc = sh(returnStatus"), "not the canonical guard")

    def test_mutation_unlisted_stage_before_guard(self) -> None:
        self._refused(lambda s: s.replace("  stages {\n", "  stages {\n    stage('Warm up') { steps { echo 'hi' } }\n"),
                      "stage 'Warm up' precedes the guard but is not in the manifest's before= set")

    def test_listed_stage_before_guard_is_allowed_but_its_effects_are_not(self) -> None:
        line = "Jenkinsfile.fixture-good | in | before=Refuse anything but main | fixture\n"
        ok = self._mutated(lambda s: s.replace("  stages {\n", "  stages {\n    stage('Refuse anything but main') { steps { sh 'git fetch origin main' } }\n"), manifest_line=line)
        self.assertEqual(ok.returncode, 0, ok.stdout)
        self._refused(lambda s: s.replace("  stages {\n", "  stages {\n    stage('Refuse anything but main') { steps { sh 'docker push x' } }\n"),
                      "mutation token before the guard", manifest_line=line)

    def test_mutation_downstream_trigger_forms(self) -> None:
        trig = "        build job: 'other-job', parameters: [string(name: 'X', value: 'y')]\n"
        self._refused(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", trig), "does not forward PERMITTED_SHA")
        fwd = "        build job: 'other-job', parameters: [string(name: 'PERMITTED_SHA', value: params.PERMITTED_SHA)]\n"
        self._refused(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", fwd), "is not preceded in its stage by require-guarded-downstream.sh other-job")
        # Codex round-1 counterexample: an annotation must not exempt an unenforced trigger.
        self._refused(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", "        // UNBOUND-DOWNSTREAM: other-job has no guard yet\n" + trig), "UNBOUND-DOWNSTREAM annotation is not a gate")
        compat = ("        script { def c = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh other-job'); if (c != 0) { error('unguarded') } }\n")
        ok = self._mutated(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", compat + fwd))
        self.assertEqual(ok.returncode, 0, ok.stdout)
        wrong_job = compat.replace("other-job", "another-job") + fwd
        self._refused(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", wrong_job), "is not preceded in its stage by require-guarded-downstream.sh other-job")

    def test_mutation_post_mutates_without_a_real_gate(self) -> None:
        self._refused(lambda s: s.replace("    always { echo 'fixture done' }", "    always { sh 'kubectl delete job old' }"), "without an `if (env.PERMITTED_SHA_GUARD == 'PASSED')` gate")
        # Codex round-1 counterexample: the flag's NAME in an echo is not a gate.
        self._refused(lambda s: s.replace("    always { echo 'fixture done' }", "    always { echo 'PERMITTED_SHA_GUARD'; sh 'kubectl delete job old' }"), "without an `if (env.PERMITTED_SHA_GUARD == 'PASSED')` gate")
        self._refused(lambda s: s.replace("    always { echo 'fixture done' }", "    always { script { if (env.PERMITTED_SHA_GUARD == 'PASSED' || params.FORCE) { sh 'kubectl delete job old' } } }"), "without an `if (env.PERMITTED_SHA_GUARD == 'PASSED')` gate")
        self._refused(lambda s: s.replace("    always { echo 'fixture done' }", "    always { script { if (env.PERMITTED_SHA_GUARD != 'PASSED') { echo 'no' } else { sh 'kubectl delete job old' } } }"), "without an `if (env.PERMITTED_SHA_GUARD == 'PASSED')` gate")
        ok = self._mutated(lambda s: s.replace("    always { echo 'fixture done' }", "    always { script { if (env.PERMITTED_SHA_GUARD == 'PASSED') { sh 'kubectl delete job old' } } }"))
        self.assertEqual(ok.returncode, 0, ok.stdout)
        ok2 = self._mutated(lambda s: s.replace("    always { echo 'fixture done' }", "    always { script { if (env.STAGED == 'true' && env.PERMITTED_SHA_GUARD == 'PASSED') { sh 'ssh host rm -rf x' } } }"))
        self.assertEqual(ok2.returncode, 0, ok2.stdout)

    def test_mutation_source_acquired_after_the_guard(self) -> None:
        # Codex round-1 counterexample: a later pull can switch the revision that gets deployed.
        self._refused(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", "        sh 'git pull origin main'\n        sh 'kubectl apply -f k8s/fixture.yaml'\n"),
                      "source acquired after the guard is not re-bound")
        self._refused(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", "        dir('app') { git url: 'x', branch: 'main' }\n        sh 'docker build app'\n"),
                      "source acquired after the guard is not re-bound")
        rebound = ("        dir('app') { git url: 'x', branch: 'main' }\n"
                   "        script { def r2 = sh(returnStatus: true, script: 'PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir app --ref main'); if (r2 != 0) { error('app') } }\n"
                   "        sh 'docker build app'\n")
        ok = self._mutated(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", rebound))
        self.assertEqual(ok.returncode, 0, ok.stdout)

    def test_mutation_unclassified_jenkinsfile(self) -> None:
        r = self._mutated(lambda s: s + "\n", extra_files={"Jenkinsfile.fixture-stray": "pipeline { agent any stages { stage('x') { steps { echo 'x' } } } }\n"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("Jenkinsfile.fixture-stray: not classified", r.stdout)

    def test_mutation_reguard_missing_and_reguard_flag_rules(self) -> None:
        line = "Jenkinsfile.fixture-good | in | reguard=Deploy | fixture\n"
        self._refused(lambda s: s + "\n", "must be immediately followed by 'Permitted commit guard (deploy workspace)'", manifest_line=line)
        # The re-guard flag may be set only by the re-guard stage.
        self._refused(lambda s: s.replace("        sh 'kubectl apply -f k8s/fixture.yaml'\n", "        script { env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED' }\n        sh 'kubectl apply -f k8s/fixture.yaml'\n"),
                      "env.DEPLOY_WORKSPACE_PERMITTED may be set only by the 'Permitted commit guard (deploy workspace)' stage")

    def test_out_of_scope_needs_a_reason(self) -> None:
        r = self._mutated(lambda s: s + "\n", manifest_line="Jenkinsfile.fixture-good | out | |\n")
        self.assertEqual(r.returncode, 1)
        self.assertIn("needs a reason", r.stdout + r.stderr)


class ServiceDeployBindingTest(unittest.TestCase):
    """service-deploy's specifics that the generic validator does not model."""

    def setUp(self) -> None:
        self.text = (ROOT / "Jenkinsfile.service-deploy").read_text()

    def test_intent_ledger_records_the_permitted_commits_and_the_image(self) -> None:
        self.assertIn('"permittedSha":os.environ.get("PERMITTED_SHA","")', self.text)
        self.assertIn('"checkedOutSha":subprocess.check_output(["git","rev-parse","HEAD"])', self.text)
        for k in ("requiredImage", "imageBuild", "processingPermittedSha", "contractsPermittedSha"):
            self.assertIn(f'rec["{k}"]=', self.text)

    def test_image_build_trigger_is_bound_and_its_image_retained(self) -> None:
        for p in ("PROCESSING_PERMITTED_SHA", "CONTRACTS_PERMITTED_SHA"):
            self.assertIn(f"string(name: '{p}', defaultValue: '', trim: true", self.text)
        self.assertRegex(self.text, r"psha\.matches\('\^\[0-9a-f\]\{40\}\$'\)")
        self.assertRegex(self.text, r"csha\.matches\('\^\[0-9a-f\]\{40\}\$'\)")
        self.assertIn("require-guarded-downstream.sh options-edge-processing", self.text)
        self.assertIn("string(name: 'PERMITTED_SHA',           value: psha)", self.text)
        self.assertIn("string(name: 'CONTRACTS_PERMITTED_SHA', value: csha)", self.text)
        self.assertIn("def child = build job: 'options-edge-processing'", self.text)
        self.assertIn("fetch-permitted-image-lock.sh options-edge-processing", self.text)
        self.assertIn("env.REQUIRED_IMAGE = sh(returnStdout: true", self.text)
        # The checks precede the trigger; the lock read follows it and precedes the deploy stage.
        i_check, i_compat, i_trig = (self.text.index(x) for x in ("psha.matches(", "require-guarded-downstream.sh options-edge-processing", "build job: 'options-edge-processing'"))
        self.assertLess(i_check, i_compat)
        self.assertLess(i_compat, i_trig)
        self.assertLess(i_trig, self.text.index("fetch-permitted-image-lock.sh"))
        self.assertLess(self.text.index("fetch-permitted-image-lock.sh"), self.text.index("stage('Deploy (service-scoped)')"))
        # service-deploy.sh consumes REQUIRED_IMAGE through the tested binder.
        sd = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        self.assertIn(". scripts/deploy/bind-required-image.sh", sd)
        self.assertIn('PINNED_IMAGE="$(bind_required_image)" ||', sd)

    def test_guard_runs_before_the_intent_claim_and_again_on_the_deploy_host_with_its_own_flag(self) -> None:
        stages = re.findall(r"^\s*stage\('([^']*)'", self.text, re.M)
        self.assertEqual(stages[0], "Permitted commit guard")
        self.assertEqual(stages[1], "Intent dedup")
        self.assertEqual(stages[stages.index("Deploy path") + 1], "Permitted commit guard (deploy workspace)")
        self.assertEqual(self.text.count("env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'"), 1)
        # post{} recovery goes through the gate script, never straight to vix-unpause.sh.
        post = self.text[self.text.index("      post {"):]
        self.assertIn("post-deploy-recovery.sh", post)
        self.assertNotIn("vix-unpause.sh", "\n".join(l for l in post.splitlines() if not l.strip().startswith("//")))


class WebServiceBindingTest(unittest.TestCase):
    def test_web_image_build_path_is_fail_closed(self) -> None:
        t = (ROOT / "Jenkinsfile.web-service").read_text()
        self.assertIn("booleanParam(name: 'BUILD_IMAGE', defaultValue: false", t)
        self.assertIn("string(name: 'WEB_PERMITTED_SHA', defaultValue: '', trim: true", t)
        self.assertRegex(t, r"wsha\.matches\('\^\[0-9a-f\]\{40\}\$'\)")
        self.assertIn("require-guarded-downstream.sh options-edge-web-deploy", t)
        self.assertLess(t.index("require-guarded-downstream.sh options-edge-web-deploy"), t.index("build job: 'options-edge-web-deploy'"))
        self.assertIn("string(name: 'PERMITTED_SHA',                value: params.WEB_PERMITTED_SHA.trim())", t)
        self.assertNotIn("UNBOUND", t)


class DisabledUmbrellaTest(unittest.TestCase):
    def test_umbrella_and_unguarded_child_triggers_refuse_before_triggering(self) -> None:
        bua = (ROOT / "Jenkinsfile.bring-up-all").read_text()
        stages = re.findall(r"^\s*stage\('([^']*)'", bua, re.M)
        self.assertTrue(stages[0].startswith("Refused: bring-up-all is disabled"))
        first_trigger = next(i for i, l in enumerate(bua.split("\n")) if "build job:" in l and not l.strip().startswith("//"))
        refusal_line = next(i for i, l in enumerate(bua.split("\n")) if "error('bring-up-all is DISABLED" in l)
        self.assertLess(refusal_line, first_trigger)
        rewind = (ROOT / "Jenkinsfile.databento-rewind-deploy").read_text()
        self.assertLess(rewind.index("error('REFUSED: the upstream rewind image build is unguarded"), rewind.index("job: 'options-edge-databento-rewind-image-build'"))
        pge = (ROOT / "Jenkinsfile.public-gate-evidence").read_text()
        self.assertLess(pge.index("error('REFUSED: BUILD_IMAGE=true would trigger options-edge-web-deploy"), pge.index("build job: 'options-edge-web-deploy'"))


if __name__ == "__main__":
    unittest.main()
