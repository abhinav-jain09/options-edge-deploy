"""The Jenkins-side permitted-commit guard (Deployment Permission Rule, options-edge rule.md).

Under test, each driven through its refusal cases, not just its happy path — and every script the
guards call is EXECUTED here (against throwaway git repositories, a local HTTP stand-in for the
controller, a sentinel kubectl, a stub registry resolver), because a textual validator cannot show
that a script refuses:

  * scripts/jenkins/permitted-sha-guard.sh — 46-case bash suite (shared byte-for-byte with the
    gateway, processing and web repositories): guard version unset/mismatched; missing / empty /
    whitespace / short / uppercase / non-SHA values; permitted A with checkout B; the branch advancing
    between queue and checkout; a forbidden ref via GIT_BRANCH, BRANCH_NAME, their contradiction, or a
    selected --ref (aliases such as origin/main refused); an unreachable origin; a bare repository /
    .git directory; a nested checkout.
  * scripts/jenkins/require-guarded-downstream.sh — refuses a child whose live definition lacks
    PERMITTED_SHA, whose PERMITTED_SHA_GUARD_VERSION default is another hash (an un-activated or
    older child), whose parameters are not strings, which lacks a required extra parameter, an
    unreachable controller, a folder-relative child resolved the way `build job:` resolves it.
  * scripts/jenkins/fetch-permitted-image-lock.sh — yields the digest for the permitted commit;
    refuses a lock from another source or contracts commit, a missing image, an unreachable build.
  * scripts/deploy/post-deploy-recovery.sh — the recovery never reaches kubectl unless ALL FOUR flags
    of THIS build are PASSED (each rejection point after the guards leaves one unset), even when an
    earlier build stranded a marker (Codex I1/I5).
  * scripts/deploy/bind-required-image.sh — the permitted build's digest replaces a moved tag; a wrong
    repository, a disagreeing authoritative render, an unserved digest are refused; a
    registry:port/repo@digest render (Codex I7) is accepted.
  * scripts/jenkins/validate-jenkinsfile-guard.py — the repository passes; the committed refused
    fixture (apply-before-guard) fails; the shared 56-case mutation suite (validate-jenkinsfile-guard-test.py)
    refuses every demonstrated bypass.
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
J = ROOT / "scripts/jenkins"
GUARD = J / "permitted-sha-guard.sh"
GUARD_SUITE = J / "permitted-sha-guard-test.sh"
VERSION = J / "permitted-sha-guard-version.sh"
COMPAT = J / "require-guarded-downstream.sh"
FETCH_LOCK = J / "fetch-permitted-image-lock.sh"
VALIDATOR = J / "validate-jenkinsfile-guard.py"
VALIDATOR_SUITE = J / "validate-jenkinsfile-guard-test.py"
RECOVERY = ROOT / "scripts/deploy/post-deploy-recovery.sh"
BIND = ROOT / "scripts/deploy/bind-required-image.sh"
FIXTURES = ROOT / "tests/fixtures/permitted-sha-guard"
SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40
DIGEST = "sha256:" + "d" * 64
OWN_HASH = subprocess.run(["bash", str(VERSION)], capture_output=True, text=True, check=True).stdout.strip()


def run_validator(root: Path, manifest: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["python3", str(VALIDATOR), "--root", str(root), "--manifest", str(manifest)], capture_output=True, text=True)


class _Server:
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

            def log_message(self, *a):
                pass

        self.httpd = socketserver.TCPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def sh(script: Path, args: list[str], env: dict[str, str], cwd: Path = ROOT) -> subprocess.CompletedProcess:
    full = {"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/tmp")}
    full.update(env)
    return subprocess.run(["bash", str(script), *args], capture_output=True, text=True, env=full, cwd=cwd)


class PermittedShaGuardScriptTest(unittest.TestCase):
    def test_guard_script_parses(self) -> None:
        subprocess.run(["bash", "-n", str(GUARD)], check=True)

    def test_guard_refuses_every_unpermitted_case_and_permits_the_exact_commit(self) -> None:
        r = subprocess.run(["bash", str(GUARD_SUITE)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("permitted-sha-guard-test: ALL PASS", r.stdout)
        self.assertNotIn("FAIL [", r.stdout)
        for case in [
            "guard version unset", "guard version mismatch", "PERMITTED_SHA unset", "PERMITTED_SHA empty",
            "short SHA (12)", "uppercase SHA", "permitted A, checkout B", "branch advanced B->D, permitted B",
            "feature commit C, permitted C", "BRANCH_NAME=main does NOT mask GIT_BRANCH=origin/feature",
            "--ref feature at a merged commit", "--ref origin/main is an alias, refused",
            "--ref refs/heads/main is an alias, refused", ".git metadata dir refused", "bare repository refused",
            "origin unreachable", "nested at B, permitted D", "nested --ref feature at a merged commit",
        ]:
            self.assertIn(f"ok   [{case}]", r.stdout)

    def test_guard_never_substitutes_a_value(self) -> None:
        code = "\n".join(l for l in GUARD.read_text().splitlines() if not l.lstrip().startswith("#"))
        self.assertNotIn("${PERMITTED_SHA:-", code)
        self.assertNotIn("GIT_COMMIT", code)
        for forbidden in ("catchError", "|| true"):
            self.assertNotIn(forbidden, code)
        self.assertIn('[ "$inside" = "true" ]', code)
        self.assertIn('[ "$declared" = "$own_version" ]', code)
        self.assertIn('[ "$ref" = "$branch" ]', code)   # a selected ref is the literal name, never an alias


class RequireGuardedDownstreamTest(unittest.TestCase):
    TREE = "?tree=property[parameterDefinitions[name,type,defaultParameterValue[value]]]"

    def _defs(self, params: list[tuple[str, str, str]]) -> str:
        return json.dumps({"property": [{"_class": "x"}, {"parameterDefinitions": [
            {"name": n, "type": t, "defaultParameterValue": {"value": d}} for n, t, d in params]}]})

    def _good(self, extra=()):
        return self._defs([("PERMITTED_SHA", "StringParameterDefinition", ""),
                           ("PERMITTED_SHA_GUARD_VERSION", "StringParameterDefinition", OWN_HASH)] + [(e, "StringParameterDefinition", "") for e in extra])

    def _run(self, routes, args, extra_env=None):
        s = _Server(routes)
        self.addCleanup(s.close)
        env = {"JENKINS_URL": s.url + "/"}
        env.update(extra_env or {})
        return sh(COMPAT, args, env)

    def test_child_enforcing_this_guard_is_accepted(self) -> None:
        r = self._run({"/job/child/api/json" + self.TREE: (200, self._good())}, ["child"])
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"enforces guard {OWN_HASH}", r.stdout)

    def test_parameter_name_alone_is_not_enforcement(self) -> None:
        # Codex N2/I2: a child that only declares PERMITTED_SHA (round-1 definition, or a hand-made
        # parameter) is refused — it does not declare which guard it runs.
        r = self._run({"/job/child/api/json" + self.TREE: (200, self._defs([("PERMITTED_SHA", "StringParameterDefinition", "")]))}, ["child"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("PERMITTED_SHA_GUARD_VERSION is not a registered string parameter", r.stderr)

    def test_other_guard_version_is_refused(self) -> None:
        r = self._run({"/job/child/api/json" + self.TREE: (200, self._defs([("PERMITTED_SHA", "StringParameterDefinition", ""), ("PERMITTED_SHA_GUARD_VERSION", "StringParameterDefinition", "0" * 64)]))}, ["child"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("the child last ran a different (older, newer or absent) guard", r.stderr)

    def test_wrong_parameter_type_is_refused(self) -> None:
        r = self._run({"/job/child/api/json" + self.TREE: (200, self._defs([("PERMITTED_SHA", "BooleanParameterDefinition", ""), ("PERMITTED_SHA_GUARD_VERSION", "StringParameterDefinition", OWN_HASH)]))}, ["child"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("PERMITTED_SHA is not a registered string parameter", r.stderr)

    def test_required_extra_parameter(self) -> None:
        r = self._run({"/job/child/api/json" + self.TREE: (200, self._good())}, ["child", "CONTRACTS_PERMITTED_SHA"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("CONTRACTS_PERMITTED_SHA is not a registered string parameter", r.stderr)
        r = self._run({"/job/child/api/json" + self.TREE: (200, self._good(["CONTRACTS_PERMITTED_SHA"]))}, ["child", "CONTRACTS_PERMITTED_SHA"])
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_child_is_resolved_relative_to_the_callers_folder(self) -> None:
        # Codex gateway I2: `build job: 'child'` from folder/caller schedules folder/child — so that is
        # what must be inspected, not the root job of the same name.
        routes = {"/job/child/api/json" + self.TREE: (200, self._good()),
                  "/job/folder/job/child/api/json" + self.TREE: (200, self._defs([("ENVIRONMENT", "StringParameterDefinition", "")]))}
        r = self._run(routes, ["child"], {"JOB_NAME": "folder/caller"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("'folder/child' does not enforce this guard", r.stderr)
        r = self._run(routes, ["child"], {"JOB_NAME": "caller"})
        self.assertEqual(r.returncode, 0, r.stderr)
        r = self._run(routes, ["folder/child"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("must be a simple job name", r.stderr)

    def test_unreachable_login_page_and_missing_url_are_refused(self) -> None:
        r = self._run({}, ["child"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("could not read the definition", r.stderr)
        r = self._run({"/job/child/api/json" + self.TREE: (200, "<html>login</html>")}, ["child"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("not JSON", r.stderr)
        r = sh(COMPAT, ["child"], {})
        self.assertEqual(r.returncode, 1)
        self.assertIn("JENKINS_URL is not set", r.stderr)


class FetchPermittedImageLockTest(unittest.TestCase):
    LOCK = (
        "OPTIONS_EDGE_IMAGE_LOCK_FORMAT=1\n"
        f"OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT={SHA_A}\n"
        f"OPTIONS_EDGE_IMAGE_LOCK_CONTRACTS_GIT_COMMIT={SHA_C}\n"
        f"OPTIONS_EDGE_IMAGE_LOCK_GUARD_VERSION={OWN_HASH}\n"
        f"ES_CVD_IMAGE=192.168.100.252:5000/options-edge-es-cvd:prod-7-aaaaaaaaaaaa@{DIGEST}\n"
        f"ES_CVD_IMAGE_GIT_COMMIT={SHA_A}\n"
        f"INDICATOR_SERVICE_IMAGE=192.168.100.252:5000/options-edge-indicator-service:prod-7-aaaaaaaaaaaa@{DIGEST}\n"
        f"INDICATOR_SERVICE_IMAGE_GIT_COMMIT={SHA_B}\n"
    )
    PATH = "/job/options-edge-processing/7/artifact/.jenkins-tmp/options-edge-image-lock.env"

    def _serve(self, body=None):
        s = _Server({self.PATH: (200, body if body is not None else self.LOCK)})
        self.addCleanup(s.close)
        return s.url

    def test_permitted_commits_yield_the_digest_pinned_image(self) -> None:
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), f"192.168.100.252:5000/options-edge-es-cvd:prod-7-aaaaaaaaaaaa@{DIGEST}")
        self.assertIn(f"with contracts {SHA_C}", r.stderr)

    def test_other_source_or_contracts_commit_is_refused(self) -> None:
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_B, "options-edge-es-cvd"], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 1)
        self.assertIn(f"was built from {SHA_A}, not the permitted {SHA_B}", r.stderr)
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_B], {"JENKINS_URL": self._serve()})
        self.assertEqual(r.returncode, 1)
        self.assertIn(f"compiled contracts {SHA_C}, not the permitted {SHA_B}", r.stderr)
        no_contracts = self.LOCK.replace(f"OPTIONS_EDGE_IMAGE_LOCK_CONTRACTS_GIT_COMMIT={SHA_C}\n", "")
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C], {"JENKINS_URL": self._serve(no_contracts)})
        self.assertEqual(r.returncode, 1)
        self.assertIn("did not record the contracts", r.stderr)

    def test_per_image_mismatch_missing_image_unreachable_build(self) -> None:
        url = self._serve()
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-indicator-service"], {"JENKINS_URL": url})
        self.assertEqual(r.returncode, 1)
        self.assertIn("INDICATOR_SERVICE_IMAGE was built from", r.stderr)
        r = sh(FETCH_LOCK, ["options-edge-processing", "7", SHA_A, "options-edge-vol-premium"], {"JENKINS_URL": url})
        self.assertEqual(r.returncode, 1)
        self.assertIn("has 0 digest-pinned entries", r.stderr)
        r = sh(FETCH_LOCK, ["options-edge-processing", "8", SHA_A, "options-edge-es-cvd"], {"JENKINS_URL": url})
        self.assertEqual(r.returncode, 1)
        self.assertIn("could not read the image lock", r.stderr)
        for args, msg in [(["options-edge-processing", "x", SHA_A, "options-edge-es-cvd"], "is not a number"),
                          (["options-edge-processing", "7", SHA_A[:12], "options-edge-es-cvd"], "not a full commit id"),
                          (["options-edge-processing", "7", SHA_A, "options-edge-es-cvd; rm"], "not a plain image name")]:
            r = sh(FETCH_LOCK, args, {"JENKINS_URL": url})
            self.assertEqual(r.returncode, 1, args)
            self.assertIn(msg, r.stderr)


class PostDeployRecoveryTest(unittest.TestCase):
    """Codex I1/I5: an earlier build stranded a recovery marker; this build is refused at SOME point
    after the guards. post{always} must not scale anything unless every check passed AND the effect
    phase began in this build."""

    FLAGS = ["PERMITTED_SHA_GUARD", "DEPLOY_WORKSPACE_PERMITTED", "SECONDARY_PERMISSIONS_PASSED", "EFFECT_STAGE_STARTED"]

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.sentinel = self.tmp / "kubectl-was-called"
        fake = self.tmp / "bin" / "kubectl"
        fake.parent.mkdir()
        fake.write_text(f"#!/usr/bin/env bash\necho called >> '{self.sentinel}'\nexit 0\n")
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        self.marker = self.tmp / "vix-paused-replicas"
        self.marker.write_text("3\n")

    def _run(self, flags: dict[str, str]) -> subprocess.CompletedProcess:
        e = {"PATH": f"{self.tmp / 'bin'}:{os.environ['PATH']}"}
        e.update(flags)
        return subprocess.run(["bash", str(RECOVERY), str(self.marker)], capture_output=True, text=True, env=e)

    def test_every_rejection_point_leaves_the_recovery_inert(self) -> None:
        # Rejection points, in pipeline order, and the flags set BEFORE each of them:
        points = {
            "first guard refused": [],
            "deploy-workspace guard refused": ["PERMITTED_SHA_GUARD"],
            "PROCESSING/CONTRACTS SHA missing, child incompatible, child refused, lock mismatch": ["PERMITTED_SHA_GUARD", "DEPLOY_WORKSPACE_PERMITTED"],
            "refused before any effect stage began": ["PERMITTED_SHA_GUARD", "DEPLOY_WORKSPACE_PERMITTED", "SECONDARY_PERMISSIONS_PASSED"],
        }
        for name, set_flags in points.items():
            if self.sentinel.exists():
                self.sentinel.unlink()
            r = self._run({f: "PASSED" for f in set_flags})
            self.assertEqual(r.returncode, 0, name)
            self.assertFalse(self.sentinel.exists(), f"kubectl reached after: {name}")
            self.assertTrue(self.marker.exists(), name)
            self.assertIn("HUMAN REQUIRED", r.stderr, name)
            self.assertIn("replicas=3", r.stderr, name)

    def test_flag_values_must_be_exactly_passed(self) -> None:
        for bad in ("passed", "PASSED ", "true", "1"):
            flags = {f: "PASSED" for f in self.FLAGS}
            flags["EFFECT_STAGE_STARTED"] = bad
            self._run(flags)
            self.assertFalse(self.sentinel.exists(), bad)

    def test_all_four_flags_run_the_recovery(self) -> None:
        r = self._run({f: "PASSED" for f in self.FLAGS})
        self.assertTrue(self.sentinel.exists(), r.stdout + r.stderr)

    def test_no_marker_nothing_to_do(self) -> None:
        self.marker.unlink()
        r = self._run({})
        self.assertEqual(r.returncode, 0)
        self.assertFalse(self.sentinel.exists())
        self.assertIn("nothing touched", r.stdout)


class BindRequiredImageTest(unittest.TestCase):
    OTHER = "sha256:" + "e" * 64

    def _run(self, env: dict[str, str], served: str | None) -> subprocess.CompletedProcess:
        serve = f"printf '%s\\n' '{served}'" if served else "return 1"
        script = f"""
          resolve_repo_digest() {{ echo "resolve $1 $2 $3" >&2; {serve}; }}
          . '{BIND}'
          bind_required_image
        """
        e = {"PATH": os.environ["PATH"], "PINNED_IMAGE": f"192.168.100.252:5000/options-edge-es-cvd:prod@{self.OTHER}",
             "MUTABLE_IMAGE": "192.168.100.252:5000/options-edge-es-cvd:prod", "PIN_IS_AUTHORITATIVE": "false", "DEPLOY_PLATFORM": "linux/amd64"}
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

    def test_authoritative_registry_port_digest_render_is_accepted(self) -> None:
        # Codex I7: `registry:5000/repo@sha256:…` has a port colon and no tag; the repository must not
        # be read as the bare registry host.
        r = self._run({"REQUIRED_IMAGE": f"192.168.100.252:5000/options-edge-es-cvd@{DIGEST}",
                       "PINNED_IMAGE": f"192.168.100.252:5000/options-edge-es-cvd@{DIGEST}", "PIN_IS_AUTHORITATIVE": "true"}, DIGEST)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), f"192.168.100.252:5000/options-edge-es-cvd@{DIGEST}")

    def test_wrong_repository_authoritative_disagreement_and_unserved_digest_are_refused(self) -> None:
        r = self._run({"REQUIRED_IMAGE": f"192.168.100.252:5000/options-edge-vol-premium:prod-7@{DIGEST}"}, DIGEST)
        self.assertEqual(r.returncode, 1)
        self.assertIn("is not this service's image", r.stderr)
        r = self._run({"REQUIRED_IMAGE": f"x/options-edge-es-cvd:t@{DIGEST}", "PIN_IS_AUTHORITATIVE": "true"}, DIGEST)
        self.assertEqual(r.returncode, 1)
        self.assertIn("the render pins", r.stderr)
        r = self._run({"REQUIRED_IMAGE": f"x/options-edge-es-cvd:t@{DIGEST}"}, None)
        self.assertEqual(r.returncode, 1)
        self.assertIn("does not serve", r.stderr)
        r = self._run({"REQUIRED_IMAGE": f"x/options-edge-es-cvd:t@{DIGEST}"}, self.OTHER)
        self.assertEqual(r.returncode, 1)
        self.assertIn("the registry answered", r.stderr)
        r = self._run({"REQUIRED_IMAGE": "x/options-edge-es-cvd:t@sha256:short"}, DIGEST)
        self.assertEqual(r.returncode, 1)
        self.assertIn("no valid digest", r.stderr)


class PermittedShaGuardValidatorTest(unittest.TestCase):
    def test_repository_passes(self) -> None:
        r = subprocess.run(["bash", str(ROOT / "scripts/ci/validate-jenkins-permitted-sha-guard.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("carry the canonical permitted-commit guard", r.stdout)

    def test_every_jenkinsfile_is_classified(self) -> None:
        manifest = (ROOT / "scripts/ci/jenkins-permitted-sha-scope.txt").read_text()
        listed = {l.split("|")[0].strip() for l in manifest.splitlines() if l.strip() and not l.startswith("#")}
        present = {p.name for p in ROOT.glob("Jenkinsfile*") if p.is_file()}
        self.assertEqual(listed, present)

    def _fixture_root(self, name: str) -> Path:
        # The fixtures carry the guard version placeholder; materialise them with the real script.
        src = FIXTURES / name
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        (tmp / "scripts/jenkins").mkdir(parents=True)
        shutil.copy(GUARD, tmp / "scripts/jenkins/permitted-sha-guard.sh")
        for f in src.iterdir():
            (tmp / f.name).write_text(f.read_text().replace("__GUARD_VERSION__", OWN_HASH))
        return tmp

    def test_good_fixture_passes(self) -> None:
        d = self._fixture_root("good")
        r = run_validator(d, d / "scope.txt")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_committed_refused_fixture_apply_before_guard(self) -> None:
        d = self._fixture_root("refused")
        r = run_validator(d, d / "scope.txt")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("stage 'Deploy' precedes the guard", r.stdout)
        self.assertIn("mutation token before the guard", r.stdout)

    def test_shared_mutation_suite_refuses_every_demonstrated_bypass(self) -> None:
        r = subprocess.run(["python3", str(VALIDATOR_SUITE)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("validate-jenkinsfile-guard-test: ALL PASS", r.stdout)
        for case in ["primary guard inverted (rc == 0)", "primary guard || true in the shell string",
                     "post mutation after the gate block", "post mutation in the else branch", "post gate negated",
                     "compatibility check inverted", "compatibility check status discarded", "git pull rebound by an echo",
                     "separate-agent stage without inline re-guard", "shell contracts guard || true",
                     "contracts re-checked-out after its guard", "guard version default is another hash"]:
            self.assertIn(f"ok   [{case}]", r.stdout)


class ServiceDeployBindingTest(unittest.TestCase):
    """service-deploy's specifics that the generic validator does not model."""

    def setUp(self) -> None:
        self.text = (ROOT / "Jenkinsfile.service-deploy").read_text()
        self.stages = re.findall(r"^\s*stage\('([^']*)'", self.text, re.M)

    def test_intent_ledger_and_receipt_record_the_binding(self) -> None:
        for k in ("permittedSha", "checkedOutSha", "guardVersion", "requiredImage", "imageBuild", "processingPermittedSha", "contractsPermittedSha"):
            self.assertIn(f'"{k}"', self.text, k)
        self.assertIn("write-permission-receipt.sh", self.text)

    def test_image_build_trigger_is_bound_and_its_image_retained(self) -> None:
        for p in ("PROCESSING_PERMITTED_SHA", "CONTRACTS_PERMITTED_SHA", "REQUIRED_IMAGE"):
            self.assertIn(f"string(name: '{p}', defaultValue: '', trim: true", self.text)
        self.assertIn("require-guarded-downstream.sh options-edge-processing CONTRACTS_PERMITTED_SHA", self.text)
        self.assertIn("string(name: 'PERMITTED_SHA',           value: psha)", self.text)
        self.assertIn("string(name: 'CONTRACTS_PERMITTED_SHA', value: csha)", self.text)
        self.assertIn("def child = build job: 'options-edge-processing'", self.text)
        self.assertRegex(self.text, r"fetch-permitted-image-lock\.sh options-edge-processing \\\n\s+\"\$\{PROCESSING_BUILD_NUMBER:\?\}\" \"\$\{PROCESSING_PERMITTED_SHA:\?\}\" \"\$\{SERVICE_IMAGE_NAME:\?\}\" \"\$\{CONTRACTS_PERMITTED_SHA:\?\}\"")
        sd = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        self.assertIn(". scripts/deploy/bind-required-image.sh", sd)
        self.assertIn('PINNED_IMAGE="$(bind_required_image)" ||', sd)

    def test_secondary_permission_and_effect_flags_are_set_after_every_check_and_before_every_effect(self) -> None:
        s = self.stages
        self.assertEqual(s[0], "Permitted commit guard")
        self.assertEqual(s[s.index("Deploy path") + 1], "Permitted commit guard (deploy workspace)")
        self.assertEqual(s[s.index("Build image (this service only)") + 1], "Secondary permissions complete")
        # SECONDARY_PERMISSIONS_PASSED is set exactly once, after the child trigger and lock read.
        self.assertEqual(self.text.count("env.SECONDARY_PERMISSIONS_PASSED = 'PASSED'"), 1)
        self.assertLess(self.text.index("fetch-permitted-image-lock.sh"), self.text.index("env.SECONDARY_PERMISSIONS_PASSED = 'PASSED'"))
        # EFFECT_STAGE_STARTED is set at the top of the VIX reconcile (the pause) and of the Deploy stage.
        vix = self.text[self.text.index("stage('Reconcile VIX option intelligence current topic')"):self.text.index("stage('Deploy (service-scoped)')")]
        self.assertLess(vix.index("env.EFFECT_STAGE_STARTED = 'PASSED'"), vix.index("scale \"$DEP\" --replicas=0"))
        dep = self.text[self.text.index("stage('Deploy (service-scoped)')"):self.text.index("stage('Verify web UI (post-rollout)')")]
        self.assertLess(dep.index("env.EFFECT_STAGE_STARTED = 'PASSED'"), dep.index("service-deploy.sh"))
        self.assertEqual(self.text.count("env.EFFECT_STAGE_STARTED = 'PASSED'"), 2)
        # post{} runs vix-unpause.sh only inside a gate on all four flags; the else branch reports.
        post = "\n".join(l for l in self.text[self.text.rindex("      post {"):].splitlines() if not l.strip().startswith("//"))
        gate = re.search(r"if \((.*?)\) \{", post).group(1)
        for f in PostDeployRecoveryTest.FLAGS:
            self.assertIn(f"env.{f} == 'PASSED'", gate)
        self.assertNotIn("||", gate)
        self.assertLess(post.index(gate), post.index("vix-unpause.sh"))
        self.assertLess(post.index("vix-unpause.sh"), post.index("} else {"))
        self.assertIn("post-deploy-recovery.sh", post[post.index("} else {"):])


class WebServiceBindingTest(unittest.TestCase):
    def test_web_image_build_path_is_fail_closed_and_its_image_retained(self) -> None:
        t = (ROOT / "Jenkinsfile.web-service").read_text()
        self.assertIn("booleanParam(name: 'BUILD_IMAGE', defaultValue: false", t)
        self.assertIn("string(name: 'WEB_PERMITTED_SHA', defaultValue: '', trim: true", t)
        self.assertIn("require-guarded-downstream.sh options-edge-web-deploy", t)
        self.assertIn("def child = build job: 'options-edge-web-deploy'", t)
        self.assertRegex(t, r"fetch-permitted-image-lock\.sh options-edge-web-deploy \\\n\s+\"\$\{WEB_BUILD_NUMBER:\?\}\" \"\$\{WEB_PERMITTED_SHA:\?\}\" options-edge-web")
        self.assertIn("write-permission-receipt.sh", t)
        self.assertNotIn("UNBOUND", t)


class DisabledUmbrellaTest(unittest.TestCase):
    def test_umbrella_and_unguarded_child_triggers_refuse_before_triggering(self) -> None:
        bua = (ROOT / "Jenkinsfile.bring-up-all").read_text()
        first_trigger = next(i for i, l in enumerate(bua.split("\n")) if "build job:" in l and not l.strip().startswith("//"))
        refusal_line = next(i for i, l in enumerate(bua.split("\n")) if "error('bring-up-all is DISABLED" in l)
        self.assertLess(refusal_line, first_trigger)
        rewind = (ROOT / "Jenkinsfile.databento-rewind-deploy").read_text()
        self.assertLess(rewind.index("error('REFUSED: the upstream rewind image build is unguarded"), rewind.index("job: 'options-edge-databento-rewind-image-build'"))
        pge = (ROOT / "Jenkinsfile.public-gate-evidence").read_text()
        self.assertLess(pge.index("error('REFUSED: BUILD_IMAGE=true would trigger options-edge-web-deploy"), pge.index("build job: 'options-edge-web-deploy'"))


if __name__ == "__main__":
    unittest.main()
