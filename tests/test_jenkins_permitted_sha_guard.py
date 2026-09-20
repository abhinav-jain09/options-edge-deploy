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
  * scripts/jenkins/require-guarded-downstream.sh — the shared executed suite (require-guarded-downstream-test.py):
    a child is accepted only when its config.xml loads the expected Jenkinsfile from the expected repository's
    */main, the forwarded SHA is main's tip, and the Jenkinsfile at that commit passes the validator with a
    guard hashing to ours; a declaration-only child (matching registered parameters, no guard executed) is
    refused (Codex N2).
  * scripts/jenkins/fetch-permitted-image-lock.sh — yields the digest for the permitted commit; the child is
    resolved by the same folder-relative resolver as the definition check, and the lock must name that very
    build (BUILD_URL, BUILD_ID, the returned child's absoluteUrl): absent-root and conflicting-root folder
    cases refused (Codex I9).
  * scripts/deploy/post-deploy-recovery.sh + vix-pause-marker.sh — recovery acts only on a marker THIS build
    wrote (service, job, BUILD_ID, permitted SHA), after all four flags, never in a dry run: a stranded
    unrelated marker, the web dry-run reproduction, a legacy marker, another job/permission all leave a
    sentinel kubectl untouched; the build's own marker is restored (Codex I8).
  * Jenkinsfile.nifty-gex-service — its build block EXECUTED with a sentinel docker/registry: the push's own
    digest, cross-checked against the per-build tag, locked by BUILD_ID and source commit (Codex I10).
  * scripts/deploy/bind-required-image.sh — the permitted build's digest replaces a moved tag; a wrong
    repository, a disagreeing authoritative render, an unserved digest are refused; a
    registry:port/repo@digest render (Codex I7) is accepted.
  * scripts/jenkins/validate-jenkinsfile-guard.py — the repository passes; the committed refused fixture
    fails; the shared mutation suite refuses every demonstrated skip path; the Codex round-3 reproductions
    applied to THIS repository's real Jenkinsfiles are refused.
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
# The onboarding document's manifest path and row, in one place, so NewServiceTemplateTest replays the
# documented procedure rather than a convenient approximation of it.
MANIFEST_REL = "scripts/ci/jenkins-permitted-sha-scope.txt"
SCOPE_ROW = ("Jenkinsfile | in |  | the service's build job: it packages source and publishes an image, so it carries the\n"
             "# permitted-commit guard — guard first, every later stage gated, the package and the image build each a\n"
             "# dedicated step after the workspace verify\n")


def run_validator(root: Path, manifest: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["python3", str(VALIDATOR), "--root", str(root), "--manifest", str(manifest)], capture_output=True, text=True)


class _Server:
    def __init__(self, routes: dict[str, tuple[int, str]]):
        self.routes = routes_local = routes

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


def _LateServer() -> _Server:
    """A server whose routes are filled in after it starts, so a served body can name the server's own URL."""
    return _Server({})


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
            "origin unreachable", "nested at the tip, permitted B", "nested --ref feature at a merged commit",
            # Codex I1: the branch HEAD must BE the permitted commit — an older main commit is refused,
            # for its own reason, and the two faults of step 2c stay distinguishable in the log.
            "older main commit B while tip is D", "older main commit, its refusal names the tip",
            "older main commit, --ref main does not excuse it", "off-branch C says off-branch, not behind",
            "back at the tip: permitted again", "nested at an older main commit, permitted B",
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
    """Codex N2 (#1043) / I2 (gateway) / I1 (web) / N3 (processing): a registered guard-version default is a
    declaration. The helper now proves the child's DEFINITION: config.xml (Pipeline from SCM, the expected
    repository, */main, the expected scriptPath, no extensions), the forwarded SHA is main's tip, and the
    Jenkinsfile fetched at that commit passes this validator with a guard hashing to ours. The shared,
    executed suite drives it through every case (a stand-in controller, real git repositories)."""

    def test_shared_definition_suite(self) -> None:
        r = subprocess.run(["python3", str(J / "require-guarded-downstream-test.py")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("require-guarded-downstream-test: ALL PASS", r.stdout)
        for case in ["declaration-only child (matching registered parameters, no guard in its Jenkinsfile) is refused",
                     "child whose effect stage is not gated on the guard is refused",
                     "inline pipeline script (not from SCM) is refused", "a parameterised branch spec is refused",
                     "another Jenkinsfile is refused", "a forwarded SHA that is not main's tip is refused",
                     "child carrying a different guard at that commit is refused (even though its own validator view is consistent)",
                     "folder caller: the folder's child is inspected, not the root job of the same name",
                     "tag remapped onto origin/main, heavyweight checkout (Codex #1043 reproduction) is refused",
                     "feature branch remapped onto origin/main, heavyweight (Codex gateway reproduction) is refused",
                     "a clean configuration with a heavyweight checkout is refused",
                     "the live service-deploy configuration (copied from the controller) is accepted",
                     "a stalled git ls-remote ends in a named refusal within the deadline",
                     "a second scriptPath after the expected one is refused (Codex M4)",
                     "a TERM-ignoring descendant holding the pipe (parent already exited): named refusal",
                     "child whose definition runs the guard at the forwarded tip is accepted"]:
            self.assertIn(f"ok   [{case}]", r.stdout)


class FetchPermittedImageLockTest(unittest.TestCase):
    """Codex I9: the lock is read from the child the compatibility check addressed — the same resolver
    (jenkins-job-path.sh, relative to the caller's folder) — and must name that very build."""

    def _lock(self, build_url: str, build_id: str = "7") -> str:
        return (
            "OPTIONS_EDGE_IMAGE_LOCK_FORMAT=1\n"
            f"OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT={SHA_A}\n"
            f"OPTIONS_EDGE_IMAGE_LOCK_CONTRACTS_GIT_COMMIT={SHA_C}\n"
            f"OPTIONS_EDGE_IMAGE_LOCK_GUARD_VERSION={OWN_HASH}\n"
            f"OPTIONS_EDGE_IMAGE_LOCK_BUILD_ID={build_id}\n"
            f"OPTIONS_EDGE_IMAGE_LOCK_BUILD_URL={build_url}\n"
            f"ES_CVD_IMAGE=192.168.100.252:5000/options-edge-es-cvd:prod-7-aaaaaaaaaaaa@{DIGEST}\n"
            f"ES_CVD_IMAGE_GIT_COMMIT={SHA_A}\n"
            f"INDICATOR_SERVICE_IMAGE=192.168.100.252:5000/options-edge-indicator-service:prod-7-aaaaaaaaaaaa@{DIGEST}\n"
            f"INDICATOR_SERVICE_IMAGE_GIT_COMMIT={SHA_B}\n"
        )

    ROOT_PATH = "/job/options-edge-processing/7/artifact/.jenkins-tmp/options-edge-image-lock.env"
    FOLDER_PATH = "/job/folder/job/options-edge-processing/7/artifact/.jenkins-tmp/options-edge-image-lock.env"

    def test_permitted_commits_yield_the_digest_pinned_image(self) -> None:
        r = self._fetch({self.ROOT_PATH: self._lock("{BASE}/job/options-edge-processing/7/")}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C])
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), f"192.168.100.252:5000/options-edge-es-cvd:prod-7-aaaaaaaaaaaa@{DIGEST}")
        self.assertIn(f"with contracts {SHA_C}", r.stderr)

    def _fetch(self, routes_fn, args, env=None):
        """routes_fn: dict path -> body, or callable(base_url) -> dict; bodies may contain {BASE}."""
        server = _LateServer()
        self.addCleanup(server.close)
        routes = routes_fn(server.url) if callable(routes_fn) else routes_fn
        server.routes.update({k: (200, v.replace("{BASE}", server.url)) for k, v in routes.items()})
        e = {"JENKINS_URL": server.url + "/"}
        e.update(env or {})
        return sh(FETCH_LOCK, args, e)

    def test_other_source_or_contracts_commit_is_refused(self) -> None:
        body = self._lock("{BASE}/job/options-edge-processing/7/")
        r = self._fetch({self.ROOT_PATH: body}, ["options-edge-processing", "7", SHA_B, "options-edge-es-cvd"])
        self.assertEqual(r.returncode, 1)
        self.assertIn(f"was built from {SHA_A}, not the permitted {SHA_B}", r.stderr)
        r = self._fetch({self.ROOT_PATH: body}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_B])
        self.assertEqual(r.returncode, 1)
        self.assertIn(f"compiled contracts {SHA_C}, not the permitted {SHA_B}", r.stderr)
        no_contracts = body.replace(f"OPTIONS_EDGE_IMAGE_LOCK_CONTRACTS_GIT_COMMIT={SHA_C}\n", "")
        r = self._fetch({self.ROOT_PATH: no_contracts}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C])
        self.assertEqual(r.returncode, 1)
        self.assertIn("did not record the contracts", r.stderr)

    def test_folder_caller_reads_the_folders_child_absent_root(self) -> None:
        # Only the folder's child exists: the old helper requested the root job and failed after the image was published.
        body = self._lock("{BASE}/job/folder/job/options-edge-processing/7/")
        r = self._fetch({self.FOLDER_PATH: body}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C], {"JOB_NAME": "folder/service-deploy"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("folder/options-edge-processing #7", r.stderr)

    def test_folder_caller_never_accepts_a_conflicting_root_lock(self) -> None:
        # A root job of the same name has build #7 with matching source fields: the folder caller must not read it.
        root_body = self._lock("{BASE}/job/options-edge-processing/7/")
        r = self._fetch({self.ROOT_PATH: root_body}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C], {"JOB_NAME": "folder/service-deploy"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("could not read the image lock of folder/options-edge-processing #7", r.stderr)
        # ... and a root lock SERVED at the folder path (a copied artifact) names another build: refused.
        r = self._fetch({self.FOLDER_PATH: root_body}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C], {"JOB_NAME": "folder/service-deploy"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("it is not that build's lock", r.stderr)

    def test_the_returned_child_identity_must_match_the_resolved_build(self) -> None:
        body = self._lock("{BASE}/job/options-edge-processing/7/")
        r = self._fetch(lambda base: {self.ROOT_PATH: body}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C],
                        {"CHILD_BUILD_URL": "http://elsewhere/job/folder/job/options-edge-processing/7/"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("the child build that ran is", r.stderr)

    def test_lock_of_another_build_number_is_refused(self) -> None:
        body = self._lock("{BASE}/job/options-edge-processing/7/", build_id="6")
        r = self._fetch({self.ROOT_PATH: body}, ["options-edge-processing", "7", SHA_A, "options-edge-es-cvd", SHA_C])
        self.assertEqual(r.returncode, 1)
        self.assertIn("carries OPTIONS_EDGE_IMAGE_LOCK_BUILD_ID '6', not 7", r.stderr)

    def test_per_image_mismatch_missing_image_unreachable_build(self) -> None:
        body = self._lock("{BASE}/job/options-edge-processing/7/")
        r = self._fetch({self.ROOT_PATH: body}, ["options-edge-processing", "7", SHA_A, "options-edge-indicator-service"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("INDICATOR_SERVICE_IMAGE was built from", r.stderr)
        r = self._fetch({self.ROOT_PATH: body}, ["options-edge-processing", "7", SHA_A, "options-edge-vol-premium"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("has 0 digest-pinned entries", r.stderr)
        r = self._fetch({self.ROOT_PATH: body}, ["options-edge-processing", "8", SHA_A, "options-edge-es-cvd"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("could not read the image lock", r.stderr)
        for args, msg in [(["options-edge-processing", "x", SHA_A, "options-edge-es-cvd"], "is not a number"),
                          (["options-edge-processing", "7", SHA_A[:12], "options-edge-es-cvd"], "not a full commit id"),
                          (["options-edge-processing", "7", SHA_A, "options-edge-es-cvd; rm"], "not a plain image name"),
                          (["folder/options-edge-processing", "7", SHA_A, "options-edge-es-cvd"], "cannot be resolved")]:
            r = self._fetch({self.ROOT_PATH: body}, args)
            self.assertEqual(r.returncode, 1, args)
            self.assertIn(msg, r.stderr)


class PostDeployRecoveryTest(unittest.TestCase):
    """Codex I8 (BLOCKER): recovery may act only on a marker THIS build wrote. The marker records service,
    job, BUILD_ID and permitted SHA; post{} verifies them (and the four flags) before any kubectl, and
    never recovers in a dry run. Sentinel kubectl records every mutation."""

    FLAGS = ["PERMITTED_SHA_GUARD", "DEPLOY_WORKSPACE_PERMITTED", "SECONDARY_PERMISSIONS_PASSED", "EFFECT_STAGE_STARTED"]
    MARKER_TOOL = ROOT / "scripts/deploy/vix-pause-marker.sh"

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.mutations = self.tmp / "kubectl-mutations"
        fake = self.tmp / "bin" / "kubectl"
        fake.parent.mkdir()
        # A deployment that exists and sits at 0 replicas: exactly the state a restore acts on.
        fake.write_text(
            "#!/usr/bin/env bash\n"
            "case \"$*\" in\n"
            "  *'--ignore-not-found -o name'*) echo deployment/vix-option-inteligence-service ;;\n"
            "  *jsonpath*) printf 0 ;;\n"
            f"  *scale*) echo \"SENTINEL_MUTATION $*\" >> '{self.mutations}' ;;\n"
            "esac\n"
            "exit 0\n")
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        self.marker = self.tmp / "vix-paused-replicas"

    def _env(self, **over: str) -> dict[str, str]:
        e = {"PATH": f"{self.tmp / 'bin'}:{os.environ['PATH']}", "JOB_NAME": "service-deploy", "BUILD_ID": "41",
             "PERMITTED_SHA": SHA_A, "SERVICE_PARAM": "vix-option-inteligence", "DEPLOY_DRY_RUN_PARAM": "false"}
        e.update(over)
        return e

    def _claim(self, replicas: str = "3", **over: str) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", str(self.MARKER_TOOL), "claim", str(self.marker), replicas], capture_output=True, text=True, env=self._env(**over))

    def _recover(self, flags: list[str] | None = None, **over: str) -> subprocess.CompletedProcess:
        e = self._env(**over)
        e.update({f: "PASSED" for f in (self.FLAGS if flags is None else flags)})
        return subprocess.run(["bash", str(RECOVERY), str(self.marker)], capture_output=True, text=True, env=e)

    def _scaled(self) -> str:
        return self.mutations.read_text() if self.mutations.exists() else ""

    def test_own_marker_with_every_flag_is_restored(self) -> None:
        r = self._claim("3")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("BUILD_ID=41", self.marker.read_text())
        self.assertIn(f"PERMITTED_SHA={SHA_A}", self.marker.read_text())
        r = self._recover()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("scale deployment/vix-option-inteligence-service --replicas=3", self._scaled())
        self.assertFalse(self.marker.exists())

    def test_stranded_unrelated_marker_is_preserved_even_with_every_flag(self) -> None:
        # Build 40 paused VIX and died; build 41 (every flag PASSED, same service) must not restore it.
        self._claim("3", BUILD_ID="40")
        r = self._recover()
        self.assertEqual(r.returncode, 0)
        self.assertEqual(self._scaled(), "")
        self.assertTrue(self.marker.exists())
        self.assertIn("HUMAN REQUIRED", r.stderr)
        self.assertIn("written by build 40, not this build 41", r.stderr)

    def test_codex_reproduction_web_dry_run_does_not_touch_a_vix_marker(self) -> None:
        # I8: SERVICE=web, BUILD_IMAGES=false, DEPLOY_DRY_RUN=true, every flag PASSED, a stranded VIX marker holding 3.
        self._claim("3", BUILD_ID="40")
        r = self._recover(SERVICE_PARAM="web", DEPLOY_DRY_RUN_PARAM="true")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(self._scaled(), "", "SENTINEL_MUTATION reached")
        self.assertTrue(self.marker.exists())
        self.assertIn("HUMAN REQUIRED", r.stderr)

    def test_dry_run_never_recovers_even_its_own_marker(self) -> None:
        self._claim("3")
        r = self._recover(DEPLOY_DRY_RUN_PARAM="true")
        self.assertEqual(self._scaled(), "")
        self.assertTrue(self.marker.exists())
        self.assertIn("this build is a dry run", r.stderr)
        r = self._claim("3", BUILD_ID="42", DEPLOY_DRY_RUN_PARAM="true")
        self.assertEqual(r.returncode, 1)

    def test_legacy_bare_number_marker_is_not_owned(self) -> None:
        self.marker.write_text("3\n")
        r = self._recover()
        self.assertEqual(self._scaled(), "")
        self.assertTrue(self.marker.exists())
        self.assertIn("not a recorded-owner marker", r.stderr)

    def test_other_job_or_permission_is_not_owned(self) -> None:
        self._claim("3")
        for over, msg in [({"JOB_NAME": "folder/service-deploy"}, "written by job 'service-deploy'"), ({"PERMITTED_SHA": SHA_B}, "written under permission")]:
            r = self._recover(**over)
            self.assertEqual(self._scaled(), "", over)
            self.assertIn(msg, r.stderr)
        self.assertTrue(self.marker.exists())

    def test_every_rejection_point_leaves_the_recovery_inert(self) -> None:
        self._claim("3")
        points = {
            "first guard refused": [],
            "deploy-workspace guard refused": ["PERMITTED_SHA_GUARD"],
            "secondary input refused": ["PERMITTED_SHA_GUARD", "DEPLOY_WORKSPACE_PERMITTED"],
            "refused before any effect stage began": ["PERMITTED_SHA_GUARD", "DEPLOY_WORKSPACE_PERMITTED", "SECONDARY_PERMISSIONS_PASSED"],
        }
        for name, set_flags in points.items():
            r = self._recover(set_flags)
            self.assertEqual(r.returncode, 0, name)
            self.assertEqual(self._scaled(), "", name)
            self.assertTrue(self.marker.exists(), name)
            self.assertIn("HUMAN REQUIRED", r.stderr, name)

    def test_flag_values_must_be_exactly_passed(self) -> None:
        self._claim("3")
        for bad in ("passed", "PASSED ", "true", "1"):
            e = self._env()
            e.update({f: "PASSED" for f in self.FLAGS})
            e["EFFECT_STAGE_STARTED"] = bad
            subprocess.run(["bash", str(RECOVERY), str(self.marker)], capture_output=True, text=True, env=e)
            self.assertEqual(self._scaled(), "", bad)

    def test_claim_refuses_to_pause_over_a_stranded_marker(self) -> None:
        self._claim("3", BUILD_ID="40")
        before = self.marker.read_text()
        r = self._claim("1")
        self.assertEqual(r.returncode, 1)
        self.assertIn("HUMAN REQUIRED", r.stderr)
        self.assertEqual(self.marker.read_text(), before, "a stranded marker must never be overwritten or adopted")
        r = self._claim("3", SERVICE_PARAM="web", BUILD_ID="43")
        self.assertEqual(r.returncode, 1)

    def test_vix_unpause_itself_reads_only_an_owned_marker(self) -> None:
        self._claim("3", BUILD_ID="40")
        r = subprocess.run(["bash", str(ROOT / "scripts/deploy/vix-unpause.sh"), str(self.marker)], capture_output=True, text=True, env=self._env())
        self.assertEqual(r.returncode, 1)
        self.assertEqual(self._scaled(), "")
        self.assertTrue(self.marker.exists())

    def test_no_marker_nothing_to_do(self) -> None:
        r = self._recover()
        self.assertEqual(r.returncode, 0)
        self.assertEqual(self._scaled(), "")
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


class NiftyPostBuildWorkspaceTest(unittest.TestCase):
    """The verification must accept the workspace a SUCCESSFUL build leaves behind, not just a clean one.

    Codex round 3 (Military): `BUILD_IMAGE=true` is the default and clones the nifty source into
    `nifty-gex-src/`, which is untracked and not ignored. The round-2 verification therefore refused the
    workspace for its own acquisition: a successful image build and push reached the deploy stage and was
    stopped there. The preparation step now removes that completed clone -- rather than declaring it to the
    verifier, which would exempt the very tree this job cloned and built."""

    def test_the_preparation_removes_the_nested_checkout_before_the_verify(self) -> None:
        text = (ROOT / "Jenkinsfile.nifty-gex-service").read_text()
        stage = text[text.index("    stage('Deploy (service-scoped)') {"):]
        self.assertIn("rm -rf nifty-gex-src", stage[:stage.index("verify-permitted-tree.sh")],
                      "the completed nifty clone must be removed BEFORE the workspace verification")
        self.assertNotIn("--allow-ignored nifty-gex-src", text,
                         "the cloned source must not be declared to the verifier")

    def test_the_real_post_build_workspace_passes_only_after_the_removal(self) -> None:
        """Build the workspace a BUILD_IMAGE=true run actually leaves, then run the exact verify line."""
        text = (ROOT / "Jenkinsfile.nifty-gex-service").read_text()
        vline = next(l for l in text.split("\n") if "verify-permitted-tree.sh --dir ." in l)
        allow = [a.strip('"') for a in vline.split("verify-permitted-tree.sh ", 1)[1].rstrip("'").split()
                 if a not in ("--dir", ".")]
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        co = tmp / "co"
        subprocess.run(["git", "-C", str(ROOT), "worktree", "add", "--detach", str(co), "HEAD"],
                       capture_output=True, text=True)
        self.addCleanup(lambda: subprocess.run(["git", "-C", str(ROOT), "worktree", "remove", "--force", str(co)],
                                               capture_output=True, text=True))
        head = subprocess.run(["git", "-C", str(co), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()

        def verify() -> subprocess.CompletedProcess:
            return subprocess.run(["bash", str(ROOT / "scripts/jenkins/verify-permitted-tree.sh"), "--dir", str(co)] + allow,
                                  capture_output=True, text=True, env={**os.environ, "PERMITTED_SHA": head})

        # what the Build image stage leaves behind on the default path
        (co / ".jenkins-tmp").mkdir(exist_ok=True)
        (co / ".jenkins-tmp/permission-receipt.env").write_text("x=1\n")
        src = co / "nifty-gex-src"
        src.mkdir()
        subprocess.run(["git", "init", "-q", str(src)], capture_output=True, text=True)
        (src / "pom.xml").write_text("<project/>\n")
        # NEGATIVE CONTROL: without the removal the ordinary successful path is refused, naming the clone
        r = verify()
        self.assertEqual(r.returncode, 1, "the post-build workspace was expected to be refused before the removal")
        self.assertIn("nifty-gex-src", r.stdout + r.stderr)
        # ...and the preparation step's removal is what makes it pass
        shutil.rmtree(src)
        r = verify()
        self.assertEqual(r.returncode, 0, "the post-build workspace must pass once the clone is removed:\n" + r.stdout + r.stderr)


class PermittedShaGuardValidatorTest(unittest.TestCase):
    def test_repository_passes(self) -> None:
        r = subprocess.run(["bash", str(ROOT / "scripts/ci/validate-jenkins-permitted-sha-guard.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("carry the canonical permitted-commit guard", r.stdout)

    def test_every_jenkinsfile_is_classified(self) -> None:
        """Repository-WIDE, not root-only.

        Codex M1 on processing #836: discovery searched `<root>/Jenkinsfile*` only, so definitions in
        subdirectories were neither judged nor reported. This repository keeps a template and two test
        fixtures outside the root; they are classified `out`, with the reason, rather than invisible.
        The inventory here is git's, so it is independent of the validator's own walk."""
        manifest = (ROOT / "scripts/ci/jenkins-permitted-sha-scope.txt").read_text()
        listed = {l.split("|")[0].strip() for l in manifest.splitlines() if l.strip() and not l.startswith("#")}
        tracked = subprocess.run(["git", "ls-files", "--", "Jenkinsfile*", "*/Jenkinsfile*"],
                                 capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(tracked.returncode, 0, tracked.stderr)
        present = {p for p in tracked.stdout.split() if p}
        self.assertEqual(listed, present)
        for nested in ("templates/Jenkinsfile.new-service",
                       "tests/fixtures/permitted-sha-guard/good/Jenkinsfile.fixture-good",
                       "tests/fixtures/permitted-sha-guard/refused/Jenkinsfile.fixture-apply-before-guard"):
            self.assertIn(nested, listed)

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

    def test_shared_mutation_suite_refuses_every_demonstrated_skip_path(self) -> None:
        r = subprocess.run(["python3", str(VALIDATOR_SUITE)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("validate-jenkinsfile-guard-test: ALL PASS", r.stdout)
        for case in ["primary guard inverted (rc == 0)", "primary guard || true in the shell string",
                     "post mutation after the gate block", "post mutation in the else branch", "post gate negated !(…)",
                     "post gate negated ! (…) with a space", "post gate compared to false", "compatibility check never entered: if (false)",
                     "compatibility check caught: catchError", "effect stage gate removed", "effect stage gate inverted (!=)",
                     "dedicated step body with extra tokens is refused: in a subshell",
                     "check skipped by return in an earlier script block, trigger in a later one (Codex gateway r4)",
                     "check and flag in if (false) plus a duplicate flag outside (Codex web r3)",
                     "dedicated step body with extra tokens is refused: in backticks (Codex #1043 r5)",
                     "nested guard as sh(script: …, 'returnStatus': true) — quoted key (Codex gateway r6 M2)",
                     "acquisition and guard skipped together by an early return, a later sibling step builds whatever app-src holds (Codex gateway I6 / web M2)",
                     "the template text inside a multi-line triple-single-quoted shell block after exit 0; exit 1 (Codex reproduction) is not the dedicated step",
                     "dedicated step body with extra tokens is refused: exit 0 hidden by a later nonzero exit (Codex #1043 r6 / gateway I7 / web M5)",
                     "dedicated step body with extra tokens is refused: an EXIT trap turning refusal into success (Codex gateway I8)",
                     "template sweep: 2354 single insertions into the dedicated command, none accepted",
                     "acquisition step with an effect on its own line is refused: `;` then docker build (Codex web M6)",
                     "guard wrapped in dir('other') while the acquisition is at the root is refused (Codex web M7)",
                     "the gateway I9 reproduction: a second checkout into other/app-src with a guard on app-src is refused",
                     "compatibility check inverted", "compatibility check status discarded", "git pull rebound by an echo",
                     "separate-agent stage without inline re-guard", "dedicated step body with extra tokens is refused: || true after it",
                     "contracts re-checked-out after its guard", "guard version default is another hash"]:
            self.assertIn(f"ok   [{case}]", r.stdout)


class NewServiceTemplateTest(unittest.TestCase):
    """templates/Jenkinsfile.new-service is the definition every new service starts life with.

    It is not a registered job — Jenkins never loads THIS path — so the validator cannot judge it
    where it lies, and it stays classified `out` in scripts/ci/jenkins-permitted-sha-scope.txt (that
    manifest's `in` means "a job the assistant may trigger", which a template is not). What is judged
    instead is the thing that matters: the COPY. The template, its companion image script and the
    guard toolkit are copied into a fixture root by REPLAYING new-service-onboarding.md's own steps —
    the same `cp` operations, the same manifest at the same path, the same validator invocation — and
    the validator judges that root. Before this, a service onboarded from the template started as an
    unguarded job that built and published an image with no permitted commit (the class Codex
    reported as processing M1 on #836).

    WHAT THIS DOES NOT ESTABLISH (Codex N1 on #1078). It proves the template is correct AT THE MOMENT
    IT IS COPIED. It installs nothing in the onboarded repository and governs no later edit there: if
    that repository removes a verify step, this test still passes, because the template it judges is
    unchanged. The onboarding document tells the onboarder to wire the validator into the new
    repository's own CI; nothing here enforces that, and until it is done the regression can reach
    Jenkins unopposed. Read this as coverage of the template, never as coverage of onboarded
    repositories.

    The mutations below are the point of the test: they show it can fail, so a later edit that quietly
    drops the guard stage, ungates a stage, folds a second command into an effect step or lets the
    declared guard version drift is caught here and not in a new repository six months later."""

    def _copy_root(self) -> Path:
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        (tmp / "scripts/jenkins").mkdir(parents=True)
        (tmp / "scripts/ci").mkdir(parents=True)
        shutil.copy(GUARD, tmp / "scripts/jenkins/permitted-sha-guard.sh")
        shutil.copy(J / "verify-permitted-tree.sh", tmp / "scripts/jenkins/verify-permitted-tree.sh")
        shutil.copy(ROOT / "templates/service-image.sh", tmp / "scripts/ci/service-image.sh")
        shutil.copy(ROOT / "templates/Jenkinsfile.new-service", tmp / "Jenkinsfile")
        # The manifest the onboarding document has the onboarder create, at the path its validator
        # command names. It is part of the procedure, not scaffolding this test invents: a fixture that
        # manufactures a prerequisite the document omits proves the template works in a world the
        # onboarder never reaches (Codex M1 on #1078 — the documented steps alone exited 1 with
        # "manifest missing"). MANIFEST_REL is the single spelling shared with the document.
        (tmp / MANIFEST_REL).write_text(SCOPE_ROW)
        return tmp

    def _validate(self, root: Path) -> subprocess.CompletedProcess:
        return run_validator(root, root / MANIFEST_REL)

    def test_the_templates_header_still_names_what_the_fixture_creates(self) -> None:
        """A REFERENCE check: every path this fixture writes is still named in the template's header.

        Deliberately narrow, and worth being precise about what it is not (Codex N2). It does not
        execute the onboarding procedure, does not compare the fixture's manifest row with the
        header's, and cannot notice a copy step added to the procedure but not to the fixture's
        fragment list — it catches a renamed or removed path, and nothing subtler. The one judgement
        it does make is the classification, because a header that showed `out` would document a job
        nobody is allowed to trigger.

        It deliberately does not read options-edge/new-service-onboarding.md: a test that reaches into
        a sibling checkout judges whatever branch happens to be on disk, which is not a fact about this
        commit. Keeping the header and that document in step is the reviewer's job, and a real
        automated check would need a shared executable procedure or a cross-repository check at pinned
        revisions — neither of which exists today. The header names the document so the next editor of
        either can find the other."""
        header = (ROOT / "templates/Jenkinsfile.new-service").read_text()
        header = header[:header.index("@Library")]
        for fragment in ("scripts/jenkins/permitted-sha-guard.sh", "scripts/jenkins/verify-permitted-tree.sh",
                         "scripts/ci/service-image.sh", MANIFEST_REL, "new-service-onboarding.md"):
            self.assertIn(fragment, header, f"the template's HOW TO USE header no longer names {fragment}")
        row = next((l for l in header.splitlines() if "Jenkinsfile |" in l), "")
        self.assertRegex(row, r"Jenkinsfile \|\s*in\b",
                         "the header's example manifest row must classify the copy `in` — `out` would "
                         "document a job the assistant may never trigger")

    def test_a_copy_of_the_template_passes_the_validator(self) -> None:
        r = self._validate(self._copy_root())
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("carry the canonical permitted-commit guard", r.stdout)

    def test_the_template_declares_the_guard_it_ships_with(self) -> None:
        """A pasted hash that the frozen guard has moved past would refuse every build of every
        service onboarded from here, at step 0 of the guard, with a version mismatch."""
        self.assertIn(f"defaultValue: '{OWN_HASH}'", (ROOT / "templates/Jenkinsfile.new-service").read_text())

    def _mutated(self, old: str, new: str) -> subprocess.CompletedProcess:
        root = self._copy_root()
        f = root / "Jenkinsfile"
        t = f.read_text()
        self.assertIn(old, t)
        f.write_text(t.replace(old, new, 1))
        return self._validate(root)

    def test_a_copy_without_the_guard_stage_is_refused(self) -> None:
        t = (ROOT / "templates/Jenkinsfile.new-service").read_text()
        stage = t[t.index("    stage('Permitted commit guard') {"):t.index("    stage('Resolve profile')")]
        r = self._mutated(stage, "")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("no stage named 'Permitted commit guard'", r.stdout)

    def test_an_ungated_effect_stage_is_refused(self) -> None:
        r = self._mutated("    stage('Image') {\n      when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }\n",
                          "    stage('Image') {\n")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("stage 'Image' after the guard has no `when` gate", r.stdout)

    def test_a_second_command_in_the_image_effect_step_is_refused(self) -> None:
        r = self._mutated("sh 'bash scripts/ci/service-image.sh'", "sh 'bash scripts/ci/service-image.sh && echo done'")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("does not fit the fixed `script` template", r.stdout)

    def test_the_package_step_without_its_verify_is_refused(self) -> None:
        r = self._mutated("        timeout(time: 10, unit: 'MINUTES') {\n"
                          "          sh 'PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir . "
                          "--allow-ignored target --allow-ignored \"*/target\" --allow-ignored build --allow-ignored .gradle'\n"
                          "        }\n"
                          "        sh 'mvn -B clean package -DskipTests'", "        sh 'mvn -B clean package -DskipTests'")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("no dedicated verify-permitted-tree step immediately precedes it", r.stdout)

    # ---- the image script's own provenance, EXECUTED (Codex I1) -----------------------------------
    # The verify step proves ONE checkout: the workspace root. DOCKERFILE and BUILD_CONTEXT are
    # parameters, so without a check they are a way past that proof — the verifier passes on the
    # workspace while docker is handed another directory's contents. Codex reproduced exactly that
    # with a sentinel docker and no edit to the template. These run the real script against a sentinel
    # docker in a throwaway checkout: the escape must be refused before docker is reached, and the
    # ordinary in-workspace build must still work (a check that refuses everything proves nothing).
    def _image_ws(self) -> tuple[Path, dict]:
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        ws, outside, bin_ = tmp / "ws", tmp / "outside", tmp / "bin"
        for d in (ws / "scripts/ci", outside, bin_):
            d.mkdir(parents=True)
        shutil.copy(ROOT / "templates/service-image.sh", ws / "scripts/ci/service-image.sh")
        (ws / "Dockerfile").write_text("FROM scratch\n")
        (outside / "Dockerfile").write_text("FROM scratch\n")
        (outside / "unpermitted.txt").write_text("never in any permitted commit\n")
        (bin_ / "docker").write_text("#!/usr/bin/env bash\necho \"SENTINEL-DOCKER $*\"\n")
        (bin_ / "docker").chmod(0o755)
        for cmd in (["git", "init", "-q", "."], ["git", "add", "-A"],
                    ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "x"]):
            subprocess.run(cmd, cwd=ws, check=True, capture_output=True)
        env = dict(os.environ, PATH=f"{bin_}:{os.environ['PATH']}", SERVICE_NAME="options-edge-foo",
                   IMAGE_REGISTRY="reg:5000", BUILD_PLATFORM="linux/arm64", PUSH_IMAGE="false",
                   JOB_NAME="options-edge-foo", BUILD_NUMBER="1")
        env.pop("DOCKERFILE", None)
        env.pop("BUILD_CONTEXT", None)
        return ws, env

    def _run_image(self, **over) -> subprocess.CompletedProcess:
        ws, env = self._image_ws()
        env.update({k: str(v) for k, v in over.items()})
        if over.get("_abs_context"):
            env["BUILD_CONTEXT"] = str(ws.parent / "outside")
            del env["_abs_context"]
        return subprocess.run(["bash", "scripts/ci/service-image.sh"], cwd=ws, env=env,
                              capture_output=True, text=True)

    def test_the_image_script_builds_from_the_verified_workspace(self) -> None:
        r = self._run_image()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("SENTINEL-DOCKER buildx build", r.stdout)
        self.assertTrue(r.stdout.rstrip().endswith(" ."), r.stdout)

    def test_a_build_context_outside_the_verified_checkout_is_refused(self) -> None:
        r = self._run_image(BUILD_CONTEXT="../outside")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("outside the verified workspace", r.stderr)
        self.assertNotIn("SENTINEL-DOCKER buildx build", r.stdout)

    def test_an_absolute_build_context_is_refused(self) -> None:
        r = self._run_image(_abs_context=True)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("not absolute", r.stderr)
        self.assertNotIn("SENTINEL-DOCKER buildx build", r.stdout)

    def test_a_dockerfile_outside_the_verified_checkout_is_refused(self) -> None:
        r = self._run_image(DOCKERFILE="../outside/Dockerfile")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("outside the verified workspace", r.stderr)
        self.assertNotIn("SENTINEL-DOCKER buildx build", r.stdout)

    def test_a_symlink_out_of_the_checkout_is_refused(self) -> None:
        """`..` is the obvious escape; a symlinked directory is the one a path check usually misses."""
        ws, env = self._image_ws()
        (ws / "link").symlink_to(ws.parent / "outside")
        env["BUILD_CONTEXT"] = "link"
        r = subprocess.run(["bash", "scripts/ci/service-image.sh"], cwd=ws, env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("outside the verified workspace", r.stderr)
        self.assertNotIn("SENTINEL-DOCKER buildx build", r.stdout)

    # ---- the newline bypass (Codex I1, round 2) ---------------------------------------------------
    # `$( … )` strips trailing newlines, so for a path whose bytes contain one the value COMPARED is
    # not the value passed to docker. Codex's executed reproduction: a sibling directory named "ws\n"
    # beside the workspace "ws" passes both the guard and the tree verifier, compares as inside, and
    # is handed to docker verbatim. These run the real script; the sibling really exists on disk.
    def test_a_newline_bearing_path_is_refused_before_it_can_be_mis_compared(self) -> None:
        ws, env = self._image_ws()
        sibling = Path(f"{ws}\n")                     # the outside directory Codex used
        sibling.mkdir()
        (sibling / "Dockerfile").write_text("FROM scratch\n")
        (sibling / "unpermitted.txt").write_text("never in any permitted commit\n")
        for var, value in (("BUILD_CONTEXT", f"../{ws.name}\n"),
                           ("DOCKERFILE", f"../{ws.name}\n/Dockerfile")):
            with self.subTest(var=var):
                r = subprocess.run(["bash", "scripts/ci/service-image.sh"], cwd=ws,
                                   env=dict(env, **{var: value}), capture_output=True, text=True)
                self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                self.assertIn("contains a newline", r.stderr)
                self.assertNotIn("SENTINEL-DOCKER buildx build", r.stdout)

    def test_a_workspace_whose_own_path_holds_a_newline_is_refused(self) -> None:
        """The containment ROOT is captured by substitution too; a stripped root is a wrong root."""
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        ws, bin_ = tmp / "ws\n", tmp / "bin"
        (ws / "scripts/ci").mkdir(parents=True)
        bin_.mkdir()
        shutil.copy(ROOT / "templates/service-image.sh", ws / "scripts/ci/service-image.sh")
        (ws / "Dockerfile").write_text("FROM scratch\n")
        (bin_ / "docker").write_text("#!/usr/bin/env bash\necho \"SENTINEL-DOCKER $*\"\n")
        (bin_ / "docker").chmod(0o755)
        for cmd in (["git", "init", "-q", "."], ["git", "add", "-A"],
                    ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "x"]):
            subprocess.run(cmd, cwd=ws, check=True, capture_output=True)
        env = dict(os.environ, PATH=f"{bin_}:{os.environ['PATH']}", SERVICE_NAME="options-edge-foo",
                   IMAGE_REGISTRY="reg:5000", BUILD_PLATFORM="linux/arm64", PUSH_IMAGE="false",
                   JOB_NAME="options-edge-foo", BUILD_NUMBER="1")
        env.pop("DOCKERFILE", None)
        env.pop("BUILD_CONTEXT", None)
        r = subprocess.run(["bash", "scripts/ci/service-image.sh"], cwd=ws, env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("the workspace path", r.stderr)
        self.assertNotIn("SENTINEL-DOCKER buildx build", r.stdout)

    def test_a_symlink_resolving_to_a_newline_bearing_path_is_refused(self) -> None:
        """Codex I1 r3: the input is newline-free, so checking the INPUT is not enough.

        `ln -s $'../ws\\n' link` committed inside the workspace resolves to the outside sibling, and
        the lossy capture of the RESOLVED path then strips the newline back to the workspace's own
        pathname — so the comparison accepted it while docker got the symlink. `chain` is the same
        thing one indirection further, to show the depth is not the limit. The symlinks are committed
        and the tree is clean, exactly as in the reproduction: the guard and the tree verifier have
        nothing to object to."""
        ws, env = self._image_ws()
        sibling = Path(f"{ws}\n")
        sibling.mkdir()
        (sibling / "Dockerfile").write_text("FROM scratch\n")
        (sibling / "unpermitted.txt").write_text("never in any permitted commit\n")
        (ws / "link").symlink_to(f"../{ws.name}\n")
        (ws / "chain").symlink_to("link")
        for cmd in (["git", "add", "-A"],
                    ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "symlinks"]):
            subprocess.run(cmd, cwd=ws, check=True, capture_output=True)
        for var, value in (("BUILD_CONTEXT", "link"), ("DOCKERFILE", "link/Dockerfile"),
                           ("BUILD_CONTEXT", "chain"), ("DOCKERFILE", "chain/Dockerfile")):
            with self.subTest(var=var, value=value):
                r = subprocess.run(["bash", "scripts/ci/service-image.sh"], cwd=ws,
                                   env=dict(env, **{var: value}), capture_output=True, text=True)
                self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                self.assertIn("RESOLVES to a pathname containing a newline", r.stderr)
                self.assertNotIn("SENTINEL-DOCKER buildx build", r.stdout)
        # and the ordinary build still works with those symlinks sitting in the tree
        r = subprocess.run(["bash", "scripts/ci/service-image.sh"], cwd=ws, env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("SENTINEL-DOCKER buildx build", r.stdout)

    def test_two_jobs_do_not_share_a_buildx_builder(self) -> None:
        """Codex I4: a shared builder name lets one service remove another's builder mid-build.

        The collision pair is the one Codex executed: sanitising is lossy, so `folder/service` and
        `folder-service` both read as `folder-service`. The names are taken from the real script,
        by running it against a sentinel docker that records the builder it is asked to create."""
        names = []
        # Codex's r2 pair, plus the r3 pair it crafted once the digest was only 32 bits — those two
        # share their first eight hex characters (697e95ab…), which is exactly why eight was not
        # enough. Both pairs must come out distinct.
        for job in ("folder/service", "folder-service",
                    "a-a/a/a/a/a-a-a-a/a-a-a-a/a/a/a-a/a-a-a-a",
                    "a-a-a-a/a/a-a-a/a-a-a-a-a/a-a-a-a-a/a-a-a"):
            ws, env = self._image_ws()
            (ws / "sentinel").mkdir()
            record = ws / "sentinel/builders"
            (ws.parent / "bin" / "docker").write_text(
                "#!/usr/bin/env bash\n"
                f'if [ "$1" = "buildx" ] && [ "$2" = "create" ]; then echo "$@" >> "{record}"; fi\n'
                'echo "SENTINEL-DOCKER $*"\n')
            r = subprocess.run(["bash", "scripts/ci/service-image.sh"], cwd=ws,
                               env=dict(env, JOB_NAME=job, BUILD_NUMBER="1"), capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            line = record.read_text()
            names.append(line.split("--name ")[1].split()[0])
        self.assertEqual(len(set(names)), len(names),
                         f"two jobs selected the same buildx builder — either can remove the other's: {names}")

    def test_a_drifted_guard_version_default_is_refused(self) -> None:
        r = self._mutated(f"defaultValue: '{OWN_HASH}'", "defaultValue: '" + "0" * 64 + "'")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("the job would declare a guard it does not run", r.stdout)


class ServiceDeployBindingTest(unittest.TestCase):
    """service-deploy's specifics that the generic validator does not model."""

    def setUp(self) -> None:
        self.text = (ROOT / "Jenkinsfile.service-deploy").read_text()
        self.stages = re.findall(r"^\s*stage\('([^']*)'", self.text, re.M)

    def test_intent_ledger_and_receipt_record_the_binding(self) -> None:
        for k in ("permittedSha", "checkedOutSha", "guardVersion", "requiredImage", "imageBuild", "processingPermittedSha", "contractsPermittedSha"):
            self.assertIn(f'"{k}"', self.text, k)
        self.assertIn("write-permission-receipt.sh", self.text)

    def test_image_build_trigger_is_bound_to_the_childs_definition_and_its_image_retained(self) -> None:
        for p in ("PROCESSING_PERMITTED_SHA", "CONTRACTS_PERMITTED_SHA", "REQUIRED_IMAGE"):
            self.assertIn(f"string(name: '{p}', defaultValue: '', trim: true", self.text)
        self.assertIn("require-guarded-downstream.sh options-edge-processing \"${PROCESSING_PERMITTED_SHA:?}\" CONTRACTS_PERMITTED_SHA", self.text)
        self.assertIn("string(name: 'PERMITTED_SHA',           value: params.PROCESSING_PERMITTED_SHA.trim())", self.text)
        self.assertIn("string(name: 'CONTRACTS_PERMITTED_SHA', value: params.CONTRACTS_PERMITTED_SHA.trim())", self.text)
        self.assertIn("def child = build job: 'options-edge-processing'", self.text)
        self.assertLess(self.text.index("env.CHILD_BUILD_URL         = child.absoluteUrl"), self.text.index("fetch-permitted-image-lock.sh options-edge-processing"))
        self.assertRegex(self.text, r"fetch-permitted-image-lock\.sh options-edge-processing \\\n\s+\"\$\{PROCESSING_BUILD_NUMBER:\?\}\" \"\$\{PROCESSING_PERMITTED_SHA:\?\}\" \"\$\{SERVICE_IMAGE_NAME:\?\}\" \"\$\{CONTRACTS_PERMITTED_SHA:\?\}\"")
        sd = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        self.assertIn(". scripts/deploy/bind-required-image.sh", sd)
        self.assertIn('PINNED_IMAGE="$(bind_required_image)" ||', sd)

    def test_required_image_repository_is_judged_before_any_effect_stage(self) -> None:
        sec = self.text[self.text.index("stage('Secondary permissions complete')"):self.text.index("stage('Reconcile feed-gateway expiry')")]
        self.assertIn('yq -r ".services[] | select(.name == \\\\\"$SERVICE_PARAM\\\\\") | .image" services.yaml', sec)
        self.assertIn("last != want", sec)
        self.assertLess(self.stages.index("Secondary permissions complete"), self.stages.index("Reconcile VIX option intelligence current topic"))

    def test_the_pause_claims_its_own_marker_and_post_recovers_only_through_the_owner_check(self) -> None:
        s = self.stages
        self.assertEqual(s[0], "Permitted commit guard")
        self.assertEqual(s[s.index("Deploy path") + 1], "Permitted commit guard (deploy workspace)")
        self.assertEqual(s[s.index("Build image (this service only)") + 1], "Secondary permissions complete")
        self.assertEqual(self.text.count("env.SECONDARY_PERMISSIONS_PASSED = 'PASSED'"), 1)
        self.assertLess(self.text.index("fetch-permitted-image-lock.sh"), self.text.index("env.SECONDARY_PERMISSIONS_PASSED = 'PASSED'"))
        vix = self.text[self.text.index("stage('Reconcile VIX option intelligence current topic')"):self.text.index("stage('Deploy (service-scoped)')")]
        self.assertIn("!params.DEPLOY_DRY_RUN", vix[:400])
        self.assertLess(vix.index("env.EFFECT_STAGE_STARTED = 'PASSED'"), vix.index("scale \"$DEP\" --replicas=0"))
        self.assertLess(vix.index('bash scripts/deploy/vix-pause-marker.sh claim "$MARK" "$PREV"'), vix.index("scale \"$DEP\" --replicas=0"))
        self.assertNotIn('printf \'%s\\n\' "$PREV" > "$MARK"', vix)
        self.assertNotIn("STRANDED", vix)
        post = "\n".join(l for l in self.text[self.text.rindex("      post {"):].splitlines() if not l.strip().startswith("//"))
        gate = re.search(r"if \((.*?)\) \{", post).group(1)
        for f in PostDeployRecoveryTest.FLAGS:
            self.assertIn(f"env.{f} == 'PASSED'", gate)
        self.assertNotIn("||", gate)
        self.assertLess(post.index(gate), post.index("post-deploy-recovery.sh"))
        self.assertLess(post.index("post-deploy-recovery.sh"), post.index("} else if"))
        self.assertEqual(post.count("post-deploy-recovery.sh"), 1)
        self.assertNotIn("vix-unpause.sh", post)
        rec = RECOVERY.read_text()
        self.assertIn('vix-pause-marker.sh" owned "$MARK"', rec)
        self.assertLess(rec.index('vix-pause-marker.sh" owned "$MARK"'), rec.index('exec bash "$here/vix-unpause.sh"'))


class WebServiceBindingTest(unittest.TestCase):
    def test_web_image_build_path_is_fail_closed_and_its_image_retained(self) -> None:
        t = (ROOT / "Jenkinsfile.web-service").read_text()
        self.assertIn("booleanParam(name: 'BUILD_IMAGE', defaultValue: false", t)
        self.assertIn("string(name: 'WEB_PERMITTED_SHA', defaultValue: '', trim: true", t)
        self.assertIn("require-guarded-downstream.sh options-edge-web-deploy \"${WEB_PERMITTED_SHA:?}\"", t)
        self.assertIn("string(name: 'PERMITTED_SHA',                value: params.WEB_PERMITTED_SHA.trim())", t)
        self.assertIn("def child = build job: 'options-edge-web-deploy'", t)
        self.assertLess(t.index("env.CHILD_BUILD_URL  = child.absoluteUrl"), t.index("fetch-permitted-image-lock.sh options-edge-web-deploy"))
        self.assertRegex(t, r"fetch-permitted-image-lock\.sh options-edge-web-deploy \\\n\s+\"\$\{WEB_BUILD_NUMBER:\?\}\" \"\$\{WEB_PERMITTED_SHA:\?\}\" options-edge-web")
        self.assertIn("write-permission-receipt.sh", t)
        self.assertNotIn("UNBOUND", t)


def _groovy_triple_body(text: str, anchor: str) -> str:
    """The body of the first sh \'\'\' block after `anchor`, as the shell receives it (Groovy's \\\\ -> \\)."""
    i = text.index(anchor)
    start = text.index("sh \'\'\'\n", i) + len("sh \'\'\'\n")
    end = text.index("\'\'\'", start)
    return text[start:end].replace("\\\\", "\\")


class NiftyImageIdentityTest(unittest.TestCase):
    """Codex I10: the Nifty build carries its guarded source identity into its image lock and the deploy.
    The build stage's shell block is EXECUTED with a sentinel docker/registry: the digest comes from this
    push's own output, must equal what the registry serves for the per-build tag, and lands in a lock keyed
    by BUILD_ID; the Groovy that follows turns only that lock into REQUIRED_IMAGE."""

    def setUp(self) -> None:
        self.text = (ROOT / "Jenkinsfile.nifty-gex-service").read_text()
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        ws = self.tmp / "ws"
        (ws / "scripts/jenkins").mkdir(parents=True)
        shutil.copy(J / "resolve-pushed-digest.sh", ws / "scripts/jenkins/resolve-pushed-digest.sh")
        subprocess.run(["git", "init", "-q", "-b", "main", str(ws)], check=True)
        subprocess.run(["git", "-C", str(ws), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "deploy"], check=True)
        src = ws / "nifty-gex-src"
        subprocess.run(["git", "init", "-q", "-b", "main", str(src)], check=True)
        subprocess.run(["git", "-C", str(src), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "app"], check=True)
        self.src_sha = subprocess.run(["git", "-C", str(src), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        self.ws = ws
        b = self.tmp / "bin"
        b.mkdir()
        (b / "docker").write_text("#!/usr/bin/env bash\n"
                                  "[ \"$1\" = push ] || exit 0\n"
                                  "tag=\"${2##*:}\"\n"
                                  "echo \"The push refers to repository [${2%:*}]\"\n"
                                  "echo \"$tag: digest: $PUSH_DIGEST size: 1234\"\n")
        (b / "curl").write_text("#!/usr/bin/env bash\nprintf 'HTTP/1.1 200 OK\\r\\nDocker-Content-Digest: %s\\r\\n\\r\\n' \"$REGISTRY_DIGEST\"\n")
        (b / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n")
        for f in b.iterdir():
            f.chmod(f.stat().st_mode | stat.S_IXUSR)
        # The dev path, step by step, exactly as the stage runs it: prepare, the DEDICATED build step (one command), the
        # push, the lock. The per-build tag reaches them through withEnv, derived as the Groovy derives it.
        stage = self.text[self.text.index("stage('Build image (native)')"):self.text.index("stage('Deploy (service-scoped)')")]
        self.prep = _groovy_triple_body(stage, "Nifty build: prepare")
        dev = stage[stage.index("} else {"):]
        self.build_line = next(l.strip() for l in dev.split("\n") if l.strip().startswith("sh 'docker build "))
        self.push = _groovy_triple_body(dev, "sh 'docker build ")
        self.lock = _groovy_triple_body(stage, "Nifty build: lock")
        self.assertIn('def uniqueTag = "b${env.BUILD_ID}-${nsha}"', stage)
        self.assertIn("def uniqueRef = \"${env.IMAGE_REF.substring(0, env.IMAGE_REF.lastIndexOf('/'))}/options-edge-nifty-gex:${uniqueTag}\"", stage)

    def _run(self, push: str, served: str) -> subprocess.CompletedProcess:
        ref = "localhost:5001/options-edge-nifty-gex:dev"
        tag = f"b57-{self.src_sha}"
        e = {"PATH": f"{self.tmp / 'bin'}:{os.environ['PATH']}", "HOME": str(self.tmp), "ENVIRONMENT": "dev", "BUILD_ID": "57", "BUILD_NUMBER": "57",
             "BUILD_URL": "http://j/job/nifty-gex-service-deploy/57/", "NIFTY_PERMITTED_SHA": self.src_sha, "IMAGE_REF": ref,
             "UNIQUE_TAG": tag, "UNIQUE_REF": f"{ref.rsplit('/', 1)[0]}/options-edge-nifty-gex:{tag}",
             "SOURCE_REPO": "git@github.com:abhinav-jain09/options-edge-nifty-gex.git", "PERMITTED_SHA_GUARD_VERSION": OWN_HASH,
             "PUSH_DIGEST": push, "REGISTRY_DIGEST": served}
        build_cmd = self.build_line[len("sh '"):-1]
        script = "\n".join(["set -e", "( " + self.prep + " )", build_cmd, "( " + self.push + " )", "( " + self.lock + " )"])
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=e, cwd=self.ws)

    def test_this_builds_push_is_locked_by_build_id_and_source_commit(self) -> None:
        r = self._run(DIGEST, DIGEST)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        lock = (self.ws / ".jenkins-tmp/image-lock-57.env").read_text()
        self.assertIn(f"OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT={self.src_sha}\n", lock)
        self.assertIn("OPTIONS_EDGE_IMAGE_LOCK_BUILD_ID=57\n", lock)
        self.assertIn(f"NIFTY_GEX_IMAGE=localhost:5001/options-edge-nifty-gex:b57-{self.src_sha}@{DIGEST}\n", lock)

    def test_a_registry_that_serves_another_digest_for_the_build_tag_is_refused(self) -> None:
        r = self._run(DIGEST, "sha256:" + "e" * 64)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("this push reported", r.stderr)
        self.assertFalse((self.ws / ".jenkins-tmp/image-lock-57.env").exists())

    def test_a_push_that_reports_no_digest_is_refused(self) -> None:
        r = self._run("", DIGEST)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("reported no digest", r.stderr)
        self.assertFalse((self.ws / ".jenkins-tmp/image-lock-57.env").exists())

    def test_a_checkout_other_than_the_permitted_commit_is_refused(self) -> None:
        self.src_sha = "f" * 40
        r = self._run(DIGEST, DIGEST)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("is not NIFTY_PERMITTED_SHA", r.stderr)

    def test_only_this_builds_lock_becomes_required_image_and_the_deploy_binds_it(self) -> None:
        g = self.text[self.text.index('def lockFile = ".jenkins-tmp/image-lock-${env.BUILD_ID}.env"'):self.text.index("stage('Deploy (service-scoped)')")]
        self.assertIn("lockBuild != env.BUILD_ID.toString()", g)
        self.assertIn("env.REQUIRED_IMAGE = ref", g)
        self.assertIn("archiveArtifacts artifacts: '.jenkins-tmp/options-edge-image-lock.env'", g)
        dep = self.text[self.text.index("stage('Deploy (service-scoped)')"):]
        self.assertIn("scripts/deploy/service-deploy.sh", dep)   # which binds REQUIRED_IMAGE (bind-required-image.sh)


class ActualJenkinsfileMutationTest(unittest.TestCase):
    """The Codex round-3 reproductions against THIS repository's real Jenkinsfiles: each mutation left the
    canonical text in place but made it non-executing, and the round-3 validator accepted it."""

    def _validate_mutated(self, name: str, old: str, new: str) -> subprocess.CompletedProcess:
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        # the whole scripts/ tree: the validator follows every repository script a step runs
        shutil.copytree(ROOT / "scripts", tmp / "scripts", ignore=shutil.ignore_patterns("__pycache__", ".helper-venv"))
        text = (ROOT / name).read_text()
        self.assertIn(old, text, name)
        (tmp / name).write_text(text.replace(old, new, 1))
        return subprocess.run(["python3", str(VALIDATOR), "--root", str(tmp), "--manifest", str(tmp / "scripts/ci/jenkins-permitted-sha-scope.txt"), "--only", name], capture_output=True, text=True)

    def test_service_deploy_compatibility_check_never_entered(self) -> None:
        t = (ROOT / "Jenkinsfile.service-deploy").read_text()
        a = t.index("          def compat = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh options-edge-processing")
        b = t.index("          }\n", t.index("if (compat != 0) {", a)) + len("          }\n")
        block = t[a:b]
        r = self._validate_mutated("Jenkinsfile.service-deploy", block, "          if (false) {\n" + block + "          }\n")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("is not executably protected", r.stdout)

    def test_service_deploy_check_returned_past_with_the_trigger_in_a_later_script_block(self) -> None:
        t = (ROOT / "Jenkinsfile.service-deploy").read_text()
        a = t.index("          def compat = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh options-edge-processing")
        mutated = t[:a] + "          return\n" + t[a:]
        mutated = mutated.replace("          def child = build job: 'options-edge-processing',", "        }\n        script {\n          def child = build job: 'options-edge-processing',", 1)
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        shutil.copytree(ROOT / "scripts", tmp / "scripts", ignore=shutil.ignore_patterns("__pycache__", ".helper-venv"))
        (tmp / "Jenkinsfile.service-deploy").write_text(mutated)
        r = subprocess.run(["python3", str(VALIDATOR), "--root", str(tmp), "--manifest", str(tmp / "scripts/ci/jenkins-permitted-sha-scope.txt"), "--only", "Jenkinsfile.service-deploy"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("is not executably protected", r.stdout)

    def test_nifty_nested_guard_skipped_by_an_early_return(self) -> None:
        # Codex gateway I6 / web M2 on a real file: the Nifty application-checkout guard (a dedicated step inside its
        # deadline block) returned past while the following sibling step builds and pushes that checkout.
        t = (ROOT / "Jenkinsfile.nifty-gex-service").read_text()
        a = t.index("        timeout(time: 10, unit: 'MINUTES') {\n          sh 'PERMITTED_SHA=\"${NIFTY_PERMITTED_SHA:-}\"")
        b = t.index("        }\n", a) + len("        }\n")
        block = t[a:b]
        r = self._validate_mutated("Jenkinsfile.nifty-gex-service", block, "        script {\n          return\n" + block.replace("\n        ", "\n          ").replace("        timeout", "          timeout", 1) + "        }\n")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("'nifty-gex-src' acquired after the guard is not re-bound", r.stdout)

    def test_nifty_guard_is_a_dedicated_step(self) -> None:
        t = (ROOT / "Jenkinsfile.nifty-gex-service").read_text()
        self.assertIn("          sh 'PERMITTED_SHA=\"${NIFTY_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir nifty-gex-src --ref main'\n", t)
        for mutated, say in [("sh 'exit 0; exit 1; PERMITTED_SHA=", "is not re-bound"), ("sh 'trap \\'exit 0\\' EXIT; PERMITTED_SHA=", "is not re-bound")]:
            r = self._validate_mutated("Jenkinsfile.nifty-gex-service", "          sh 'PERMITTED_SHA=", "          " + mutated)
            self.assertEqual(r.returncode, 1, r.stdout)
            self.assertIn(say, r.stdout)

    def test_nifty_source_is_provenance_verified_before_it_is_built(self) -> None:
        # The Nifty source is shipped (rsync, production) and built (docker build, dev); each is a dedicated effect step
        # right after its own verify of the nifty checkout. Remove either verify and the file is refused.
        t = (ROOT / "Jenkinsfile.nifty-gex-service").read_text()
        block = ("              timeout(time: 10, unit: 'MINUTES') {\n"
                 "                sh 'PERMITTED_SHA=\"${NIFTY_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir nifty-gex-src'\n"
                 "              }\n")
        self.assertEqual(t.count(block), 2)
        for effect in ("              sh 'rsync -a --delete --exclude .git nifty-gex-src/", "              sh 'docker build -t \"${IMAGE_REF}\""):
            r = self._validate_mutated("Jenkinsfile.nifty-gex-service", block + effect, effect)
            self.assertEqual(r.returncode, 1, r.stdout)
            self.assertIn("the nested checkout 'nifty-gex-src'", r.stdout)

    def test_the_effect_steps_own_body_cannot_change_the_source(self) -> None:
        # Codex deploy r11 (the class): statement adjacency protects the statement boundary, not the commands inside the
        # consuming step. Each effect is now a DEDICATED step whose whole body is one command matched against its fixed
        # template — a change to the source inside that step, however spelled, is refused.
        build = "              sh 'docker build -t \"${IMAGE_REF}\" -t \"${UNIQUE_REF}\" nifty-gex-src'\n"
        rsync = "              sh 'rsync -a --delete --exclude .git nifty-gex-src/ \"abhinav@${PROD_BUILD_HOST}:${BUILD_DIR}/\"'\n"
        tq = chr(39) * 3
        for old, new, say in [
            (build, "              sh 'cp -r /tmp/other/. nifty-gex-src/ && docker build -t \"${IMAGE_REF}\" -t \"${UNIQUE_REF}\" nifty-gex-src'\n", "does not fit the fixed `docker` template"),
            (build, "              sh " + tq + "\n                git -C nifty-gex-src apply /tmp/p.diff\n                docker build -t \"$IMAGE_REF\" nifty-gex-src\n              " + tq + "\n", "is not a DEDICATED effect step"),
            (build, "              sh 'docker build --build-context extra=/tmp/other -t \"${IMAGE_REF}\" nifty-gex-src'\n", "is not an option of the docker build template"),
            (build, "              sh 'docker build -t \"${IMAGE_REF}\" \"${CTX}\"'\n", "the build context must be a literal"),
            (rsync, "              sh " + tq + "\n                set -euo pipefail\n                tar -xf /tmp/other.tar -C nifty-gex-src\n                rsync -a --delete nifty-gex-src/ \"abhinav@$PROD_BUILD_HOST:$BUILD_DIR/\"\n              " + tq + "\n", "is not a DEDICATED effect step"),
            (rsync, "              sh 'rsync -a --delete --rsync-path=/tmp/x nifty-gex-src/ \"abhinav@${PROD_BUILD_HOST}:${BUILD_DIR}/\"'\n", "is not an option of the rsync template"),
        ]:
            r = self._validate_mutated("Jenkinsfile.nifty-gex-service", old, new)
            self.assertEqual(r.returncode, 1, new + r.stdout)
            self.assertIn(say, r.stdout)

    def test_a_permission_variable_cannot_be_repointed_around_a_verify(self) -> None:
        t = (ROOT / "Jenkinsfile.nifty-gex-service").read_text()
        old = "          withEnv([\"UNIQUE_TAG=${uniqueTag}\", "
        r = self._validate_mutated("Jenkinsfile.nifty-gex-service", old, "          withEnv([\"NIFTY_PERMITTED_SHA=${params.PERMITTED_SHA}\", \"UNIQUE_TAG=${uniqueTag}\", ")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("NIFTY_PERMITTED_SHA is re-assigned after the parameters{} block", r.stdout)

    def test_every_other_restructured_ship_is_a_dedicated_verified_step(self) -> None:
        tq = chr(39) * 3
        cases = [
            ("Jenkinsfile.es4-deploy", "        sh 'rsync -az --delete infra/es4 \"${ES4_HOST}:/home/es4/repo/infra/\"'\n",
             "        sh " + tq + "\n          set -euo pipefail\n          rsync -az --delete infra/es4 \"$ES4_HOST\":/home/es4/repo/infra/\n        " + tq + "\n", "is not a DEDICATED effect step"),
            ("Jenkinsfile.kafka-reset", "        sh 'scp -o BatchMode=yes -o StrictHostKeyChecking=accept-new scripts/ops/daily-kafka-reset.sh",
             "        sh 'cp /tmp/x scripts/ops/daily-kafka-reset.sh; scp -o BatchMode=yes -o StrictHostKeyChecking=accept-new scripts/ops/daily-kafka-reset.sh", "does not fit the fixed `scp` template"),
            ("Jenkinsfile.loki", "-e \"confirm_loki_deploy=${CONFIRM_DEPLOY}\"'", "-e @/tmp/vars.yml'", "never a file"),
            ("Jenkinsfile.archive-scripts-deploy", "        sh 'scp -o BatchMode=yes scripts/ops/archive/", "        sh 'scp -o BatchMode=yes \"$EXTRA\" scripts/ops/archive/", "every scp source must be a literal path"),
        ]
        for name, old, new, say in cases:
            r = self._validate_mutated(name, old, new)
            self.assertEqual(r.returncode, 1, name + r.stdout)
            self.assertIn(say, r.stdout, name)
        # removing a verify in front of a restructured ship is refused
        for name, eff in [("Jenkinsfile.es4-deploy", "        sh 'rsync -az --delete infra/es4 "), ("Jenkinsfile.kafka-reset", "        sh 'scp "),
                          ("Jenkinsfile.es-predown", "          sh 'scp -o BatchMode=yes scripts/ops/es-predown.sh"),
                          ("Jenkinsfile.archive-scripts-deploy", "        sh 'scp -o BatchMode=yes scripts/ops/archive/"),
                          ("Jenkinsfile.loki", "            sh 'ansible-playbook ")]:
            t = (ROOT / name).read_text()
            i = t.index(eff)
            j = t.rindex("timeout(time: 10, unit: 'MINUTES') {", 0, i)
            j = t.rindex("\n", 0, j) + 1
            r = self._validate_mutated(name, t[j:i] + eff, eff)
            self.assertEqual(r.returncode, 1, name + r.stdout)
            self.assertIn("no dedicated verify-permitted-tree step immediately precedes it", r.stdout, name)

    def test_the_archive_ship_is_exactly_the_unit(self) -> None:
        t = (ROOT / "Jenkinsfile.archive-scripts-deploy").read_text()
        unit = re.search(r'^    UNIT\s*=\s*"([^"]*)"', t, re.M).group(1).split()
        line = next(l.strip() for l in t.split("\n") if l.strip().startswith("sh 'scp -o BatchMode=yes scripts/ops/archive/"))
        srcs = line[len("sh 'scp -o BatchMode=yes "):].rsplit(" ", 1)[0].split()
        self.assertEqual([os.path.basename(x) for x in srcs], unit)
        for x in srcs:
            self.assertTrue((ROOT / x).is_file(), x)
        self.assertIn("scripts/jenkins/market_calendar.py", srcs)   # the committed file, not the gitignored staged copy

    def test_verify_permitted_tree_runtime_suite_passes(self) -> None:
        # Codex's replacement reproductions run as REAL git checkouts against verify-permitted-tree.sh.
        r = subprocess.run(["bash", str(J / "verify-permitted-tree-test.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("verify-permitted-tree-test: ALL PASS", r.stdout)

    def test_service_deploy_post_recovery_negated_with_a_space(self) -> None:
        r = self._validate_mutated("Jenkinsfile.service-deploy", "        if (env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' && env.SECONDARY_PERMISSIONS_PASSED == 'PASSED' && env.EFFECT_STAGE_STARTED == 'PASSED') {",
                                   "        if (! (env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED')) {")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("post-deploy-recovery.sh (may scale a deployment) is not inside the true branch", r.stdout)

    def test_kafka_cleanup_post_compared_to_false(self) -> None:
        t = (ROOT / "Jenkinsfile.kafka-cleanup").read_text()
        m = re.search(r"if \((env\.PERMITTED_SHA_GUARD == 'PASSED'[^)]*)\) \{", t[t.index("post {"):])
        self.assertIsNotNone(m)
        r = self._validate_mutated("Jenkinsfile.kafka-cleanup", m.group(0), f"if (({m.group(1)}) == false) {{")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("is not inside the true branch", r.stdout)

    def test_effect_stage_gate_removed_from_a_real_stage(self) -> None:
        r = self._validate_mutated("Jenkinsfile.service-deploy", "    stage('Deploy (service-scoped)') {\n      when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' } }\n",
                                   "    stage('Deploy (service-scoped)') {\n")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("stage 'Deploy (service-scoped)' after the guard has no `when` gate", r.stdout)


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
