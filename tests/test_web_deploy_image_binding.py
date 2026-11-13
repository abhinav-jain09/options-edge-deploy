# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""A web deploy NAMES the image it ships; it never resolves a mutable tag.

THE RACE. Jenkinsfile.web-service has two routes into its Deploy stage and until this change only
one of them bound an image. BUILD_IMAGE=true triggers options-edge-web-deploy under
WEB_PERMITTED_SHA, reads that build's image lock and deploys exactly its digest. BUILD_IMAGE=false -
the DEFAULT - skipped that stage, left REQUIRED_IMAGE empty, and bind-required-image.sh reads an
empty REQUIRED_IMAGE as "keep the mutable-tag pin": apply.sh then shipped whatever :dev resolved to
at that instant, which any other web build could have moved. A WEB_PERMITTED_SHA passed on that path
was read by nothing at all, while write-permission-receipt.sh recorded it as though it had bound
something.

WHAT THIS MODULE CHECKS, AND HOW - the two halves differ in strength and the difference is the point:

  1. EXECUTED. Every refusal in scripts/jenkins/resolve-existing-web-image.sh is driven by actually
     running the script with a parameter environment and asserting the exit status and the reason.
     That script validates all of its parameters BEFORE it reaches the network, which is what makes
     this possible with no Jenkins controller, no registry and no credentials. These tests fail if a
     refusal is deleted, widened, or turned into a default.

  2. STRUCTURAL - and it claims no more than that. The Jenkinsfile's own `when` conditions and its
     pre-apply check are read as TEXT and the boolean conditions are re-evaluated in Python. That
     proves the two stages' conditions are exact complements, so every route into Deploy passes
     through one of them. It does NOT prove Jenkins executes the file that way: no test here runs
     Groovy. Said plainly so nobody reads this module as an end-to-end proof of the pipeline.
"""
from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/jenkins/resolve-existing-web-image.sh"
JENKINSFILE = ROOT / "Jenkinsfile.web-service"

SHA = "a" * 40
DIGEST = "sha256:" + "b" * 64
REF = f"host.docker.internal:5001/options-edge-web:dev@{DIGEST}"


def run(**env: str) -> subprocess.CompletedProcess:
    """Run the resolver with exactly this parameter environment.

    `env` is passed as the WHOLE environment, not merged into the test runner's: a stray
    WEB_IMAGE_REF or JENKINS_URL inherited from CI would otherwise change what is under test. PATH is
    supplied because the script calls grep.
    """
    return subprocess.run(
        ["bash", str(SCRIPT)],
        env={"PATH": "/usr/bin:/bin:/usr/local/bin", **env},
        capture_output=True, text=True, cwd=str(ROOT),
    )


class ResolverRefuses(unittest.TestCase):
    """The refusals, executed. Each asserts the exit status AND that the reason names the problem --
    a refusal for the wrong reason would send the operator to the wrong parameter."""

    def test_no_identity_at_all_is_refused(self):
        """THE DEFECT ITSELF: the default path, with nothing named, used to deploy the mutable tag."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false")
        self.assertEqual(1, r.returncode, r.stderr)
        self.assertIn("REFUSED", r.stderr)
        self.assertIn("must say WHICH one", r.stderr)
        # The refusal has to be actionable: it names BOTH accepted forms.
        self.assertIn("WEB_PERMITTED_SHA + WEB_IMAGE_BUILD_NUMBER", r.stderr)
        self.assertIn("WEB_IMAGE_REF", r.stderr)
        self.assertEqual("", r.stdout)

    def test_a_commit_without_its_build_number_is_refused(self):
        """Half of form 1. A commit does not identify an image: the same commit can be built many
        times, and the lock that proves what was built belongs to one BUILD."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false", WEB_PERMITTED_SHA=SHA)
        self.assertEqual(1, r.returncode)
        self.assertIn("without WEB_IMAGE_BUILD_NUMBER", r.stderr)
        self.assertEqual("", r.stdout)

    def test_a_build_number_without_its_commit_is_refused(self):
        """The other half. A build number alone would deploy that build's image without verifying
        the commit it was built from -- the substitution the resolver exists to prevent."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false", WEB_IMAGE_BUILD_NUMBER="42")
        self.assertEqual(1, r.returncode)
        self.assertIn("without WEB_PERMITTED_SHA", r.stderr)
        self.assertEqual("", r.stdout)

    def test_both_forms_together_are_refused_as_ambiguous(self):
        """Not resolved by preference. Two answers to "what deploys" that could disagree must not be
        silently reconciled -- and the refusal must say AMBIGUOUS, not "missing field"."""
        for extra in ({"WEB_PERMITTED_SHA": SHA}, {"WEB_IMAGE_BUILD_NUMBER": "42"},
                      {"WEB_PERMITTED_SHA": SHA, "WEB_IMAGE_BUILD_NUMBER": "42"}):
            with self.subTest(extra=sorted(extra)):
                r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false", WEB_IMAGE_REF=REF, **extra)
                self.assertEqual(1, r.returncode)
                self.assertIn("both image forms were given", r.stderr)
                self.assertEqual("", r.stdout)

    def test_a_bare_tag_is_refused(self):
        """WEB_IMAGE_REF without a digest is the very moving target the parameter replaces."""
        for bad in ("host.docker.internal:5001/options-edge-web:dev",
                    "host.docker.internal:5001/options-edge-web",
                    f"host.docker.internal:5001/options-edge-web:dev@{DIGEST.upper()}",
                    "host.docker.internal:5001/options-edge-web:dev@sha256:" + "b" * 63,
                    f"has space/options-edge-web:dev@{DIGEST}",
                    f"@{DIGEST}"):
                with self.subTest(ref=bad):
                    r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false", WEB_IMAGE_REF=bad)
                    self.assertEqual(1, r.returncode, f"accepted {bad!r}")
                    self.assertIn("digest-pinned reference", r.stderr)
                    self.assertEqual("", r.stdout)

    def test_a_malformed_commit_or_build_number_is_refused(self):
        for sha, build, why in (("abc123", "42", "40-character"),
                                (SHA.upper(), "42", "40-character"),
                                (SHA, "4x", "plain build number"),
                                (SHA, "-1", "plain build number")):
            with self.subTest(sha=sha, build=build):
                r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false",
                        WEB_PERMITTED_SHA=sha, WEB_IMAGE_BUILD_NUMBER=build)
                self.assertEqual(1, r.returncode)
                self.assertIn(why, r.stderr)
                self.assertEqual("", r.stdout)

    def test_the_build_path_may_not_also_name_an_image(self):
        """BUILD_IMAGE=true mints its identity from the child build it triggers. A second, quieter
        answer here is refused rather than preferred over or merged with that one."""
        for extra in ({"WEB_IMAGE_REF": REF}, {"WEB_IMAGE_BUILD_NUMBER": "42"}):
            with self.subTest(extra=sorted(extra)):
                r = run(BUILD_IMAGE="true", DEPLOY_DRY_RUN="false", WEB_PERMITTED_SHA=SHA, **extra)
                self.assertEqual(1, r.returncode)
                self.assertIn("mints its own image identity", r.stderr)
                self.assertEqual("", r.stdout)

    def test_an_unrecognised_boolean_is_refused_not_guessed(self):
        """`BUILD_IMAGE=ture` read as false would demand an identity the build path was going to
        mint; read as true it would skip the requirement. Neither guess is sound."""
        r = run(BUILD_IMAGE="ture", DEPLOY_DRY_RUN="false")
        self.assertEqual(1, r.returncode)
        self.assertIn("BUILD_IMAGE must be exactly", r.stderr)
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="yes")
        self.assertEqual(1, r.returncode)
        self.assertIn("DEPLOY_DRY_RUN must be exactly", r.stderr)


class ResolverAccepts(unittest.TestCase):
    """The accepted forms, executed -- so a resolver that refuses everything also fails."""

    def test_a_digest_reference_is_printed_verbatim_on_stdout(self):
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false", WEB_IMAGE_REF=REF)
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual(REF, r.stdout.strip())

    def test_the_digest_form_says_it_carries_no_commit_claim(self):
        """A digest alone does not say what source built it, and the operator is told so rather than
        left to infer it from the absence of a commit in the log."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false", WEB_IMAGE_REF=REF)
        self.assertIn("NO commit", r.stderr)

    def test_the_digest_form_says_the_tag_is_not_what_binds(self):
        """The registry carries an immutable `<build>-<sha12>` tag per build beside the floating one,
        so an operator may reasonably put it in WEB_IMAGE_REF and believe that pins the build. It does
        not: bind-required-image.sh compares repository names with the tag stripped and then returns
        the RENDER's repo:tag with this digest, so the tag written here never reaches the pod spec.
        A tag disagreeing with its digest would change nothing about what deploys while reading in the
        log as though it had, so the resolver says which half is load-bearing."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false",
                WEB_IMAGE_REF=f"host.docker.internal:5001/options-edge-web:1700-c597c06392f7@{DIGEST}")
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("only the digest binds", r.stderr)

    def test_an_immutable_per_build_tag_is_accepted_not_required(self):
        """Both tag shapes are accepted. Requiring the immutable tag would refuse the very value an
        operator reads off a running pod -- `bind_required_image` puts the RENDER's tag in the pod
        spec, so a pod pinned by this job reads `options-edge-web:dev@sha256:...` however it was
        deployed. A refusal there would buy nothing and block the form's main use."""
        for tag in ("dev", "1700-c597c06392f7", "prod"):
            with self.subTest(tag=tag):
                r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false",
                        WEB_IMAGE_REF=f"host.docker.internal:5001/options-edge-web:{tag}@{DIGEST}")
                self.assertEqual(0, r.returncode, r.stderr)
                self.assertEqual(f"host.docker.internal:5001/options-edge-web:{tag}@{DIGEST}",
                                 r.stdout.strip())

    def test_the_build_path_prints_nothing_and_passes(self):
        """It mints its own identity downstream; this resolver only checks it was not also given one."""
        r = run(BUILD_IMAGE="true", DEPLOY_DRY_RUN="false", WEB_PERMITTED_SHA=SHA)
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual("", r.stdout)

    def test_a_dry_run_needs_no_identity_but_says_what_it_is_not(self):
        """DEPLOY_DRY_RUN applies nothing, so it is exempt -- the same exemption
        scripts/deploy/require-batch-image-binding.sh makes. But its render then pins whatever the
        tag resolves to now, so it must NOT be read as evidence about a particular image."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="true")
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual("", r.stdout)
        self.assertIn("not evidence", r.stderr)

    def test_a_dry_run_still_honours_an_identity_it_is_given(self):
        """So a dry run CAN be made to describe exactly one image."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="true", WEB_IMAGE_REF=REF)
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual(REF, r.stdout.strip())

    def test_the_reviewed_form_delegates_to_the_image_lock_fetcher(self):
        """Form 1's happy path needs a Jenkins controller, so what is checked here is the HANDOFF:
        with well-formed parameters the resolver stops validating and calls
        fetch-permitted-image-lock.sh, which then refuses for its own reason (no JENKINS_URL). A
        resolver that answered from its own parameters -- never consulting the lock that proves what
        was built -- would not produce this message."""
        r = run(BUILD_IMAGE="false", DEPLOY_DRY_RUN="false",
                WEB_PERMITTED_SHA=SHA, WEB_IMAGE_BUILD_NUMBER="42")
        self.assertEqual(1, r.returncode)
        self.assertIn("fetch-permitted-image-lock", r.stderr)
        self.assertIn("JENKINS_URL", r.stderr)
        self.assertEqual("", r.stdout)


# --------------------------------------------------------------------------------------------------
# STRUCTURAL half. Text, not execution -- see the module docstring.
GATE = r"when \{ expression \{ env\.PERMITTED_SHA_GUARD == 'PASSED' && env\.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' && \((?P<cond>.*)\) \} \}"


def groovy_cond_to_python(cond: str) -> str:
    """Translate the `params.X`/&&/||/! subset the two gates use into a Python expression."""
    out = re.sub(r"params\.([A-Z_]+)", r"p['\1']", cond)
    out = out.replace("&&", " and ").replace("||", " or ")
    out = re.sub(r"!(?=[\s(p])", " not ", out)
    return out


def stage_condition(text: str, stage: str) -> str:
    """The trailing `(condition)` of that stage's canonical `when` gate."""
    body = text.split(f"stage('{stage}')", 1)
    assert len(body) == 2, f"stage {stage!r} not found"
    m = re.search(GATE, body[1][:900])
    assert m, f"stage {stage!r} has no canonical when gate in its header"
    return m.group("cond")


class JenkinsfileWiring(unittest.TestCase):
    def setUp(self):
        self.text = JENKINSFILE.read_text()

    def test_the_two_stage_conditions_are_exact_complements(self):
        """THE INVARIANT THAT CLOSES THE PATH, rather than one of its cases: for every combination of
        BUILD_IMAGE and DEPLOY_DRY_RUN exactly ONE of the Build and Bind stages runs, so no route
        into Deploy skips both. BUILD_IMAGE=true with DEPLOY_DRY_RUN=true is the case that a
        hand-written `!params.BUILD_IMAGE` would have missed: the Build stage excludes dry runs, so
        that combination used to reach Deploy with no identity either."""
        build = groovy_cond_to_python(stage_condition(self.text, "Build web image (native)"))
        bind = groovy_cond_to_python(stage_condition(self.text, "Bind existing web image"))
        for bi in (True, False):
            for dr in (True, False):
                p = {"BUILD_IMAGE": bi, "DEPLOY_DRY_RUN": dr}
                ran = [bool(eval(build, {}, {"p": p})), bool(eval(bind, {}, {"p": p}))]
                with self.subTest(BUILD_IMAGE=bi, DEPLOY_DRY_RUN=dr):
                    self.assertEqual(1, sum(ran),
                                     f"Build={ran[0]} Bind={ran[1]} — exactly one must run")

    def test_the_bind_stage_runs_the_resolver_and_assigns_required_image(self):
        bind = self.text.split("stage('Bind existing web image')", 1)[1].split("stage('", 1)[0]
        self.assertIn("bash scripts/jenkins/resolve-existing-web-image.sh", bind)
        self.assertIn("env.REQUIRED_IMAGE = bound", bind)
        self.assertRegex(bind, r"\^\[\^@\\\\s\]\+@sha256:\[0-9a-f\]\{64\}\$",
                         "the resolved reference must be asserted digest-pinned before it is used")

    def test_the_deploy_stage_refuses_an_unbound_apply(self):
        """A LAST CHECK independent of which stage above ran: if the two `when` conditions ever stop
        being complements, this refuses instead of letting an unbound deploy through the gap."""
        deploy = self.text.split("stage('Deploy (service-scoped)')", 1)[1]
        # It must REFUSE, not warn. Asserting only that the message exists passed against a mutant
        # that had turned error() into echo() -- a check that printed the problem and deployed
        # anyway, which is the exact failure this whole change is about.
        self.assertRegex(deploy, r"""error\(["']no digest-pinned image was bound""",
                         "the unbound-deploy check must error(), not merely print")
        # Gated on the dry-run exemption, and placed BEFORE the sh step that applies anything.
        self.assertIn("!params.DEPLOY_DRY_RUN", deploy)
        self.assertLess(deploy.index("no digest-pinned image was bound"),
                        deploy.index("service-deploy.sh"),
                        "the check must come before the apply")

    def test_every_new_parameter_is_declared(self):
        for name in ("WEB_IMAGE_BUILD_NUMBER", "WEB_IMAGE_REF"):
            self.assertIn(f"string(name: '{name}'", self.text)
            # trim: true -- a parameter pasted with a trailing newline must not read as malformed.
            block = self.text.split(f"string(name: '{name}'", 1)[1][:200]
            self.assertIn("trim: true", block, f"{name} must be trimmed")

    def test_the_parameter_descriptions_no_longer_promise_the_old_behaviour(self):
        """BUILD_IMAGE=false used to be documented as "deploy the image already in the registry",
        which read as "no further input needed". WEB_PERMITTED_SHA was documented as mattering only
        when BUILD_IMAGE=true, which is what made passing it on the other path look meaningful."""
        self.assertNotIn("false (DEFAULT) = deploy the image already in the registry", self.text)
        self.assertNotIn("REQUIRED when BUILD_IMAGE=true — Deployment Permission Rule. The full 40",
                         self.text)


if __name__ == "__main__":
    unittest.main()
