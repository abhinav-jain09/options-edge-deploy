# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
#
# PROMOTED from CI-COLLECT-ONLY on 2026-11-11, by the defect it would have caught. This repo's COPY of
# validate-jenkinsfile-guard.py judges the PROCESSING repo's Jenkinsfile, and its IMAGE_GROUP_METHODS was
# never updated when buildBreakLineImages was added there on 2026-11-08. The whole Jenkinsfile then failed
# to validate and the deploy gate refused EVERY processing image build for three days - with a message
# about the call SEQUENCE that named neither the unknown method nor the divergence. The module sat here
# dormant throughout: imported, never executed. A test that does not run is not a test.
"""Caller-side coverage for the processing pipeline's bounded image groups."""
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts/jenkins/validate-jenkinsfile-guard.py"
SPEC = importlib.util.spec_from_file_location("deploy_guard", VALIDATOR)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)

VERIFY = """    timeout(time: 10, unit: 'MINUTES') {
      sh 'PERMITTED_SHA="${PERMITTED_SHA:-}" bash scripts/jenkins/verify-permitted-tree.sh --dir . --allow-ignored target'
    }
"""


def grouped_jenkinsfile():
    """A compact, complete child defining EVERY group in the validator's own allow-list.

    Derived from ``guard.IMAGE_GROUP_METHODS`` rather than a hardcoded list of names. The hardcoded
    version named six groups and rotted the moment a seventh was added: the validator then demanded
    a definition and a call for groups the fixture did not have, and both of this module's
    assertions failed -- silently, because the module was dormant and never executed. A fixture that
    is generated from the list under test cannot drift from it.
    """
    names = [m[len("build"):-len("Images")] for m in guard.IMAGE_GROUP_METHODS]
    groups = []
    for name in names:
        groups.append(f"""def build{name}Images() {{
  buildImage('options-edge-{name.lower()}', 'Dockerfile', '.', '{name.upper()}_IMAGE', false) {{
{VERIFY}    sh 'docker build -t app .'
  }}
}}
""")
    calls = "\n".join(f"              build{name}Images()" for name in names)
    digest = hashlib.sha256((ROOT / "scripts/jenkins/permitted-sha-guard.sh").read_bytes()).hexdigest()
    return """def buildImage(String image, String dockerfile, String context, String imageVar, boolean baseImage, Closure localBuild) {
  localBuild()
}
""" + "\n".join(groups) + f"""pipeline {{
  agent any
  parameters {{
    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')
    string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '{digest}', description: 'guard version')
  }}
  options {{ disableRestartFromStage() }}
  stages {{
    stage('Permitted commit guard') {{
      options {{ timeout(time: 10, unit: 'MINUTES') }}
      steps {{
        script {{
          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')
          if (rc != 0) {{
            error("refused (rc=${{rc}})")
          }}
          env.PERMITTED_SHA_GUARD = 'PASSED'
        }}
      }}
    }}
    stage('Build Images') {{
      when {{ expression {{ env.PERMITTED_SHA_GUARD == 'PASSED' }} }}
      steps {{
        script {{
          def imagesEnv = []
          withEnv(imagesEnv) {{
{calls}
          }}
        }}
      }}
    }}
  }}
}}
"""


class ProcessingImageGroupValidatorTest(unittest.TestCase):
    def validate(self, source):
        digest = hashlib.sha256((ROOT / "scripts/jenkins/permitted-sha-guard.sh").read_bytes()).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            pipeline = Path(directory) / "Jenkinsfile"
            manifest = Path(directory) / "scope.txt"
            pipeline.write_text(source)
            manifest.write_text("Jenkinsfile | in | | processing image group fixture\n")
            entry = guard.parse_manifest(str(manifest))["Jenkinsfile"]
            return guard.check_in_scope(str(pipeline), entry, digest, str(ROOT))

    def test_this_copys_group_list_is_the_reviewed_one(self):
        """A LOCAL expected-contract pin. It does NOT prove the two repos agree.

        This repo's validator judges the PROCESSING repo's Jenkinsfile, and a group defined there
        and unknown here fails the WHOLE file -- which refused every processing image build for
        three days from 2026-11-08. This pin catches an accidental local edit to the list, and
        makes a deliberate one a reviewed diff.

        What it CANNOT do, and an earlier version of this comment wrongly claimed it could: prove
        the lists in the two repos match. Processing can add a group and update its own pin while
        this repo's CI stays green, because the processing checkout is not available here. The
        divergence still surfaces only when this caller validates that downstream Jenkinsfile, at
        deploy time. Closing that properly needs one canonical versioned group contract consumed by
        both repos, or a CI check that fetches and parses the real processing Jenkinsfile. Neither
        exists yet, and this pin is not a substitute for either.
        """
        self.assertEqual(
            ("buildCoreImages", "buildFlowImages", "buildStructureImages", "buildResearchImages",
             "buildExecutionImages", "buildFeedImages", "buildScoringImages", "buildBreakLineImages",
             "buildGammaZoneImages"),
            guard.IMAGE_GROUP_METHODS,
            "this copy's image-group list changed. It must match the groups the processing "
            "Jenkinsfile defines, or the deploy gate refuses every processing image build.")

    def test_a_group_this_copy_does_not_know_fails_the_WHOLE_file_on_the_call_sequence(self):
        """The real drift shape, and the exact diagnostic it produces.

        Not a renamed existing group -- that is a different failure. The shape that actually
        happened is: the processing Jenkinsfile keeps every group this copy knows AND adds one more,
        defined and called like the rest. The extra call then sits among the recognised ones, and
        the validator refuses the file for the call SEQUENCE -- naming neither the unknown method
        nor the divergence, which is why the three-day outage was read as a call-site mistake.
        """
        source = grouped_jenkinsfile()
        extra_def = """def buildSomethingThisCopyNeverHeardOfImages() {
  buildImage('options-edge-extra', 'Dockerfile', '.', 'EXTRA_IMAGE', false) {
""" + VERIFY + """    sh 'docker build -t app .'
  }
}
"""
        source = extra_def + source
        last_known = f"              build{guard.IMAGE_GROUP_METHODS[-1][len('build'):-len('Images')]}Images()"
        assert last_known in source, last_known
        source = source.replace(
            last_known, last_known + "\n              buildSomethingThisCopyNeverHeardOfImages()", 1)

        _view, _mapping, problems = guard.expand_image_groups(source)
        self.assertEqual(
            ["image-group helpers: calls must form a consecutive unconditional sequence"], problems,
            "the drift must fail -- and this is the exact message it fails with, which names "
            "neither the unknown method nor the divergence")

    def test_groups_are_expanded_and_every_effect_remains_guarded(self):
        source = grouped_jenkinsfile()
        expanded, mapping, problems = guard.expand_image_groups(source)
        self.assertEqual([], problems)
        # one per group in the allow-list, not a hardcoded 6 -- the same drift that rotted the
        # fixture above
        self.assertEqual(len(guard.IMAGE_GROUP_METHODS),
                         expanded.count("sh 'docker build -t app .'"))
        self.assertEqual(len(expanded.split("\n")), len(mapping))
        self.assertEqual([], self.validate(source))

    def test_missing_verify_inside_group_is_refused(self):
        problems = self.validate(grouped_jenkinsfile().replace(VERIFY, "", 1))
        self.assertTrue(any("no dedicated verify-permitted-tree step" in p for p in problems), problems)

    def test_indirect_or_conditional_group_dispatch_is_refused(self):
        source = grouped_jenkinsfile()
        direct = "              buildCoreImages()\n"
        for replacement in ("              this.buildCoreImages()\n",
                            "              if (true) {\n" + direct + "              }\n"):
            _, _, problems = guard.expand_image_groups(source.replace(direct, replacement, 1))
            self.assertTrue(problems, replacement)


if __name__ == "__main__":
    unittest.main()
