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

    def test_this_copy_knows_every_group_the_processing_pipeline_defines(self):
        """The divergence that broke the gate for three days, pinned.

        This repo's validator judges the PROCESSING repo's Jenkinsfile, so a group defined there and
        unknown here fails the WHOLE file and refuses every image build. The processing checkout is
        not available in CI, so the list is pinned here and the processing repo's own
        tests/test_processing_image_groups.py pins the same names on its side: a new group has to be
        added in both, and whichever side is forgotten goes red.
        """
        self.assertEqual(
            ("buildCoreImages", "buildFlowImages", "buildStructureImages", "buildResearchImages",
             "buildExecutionImages", "buildFeedImages", "buildScoringImages", "buildBreakLineImages",
             "buildGammaZoneImages"),
            guard.IMAGE_GROUP_METHODS,
            "this copy's image-group list has drifted from the processing pipeline's. A group the "
            "processing Jenkinsfile defines but this list does not know makes the whole file fail to "
            "validate, and the deploy gate then refuses every processing image build.")

    def test_a_group_this_copy_does_not_know_is_refused_with_a_sequence_error(self):
        """Why the drift was hard to see: the message does not name the unknown method."""
        source = grouped_jenkinsfile().replace("buildCoreImages", "buildSomethingNobodyDeclared")
        _view, _map, problems = guard.expand_image_groups(source)
        # It fails -- which is the safe direction -- but the reason it gives is about the call
        # sequence, not about an unrecognised group. The assertion above is what actually catches
        # the drift; this one records WHY a reader of the failure could not tell.
        self.assertTrue(problems, "an unknown group must not validate")

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
