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
    """Return a compact, complete child with the six processing image groups."""
    names = ("Core", "Flow", "Structure", "Research", "Execution", "Feed")
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

    def test_groups_are_expanded_and_every_effect_remains_guarded(self):
        source = grouped_jenkinsfile()
        expanded, mapping, problems = guard.expand_image_groups(source)
        self.assertEqual([], problems)
        self.assertEqual(6, expanded.count("sh 'docker build -t app .'"))
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
