# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""schema-mirrors-generate.py (docs/schema-governance R3f): the committed activeMirrors header equals what the mirror jobs declare, and a
mirror that does not say how it moves records fails. Each negative control mutates one fact in a copy of a minimal job set."""
import importlib.util, json, os, shutil, subprocess, sys, tempfile, unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GEN = os.path.join(ROOT, "scripts/ci/schema-mirrors-generate.py")
spec = importlib.util.spec_from_file_location("gen", GEN)
gen = importlib.util.module_from_spec(spec); spec.loader.exec_module(gen)

JOB = """// MIRROR-DIRECTION: es4->default
// MIRROR-TRANSPORT: bytes
// MIRROR-TOPICS: @choice(TOPIC)
// MIRROR-SUBJECTS: none
pipeline {
  parameters {
    choice(name: 'TOPIC', choices: ['a.b', 'c.d'], description: 'x')
    choice(name: 'ACTION', choices: ['install', 'teardown'], description: 'x')
  }
}
"""


class Case(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.d, "schema-manifest"))
        json.dump({"entries": []}, open(os.path.join(self.d, "schema-manifest/manifest.json"), "w"))

    def tearDown(self):
        shutil.rmtree(self.d)

    def job(self, name, text):
        open(os.path.join(self.d, name), "w").write(text)

    def run_gen(self, *args):
        return subprocess.run([sys.executable, GEN, "--root", self.d, *args], capture_output=True, text=True)

    def test_a_declared_job_generates_one_record_per_target(self):
        self.job("Jenkinsfile.es-x-mirror", JOB)
        got = gen.generate(self.d)
        self.assertEqual([(m["from"], m["to"], m["transport"], m["unframed"], m["topics"]) for m in got],
                         [("es4", "dev", "bytes", True, ["a.b", "c.d"]), ("es4", "prod", "bytes", True, ["a.b", "c.d"])])

    def test_a_mirror_named_job_without_a_transport_marker_fails(self):
        self.job("Jenkinsfile.es-x-mirror", JOB.replace("// MIRROR-TRANSPORT: bytes\n", ""))
        r = self.run_gen()
        self.assertNotEqual(r.returncode, 0); self.assertIn("MIRROR-TRANSPORT", r.stderr)

    def test_a_job_with_a_marker_but_a_non_mirror_name_is_still_discovered(self):
        self.job("Jenkinsfile.hidden", JOB.replace("// MIRROR-TRANSPORT: bytes\n", ""))
        self.assertNotEqual(self.run_gen().returncode, 0)

    def test_a_bad_transport_fails(self):
        self.job("Jenkinsfile.es-x-mirror", JOB.replace("bytes", "copy"))
        self.assertIn("bytes or reencode", self.run_gen().stderr)

    def test_topics_that_differ_from_the_choice_parameter_fail(self):
        self.job("Jenkinsfile.es-x-mirror", JOB.replace("@choice(TOPIC)", "a.b"))
        self.assertIn("differs from the job's TOPIC choice", self.run_gen().stderr)

    def test_a_reencode_job_with_an_install_action_must_say_why_it_is_true(self):
        base = JOB.replace("bytes", "reencode")
        self.job("Jenkinsfile.es-x-mirror", base)
        self.assertIn("MIRROR-INSTALL: reencodes", self.run_gen().stderr)
        self.job("Jenkinsfile.es-x-mirror", base.replace("pipeline {", "// MIRROR-INSTALL: reencodes\npipeline {"))
        self.assertEqual(self.run_gen().returncode, 0)
        retired = base.replace("pipeline {", "// MIRROR-LEGACY-INSTALL: retired\npipeline {")
        self.job("Jenkinsfile.es-x-mirror", retired)
        self.assertIn("no stage named", self.run_gen().stderr)
        self.job("Jenkinsfile.es-x-mirror", retired + "stage('Legacy byte-copy install is retired (x)')\n")
        self.assertEqual(self.run_gen().returncode, 0)

    def test_an_ungoverned_path_stays_visible_and_is_not_dropped(self):
        self.job("Jenkinsfile.es-x-mirror", JOB.replace("// MIRROR-DIRECTION: es4->default", "// MIRROR-PATHS: prod->es4; dev->mac74"))
        got = gen.generate(self.d)
        self.assertEqual([(m["from"], m["to"], m.get("ungovernedPaths")) for m in got], [("prod", "es4", ["dev->mac74"])])

    def test_check_detects_drift_in_both_directions(self):
        self.job("Jenkinsfile.es-x-mirror", JOB)
        self.assertEqual(self.run_gen().returncode, 0)
        self.assertEqual(self.run_gen("--check").returncode, 0)
        self.job("Jenkinsfile.es-x-mirror", JOB.replace("none", "some.Subject"))
        r = self.run_gen("--check")
        self.assertNotEqual(r.returncode, 0); self.assertIn("MISSING", r.stderr); self.assertIn("STALE", r.stderr)

    def test_the_committed_header_matches_the_real_jobs(self):
        r = subprocess.run([sys.executable, GEN, "--root", ROOT, "--check"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()
