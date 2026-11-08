# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
import os, shutil, subprocess, sys, tempfile, unittest
GEN = os.path.join(os.path.dirname(__file__), "../scripts/ci/schema-r6-backlog.py")
AVRO = "import com.optionsedge.contracts.hpsf.UnderlyingPriceAvro;\nclass A {}\n"


class Backlog(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp(); self.bl = os.path.join(self.d, "backlog.txt")

    def tearDown(self):
        shutil.rmtree(self.d)

    def module(self, name, text=AVRO, req=False):
        p = os.path.join(self.d, "p", name, "src/main/java"); os.makedirs(p, exist_ok=True)
        with open(os.path.join(p, "A.java"), "w") as f: f.write(text)
        if req:
            r = os.path.join(self.d, "p", name, "src/main/resources"); os.makedirs(r, exist_ok=True)
            with open(os.path.join(r, "schema-requirements.json"), "w") as f: f.write("{}")

    def run_it(self, backlog_lines):
        with open(self.bl, "w") as f: f.write("".join(l + "\n" for l in backlog_lines))
        return subprocess.run([sys.executable, GEN, os.path.join(self.d, "p"), "--backlog", self.bl], capture_output=True, text=True)

    def test_listed_module_passes_and_count_is_printed(self):
        self.module("a-service")
        r = self.run_it(["a-service"]); self.assertEqual(r.returncode, 0, r.stdout); self.assertIn("1 of 1", r.stdout)

    def test_a_new_avro_module_without_the_file_and_off_the_list_fails(self):
        self.module("a-service"); self.module("b-service")
        r = self.run_it(["a-service"]); self.assertNotEqual(r.returncode, 0); self.assertIn("b-service uses Avro", r.stdout)

    def test_a_listed_module_that_gained_the_file_must_leave_the_list(self):
        self.module("a-service", req=True)
        r = self.run_it(["a-service"]); self.assertNotEqual(r.returncode, 0); self.assertIn("now has schema-requirements.json", r.stdout)

    def test_a_stale_entry_fails_and_a_non_avro_module_is_not_counted(self):
        self.module("a-service"); self.module("plain-service", "class B {}\n")
        r = self.run_it(["a-service", "gone-service"]); self.assertNotEqual(r.returncode, 0); self.assertIn("no longer exists", r.stdout)
        self.assertEqual(self.run_it(["a-service"]).returncode, 0)


if __name__ == "__main__":
    unittest.main()
