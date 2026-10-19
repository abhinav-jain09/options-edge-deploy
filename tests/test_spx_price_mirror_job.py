"""Jenkinsfile.spx-price-reencoder: the job that replaces the two byte copiers of underlying.spx.price (the spx-es4-bridge console pipe and
the dev->.74 MirrorMaker whitelist entry). Structural contract plus the one executable guard worth running: the check that the MM1
whitelist no longer carries the topic."""
import re, subprocess, tempfile, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOB = (ROOT / "Jenkinsfile.spx-price-reencoder").read_text()
SCOPE = (ROOT / "scripts/ci/jenkins-permitted-sha-scope.txt").read_text()
CODE = "\n".join(l for l in JOB.splitlines() if not l.lstrip().startswith(("//", "#")))


class SpxPriceMirrorJob(unittest.TestCase):
    def test_the_two_paths_are_exactly_the_profile_table(self):
        self.assertIn("'192.168.100.252:9092' : '127.0.0.1:19092'", JOB)
        self.assertIn("'192.168.100.4:9092' : '192.168.100.74:9092'", JOB)
        self.assertIn("'es.underlying.spx.price' : 'underlying.spx.price'", JOB)
        self.assertIn("'http://192.168.100.4:8081' : 'http://192.168.100.74:8082'", JOB)
        self.assertIn("choice(name: 'TARGET', choices: ['es4', 'mac74']", JOB)

    def test_no_byte_copier_is_used(self):
        for banned in ("kafka-console-producer", "kafka-mirror-maker", "connect-mirror-maker", "kcat", "rm -rf"):
            self.assertNotIn(banned, CODE, banned)
        self.assertNotIn(".v2", CODE)   # no versioned topic names
        self.assertIn("SpxPriceAvroMirror.java", CODE)

    def test_the_mirror_does_not_share_the_mm1_consumer_group(self):
        self.assertIn('GROUP     = "spx-price-mirror-${params.TARGET}"', JOB)
        self.assertNotIn("GROUP     = \"oe-mac74-input-from-dev\"", JOB)

    def test_the_old_es4_bridge_is_retired_before_any_target_state_check(self):
        retire = JOB.index("com.optionsedge.spx-es4-bridge.plist")
        self.assertLess(retire, JOB.index("assert-target-fresh.sh"))
        self.assertLess(retire, JOB.index("assert-subject-parity.sh"))
        self.assertLess(JOB.index("Preflight (fail-closed)"), retire)
        self.assertLess(retire, JOB.index("stage('Preflight (registries, topics, group)')"))
        self.assertNotIn("spx-es4-bridge.plist", JOB[JOB.index("stage('Install')"):], "Install must not be where the copier is retired")
        self.assertIn(".retired-by-spx-price-mirror", JOB)

    def test_the_gates_run_by_literal_repository_path_and_the_job_is_in_the_guard_scope(self):
        for s in ("assert-subject-parity.sh", "verify-target-record.sh"):
            self.assertIn(f"scripts/kafka/spx-price-mirror/{s}", JOB)
        self.assertRegex(SCOPE, r"(?m)^Jenkinsfile\.spx-price-reencoder\s+\| in ")
        self.assertIn("bash scripts/jenkins/permitted-sha-guard.sh", JOB)

    def test_preflight_gates_are_wired(self):
        self.assertIn('assert-timestamp-type.sh" "$TGT_BS" "$TGT_TOPIC"', JOB)
        self.assertIn('"$SRC_REG" "$TGT_REG" "$SRC_BS" "$SRC_TOPIC"', JOB)
        self.assertRegex(JOB, r'CHECK_FIRST=true ALLOW_EMPTY=true bash "scripts/kafka/spx-price-mirror/verify-target-record\.sh" "\$SRC_BS"')
        self.assertRegex(JOB, r'CHECK_FIRST=true ALLOW_EMPTY=true bash "scripts/kafka/spx-price-mirror/verify-target-record\.sh" "\$TGT_BS"')

    def test_flow_is_proven_by_the_mirror_group_not_the_target_offset(self):
        self.assertIn("verify-flow.sh", JOB)
        self.assertIn("assert-target-fresh.sh", JOB)
        self.assertNotIn("TGT1", JOB)

    def test_the_mm1_guard_is_a_script_run_by_literal_path(self):
        self.assertIn('bash "scripts/kafka/spx-price-mirror/assert-mm1-whitelist-clean.sh" /Users/abhinav/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh', JOB)

    def test_the_mm1_guard_fails_on_every_spelling_of_the_topic_and_passes_when_absent(self):
        guard = ROOT / "scripts/kafka/spx-price-mirror/assert-mm1-whitelist-clean.sh"
        def rc(text):
            with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as f:
                f.write(text)
            return subprocess.run(["bash", str(guard), f.name], capture_output=True, text=True).returncode
        self.assertEqual(rc(r"--whitelist 'a\.b|underlying\.spx\.price|options\.ibkr\.raw'"), 1)
        self.assertEqual(rc(r"--whitelist 'underlying\.es\.trades|underlying\.spx\.price'"), 1)
        self.assertEqual(rc("--whitelist 'underlying[.]spx[.]price|x'"), 1)
        self.assertEqual(rc("--whitelist 'underlying.spx.price'"), 1)
        self.assertEqual(rc(r"--whitelist 'underlying\.spx\.index\.price|underlying\.vix\.price'"), 0)
        self.assertEqual(rc(r"--whitelist 'underlying\.spx\.price\.other'"), 0)
        self.assertEqual(subprocess.run(["bash", str(guard), "/nonexistent"], capture_output=True).returncode, 1)
        real = Path.home() / "oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh"
        if real.exists() and "underlying" in real.read_text():
            self.assertEqual(subprocess.run(["bash", str(guard), str(real)], capture_output=True).returncode,
                             1 if re.search(r"underlying\\\.spx\\\.price", real.read_text()) else 0)


if __name__ == "__main__":
    unittest.main()
