"""Line health milestone M1, deploy repository: the status schema snapshot, the per-environment schema gate scope, the configuration ConfigMap and its validator.
Every guard has a test that makes it fail (the mutations below are the proof; each is a separate case so one cannot hide another)."""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCOPE = ROOT / "scripts/ci/schema-sync-scope.sh"
VALIDATOR = ROOT / "scripts/ci/validate-line-health-config.sh"
CONFIG_FILE = "k8s/overlays/dev/level-absorption-config.yaml"
PATCH = "k8s/overlays/dev/level-absorption-dev-enable-patch.yaml"


def scope(service, environment, manifest=None, exemptions=None):
    env = dict(os.environ)
    if manifest:
        env["SCHEMA_MANIFEST"] = str(manifest)
    if exemptions:
        env["SCHEMA_EXEMPTIONS"] = str(exemptions)
    return subprocess.run(["bash", str(SCOPE), service, environment], capture_output=True, text=True, env=env)


class StatusSnapshot(unittest.TestCase):
    def test_the_status_snapshot_follows_contracts_0_81_0_with_display_values_then_epoch_sequence_count(self):
        text = (ROOT / "schema-manifest/schemas/com.optionsedge.contracts.linehealth.LineHealthStatus/v1.avsc").read_text()
        fields = json.loads(text)["fields"]
        self.assertEqual([f["name"] for f in fields[:5]], ["schemaVersion", "asOfMs", "displayStaleSeconds", "retiredDisplayMinutes", "statusEpoch"])
        self.assertEqual(fields[6], {"name": "systemRecordCount", "type": ["null", "long"], "default": None})
        self.assertEqual(len(fields), 26)


class SchemaGateScope(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.manifest = self.tmp / "m.json"
        self.exemptions = self.tmp / "e.txt"
        self.write_manifest({"level-absorption": ["dev"], "other": ["dev", "prod"]})
        self.exemptions.write_text("# c\nlevel-absorption|production|reason text\n")

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def write_manifest(self, bindings):
        entries = [{"producers": [{"service": s, "environments": e}], "consumers": []} for s, e in bindings.items()]
        self.manifest.write_text(json.dumps({"entries": entries}))

    def run_scope(self, service, environment):
        return scope(service, environment, self.manifest, self.exemptions)

    def test_a_declared_dev_only_service_is_exempt_in_production_and_gated_in_dev(self):
        r = self.run_scope("level-absorption", "production")
        self.assertEqual((r.returncode, r.stdout.strip()), (0, "EXEMPT: reason text"))
        self.assertEqual(self.run_scope("level-absorption", "dev").stdout.strip(), "GATE")

    def test_an_exemption_is_void_once_a_binding_names_the_environment(self):
        self.write_manifest({"level-absorption": ["dev", "prod"]})
        self.assertEqual(self.run_scope("level-absorption", "production").stdout.strip(), "GATE")

    def test_a_service_bound_in_other_environments_but_not_declared_stays_gated(self):
        self.write_manifest({"level-absorption": ["dev"], "es-liquidity-wall": ["dev"]})
        self.assertEqual(self.run_scope("es-liquidity-wall", "production").stdout.strip(), "GATE")

    def test_the_exemption_is_for_the_named_environment_only(self):
        self.assertEqual(self.run_scope("level-absorption", "es4").stdout.strip(), "GATE")

    def test_an_unnamed_service_keeps_the_coverage_ratchet(self):
        self.assertEqual(self.run_scope("nobody", "production").stdout.strip(), "NOT_GATED_UNCOVERED")

    def test_a_malformed_exemption_line_fails_closed(self):
        self.exemptions.write_text("level-absorption|production\n")
        r = self.run_scope("level-absorption", "production")
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(r.stdout.strip(), "")

    def test_every_environment_service_deploy_supports_is_decided(self):
        for environment in ("dev", "production", "experiment"):
            self.assertEqual(self.run_scope("nobody", environment).returncode, 0, environment)
        self.assertEqual(self.run_scope("level-absorption", "experiment").stdout.strip(), "GATE")

    def test_experiment_behaviour_is_unchanged_and_level_absorption_has_no_experiment_overlay(self):
        """schema-sync.sh has always refused experiment for a manifest-covered service (see scripts/ci/schema-sync-coverage-allowlist.txt); the scope script
        keeps that (GATE) and does not invent an exemption. Level absorption has no experiment overlay, so its new dev binding changes nothing there."""
        self.assertFalse((ROOT / "k8s/services/level-absorption/overlays/experiment").exists())
        self.assertIn("a covered service is refused there by schema-sync.sh", (ROOT / "scripts/ci/schema-sync-coverage-allowlist.txt").read_text())

    def test_an_unknown_environment_or_missing_manifest_fails_closed(self):
        self.assertNotEqual(self.run_scope("level-absorption", "staging").returncode, 0)
        self.manifest.unlink()
        self.assertNotEqual(self.run_scope("level-absorption", "dev").returncode, 0)

    def test_the_real_repository_decides_as_intended(self):
        self.assertTrue(scope("level-absorption", "production").stdout.startswith("EXEMPT: "))
        self.assertEqual(scope("level-absorption", "dev").stdout.strip(), "GATE")
        for service in ("early-auction-verdict", "es-liquidity-wall"):
            self.assertEqual(scope(service, "production").stdout.strip(), "GATE", service)
        text = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        self.assertIn('bash scripts/ci/schema-sync-scope.sh "$SERVICE" "$ENVIRONMENT"', text)
        self.assertEqual(len([l for l in (ROOT / "scripts/ci/schema-sync-environment-exemptions.txt").read_text().splitlines() if l.strip() and not l.startswith("#")]), 1)


class Wiring(unittest.TestCase):
    def test_the_validator_and_these_tests_are_run_by_the_repository_checks(self):
        self.assertIn("bash scripts/ci/validate-line-health-config.sh", (ROOT / "scripts/ci/validate-services.sh").read_text())
        self.assertIn("tests.test_line_health_m1_deploy", (ROOT / ".github/workflows/deploy-validation.yml").read_text())

    def test_the_jenkinsfile_is_untouched_by_this_change(self):
        self.assertNotIn("line-health", (ROOT / "Jenkinsfile.service-deploy").read_text())


class ConfigurationValidator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = Path(tempfile.mkdtemp())
        shutil.copytree(ROOT / "k8s", cls.base / "k8s")
        (cls.base / "scripts").mkdir()
        shutil.copytree(ROOT / "scripts/es4", cls.base / "scripts/es4")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.base)

    def setUp(self):
        self.work = Path(tempfile.mkdtemp())
        shutil.copytree(self.base / "k8s", self.work / "k8s")
        shutil.copytree(self.base / "scripts", self.work / "scripts")

    def tearDown(self):
        shutil.rmtree(self.work)

    def run_validator(self):
        return subprocess.run(["bash", str(VALIDATOR)], capture_output=True, text=True, env={**os.environ, "LINE_HEALTH_ROOT": str(self.work)})

    def edit(self, rel, fn):
        p = self.work / rel
        p.write_text(fn(p.read_text()))

    def rename_to_hash(self):
        """Keep the name consistent with the (edited) content, so a case fails for ITS reason only."""
        import hashlib
        p = self.work / CONFIG_FILE
        text = p.read_text()
        body = text.split("  line-health-configuration.txt: |\n", 1)[1]
        content = "".join(l[4:] + "\n" for l in body.splitlines())
        new = "level-absorption-line-health-config-" + hashlib.sha256(content.encode()).hexdigest()[:12]
        old = re.search(r"name: (level-absorption-line-health-config-[0-9a-f]{12})", text).group(1)
        p.write_text(text.replace(old, new))
        self.edit(PATCH, lambda s: s.replace(old, new))

    def assert_fails(self, needle):
        r = self.run_validator()
        self.assertNotEqual(r.returncode, 0, r.stdout)
        self.assertIn(needle, r.stderr)

    def test_the_repository_passes(self):
        r = subprocess.run(["bash", str(VALIDATOR)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("71 keys", r.stdout)

    def test_a_name_that_is_not_the_hash_fails(self):
        self.edit(CONFIG_FILE, lambda s: s.replace("tapeWindowSeconds=20", "tapeWindowSeconds=21"))
        self.assert_fails("must be named")

    def test_an_unknown_key_fails(self):
        self.edit(CONFIG_FILE, lambda s: s.replace("    vacuumDepthContracts=398\n", "    vacuumDepthContracts=398\n    zzzExtra=1\n"))
        self.rename_to_hash()
        self.assert_fails("unknown key(s): zzzExtra")

    def test_a_missing_required_key_fails(self):
        self.edit(CONFIG_FILE, lambda s: s.replace("    depthEnvironment=Rithmic Paper Trading\n", ""))
        self.rename_to_hash()
        self.assert_fails("missing key(s): depthEnvironment")

    def test_a_non_integer_value_fails(self):
        self.edit(CONFIG_FILE, lambda s: s.replace("bandTicks=4", "bandTicks=4.5"))
        self.rename_to_hash()
        self.assert_fails("bandTicks must be a non-negative integer")

    def test_unsorted_keys_fail(self):
        self.edit(CONFIG_FILE, lambda s: s.replace("    bandTicks=4\n    basisBracketMillis=2000\n", "    basisBracketMillis=2000\n    bandTicks=4\n"))
        self.rename_to_hash()
        self.assert_fails("keys are not sorted")

    def test_a_mutable_config_map_fails(self):
        self.edit(CONFIG_FILE, lambda s: re.sub(r"(name: level-absorption-line-health-config-[0-9a-f]{12}\n  namespace: options-edge\n)immutable: true", r"\1immutable: false", s))
        self.assert_fails("must be immutable")

    def test_the_dev_patch_must_turn_it_on(self):
        self.edit(PATCH, lambda s: s.replace('LEVEL_ABSORPTION_LINE_HEALTH_ENABLED\n              value: "true"', 'LEVEL_ABSORPTION_LINE_HEALTH_ENABLED\n              value: "false"'))
        self.assert_fails("LEVEL_ABSORPTION_LINE_HEALTH_ENABLED=true")

    def test_the_dev_patch_must_name_the_content_named_config_map_and_mount_it_read_only(self):
        self.edit(PATCH, lambda s: s.replace("optional: false", "optional: true"))
        self.assert_fails("optional: false")
        self.edit(PATCH, lambda s: s.replace("optional: true", "optional: false").replace("readOnly: true", "readOnly: false"))
        self.assert_fails("read-only")

    def test_another_file_turning_it_on_fails(self):
        shutil.copy(self.work / PATCH, self.work / "k8s/overlays/production/zz-enable.yaml")
        self.assert_fails("only the dev patch and its generated dev slice may")

    def test_the_base_deployment_enabling_it_reaches_production_and_fails(self):
        self.edit("k8s/services/level-absorption/base/level-absorption-deployment.yaml",
                  lambda s: s.replace("          env:\n", "          env:\n            - name: LEVEL_ABSORPTION_LINE_HEALTH_ENABLED\n              value: \"true\"\n", 1))
        r = self.run_validator()
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("render carries line-health configuration", r.stderr)


if __name__ == "__main__":
    unittest.main()
