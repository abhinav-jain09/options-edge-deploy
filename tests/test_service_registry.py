import pathlib
import shutil
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class ServiceRegistryTest(unittest.TestCase):
    """services.yaml is the single source of truth (standalone-service-deployment §13.9).

    scripts/ci/validate-services.sh enforces completeness (every rendered Deployment is
    registered), blast radius (standalone slices own only service kinds), and the mirror
    rule (standalone render == monolith render of the same workload). Running it here
    puts the gate in every `unittest discover` CI pass.
    """

    def test_registry_lists_web_as_standalone(self):
        text = (ROOT / "services.yaml").read_text()
        self.assertIn("name: web", text)
        self.assertIn("managedBy: standalone", text)

    def test_validate_services_gate_passes(self):
        if shutil.which("kubectl") is None or shutil.which("yq") is None:
            self.skipTest("kubectl/yq not available on this agent")
        result = subprocess.run(
            ["bash", str(ROOT / "scripts" / "ci" / "validate-services.sh")],
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"validate-services.sh failed:\n{result.stdout}\n{result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
