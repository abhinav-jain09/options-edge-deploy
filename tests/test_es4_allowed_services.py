"""es4 runs ONLY ES-related services (owner 2026-11-02). k8s/es4/allowed-services.txt is the one list."""
import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REMOVED = {
    "databento-gex", "databento-gex-history", "databento-maxpain", "databento-volume-aggregator",
    "strike-flow-classifier", "strike-flow-avro-adapter", "raw-to-display", "delta-flow",
    "gamma-migration", "gex-delta-redis-writer", "strike-liquidity-heatmap", "option-price-behavior",
    "es-web", "vix-option-inteligence", "pin-postgres-writer", "pressure-postgres-writer",
}
SCRIPT = ROOT / "scripts/es4/assert-allowed-services.sh"


def allowed():
    return {l.strip() for l in (ROOT / "k8s/es4/allowed-services.txt").read_text().splitlines()
            if l.strip() and not l.lstrip().startswith("#")}


class Es4AllowedServices(unittest.TestCase):
    def test_manifest_directory_equals_allowlist(self):
        on_disk = {p.stem for p in (ROOT / "k8s/es4/services").glob("*.yaml")}
        self.assertEqual(on_disk, allowed())

    def test_removed_services_are_not_allowed_or_declared(self):
        self.assertFalse(REMOVED & allowed())
        renderer = (ROOT / "scripts/es4/render_es4_manifests.py").read_text()
        services = re.search(r"^SERVICES = \[(.*?)\n\]", renderer, re.S | re.M).group(1)
        self.assertFalse(REMOVED & set(re.findall(r'"([a-z0-9-]+)"', services)))

    def test_jenkins_service_choices_are_allowed(self):
        jf = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        block = re.search(r"choice\(name: 'SERVICE', choices: \[(.*?)\]\s*,\s*description", jf, re.S).group(1)
        names = set(re.findall(r"'([a-z0-9-]+)'", re.sub(r"//.*", "", block)))
        self.assertTrue(names)
        self.assertFalse(names - allowed())

    def test_jenkins_runs_the_allowlist_gate_before_any_apply(self):
        jf = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        stage = jf[jf.index("stage('Deploy to es4')"):]
        self.assertLess(stage.index("assert-allowed-services.sh"), stage.index("pin-and-apply.sh"))

    def test_gate_refuses_removed_and_accepts_allowed(self):
        for n in sorted(REMOVED):
            r = subprocess.run(["bash", str(SCRIPT), "service", n], capture_output=True, text=True)
            self.assertEqual(r.returncode, 1, n)
            self.assertIn("REFUSED", r.stderr)
        for n in sorted(allowed()):
            self.assertEqual(subprocess.run(["bash", str(SCRIPT), "service", n]).returncode, 0, n)
        self.assertEqual(subprocess.run(["bash", str(SCRIPT), "manifests"]).returncode, 0)


if __name__ == "__main__":
    unittest.main()
