from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class HpsfReleaseGateTest(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text()

    def test_release_gate_script_parses_and_checks_required_artifacts(self) -> None:
        script = ROOT / "scripts/smoke/check-hpsf-release-gate.sh"
        subprocess.run(["bash", "-n", str(script)], check=True)
        text = script.read_text()
        for item in [
            "hpsf.md",
            "optionedge_hpsf_v2_expected_output_contract.md",
            "codex_feedback_databento_to_hpsf_field_mapping.md",
            "HPSF_TOPIC_LATEST_SIGNAL",
            "HPSF_ORDER_PLACEMENT_ENABLED",
            "HPSF_SOURCE_ROOT",
            "check-hpsf-deployment.sh",
        ]:
            self.assertIn(item, text)
        self.assertNotIn("ibkr-feed-service", text)

    def test_release_notes_include_signal_only_rf1_and_databento_scope(self) -> None:
        notes = self.read("docs/hpsf-v2-release-notes.md")
        for item in [
            "HPSF V2.1 is signal-only",
            "RF=1 has no broker-failure durability",
            "Databento",
            "SHADOW",
            "REPLAY",
            "options.hpsf.latest-signal",
            "Do not modify or depend on IBKR feed/services",
        ]:
            self.assertIn(item, notes)
        self.assertNotIn('"enabled":true', notes)

    def test_release_gate_jenkins_pipeline_has_static_remote_and_evidence_stages(self) -> None:
        pipeline = self.read("Jenkinsfile.hpsf-release-gate")
        for item in [
            "Static Release Gate",
            "Checkout Source Specs",
            "Remote Release Gate",
            "Archive Release Evidence",
            "HPSF_SOURCE_ROOT",
            "check-hpsf-release-gate.sh --dry-run",
            "scripts/kafka/verify-hpsf-topics.sh",
            "hpsf_signal_only=true",
            "hpsf_databento_only=true",
        ]:
            self.assertIn(item, pipeline)


if __name__ == "__main__":
    unittest.main()
