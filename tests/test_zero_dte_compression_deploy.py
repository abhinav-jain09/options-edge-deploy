from __future__ import annotations

import os
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SHA = "d774ed59ca3148e6f9b36accfc0a4744d43fc1f6bf36cdaa30d4ae899f419d82"
TOPICS = (
    "context-tape.compression.current",
    "context-tape.compression.history",
    "context-tape.compression.checkpoint",
)


class ZeroDteCompressionDeployTest(unittest.TestCase):
    def test_dev_and_production_enable_same_hash_pinned_shadow_contract(self) -> None:
        for environment in ("dev", "production"):
            patch = (
                ROOT
                / "k8s"
                / "overlays"
                / environment
                / f"context-tape-direction-{'dev' if environment == 'dev' else 'prod'}-env-patch.yaml"
            ).read_text()
            rendered = (
                ROOT / "k8s" / "services" / "context-tape" / "overlays" / environment / "manifest.yaml"
            ).read_text()
            for text in (patch, rendered):
                self.assertIn("ZERO_DTE_COMPRESSION_ENABLED", text)
                self.assertIn("ZERO_DTE_SPOT_TOPIC", text)
                self.assertIn("underlying.spx.price", text)
                self.assertIn("ZERO_DTE_ES_TOPIC", text)
                self.assertIn("underlying.es.trades", text)
                self.assertIn("ZERO_DTE_OPTION_TOPIC", text)
                self.assertIn("options.databento.gex.strike", text)
                self.assertIn("ZERO_DTE_CURRENT_TOPIC", text)
                self.assertIn("ZERO_DTE_HISTORY_TOPIC", text)
                self.assertIn("ZERO_DTE_CHECKPOINT_TOPIC", text)
                self.assertIn("ZERO_DTE_ARTIFACT_SHA256", text)
                self.assertIn(SHA, text)
            self.assertIn(f"context-tape-compression-{'dev' if environment == 'dev' else 'prod'}-v1", patch)

    def test_output_and_recovery_topics_are_durable_and_classified(self) -> None:
        topics = (ROOT / "scripts" / "kafka" / "topics.env").read_text()
        for topic in TOPICS:
            self.assertIn(f"{topic}:1", topics)
            self.assertIn(f"{topic}=-1", topics)
        self.assertIn(
            'OPTIONS_EDGE_COMPACTED_TOPICS="$OPTIONS_EDGE_COMPACTED_TOPICS '
            "context-tape.compression.current context-tape.compression.checkpoint\"",
            topics,
        )
        preserved = next(line for line in topics.splitlines()
                         if line.startswith('OPTIONS_EDGE_RESET_PRESERVED_TOPICS="'))
        rebuildable = next(line for line in topics.splitlines()
                           if line.startswith('OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="'))
        self.assertIn("context-tape.compression.history", preserved)
        self.assertIn("context-tape.compression.checkpoint", preserved)
        self.assertIn("context-tape.compression.current", rebuildable)

    def test_monitoring_and_jenkins_gate_cover_the_runtime(self) -> None:
        rules = (ROOT / "scripts" / "monitoring" / "hpsf-alert-rules.yaml").read_text()
        for alert in (
            "ZeroDteCompressionNotReadyWhileSpotFlows",
            "ZeroDteCompressionOutputStalled",
            "ZeroDteCompressionRuntimeFailures",
            "ZeroDteCompressionRejectedInputBurst",
        ):
            self.assertIn(alert, rules)

        deploy = (ROOT / "scripts" / "deploy" / "service-deploy.sh").read_text()
        smoke_path = ROOT / "scripts" / "smoke" / "check-zero-dte-compression.sh"
        smoke = smoke_path.read_text()
        self.assertIn('SERVICE" = "context-tape', deploy)
        self.assertIn("scripts/smoke/check-zero-dte-compression.sh", deploy)
        self.assertTrue(os.access(smoke_path, os.X_OK))
        self.assertIn("/health/compression", smoke)
        self.assertIn("/api/context-tape/compression", smoke)
        self.assertIn(SHA, smoke)
        self.assertIn("SHADOW_NOT_FOR_TRADING", smoke)


if __name__ == "__main__":
    unittest.main()
