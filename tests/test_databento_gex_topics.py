from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DatabentoGexTopicsTest(unittest.TestCase):
    """The databento GEX family (per-strike GEX, gamma-flow, and the magnet/pin) must be provisioned by the
    Jenkins topics step (apply-topics.sh + verify-topics.sh read topics.env), so it is recreated with the right
    partitions/cleanup after a clean-slate wipe — not left to the service's ensureTopic auto-create alone."""

    def _topics_env(self) -> str:
        return (ROOT / "scripts" / "kafka" / "topics.env").read_text()

    def test_gex_family_declared_with_32_partitions(self) -> None:
        topics = self._topics_env()
        for topic in (
            "options.databento.gex.strike:32",
            "options.databento.gex.strike.history:32",
            "options.databento.gex.flow.by-strike:32",
            "options.databento.gex.flow.10s:32",
            "options.databento.gex.flow.1m:32",
            "options.databento.gex.flow.5m:32",
            "options.databento.gex.flow.15m:32",
            "options.databento.gex.flow.session:32",
            "options.databento.gex.flow.dashboard:32",
            "options.databento.gex.magnet:32",
        ):
            self.assertIn(topic, topics, f"{topic} missing from OPTIONS_EDGE_TOPICS")

    def test_gex_strike_is_compacted_but_flow_and_magnet_are_not(self) -> None:
        topics = self._topics_env()
        compacted = topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1]
        # gex.strike is last-value-per-strike -> compacted.
        self.assertIn("options.databento.gex.strike", compacted)
        # gex.strike.history, gex-flow.* and gex.magnet are plain delete -> must NOT be compacted.
        self.assertNotIn("options.databento.gex.strike.history", compacted)
        self.assertNotIn("options.databento.gex.magnet", compacted)
        self.assertNotIn("options.databento.gex.flow", compacted)


if __name__ == "__main__":
    unittest.main()
