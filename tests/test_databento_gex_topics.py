# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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

    def _var(self, name: str) -> list[str]:
        """The effective token list of a shell var — parsed exactly (so an es.-prefixed entry can
        never satisfy an assertion meant for the unprefixed dev/prod name) and combining EVERY
        assignment, including later `VAR="$VAR ..."` appends (the self-reference token is dropped).
        Missing this would let an append add a topic to a compacted list undetected."""
        import re

        tokens: list[str] = []
        for m in re.finditer(rf'^{name}="([^"]*)"', self._topics_env(), re.M):
            tokens.extend(t for t in m.group(1).split() if t != f"${name}")
        return tokens

    def test_preopen_topic_provisioned_delete_only_with_72h_retention(self) -> None:
        """The context tape's pre-open gex source: exact declaration on dev/prod AND es4, plain-delete
        (in NEITHER compacted list), with 72h retention so the es4 ~23h GLOBEX pre-open window replays."""
        # Exact entries — not a whole-file substring (es.<name> contains <name>).
        self.assertIn("options.databento.gex.strike.preopen:32", self._var("OPTIONS_EDGE_TOPICS"))
        self.assertIn("es.options.databento.gex.strike.preopen:32", self._var("OPTIONS_EDGE_ES4_TOPICS"))
        # Plain delete: absent from BOTH compacted lists.
        self.assertNotIn("options.databento.gex.strike.preopen", self._var("OPTIONS_EDGE_COMPACTED_TOPICS"))
        self.assertNotIn(
            "es.options.databento.gex.strike.preopen", self._var("OPTIONS_EDGE_ES4_COMPACTED_TOPICS"))
        # 72h retention override on each broker's prefixed name.
        self.assertIn(
            "options.databento.gex.strike.preopen=259200000",
            self._var("OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"))
        self.assertIn(
            "es.options.databento.gex.strike.preopen=259200000",
            self._var("OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES"))

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
        # the EFFECTIVE compacted list (every assignment incl. appends), parsed as exact tokens
        compacted = self._var("OPTIONS_EDGE_COMPACTED_TOPICS")
        # gex.strike is last-value-per-strike -> compacted.
        self.assertIn("options.databento.gex.strike", compacted)
        # gex.strike.history, gex.strike.preopen, gex-flow.* and gex.magnet are plain delete -> NOT compacted.
        self.assertNotIn("options.databento.gex.strike.history", compacted)
        self.assertNotIn("options.databento.gex.strike.preopen", compacted)
        self.assertNotIn("options.databento.gex.magnet", compacted)
        self.assertNotIn("options.databento.gex.flow.by-strike", compacted)


if __name__ == "__main__":
    unittest.main()
