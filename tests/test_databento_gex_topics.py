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

    def test_es4_disables_opra_oi_fetch_with_an_explicit_override(self) -> None:
        import yaml

        renderer = (ROOT / "scripts" / "es4" / "render_es4_manifests.py").read_text()
        self.assertIn(
            '{"name": "DATABENTO_GEX_OI_DIRECT_FETCH_ENABLED", "value": "false", "_override": True}',
            renderer,
            "the production slice already defines true, so the ES value must be an override",
        )

        manifest = ROOT / "k8s" / "es4" / "services" / "databento-gex.yaml"
        for doc in yaml.safe_load_all(manifest.read_text()):
            if doc and doc.get("kind") == "Deployment" and doc["metadata"]["name"] == "databento-gex-service":
                env = {item["name"]: item.get("value")
                       for item in doc["spec"]["template"]["spec"]["containers"][0].get("env", [])}
                self.assertEqual("false", env.get("DATABENTO_GEX_OI_DIRECT_FETCH_ENABLED"))
                break
        else:
            self.fail("databento-gex-service Deployment missing from the ES4 manifest")


if __name__ == "__main__":
    unittest.main()
