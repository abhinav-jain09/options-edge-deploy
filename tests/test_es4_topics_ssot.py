"""es4 topic creation must be driven ENTIRELY by scripts/kafka/topics.env — the same SSOT that
Jenkins applies for dev/prod — and must declare exactly the topics the es4 broker actually needs.

unittest, not bare pytest-style functions: pytest is not installed and .github/workflows/
deploy-validation.yml runs `python3 -m unittest`, so as module-level test_ functions this whole
file collected ZERO tests and had never run in CI. A test nobody runs is not a test.
"""
import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ENV = (REPO / "scripts" / "kafka" / "topics.env").read_text()
CLEANUP = (REPO / "scripts" / "es4" / "cleanup-es4.sh").read_text()
CREATE = (REPO / "scripts" / "es4" / "create-es-topics.sh").read_text()
APPLY = (REPO / "scripts" / "kafka" / "apply-topics.sh").read_text()


def _var(name):
    """Every assignment of `name`, concatenated. topics.env appends to its lists
    (VAR="$VAR more:4"), so reading only the first match silently truncates the set."""
    out = []
    for m in re.finditer(rf'^{name}="([^"]*)"$', ENV, re.M):
        out += m.group(1).split()
    return [v for v in out if not v.startswith("$")]


def _topics(name):
    return {e.rsplit(":", 1)[0] for e in _var(name)}


class Es4TopicSsotTest(unittest.TestCase):
    def test_var_parser_sees_the_appended_declarations(self):
        """Guards the helper above: OPTIONS_EDGE_ES4_TOPICS is assigned twice, and a parser that
        reads one assignment would quietly shrink every set-membership assertion in this file."""
        self.assertGreater(len(re.findall(r'^OPTIONS_EDGE_ES4_TOPICS="', ENV, re.M)), 1,
                           "expected topics.env to append to OPTIONS_EDGE_ES4_TOPICS")
        self.assertIn("es.open-direction.forecast", _topics("OPTIONS_EDGE_ES4_TOPICS"),
                      "parser missed a topic from an APPENDED assignment")

    def test_reset_holds_no_topic_knowledge(self):
        """The script must be dumb about topics: no snapshot/replay, no hardcoded topic lists."""
        self.assertNotIn("snapshot_topics", CLEANUP)
        self.assertNotIn("TOPIC_STATE", CLEANUP)
        self.assertNotIn("TOPICS_DELETE=(", CREATE)
        self.assertNotIn("TOPICS_COMPACT=(", CREATE)
        self.assertIsNone(re.search(r'^ensure_topic_custom es\.', CREATE, re.M))

    def test_topic_creation_delegates_to_the_jenkins_applier(self):
        self.assertIn("apply-topics.sh", CREATE)
        self.assertIn("TOPIC_SET=es4", CREATE)

    def test_apply_topics_supports_a_scoped_set(self):
        self.assertIn("TOPIC_SET", APPLY)
        self.assertIn("OPTIONS_EDGE_ES4_TOPICS", APPLY)

    def test_every_es4_declaration_is_substituted_under_topic_set_es4(self):
        """The durable form of "TOPIC_SET empty behaves exactly as before".

        This replaces a test that asserted the five dev/prod variables were BYTE-IDENTICAL to
        `git show origin/main:scripts/kafka/topics.env`. That can only ever hold on main itself:
        it failed on the branch of every PR that deliberately changed a dev/prod declaration
        (#774, #775, #777 all did) and passed on main only by comparing main to itself. It was a
        one-shot review aid for the es4-SSOT PR, not an invariant.

        What IS durable is the mechanism it was standing in for: apply-topics.sh swaps the es4
        declarations in WHOLESALE, so no dev/prod list can reach an es4 run. Adding an
        OPTIONS_EDGE_ES4_* list to topics.env without wiring it into that case arm would leave the
        es4 broker silently reconciled against the dev/prod value — which is exactly how es4 topics
        got the wrong shape before the SSOT change.
        """
        es4_vars = sorted(set(re.findall(r'^(OPTIONS_EDGE_ES4_[A-Z_0-9]+)=', ENV, re.M)))
        self.assertTrue(es4_vars, "no OPTIONS_EDGE_ES4_* declarations found — parser drifted")
        es4_branch = re.search(r'^\s*es4\)\n(.*?)^\s*;;', APPLY, re.M | re.S)
        self.assertIsNotNone(es4_branch, "could not find the es4 arm of apply-topics.sh's TOPIC_SET case")
        body = es4_branch.group(1)
        for v in es4_vars:
            default = v.replace("OPTIONS_EDGE_ES4_", "OPTIONS_EDGE_", 1)
            self.assertRegex(
                body, rf'(?m)^\s*{default}="\$\{{?{v}\b',
                f"{v} is declared in topics.env but apply-topics.sh's es4 arm never assigns it to "
                f"{default} — an es4 run would use the dev/prod value")

    def test_no_dev_prod_only_override_can_leak_onto_es4(self):
        """OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES is the one list apply-topics.sh consults
        that has NO es4 twin and is NOT reset by the es4 arm, so an entry for an es.* topic would
        be applied on the es4 broker from the dev/prod declaration. Harmless today because the only
        entry is options.ibkr.gex.status; asserted so it stays that way."""
        leak_keys = {e.split("=", 1)[0] for e in _var("OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES")}
        self.assertFalse(
            leak_keys & _topics("OPTIONS_EDGE_ES4_TOPICS"),
            "a dev/prod delete.retention override names an es4 topic; the es4 arm does not reset "
            "that list, so it would be applied to the es4 broker too")

    def test_es4_set_is_scoped_to_es_topics_only(self):
        """Applying the SPX set to the es4 broker would create ~52 unrelated options.* topics."""
        es4 = _topics("OPTIONS_EDGE_ES4_TOPICS")
        self.assertTrue(es4, "ES4 set missing")
        for t in sorted(es4):
            self.assertTrue(t.startswith("es."), f"non-es topic in the es4 set: {t}")

    def test_gateway_blocking_topics_are_declared_even_with_no_producer(self):
        """These eight were once asserted ABSENT, as "orphans whose producing service does not run
        on es4". #656 reversed that on purpose and declared all eight, so the old assertion had been
        failing on main ever since — it was testing a policy the repo had abandoned.

        The current rule, from the topics.env block that declares them: es-feed-gateway resolves the
        FULL topic list of each of its three consumers at startup and blocks on partitionsFor() for
        every one, so a missing topic throws TopicMetadataTimeoutException and restarts the consumer
        on a 30s loop forever. Most of these belong to services in ES4_KEEP_DOWN, so nothing ever
        produces to them — they still have to EXIST, because the gateway blocks on topic metadata,
        not on data. Turning broker auto-create off on es4 exposed this by blanking es.fullfunding.nl
        for ~40 minutes on 2026-07-31.

        Same eight names, asserted in the direction the repo actually holds.
        """
        es4 = _topics("OPTIONS_EDGE_ES4_TOPICS")
        for needed in (
            "es.options.ibkr.display",
            "es.options.hpsf.latest-signal",
            "es.options.spx.mission-control.current",
            "es.options.spx.strike-invasion.current",
            "es.options.spx.strike-sr.current",
            "es.open-direction.forecast",
            "es.options.databento.volume-sandwich.current",
            "es.market.spx.market-carry-service.current",
        ):
            self.assertIn(needed, es4,
                          f"gateway-blocking topic not declared for es4: {needed} — the gateway "
                          f"waits on partitionsFor() for it and auto-create is off on that broker")

    def test_output_topics_of_deployed_es4_services_are_declared(self):
        """These ARE produced on es4; leaving them out means broker auto-create picks the partitions."""
        es4 = _topics("OPTIONS_EDGE_ES4_TOPICS")
        for needed in (
            "es.delta-flow-dashboard",
            "es.options.databento.gex.flow.dashboard",
            "es.strike-intelligence-by-strike",
            "es.option-price-behavior-by-strike",
            "es.options.databento.pace.rank",
            "es.options.spx.greek-move-auth.current",
            "es.options.es.option-truth-engine-service.by-strike",
        ):
            self.assertIn(needed, es4, f"missing required es4 topic: {needed}")

    def test_single_partition_contracts_are_enforced_exactly(self):
        """Auto-create would give these the default partition count; the contract needs exactly 1."""
        exact = set(_var("OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS"))
        entries = dict(e.rsplit(":", 1) for e in _var("OPTIONS_EDGE_ES4_TOPICS"))
        for t in ("es.reversal.verdicts", "es.reversal.strength", "es.signal-follower.hot-strike"):
            self.assertIn(t, exact, f"{t} must be exact-partition")
            self.assertEqual(entries[t], "1", f"{t} must be 1 partition, got {entries[t]}")

    def test_heatmap_topics_declared_at_the_services_default_partition_count(self):
        """Incident guard (2026-07-19/07-26): the strike-liquidity-heatmap service creates its
        dashboard/bucket topics at the 32-partition default. If the SSOT declares them lower
        (e.g. 4), the topic is born at 4 and the service grows it to 32 AFTER boot; if
        es-feed-gateway snapshots the partition count in between, LiquidityHistoryStore trips a
        STICKY topology alarm and 503s ALL /api/liquidity-history requests until restart.
        Declaring them at 32 makes the service's ensureTopic a no-op so the count never changes
        post-boot."""
        entries = dict(e.rsplit(":", 1) for e in _var("OPTIONS_EDGE_ES4_TOPICS"))
        for t in ("es.strike-liquidity-heatmap-dashboard", "es.strike-liquidity-heatmap-bucket"):
            self.assertEqual(entries.get(t), "32", (
                f"{t} must be declared at 32 partitions to match the heatmap service default "
                f"(KAFKA_TOPIC_PARTITIONS_DEFAULT=32); got {entries.get(t)}. A lower value "
                f"re-opens the 4->32 boot race that 503s the liquidity-history endpoint."
            ))

    def test_es4_gateway_topology_guard_is_calibrated_to_32(self):
        """The gateway's liquidity-history topology guard defaults to expecting 4 partitions; on
        es4 the heatmap dashboard topic is 32, so the guard must be told 32 or it WARNs a false
        policy drift."""
        manifest = (REPO / "k8s" / "es4" / "services" / "es-feed-gateway.yaml").read_text()
        self.assertRegex(
            manifest,
            r'name:\s*HEATMAP_HISTORY_EXPECTED_PARTITIONS\s*\n\s*value:\s*"32"',
            "es-feed-gateway.yaml must set HEATMAP_HISTORY_EXPECTED_PARTITIONS=32 for the "
            "32-partition es4 heatmap topic")


if __name__ == "__main__":
    unittest.main()
