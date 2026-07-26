"""es4 topic creation must be driven ENTIRELY by scripts/kafka/topics.env — the same SSOT that
Jenkins applies for dev/prod — and must create ONLY the topics es4 actually needs."""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ENV = (REPO / "scripts" / "kafka" / "topics.env").read_text()
CLEANUP = (REPO / "scripts" / "es4" / "cleanup-es4.sh").read_text()
CREATE = (REPO / "scripts" / "es4" / "create-es-topics.sh").read_text()
APPLY = (REPO / "scripts" / "kafka" / "apply-topics.sh").read_text()


def _var(name):
    m = re.search(rf'^{name}="([^"]*)"', ENV, re.M)
    return m.group(1).split() if m else []


def test_reset_holds_no_topic_knowledge():
    """The script must be dumb about topics: no snapshot/replay, no hardcoded topic lists."""
    assert "snapshot_topics" not in CLEANUP
    assert "TOPIC_STATE" not in CLEANUP
    assert "TOPICS_DELETE=(" not in CREATE and "TOPICS_COMPACT=(" not in CREATE
    assert not re.search(r'^ensure_topic_custom es\.', CREATE, re.M)


def test_topic_creation_delegates_to_the_jenkins_applier():
    assert "apply-topics.sh" in CREATE
    assert "TOPIC_SET=es4" in CREATE


def test_apply_topics_supports_a_scoped_set():
    assert "TOPIC_SET" in APPLY
    assert "OPTIONS_EDGE_ES4_TOPICS" in APPLY


def test_default_set_is_untouched_for_dev_and_prod():
    """TOPIC_SET empty must behave EXACTLY as before. Asserted as byte-equality against origin/main,
    not 'contains no es.*' — the SPX set legitimately carries es.open-direction.* (those live on the
    PROD broker), so a prefix check would be wrong."""
    import subprocess
    main_env = subprocess.run(
        ["git", "show", "origin/main:scripts/kafka/topics.env"],
        capture_output=True, text=True, cwd=REPO, check=True).stdout
    def var_of(text, name):
        m = re.search(rf'^{name}="([^"]*)"', text, re.M)
        return m.group(1).split() if m else []
    for v in ("OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS",
              "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS",
              "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"):
        assert var_of(ENV, v) == var_of(main_env, v), f"{v} changed — dev/prod behaviour altered"


def test_es4_set_is_scoped_to_es_topics_only():
    """Applying the SPX set to the es4 broker would create ~52 unrelated options.* topics."""
    es4 = [e.rsplit(":", 1)[0] for e in _var("OPTIONS_EDGE_ES4_TOPICS")]
    assert es4, "ES4 set missing"
    assert all(t.startswith("es.") for t in es4)


def test_orphan_topics_of_services_not_on_es4_are_excluded():
    """A reset must not resurrect topics whose producing service does not run on es4."""
    es4 = {e.rsplit(":", 1)[0] for e in _var("OPTIONS_EDGE_ES4_TOPICS")}
    for orphan in (
        "es.options.ibkr.display",
        "es.options.hpsf.latest-signal",
        "es.options.spx.mission-control.current",
        "es.options.spx.strike-invasion.current",
        "es.options.spx.strike-sr.current",
        "es.open-direction.forecast",
        "es.options.databento.volume-sandwich.current",
        "es.market.spx.market-carry-service.current",
    ):
        assert orphan not in es4, f"orphan topic declared for es4: {orphan}"


def test_output_topics_of_deployed_es4_services_are_declared():
    """These ARE produced on es4; leaving them out means broker auto-create picks the partitions."""
    es4 = {e.rsplit(":", 1)[0] for e in _var("OPTIONS_EDGE_ES4_TOPICS")}
    for needed in (
        "es.delta-flow-dashboard",
        "es.options.databento.gex.flow.dashboard",
        "es.strike-intelligence-by-strike",
        "es.option-price-behavior-by-strike",
        "es.options.databento.pace.rank",
        "es.options.spx.greek-move-auth.current",
        "es.options.es.option-truth-engine-service.by-strike",
    ):
        assert needed in es4, f"missing required es4 topic: {needed}"


def test_single_partition_contracts_are_enforced_exactly():
    """Auto-create would give these the default partition count; the contract needs exactly 1."""
    exact = set(_var("OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS"))
    entries = dict(e.rsplit(":", 1) for e in _var("OPTIONS_EDGE_ES4_TOPICS"))
    for t in ("es.reversal.verdicts", "es.reversal.strength", "es.signal-follower.hot-strike"):
        assert t in exact, f"{t} must be exact-partition"
        assert entries[t] == "1", f"{t} must be 1 partition, got {entries[t]}"


def test_heatmap_topics_declared_at_the_services_default_partition_count():
    """Incident guard (2026-07-19/07-26): the strike-liquidity-heatmap service creates its
    dashboard/bucket topics at the 32-partition default. If the SSOT declares them lower (e.g. 4),
    the topic is born at 4 and the service grows it to 32 AFTER boot; if es-feed-gateway snapshots
    the partition count in between, LiquidityHistoryStore trips a STICKY topology alarm and 503s ALL
    /api/liquidity-history requests until restart. Declaring them at 32 makes the service's
    ensureTopic a no-op so the count never changes post-boot."""
    entries = dict(e.rsplit(":", 1) for e in _var("OPTIONS_EDGE_ES4_TOPICS"))
    for t in ("es.strike-liquidity-heatmap-dashboard", "es.strike-liquidity-heatmap-bucket"):
        assert entries.get(t) == "32", (
            f"{t} must be declared at 32 partitions to match the heatmap service default "
            f"(KAFKA_TOPIC_PARTITIONS_DEFAULT=32); got {entries.get(t)}. A lower value re-opens the "
            f"4->32 boot race that 503s the liquidity-history endpoint."
        )


def test_es4_gateway_topology_guard_is_calibrated_to_32():
    """The gateway's liquidity-history topology guard defaults to expecting 4 partitions; on es4 the
    heatmap dashboard topic is 32, so the guard must be told 32 or it WARNs a false policy drift."""
    manifest = (REPO / "k8s" / "es4" / "services" / "es-feed-gateway.yaml").read_text()
    assert re.search(
        r'name:\s*HEATMAP_HISTORY_EXPECTED_PARTITIONS\s*\n\s*value:\s*"32"', manifest
    ), "es-feed-gateway.yaml must set HEATMAP_HISTORY_EXPECTED_PARTITIONS=32 for the 32-partition es4 heatmap topic"
