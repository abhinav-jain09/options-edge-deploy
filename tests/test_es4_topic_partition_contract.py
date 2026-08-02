"""Regression lock for the 2026-08-02/03 es4 wedge: an UNDER-declared topic partition count in
scripts/kafka/topics.env is created small, widened moments later by the owning service, and any
Kafka Streams app that read metadata in that window builds its repartition/changelog topics at the
small size — then dies on every rebalance ("invalid partitions: expected: 32; actual: 4") behind a
pod that still reports 1/1 Running. strike-flow-classifier processed 0 records for 3h that way.

Two things must hold forever: the declared shapes are the REAL shapes, and a guard re-proves that
on every topic reconciliation instead of trusting the file."""
import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ENV = (REPO / "scripts" / "kafka" / "topics.env").read_text()
CLEANUP = (REPO / "scripts" / "es4" / "cleanup-es4.sh").read_text()
VERIFY_PATH = REPO / "scripts" / "es4" / "verify-topic-partition-contract.sh"
JENKINS = (REPO / "Jenkinsfile.es4-deploy").read_text()

# Verified 2026-08-03 against BOTH the live es4 broker and prod's shape for the same topic
# (23 have a prod twin, every one at 32; es.options.databento.gex.spxbridge is es4-native at 32).
WIDENED_TO_32 = (
    "es.dealer-ledger-outcome-scored",
    "es.dealer-ledger-signal-fired",
    "es.delta-flow-by-trade",
    "es.delta-flow-session",
    "es.options.databento.display",
    "es.options.databento.display.volume.current",
    "es.options.databento.events.raw",
    "es.options.databento.gex.spxbridge",
    "es.options.databento.gex.strike",
    "es.options.databento.gex.strike.history",
    "es.options.databento.market-pressure.mission",
    "es.options.databento.maxpain",
    "es.options.databento.normalized",
    "es.options.databento.pace.mission",
    "es.options.databento.raw",
    "es.options.databento.sandwich.mission",
    "es.options.databento.strike-flow",
    "es.options.databento.strike-flow.strike.avro",
    "es.options.databento.volume.state.compacted",
    "es.options.opra.tcbbo",
    "es.options.spx.greek-move-auth.current",
    "es.options.spx.greek-move-auth.events",
    "es.strike-liquidity-heatmap-bucket",
    "es.strike-liquidity-heatmap-dashboard",
)


def _es4_entries():
    m = re.search(r'^OPTIONS_EDGE_ES4_TOPICS="([^"]*)"', ENV, re.M)
    assert m, "OPTIONS_EDGE_ES4_TOPICS missing"
    return dict(e.rsplit(":", 1) for e in m.group(1).split())


def test_incident_topics_are_declared_at_their_real_shape():
    entries = _es4_entries()
    for topic in WIDENED_TO_32:
        assert topic in entries, f"{topic} dropped from the es4 contract"
        assert entries[topic] == "32", (
            f"{topic} declared at {entries[topic]}, but its real shape is 32. Declaring it smaller "
            "recreates the strike-flow-classifier wedge on the next clean-reset."
        )


def test_source_topic_of_the_wedged_classifier_is_not_under_declared():
    """The single entry that caused the incident, called out on its own so a bulk edit cannot
    quietly revert it."""
    assert _es4_entries()["es.options.opra.tcbbo"] == "32"


def test_verify_script_exists_and_is_executable():
    assert VERIFY_PATH.exists(), "verify-topic-partition-contract.sh missing"
    assert VERIFY_PATH.stat().st_mode & 0o111, "verify-topic-partition-contract.sh not executable"


def test_verify_script_is_fail_closed():
    text = VERIFY_PATH.read_text()
    # unreachable broker, missing topic and unparseable describe must all be failures, never a pass
    assert "exit 1" in text
    assert "fail-closed" in text.lower()
    assert 'ALLOW_DRIFT="${ES4_ALLOW_PARTITION_DRIFT:-false}"' in text, (
        "the drift escape hatch must default to OFF"
    )
    # an exact comparison, not the '>=' tolerance that hid the drift in apply-topics.sh
    assert '[ "$live" != "$declared" ]' in text


def test_verify_script_syntax():
    subprocess.run(["bash", "-n", str(VERIFY_PATH)], check=True)


def test_clean_reset_verifies_before_restoring_the_apps():
    """The audit is only exact while every app is at 0 replicas. After the restore the services
    widen topics themselves and the check becomes a race."""
    reconcile = CLEANUP.index("create-es-topics.sh")
    verify = CLEANUP.index("verify-topic-partition-contract.sh")
    restore = CLEANUP.index("restoring es4 Deployments")
    assert reconcile < verify < restore, "verify must run after topic reconcile, before app restore"


def test_clean_reset_detects_wedged_topologies_after_restore():
    assert "invalid partitions: expected" in CLEANUP, (
        "the green-pod/dead-topology scan is the only signal that survives a wedge — k8s readiness "
        "cannot see it"
    )


def test_create_topics_job_runs_the_audit():
    assert "verify-topic-partition-contract.sh" in JENKINS, (
        "ACTION=create-topics must audit what it just created"
    )
