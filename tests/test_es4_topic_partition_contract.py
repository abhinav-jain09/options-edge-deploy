"""Regression lock for the 2026-08-02/03 es4 wedge.

An UNDER-declared topic partition count in scripts/kafka/topics.env is created small, widened moments
later by the owning service, and any Kafka Streams app that read metadata in that window builds its
repartition/changelog topics at the small size — then dies on every rebalance ("invalid partitions:
expected: 32; actual: 4") behind a pod that still reports 1/1 Running. strike-flow-classifier
processed 0 records for 3h that way.

The verifier is EXECUTED here against a fake kafka-topics CLI, not grepped for reassuring strings:
the first version of it passed a source-text test while carrying two fail-open paths."""
import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ENV = (REPO / "scripts" / "kafka" / "topics.env").read_text()
CLEANUP = (REPO / "scripts" / "es4" / "cleanup-es4.sh").read_text()
VERIFY = REPO / "scripts" / "es4" / "verify-topic-partition-contract.sh"
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


def _declared():
    """Every OPTIONS_EDGE_ES4_TOPICS assignment, in order — the second one appends to the first."""
    out = {}
    for m in re.finditer(r'^OPTIONS_EDGE_ES4_TOPICS="([^"]*)"', ENV, re.M):
        for entry in m.group(1).split():
            if entry.startswith("$"):
                continue
            name, count = entry.rsplit(":", 1)
            out[name] = count
    assert out, "OPTIONS_EDGE_ES4_TOPICS missing"
    return out


# --------------------------------------------------------------------------- the contract itself

def test_incident_topics_are_declared_at_their_real_shape():
    declared = _declared()
    for topic in WIDENED_TO_32:
        assert topic in declared, f"{topic} dropped from the es4 contract"
        assert declared[topic] == "32", (
            f"{topic} declared at {declared[topic]}, but its real shape is 32. Declaring it smaller "
            "recreates the strike-flow-classifier wedge on the next clean-reset."
        )


def test_source_topic_of_the_wedged_classifier_is_not_under_declared():
    """The single entry that caused the incident, called out on its own so a bulk edit cannot
    quietly revert it."""
    assert _declared()["es.options.opra.tcbbo"] == "32"


def test_every_declaration_is_well_formed_and_unique():
    seen = {}
    for m in re.finditer(r'^OPTIONS_EDGE_ES4_TOPICS="([^"]*)"', ENV, re.M):
        for entry in m.group(1).split():
            if entry.startswith("$"):
                continue
            assert re.fullmatch(r"[^:\s]+:[1-9][0-9]*", entry), f"malformed entry: {entry}"
            name, count = entry.rsplit(":", 1)
            assert seen.get(name, count) == count, f"conflicting duplicate declaration for {name}"
            seen[name] = count


# ------------------------------------------------------------------- the verifier, actually run

def _run_verify(mode, describe_stdout, describe_status=0, env=None):
    """Execute the real script with a fake kafka-topics on PATH."""
    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp) / "kafka-topics"
        fake.write_text(
            "#!/usr/bin/env bash\n"
            f"cat <<'DESCRIBE_EOF'\n{describe_stdout}\nDESCRIBE_EOF\n"
            f"exit {describe_status}\n"
        )
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
        environ = dict(os.environ)
        environ["PATH"] = f"{tmp}:{environ['PATH']}"
        environ.pop("ES4_ALLOW_PARTITION_DRIFT", None)
        environ.update(env or {})
        return subprocess.run(
            ["bash", str(VERIFY), mode],
            capture_output=True, text=True, env=environ, cwd=str(REPO),
        )


def _describe_all(overrides=None):
    """A describe body for every declared topic, at its declared shape unless overridden."""
    overrides = overrides or {}
    lines = []
    for name, count in _declared().items():
        if overrides.get(name) == "OMIT":
            continue
        shown = overrides.get(name, count)
        lines.append(
            f"Topic: {name}\tTopicId: fake\tPartitionCount: {shown}\tReplicationFactor: 1\tConfigs:"
        )
    return "\n".join(lines)


def test_verify_passes_when_live_matches_the_contract():
    for mode in ("created", "steady"):
        r = _run_verify(mode, _describe_all())
        assert r.returncode == 0, f"{mode} failed on a clean broker: {r.stdout}{r.stderr}"


def test_steady_mode_fails_on_a_wider_than_declared_topic():
    """The 2026-08-02 signal: the owning service widened the topic past the contract."""
    r = _run_verify("steady", _describe_all({"es.options.opra.tcbbo": "64"}))
    assert r.returncode != 0, "a live-vs-declared disagreement must fail the steady audit"
    assert "WIDER-THAN-DECLARED" in r.stderr


def test_steady_failure_names_both_remedies_rather_than_guessing():
    """live > declared does not by itself prove which side is wrong, so the audit must not assert
    that it does — it must make the operator adjudicate."""
    r = _run_verify("steady", _describe_all({"es.options.opra.tcbbo": "64"}))
    assert "deliberate" in r.stderr and "record the live count" in r.stderr
    assert "was not deliberate" in r.stderr


def test_created_mode_does_not_adjudicate_width():
    """Right after reconciliation a pre-existing wider topic is not proof of a contract defect;
    the verdict belongs to the steady-state audit."""
    r = _run_verify("created", _describe_all({"es.options.opra.tcbbo": "64"}))
    assert r.returncode == 0
    assert "deferring the verdict" in r.stderr


def test_missing_topic_is_fatal_in_both_modes():
    for mode in ("created", "steady"):
        r = _run_verify(mode, _describe_all({"es.options.opra.tcbbo": "OMIT"}))
        assert r.returncode != 0, f"{mode} passed with a declared topic absent"
        assert "MISSING" in r.stderr


def test_narrower_topic_is_fatal_in_both_modes():
    for mode in ("created", "steady"):
        r = _run_verify(mode, _describe_all({"es.options.opra.tcbbo": "4"}))
        assert r.returncode != 0, f"{mode} passed with a topic narrower than declared"
        assert "NARROWER" in r.stderr


def test_describe_failure_is_fatal_even_with_partial_output():
    """A CLI/shim/docker failure that still prints plausible stdout must never read as a pass."""
    r = _run_verify("steady", _describe_all(), describe_status=1)
    assert r.returncode != 0
    assert "UNREACHABLE" in r.stderr


def test_empty_describe_is_fatal():
    r = _run_verify("steady", "")
    assert r.returncode != 0
    assert "UNREACHABLE" in r.stderr


def test_unparseable_describe_line_is_fatal():
    body = _describe_all({"es.options.opra.tcbbo": "OMIT"})
    body += "\nTopic: es.options.opra.tcbbo\tTopicId: fake\tPartitionCount: many\tConfigs:"
    r = _run_verify("steady", body)
    assert r.returncode != 0
    assert "UNPARSEABLE" in r.stderr


def test_escape_hatch_covers_drift_only():
    allow = {"ES4_ALLOW_PARTITION_DRIFT": "true"}
    # drift: tolerated
    r = _run_verify("steady", _describe_all({"es.options.opra.tcbbo": "64"}), env=allow)
    assert r.returncode == 0, "the hatch must cover validated wider-than-declared drift"
    # everything else: still fatal
    for overrides, label in (
        ({"es.options.opra.tcbbo": "OMIT"}, "missing"),
        ({"es.options.opra.tcbbo": "4"}, "narrower"),
    ):
        r = _run_verify("steady", _describe_all(overrides), env=allow)
        assert r.returncode != 0, f"the hatch must NOT suppress a {label} topic"
    r = _run_verify("steady", _describe_all(), describe_status=1, env=allow)
    assert r.returncode != 0, "the hatch must NOT suppress an unreachable broker"


def test_topic_name_matching_is_anchored():
    """es.options.databento.strike-flow must not be validated against the shape of
    es.options.databento.strike-flow.strike.avro."""
    r = _run_verify("steady", _describe_all({"es.options.databento.strike-flow": "OMIT"}))
    assert r.returncode != 0
    assert "MISSING: declared topic es.options.databento.strike-flow does not exist" in r.stderr


def test_unknown_mode_is_rejected():
    r = _run_verify("", _describe_all())
    assert r.returncode != 0
    r = subprocess.run(["bash", str(VERIFY), "whatever"], capture_output=True, text=True)
    assert r.returncode != 0


def test_verify_script_syntax_and_permissions():
    subprocess.run(["bash", "-n", str(VERIFY)], check=True)
    assert VERIFY.stat().st_mode & 0o111, "verify-topic-partition-contract.sh not executable"


# ------------------------------------------------------------------------ wiring into the reset

def test_clean_reset_runs_created_before_restore_and_steady_after():
    created = CLEANUP.index("verify-topic-partition-contract.sh' created")
    restore = CLEANUP.index("restoring es4 Deployments")
    steady = CLEANUP.index("verify-topic-partition-contract.sh' steady")
    assert created < restore < steady, (
        "the tautological creation check belongs before the restore; the audit with teeth only "
        "works once the owners have applied their own contracts"
    )


def test_post_restore_findings_are_fatal_not_advisory():
    """A reset that knowingly leaves a dead pipeline must not exit green."""
    assert "post_fail=1" in CLEANUP
    assert "post-restore audit FAILED" in CLEANUP


def test_wedge_scan_cannot_read_an_error_as_a_pass():
    assert "WEDGE SCAN INCONCLUSIVE" in CLEANUP, "an unreadable pod is an UNKNOWN, not a pass"
    assert "logs_status" in CLEANUP, "the log read's exit status must be captured separately"
    assert "grep -q 'invalid partitions" not in CLEANUP, (
        "grep -q makes kubectl die of SIGPIPE, so under pipefail a MATCH reads as failure"
    )


def test_create_topics_job_runs_the_steady_audit():
    assert "verify-topic-partition-contract.sh steady" in JENKINS, (
        "ACTION=create-topics runs against a live es4 — that is the steady-state contract audit"
    )


# ---------------------------------------------------- the reset's post-restore wiring
# These assert on cleanup-es4.sh's source: the script drives a whole cluster wipe and restore, so it
# cannot be executed here. The checks are written to pin the exact defects Codex found, each of which
# is a specific line shape rather than a vibe.

def test_settle_seconds_is_validated_not_passed_straight_to_sleep():
    """A non-numeric value would abort under `set -e` AFTER the state file is cleared, with no
    diagnostic — the worst possible moment to exit silently."""
    assert "ES4_POST_RESTORE_SETTLE_SECONDS must be a non-negative integer" in CLEANUP
    assert "exceeds the 3600s bound" in CLEANUP


def test_pod_discovery_failure_is_fatal():
    """`for p in $(kubectl get pods ...)` turns an RBAC/API failure into an empty list, and `set -e`
    does not fire on a substitution that only supplies loop words — the scan would find nothing and
    report success."""
    assert "pods_status" in CLEANUP
    assert "cannot list pods to scan for wedged topologies" in CLEANUP
    assert "pod list came back EMPTY" in CLEANUP
    assert "for p in $($KC get pods" not in CLEANUP


def test_wedge_scan_is_time_bounded_not_line_bounded():
    """The StreamsException is emitted once at startup; a chatty app pushes it past any fixed tail
    during the settle window."""
    assert '--since="${scan_window}s"' in CLEANUP
    assert "RESTORE_T0" in CLEANUP
    assert "--tail=5000" not in CLEANUP


def test_wedges_and_unreadable_pods_are_reported_independently():
    """An `elif` would hide unreadable pods whenever any wedge was found, understating the repair."""
    wedge_block = CLEANUP.index("WEDGED STREAMS TOPOLOGIES")
    inconclusive = CLEANUP.index("WEDGE SCAN INCONCLUSIVE")
    between = CLEANUP[wedge_block:inconclusive]
    assert "elif" not in between, "the two findings must not be mutually exclusive"


def test_audits_wait_for_restarted_rollouts_first():
    """Elapsed time alone does not establish that the owners are back and have applied their
    contracts — the self-heal restarts pods after the earlier readiness check."""
    assert "waiting for the ${healed} restarted deployment(s) to become available again" in CLEANUP
