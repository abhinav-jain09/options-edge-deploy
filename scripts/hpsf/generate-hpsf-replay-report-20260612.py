#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPLAY_DATE = "2026-06-12"
START = "2026-06-12T13:30:00Z"
END = "2026-06-12T20:00:00Z"
def parse_args() -> argparse.Namespace:
    replay_date = os.environ.get("REPLAY_DATE", REPLAY_DATE)
    compact_date = "".join(ch for ch in replay_date if ch.isdigit()) or "20260612"
    parser = argparse.ArgumentParser(description="Generate HPSF replay report")
    parser.add_argument("--input", default=f"build/hpsf-replay-{compact_date}/evidence.json")
    parser.add_argument("--output", default=f"hpsf-replay-report-{compact_date}.md")
    parser.add_argument("--previous", default="", help="Optional previous replay summary JSON for build-to-build comparison")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    evidence = read_evidence(Path(args.input))
    previous = read_optional_evidence(args.previous)
    report, passed = build_report(evidence, previous)
    Path(args.output).write_text(report, encoding="utf-8")
    print(args.output)
    return 0 if passed else 1


def read_evidence(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Evidence file not found: {path}")
    evidence = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(evidence, dict):
        enrich_evidence_with_local_diagnostics(evidence, path)
    return evidence


def enrich_evidence_with_local_diagnostics(evidence: dict[str, Any], input_path: Path) -> None:
    artifact_dir = input_path.parent if input_path.parent.name == "artifacts" else Path("artifacts")
    replay = evidence.get("replay") if isinstance(evidence.get("replay"), dict) else {}
    replay_date = str(replay.get("date") or REPLAY_DATE)
    compact_date = "".join(ch for ch in replay_date if ch.isdigit()) or "20260612"
    build_dir = Path(f"build/hpsf-replay-{compact_date}")
    diagnostics_path = artifact_dir / "logs" / "root-cause-diagnostics.json"
    if diagnostics_path.exists() and "rootCauseDiagnostics" not in evidence:
        evidence["rootCauseDiagnostics"] = read_optional_json(diagnostics_path)
    download_summary = build_dir / "download-summary.json"
    if download_summary.exists() and "downloadSummary" not in evidence:
        evidence["downloadSummary"] = read_optional_json(download_summary)
    publish_summary = build_dir / "publish-summary.json"
    if publish_summary.exists() and "publishSummary" not in evidence:
        evidence["publishSummary"] = read_optional_json(publish_summary)


def read_optional_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def read_optional_evidence(path_value: str) -> dict[str, Any] | None:
    if not path_value:
        return None
    path = Path(path_value)
    if not path.exists():
        return {
            "_comparisonError": f"Previous replay summary not found: {path}",
        }
    return json.loads(path.read_text(encoding="utf-8"))


def build_report(evidence: dict[str, Any], previous: dict[str, Any] | None = None) -> tuple[str, bool]:
    jenkins = evidence.get("jenkins", {})
    replay = evidence.get("replay", {})
    evidence_mode = str(evidence.get("evidenceMode", "REAL")).upper()
    counts = evidence.get("counts", {})
    es_selection = evidence.get("esSelection", {})
    samples = evidence.get("samples", {})
    key_validation = evidence.get("keyValidation", {})
    action_counts = Counter(evidence.get("actionCounts", {}))
    gate_reason_counts = Counter(evidence.get("gateReasonCounts", {}))
    top_execution = evidence.get("topExecutionStrikes", [])
    top_anchors = evidence.get("topFlowAnchors", [])
    order_enabled = bool(evidence.get("orderInstructionEnabledTrueFound", False))
    stage_a = evidence.get("stageA", {})
    stage_b = evidence.get("stageB", {})
    stage_b_performance = evidence.get("stageBPerformance", {}) or {}
    topic_configs = evidence.get("topicConfigs", {})
    validation_failures = evidence.get("validationFailures") or []
    replay_date = str(replay.get("date") or REPLAY_DATE)
    compact_date = "".join(ch for ch in replay_date if ch.isdigit()) or "20260612"
    replay_run_name = str(
        jenkins.get("replayRunName")
        or replay.get("runName")
        or f"hpsf-historical-replay-{compact_date}"
    )
    validation_result = str(evidence.get("validationResult") or evidence.get("result") or "").upper()
    canonical_validation_pass = validation_result == "PASS" and not validation_failures
    stage_b_scheduler_required = [
        "stageB.chainSnapshot.update.count",
        "activeChainStorePutCount",
        "activeChainRegisteredCount",
        "stageB.activeChains.count.max",
        "stageB.punctuation.fire.count",
        "activeChainsEvaluatedCount",
        "stageB.evaluation.count",
    ]
    stage_b_required = [
        *stage_b_scheduler_required,
        "underlyingStateRecordsReceived",
        "underlyingStateRecordsStored",
        "underlyingStateLookupHitCount",
        "underlyingStateLookupMissCount",
        "underlyingStateNullVwapCount",
        "underlyingStateNullDistanceCount",
        "healthyUnderlyingStateCount",
    ]

    def stage_b_counter(name: str) -> int | None:
        value = stage_b_performance.get(name, counts.get(name))
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def selected_stats() -> dict[str, Any]:
        stats = es_selection.get("selectedStats")
        return stats if isinstance(stats, dict) else {}

    def es_count(name: str) -> Any:
        if counts.get(name) not in (None, ""):
            return counts.get(name)
        field = {
            "esFirstEventTime": "firstEventTime",
            "esLastEventTime": "lastEventTime",
            "esVwapFirst": "esVwapFirst",
            "esVwapLast": "esVwapLast",
        }.get(name)
        return selected_stats().get(field, "") if field else ""

    def suspicious_candidate_copy() -> bool:
        candidates = es_selection.get("candidates", [])
        if len(candidates) < 2:
            return False
        first, second = candidates[0] or {}, candidates[1] or {}
        if second.get("comparisonStatus") == "NOT_AVAILABLE":
            return False
        a = first.get("stats", {}) or {}
        b = second.get("stats", {}) or {}
        independent_marker = any(first.get(key) or second.get(key) for key in ["verificationHash", "queryId", "sourceQueryId"])
        compared = ["tradeCount", "totalSize", "firstEventTime", "lastEventTime"]
        return bool(a and b and not independent_marker and all(a.get(key) == b.get(key) for key in compared))

    missing_stage_b_counters = [name for name in stage_b_required if stage_b_counter(name) is None]
    chain_updates = stage_b_counter("stageB.chainSnapshot.update.count")
    active_puts = stage_b_counter("activeChainStorePutCount")
    active_max = stage_b_counter("stageB.activeChains.count.max")
    punctuation = stage_b_counter("stageB.punctuation.fire.count")
    active_evaluated = stage_b_counter("activeChainsEvaluatedCount")
    evaluations = stage_b_counter("stageB.evaluation.count")
    received = stage_b_counter("underlyingStateRecordsReceived")
    stored = stage_b_counter("underlyingStateRecordsStored")
    hits = stage_b_counter("underlyingStateLookupHitCount")
    misses = stage_b_counter("underlyingStateLookupMissCount")
    null_vwap = stage_b_counter("underlyingStateNullVwapCount")
    null_distance = stage_b_counter("underlyingStateNullDistanceCount")
    healthy = stage_b_counter("healthyUnderlyingStateCount")
    stage_b_join_healthy = (
        not missing_stage_b_counters
        and (received or 0) > 0
        and (stored or 0) > 0
        and (hits or 0) > 0
        and (healthy or 0) > 0
    )
    stage_b_vwap_usable = (
        not missing_stage_b_counters
        and (healthy or 0) > 0
        and (null_vwap or 0) == 0
        and (null_distance or 0) == 0
    )

    failures: list[str] = []
    report_diagnostics: list[str] = []

    def add_report_only_diagnostic(message: str) -> None:
        if canonical_validation_pass:
            report_diagnostics.append(message)
        else:
            failures.append(message)

    if "FIXTURE" in evidence_mode or "DRY_RUN" in evidence_mode:
        failures.append(f"{evidence_mode} evidence cannot close HPSF-82A/HPSF-83")
    for failure in validation_failures:
        if isinstance(failure, dict):
            code = failure.get("code", "VALIDATION_FAILURE")
            detail = failure.get("detail", "")
            failures.append(f"{code}: {detail}".strip(": "))
        elif failure:
            failures.append(str(failure))
    if not str(jenkins.get("buildUrl", "")).strip():
        failures.append("Jenkins build URL missing")
    if not str(jenkins.get("commitSha", "")).strip():
        failures.append("commit SHA missing")
    if not str(es_selection.get("selectedSymbol", "")).strip():
        failures.append("selected ES symbol missing")
    if not str(es_selection.get("resolvedInstrumentId", "")).strip():
        failures.append("selected ES instrument_id missing")
    if int(counts.get("esTradesRead", 0)) <= 0:
        failures.append("ES trades read = 0")
    if int(counts.get("opraTcbboRecordsRead", 0)) <= 0:
        failures.append("OPRA records read = 0")
    if int(counts.get("strikeFlowRecordsEmitted", 0)) <= 0:
        failures.append("strike-flow topic is empty")
    if int(counts.get("underlyingStateRecordsEmitted", 0)) <= 0:
        failures.append("underlying-state topic is empty")
    if int(counts.get("signalRecordsEmitted", 0)) <= 0:
        failures.append("signal topic is empty")
    if int(counts.get("latestSignalRecordsEmitted", 0)) <= 0:
        failures.append("latest-signal topic is empty")
    if int(counts.get("auditRecordsEmitted", 0)) <= 0:
        failures.append("audit topic is empty")
    if int(counts.get("signalRecordsEmitted", 0)) > 0 and not action_counts:
        failures.append("actionCounts empty while signal records exist")
    if int(counts.get("auditRecordsEmitted", 0)) > 0 and not gate_reason_counts:
        failures.append("gateReasonCounts empty while audit records exist")
    if suspicious_candidate_copy():
        add_report_only_diagnostic("ESM6/ESU6 candidate stats look copied; independent query evidence missing")
    for key in ["esFirstEventTime", "esLastEventTime", "esVwapFirst", "esVwapLast"]:
        if es_count(key) in (None, ""):
            add_report_only_diagnostic(f"{key} missing")
    if missing_stage_b_counters:
        failures.append(f"Stage B underlying-state counters missing: {missing_stage_b_counters}")
    if (chain_updates or 0) <= 0:
        failures.append("STAGE_B_CHAIN_SNAPSHOT_UPDATE_COUNT_ZERO")
    if (chain_updates or 0) > 0 and (active_puts or 0) <= 0:
        failures.append("STAGE_B_ACTIVE_CHAIN_NOT_REGISTERED")
    if (active_puts or 0) > 0 and (punctuation or 0) <= 0:
        failures.append("STAGE_B_SCHEDULED_PUNCTUATION_DID_NOT_FIRE")
    if (punctuation or 0) > 0 and (evaluations or 0) <= 0 and (active_max or 0) <= 0:
        failures.append("STAGE_B_NO_ACTIVE_CHAINS_AT_PUNCTUATION")
    if (punctuation or 0) > 0 and (active_max or 0) > 0 and (evaluations or 0) <= 0:
        failures.append("STAGE_B_ACTIVE_CHAINS_NOT_EVALUATED")
    if (active_evaluated or 0) <= 0:
        failures.append("STAGE_B_ACTIVE_CHAINS_EVALUATED_COUNT_ZERO")
    if not stage_b_join_healthy:
        failures.append("Stage B underlying join unhealthy")
    if not stage_b_vwap_usable:
        failures.append("Stage B VWAP unusable")
    signal_sample = samples.get("signal") if isinstance(samples.get("signal"), dict) else {}
    latest_signal_sample = samples.get("latestSignal") if isinstance(samples.get("latestSignal"), dict) else {}
    audit_sample = samples.get("audit") if isinstance(samples.get("audit"), dict) else {}
    def reason_code(reason: Any) -> str:
        if not isinstance(reason, str):
            return ""
        text = reason.strip()
        if not text:
            return ""
        return text.replace("=", " ").replace(":", " ").split()[0]

    def reason_codes(values: set[Any]) -> set[str]:
        return {code for code in (reason_code(value) for value in values) if code}

    sample_reasons = set(signal_sample.get("reasons") or [])
    sample_reasons.update(audit_sample.get("noTradeGates") or [])
    sample_reason_codes = reason_codes(sample_reasons)
    aggregate_reason_codes = set(sample_reason_codes)
    aggregate_reason_codes.update(
        reason_code(reason)
        for reason, count in gate_reason_counts.items()
        if as_int(count) > 0 and reason_code(reason)
    )
    expected_trade_date = str(replay.get("date") or "")
    for label, sample_payload in [
        ("signal", signal_sample),
        ("latestSignal", latest_signal_sample),
        ("audit", audit_sample),
    ]:
        trade_date = str(sample_payload.get("tradeDate") or "")
        expiry = str(sample_payload.get("expiry") or "")
        if trade_date and expiry and trade_date != expiry:
            failures.append(f"SAME_DAY_EXPIRY_MISMATCH: {label}.tradeDate={trade_date} {label}.expiry={expiry}")
        if expected_trade_date and expiry and expiry != expected_trade_date:
            failures.append(f"REPLAY_EXPIRY_DOES_NOT_MATCH_REPLAY_DATE: {label}.expiry={expiry}, replayDate={expected_trade_date}")
    if "NO_VALID_TODAY_EXPIRY" in aggregate_reason_codes:
        failures.append("NO_VALID_TODAY_EXPIRY observed in replay outputs")
    data_failure_reasons = {
        "SPX_SPOT_STALE",
        "ES_TRADE_STALE",
        "VWAP_STALE",
        "BASIS_STALE",
        "OPRA_FLOW_STALE",
        "INSUFFICIENT_CHAIN_COVERAGE",
        "INSUFFICIENT_CALL_COVERAGE",
        "INSUFFICIENT_PUT_COVERAGE",
        "NO_LIQUID_EXECUTION_CANDIDATE",
        "EXECUTION_CANDIDATE_TOO_FAR",
    }
    strategy_valid_reasons = {
        "NO_ACTIVE_VWAP_SETUP",
        "MARKET_SCORE_BELOW_THRESHOLD",
        "MIXED_FLOW",
        "COMPLEX_FLOW_DOMINANT",
    }
    fallback_reasons = {
        "MARKET_EVALUATOR_NOT_READY",
        "UNDERLYING_STATE_UNAVAILABLE",
        "VWAP",
    }
    if action_counts.get("NO_TRADE", 0) == int(counts.get("signalRecordsEmitted", 0) or 0):
        sample_vwap_available = (
            signal_sample.get("vwap") is not None
            and signal_sample.get("distanceToVwap") is not None
            and str(signal_sample.get("internalVwapState") or "").upper() not in {"", "VWAP_UNAVAILABLE"}
        )
        if sample_reason_codes & fallback_reasons and not sample_vwap_available:
            failures.append("all NO_TRADE outputs are fallback because underlying/VWAP context is unavailable")
        if aggregate_reason_codes & data_failure_reasons and not aggregate_reason_codes & strategy_valid_reasons:
            failures.append("all NO_TRADE outputs are explained only by data-health gates")
    if not isinstance(audit_sample.get("gateDiagnostics"), dict) or not audit_sample.get("gateDiagnostics"):
        failures.append("STAGE_B_AUDIT_GATE_DIAGNOSTICS_EMPTY")
    if not isinstance(audit_sample.get("chainCoverageDiagnostics"), dict) or not audit_sample.get("chainCoverageDiagnostics"):
        failures.append("STAGE_B_AUDIT_CHAIN_COVERAGE_DIAGNOSTICS_EMPTY")
    sample_time = parse_time(signal_sample.get("eventTime"))
    replay_start = parse_time(replay.get("start", START))
    replay_end = parse_time(replay.get("end", END))
    if sample_time is None:
        failures.append("STAGE_B_SAMPLE_SIGNAL_EVENT_TIME_MISSING")
    elif replay_start and replay_end and not (replay_start <= sample_time <= replay_end):
        failures.append("STAGE_B_SAMPLE_SIGNAL_EVENT_TIME_OUTSIDE_REPLAY_WINDOW")
    if order_enabled:
        failures.append("orderInstruction.enabled=true appeared")
    if not stage_a.get("started", False):
        failures.append("Stage A startup evidence missing")
    if not stage_b.get("started", False):
        failures.append("Stage B startup evidence missing")
    if int(counts.get("stageAEmittedFinalSignalCount", 0)) > 0:
        failures.append("Stage A emitted final signal")
    if key_validation.get("signalKeyValid") is not True:
        failures.append("signal key validation failed")
    if key_validation.get("latestSignalKeyValid") is not True:
        failures.append("latest-signal key validation failed")
    if key_validation.get("auditKeyValid") is not True:
        failures.append("audit key validation failed")
    for sample_key in ["signal", "latestSignal", "audit"]:
        if not samples.get(sample_key):
            failures.append(f"sample {sample_key} JSON missing")

    passed = not failures
    lines: list[str] = []
    lines.append(f"# HPSF Replay Report {replay_date}")
    lines.append("")
    lines.append(f"Final result: {'PASS' if passed else 'FAIL'}")
    lines.append(f"Evidence mode: {evidence_mode}")
    lines.append("")
    lines.append("## Jenkins")
    lines.append(f"- Build URL: {jenkins.get('buildUrl', '')}")
    lines.append(f"- Build number: {jenkins.get('buildNumber', '')}")
    lines.append(f"- Commit SHA: {jenkins.get('commitSha', '')}")
    lines.append(f"- Job name: {jenkins.get('jobName', '')}")
    lines.append(f"- Replay run name: {replay_run_name}")
    if failures:
        lines.append("")
        lines.append("Failures:")
        lines.extend(f"- {failure}" for failure in failures)
        lines.append("")
        lines.append("## Root Cause Analysis")
        lines.extend(format_root_cause_analysis(evidence, failures))
        lines.append("")
        lines.append("## Failure Detail")
        if validation_failures:
            lines.extend(f"- validationFailure: {format_validation_failure(failure)}" for failure in validation_failures)
        else:
            lines.append("- validationFailure: none recorded")
        lines.append(f"- dominantAction: {dominant_counter(action_counts)}")
        lines.append(f"- topGateReason: {dominant_counter(gate_reason_counts)}")
        lines.extend(format_sample_failure_context(signal_sample, audit_sample))
    if report_diagnostics:
        lines.append("")
        lines.append("Report diagnostics:")
        lines.extend(f"- {diagnostic}" for diagnostic in report_diagnostics)
    lines.append("")
    lines.append("## Previous Build Comparison")
    lines.extend(format_previous_comparison(evidence, previous, passed))
    lines.append("")
    lines.append("## Databento Request")
    lines.append(f"- Replay date: {replay.get('date', REPLAY_DATE)}")
    lines.append(f"- Date range requested: {replay.get('start', START)} to {replay.get('end', END)}")
    lines.append(f"- Replay topic namespace: {replay.get('topicPrefix', 'options.replay.20260612')}")
    lines.append(f"- OPRA dataset/schema: {replay.get('opraDataset', 'OPRA.PILLAR')} / {replay.get('opraSchema', 'tcbbo')}")
    lines.append(f"- ES dataset/schema: {replay.get('esDataset', 'GLBX.MDP3')} / {replay.get('esSchema', 'trades')}")
    lines.append(f"- SPX spot source: {replay.get('spxSpotSource', counts.get('spxSpotSource', ''))}")
    lines.append(f"- ES reference mode: {es_selection.get('referenceMode', 'UNKNOWN')}")
    lines.append(f"- Requested ES symbol: {es_selection.get('requestedSymbol', 'UNKNOWN')}")
    lines.append(f"- Requested ES stype_in: {es_selection.get('requestedStypeIn', 'UNKNOWN')}")
    lines.append("")
    lines.append("## ES Resolution")
    lines.append(f"- resolvedRawSymbol: {es_selection.get('resolvedRawSymbol', '')}")
    lines.append(f"- resolvedInstrumentId: {es_selection.get('resolvedInstrumentId', '')}")
    lines.append("- resolution intervals:")
    for interval in es_selection.get("resolution", {}).get("resolvedIntervals", []):
        lines.append(f"  - {interval.get('rawSymbol')} {interval.get('instrumentId')} {interval.get('startTime')} to {interval.get('endTime')} source={interval.get('source')}")
    lines.append("")
    lines.append("## ESM6 vs ESU6 Comparison")
    for candidate in es_selection.get("candidates", []):
        stats = candidate.get("stats", {})
        if candidate.get("comparisonStatus") == "NOT_AVAILABLE":
            lines.append(f"- {candidate.get('symbol')}: comparisonStatus=NOT_AVAILABLE reason={candidate.get('reason', '')}")
        else:
            lines.append(f"- {candidate.get('symbol')}: trade count={stats.get('tradeCount', 0)} total size={stats.get('totalSize', 0)} selected={candidate.get('selected', False)} reason={candidate.get('reason', '')}")
    lines.append(f"- selected contract: {es_selection.get('selectedSymbol', '')}")
    lines.append(f"- selection reason: {es_selection.get('selectionReason', '')}")
    lines.append("")
    lines.append("## ES Replay Counts")
    for key in ["esTradesRead", "esTradesPublished", "esTotalSize", "esFirstEventTime", "esLastEventTime", "esVwapFirst", "esVwapLast"]:
        lines.append(f"- {key}: {es_count(key)}")
    lines.append("")
    lines.append("## OPRA Counts")
    for key in ["opraTcbboRecordsRead", "opraTcbboRecordsNormalized", "unknownInstrumentCount", "dlqCount"]:
        lines.append(f"- {key}: {counts.get(key, 0)}")
    lines.append("")
    lines.append("## HPSF Output Counts")
    for key in ["spxSpotRecordsProduced", "strikeFlowRecordsEmitted", "underlyingStateRecordsEmitted", "marketFlowRecordsEmitted", "strikeScoreRecordsEmitted", "signalRecordsEmitted", "latestSignalRecordsEmitted", "auditRecordsEmitted", "stageAEmittedFinalSignalCount"]:
        lines.append(f"- {key}: {counts.get(key, 0)}")
    lines.append("")
    lines.append("## Stage B Scheduled Evaluation Health")
    for key in stage_b_scheduler_required:
        value = stage_b_counter(key)
        lines.append(f"- {key}: {'' if value is None else value}")
    lines.append("")
    lines.append("## Stage B Underlying-State Health")
    for key in stage_b_required:
        if key in stage_b_scheduler_required:
            continue
        value = stage_b_counter(key)
        lines.append(f"- {key}: {'' if value is None else value}")
    lines.append(f"- Stage B underlying join healthy: {str(stage_b_join_healthy).lower()}")
    lines.append(f"- Stage B VWAP usable: {str(stage_b_vwap_usable).lower()}")
    lines.append("")
    lines.append("## Kafka Key Validation")
    lines.append(f"- signal key = tradeDate|expiry|evaluationId: {key_validation.get('signalKeyValid', False)}")
    lines.append(f"- latest-signal key = tradeDate|expiry: {key_validation.get('latestSignalKeyValid', False)}")
    lines.append(f"- audit key = tradeDate|expiry|evaluationId: {key_validation.get('auditKeyValid', False)}")
    lines.append("")
    lines.append("## Startup Evidence")
    lines.append(f"- Stage A startup log evidence: {stage_a.get('startupLog', '')}")
    lines.append(f"- Stage B startup log evidence: {stage_b.get('startupLog', '')}")
    lines.append("")
    lines.append("## Topic Configs")
    for topic, config in topic_configs.items():
        lines.append(f"- {topic}: {config}")
    lines.append("")
    lines.append("## Samples")
    for title, key in [("Sample final signal JSON", "signal"), ("Sample latest-signal JSON", "latestSignal"), ("Sample audit JSON", "audit")]:
        lines.append(f"### {title}")
        lines.append("```json")
        lines.append(json.dumps(samples.get(key, {}), indent=2, sort_keys=True))
        lines.append("```")
    lines.append("")
    lines.append("## Count By Action")
    for action in ["NO_TRADE", "WATCH_CALL_RECLAIM", "BUY_CALL_EARLY", "BUY_CALL_CONFIRMED", "WATCH_PUT_BREAKDOWN", "BUY_PUT_EARLY", "BUY_PUT_CONFIRMED"]:
        lines.append(f"- {action}: {action_counts.get(action, 0)}")
    lines.append("")
    lines.append("## Gate Reason Counts")
    if gate_reason_counts:
        lines.extend(f"- {reason}: {count}" for reason, count in sorted(gate_reason_counts.items()))
    else:
        lines.append("- none: 0")
    lines.append("")
    lines.append("## Top Execution Strikes Selected")
    lines.extend(f"- {item}" for item in top_execution) if top_execution else lines.append("- none")
    lines.append("")
    lines.append("## Top Flow Anchors Selected")
    lines.extend(f"- {item}" for item in top_anchors) if top_anchors else lines.append("- none")
    lines.append("")
    lines.append("## Missing Scenario Notes")
    for note in evidence.get("missingScenarioNotes", ["Historical replay evidence is deterministic; live market smoke remains separate."]):
        lines.append(f"- {note}")
    lines.append("")
    lines.append(f"orderInstruction.enabled=true ever appeared: {'YES' if order_enabled else 'NO'}")
    lines.append(f"Final PASS/FAIL: {'PASS' if passed else 'FAIL'}")
    lines.append("")
    return "\n".join(lines), passed


def format_root_cause_analysis(evidence: dict[str, Any], failures: list[str]) -> list[str]:
    counts = evidence.get("counts", {}) or {}
    stage_b_performance = evidence.get("stageBPerformance", {}) or {}
    diagnostics = evidence.get("rootCauseDiagnostics", {}) or {}
    publish_summary = evidence.get("publishSummary", {}) or {}
    download_summary = evidence.get("downloadSummary", {}) or {}
    evidence_mode = str(evidence.get("evidenceMode", "REAL")).upper()
    stage_a = diagnostics.get("services", {}).get("stageA", {}) if isinstance(diagnostics.get("services"), dict) else {}
    stage_b = diagnostics.get("services", {}).get("stageB", {}) if isinstance(diagnostics.get("services"), dict) else {}
    kafka = diagnostics.get("kafka", {}) if isinstance(diagnostics.get("kafka"), dict) else {}
    stage_a_group = kafka.get("stageAConsumerGroup", {}) if isinstance(kafka.get("stageAConsumerGroup"), dict) else {}
    stage_a_internal = kafka.get("stageAInternalTopics", {}) if isinstance(kafka.get("stageAInternalTopics"), dict) else {}

    opra_input = max(
        as_int(counts.get("opraTcbboRecordsRead")),
        as_int(counts.get("opraTcbboRecordsPublished")),
        topic_count((publish_summary.get("publish", {}) or {}).get("topicCounts", {}) or {}, ".opra.tcbbo"),
        as_int((download_summary.get("counts", {}) or {}).get("opraTcbboRecordsRead")),
        as_int(download_summary.get("opraTcbboRecordsRead")),
    )
    es_input = max(
        as_int(counts.get("esTradesRead")),
        as_int(counts.get("esTradesPublished")),
        as_int(download_summary.get("esTradesRead")),
    )
    strike_flow = as_int(counts.get("strikeFlowRecordsEmitted"))
    signal_count = as_int(counts.get("signalRecordsEmitted"))
    latest_count = as_int(counts.get("latestSignalRecordsEmitted"))
    audit_count = as_int(counts.get("auditRecordsEmitted"))
    underlying_emitted = as_int(counts.get("underlyingStateRecordsEmitted"))
    def stage_b_runtime_counter(name: str) -> int:
        if name in stage_b_performance:
            return as_int(stage_b_performance.get(name))
        return as_int(counts.get(name))

    underlying_received = stage_b_runtime_counter("underlyingStateRecordsReceived")
    underlying_stored = stage_b_runtime_counter("underlyingStateRecordsStored")
    underlying_hits = stage_b_runtime_counter("underlyingStateLookupHitCount")
    underlying_misses = stage_b_runtime_counter("underlyingStateLookupMissCount")
    stage_a_state = str(stage_a.get("streamState") or infer_stream_state(stage_a.get("logTail", "")) or "")
    stage_b_state = str(stage_b.get("streamState") or infer_stream_state(stage_b.get("logTail", "")) or "")
    group_exists = stage_a_group.get("exists")
    internal_count = as_int(stage_a_internal.get("count"))

    root_cause = "Replay failed, but available artifacts were not sufficient to isolate one service or Kafka layer automatically."
    if opra_input <= 0 and es_input <= 0 and strike_flow <= 0:
        root_cause = "Replay input was not loaded or published; Kafka output validation failed downstream because there were no source records to process."
    elif strike_flow <= 0 and "REBALANCING" in stage_a_state and "RUNNING" not in stage_a_state:
        root_cause = "Stage A Kafka Streams did not become operational; it remained in REBALANCING, so real replay input was not processed into strike-flow."
    elif strike_flow <= 0 and group_exists is False:
        root_cause = "Stage A did not establish its replay Kafka consumer group, so it could not consume the OPRA replay topic."
    elif strike_flow <= 0 and "count" in stage_a_internal and internal_count == 0:
        root_cause = "Stage A did not create Kafka Streams internal topics for this replay application id, indicating startup or group-assignment failed before processing."
    elif strike_flow <= 0:
        root_cause = "Stage A produced zero strike-flow records even though replay input existed; inspect Stage A service logs and DLQ for processor-level rejection or startup failure."
    elif signal_count <= 0 or latest_count <= 0 or audit_count <= 0:
        root_cause = "Stage A produced strike-flow, but downstream Stage B outputs were empty or incomplete; inspect Stage B service state, scheduler counters, and underlying-state join health."
    elif underlying_emitted > 0 and (underlying_received <= 0 or underlying_stored <= 0 or underlying_hits <= 0):
        root_cause = "Underlying-state records were produced, but Stage B did not materialize them before validation; wait for the Stage B replay consumer group to catch up and inspect the underlying-state join if received/stored/hit counters remain zero."
    elif any("ALL_NO_TRADE_DATA_FAILURE" in failure for failure in failures):
        root_cause = "Stage B consumed underlying-state and produced signals, but every final action remained NO_TRADE because option-chain coverage, liquid execution-candidate, or market-scoring warmup gates blocked candidate selection."

    lines = [
        f"- Primary root cause: {root_cause}",
        f"- Replay input health: OPRA records={opra_input}, ES records={es_input}",
        f"- Stage A output health: strike-flow records={strike_flow}",
        f"- Stage B output health: signal={signal_count}, latest-signal={latest_count}, audit={audit_count}",
    ]
    if stage_a_state:
        lines.append(f"- Stage A stream state: {stage_a_state}")
    if stage_b_state:
        lines.append(f"- Stage B stream state: {stage_b_state}")
    if underlying_emitted > 0:
        lines.append(f"- Underlying-state records emitted: {underlying_emitted}")
        lines.append(
            "- Stage B underlying-state counters: "
            f"received={underlying_received}, stored={underlying_stored}, "
            f"lookupHits={underlying_hits}, lookupMisses={underlying_misses}"
        )
    if group_exists is not None:
        lines.append(f"- Stage A replay consumer group exists: {str(bool(group_exists)).lower()}")
    if "count" in stage_a_internal:
        lines.append(f"- Stage A Kafka Streams internal topics found: {internal_count}")
    if diagnostics.get("logging", {}).get("slf4jNoopDetected"):
        lines.append("- Diagnostic limitation: SLF4J NOP logging was detected, so Kafka Streams may have hidden the low-level exception.")
    if "DRY_RUN" in evidence_mode and (opra_input > 0 or es_input > 0):
        lines.append("- Evidence-mode note: report metadata says DRY_RUN, but collected replay summaries show real input existed; this is a fallback-report marker, not the primary replay root cause.")
    if failures:
        lines.append("- Validation symptoms: " + "; ".join(failures[:8]))
    return lines


def infer_stream_state(text: Any) -> str:
    value = str(text or "")
    if "RUNNING" in value:
        return "RUNNING"
    if "REBALANCING" in value:
        return "REBALANCING"
    if "CREATED" in value:
        return "CREATED"
    return ""


def topic_count(topic_counts: dict[str, Any], suffix: str) -> int:
    for topic, count in topic_counts.items():
        if str(topic).endswith(suffix):
            return as_int(count)
    return 0


def format_validation_failure(failure: Any) -> str:
    if isinstance(failure, dict):
        code = failure.get("code", "VALIDATION_FAILURE")
        detail = failure.get("detail", "")
        return f"{code} - {detail}" if detail else str(code)
    return str(failure)


def dominant_counter(counter: Counter) -> str:
    if not counter:
        return "none"
    item, count = counter.most_common(1)[0]
    return f"{item} ({count})"


def format_sample_failure_context(signal_sample: dict[str, Any], audit_sample: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if signal_sample:
        for key in ["action", "spot", "executionStrike", "flowAnchorStrike", "internalVwapState", "vwap", "distanceToVwap"]:
            if key in signal_sample:
                lines.append(f"- sampleSignal.{key}: {signal_sample.get(key)}")
    diagnostics = audit_sample.get("chainCoverageDiagnostics") if isinstance(audit_sample, dict) else None
    if isinstance(diagnostics, dict):
        for key in ["atmStrike", "atmStrikePresent", "activeStrikeCount", "validQuoteStrikeCount", "coverageRatio", "requiredRangeAroundSpotPoints", "strikeStep"]:
            if key in diagnostics:
                lines.append(f"- chainCoverage.{key}: {diagnostics.get(key)}")
    gate_diagnostics = audit_sample.get("gateDiagnostics") if isinstance(audit_sample, dict) else None
    if isinstance(gate_diagnostics, dict):
        for key in ["marketScoringWarmupActive", "marketScoringSampleCount", "marketScoringMinSamples", "esTradeAgeMs", "vwapAgeMs", "basisAgeMs", "spxSpotAgeMs"]:
            if key in gate_diagnostics:
                lines.append(f"- gateDiagnostics.{key}: {gate_diagnostics.get(key)}")
    return lines or ["- sample context unavailable"]


def format_previous_comparison(current: dict[str, Any], previous: dict[str, Any] | None, passed: bool) -> list[str]:
    if not previous:
        return ["- Previous replay summary: unavailable"]
    if previous.get("_comparisonError"):
        return [f"- Previous replay summary: {previous.get('_comparisonError')}"]

    current_counts = current.get("counts", {}) or {}
    previous_counts = previous.get("counts", {}) or {}
    current_actions = Counter(current.get("actionCounts", {}) or {})
    previous_actions = Counter(previous.get("actionCounts", {}) or {})
    current_failures = current.get("validationFailures") or []
    previous_failures = previous.get("validationFailures") or []

    comparisons = [
        ("validationResult", current.get("validationResult", "UNKNOWN"), previous.get("validationResult", "UNKNOWN")),
        ("validationFailureCount", len(current_failures), len(previous_failures)),
        ("signalRecordsEmitted", as_int(current_counts.get("signalRecordsEmitted")), as_int(previous_counts.get("signalRecordsEmitted"))),
        ("latestSignalRecordsEmitted", as_int(current_counts.get("latestSignalRecordsEmitted")), as_int(previous_counts.get("latestSignalRecordsEmitted"))),
        ("auditRecordsEmitted", as_int(current_counts.get("auditRecordsEmitted")), as_int(previous_counts.get("auditRecordsEmitted"))),
        ("nonNoTradeActions", non_no_trade_count(current_actions), non_no_trade_count(previous_actions)),
        ("noTradeActions", current_actions.get("NO_TRADE", 0), previous_actions.get("NO_TRADE", 0)),
        ("dlqCount", as_int(current_counts.get("dlqCount")), as_int(previous_counts.get("dlqCount"))),
        ("stageB.evaluation.count", as_int(current_counts.get("stageB.evaluation.count")), as_int(previous_counts.get("stageB.evaluation.count"))),
        ("underlyingStateLookupHitCount", as_int(current_counts.get("underlyingStateLookupHitCount")), as_int(previous_counts.get("underlyingStateLookupHitCount"))),
    ]

    lines = [
        f"- Previous build: {previous.get('jenkins', {}).get('buildNumber', '')}",
        f"- Current build: {current.get('jenkins', {}).get('buildNumber', '')}",
    ]
    for name, current_value, previous_value in comparisons:
        lines.append(f"- {name}: current={current_value} previous={previous_value} delta={delta_text(current_value, previous_value)}")

    improvements = improvement_notes(current, previous)
    if passed:
        if improvements:
            lines.append("- Success improvement summary: " + "; ".join(improvements))
        else:
            lines.append("- Success improvement summary: PASS retained; no numeric improvement detected against previous summary")
    else:
        lines.append("- Success improvement summary: not applicable because current replay did not pass")
    return lines


def as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def non_no_trade_count(actions: Counter) -> int:
    return sum(count for action, count in actions.items() if action != "NO_TRADE")


def delta_text(current: Any, previous: Any) -> str:
    if isinstance(current, int) and isinstance(previous, int):
        delta = current - previous
        return f"{delta:+d}"
    return "changed" if current != previous else "unchanged"


def improvement_notes(current: dict[str, Any], previous: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    current_result = current.get("validationResult")
    previous_result = previous.get("validationResult")
    if current_result == "PASS" and previous_result != "PASS":
        notes.append(f"validation improved from {previous_result or 'UNKNOWN'} to PASS")
    current_failures = len(current.get("validationFailures") or [])
    previous_failures = len(previous.get("validationFailures") or [])
    if current_failures < previous_failures:
        notes.append(f"validation failures reduced from {previous_failures} to {current_failures}")
    current_actions = Counter(current.get("actionCounts", {}) or {})
    previous_actions = Counter(previous.get("actionCounts", {}) or {})
    if non_no_trade_count(current_actions) > non_no_trade_count(previous_actions):
        notes.append("non-NO_TRADE actions increased")
    current_counts = current.get("counts", {}) or {}
    previous_counts = previous.get("counts", {}) or {}
    if as_int(current_counts.get("underlyingStateLookupHitCount")) > as_int(previous_counts.get("underlyingStateLookupHitCount")):
        notes.append("underlying-state lookup hits increased")
    if as_int(current_counts.get("dlqCount")) < as_int(previous_counts.get("dlqCount")):
        notes.append("DLQ count decreased")
    return notes


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
