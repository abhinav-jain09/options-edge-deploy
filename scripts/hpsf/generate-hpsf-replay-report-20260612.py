#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

REPLAY_DATE = "2026-06-12"
START = "2026-06-12T13:30:00Z"
END = "2026-06-12T20:00:00Z"
REPORT_NAME = "hpsf-replay-report-20260612.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate HPSF replay report for 2026-06-12")
    parser.add_argument("--input", default="build/hpsf-replay-20260612/evidence.json")
    parser.add_argument("--output", default=REPORT_NAME)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    evidence = read_evidence(Path(args.input))
    report, passed = build_report(evidence)
    Path(args.output).write_text(report, encoding="utf-8")
    print(args.output)
    return 0 if passed else 1


def read_evidence(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Evidence file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_report(evidence: dict[str, Any]) -> tuple[str, bool]:
    counts = evidence.get("counts", {})
    es_selection = evidence.get("esSelection", {})
    samples = evidence.get("samples", {})
    action_counts = Counter(evidence.get("actionCounts", {}))
    gate_reason_counts = Counter(evidence.get("gateReasonCounts", {}))
    top_execution = evidence.get("topExecutionStrikes", [])
    top_anchors = evidence.get("topFlowAnchors", [])
    order_enabled = bool(evidence.get("orderInstructionEnabledTrueFound", False))
    stage_a = evidence.get("stageA", {})
    stage_b = evidence.get("stageB", {})
    topic_configs = evidence.get("topicConfigs", {})

    failures: list[str] = []
    if int(counts.get("esTradesRead", 0)) <= 0:
        failures.append("ES trades read = 0")
    if int(counts.get("signalRecordsEmitted", 0)) <= 0:
        failures.append("signal topic is empty")
    if int(counts.get("latestSignalRecordsEmitted", 0)) <= 0:
        failures.append("latest-signal topic is empty")
    if int(counts.get("auditRecordsEmitted", 0)) <= 0:
        failures.append("audit topic is empty")
    if order_enabled:
        failures.append("orderInstruction.enabled=true appeared")
    if not stage_a.get("started", False):
        failures.append("Stage A startup evidence missing")
    if not stage_b.get("started", False):
        failures.append("Stage B startup evidence missing")

    passed = not failures
    lines: list[str] = []
    lines.append("# HPSF Replay Report 2026-06-12")
    lines.append("")
    lines.append(f"Final result: {'PASS' if passed else 'FAIL'}")
    if failures:
        lines.append("")
        lines.append("Failures:")
        lines.extend(f"- {failure}" for failure in failures)
    lines.append("")
    lines.append("## Databento Request")
    lines.append(f"- Date range requested: {START} to {END}")
    lines.append("- OPRA dataset/schema: OPRA.PILLAR / tcbbo")
    lines.append("- ES dataset/schema: GLBX.MDP3 / trades")
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
        lines.append(f"- {candidate.get('symbol')}: trade count={stats.get('tradeCount', 0)} total size={stats.get('totalSize', 0)} selected={candidate.get('selected', False)} reason={candidate.get('reason', '')}")
    lines.append(f"- selected contract: {es_selection.get('selectedSymbol', '')}")
    lines.append(f"- selection reason: {es_selection.get('selectionReason', '')}")
    lines.append("")
    lines.append("## ES Replay Counts")
    for key in ["esTradesRead", "esTradesPublished", "esTotalSize", "esFirstEventTime", "esLastEventTime", "esVwapFirst", "esVwapLast"]:
        lines.append(f"- {key}: {counts.get(key, '')}")
    lines.append("")
    lines.append("## OPRA Counts")
    for key in ["opraTcbboRecordsRead", "opraTcbboRecordsNormalized", "unknownInstrumentCount", "dlqCount"]:
        lines.append(f"- {key}: {counts.get(key, 0)}")
    lines.append("")
    lines.append("## HPSF Output Counts")
    for key in ["spxSpotRecordsProduced", "strikeFlowRecordsEmitted", "marketFlowRecordsEmitted", "strikeScoreRecordsEmitted", "signalRecordsEmitted", "latestSignalRecordsEmitted", "auditRecordsEmitted"]:
        lines.append(f"- {key}: {counts.get(key, 0)}")
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


if __name__ == "__main__":
    raise SystemExit(main())
