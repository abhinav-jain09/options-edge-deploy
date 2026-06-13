#!/usr/bin/env python3
"""Merge HPSF Stage B runtime metrics from stage-b.log into replay summary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PREFIX = "HPSF_STAGE_B_PERFORMANCE_METRICS_JSON="
REQUIRED_COUNTERS = [
    "underlyingStateRecordsReceived",
    "underlyingStateRecordsStored",
    "underlyingStateLookupHitCount",
    "underlyingStateLookupMissCount",
    "underlyingStateNullVwapCount",
    "underlyingStateNullDistanceCount",
    "healthyUnderlyingStateCount",
]


def parse_metrics(stage_b_log: Path) -> dict[str, Any]:
    if not stage_b_log.exists():
        raise SystemExit(f"Stage B log not found: {stage_b_log}")
    latest: dict[str, Any] | None = None
    for raw in stage_b_log.read_text(encoding="utf-8", errors="replace").splitlines():
        if PREFIX not in raw:
            continue
        payload = raw.split(PREFIX, 1)[1].strip()
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Malformed Stage B metrics JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise SystemExit("Stage B metrics JSON must be an object")
        latest = parsed
    if latest is None:
        raise SystemExit(f"Missing {PREFIX} line in {stage_b_log}")
    missing = [key for key in REQUIRED_COUNTERS if key not in latest]
    if missing:
        raise SystemExit(f"Missing Stage B metrics counters: {missing}")
    return latest


def as_int(value: Any, key: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"Stage B metrics counter {key} is not an integer: {value!r}") from exc


def merge(stage_b_log: Path, summary_path: Path) -> dict[str, Any]:
    if not summary_path.exists():
        raise SystemExit(f"Replay summary not found: {summary_path}")
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Malformed replay summary JSON: {exc}") from exc
    if not isinstance(summary, dict):
        raise SystemExit("Replay summary JSON must be an object")

    metrics = parse_metrics(stage_b_log)
    normalized = dict(metrics)
    for key in REQUIRED_COUNTERS:
        normalized[key] = as_int(metrics.get(key), key)

    summary["stageBPerformance"] = normalized
    counts = summary.setdefault("counts", {})
    if not isinstance(counts, dict):
        raise SystemExit("summary.counts must be an object")
    for key in REQUIRED_COUNTERS:
        counts[key] = normalized[key]

    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge Stage B metrics into HPSF replay summary")
    parser.add_argument("--stage-b-log", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args(argv)
    merge(args.stage_b_log, args.summary)
    print(f"Stage B metrics merged into {args.summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
