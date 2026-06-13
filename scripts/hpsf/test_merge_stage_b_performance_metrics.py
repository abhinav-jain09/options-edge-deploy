#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "hpsf" / "merge-stage-b-performance-metrics.py"
spec = importlib.util.spec_from_file_location("merge_stage_b_performance_metrics", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


class MergeStageBPerformanceMetricsTest(unittest.TestCase):
    def test_merge_adds_stage_b_counters_to_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            log = root / "stage-b.log"
            summary = root / "hpsf-replay-summary.json"
            log.write_text(
                'noise\nHPSF_STAGE_B_PERFORMANCE_METRICS_JSON={"stageB.evaluation.count":3,"underlyingStateRecordsReceived":10,"underlyingStateRecordsStored":9,"underlyingStateLookupHitCount":8,"underlyingStateLookupMissCount":1,"underlyingStateNullVwapCount":0,"underlyingStateNullDistanceCount":0,"healthyUnderlyingStateCount":9}\n',
                encoding="utf-8",
            )
            summary.write_text(json.dumps({"counts": {"signalRecordsEmitted": 1}}), encoding="utf-8")

            module.merge(log, summary)

            data = json.loads(summary.read_text(encoding="utf-8"))
            self.assertEqual(10, data["stageBPerformance"]["underlyingStateRecordsReceived"])
            self.assertEqual(8, data["stageBPerformance"]["underlyingStateLookupHitCount"])
            self.assertEqual(9, data["counts"]["healthyUnderlyingStateCount"])

    def test_missing_metrics_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            log = root / "stage-b.log"
            summary = root / "hpsf-replay-summary.json"
            log.write_text("no metrics here\n", encoding="utf-8")
            summary.write_text(json.dumps({"counts": {}}), encoding="utf-8")

            with self.assertRaises(SystemExit):
                module.merge(log, summary)

    def test_malformed_metrics_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            log = root / "stage-b.log"
            summary = root / "hpsf-replay-summary.json"
            log.write_text("HPSF_STAGE_B_PERFORMANCE_METRICS_JSON={not-json}\n", encoding="utf-8")
            summary.write_text(json.dumps({"counts": {}}), encoding="utf-8")

            with self.assertRaises(SystemExit):
                module.merge(log, summary)


if __name__ == "__main__":
    unittest.main()
