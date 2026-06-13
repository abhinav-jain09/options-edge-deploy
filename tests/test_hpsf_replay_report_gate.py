from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT_SCRIPT = ROOT / "scripts" / "hpsf" / "generate-hpsf-replay-report-20260612.py"
RUN_SCRIPT = ROOT / "scripts" / "hpsf" / "run-hpsf-replay-20260612.sh"
JENKINSFILE = ROOT / "Jenkinsfile.hpsf-replay-gate"


class HpsfReplayReportGateTest(unittest.TestCase):
    def test_ReplayReportIncludesEsReferenceTest(self) -> None:
        report = generate_report(evidence())

        self.assertIn("resolvedRawSymbol: ESM6", report)
        self.assertIn("resolvedInstrumentId: 42140864", report)

    def test_ReplayReportIncludesEsm6Esu6ComparisonTest(self) -> None:
        report = generate_report(evidence())

        self.assertIn("ESM6: trade count=2 total size=10", report)
        self.assertIn("ESU6: trade count=1 total size=4", report)

    def test_ReplayReportIncludesStageAStageBCountsTest(self) -> None:
        report = generate_report(evidence())

        self.assertIn("strikeFlowRecordsEmitted: 2", report)
        self.assertIn("signalRecordsEmitted: 1", report)
        self.assertIn("Stage A startup log evidence", report)
        self.assertIn("Stage B startup log evidence", report)

    def test_ReplayReportFailsWhenNoSignalTest(self) -> None:
        data = evidence()
        data["counts"]["signalRecordsEmitted"] = 0
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("signal topic is empty", report)
        self.assertIn("Final PASS/FAIL: FAIL", report)

    def test_ReplayReportFailsWhenOrderInstructionEnabledTest(self) -> None:
        data = evidence()
        data["orderInstructionEnabledTrueFound"] = True
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("orderInstruction.enabled=true appeared", report)
        self.assertIn("orderInstruction.enabled=true ever appeared: YES", report)

    def test_JenkinsReplayScriptCreatesTopicsTest(self) -> None:
        subprocess.run(["bash", "-n", str(RUN_SCRIPT)], check=True)
        text = RUN_SCRIPT.read_text()

        self.assertIn("create-replay-topics-20260612.sh", text)
        self.assertIn("generate-hpsf-replay-report-20260612.py", text)

    def test_JenkinsReplayScriptFailsNoEsTradesTest(self) -> None:
        data = evidence()
        data["counts"]["esTradesRead"] = 0
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("ES trades read = 0", report)

    def test_JenkinsReplayScriptFailsNoStageBSignalTest(self) -> None:
        data = evidence()
        data["stageB"]["started"] = False
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Stage B startup evidence missing", report)

    def test_JenkinsReplayScriptArchivesReportTest(self) -> None:
        pipeline = JENKINSFILE.read_text()

        self.assertIn("archiveArtifacts", pipeline)
        self.assertIn("hpsf-replay-report-20260612.md", pipeline)
        for stage in [
            "Checkout repos",
            "Build contracts",
            "Build Databento feed replay tooling",
            "Build HPSF processing",
            "Create replay topics",
            "Download/read Databento historical data",
            "Publish replay OPRA ES SPX records",
            "Run Stage A replay",
            "Run underlying replay",
            "Run Stage B replay",
            "Consume replay latest-signal and audit",
            "Generate replay report",
            "Comment Bugzilla with replay result",
        ]:
            self.assertIn(stage, pipeline)

    def test_run_script_fixture_mode_generates_pass_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "hpsf-replay-report-20260612.md"
            result = subprocess.run(
                [str(RUN_SCRIPT), "--dry-run", "--fixture-mode"],
                cwd=ROOT,
                env={"PATH": "/bin:/usr/bin:/usr/local/bin", "HPSF_REPLAY_REPORT": str(report)},
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, result.returncode, result.stderr + result.stdout)
            self.assertIn("Final PASS/FAIL: PASS", report.read_text())


def generate_report(data: dict) -> str:
    result, report = generate_report_result(data)
    if result.returncode != 0:
        raise AssertionError(result.stderr + report)
    return report


def generate_report_result(data: dict):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "evidence.json"
        report = Path(tmp) / "report.md"
        path.write_text(json.dumps(data), encoding="utf-8")
        result = subprocess.run([str(REPORT_SCRIPT), "--input", str(path), "--output", str(report)], text=True, capture_output=True)
        return result, report.read_text(encoding="utf-8") if report.exists() else result.stderr


def evidence() -> dict:
    return {
        "counts": {
            "esTradesRead": 2,
            "esTradesPublished": 2,
            "esTotalSize": 10,
            "esFirstEventTime": "2026-06-12T14:31:04.000Z",
            "esLastEventTime": "2026-06-12T14:31:05.000Z",
            "esVwapFirst": 6030.25,
            "esVwapLast": 6036.10,
            "opraTcbboRecordsRead": 2,
            "opraTcbboRecordsNormalized": 2,
            "unknownInstrumentCount": 0,
            "dlqCount": 0,
            "spxSpotRecordsProduced": 1,
            "strikeFlowRecordsEmitted": 2,
            "marketFlowRecordsEmitted": 1,
            "strikeScoreRecordsEmitted": 1,
            "signalRecordsEmitted": 1,
            "latestSignalRecordsEmitted": 1,
            "auditRecordsEmitted": 1,
        },
        "esSelection": {
            "referenceMode": "PINNED_RAW_CONTRACT",
            "requestedSymbol": "ESM6",
            "requestedStypeIn": "raw_symbol",
            "selectedSymbol": "ESM6",
            "selectedStypeIn": "raw_symbol",
            "resolvedRawSymbol": "ESM6",
            "resolvedInstrumentId": "42140864",
            "selectionReason": "PINNED_RAW_CONTRACT selected ESM6",
            "resolution": {"resolvedIntervals": [{"rawSymbol": "ESM6", "instrumentId": "42140864", "startTime": "2026-06-12T13:30:00Z", "endTime": "2026-06-12T20:00:00Z", "source": "DATABENTO_SYMBOLOGY"}]},
            "candidates": [
                {"symbol": "ESM6", "selected": True, "reason": "PINNED_RAW_CONTRACT selected ESM6", "stats": {"tradeCount": 2, "totalSize": 10}},
                {"symbol": "ESU6", "selected": False, "reason": "NEXT_CONTRACT_VOLUME_COMPARISON", "stats": {"tradeCount": 1, "totalSize": 4}},
            ],
        },
        "stageA": {"started": True, "startupLog": "HPSF Stage A topology enabled"},
        "stageB": {"started": True, "startupLog": "HPSF Stage B topology enabled"},
        "topicConfigs": {"options.replay.20260612.hpsf.signal": "cleanup.policy=delete"},
        "samples": {
            "signal": {"action": "NO_TRADE", "orderInstruction": {"enabled": False}},
            "latestSignal": {"orderInstruction": {"enabled": False}},
            "audit": {"selectedAction": "NO_TRADE"},
        },
        "actionCounts": {"NO_TRADE": 1},
        "gateReasonCounts": {"MARKET_SCORE_BELOW_THRESHOLD": 1},
        "topExecutionStrikes": ["6005 CALL count=1"],
        "topFlowAnchors": ["6005 CALL count=1"],
        "orderInstructionEnabledTrueFound": False,
    }


if __name__ == "__main__":
    unittest.main()
