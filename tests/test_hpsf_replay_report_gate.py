from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT_SCRIPT = ROOT / "scripts" / "hpsf" / "generate-hpsf-replay-report-20260612.py"
RUN_SCRIPT = ROOT / "scripts" / "hpsf" / "run-hpsf-replay-20260612.sh"
JENKINSFILE = ROOT / "Jenkinsfile.hpsf-replay-gate"
VALIDATE_SPEC = ROOT / "scripts" / "hpsf" / "validate-hpsf-spec-compliance.sh"
DOWNLOAD_SCRIPT = ROOT / "scripts" / "hpsf" / "download-databento-history.sh"
PUBLISH_SCRIPT = ROOT / "scripts" / "hpsf" / "publish-historical-replay.sh"
VALIDATE_OUTPUT_SCRIPT = ROOT / "scripts" / "hpsf" / "validate-replay-output.sh"
GENERATE_REPORT_SCRIPT = ROOT / "scripts" / "hpsf" / "generate-replay-report.sh"


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
        self.assertIn("underlyingStateRecordsEmitted: 1", report)
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

    def test_ReplayReportFailsWhenFixtureEvidenceTest(self) -> None:
        data = evidence()
        data["evidenceMode"] = "FIXTURE"
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("FIXTURE evidence cannot close HPSF-82A/HPSF-83", report)

    def test_ReplayReportFailsWhenUnderlyingStateMissingTest(self) -> None:
        data = evidence()
        data["counts"]["underlyingStateRecordsEmitted"] = 0
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("underlying-state topic is empty", report)

    def test_ReplayReportFailsWhenKeysInvalidTest(self) -> None:
        data = evidence()
        data["keyValidation"]["latestSignalKeyValid"] = False
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("latest-signal key validation failed", report)

    def test_JenkinsReplayScriptCreatesTopicsTest(self) -> None:
        for script in [RUN_SCRIPT, VALIDATE_SPEC, DOWNLOAD_SCRIPT, PUBLISH_SCRIPT, VALIDATE_OUTPUT_SCRIPT, GENERATE_REPORT_SCRIPT]:
            subprocess.run(["bash", "-n", str(script)], check=True)
        text = RUN_SCRIPT.read_text()

        self.assertIn("create-replay-topics-20260612.sh", text)
        self.assertIn("generate-replay-report.sh", text)

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
        self.assertIn("artifacts/hpsf-replay-report-20260612.md", pipeline)
        self.assertIn("allowEmptyArchive: false", pipeline)
        self.assertIn("rm -f artifacts/hpsf-replay-report-20260612.md", pipeline)
        self.assertIn("HPSF_REPLAY_FAILURE_REASON", pipeline)
        self.assertIn("JENKINS_PUBLIC_URL", pipeline)
        self.assertIn("sed 's#/#/job/#g'", pipeline)
        self.assertIn("command -v python3.11", pipeline)
        self.assertIn("\"$FEED_PYTHON\" -m venv .venv", pipeline)
        self.assertIn(".venv/bin/python -m pip install -e .", pipeline)
        self.assertIn('export DATABENTO_FEED_REPO="$PWD/.replay/options-edge-databento-feed"', pipeline)
        self.assertIn('export DATABENTO_FEED_PYTHON="$PWD/.replay/options-edge-databento-feed/.venv/bin/python"', pipeline)
        self.assertIn("export HPSF_REPLAY_RESET_TOPICS=true", pipeline)
        self.assertIn("options-edge-hpsf-stage-a-replay-20260612-${BUILD_NUMBER:-manual}", pipeline)
        self.assertIn("options-edge-hpsf-underlying-replay-20260612-${BUILD_NUMBER:-manual}", pipeline)
        self.assertIn("options-edge-hpsf-stage-b-replay-20260612-${BUILD_NUMBER:-manual}", pipeline)
        for stage in [
            "Checkout",
            "Checkout repos",
            "Build",
            "Static safety check",
            "Create replay topics",
            "Resolve ES reference",
            "Download / load historical Databento data",
            "Publish replay input records",
            "Run Stage A replay",
            "Validate Stage A replay",
            "Run underlying replay processor",
            "Run Stage B replay",
            "Validate replay outputs",
            "Generate replay report",
            "Archive artifacts",
            "Bugzilla update",
        ]:
            self.assertIn(stage, pipeline)

    def test_download_script_prepares_missing_databento_jsonl(self) -> None:
        text = DOWNLOAD_SCRIPT.read_text()

        self.assertIn("Preparing Databento replay JSONL files", text)
        self.assertIn("--prepare-jsonl-only", text)
        self.assertIn("--download-dir \"$SOURCE_DIR\"", text)
        self.assertIn("DATABENTO_FEED_REPO", text)
        self.assertIn("DATABENTO_FEED_PYTHON", text)
        self.assertIn("import databento", text)
        self.assertIn("Missing or empty replay file after Databento preparation", text)

    def test_publish_script_uses_feed_python_with_kafka_dependency(self) -> None:
        text = PUBLISH_SCRIPT.read_text()

        self.assertIn("DATABENTO_FEED_PYTHON", text)
        self.assertIn("import confluent_kafka", text)
        self.assertIn('\"$DATABENTO_FEED_PYTHON\" -m options_edge_databento_feed.hpsf_replay_cli', text)

    def test_validate_stage_a_ignores_kafka_cli_warning_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            consumer = fake_bin / "kafka-console-consumer"
            consumer.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
topic=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --topic) topic="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [[ "$topic" == *".hpsf.strike-flow" ]]; then
  printf '2026-06-12|2026-06-12|6000|CALL\\t{"eventId":"strike-flow-1"}\\n'
elif [[ "$topic" == *".hpsf.signal" ]]; then
  printf 'Option --property is deprecated and will be removed in a future version. Use --formatter-property instead.\\n'
fi
""",
                encoding="utf-8",
            )
            consumer.chmod(0o755)
            build_dir = root / "build"
            artifact_dir = root / "artifacts"

            result = subprocess.run(
                [str(VALIDATE_OUTPUT_SCRIPT), "--stage-a-only"],
                cwd=ROOT,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "HPSF_REPLAY_BUILD_DIR": str(build_dir),
                    "HPSF_REPLAY_ARTIFACT_DIR": str(artifact_dir),
                    "TOPIC_PREFIX": "options.replay.20260612",
                },
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, result.returncode, result.stderr + result.stdout)
            self.assertEqual("0", (build_dir / "stage-a-signal-count.txt").read_text(encoding="utf-8").strip())
            validate_script = VALIDATE_OUTPUT_SCRIPT.read_text(encoding="utf-8")
            self.assertIn("--formatter-property print.key=true", validate_script)
            self.assertNotIn("--property print.key=true", validate_script)

    def test_run_script_fixture_mode_fails_closed_with_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "hpsf-replay-report-20260612.md"
            artifacts = Path(tmp) / "artifacts"
            result = subprocess.run(
                [str(RUN_SCRIPT), "--dry-run", "--fixture-mode"],
                cwd=ROOT,
                env={
                    "PATH": "/bin:/usr/bin:/usr/local/bin",
                    "HPSF_REPLAY_REPORT": str(report),
                    "HPSF_REPLAY_ARTIFACT_DIR": str(artifacts),
                },
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("Final PASS/FAIL: FAIL", report.read_text())
            self.assertIn("Dry-run/fixture replay is not valid", report.read_text())
            self.assertIn("FIXTURE_DRY_RUN evidence cannot close HPSF-82A/HPSF-83", report.read_text())

    def test_run_script_custom_failure_reason_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "hpsf-replay-report-20260612.md"
            artifacts = Path(tmp) / "artifacts"
            result = subprocess.run(
                [str(RUN_SCRIPT), "--dry-run"],
                cwd=ROOT,
                env={
                    "PATH": "/bin:/usr/bin:/usr/local/bin",
                    "HPSF_REPLAY_REPORT": str(report),
                    "HPSF_REPLAY_ARTIFACT_DIR": str(artifacts),
                    "HPSF_REPLAY_FAILURE_REASON": "Missing Jenkins credential options-edge-databento-api-key",
                    "BUILD_URL": "http://jenkins.example/job/hpsf-historical-replay-20260612/9/",
                    "BUILD_NUMBER": "9",
                    "CODE_GIT_SHA": "deadbeef",
                    "JOB_NAME": "hpsf-historical-replay-20260612",
                },
                text=True,
                capture_output=True,
            )

            text = report.read_text()
            self.assertNotEqual(0, result.returncode)
            self.assertIn("Missing Jenkins credential options-edge-databento-api-key", text)
            self.assertIn("Build number: 9", text)
            self.assertIn("Commit SHA: deadbeef", text)

    def test_run_script_uses_jenkins_url_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "hpsf-replay-report-20260612.md"
            artifacts = Path(tmp) / "artifacts"
            result = subprocess.run(
                [str(RUN_SCRIPT), "--dry-run"],
                cwd=ROOT,
                env={
                    "PATH": "/bin:/usr/bin:/usr/local/bin",
                    "HPSF_REPLAY_REPORT": str(report),
                    "HPSF_REPLAY_ARTIFACT_DIR": str(artifacts),
                    "JENKINS_PUBLIC_URL": "http://jenkins.example",
                    "JOB_NAME": "folder/hpsf-historical-replay-20260612",
                    "BUILD_NUMBER": "10",
                    "CODE_GIT_SHA": "feedface",
                },
                text=True,
                capture_output=True,
            )

            text = report.read_text()
            self.assertNotEqual(0, result.returncode)
            self.assertIn("Build URL: http://jenkins.example/job/folder/job/hpsf-historical-replay-20260612/10/", text)
            self.assertIn("Build number: 10", text)


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
        "evidenceMode": "REAL",
        "jenkins": {
            "buildUrl": "http://jenkins.example/job/hpsf-historical-replay-20260612/1/",
            "buildNumber": "1",
            "commitSha": "abc123",
            "jobName": "hpsf-historical-replay-20260612",
        },
        "replay": {
            "date": "2026-06-12",
            "start": "2026-06-12T13:30:00Z",
            "end": "2026-06-12T20:00:00Z",
            "topicPrefix": "options.replay.20260612",
            "spxSpotSource": "ES_BASIS_PROXY",
        },
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
            "underlyingStateRecordsEmitted": 1,
            "marketFlowRecordsEmitted": 1,
            "strikeScoreRecordsEmitted": 1,
            "signalRecordsEmitted": 1,
            "latestSignalRecordsEmitted": 1,
            "auditRecordsEmitted": 1,
            "stageAEmittedFinalSignalCount": 0,
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
        "keyValidation": {"signalKeyValid": True, "latestSignalKeyValid": True, "auditKeyValid": True},
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
