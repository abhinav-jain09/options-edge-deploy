from __future__ import annotations

import json
import os
import re
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
MERGE_STAGE_B_METRICS_SCRIPT = ROOT / "scripts" / "hpsf" / "merge-stage-b-performance-metrics.py"
MERGE_STAGE_B_METRICS_TEST = ROOT / "scripts" / "hpsf" / "test_merge_stage_b_performance_metrics.py"
ARCHIVE_EVIDENCE_SCRIPT = ROOT / "scripts" / "hpsf" / "archive-replay-evidence-report.sh"
WAIT_GROUP_CATCHUP_SCRIPT = ROOT / "scripts" / "hpsf" / "wait-kafka-consumer-group-caught-up.sh"


class HpsfReplayReportGateTest(unittest.TestCase):
    def test_ReplayReportIncludesEsReferenceTest(self) -> None:
        report = generate_report(evidence())

        self.assertIn("resolvedRawSymbol: ESM6", report)
        self.assertIn("resolvedInstrumentId: 42140864", report)

    def test_ReplayReportIncludesEsm6Esu6ComparisonTest(self) -> None:
        report = generate_report(evidence())

        self.assertIn("ESM6: trade count=2 total size=10", report)
        self.assertIn("ESU6: trade count=1 total size=4", report)

    def test_ReplayReportFailsCopiedEsm6Esu6ComparisonTest(self) -> None:
        data = evidence()
        copied = {
            "tradeCount": 2,
            "totalSize": 10,
            "firstEventTime": "2026-06-12T14:31:04Z",
            "lastEventTime": "2026-06-12T14:31:05Z",
        }
        data["esSelection"]["candidates"][0]["stats"] = dict(copied)
        data["esSelection"]["candidates"][1]["stats"] = dict(copied)
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("ESM6/ESU6 candidate stats look copied", report)

    def test_ReplayReportDoesNotFailPassValidationForEsLineageDiagnosticsTest(self) -> None:
        data = evidence()
        data["validationResult"] = "PASS"
        data["validationFailures"] = []
        copied = {
            "tradeCount": 428698,
            "totalSize": 898207,
            "firstEventTime": "",
            "lastEventTime": "",
        }
        data["esSelection"]["candidates"][0]["stats"] = dict(copied)
        data["esSelection"]["candidates"][1]["stats"] = dict(copied)
        for key in ["esFirstEventTime", "esLastEventTime", "esVwapFirst", "esVwapLast"]:
            data["counts"][key] = ""

        result, report = generate_report_result(data)

        self.assertEqual(0, result.returncode)
        self.assertIn("Final PASS/FAIL: PASS", report)
        self.assertIn("Report diagnostics:", report)
        self.assertIn("ESM6/ESU6 candidate stats look copied", report)
        self.assertIn("esFirstEventTime missing", report)

    def test_ReplayReportAllowsUnavailableEsu6ComparisonTest(self) -> None:
        data = evidence()
        data["esSelection"]["candidates"][1] = {
            "symbol": "ESU6",
            "comparisonStatus": "NOT_AVAILABLE",
            "reason": "ESU6 candidate stats were not fetched",
        }
        report = generate_report(data)

        self.assertIn("ESU6: comparisonStatus=NOT_AVAILABLE", report)

    def test_ReplayReportIncludesStageAStageBCountsTest(self) -> None:
        report = generate_report(evidence())

        self.assertIn("strikeFlowRecordsEmitted: 2", report)
        self.assertIn("underlyingStateRecordsEmitted: 1", report)
        self.assertIn("signalRecordsEmitted: 1", report)
        self.assertIn("Stage A startup log evidence", report)
        self.assertIn("Stage B startup log evidence", report)

    def test_ReplayReportFailsEmptyActionCountsWithSignalsTest(self) -> None:
        data = evidence()
        data["actionCounts"] = {}
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("actionCounts empty while signal records exist", report)

    def test_ReplayReportFailsAllNoTradeDataHealthOnlyTest(self) -> None:
        data = evidence()
        data["actionCounts"] = {"NO_TRADE": 1}
        data["gateReasonCounts"] = {"SPX_SPOT_STALE": 1}
        data["samples"]["signal"]["reasons"] = ["SPX_SPOT_STALE"]
        data["samples"]["audit"]["noTradeGates"] = ["SPX_SPOT_STALE"]
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("all NO_TRADE outputs are explained only by data-health gates", report)

    def test_ReplayReportFailsFallbackOnlyNoTradeTest(self) -> None:
        data = evidence()
        data["actionCounts"] = {"NO_TRADE": 1}
        data["gateReasonCounts"] = {
            "MARKET_EVALUATOR_NOT_READY": 1,
            "UNDERLYING_STATE_UNAVAILABLE": 1,
            "VWAP unavailable": 1,
        }
        data["samples"]["signal"]["internalVwapState"] = "VWAP_UNAVAILABLE"
        data["samples"]["signal"]["reasons"] = [
            "MARKET_EVALUATOR_NOT_READY",
            "UNDERLYING_STATE_UNAVAILABLE",
            "VWAP unavailable",
        ]
        data["samples"]["audit"]["noTradeGates"] = [
            "MARKET_EVALUATOR_NOT_READY",
            "UNDERLYING_STATE_UNAVAILABLE",
            "VWAP unavailable",
        ]

        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("all NO_TRADE outputs are fallback because underlying/VWAP context is unavailable", report)

    def test_ReplayReportAllowsAllNoTradeWhenAggregateAuditHasStrategyGateTest(self) -> None:
        data = evidence()
        data["actionCounts"] = {"NO_TRADE": 1}
        data["gateReasonCounts"] = {
            "ATM_STRIKE_MISSING: atmStrike=7400.0": 1,
            "NO_LIQUID_EXECUTION_CANDIDATE": 1,
            "MIXED_FLOW": 1,
        }
        data["samples"]["signal"]["reasons"] = [
            "ATM_STRIKE_MISSING: atmStrike=7400.0",
            "NO_LIQUID_EXECUTION_CANDIDATE",
        ]
        data["samples"]["audit"]["noTradeGates"] = [
            "ATM_STRIKE_MISSING: atmStrike=7400.0",
            "NO_LIQUID_EXECUTION_CANDIDATE",
        ]

        result, report = generate_report_result(data)

        self.assertEqual(0, result.returncode)
        self.assertIn("Final PASS/FAIL: PASS", report)
        self.assertNotIn("all NO_TRADE outputs are explained only by data-health gates", report)

    def test_ReplayReportFailsSameDayExpiryMismatchTest(self) -> None:
        data = evidence()
        data["replay"]["date"] = "2026-06-11"
        for sample in data["samples"].values():
            sample["tradeDate"] = "2026-06-11"
            sample["expiry"] = "2026-06-12"
        data["gateReasonCounts"] = {"NO_VALID_TODAY_EXPIRY: tradeDate=2026-06-11 expiry=2026-06-12": 1}

        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("SAME_DAY_EXPIRY_MISMATCH", report)
        self.assertIn("NO_VALID_TODAY_EXPIRY observed in replay outputs", report)

    def test_ReplayReportPopulatesEsLineageFromSelectedStatsTest(self) -> None:
        data = evidence()
        for key in ["esFirstEventTime", "esLastEventTime", "esVwapFirst", "esVwapLast"]:
            data["counts"].pop(key)
        data["esSelection"]["selectedStats"] = {
            "firstEventTime": "2026-06-12T14:31:04Z",
            "lastEventTime": "2026-06-12T14:31:05Z",
            "esVwapFirst": 6030.25,
            "esVwapLast": 6036.10,
        }
        report = generate_report(data)

        self.assertIn("esFirstEventTime: 2026-06-12T14:31:04Z", report)
        self.assertIn("esVwapLast: 6036.1", report)

    def test_ReplayReportIncludesStageBUnderlyingHealthTest(self) -> None:
        report = generate_report(evidence())

        self.assertIn("## Stage B Scheduled Evaluation Health", report)
        self.assertIn("stageB.punctuation.fire.count: 3", report)
        self.assertIn("activeChainsEvaluatedCount: 3", report)
        self.assertIn("## Stage B Underlying-State Health", report)
        self.assertIn("underlyingStateRecordsReceived: 10", report)
        self.assertIn("underlyingStateLookupHitCount: 8", report)
        self.assertIn("Stage B underlying join healthy: true", report)
        self.assertIn("Stage B VWAP usable: true", report)

    def test_ReplayReportFailsSynchronizedCatchupAuditWithoutAllowedMetricTest(self) -> None:
        data = evidence()
        data["samples"]["audit"]["gateDiagnostics"] = {
            "evaluationTime": "2026-06-12T14:31:17Z",
            "stageBEvaluationTrigger": "SYNCHRONIZED_CATCHUP",
            "feedSynchronized": True,
        }
        data["stageBPerformance"]["stageB.feedSync.catchup.allowed.count"] = 0
        data["stageBPerformance"]["stageB.feedSync.catchup.skipped.count"] = 4

        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("STAGE_B_FEED_SYNC_METRIC_AUDIT_MISMATCH", report)
        self.assertIn("stageB.feedSync.catchup.allowed.count is 0", report)
        self.assertIn("metrics may be stale or from a different pod/instance", report)

    def test_ReplayReportFailsWhenNoSignalTest(self) -> None:
        data = evidence()
        data["counts"]["signalRecordsEmitted"] = 0
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("signal topic is empty", report)
        self.assertIn("## Failure Detail", report)
        self.assertIn("Final PASS/FAIL: FAIL", report)

    def test_ReplayReportIncludesRootCauseForStageARebalancingTest(self) -> None:
        data = evidence()
        data["evidenceMode"] = "DRY_RUN"
        data["counts"]["strikeFlowRecordsEmitted"] = 0
        data["counts"]["signalRecordsEmitted"] = 0
        data["counts"]["latestSignalRecordsEmitted"] = 0
        data["counts"]["auditRecordsEmitted"] = 0
        data["publishSummary"] = {
            "publish": {
                "dryRun": False,
                "topicCounts": {
                    "options.replay.20260612.opra.tcbbo": 282265,
                },
            },
        }
        data["rootCauseDiagnostics"] = {
            "services": {
                "stageA": {
                    "streamState": "REBALANCING",
                    "logTail": "HPSF stage-a stream state transition CREATED -> REBALANCING",
                },
            },
            "kafka": {
                "stageAConsumerGroup": {"exists": False},
                "stageAInternalTopics": {"count": 0, "topics": []},
            },
            "logging": {"slf4jNoopDetected": True},
        }
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("## Root Cause Analysis", report)
        self.assertIn("Stage A Kafka Streams did not become operational", report)
        self.assertIn("Replay input health: OPRA records=282265", report)
        self.assertIn("Stage A stream state: REBALANCING", report)
        self.assertIn("Stage A replay consumer group exists: false", report)
        self.assertIn("Evidence-mode note: report metadata says DRY_RUN", report)

    def test_ReplayReportIncludesPreviousBuildComparisonTest(self) -> None:
        previous = evidence()
        previous["jenkins"]["buildNumber"] = "22"
        previous["validationResult"] = "FAIL"
        previous["validationFailures"] = [{"code": "REPLAY_VALIDATION_FAILED", "detail": "ALL_NO_TRADE_DATA_FAILURE"}]
        previous["counts"]["dlqCount"] = 5
        previous["counts"]["underlyingStateLookupHitCount"] = 1
        current = evidence()
        current["jenkins"]["buildNumber"] = "23"
        current["validationResult"] = "PASS"
        current["counts"]["underlyingStateLookupHitCount"] = 8

        report = generate_report(current, previous)

        self.assertIn("## Previous Build Comparison", report)
        self.assertIn("Previous build: 22", report)
        self.assertIn("Current build: 23", report)
        self.assertIn("validationResult: current=PASS previous=FAIL", report)
        self.assertIn("validation failures reduced from 1 to 0", report)
        self.assertIn("underlying-state lookup hits increased", report)

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

    def test_ReplayReportFailsWhenStageBUnderlyingLookupHitZeroTest(self) -> None:
        data = evidence()
        data["stageBPerformance"]["underlyingStateLookupHitCount"] = 0
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Stage B underlying join unhealthy", report)
        self.assertIn("underlyingStateLookupHitCount: 0", report)

    def test_ReplayReportFailsWhenKeysInvalidTest(self) -> None:
        data = evidence()
        data["keyValidation"]["latestSignalKeyValid"] = False
        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("latest-signal key validation failed", report)

    def test_JenkinsReplayScriptCreatesTopicsTest(self) -> None:
        for script in [RUN_SCRIPT, VALIDATE_SPEC, DOWNLOAD_SCRIPT, PUBLISH_SCRIPT, VALIDATE_OUTPUT_SCRIPT, GENERATE_REPORT_SCRIPT, WAIT_GROUP_CATCHUP_SCRIPT]:
            subprocess.run(["bash", "-n", str(script)], check=True)
        subprocess.run(["python3", str(MERGE_STAGE_B_METRICS_TEST)], check=True)
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
        self.assertIn("Create replay evidence report", pipeline)
        self.assertIn("scripts/hpsf/archive-replay-evidence-report.sh", pipeline)
        self.assertIn("scripts/hpsf/collect-replay-root-cause-diagnostics.sh || true", pipeline)
        self.assertIn("HPSF_REPLAY_RUN_NAME", pipeline)
        self.assertIn("hpsf-historical-replay-${replayDateId}", pipeline)
        self.assertIn("artifacts/hpsf-replay-report-${replayDateId}.md", pipeline)
        self.assertIn(".replay/options-edge/evidence-report/${env.HPSF_REPLAY_RUN_NAME}/**/*.md", pipeline)
        self.assertIn("$HPSF_REPLAY_REPORT", pipeline)
        self.assertIn("allowEmptyArchive: false", pipeline)
        archive_lines = re.findall(r"""archiveArtifacts artifacts: ["']([^"']+)["']""", pipeline)
        evidence_archive_lines = [
            archived
            for archived in archive_lines
            if archived == ".replay/options-edge/evidence-report/${env.HPSF_REPLAY_RUN_NAME}/**/*.md"
        ]
        self.assertEqual(
            [
                ".replay/options-edge/evidence-report/${env.HPSF_REPLAY_RUN_NAME}/**/*.md",
                ".replay/options-edge/evidence-report/${env.HPSF_REPLAY_RUN_NAME}/**/*.md",
            ],
            evidence_archive_lines,
        )
        for archived in evidence_archive_lines:
            self.assertNotIn("*.json", archived)
            self.assertNotIn("artifacts/logs", archived)
            self.assertNotIn("artifacts/hpsf-replay-report-20260612.md", archived)
        self.assertIn("Run HPSF black-box replay validation", pipeline)
        self.assertIn(".replay/options-edge-processing/scripts/hpsf/run_blackbox_replay_validation.sh", pipeline)
        self.assertIn("scripts/hpsf/run_blackbox_replay_validation.sh", pipeline)
        self.assertIn("ARTIFACTS_DIR=\"$workspace/artifacts\"", pipeline)
        self.assertIn("HPSF_REPLAY_VALIDATION_PROFILE=FULL_RTH_RELEASE", pipeline)
        self.assertIn("HPSF_0DTE_DATA_QUALITY_REQUIRED=true", pipeline)
        self.assertIn("HPSF_CHAIN_COVERAGE_DRILL_REQUIRED=true", pipeline)
        self.assertIn("HPSF_CASE2_DIAGNOSTIC_REQUIRED=false", pipeline)
        self.assertIn("hpsf-blackbox-replay-validation.exit-code", pipeline)
        self.assertIn("Replay report was archived, but HPSF black-box replay validation failed", pipeline)
        for artifact in [
            "artifacts/hpsf-0dte-data-quality-validation-result.json",
            "artifacts/hpsf-0dte-data-quality-validation-report.md",
            "artifacts/hpsf-chain-coverage-drill-result.json",
            "artifacts/hpsf-chain-coverage-drill-report.md",
            "artifacts/hpsf-blackbox-validation-result.json",
            "artifacts/hpsf-blackbox-validation-report.md",
        ]:
            self.assertIn(artifact, pipeline)
        self.assertIn("artifacts/case2-diagnostic-validation-result.json", pipeline)
        self.assertIn("artifacts/case2-diagnostic-validation-report.md", pipeline)
        self.assertIn("current_report_matches_build=false", pipeline)
        self.assertIn('grep -F "Build number: ${BUILD_NUMBER:-}" "$HPSF_REPLAY_REPORT"', pipeline)
        self.assertIn('rm -rf artifacts "$HPSF_REPLAY_BUILD_DIR" artifact-manifest.json', pipeline)
        self.assertIn("artifacts/hpsf-stage-a-validation.json", pipeline)
        self.assertIn('"$HPSF_REPLAY_BUILD_DIR/stage-a-validation.json"', pipeline)
        self.assertIn("HPSF_REPLAY_FAILURE_REASON", pipeline)
        self.assertIn('HPSF_REPLAY_FAILURE_CODE="STAGE_A_STRIKE_FLOW_EMPTY"', pipeline)
        self.assertIn("JENKINS_PUBLIC_URL", pipeline)
        self.assertIn("sed 's#/#/job/#g'", pipeline)
        self.assertIn("command -v python3.11", pipeline)
        self.assertIn("\"$FEED_PYTHON\" -m venv .venv", pipeline)
        self.assertIn(".venv/bin/python -m pip install -e .", pipeline)
        self.assertIn('docker build -t "$HPSF_PROCESSING_IMAGE"', pipeline)
        self.assertIn('docker push "$HPSF_PROCESSING_IMAGE"', pipeline)
        self.assertIn("--image-pull-policy=Always", pipeline)
        self.assertIn('HPSF_REPLAY_POD_OVERRIDES = \'{"spec":{"hostNetwork":true,"dnsPolicy":"ClusterFirstWithHostNet"}}\'', pipeline)
        self.assertEqual(3, pipeline.count('--overrides="$HPSF_REPLAY_POD_OVERRIDES"'))
        self.assertEqual(3, pipeline.count("scripts/hpsf/wait-kafka-consumer-group-caught-up.sh"))
        self.assertIn(
            '"options-edge-hpsf-stage-b-replay-${HPSF_REPLAY_DATE_ID}-${BUILD_NUMBER:-manual}"',
            pipeline,
        )
        self.assertIn("artifacts/logs/stage-b-consumer-group-catchup.log", pipeline)
        self.assertEqual(3, pipeline.count("--env=HPSF_MARKET_CALENDAR_ALLOW_INCOMPLETE=true"))
        self.assertEqual(3, pipeline.count("--env=HPSF_MARKET_CALENDAR_MIN_HOLIDAYS_PER_YEAR=0"))
        self.assertEqual(3, pipeline.count("--env=HPSF_MARKET_CALENDAR_REQUIRE_EARLY_CLOSES=false"))
        self.assertEqual(3, pipeline.count("--env=HPSF_STREAMS_PROCESSING_GUARANTEE=at_least_once"))
        self.assertEqual(3, pipeline.count("--env=HPSF_STREAMS_NUM_STREAM_THREADS=1"))
        self.assertEqual(3, pipeline.count("--env=HPSF_STREAMS_CACHE_MAX_BYTES_BUFFERING=0"))
        self.assertEqual(3, pipeline.count("--env=HPSF_STREAMS_CONSUMER_ISOLATION_LEVEL=read_uncommitted"))
        self.assertIn('export DATABENTO_FEED_REPO="$PWD/.replay/options-edge-databento-feed"', pipeline)
        self.assertIn('export DATABENTO_FEED_PYTHON="$PWD/.replay/options-edge-databento-feed/.venv/bin/python"', pipeline)
        self.assertIn("export HPSF_REPLAY_RESET_TOPICS=true", pipeline)
        self.assertIn('export UNDERLYING_TOPIC_PREFIX="${UNDERLYING_TOPIC_PREFIX:-underlying.replay.${compact_date}}"', pipeline)
        self.assertIn('underlying_topic_prefix="${UNDERLYING_TOPIC_PREFIX:-underlying.replay.${compact_date}}"', pipeline)
        self.assertIn('--env=HPSF_TOPIC_UNDERLYING_ES_TRADES="$underlying_topic_prefix.es.trades"', pipeline)
        self.assertIn('--env=HPSF_TOPIC_UNDERLYING_SPX_PRICE="$underlying_topic_prefix.spx.price"', pipeline)
        self.assertNotIn('--env=HPSF_TOPIC_UNDERLYING_ES_TRADES="underlying.replay.20260612.es.trades"', pipeline)
        self.assertNotIn('--env=HPSF_TOPIC_UNDERLYING_SPX_PRICE="underlying.replay.20260612.spx.price"', pipeline)
        self.assertIn("export HPSF_REPLAY_REFRESH_SOURCE=true", pipeline)
        self.assertIn("artifacts/logs/stage-b-startup.log", pipeline)
        self.assertIn("artifacts/logs/stage-b-runtime.log", pipeline)
        self.assertIn("cat artifacts/logs/stage-b-startup.log artifacts/logs/stage-b-runtime.log > artifacts/logs/stage-b.log", pipeline)
        self.assertIn("python3 scripts/hpsf/test_merge_stage_b_performance_metrics.py", pipeline)
        self.assertIn("Merge Stage B Metrics Into Replay Summary", pipeline)
        self.assertIn("python3 scripts/hpsf/merge-stage-b-performance-metrics.py", pipeline)
        self.assertIn("--stage-b-log artifacts/logs/stage-b.log", pipeline)
        self.assertIn("--summary artifacts/hpsf-replay-summary.json", pipeline)
        self.assertIn("scripts/hpsf/validate-replay-output.sh --final --summary-only", pipeline)
        self.assertIn("Replay validation failed with status", pipeline)
        self.assertIn("scripts/hpsf/validate-replay-output.sh --stage-a-only 2>&1 | tee artifacts/logs/validate-stage-a.log", pipeline)
        self.assertIn("HPSF_REPLAY_REPORT_ALLOW_FAIL=true", pipeline)
        self.assertIn("HPSF_PREVIOUS_REPLAY_SUMMARY", pipeline)
        self.assertIn("replay-validation-result.json", pipeline)
        self.assertIn("Bugzilla PASS comment blocked because validation result is not PASS", pipeline)
        self.assertIn("activeChainsMax=", pipeline)
        self.assertIn("stageB.punctuation.fire.count", pipeline)
        self.assertIn("options-edge-hpsf-stage-a-replay-${HPSF_REPLAY_DATE_ID}-${BUILD_NUMBER:-manual}", pipeline)
        self.assertIn("options-edge-hpsf-underlying-replay-${HPSF_REPLAY_DATE_ID}-${BUILD_NUMBER:-manual}", pipeline)
        self.assertIn("options-edge-hpsf-stage-b-replay-${HPSF_REPLAY_DATE_ID}-${BUILD_NUMBER:-manual}", pipeline)
        self.assertIn("state=stage-a=RUNNING", pipeline)
        self.assertIn("state=underlying-replay=RUNNING", pipeline)
        self.assertIn("state=stage-b=RUNNING", pipeline)
        self.assertNotIn("--for=condition=Ready", pipeline)
        self.assertIn("Replay report was archived, but validation failed", pipeline)
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
            "Collect replay outputs",
            "Merge Stage B Metrics Into Replay Summary",
            "Validate replay outputs",
            "Run HPSF black-box replay validation",
            "Generate replay report",
            "Create replay evidence report",
            "Validate replay report PASS",
            "Archive artifacts",
            "Bugzilla update",
        ]:
            self.assertIn(stage, pipeline)
        self.assertLess(pipeline.index("Run Stage B replay"), pipeline.index("Merge Stage B Metrics Into Replay Summary"))
        self.assertLess(pipeline.index("Merge Stage B Metrics Into Replay Summary"), pipeline.index("Validate replay outputs"))
        self.assertLess(pipeline.index("Validate replay outputs"), pipeline.index("Run HPSF black-box replay validation"))
        self.assertLess(pipeline.index("Run HPSF black-box replay validation"), pipeline.index("Generate replay report"))
        self.assertLess(pipeline.index("Merge Stage B Metrics Into Replay Summary"), pipeline.index("Generate replay report"))
        self.assertLess(pipeline.index("Create replay evidence report"), pipeline.index("Archive artifacts"))
        self.assertLess(pipeline.index("Archive artifacts"), pipeline.index("Validate replay report PASS"))

    def test_JenkinsReplayJobInputsAreDateNeutralTest(self) -> None:
        pipeline = JENKINSFILE.read_text()

        self.assertIn("string(name: 'REPLAY_DATE', defaultValue: ''", pipeline)
        self.assertIn("string(name: 'REPLAY_START', defaultValue: ''", pipeline)
        self.assertIn("string(name: 'REPLAY_END', defaultValue: ''", pipeline)
        self.assertIn("string(name: 'TOPIC_PREFIX', defaultValue: ''", pipeline)
        self.assertIn("string(name: 'HPSF_REPLAY_TICK_BY_TICK', defaultValue: 'false'", pipeline)
        self.assertIn("string(name: 'HPSF_STATE_DIR', defaultValue: ''", pipeline)
        self.assertIn("string(name: 'HPSF_REPLAY_SOURCE_DIR', defaultValue: ''", pipeline)
        self.assertIn('error("REPLAY_DATE is required, for example 2026-06-11")', pipeline)
        self.assertIn('env.REPLAY_START = "${replayDate}T13:30:00Z"', pipeline)
        self.assertIn('env.REPLAY_END = "${replayDate}T20:00:00Z"', pipeline)
        self.assertNotIn("params.TOPIC_PREFIX == 'options.replay.20260612'", pipeline)
        self.assertNotIn("params.HPSF_STATE_DIR == '/home/options-edge/data/kafka-streams/hpsf-replay-20260612'", pipeline)
        self.assertNotIn("params.HPSF_REPLAY_SOURCE_DIR == '/home/options-edge/data/hpsf-replay/20260612'", pipeline)
        self.assertIn('replay_topic_prefix="${TOPIC_PREFIX:-options.replay.${HPSF_REPLAY_DATE_ID:-${REPLAY_DATE//-/}}}"', pipeline)
        self.assertIn('export HPSF_REPLAY_TICK_BY_TICK="${HPSF_REPLAY_TICK_BY_TICK}"', pipeline)
        self.assertNotIn("${TOPIC_PREFIX:-options.replay.20260612}", pipeline)

    def test_PublishReplayScriptPassesTickByTickSamplingFlag(self) -> None:
        script = PUBLISH_SCRIPT.read_text()

        self.assertIn('--tick-by-tick "${HPSF_REPLAY_TICK_BY_TICK:-false}"', script)

    def test_JenkinsReplayGateUsesCredentialIdInsteadOfPasswordParameter(self) -> None:
        pipeline = JENKINSFILE.read_text()

        self.assertIn("string(name: 'BUGZILLA_PASSWORD_CREDENTIAL_ID'", pipeline)
        self.assertIn('withCredentials([string(credentialsId: "${params.BUGZILLA_PASSWORD_CREDENTIAL_ID}"', pipeline)
        self.assertIn('bugzilla_password="${BUGZILLA_PASSWORD_FROM_CREDENTIAL:-}"', pipeline)
        self.assertNotIn("password(name: 'BUGZILLA_PASSWORD'", pipeline)
        self.assertNotIn("${BUGZILLA_PASSWORD:-}", pipeline)

    def test_JenkinsReplayGateUsesReplaySpecificProcessingImageTag(self) -> None:
        pipeline = JENKINSFILE.read_text()

        self.assertIn("string(name: 'HPSF_PROCESSING_IMAGE', defaultValue: ''", pipeline)
        self.assertIn("options-edge-hpsf-processing:replay-${replayDateId}-${env.BUILD_NUMBER ?: 'manual'}", pipeline)
        self.assertIn("def blockedReplayTags = ['dev', 'latest', 'main', 'prod', 'production', 'stable']", pipeline)
        self.assertIn("HPSF_PROCESSING_IMAGE must be replay-specific", pipeline)
        self.assertNotIn("defaultValue: '192.168.100.252:5000/options-edge-hpsf-processing:dev'", pipeline)

    def test_archive_manifest_fallback_job_name_is_date_neutral(self) -> None:
        archiver = (ROOT / "scripts" / "hpsf" / "archive-replay-evidence-report.sh").read_text()

        self.assertIn('os.environ.get("JOB_NAME", "hpsf-historical-replay")', archiver)
        self.assertNotIn('os.environ.get("JOB_NAME", "hpsf-historical-replay-20260612")', archiver)
        self.assertIn('REPLAY_DATE="unknown"', archiver)
        self.assertIn('replay_date = "unknown"', archiver)
        self.assertNotIn('REPLAY_DATE="${REPLAY_DATE:-2026-06-12}"', archiver)
        self.assertNotIn('os.environ.get("REPLAY_DATE", "2026-06-12")', archiver)

    def test_JenkinsReplayGateAllowsBranchSelectedProcessingBuildTest(self) -> None:
        pipeline = JENKINSFILE.read_text()

        self.assertIn("JENKINS_ALLOWED_DEPLOY_BRANCH", pipeline)
        self.assertIn('export JENKINS_ALLOWED_DEPLOY_BRANCH="${JENKINS_ALLOWED_DEPLOY_BRANCH:-main}"', pipeline)
        self.assertIn("PROCESSING_BRANCH", pipeline)
        self.assertIn("HPSF_REPLAY_GROUP_CATCHUP_ATTEMPTS", pipeline)
        self.assertIn('git clone --depth 1 --branch "$branch"', pipeline)
        self.assertIn(
            'clone_branch git@github.com:abhinav-jain09/options-edge-processing.git "${PROCESSING_BRANCH:-main}" .replay/options-edge-processing',
            pipeline,
        )
        self.assertIn("artifacts/hpsf-processing-source-sha.txt", pipeline)
        self.assertIn("artifacts/hpsf-processing-source-branch.txt", pipeline)

    def test_ReplayEvidenceArchiveScriptExistsAndIsExecutable(self) -> None:
        self.assertTrue(ARCHIVE_EVIDENCE_SCRIPT.exists())
        self.assertTrue(os.access(ARCHIVE_EVIDENCE_SCRIPT, os.X_OK))

    def test_ReplayRootCauseDiagnosticScriptExistsAndIsExecutable(self) -> None:
        script = ROOT / "scripts" / "hpsf" / "collect-replay-root-cause-diagnostics.sh"

        self.assertTrue(script.exists())
        self.assertTrue(os.access(script, os.X_OK))
        text = script.read_text(encoding="utf-8")
        self.assertIn('fresh_log="$LOG_DIR/${log_name}.fresh"', text)
        self.assertIn('[[ -s "$fresh_log" || ! -s "$LOG_DIR/${log_name}" ]]', text)
        subprocess.run(["bash", "-n", str(script)], check=True)

    def test_consumer_group_catchup_treats_empty_internal_topics_as_caught_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            consumer_groups = fake_bin / "kafka-consumer-groups"
            consumer_groups.write_text(
                """#!/usr/bin/env bash
cat <<'OUT'
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
group app-hpsf-by-strike-repartition 0 - 0 - consumer host client
group options.replay.20260611.opra.tcbbo 0 - 0 - consumer host client
OUT
""",
                encoding="utf-8",
            )
            consumer_groups.chmod(0o755)
            result = subprocess.run(
                [str(WAIT_GROUP_CATCHUP_SCRIPT), "group", str(root / "catchup.log")],
                cwd=ROOT,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "HPSF_REPLAY_GROUP_CATCHUP_ATTEMPTS": "1",
                },
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, result.returncode, result.stderr + result.stdout)
            self.assertIn("caught up", result.stdout)

    def test_archive_script_defaults_to_options_edge_project_evidence_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            deploy = root / "options-edge-deploy"
            options_edge = root / "options-edge"
            deploy.mkdir()
            options_edge.mkdir()
            (deploy / "scripts" / "hpsf").mkdir(parents=True)
            (deploy / ".replay" / "options-edge").mkdir(parents=True)
            local_script = deploy / "scripts" / "hpsf" / "archive-replay-evidence-report.sh"
            local_script.write_text(ARCHIVE_EVIDENCE_SCRIPT.read_text(encoding="utf-8"), encoding="utf-8")
            local_script.chmod(0o755)
            artifact_dir = deploy / "artifacts"
            artifact_dir.mkdir()
            (artifact_dir / "hpsf-replay-report-20260611.md").write_text("# report\nFinal PASS/FAIL: FAIL\n", encoding="utf-8")
            (artifact_dir / "hpsf-replay-summary.json").write_text(json.dumps({"validationResult": "FAIL"}), encoding="utf-8")
            (artifact_dir / "replay-validation-result.json").write_text(json.dumps({"validationResult": "FAIL"}), encoding="utf-8")

            result = subprocess.run(
                [str(local_script)],
                cwd=deploy,
                env={
                    **os.environ,
                    "BUILD_NUMBER": "manual-test",
                    "REPLAY_DATE": "2026-06-11",
                    "HPSF_REPLAY_ARTIFACT_DIR": "artifacts",
                },
                text=True,
                capture_output=True,
            )

            project_folder = options_edge / "evidence-report" / "hpsf-historical-replay-20260611" / "manual-test"
            jenkins_archive_folder = (
                deploy
                / ".replay"
                / "options-edge"
                / "evidence-report"
                / "hpsf-historical-replay-20260611"
                / "manual-test"
            )
            self.assertNotEqual(0, result.returncode)
            self.assertTrue((project_folder / "hpsf-replay-report-20260611.md").exists(), result.stderr + result.stdout)
            self.assertTrue((project_folder / "hpsf-replay-summary.json").exists())
            self.assertTrue((project_folder / "replay-validation-result.json").exists())
            self.assertTrue((jenkins_archive_folder / "hpsf-replay-report-20260611.md").exists(), result.stderr + result.stdout)
            self.assertIn("Mirrored replay evidence report to: .replay/options-edge/evidence-report", result.stdout)

    def test_download_script_prepares_missing_databento_jsonl(self) -> None:
        text = DOWNLOAD_SCRIPT.read_text()

        self.assertIn("Preparing Databento replay JSONL files", text)
        self.assertIn("--prepare-jsonl-only", text)
        self.assertIn("HPSF_REPLAY_OPRA_SYMBOLS", text)
        self.assertIn("--opra-symbols", text)
        self.assertIn("OPRA definition expiry mismatch", text)
        self.assertIn("--download-dir \"$SOURCE_DIR\"", text)
        self.assertIn("DATABENTO_FEED_REPO", text)
        self.assertIn("DATABENTO_FEED_PYTHON", text)
        self.assertIn("import databento", text)
        self.assertIn("HPSF_REPLAY_REFRESH_SOURCE", text)
        self.assertIn("Refreshing Databento replay JSONL files", text)
        self.assertIn("Missing or empty replay file after Databento preparation", text)

    def test_publish_script_uses_feed_python_with_kafka_dependency(self) -> None:
        text = PUBLISH_SCRIPT.read_text()

        self.assertIn("DATABENTO_FEED_PYTHON", text)
        self.assertIn("import confluent_kafka", text)
        self.assertIn('\"$DATABENTO_FEED_PYTHON\" -m options_edge_databento_feed.hpsf_replay_cli', text)

    def test_validate_script_fails_same_day_expiry_mismatch(self) -> None:
        text = VALIDATE_OUTPUT_SCRIPT.read_text()

        self.assertIn("SAME_DAY_EXPIRY_MISMATCH", text)
        self.assertIn("NO_VALID_TODAY_EXPIRY observed in replay outputs", text)

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
            self.assertIn("HPSF_STAGE_A_VALIDATION_ATTEMPTS", validate_script)
            self.assertIn("strike-flow count = 0 on attempt", validate_script)

    def test_validate_stage_a_writes_failure_result_when_strike_flow_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            consumer = fake_bin / "kafka-console-consumer"
            consumer.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
printf 'Processed a total of 0 messages\\n' >&2
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
                    "HPSF_STAGE_A_VALIDATION_ATTEMPTS": "1",
                    "HPSF_STAGE_A_VALIDATION_TIMEOUT_MS": "1",
                    "TOPIC_PREFIX": "options.replay.20260612",
                },
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(0, result.returncode)
            validation = json.loads((artifact_dir / "replay-validation-result.json").read_text(encoding="utf-8"))
            stage_a = json.loads((artifact_dir / "hpsf-stage-a-validation.json").read_text(encoding="utf-8"))
            self.assertEqual(["STAGE_A_STRIKE_FLOW_EMPTY"], validation["failureReasons"])
            self.assertEqual(0, stage_a["strikeFlowRecordsEmitted"])

    def test_validate_final_uses_build_git_sha_fallback(self) -> None:
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
case "$topic" in
  *.hpsf.strike-flow) printf '2026-06-12|2026-06-12|6000|CALL\\t{"eventId":"strike-flow-1"}\\n' ;;
  *.hpsf.underlying-state) printf '2026-06-12|2026-06-12\\t{"esVwap":6030.25,"spxEquivalentVwap":6030.25,"distanceToVwap":1.25}\\n' ;;
  *.hpsf.market-flow) printf '2026-06-12|2026-06-12\\t{"marketBias":"NEUTRAL"}\\n' ;;
  *.hpsf.strike-score) printf '2026-06-12|2026-06-12|6000|CALL|EXECUTION\\t{"score":1.0,"executionStrike":6000,"flowAnchorStrike":6000,"optionType":"CALL"}\\n' ;;
  *.hpsf.signal) printf '2026-06-12|2026-06-12|eval-1\\t{"evaluationId":"eval-1","eventTime":"2026-06-12T14:31:17Z","action":"NO_TRADE","reasons":["MARKET_SCORE_BELOW_THRESHOLD"],"internalVwapState":"VWAP_RECLAIM_CONFIRMED","vwap":6000.0,"distanceToVwap":5.0,"orderInstruction":{"enabled":false}}\\n' ;;
  *.hpsf.latest-signal) printf '2026-06-12|2026-06-12\\t{"evaluationId":"eval-1","orderInstruction":{"enabled":false}}\\n' ;;
  *.hpsf.audit) printf '2026-06-12|2026-06-12|eval-1\\t{"evaluationId":"eval-1","selectedAction":"NO_TRADE","noTradeGates":["MARKET_SCORE_BELOW_THRESHOLD"],"gateDiagnostics":{"evaluationTime":"2026-06-12T14:31:17Z"},"chainCoverageDiagnostics":{"coverageRatio":1.0}}\\n' ;;
esac
""",
                encoding="utf-8",
            )
            consumer.chmod(0o755)
            build_dir = root / "build"
            artifact_dir = root / "artifacts"
            build_dir.mkdir()
            artifact_dir.mkdir()
            (artifact_dir / "hpsf-replay-summary.json").write_text(
                json.dumps(
                    {
                        "stageBPerformance": {
                            "stageB.chainSnapshot.update.count": 12,
                            "activeChainStorePutCount": 2,
                            "activeChainRegisteredCount": 2,
                            "stageB.activeChains.count.max": 1,
                            "stageB.punctuation.fire.count": 3,
                            "activeChainsEvaluatedCount": 3,
                            "stageB.evaluation.count": 3,
                            "underlyingStateRecordsReceived": 10,
                            "underlyingStateRecordsStored": 10,
                            "underlyingStateLookupHitCount": 9,
                            "underlyingStateLookupMissCount": 0,
                            "underlyingStateNullVwapCount": 0,
                            "underlyingStateNullDistanceCount": 0,
                            "healthyUnderlyingStateCount": 10,
                        }
                    }
                ),
                encoding="utf-8",
            )
            (root / "build-git-sha.txt").write_text("cafebabe\n", encoding="utf-8")
            (build_dir / "download-summary.json").write_text(
                json.dumps(
                    {
                        "opraTcbboRecordsRead": 10,
                        "esTradesRead": 20,
                        "esTotalSize": 30,
                        "spxSpotRecordsProduced": 20,
                        "resolvedInstrumentId": "42140864",
                    }
                ),
                encoding="utf-8",
            )
            (build_dir / "publish-summary.json").write_text(
                json.dumps(
                    {
                        "counts": {
                            "opraTcbboRecordsNormalized": 9,
                            "unknownInstrumentCount": 0,
                            "dlqCount": 1,
                            "esTradesPublished": 20,
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
                            "resolution": {"resolvedIntervals": []},
                            "candidates": [],
                        },
                    }
                ),
                encoding="utf-8",
            )
            (build_dir / "stage-a-validation.json").write_text(
                json.dumps({"stageAEmittedFinalSignalCount": 0}),
                encoding="utf-8",
            )
            env = {k: v for k, v in os.environ.items() if k not in {"CODE_GIT_SHA", "GIT_COMMIT"}}
            env.update(
                {
                    "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "HPSF_REPLAY_BUILD_DIR": str(build_dir),
                    "HPSF_REPLAY_ARTIFACT_DIR": str(artifact_dir),
                    "TOPIC_PREFIX": "options.replay.20260612",
                    "BUILD_URL": "http://jenkins.example/job/hpsf-historical-replay-20260612/16/",
                    "BUILD_NUMBER": "16",
                    "JOB_NAME": "hpsf-historical-replay-20260612",
                }
            )

            result = subprocess.run(
                [str(VALIDATE_OUTPUT_SCRIPT), "--final"],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, result.returncode, result.stderr + result.stdout)
            evidence_data = json.loads((build_dir / "evidence.json").read_text(encoding="utf-8"))
            self.assertEqual("cafebabe", evidence_data["jenkins"]["commitSha"])
            self.assertTrue(evidence_data["keyValidation"]["signalKeyValid"])
            self.assertEqual(9, evidence_data["stageBPerformance"]["underlyingStateLookupHitCount"])
            self.assertEqual({"NO_TRADE": 1}, evidence_data["actionCounts"])
            self.assertEqual({"MARKET_SCORE_BELOW_THRESHOLD": 1}, evidence_data["gateReasonCounts"])
            self.assertEqual(["6000 CALL count=1"], evidence_data["topExecutionStrikes"])
            self.assertTrue((artifact_dir / "hpsf-sample-signal.json").exists())

    def test_replay_report_root_cause_names_stage_b_underlying_not_materialized(self) -> None:
        data = evidence()
        data["validationResult"] = "FAIL"
        data["validationFailures"] = [
            "REPLAY_VALIDATION_FAILED: underlyingStateRecordsReceived <= 0",
            "REPLAY_VALIDATION_FAILED: ALL_NO_TRADE_FALLBACK_OUTPUT",
        ]
        data["counts"]["underlyingStateRecordsEmitted"] = 100
        data["stageBPerformance"].update(
            {
                "underlyingStateRecordsReceived": 0,
                "underlyingStateRecordsStored": 0,
                "underlyingStateLookupHitCount": 0,
                "underlyingStateLookupMissCount": 4,
                "healthyUnderlyingStateCount": 0,
            }
        )

        result, report = generate_report_result(data)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Stage B did not materialize", report)
        self.assertIn("Underlying-state records emitted: 100", report)
        self.assertIn("received=0, stored=0, lookupHits=0, lookupMisses=4", report)

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
            validation = json.loads((artifacts / "replay-validation-result.json").read_text(encoding="utf-8"))
            self.assertEqual("FAIL", validation["validationResult"])
            self.assertEqual(["JENKINS_REPLAY_PIPELINE_FAILED"], validation["failureReasons"])
            self.assertIn("Missing Jenkins credential options-edge-databento-api-key", validation["details"])

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


def generate_report(data: dict, previous: dict | None = None) -> str:
    result, report = generate_report_result(data, previous)
    if result.returncode != 0:
        raise AssertionError(result.stderr + report)
    return report


def generate_report_result(data: dict, previous: dict | None = None):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "evidence.json"
        report = Path(tmp) / "report.md"
        path.write_text(json.dumps(data), encoding="utf-8")
        cmd = [str(REPORT_SCRIPT), "--input", str(path), "--output", str(report)]
        if previous is not None:
            previous_path = Path(tmp) / "previous.json"
            previous_path.write_text(json.dumps(previous), encoding="utf-8")
            cmd.extend(["--previous", str(previous_path)])
        result = subprocess.run(cmd, text=True, capture_output=True)
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
        "stageBPerformance": {
            "stageB.chainSnapshot.update.count": 12,
            "activeChainStorePutCount": 2,
            "activeChainRegisteredCount": 2,
            "stageB.activeChains.count.max": 1,
            "stageB.punctuation.fire.count": 3,
            "activeChainsEvaluatedCount": 3,
            "stageB.evaluation.count": 3,
            "underlyingStateRecordsReceived": 10,
            "underlyingStateRecordsStored": 10,
            "underlyingStateLookupHitCount": 8,
            "underlyingStateLookupMissCount": 0,
            "underlyingStateNullVwapCount": 0,
            "underlyingStateNullDistanceCount": 0,
            "healthyUnderlyingStateCount": 10,
        },
        "keyValidation": {"signalKeyValid": True, "latestSignalKeyValid": True, "auditKeyValid": True},
        "topicConfigs": {"options.replay.20260612.hpsf.signal": "cleanup.policy=delete"},
        "samples": {
            "signal": {
                "action": "NO_TRADE",
                "eventTime": "2026-06-12T14:31:17Z",
                "internalVwapState": "VWAP_RECLAIM_CONFIRMED",
                "vwap": 6000.0,
                "distanceToVwap": 5.0,
                "orderInstruction": {"enabled": False},
            },
            "latestSignal": {"orderInstruction": {"enabled": False}},
            "audit": {
                "selectedAction": "NO_TRADE",
                "gateDiagnostics": {"evaluationTime": "2026-06-12T14:31:17Z"},
                "chainCoverageDiagnostics": {"coverageRatio": 1.0},
            },
        },
        "actionCounts": {"NO_TRADE": 1},
        "gateReasonCounts": {"MARKET_SCORE_BELOW_THRESHOLD": 1},
        "topExecutionStrikes": ["6005 CALL count=1"],
        "topFlowAnchors": ["6005 CALL count=1"],
        "orderInstructionEnabledTrueFound": False,
    }


if __name__ == "__main__":
    unittest.main()
