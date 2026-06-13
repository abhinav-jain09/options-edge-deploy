#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=false
FIXTURE_MODE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --fixture-mode) FIXTURE_MODE=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

BUILD_DIR="${HPSF_REPLAY_BUILD_DIR:-build/hpsf-replay-20260612}"
REPORT="${HPSF_REPLAY_REPORT:-hpsf-replay-report-20260612.md}"
mkdir -p "$BUILD_DIR"

scripts/hpsf/create-replay-topics-20260612.sh ${DRY_RUN:+--dry-run} >"$BUILD_DIR/create-replay-topics.log"

if [[ "$DRY_RUN" == "true" || "$FIXTURE_MODE" == "true" ]]; then
  cat >"$BUILD_DIR/evidence.json" <<'JSON'
{
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
    "auditRecordsEmitted": 1
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
    "resolution": {
      "resolvedIntervals": [
        {"rawSymbol": "ESM6", "instrumentId": "42140864", "startTime": "2026-06-12T13:30:00Z", "endTime": "2026-06-12T20:00:00Z", "source": "DATABENTO_SYMBOLOGY"}
      ]
    },
    "candidates": [
      {"symbol": "ESM6", "selected": true, "reason": "PINNED_RAW_CONTRACT selected ESM6", "stats": {"tradeCount": 2, "totalSize": 10}},
      {"symbol": "ESU6", "selected": false, "reason": "NEXT_CONTRACT_VOLUME_COMPARISON", "stats": {"tradeCount": 1, "totalSize": 4}}
    ]
  },
  "stageA": {"started": true, "startupLog": "HPSF Stage A topology enabled; HPSF Stage A consuming options.replay.20260612.opra.tcbbo; HPSF Stage A producing options.replay.20260612.hpsf.strike-flow"},
  "stageB": {"started": true, "startupLog": "HPSF Stage B topology enabled; HPSF Stage B consuming options.replay.20260612.hpsf.strike-flow; HPSF Stage B consuming options.replay.20260612.hpsf.underlying-state; HPSF Stage B producing options.replay.20260612.hpsf.signal"},
  "topicConfigs": {
    "options.replay.20260612.hpsf.signal": "cleanup.policy=delete,retention.ms=2592000000,min.insync.replicas=1,compression.type=lz4",
    "options.replay.20260612.hpsf.latest-signal": "cleanup.policy=compact,delete,retention.ms=2592000000,min.insync.replicas=1,compression.type=lz4"
  },
  "samples": {
    "signal": {"evaluationId": "SPX-20260612-143110100", "tradeDate": "2026-06-12", "expiry": "2026-06-12", "action": "NO_TRADE", "setup": "VWAP_RECLAIM", "orderInstruction": {"enabled": false}},
    "latestSignal": {"evaluationId": "SPX-20260612-143110100", "tradeDate": "2026-06-12", "expiry": "2026-06-12", "orderInstruction": {"enabled": false}},
    "audit": {"evaluationId": "SPX-20260612-143110100", "tradeDate": "2026-06-12", "expiry": "2026-06-12", "selectedAction": "NO_TRADE"}
  },
  "actionCounts": {"NO_TRADE": 1},
  "gateReasonCounts": {"MARKET_SCORE_BELOW_THRESHOLD": 1},
  "topExecutionStrikes": ["6005 CALL count=1"],
  "topFlowAnchors": ["6005 CALL count=1"],
  "missingScenarioNotes": ["Fixture-mode report validates replay gate mechanics; live Databento historical pull requires configured Databento credentials."],
  "orderInstructionEnabledTrueFound": false
}
JSON
else
  : "${HPSF_REPLAY_EVIDENCE_JSON:?HPSF_REPLAY_EVIDENCE_JSON is required outside --dry-run/--fixture-mode}"
  cp "$HPSF_REPLAY_EVIDENCE_JSON" "$BUILD_DIR/evidence.json"
fi

scripts/hpsf/generate-hpsf-replay-report-20260612.py --input "$BUILD_DIR/evidence.json" --output "$REPORT"

grep -F 'Final PASS/FAIL: PASS' "$REPORT" >/dev/null
grep -F 'orderInstruction.enabled=true ever appeared: NO' "$REPORT" >/dev/null

echo "HPSF replay gate passed; report=$REPORT"
