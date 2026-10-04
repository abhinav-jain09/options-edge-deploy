#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
CALIBRATION=${1:-"$ROOT/evidence/zero-dte-es-challenger/calibration-summary.json"}
REPLAY=${2:-"$ROOT/evidence/zero-dte-es-challenger/nas-runtime-replay.json"}
LOCK=${3:-"$ROOT/evidence/zero-dte-es-challenger/activation-lock.json"}
ARTIFACT_SHA=9b59499f2db8651262992cea9107d3333eeede3af46e5920c4e6e283a9a6e3f3
CORPUS_SHA=c28a01c36e9c63d3ccba4f35a07d6fe301748e9c4973b3b6e0d9f260c8fd4e5c
EVALUATION_SHA=0ae27038e58e28e184a833edabd9061b5ffe4be779753b64efb24d107a342df0
ACTUAL_IMAGE_DIGEST=${ZERO_DTE_ES_CHALLENGER_ACTUAL_IMAGE_DIGEST:-}

if [[ $# -gt 3 ]]; then
  echo "REFUSING: expected zero arguments, or calibration, NAS replay, and activation-lock paths" >&2
  exit 2
fi
for evidence in "$CALIBRATION" "$REPLAY" "$LOCK"; do
  if [[ ! -f "$evidence" || -L "$evidence" ]]; then
    echo "REFUSING: required regular evidence file is missing or symlinked: $evidence" >&2
    exit 1
  fi
  jq -e . "$evidence" >/dev/null
done
if [[ ! "$ACTUAL_IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "REFUSING: ZERO_DTE_ES_CHALLENGER_ACTUAL_IMAGE_DIGEST must be the deployed sha256 digest" >&2
  exit 1
fi

jq -e --arg artifact "$ARTIFACT_SHA" --arg corpus "$CORPUS_SHA" \
  --arg evaluation "$EVALUATION_SHA" '
    .schemaVersion == "zdce.es-calibration-attestation.1" and
    .status == "PASS" and
    .authority == "SHADOW_NOT_FOR_TRADING" and
    .capability == "PRICE_VOLUME_ONLY" and
    .modelVersion == "zdce-es-challenger-v1" and
    .artifactSha256 == $artifact and
    .corpusSha256 == $corpus and
    .evaluationSha256 == $evaluation and
    .quality.trainEligibleSessions == 149 and
    .quality.validationEligibleSessions == 50 and
    .quality.historicalReevaluationEligibleSessions == 50 and
    .validation.confirmedCompressionCount == 98 and
    .validation.releaseCount == 82 and
    .validation.labelledExpansionOnsetCount == 456 and
    .validation.matchedExpansionOnsetCount == 46 and
    .validation.missedExpansionOnsetCount == 410 and
    .validation.latencyAndConsumptionIncludeMisses == true and
    .validation.medianOnsetDelayMinutes == 6 and
    .validation.medianMovementConsumed == 1 and
    .historicalReevaluation.confirmedCompressionCount == 53 and
    .historicalReevaluation.releaseCount == 63 and
    .historicalReevaluation.labelledExpansionOnsetCount == 416 and
    .historicalReevaluation.matchedExpansionOnsetCount == 39 and
    .historicalReevaluation.missedExpansionOnsetCount == 377 and
    .historicalReevaluation.latencyAndConsumptionIncludeMisses == true and
    .historicalReevaluation.medianOnsetDelayMinutes == 6 and
    .historicalReevaluation.medianMovementConsumed == 1
  ' "$CALIBRATION" >/dev/null || {
    echo "REFUSING: frozen NAS calibration/backtest attestation is invalid" >&2
    exit 1
  }

jq -e --arg artifact "$ARTIFACT_SHA" --arg corpus "$CORPUS_SHA" '
    .schemaVersion == "zdce.es-nas-runtime-replay.1" and
    .status == "PASS" and
    .authority == "SHADOW_NOT_FOR_TRADING" and
    .capability == "PRICE_VOLUME_ONLY" and
    .modelVersion == "zdce-es-challenger-v1" and
    .artifactSha256 == $artifact and
    .corpusSha256 == $corpus and
    .corpusPath == "/Volumes/database/optionsedge/corpus/incoming/ladder-1yr" and
    (.runtime.gitSha | test("^[0-9a-f]{40}$")) and
    (.runtime.jarSha256 | test("^[0-9a-f]{64}$")) and
    .runtime.engineClass == "com.optionsedge.processing.contexttape.regime.EsZeroDteEngine" and
    .replay.engineClass == "com.optionsedge.processing.contexttape.regime.EsZeroDteEngine" and
    .replay.replayedSessionCount == 100 and
    .replay.pointCount == 39000 and
    .replay.checkpointParitySessionCount == 100 and
    .replay.checkpointParity == true and
    .replay.mappingUnavailablePointCount == 39000 and
    (.replay.combinedSha256 | test("^[0-9a-f]{64}$")) and
    .splits.train.sessionCount == 151 and
    .splits.train.eligibleSessionCount == 149 and
    .splits.validation.sessionCount == 50 and
    .splits.validation.eligibleSessionCount == 50 and
    .splits.historicalReevaluation.sessionCount == 50 and
    .splits.historicalReevaluation.eligibleSessionCount == 50 and
    (.splits.validation.runtimeReplays | length) == 50 and
    (.splits.historicalReevaluation.runtimeReplays | length) == 50 and
    all(.splits.validation.runtimeReplays[];
      .pointCount == 390 and .checkpointParity == true and .unmappedPointCount == 390 and
      (.sha256 | test("^[0-9a-f]{64}$"))) and
    all(.splits.historicalReevaluation.runtimeReplays[];
      .pointCount == 390 and .checkpointParity == true and .unmappedPointCount == 390 and
      (.sha256 | test("^[0-9a-f]{64}$"))) and
    .robustness.outOfOrderParity == true and
    .robustness.sourceAblationMonotonic == true and
    .robustness.missingMinuteUnknown == true and
    .robustness.sessionRollReadyFailClosed == true and
    .performance.loadTestPassed == true and
    .performance.inputRecordCount == 250000 and
    .performance.queueRecordLimit == 250000 and
    .performance.queueByteLimit == 67108864 and
    .performance.encodedInputBytes > 0 and
    .performance.encodedInputBytes <= .performance.queueByteLimit and
    .performance.configuredMaxHeapBytes == 671088640 and
    .performance.maxHeapBytes > 0 and
    .performance.maxHeapBytes <= .performance.configuredMaxHeapBytes and
    .performance.p99ProcessingLatencyMs >= 0 and
    .performance.p99ProcessingLatencyMs <= 50 and
    .performance.throughputRecordsPerSecond > 0
  ' "$REPLAY" >/dev/null || {
    echo "REFUSING: Java NAS replay, recovery, robustness, or bounded-load evidence is invalid" >&2
    exit 1
  }

REPLAY_SHA=$(shasum -a 256 "$REPLAY" | awk '{print $1}')
RUNTIME_GIT_SHA=$(jq -er '.runtime.gitSha' "$REPLAY")
RUNTIME_JAR_SHA=$(jq -er '.runtime.jarSha256' "$REPLAY")
jq -e --arg artifact "$ARTIFACT_SHA" --arg corpus "$CORPUS_SHA" \
  --arg evaluation "$EVALUATION_SHA" --arg replay "$REPLAY_SHA" \
  --arg runtimeGit "$RUNTIME_GIT_SHA" --arg runtimeJar "$RUNTIME_JAR_SHA" \
  --arg image "$ACTUAL_IMAGE_DIGEST" '
    .schemaVersion == "zdce.es-activation-lock.1" and
    .status == "APPROVED_FOR_SHADOW_ACTIVATION" and
    .authority == "SHADOW_NOT_FOR_TRADING" and
    .reviewed == true and
    (.approvedBy | type == "string" and length > 0) and
    (.approvedAt | type == "string" and test("Z$")) and
    .artifactSha256 == $artifact and
    .corpusSha256 == $corpus and
    .evaluationSha256 == $evaluation and
    .replaySha256 == $replay and
    .runtimeGitSha == $runtimeGit and
    .runtimeJarSha256 == $runtimeJar and
    .imageDigest == $image
  ' "$LOCK" >/dev/null || {
    echo "REFUSING: activation lock does not bind the reviewed replay, runtime JAR, and deployed image digest" >&2
    exit 1
  }

echo "PASS: ES calibration, exact Java replay, robustness/load proofs, and deployed image are bound for shadow activation"
