#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
CALIBRATION=${1:-"$ROOT/evidence/zero-dte-es-challenger/calibration-summary.json"}
REPLAY=${2:-"$ROOT/evidence/zero-dte-es-challenger/nas-runtime-replay.json"}
ARTIFACT_SHA=ede3998e7ef41804bfc627d61e211d62491f2fca7ecd6582a4d2ff8953bc8ba5
CORPUS_SHA=c28a01c36e9c63d3ccba4f35a07d6fe301748e9c4973b3b6e0d9f260c8fd4e5c
EVALUATION_SHA=5c581a7c75fbff736f87f26f506914ecef436b170458fa3cc2d140a143b52d9f

if [[ $# -gt 2 ]]; then
  echo "REFUSING: expected zero arguments, or calibration and NAS replay evidence paths" >&2
  exit 2
fi
for evidence in "$CALIBRATION" "$REPLAY"; do
  if [[ ! -f "$evidence" || -L "$evidence" ]]; then
    echo "REFUSING: required regular evidence file is missing or symlinked: $evidence" >&2
    exit 1
  fi
  jq -e . "$evidence" >/dev/null
done

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
    .validation.confirmedCompressionCount > 0 and
    .validation.releaseCount > 0 and
    .validation.falseCompressionRate >= 0 and .validation.falseCompressionRate <= 1 and
    .validation.falseReleaseRate >= 0 and .validation.falseReleaseRate <= 1 and
    .historicalReevaluation.confirmedCompressionCount > 0 and
    .historicalReevaluation.releaseCount > 0 and
    .historicalReevaluation.falseCompressionRate >= 0 and
    .historicalReevaluation.falseCompressionRate <= 1 and
    .historicalReevaluation.falseReleaseRate >= 0 and
    .historicalReevaluation.falseReleaseRate <= 1
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
      (.sha256 | test("^[0-9a-f]{64}$")))
  ' "$REPLAY" >/dev/null || {
    echo "REFUSING: Java NAS replay/checkpoint evidence is absent or invalid" >&2
    exit 1
  }

echo "PASS: frozen ES calibration and Java NAS runtime replay evidence are valid"
