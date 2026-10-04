#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

cp "$ROOT/evidence/zero-dte-es-challenger/calibration-summary.json" "$TMP/calibration.json"

sessions() {
  local first=$1
  local count=$2
  jq -n --arg first "$first" --argjson count "$count" '
    [range(0; $count) | {
      session: ($first + "-" + (.|tostring)),
      sha256: ("a" * 64), pointCount: 390,
      checkpointParity: true, unmappedPointCount: 390
    }]'
}

jq -n \
  --argjson validation "$(sessions validation 50)" \
  --argjson historical "$(sessions historical 50)" '
  {
    schemaVersion: "zdce.es-nas-runtime-replay.1",
    status: "PASS", authority: "SHADOW_NOT_FOR_TRADING", capability: "PRICE_VOLUME_ONLY",
    modelVersion: "zdce-es-challenger-v1",
    artifactSha256: "ede3998e7ef41804bfc627d61e211d62491f2fca7ecd6582a4d2ff8953bc8ba5",
    corpusSha256: "c28a01c36e9c63d3ccba4f35a07d6fe301748e9c4973b3b6e0d9f260c8fd4e5c",
    splits: {
      train: {sessionCount: 151, eligibleSessionCount: 149},
      validation: {sessionCount: 50, eligibleSessionCount: 50, runtimeReplays: $validation},
      historicalReevaluation: {sessionCount: 50, eligibleSessionCount: 50,
        runtimeReplays: $historical}
    },
    replay: {
      engineClass: "com.optionsedge.processing.contexttape.regime.EsZeroDteEngine",
      replayedSessionCount: 100, pointCount: 39000,
      checkpointParitySessionCount: 100, checkpointParity: true,
      mappingUnavailablePointCount: 39000, combinedSha256: ("b" * 64)
    }
  }' > "$TMP/replay-good.json"

bash "$ROOT/scripts/es4/verify-zero-dte-es-evidence.sh" \
  "$TMP/calibration.json" "$TMP/replay-good.json" >/dev/null

# Negative control: the named mechanism is checkpoint recovery parity. Keep every other field valid
# and prove that removing only this mechanism turns the guard red.
jq '.replay.checkpointParity = false' "$TMP/replay-good.json" > "$TMP/replay-no-parity.json"
if bash "$ROOT/scripts/es4/verify-zero-dte-es-evidence.sh" \
    "$TMP/calibration.json" "$TMP/replay-no-parity.json" >/dev/null 2>&1; then
  echo "FAIL: evidence guard accepted replay without checkpoint parity" >&2
  exit 1
fi

# The activation tree deliberately has no replay evidence yet. Prove the default path refuses it;
# this prevents an absent proof from being interpreted as an empty-but-valid proof.
if bash "$ROOT/scripts/es4/verify-zero-dte-es-evidence.sh" >/dev/null 2>&1; then
  echo "FAIL: default evidence guard accepted an absent NAS runtime replay" >&2
  exit 1
fi

echo "PASS: evidence guard accepts the complete fixture and rejects both missing parity and missing proof"
