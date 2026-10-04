#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

CALIBRATION="$ROOT/evidence/zero-dte-es-challenger/calibration-summary.json"
cp "$ROOT/evidence/zero-dte-es-challenger/nas-runtime-replay.json" "$TMP/replay-good.json"
IMAGE_DIGEST="sha256:$(printf 'c%.0s' {1..64})"

make_lock() {
  local replay=$1 lock=$2 runtime_git=${3:-}
  local replay_sha actual_git runtime_jar
  replay_sha=$(shasum -a 256 "$replay" | awk '{print $1}')
  actual_git=$(jq -er '.runtime.gitSha' "$replay")
  runtime_jar=$(jq -er '.runtime.jarSha256' "$replay")
  [[ -n "$runtime_git" ]] || runtime_git=$actual_git
  jq -n --arg replay "$replay_sha" --arg runtimeGit "$runtime_git" \
    --arg runtimeJar "$runtime_jar" --arg image "$IMAGE_DIGEST" '
    {
      schemaVersion: "zdce.es-activation-lock.1",
      status: "APPROVED_FOR_SHADOW_ACTIVATION",
      authority: "SHADOW_NOT_FOR_TRADING",
      reviewed: true,
      approvedBy: "evidence-guard-test",
      approvedAt: "2026-10-04T21:00:00Z",
      artifactSha256: "9b59499f2db8651262992cea9107d3333eeede3af46e5920c4e6e283a9a6e3f3",
      corpusSha256: "c28a01c36e9c63d3ccba4f35a07d6fe301748e9c4973b3b6e0d9f260c8fd4e5c",
      evaluationSha256: "0ae27038e58e28e184a833edabd9061b5ffe4be779753b64efb24d107a342df0",
      replaySha256: $replay,
      runtimeGitSha: $runtimeGit,
      runtimeJarSha256: $runtimeJar,
      imageDigest: $image
    }' > "$lock"
}

expect_rejected() {
  local label=$1 replay=$2 lock=$3 actual_image=${4:-$IMAGE_DIGEST}
  if ZERO_DTE_ES_CHALLENGER_ACTUAL_IMAGE_DIGEST="$actual_image" \
      bash "$ROOT/scripts/es4/verify-zero-dte-es-evidence.sh" \
      "$CALIBRATION" "$replay" "$lock" >/dev/null 2>&1; then
    echo "FAIL: evidence guard accepted $label" >&2
    exit 1
  fi
}

make_lock "$TMP/replay-good.json" "$TMP/lock-good.json"
ZERO_DTE_ES_CHALLENGER_ACTUAL_IMAGE_DIGEST="$IMAGE_DIGEST" \
  bash "$ROOT/scripts/es4/verify-zero-dte-es-evidence.sh" \
  "$CALIBRATION" "$TMP/replay-good.json" "$TMP/lock-good.json" >/dev/null

# Each negative control repairs the replay digest in its lock where appropriate, proving that the
# named semantic field—not merely a stale whole-file digest—is what turns the guard red.
jq '.replay.checkpointParity = false' "$TMP/replay-good.json" > "$TMP/replay-no-parity.json"
make_lock "$TMP/replay-no-parity.json" "$TMP/lock-no-parity.json"
expect_rejected "replay without checkpoint parity" \
  "$TMP/replay-no-parity.json" "$TMP/lock-no-parity.json"

jq '.performance.p99ProcessingLatencyMs = 50.001' "$TMP/replay-good.json" \
  > "$TMP/replay-slow.json"
make_lock "$TMP/replay-slow.json" "$TMP/lock-slow.json"
expect_rejected "replay exceeding the p99 latency bound" "$TMP/replay-slow.json" "$TMP/lock-slow.json"

make_lock "$TMP/replay-good.json" "$TMP/lock-wrong-runtime.json" \
  dddddddddddddddddddddddddddddddddddddddd
expect_rejected "activation lock for another runtime commit" \
  "$TMP/replay-good.json" "$TMP/lock-wrong-runtime.json"

expect_rejected "activation lock for another deployed image" \
  "$TMP/replay-good.json" "$TMP/lock-good.json" "sha256:$(printf 'e%.0s' {1..64})"

expect_rejected "missing activation approval" \
  "$TMP/replay-good.json" "$TMP/does-not-exist.json"

ln -s "$TMP/lock-good.json" "$TMP/lock-symlink.json"
expect_rejected "symlinked activation lock" "$TMP/replay-good.json" "$TMP/lock-symlink.json"

echo "PASS: evidence guard binds calibration, replay semantics, runtime, approval, and deployed image"
