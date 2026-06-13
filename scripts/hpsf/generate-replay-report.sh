#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${HPSF_REPLAY_BUILD_DIR:-build/hpsf-replay-20260612}"
ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
CANONICAL_SUMMARY="$ARTIFACT_DIR/hpsf-replay-summary.json"
if [[ -n "${HPSF_REPLAY_EVIDENCE:-}" ]]; then
  EVIDENCE="$HPSF_REPLAY_EVIDENCE"
elif [[ -f "$CANONICAL_SUMMARY" ]]; then
  EVIDENCE="$CANONICAL_SUMMARY"
else
  EVIDENCE="$BUILD_DIR/evidence.json"
fi
REPORT="${HPSF_REPLAY_REPORT:-$ARTIFACT_DIR/hpsf-replay-report-20260612.md}"
mkdir -p "$BUILD_DIR" "$ARTIFACT_DIR"

scripts/hpsf/generate-hpsf-replay-report-20260612.py --input "$EVIDENCE" --output "$REPORT"
if [[ "$EVIDENCE" != "$CANONICAL_SUMMARY" ]]; then
  cp "$EVIDENCE" "$CANONICAL_SUMMARY"
fi
cp "$CANONICAL_SUMMARY" "$BUILD_DIR/evidence.json"
echo "Replay report generated: $REPORT"
