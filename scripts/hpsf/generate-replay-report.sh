#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${HPSF_REPLAY_BUILD_DIR:-build/hpsf-replay-20260612}"
ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
EVIDENCE="${HPSF_REPLAY_EVIDENCE:-$BUILD_DIR/evidence.json}"
REPORT="${HPSF_REPLAY_REPORT:-$ARTIFACT_DIR/hpsf-replay-report-20260612.md}"
mkdir -p "$BUILD_DIR" "$ARTIFACT_DIR"

scripts/hpsf/generate-hpsf-replay-report-20260612.py --input "$EVIDENCE" --output "$REPORT"
cp "$EVIDENCE" "$ARTIFACT_DIR/hpsf-replay-summary.json"
echo "Replay report generated: $REPORT"
