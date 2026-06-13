#!/usr/bin/env bash
set -euo pipefail

REPLAY_DATE="${REPLAY_DATE:-2026-06-12}"
REPLAY_START="${REPLAY_START:-2026-06-12T13:30:00Z}"
REPLAY_END="${REPLAY_END:-2026-06-12T20:00:00Z}"
SOURCE_DIR="${HPSF_REPLAY_SOURCE_DIR:?HPSF_REPLAY_SOURCE_DIR is required}"
DATABENTO_FEED_REPO="${DATABENTO_FEED_REPO:-../options-edge-databento-feed}"
BUILD_DIR="${HPSF_REPLAY_BUILD_DIR:-build/hpsf-replay-20260612}"
ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
SUMMARY="$BUILD_DIR/publish-summary.json"
mkdir -p "$BUILD_DIR" "$ARTIFACT_DIR" "$BUILD_DIR/logs"

if [[ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]]; then
  echo "KAFKA_BOOTSTRAP_SERVERS is required to publish historical replay records." >&2
  exit 1
fi
if [[ ! -d "$DATABENTO_FEED_REPO/src/options_edge_databento_feed" ]]; then
  echo "DATABENTO_FEED_REPO does not point to options-edge-databento-feed: $DATABENTO_FEED_REPO" >&2
  exit 1
fi

PYTHONPATH="$DATABENTO_FEED_REPO/src${PYTHONPATH:+:$PYTHONPATH}" \
python3 -m options_edge_databento_feed.hpsf_replay_cli \
  --date "$REPLAY_DATE" \
  --start "$REPLAY_START" \
  --end "$REPLAY_END" \
  --opra-dataset "${OPRA_DATASET:-OPRA.PILLAR}" \
  --opra-schema "${OPRA_SCHEMA:-tcbbo}" \
  --es-dataset "${ES_DATASET:-GLBX.MDP3}" \
  --es-schema "${ES_SCHEMA:-trades}" \
  --es-reference-mode "${ES_REFERENCE_MODE:-PINNED_RAW_CONTRACT}" \
  --es-symbol "${ES_SYMBOL:-ESM6}" \
  --es-stype-in "${ES_STYPE_IN:-raw_symbol}" \
  --compare-next-contract-volume "${COMPARE_NEXT_CONTRACT_VOLUME:-true}" \
  --next-contract-symbol "${NEXT_CONTRACT_SYMBOL:-ESU6}" \
  --topic-prefix "${TOPIC_PREFIX:-options.replay.20260612}" \
  --speed-mode "${REPLAY_SPEED:-FAST}" \
  --fixture-dir "$SOURCE_DIR" \
  --kafka-bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
  > "$SUMMARY"

cp "$SUMMARY" "$ARTIFACT_DIR/hpsf-publish-summary.json"
echo "Historical replay records published; summary=$SUMMARY"
