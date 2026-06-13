#!/usr/bin/env bash
set -euo pipefail

REPLAY_DATE="${REPLAY_DATE:-2026-06-12}"
REPLAY_START="${REPLAY_START:-2026-06-12T13:30:00Z}"
REPLAY_END="${REPLAY_END:-2026-06-12T20:00:00Z}"
OPRA_DATASET="${OPRA_DATASET:-OPRA.PILLAR}"
OPRA_SCHEMA="${OPRA_SCHEMA:-tcbbo}"
ES_DATASET="${ES_DATASET:-GLBX.MDP3}"
ES_SCHEMA="${ES_SCHEMA:-trades}"
ES_REFERENCE_MODE="${ES_REFERENCE_MODE:-PINNED_RAW_CONTRACT}"
ES_SYMBOL="${ES_SYMBOL:-ESM6}"
ES_STYPE_IN="${ES_STYPE_IN:-raw_symbol}"
NEXT_CONTRACT_SYMBOL="${NEXT_CONTRACT_SYMBOL:-ESU6}"
SPX_SPOT_SOURCE="${SPX_SPOT_SOURCE:-ES_BASIS_PROXY}"
SOURCE_DIR="${HPSF_REPLAY_SOURCE_DIR:-}"
DATABENTO_FEED_REPO="${DATABENTO_FEED_REPO:-../options-edge-databento-feed}"
DATABENTO_FEED_PYTHON="${DATABENTO_FEED_PYTHON:-}"
BUILD_DIR="${HPSF_REPLAY_BUILD_DIR:-build/hpsf-replay-20260612}"
ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
SUMMARY="$BUILD_DIR/download-summary.json"
mkdir -p "$BUILD_DIR" "$ARTIFACT_DIR" "$BUILD_DIR/logs"

if [[ "$ES_SYMBOL" == "ES.v.0" && "$ES_STYPE_IN" != "continuous" ]]; then
  echo "ES.v.0 requires ES_STYPE_IN=continuous" >&2
  exit 1
fi
if [[ "$ES_SYMBOL" =~ ^ES(M6|U6)$ && "$ES_STYPE_IN" != "raw_symbol" ]]; then
  echo "$ES_SYMBOL requires ES_STYPE_IN=raw_symbol" >&2
  exit 1
fi
if [[ -z "${DATABENTO_API_KEY:-}" ]]; then
  echo "DATABENTO_API_KEY is required for HPSF historical replay." >&2
  exit 1
fi
if [[ -z "$SOURCE_DIR" ]]; then
  echo "HPSF_REPLAY_SOURCE_DIR is required for HPSF historical replay JSONL files." >&2
  exit 1
fi
if [[ ! -d "$DATABENTO_FEED_REPO/src/options_edge_databento_feed" ]]; then
  echo "DATABENTO_FEED_REPO does not point to options-edge-databento-feed: $DATABENTO_FEED_REPO" >&2
  exit 1
fi
if [[ -z "$DATABENTO_FEED_PYTHON" ]]; then
  if [[ -x "$DATABENTO_FEED_REPO/.venv/bin/python" ]]; then
    DATABENTO_FEED_PYTHON="$DATABENTO_FEED_REPO/.venv/bin/python"
  else
    DATABENTO_FEED_PYTHON="python3"
  fi
fi
if ! "$DATABENTO_FEED_PYTHON" - <<'PY' >/dev/null 2>&1
import databento
PY
then
  echo "DATABENTO_FEED_PYTHON cannot import databento: $DATABENTO_FEED_PYTHON" >&2
  echo "Run the Jenkins Build stage dependency install for options-edge-databento-feed before replay download." >&2
  exit 1
fi

mkdir -p "$SOURCE_DIR"
missing_source=false
for file in opra-definitions.jsonl opra-tcbbo.jsonl es-trades.jsonl spx-price.jsonl; do
  if [[ ! -s "$SOURCE_DIR/$file" ]]; then
    missing_source=true
  fi
done
if [[ "$missing_source" == "true" ]]; then
  echo "Preparing Databento replay JSONL files in $SOURCE_DIR"
  PYTHONPATH="$DATABENTO_FEED_REPO/src${PYTHONPATH:+:$PYTHONPATH}" \
  "$DATABENTO_FEED_PYTHON" -m options_edge_databento_feed.hpsf_replay_cli \
    --date "$REPLAY_DATE" \
    --start "$REPLAY_START" \
    --end "$REPLAY_END" \
    --opra-dataset "$OPRA_DATASET" \
    --opra-schema "$OPRA_SCHEMA" \
    --es-dataset "$ES_DATASET" \
    --es-schema "$ES_SCHEMA" \
    --es-reference-mode "$ES_REFERENCE_MODE" \
    --es-symbol "$ES_SYMBOL" \
    --es-stype-in "$ES_STYPE_IN" \
    --compare-next-contract-volume "${COMPARE_NEXT_CONTRACT_VOLUME:-true}" \
    --next-contract-symbol "$NEXT_CONTRACT_SYMBOL" \
    --databento-api-key "$DATABENTO_API_KEY" \
    --download-dir "$SOURCE_DIR" \
    --prepare-jsonl-only \
    > "$BUILD_DIR/databento-prepare-summary.json"
else
  echo "Using existing prepared Databento replay JSONL files in $SOURCE_DIR"
fi

for file in opra-definitions.jsonl opra-tcbbo.jsonl es-trades.jsonl spx-price.jsonl; do
  if [[ ! -s "$SOURCE_DIR/$file" ]]; then
    echo "Missing or empty replay file after Databento preparation: $SOURCE_DIR/$file" >&2
    exit 1
  fi
done

python3 - "$SOURCE_DIR" "$SUMMARY" <<'PY'
import json
import sys
from pathlib import Path
source = Path(sys.argv[1])
summary_path = Path(sys.argv[2])

def read_jsonl(name):
    rows = []
    with (source / name).open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def instrument_id(rows):
    for row in rows:
        value = row.get("instrument_id") or row.get("instrumentId")
        if value is not None:
            return str(value)
    return ""

def total_size(rows):
    total = 0
    for row in rows:
        try:
            total += int(row.get("size", row.get("tradeSize", 0)) or 0)
        except ValueError:
            pass
    return total

opra = read_jsonl("opra-tcbbo.jsonl")
es = read_jsonl("es-trades.jsonl")
spx = read_jsonl("spx-price.jsonl")
definitions = read_jsonl("opra-definitions.jsonl")
if not opra:
    raise SystemExit("OPRA records read = 0")
if not es:
    raise SystemExit("ES trades read = 0")
summary = {
    "opraTcbboRecordsRead": len(opra),
    "opraDefinitionsRead": len(definitions),
    "esTradesRead": len(es),
    "esTotalSize": total_size(es),
    "spxSpotRecordsProduced": len(spx),
    "resolvedInstrumentId": instrument_id(es),
    "esFirstEventTime": (es[0].get("ts_event") or es[0].get("eventTime") or ""),
    "esLastEventTime": (es[-1].get("ts_event") or es[-1].get("eventTime") or ""),
}
if not summary["resolvedInstrumentId"]:
    raise SystemExit("Selected ES instrument_id missing")
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY

cat > "$BUILD_DIR/replay-request.json" <<JSON
{
  "date": "$REPLAY_DATE",
  "start": "$REPLAY_START",
  "end": "$REPLAY_END",
  "opraDataset": "$OPRA_DATASET",
  "opraSchema": "$OPRA_SCHEMA",
  "esDataset": "$ES_DATASET",
  "esSchema": "$ES_SCHEMA",
  "esReferenceMode": "$ES_REFERENCE_MODE",
  "esSymbol": "$ES_SYMBOL",
  "esStypeIn": "$ES_STYPE_IN",
  "nextContractSymbol": "$NEXT_CONTRACT_SYMBOL",
  "spxSpotSource": "$SPX_SPOT_SOURCE",
  "sourceDir": "$SOURCE_DIR"
}
JSON
cp "$SUMMARY" "$ARTIFACT_DIR/hpsf-download-summary.json"
echo "Databento replay files loaded; summary=$SUMMARY"
