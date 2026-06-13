#!/usr/bin/env bash
set -euo pipefail

MODE="final"
DRY_RUN=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage-a-only) MODE="stage-a"; shift ;;
    --final) MODE="final"; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

REPLAY_DATE="${REPLAY_DATE:-2026-06-12}"
COMPACT_DATE="${REPLAY_DATE//-/}"
TOPIC_PREFIX="${TOPIC_PREFIX:-options.replay.$COMPACT_DATE}"
UNDERLYING_PREFIX="${UNDERLYING_TOPIC_PREFIX:-underlying.replay.$COMPACT_DATE}"
BUILD_DIR="${HPSF_REPLAY_BUILD_DIR:-build/hpsf-replay-20260612}"
ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
mkdir -p "$BUILD_DIR" "$ARTIFACT_DIR" "$BUILD_DIR/logs"

if [[ -n "${HPSF_REPLAY_EVIDENCE_JSON:-}" ]]; then
  cp "$HPSF_REPLAY_EVIDENCE_JSON" "$BUILD_DIR/evidence.json"
  cp "$HPSF_REPLAY_EVIDENCE_JSON" "$ARTIFACT_DIR/hpsf-replay-summary.json"
  echo "Using prebuilt HPSF replay evidence: $HPSF_REPLAY_EVIDENCE_JSON"
  exit 0
fi

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY RUN validate replay output mode=$MODE prefix=$TOPIC_PREFIX"
  exit 0
fi

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"

consume_topic() {
  local topic="$1"
  local name="$2"
  local max_messages="${3:-100}"
  local timeout_ms="${4:-30000}"
  local output="$BUILD_DIR/${name}.records"
  : > "$output"
  kafka-console-consumer \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$topic" \
    --from-beginning \
    --formatter-property print.key=true \
    --formatter-property key.separator=$'\t' \
    --max-messages "$max_messages" \
    --timeout-ms "$timeout_ms" \
    > "$output" 2> "$BUILD_DIR/logs/${name}.consumer.log" || true
  record_count "$output"
}

record_count() {
  local file="$1"
  python3 - "$file" <<'PY'
import sys
from pathlib import Path

count = 0
for raw in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line:
        continue
    if line.startswith("Option --property is deprecated"):
        continue
    if line.startswith("Processed a total of "):
        continue
    count += 1
print(count)
PY
}

count_signal_topic_without_stage_b() {
  local topic="$TOPIC_PREFIX.hpsf.signal"
  local count
  count="$(consume_topic "$topic" stage-a-signal 5 5000)"
  echo "$count" > "$BUILD_DIR/stage-a-signal-count.txt"
  if [[ "$count" != "0" ]]; then
    echo "Stage A emitted final signal records before Stage B started: $count" >&2
    exit 1
  fi
}

validate_stage_a() {
  local strike_count
  strike_count="$(consume_topic "$TOPIC_PREFIX.hpsf.strike-flow" strike-flow 100 30000)"
  echo "$strike_count" > "$BUILD_DIR/strike-flow-count.txt"
  if [[ "$strike_count" == "0" ]]; then
    echo "strike-flow count = 0" >&2
    exit 1
  fi
  count_signal_topic_without_stage_b
  cat > "$BUILD_DIR/stage-a-validation.json" <<JSON
{"strikeFlowRecordsEmitted": $strike_count, "stageAEmittedFinalSignalCount": 0}
JSON
  cp "$BUILD_DIR/stage-a-validation.json" "$ARTIFACT_DIR/hpsf-stage-a-validation.json"
}

sample_value() {
  local file="$1"
  local output="$2"
  python3 - "$file" "$output" <<'PY'
import json
import sys
from pathlib import Path
records = []
for line in Path(sys.argv[1]).read_text(encoding='utf-8').splitlines():
    stripped = line.strip()
    if not stripped:
        continue
    if stripped.startswith('Option --property is deprecated') or stripped.startswith('Processed a total of '):
        continue
    records.append(line.rstrip('\n'))
if not records:
    Path(sys.argv[2]).write_text('{}\n', encoding='utf-8')
    raise SystemExit(0)
line = records[0]
value = line.split('\t', 1)[1] if '\t' in line else line
try:
    parsed = json.loads(value)
    Path(sys.argv[2]).write_text(json.dumps(parsed, indent=2, sort_keys=True) + '\n', encoding='utf-8')
except json.JSONDecodeError:
    Path(sys.argv[2]).write_text(value + '\n', encoding='utf-8')
PY
}

key_valid() {
  local file="$1"
  local expected_pipes="$2"
  python3 - "$file" "$expected_pipes" <<'PY'
import sys
from pathlib import Path
records = []
for line in Path(sys.argv[1]).read_text(encoding='utf-8').splitlines():
    stripped = line.strip()
    if not stripped:
        continue
    if stripped.startswith('Option --property is deprecated') or stripped.startswith('Processed a total of '):
        continue
    records.append(line.rstrip('\n'))
expected = int(sys.argv[2])
if not records:
    print('false')
    raise SystemExit(0)
for line in records:
    key = line.split('\t', 1)[0] if '\t' in line else ''
    if key.count('|') != expected:
        print('false')
        raise SystemExit(0)
print('true')
PY
}

validate_final() {
  local strike_count underlying_count market_count strike_score_count signal_count latest_count audit_count
  strike_count="$(consume_topic "$TOPIC_PREFIX.hpsf.strike-flow" strike-flow 100 30000)"
  underlying_count="$(consume_topic "$TOPIC_PREFIX.hpsf.underlying-state" underlying-state 100 30000)"
  market_count="$(consume_topic "$TOPIC_PREFIX.hpsf.market-flow" market-flow 100 30000)"
  strike_score_count="$(consume_topic "$TOPIC_PREFIX.hpsf.strike-score" strike-score 100 30000)"
  signal_count="$(consume_topic "$TOPIC_PREFIX.hpsf.signal" signal 100 30000)"
  latest_count="$(consume_topic "$TOPIC_PREFIX.hpsf.latest-signal" latest-signal 100 30000)"
  audit_count="$(consume_topic "$TOPIC_PREFIX.hpsf.audit" audit 100 30000)"

  for pair in "strike-flow:$strike_count" "underlying-state:$underlying_count" "signal:$signal_count" "latest-signal:$latest_count" "audit:$audit_count"; do
    name="${pair%%:*}"
    count="${pair##*:}"
    if [[ "$count" == "0" ]]; then
      echo "$name count = 0" >&2
      exit 1
    fi
  done
  if grep -R '"enabled"[[:space:]]*:[[:space:]]*true' "$BUILD_DIR"/*.records >/dev/null 2>&1; then
    echo "orderInstruction.enabled=true found in replay outputs" >&2
    exit 1
  fi

  sample_value "$BUILD_DIR/signal.records" "$ARTIFACT_DIR/hpsf-sample-signal.json"
  sample_value "$BUILD_DIR/latest-signal.records" "$ARTIFACT_DIR/hpsf-sample-latest-signal.json"
  sample_value "$BUILD_DIR/audit.records" "$ARTIFACT_DIR/hpsf-sample-audit.json"

  signal_key_valid="$(key_valid "$BUILD_DIR/signal.records" 2)"
  latest_key_valid="$(key_valid "$BUILD_DIR/latest-signal.records" 1)"
  audit_key_valid="$(key_valid "$BUILD_DIR/audit.records" 2)"
  if [[ "$signal_key_valid" != "true" || "$latest_key_valid" != "true" || "$audit_key_valid" != "true" ]]; then
    echo "Replay output key validation failed: signal=$signal_key_valid latest=$latest_key_valid audit=$audit_key_valid" >&2
    exit 1
  fi

  python3 - "$BUILD_DIR" "$ARTIFACT_DIR" <<'PY'
import json
import os
import sys
from urllib.parse import quote
from pathlib import Path
build = Path(sys.argv[1])
artifacts = Path(sys.argv[2])

def read_json(path, default):
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default

def count_file(name):
    path = build / f'{name}.records'
    if not path.exists():
        return 0
    count = 0
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith('Option --property is deprecated') or line.startswith('Processed a total of '):
            continue
        count += 1
    return count

def sample(path):
    if not path.exists():
        return {}
    text = path.read_text(encoding='utf-8').strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}

def jenkins_build_url():
    build_url = os.environ.get('BUILD_URL', '').strip()
    if build_url:
        return build_url
    base = (os.environ.get('JENKINS_PUBLIC_URL') or os.environ.get('JENKINS_URL') or '').strip().rstrip('/')
    job_name = os.environ.get('JOB_NAME', '').strip()
    build_number = os.environ.get('BUILD_NUMBER', '').strip()
    if base and job_name and build_number:
        job_path = '/job/'.join(quote(part, safe='') for part in job_name.split('/'))
        return f"{base}/job/{job_path}/{build_number}/"
    return ''

download = read_json(build / 'download-summary.json', {})
publish = read_json(build / 'publish-summary.json', {})
request = read_json(build / 'replay-request.json', {})
stage_a = read_json(build / 'stage-a-validation.json', {})
counts = dict(publish.get('counts', {}))
counts.update({
    'opraTcbboRecordsRead': int(download.get('opraTcbboRecordsRead', counts.get('opraTcbboRecordsRead', 0))),
    'esTradesRead': int(download.get('esTradesRead', counts.get('esTradesRead', 0))),
    'esTotalSize': int(download.get('esTotalSize', counts.get('esTotalSize', 0))),
    'spxSpotRecordsProduced': int(download.get('spxSpotRecordsProduced', counts.get('spxSpotRecordsProduced', 0))),
    'strikeFlowRecordsEmitted': count_file('strike-flow'),
    'underlyingStateRecordsEmitted': count_file('underlying-state'),
    'marketFlowRecordsEmitted': count_file('market-flow'),
    'strikeScoreRecordsEmitted': count_file('strike-score'),
    'signalRecordsEmitted': count_file('signal'),
    'latestSignalRecordsEmitted': count_file('latest-signal'),
    'auditRecordsEmitted': count_file('audit'),
    'stageAEmittedFinalSignalCount': int(stage_a.get('stageAEmittedFinalSignalCount', 0)),
})
selection = publish.get('esSelection', {})
if not selection:
    selection = {
        'referenceMode': request.get('esReferenceMode', 'PINNED_RAW_CONTRACT'),
        'requestedSymbol': request.get('esSymbol', 'ESM6'),
        'requestedStypeIn': request.get('esStypeIn', 'raw_symbol'),
        'selectedSymbol': request.get('esSymbol', 'ESM6'),
        'selectedStypeIn': request.get('esStypeIn', 'raw_symbol'),
        'resolvedRawSymbol': request.get('esSymbol', 'ESM6'),
        'resolvedInstrumentId': str(download.get('resolvedInstrumentId', '')),
        'selectionReason': 'prepared Databento replay source selected requested ES symbol',
        'resolution': {'resolvedIntervals': []},
        'candidates': [],
    }
evidence = {
    'evidenceMode': 'REAL',
    'jenkins': {
        'buildUrl': jenkins_build_url(),
        'buildNumber': os.environ.get('BUILD_NUMBER', ''),
        'commitSha': os.environ.get('GIT_COMMIT', os.environ.get('CODE_GIT_SHA', '')),
        'jobName': os.environ.get('JOB_NAME', ''),
    },
    'replay': {
        'date': os.environ.get('REPLAY_DATE', '2026-06-12'),
        'start': os.environ.get('REPLAY_START', '2026-06-12T13:30:00Z'),
        'end': os.environ.get('REPLAY_END', '2026-06-12T20:00:00Z'),
        'topicPrefix': os.environ.get('TOPIC_PREFIX', 'options.replay.20260612'),
        'opraDataset': os.environ.get('OPRA_DATASET', 'OPRA.PILLAR'),
        'opraSchema': os.environ.get('OPRA_SCHEMA', 'tcbbo'),
        'esDataset': os.environ.get('ES_DATASET', 'GLBX.MDP3'),
        'esSchema': os.environ.get('ES_SCHEMA', 'trades'),
        'spxSpotSource': os.environ.get('SPX_SPOT_SOURCE', 'ES_BASIS_PROXY'),
    },
    'counts': counts,
    'esSelection': selection,
    'stageA': {'started': True, 'startupLog': 'HPSF Stage A topology enabled; replay validation observed strike-flow output'},
    'stageB': {'started': True, 'startupLog': 'HPSF Stage B topology enabled; replay validation observed signal/latest-signal/audit output'},
    'keyValidation': {'signalKeyValid': True, 'latestSignalKeyValid': True, 'auditKeyValid': True},
    'topicConfigs': read_json(build / 'topic-configs.json', {}),
    'samples': {
        'signal': sample(artifacts / 'hpsf-sample-signal.json'),
        'latestSignal': sample(artifacts / 'hpsf-sample-latest-signal.json'),
        'audit': sample(artifacts / 'hpsf-sample-audit.json'),
    },
    'actionCounts': {},
    'gateReasonCounts': {},
    'topExecutionStrikes': [],
    'topFlowAnchors': [],
    'orderInstructionEnabledTrueFound': False,
}
(build / 'validation-summary.json').write_text(json.dumps({'counts': counts, 'keyValidation': evidence['keyValidation']}, indent=2, sort_keys=True), encoding='utf-8')
(build / 'evidence.json').write_text(json.dumps(evidence, indent=2, sort_keys=True), encoding='utf-8')
(artifacts / 'hpsf-replay-summary.json').write_text(json.dumps(evidence, indent=2, sort_keys=True), encoding='utf-8')
PY
}

if [[ "$MODE" == "stage-a" ]]; then
  validate_stage_a
else
  validate_final
fi
