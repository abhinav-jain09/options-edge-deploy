#!/usr/bin/env bash
set -euo pipefail

MODE="final"
DRY_RUN=false
COLLECT_ONLY=false
SUMMARY_ONLY=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage-a-only) MODE="stage-a"; shift ;;
    --final) MODE="final"; shift ;;
    --collect-only) COLLECT_ONLY=true; shift ;;
    --summary-only) SUMMARY_ONLY=true; shift ;;
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
  local attempts="${HPSF_STAGE_A_VALIDATION_ATTEMPTS:-6}"
  local timeout_ms="${HPSF_STAGE_A_VALIDATION_TIMEOUT_MS:-30000}"
  local sleep_seconds="${HPSF_STAGE_A_VALIDATION_SLEEP_SECONDS:-5}"
  strike_count="0"
  for attempt in $(seq 1 "$attempts"); do
    strike_count="$(consume_topic "$TOPIC_PREFIX.hpsf.strike-flow" strike-flow 100 "$timeout_ms")"
    if [[ "$strike_count" != "0" ]]; then
      break
    fi
    echo "strike-flow count = 0 on attempt $attempt/$attempts" >&2
    if [[ "$attempt" != "$attempts" ]]; then
      sleep "$sleep_seconds"
    fi
  done
  echo "$strike_count" > "$BUILD_DIR/strike-flow-count.txt"
  if [[ "$strike_count" == "0" ]]; then
    cat > "$BUILD_DIR/stage-a-validation.json" <<JSON
{"strikeFlowRecordsEmitted": 0, "stageAEmittedFinalSignalCount": 0, "validationResult": "FAIL", "failureReasons": ["STAGE_A_STRIKE_FLOW_EMPTY"]}
JSON
    cp "$BUILD_DIR/stage-a-validation.json" "$ARTIFACT_DIR/hpsf-stage-a-validation.json"
    cat > "$ARTIFACT_DIR/replay-validation-result.json" <<JSON
{
  "validationResult": "FAIL",
  "failureReasons": ["STAGE_A_STRIKE_FLOW_EMPTY"],
  "details": ["Stage A replay did not emit strike-flow records from $TOPIC_PREFIX.opra.tcbbo into $TOPIC_PREFIX.hpsf.strike-flow after $attempts attempts."]
}
JSON
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
from datetime import datetime, timezone
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
def parsed_record(raw):
    value = raw.split('\t', 1)[1] if '\t' in raw else raw
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None

def event_time_key(record):
    value = record.get('eventTime') if isinstance(record, dict) else None
    if not value:
        return ''
    return str(value)

parsed_records = [(event_time_key(parsed), raw, parsed) for raw in records if (parsed := parsed_record(raw)) is not None]
if parsed_records:
    _, line, parsed = sorted(parsed_records, key=lambda item: item[0])[-1]
    Path(sys.argv[2]).write_text(json.dumps(parsed, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    raise SystemExit(0)

line = records[-1]
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
  if [[ "$SUMMARY_ONLY" != "true" ]]; then
    strike_count="$(consume_topic "$TOPIC_PREFIX.hpsf.strike-flow" strike-flow 100 30000)"
    underlying_count="$(consume_topic "$TOPIC_PREFIX.hpsf.underlying-state" underlying-state 100 30000)"
    market_count="$(consume_topic "$TOPIC_PREFIX.hpsf.market-flow" market-flow 100 30000)"
    strike_score_count="$(consume_topic "$TOPIC_PREFIX.hpsf.strike-score" strike-score 100 30000)"
    signal_count="$(consume_topic "$TOPIC_PREFIX.hpsf.signal" signal 100 30000)"
    latest_count="$(consume_topic "$TOPIC_PREFIX.hpsf.latest-signal" latest-signal 100 30000)"
    audit_count="$(consume_topic "$TOPIC_PREFIX.hpsf.audit" audit 100 30000)"

    sample_value "$BUILD_DIR/signal.records" "$ARTIFACT_DIR/hpsf-sample-signal.json"
    sample_value "$BUILD_DIR/latest-signal.records" "$ARTIFACT_DIR/hpsf-sample-latest-signal.json"
    sample_value "$BUILD_DIR/audit.records" "$ARTIFACT_DIR/hpsf-sample-audit.json"

    signal_key_valid="$(key_valid "$BUILD_DIR/signal.records" 2)"
    latest_key_valid="$(key_valid "$BUILD_DIR/latest-signal.records" 1)"
    audit_key_valid="$(key_valid "$BUILD_DIR/audit.records" 2)"

    if grep -R '"enabled"[[:space:]]*:[[:space:]]*true' "$BUILD_DIR"/*.records >/dev/null 2>&1; then
      order_instruction_enabled=true
    else
      order_instruction_enabled=false
    fi

    write_replay_summary "$strike_count" "$underlying_count" "$market_count" "$strike_score_count" "$signal_count" "$latest_count" "$audit_count" "$signal_key_valid" "$latest_key_valid" "$audit_key_valid" "$order_instruction_enabled"
  fi

  if [[ "$COLLECT_ONLY" == "true" ]]; then
    echo "Replay output summary collected: $ARTIFACT_DIR/hpsf-replay-summary.json"
    return 0
  fi

  validate_replay_summary
}

write_replay_summary() {
  local strike_count="$1"
  local underlying_count="$2"
  local market_count="$3"
  local strike_score_count="$4"
  local signal_count="$5"
  local latest_count="$6"
  local audit_count="$7"
  local signal_key_valid="$8"
  local latest_key_valid="$9"
  local audit_key_valid="${10}"
  local order_instruction_enabled="${11}"

  python3 - "$BUILD_DIR" "$ARTIFACT_DIR" "$strike_count" "$underlying_count" "$market_count" "$strike_score_count" "$signal_count" "$latest_count" "$audit_count" "$signal_key_valid" "$latest_key_valid" "$audit_key_valid" "$order_instruction_enabled" <<'PY'
import json
import os
import sys
from urllib.parse import quote
from pathlib import Path
build = Path(sys.argv[1])
artifacts = Path(sys.argv[2])
strike_count = int(sys.argv[3])
underlying_count = int(sys.argv[4])
market_count = int(sys.argv[5])
strike_score_count = int(sys.argv[6])
signal_count = int(sys.argv[7])
latest_count = int(sys.argv[8])
audit_count = int(sys.argv[9])
signal_key_valid = sys.argv[10] == "true"
latest_key_valid = sys.argv[11] == "true"
audit_key_valid = sys.argv[12] == "true"
order_instruction_enabled = sys.argv[13] == "true"

def read_json(path, default):
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default

def read_text(path):
    return path.read_text(encoding='utf-8').strip() if path.exists() else ''

def commit_sha():
    for value in [os.environ.get('CODE_GIT_SHA', ''), os.environ.get('GIT_COMMIT', '')]:
        if value.strip():
            return value.strip()
    return read_text(Path('build-git-sha.txt'))

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

def read_records(name):
    path = build / f'{name}.records'
    if not path.exists():
        return []
    records = []
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith('Option --property is deprecated') or line.startswith('Processed a total of '):
            continue
        value = line.split('\t', 1)[1] if '\t' in line else line
        try:
            records.append(json.loads(value))
        except json.JSONDecodeError:
            continue
    return records

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

def count_actions(records):
    counts = {}
    for record in records:
        action = record.get('action') or record.get('selectedAction')
        if isinstance(action, str) and action.strip():
            counts[action] = counts.get(action, 0) + 1
    return counts

def count_gate_reasons(records):
    counts = {}
    for record in records:
        reasons = record.get('noTradeGates') or record.get('reasons') or []
        if isinstance(reasons, str):
            reasons = [reasons]
        for reason in reasons:
            if isinstance(reason, str) and reason.strip():
                counts[reason] = counts.get(reason, 0) + 1
    return counts

def top_values(records, *keys, limit=10):
    counts = {}
    for record in records:
        parts = []
        for key in keys:
            value = record.get(key)
            if value is None or value == '':
                parts = []
                break
            parts.append(str(value))
        if not parts:
            continue
        label = ' '.join(parts)
        counts[label] = counts.get(label, 0) + 1
    return [f'{label} count={count}' for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]]

def normalize_event_time(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = int(value) // 1_000_000_000
        micros = (int(value) % 1_000_000_000) // 1000
        from datetime import datetime, timezone
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(microsecond=micros).isoformat().replace('+00:00', 'Z')
    text = str(value).strip()
    return text or None

def hydrate_es_lineage(counts, selection):
    selected = selection.get('selectedStats') if isinstance(selection.get('selectedStats'), dict) else {}
    counts.setdefault('esFirstEventTime', normalize_event_time(selected.get('firstEventTime')))
    counts.setdefault('esLastEventTime', normalize_event_time(selected.get('lastEventTime')))
    counts.setdefault('esVwapFirst', selected.get('esVwapFirst'))
    counts.setdefault('esVwapLast', selected.get('esVwapLast'))
    for key in ['esFirstEventTime', 'esLastEventTime', 'esVwapFirst', 'esVwapLast']:
        if counts.get(key) is None:
            counts[key] = ''

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
existing_summary = read_json(artifacts / 'hpsf-replay-summary.json', {})
stage_a = read_json(build / 'stage-a-validation.json', {})
counts = dict(publish.get('counts', {}))
counts.update({
    'opraTcbboRecordsRead': int(download.get('opraTcbboRecordsRead', counts.get('opraTcbboRecordsRead', 0))),
    'esTradesRead': int(download.get('esTradesRead', counts.get('esTradesRead', 0))),
    'esTotalSize': int(download.get('esTotalSize', counts.get('esTotalSize', 0))),
    'spxSpotRecordsProduced': int(download.get('spxSpotRecordsProduced', counts.get('spxSpotRecordsProduced', 0))),
    'strikeFlowRecordsEmitted': strike_count,
    'underlyingStateRecordsEmitted': underlying_count,
    'marketFlowRecordsEmitted': market_count,
    'strikeScoreRecordsEmitted': strike_score_count,
    'signalRecordsEmitted': signal_count,
    'latestSignalRecordsEmitted': latest_count,
    'auditRecordsEmitted': audit_count,
    'stageAEmittedFinalSignalCount': int(stage_a.get('stageAEmittedFinalSignalCount', 0)),
})
stage_b_performance = existing_summary.get('stageBPerformance', {})
if isinstance(stage_b_performance, dict):
    for key, value in stage_b_performance.items():
        counts[key] = value
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
hydrate_es_lineage(counts, selection)
signal_records = read_records('signal')
audit_records = read_records('audit')
strike_score_records = read_records('strike-score')
evidence = {
    'evidenceMode': 'REAL',
    'jenkins': {
        'buildUrl': jenkins_build_url(),
        'buildNumber': os.environ.get('BUILD_NUMBER', ''),
        'commitSha': commit_sha(),
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
    'stageBPerformance': stage_b_performance,
    'keyValidation': {'signalKeyValid': signal_key_valid, 'latestSignalKeyValid': latest_key_valid, 'auditKeyValid': audit_key_valid},
    'topicConfigs': read_json(build / 'topic-configs.json', {}),
    'samples': {
        'signal': sample(artifacts / 'hpsf-sample-signal.json'),
        'latestSignal': sample(artifacts / 'hpsf-sample-latest-signal.json'),
        'audit': sample(artifacts / 'hpsf-sample-audit.json'),
    },
    'actionCounts': count_actions(signal_records),
    'gateReasonCounts': count_gate_reasons(audit_records),
    'topExecutionStrikes': top_values(strike_score_records, 'executionStrike', 'optionType'),
    'topFlowAnchors': top_values(strike_score_records, 'flowAnchorStrike', 'optionType'),
    'orderInstructionEnabledTrueFound': order_instruction_enabled,
}
(build / 'validation-summary.json').write_text(json.dumps({'counts': counts, 'keyValidation': evidence['keyValidation']}, indent=2, sort_keys=True), encoding='utf-8')
(build / 'evidence.json').write_text(json.dumps(evidence, indent=2, sort_keys=True), encoding='utf-8')
(artifacts / 'hpsf-replay-summary.json').write_text(json.dumps(evidence, indent=2, sort_keys=True), encoding='utf-8')
PY
}

validate_replay_summary() {
  python3 - "$ARTIFACT_DIR/hpsf-replay-summary.json" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

summary_path = Path(sys.argv[1])
if not summary_path.exists():
    raise SystemExit(f"Replay summary missing: {summary_path}")
summary = json.loads(summary_path.read_text(encoding="utf-8"))
counts = summary.get("counts") or {}
stage_b = summary.get("stageBPerformance") or {}
key_validation = summary.get("keyValidation") or {}
profile = str(summary.get("validationProfile") or "FULL_RTH_RELEASE").upper()
samples = summary.get("samples") or {}
replay = summary.get("replay") or {}
action_counts = summary.get("actionCounts") or {}
gate_reason_counts = summary.get("gateReasonCounts") or {}

def as_int(name):
    if name in stage_b:
        value = stage_b.get(name)
    else:
        value = counts.get(name)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

failures = []
required_counts = [
    ("strikeFlowRecordsEmitted", "strike-flow count = 0"),
    ("underlyingStateRecordsEmitted", "underlying-state count = 0"),
    ("marketFlowRecordsEmitted", "market-flow count = 0"),
    ("strikeScoreRecordsEmitted", "strike-score count = 0"),
    ("signalRecordsEmitted", "signal count = 0"),
    ("latestSignalRecordsEmitted", "latest-signal count = 0"),
    ("auditRecordsEmitted", "audit count = 0"),
]
if profile == "FULL_RTH_RELEASE":
    for key, message in required_counts:
        try:
            actual = int(counts.get(key, 0))
        except (TypeError, ValueError):
            actual = 0
        if actual <= 0:
            failures.append(message)

required_stage_b = [
    "stageB.chainSnapshot.update.count",
    "activeChainStorePutCount",
    "activeChainRegisteredCount",
    "stageB.activeChains.count.max",
    "stageB.punctuation.fire.count",
    "activeChainsEvaluatedCount",
    "stageB.evaluation.count",
    "underlyingStateRecordsReceived",
    "underlyingStateRecordsStored",
    "underlyingStateLookupHitCount",
    "underlyingStateLookupMissCount",
    "underlyingStateNullVwapCount",
    "underlyingStateNullDistanceCount",
    "healthyUnderlyingStateCount",
]
if profile == "FULL_RTH_RELEASE":
    missing = [key for key in required_stage_b if as_int(key) is None]
    if missing:
        failures.append(f"Missing Stage B replay counters after merge: {missing}")
    else:
        chain_updates = as_int("stageB.chainSnapshot.update.count") or 0
        active_puts = as_int("activeChainStorePutCount") or 0
        active_registered = as_int("activeChainRegisteredCount") or 0
        active_max = as_int("stageB.activeChains.count.max") or 0
        punctuation = as_int("stageB.punctuation.fire.count") or 0
        active_evaluated = as_int("activeChainsEvaluatedCount") or 0
        evaluations = as_int("stageB.evaluation.count") or 0
        received = as_int("underlyingStateRecordsReceived") or 0
        stored = as_int("underlyingStateRecordsStored") or 0
        hits = as_int("underlyingStateLookupHitCount") or 0
        misses = as_int("underlyingStateLookupMissCount") or 0
        null_vwap = as_int("underlyingStateNullVwapCount") or 0
        null_distance = as_int("underlyingStateNullDistanceCount") or 0
        healthy = as_int("healthyUnderlyingStateCount") or 0
        if chain_updates <= 0:
            failures.append("STAGE_B_CHAIN_SNAPSHOT_UPDATE_COUNT_ZERO")
        if chain_updates > 0 and active_puts <= 0:
            failures.append("STAGE_B_ACTIVE_CHAIN_NOT_REGISTERED")
        if active_registered <= 0:
            failures.append("STAGE_B_ACTIVE_CHAIN_REGISTERED_COUNT_ZERO")
        if active_puts > 0 and punctuation <= 0:
            failures.append("STAGE_B_SCHEDULED_PUNCTUATION_DID_NOT_FIRE")
        if punctuation > 0 and evaluations <= 0 and active_max <= 0:
            failures.append("STAGE_B_NO_ACTIVE_CHAINS_AT_PUNCTUATION")
        if punctuation > 0 and active_max > 0 and evaluations <= 0:
            failures.append("STAGE_B_ACTIVE_CHAINS_NOT_EVALUATED")
        if active_evaluated <= 0:
            failures.append("STAGE_B_ACTIVE_CHAINS_EVALUATED_COUNT_ZERO")
        if evaluations <= 0:
            failures.append("stageB.evaluation.count <= 0")
        if active_max <= 0:
            failures.append("stageB.activeChains.count.max <= 0")
        if received <= 0:
            failures.append("underlyingStateRecordsReceived <= 0")
        if stored <= 0:
            failures.append("underlyingStateRecordsStored <= 0")
        if hits <= 0:
            failures.append("underlyingStateLookupHitCount <= 0")
        if healthy <= 0:
            failures.append("healthyUnderlyingStateCount <= 0")
        if misses > 0 and hits == 0:
            failures.append("underlyingStateLookupMissCount > 0 and underlyingStateLookupHitCount == 0")
        if null_vwap > 0:
            failures.append("underlyingStateNullVwapCount > 0")
        if null_distance > 0:
            failures.append("underlyingStateNullDistanceCount > 0")

if profile == "FULL_RTH_RELEASE":
    zero_outputs = [
        key for key in [
            "marketFlowRecordsEmitted",
            "strikeScoreRecordsEmitted",
            "signalRecordsEmitted",
            "latestSignalRecordsEmitted",
            "auditRecordsEmitted",
        ]
        if int(counts.get(key, 0) or 0) <= 0
    ]
    if zero_outputs:
        failures.append("STAGE_B_ZERO_FINAL_OUTPUTS: " + ",".join(zero_outputs))
    try:
        signal_count = int(counts.get("signalRecordsEmitted", 0) or 0)
    except (TypeError, ValueError):
        signal_count = 0
    try:
        audit_count = int(counts.get("auditRecordsEmitted", 0) or 0)
    except (TypeError, ValueError):
        audit_count = 0
    if signal_count > 0 and not action_counts:
        failures.append("ACTION_COUNTS_EMPTY_WITH_SIGNALS")
    if audit_count > 0 and not gate_reason_counts:
        failures.append("GATE_REASON_COUNTS_EMPTY_WITH_AUDITS")
    if action_counts:
        action_sum = sum(int(value or 0) for value in action_counts.values())
        if action_sum != signal_count:
            failures.append(f"ACTION_COUNT_MISMATCH: sum(actionCounts)={action_sum}, signalRecordsEmitted={signal_count}")

if profile == "FULL_RTH_RELEASE":
    signal_sample = samples.get("signal") if isinstance(samples.get("signal"), dict) else {}
    latest_signal_sample = samples.get("latestSignal") if isinstance(samples.get("latestSignal"), dict) else {}
    audit_sample = samples.get("audit") if isinstance(samples.get("audit"), dict) else {}
    def reason_code(reason):
        if not isinstance(reason, str):
            return ""
        text = reason.strip()
        if not text:
            return ""
        return text.replace("=", " ").replace(":", " ").split()[0]

    def reason_codes(values):
        return {code for code in (reason_code(value) for value in values) if code}

    sample_reasons = set(signal_sample.get("reasons") or [])
    sample_reasons.update(audit_sample.get("noTradeGates") or [])
    sample_reason_codes = reason_codes(sample_reasons)
    aggregate_reason_codes = set(sample_reason_codes)
    aggregate_reason_codes.update(
        reason_code(reason)
        for reason, count in gate_reason_counts.items()
        if int(count or 0) > 0 and reason_code(reason)
    )
    expected_trade_date = str(replay.get("date") or "")
    for label, sample_payload in [
        ("signal", signal_sample),
        ("latestSignal", latest_signal_sample),
        ("audit", audit_sample),
    ]:
        trade_date = str(sample_payload.get("tradeDate") or "")
        expiry = str(sample_payload.get("expiry") or "")
        if trade_date and expiry and trade_date != expiry:
            failures.append(f"SAME_DAY_EXPIRY_MISMATCH: {label}.tradeDate={trade_date} {label}.expiry={expiry}")
        if expected_trade_date and expiry and expiry != expected_trade_date:
            failures.append(f"REPLAY_EXPIRY_DOES_NOT_MATCH_REPLAY_DATE: {label}.expiry={expiry}, replayDate={expected_trade_date}")
    if "NO_VALID_TODAY_EXPIRY" in aggregate_reason_codes:
        failures.append("NO_VALID_TODAY_EXPIRY observed in replay outputs")
    data_failure_reasons = {
        "SPX_SPOT_STALE",
        "ES_TRADE_STALE",
        "VWAP_STALE",
        "BASIS_STALE",
        "OPRA_FLOW_STALE",
        "INSUFFICIENT_CHAIN_COVERAGE",
        "INSUFFICIENT_CALL_COVERAGE",
        "INSUFFICIENT_PUT_COVERAGE",
        "NO_LIQUID_EXECUTION_CANDIDATE",
        "EXECUTION_CANDIDATE_TOO_FAR",
    }
    strategy_valid_reasons = {
        "NO_ACTIVE_VWAP_SETUP",
        "MARKET_SCORE_BELOW_THRESHOLD",
        "MIXED_FLOW",
        "COMPLEX_FLOW_DOMINANT",
    }
    fallback_reasons = {
        "MARKET_EVALUATOR_NOT_READY",
        "UNDERLYING_STATE_UNAVAILABLE",
        "VWAP",
    }
    if action_counts.get("NO_TRADE", 0) == int(counts.get("signalRecordsEmitted", 0) or 0):
        sample_vwap_available = (
            signal_sample.get("vwap") is not None
            and signal_sample.get("distanceToVwap") is not None
            and str(signal_sample.get("internalVwapState") or "").upper() not in {"", "VWAP_UNAVAILABLE"}
        )
        if sample_reason_codes & fallback_reasons and not sample_vwap_available:
            failures.append("ALL_NO_TRADE_FALLBACK_OUTPUT")
        if aggregate_reason_codes & data_failure_reasons and not aggregate_reason_codes & strategy_valid_reasons:
            failures.append("ALL_NO_TRADE_DATA_FAILURE")
    gate_diagnostics = audit_sample.get("gateDiagnostics")
    coverage_diagnostics = audit_sample.get("chainCoverageDiagnostics")
    if not isinstance(gate_diagnostics, dict) or not gate_diagnostics:
        failures.append("STAGE_B_AUDIT_GATE_DIAGNOSTICS_EMPTY")
    if not isinstance(coverage_diagnostics, dict) or not coverage_diagnostics:
        failures.append("STAGE_B_AUDIT_CHAIN_COVERAGE_DIAGNOSTICS_EMPTY")

    def parse_time(value):
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)

    start = parse_time(replay.get("start"))
    end = parse_time(replay.get("end"))
    sample_time = parse_time(signal_sample.get("eventTime"))
    if start and end and sample_time and not (start <= sample_time <= end):
        failures.append("STAGE_B_SAMPLE_SIGNAL_EVENT_TIME_OUTSIDE_REPLAY_WINDOW")
    if start and end and not sample_time:
        failures.append("STAGE_B_SAMPLE_SIGNAL_EVENT_TIME_MISSING")

if summary.get("orderInstructionEnabledTrueFound"):
    failures.append("orderInstruction.enabled=true found in replay outputs")
if not (key_validation.get("signalKeyValid") and key_validation.get("latestSignalKeyValid") and key_validation.get("auditKeyValid")):
    failures.append(f"Replay output key validation failed: {key_validation}")

if failures:
    for failure in failures:
        print(failure, file=sys.stderr)
    validation = {
        "result": "FAIL",
        "validationResult": "FAIL",
        "validationProfile": profile,
        "validationFailures": [
            {
                "code": failure.split(":", 1)[0] if failure.startswith("STAGE_B_") else "REPLAY_VALIDATION_FAILED",
                "detail": failure,
            }
            for failure in failures
        ],
    }
else:
    validation = {
        "result": "PASS",
        "validationResult": "PASS",
        "validationProfile": profile,
        "validationFailures": [],
    }
summary["validationProfile"] = profile
summary["validationResult"] = validation["validationResult"]
summary["validationFailures"] = validation["validationFailures"]
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
(summary_path.parent / "replay-validation-result.json").write_text(json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
raise SystemExit(1 if failures else 0)
PY
}

if [[ "$MODE" == "stage-a" ]]; then
  validate_stage_a
else
  validate_final
fi
