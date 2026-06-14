#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
LOG_DIR="$ARTIFACT_DIR/logs"
mkdir -p "$LOG_DIR"

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
NAMESPACE="${NAMESPACE:-options-edge}"
BUILD="${BUILD_NUMBER:-manual}"
TOPIC_PREFIX="${TOPIC_PREFIX:-options.replay.20260612}"
PATH="/home/confluent/confluent-8.2.1/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"

run_capture() {
  local output="$1"
  shift
  set +e
  "$@" > "$output" 2>&1
  local status=$?
  set -e
  printf '%s\n' "$status" > "$output.exit-code"
  return 0
}

collect_service() {
  local label="$1"
  local pod="$2"
  local app_id="$3"
  local log_name="$4"

  kubectl -n "$NAMESPACE" get pod "$pod" -o json > "$LOG_DIR/${label}-pod.json" 2>/dev/null || true
  kubectl -n "$NAMESPACE" describe pod "$pod" > "$LOG_DIR/${label}-describe.log" 2>/dev/null || true
  kubectl -n "$NAMESPACE" logs "$pod" --tail=600 > "$LOG_DIR/${log_name}" 2>/dev/null || true
  run_capture "$LOG_DIR/${label}-consumer-group.log" kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$app_id"
  set +e
  kafka-topics --bootstrap-server "$BOOTSTRAP" --list > "$LOG_DIR/all-kafka-topics.log" 2>&1
  set -e
  grep -E "^${app_id//./\\.}-" "$LOG_DIR/all-kafka-topics.log" > "$LOG_DIR/${label}-internal-topics.log" 2>/dev/null || true
}

collect_service stage-a hpsf-stage-a-replay-20260612 "options-edge-hpsf-stage-a-replay-20260612-${BUILD}" stage-a.log
collect_service underlying hpsf-underlying-replay-20260612 "options-edge-hpsf-underlying-replay-20260612-${BUILD}" underlying.log
collect_service stage-b hpsf-stage-b-replay-20260612 "options-edge-hpsf-stage-b-replay-20260612-${BUILD}" stage-b.log

run_capture "$LOG_DIR/replay-source-topic.log" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC_PREFIX.opra.tcbbo"
run_capture "$LOG_DIR/replay-strike-flow-topic.log" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC_PREFIX.hpsf.strike-flow"
run_capture "$LOG_DIR/replay-dlq-topic.log" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC_PREFIX.hpsf.dlq"

python3 - <<'PY'
import json
import os
import re
from pathlib import Path

log_dir = Path(os.environ.get("HPSF_REPLAY_ARTIFACT_DIR", "artifacts")) / "logs"
build = os.environ.get("BUILD_NUMBER", "manual")
topic_prefix = os.environ.get("TOPIC_PREFIX", "options.replay.20260612")


def read(path: Path, limit: int = 12000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-limit:]


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def stream_state(text: str) -> str:
    states = re.findall(r"state transition [A-Z_]+ -> ([A-Z_]+)", text)
    if states:
        return states[-1]
    if "RUNNING" in text:
        return "RUNNING"
    if "REBALANCING" in text:
        return "REBALANCING"
    if "CREATED" in text:
        return "CREATED"
    return ""


def group_exists(label: str) -> bool:
    text = read(log_dir / f"{label}-consumer-group.log")
    if "GroupIdNotFoundException" in text or "does not exist" in text:
        return False
    return bool(text.strip()) and "Error:" not in text


def internal_topics(label: str) -> list[str]:
    text = read(log_dir / f"{label}-internal-topics.log")
    return [line.strip() for line in text.splitlines() if line.strip()]


def pod_phase(label: str) -> str:
    pod = read_json(log_dir / f"{label}-pod.json")
    return str((pod.get("status") or {}).get("phase") or "")


stage_a_log = read(log_dir / "stage-a.log")
stage_b_log = read(log_dir / "stage-b.log")
underlying_log = read(log_dir / "underlying.log")
stage_a_internal = internal_topics("stage-a")
stage_b_internal = internal_topics("stage-b")
underlying_internal = internal_topics("underlying")

payload = {
    "build": build,
    "topicPrefix": topic_prefix,
    "services": {
        "stageA": {
            "pod": "hpsf-stage-a-replay-20260612",
            "phase": pod_phase("stage-a"),
            "streamState": stream_state(stage_a_log),
            "logTail": stage_a_log,
        },
        "underlying": {
            "pod": "hpsf-underlying-replay-20260612",
            "phase": pod_phase("underlying"),
            "streamState": stream_state(underlying_log),
            "logTail": underlying_log,
        },
        "stageB": {
            "pod": "hpsf-stage-b-replay-20260612",
            "phase": pod_phase("stage-b"),
            "streamState": stream_state(stage_b_log),
            "logTail": stage_b_log,
        },
    },
    "kafka": {
        "stageAConsumerGroup": {
            "groupId": f"options-edge-hpsf-stage-a-replay-20260612-{build}",
            "exists": group_exists("stage-a"),
        },
        "underlyingConsumerGroup": {
            "groupId": f"options-edge-hpsf-underlying-replay-20260612-{build}",
            "exists": group_exists("underlying"),
        },
        "stageBConsumerGroup": {
            "groupId": f"options-edge-hpsf-stage-b-replay-20260612-{build}",
            "exists": group_exists("stage-b"),
        },
        "stageAInternalTopics": {
            "count": len(stage_a_internal),
            "topics": stage_a_internal,
        },
        "underlyingInternalTopics": {
            "count": len(underlying_internal),
            "topics": underlying_internal,
        },
        "stageBInternalTopics": {
            "count": len(stage_b_internal),
            "topics": stage_b_internal,
        },
    },
    "logging": {
        "slf4jNoopDetected": "SLF4J: Defaulting to no-operation" in "\n".join([stage_a_log, stage_b_log, underlying_log]),
    },
}
(log_dir / "root-cause-diagnostics.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
(log_dir / "root-cause-diagnostics.log").write_text(
    "\n".join(
        [
            f"build={build}",
            f"topicPrefix={topic_prefix}",
            f"stageA.streamState={payload['services']['stageA']['streamState']}",
            f"stageA.consumerGroup.exists={payload['kafka']['stageAConsumerGroup']['exists']}",
            f"stageA.internalTopic.count={payload['kafka']['stageAInternalTopics']['count']}",
            f"slf4jNoopDetected={payload['logging']['slf4jNoopDetected']}",
        ]
    )
    + "\n",
    encoding="utf-8",
)
PY

echo "Replay root-cause diagnostics written to $LOG_DIR/root-cause-diagnostics.json"
