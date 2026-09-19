#!/usr/bin/env bash
# ensure-gamma-ladder-path-topics.sh — gamma-ladder-path-service's two output topics must EXIST with their declared
# shape BEFORE the service rolls out (the same barrier as ensure-multileg-topics / ensure-gamma-tilt-ledger).
#
# Why: apply-topics lives in a different job. On a FIRST deploy neither topic exists, and the broker either
# auto-creates them with cluster defaults (state NOT compacted, time retention — the forward-test record then
# expires) or, with auto-create off, the Streams producer fails and the pod crash-loops. Declared in
# scripts/kafka/topics.env (both reset-preserved, retention -1):
#   options.spx.gamma-ladder-path.state   1 partition, cleanup.policy=compact, retention.ms=-1
#   options.spx.gamma-ladder-path.events  1 partition, cleanup.policy=delete,  retention.ms=-1
#
# CREATE-only if absent; VERIFY (fail closed) if present. Never alters an existing topic.
set -euo pipefail

STATE_TOPIC="${KAFKA_LADDER_PATH_STATE_TOPIC:-options.spx.gamma-ladder-path.state}"
EVENTS_TOPIC="${KAFKA_LADDER_PATH_EVENTS_TOPIC:-options.spx.gamma-ladder-path.events}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }

echo "=== ensure-gamma-ladder-path-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
LIST=$(kt --list 2>&1) || { echo "FAIL: cannot list topics: $LIST"; exit 1; }

ensure() { # <topic> <policy compact|delete>
  local topic="$1" policy="$2" parts cfg
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$policy, retention.ms=-1)"
    kt --create --topic "$topic" --partitions 1 --replication-factor "$RF" \
       --config cleanup.policy="$policy" --config retention.ms=-1
    return 0
  fi
  parts=$(kt --describe --topic "$topic" 2>/dev/null \
    | awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}')
  if [ "${parts:-0}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (one key per trade date, total event order)."
    return 1
  fi
  cfg=$(kc --entity-type topics --entity-name "$topic" --describe 2>&1) || { echo "FAIL: cannot describe '$topic': $cfg"; return 1; }
  if ! grep -qE 'retention\.ms=-1' <<<"$cfg"; then
    echo "FAIL: '$topic' retention.ms is not -1 — the forward-test record would expire. Fix the topic, then re-run."
    return 1
  fi
  if [ "$policy" = compact ]; then
    if ! grep -qE 'cleanup\.policy=compact( |$|,)' <<<"$cfg" || grep -qE 'cleanup\.policy=[^ ]*delete' <<<"$cfg"; then
      echo "FAIL: '$topic' must be pure cleanup.policy=compact (one record per trade date)."
      return 1
    fi
  else
    if grep -qE 'cleanup\.policy=[^ ]*compact' <<<"$cfg"; then
      echo "FAIL: '$topic' contains compact — compaction erases the event chronology (events share the trade-date key)."
      return 1
    fi
  fi
  echo "ok: '$topic' shape verified (1 partition, $policy, retention.ms=-1)"
}

rc=0
ensure "$STATE_TOPIC" compact || rc=1
ensure "$EVENTS_TOPIC" delete || rc=1
exit $rc
