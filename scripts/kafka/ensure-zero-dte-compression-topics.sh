#!/usr/bin/env bash
# Create and verify the three zero-DTE compression shadow topics before Context Tape rolls out.
# The standalone service deploy does not run the fleet-wide topic reconciler, so a first rollout
# must establish these topics explicitly rather than relying on broker auto-creation.
set -euo pipefail

CURRENT_TOPIC="${ZERO_DTE_CURRENT_TOPIC:-context-tape.compression.current}"
HISTORY_TOPIC="${ZERO_DTE_HISTORY_TOPIC:-context-tape.compression.history}"
CHECKPOINT_TOPIC="${ZERO_DTE_CHECKPOINT_TOPIC:-context-tape.compression.checkpoint}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"

echo "=== ensure-zero-dte-compression-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"
  printf '%s\n' "$LIST"
  exit 1
fi

ensure() { # <topic> <exact cleanup.policy: compact|delete>
  local topic="$1" want="$2" out desc parts cfg policy retention retention_bytes
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$want, retention.ms=-1, retention.bytes=-1)"
    if ! out=$(kt --create --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy="$want" --config retention.ms=-1 \
                  --config retention.bytes=-1 2>&1); then
      echo "FAIL: could not create '$topic':"
      printf '%s\n' "$out"
      return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"
    printf '%s\n' "$desc"
    return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1."
    return 1
  fi
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe 2>&1); then
    echo "FAIL: could not read the config of '$topic':"
    printf '%s\n' "$cfg"
    return 1
  fi
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  retention_bytes=$(extract 'retention\.bytes' "$cfg")
  if [ "${policy:-}" != "$want" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly '$want'."
    return 1
  fi
  if [ "${retention:-}" != "-1" ]; then
    echo "FAIL: '$topic' has retention.ms='${retention:-<unset>}', expected -1."
    return 1
  fi
  if [ "${retention_bytes:-}" != "-1" ]; then
    echo "FAIL: '$topic' has retention.bytes='${retention_bytes:-<unset>}', expected -1."
    return 1
  fi
  echo "ok: '$topic' — partitions=1 cleanup.policy=$want retention.ms=-1 retention.bytes=-1"
}

rc=0
if ! ensure "$CURRENT_TOPIC" compact; then rc=1; fi
if ! ensure "$HISTORY_TOPIC" delete; then rc=1; fi
if ! ensure "$CHECKPOINT_TOPIC" compact; then rc=1; fi
exit "$rc"
