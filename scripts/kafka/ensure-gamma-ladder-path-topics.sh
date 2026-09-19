#!/usr/bin/env bash
# ensure-gamma-ladder-path-topics.sh — gamma-ladder-path-service's two output topics must EXIST with their declared
# shape BEFORE the service rolls out (the same barrier as ensure-oi-anchor-topic / ensure-multileg-topics).
#
# Why: apply-topics lives in a different job. On a FIRST deploy neither topic exists, and the broker either
# auto-creates them with cluster defaults (state NOT compacted, time retention — the forward-test record then
# expires) or, with auto-create off, the Streams producer fails and the pod crash-loops. Declared in
# scripts/kafka/topics.env (both reset-preserved, retention -1):
#   options.spx.gamma-ladder-path.state   1 partition, cleanup.policy=compact, retention.ms=-1
#   options.spx.gamma-ladder-path.events  1 partition, cleanup.policy=delete,  retention.ms=-1
#
# CREATE if absent, then VERIFY every topic — including one just created — and fail closed. Never alters an
# existing topic. Every CLI call's status is checked explicitly: ensure() runs in an `if` context, where bash
# ignores `set -e`, so nothing here may rely on errexit (Codex PR #1071 r1).
set -euo pipefail

STATE_TOPIC="${KAFKA_LADDER_PATH_STATE_TOPIC:-options.spx.gamma-ladder-path.state}"
EVENTS_TOPIC="${KAFKA_LADDER_PATH_EVENTS_TOPIC:-options.spx.gamma-ladder-path.events}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"   # extract(): the ONE tested config parser

echo "=== ensure-gamma-ladder-path-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

ensure() { # <topic> <exact cleanup.policy: compact|delete>
  local topic="$1" want="$2" out desc parts cfg policy retention
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$want, retention.ms=-1)"
    if ! out=$(kt --create --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy="$want" --config retention.ms=-1 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (one key per trade date, total event order)."
    return 1
  fi
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  # extract() takes the FIRST whitespace/comma-bounded key=value, i.e. the topic's own value, never a
  # synonyms={... DEFAULT_CONFIG:log.cleanup.policy=...} entry; exact equality rejects "compact,delete".
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  if [ "${policy:-}" != "$want" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly '$want'."
    return 1
  fi
  if [ "${retention:-}" != "-1" ]; then
    echo "FAIL: '$topic' has retention.ms='${retention:-<unset, inherits the cluster default>}', expected -1."
    return 1
  fi
  echo "ok: '$topic' — partitions=1 cleanup.policy=$want retention.ms=-1"
}

rc=0
if ! ensure "$STATE_TOPIC" compact; then rc=1; fi
if ! ensure "$EVENTS_TOPIC" delete; then rc=1; fi
exit $rc
