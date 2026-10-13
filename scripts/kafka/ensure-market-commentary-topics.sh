#!/usr/bin/env bash
# ensure-market-commentary-topics.sh — market-commentary-service's output and recovery topics must EXIST with their
# declared shape BEFORE the service rolls out (the same barrier as ensure-hedging-pressure-topics).
#
# Why: the service never relies on auto-creation, and its own boot verification refuses readiness until every
# topic matches (existence, exactly one partition, cleanup.policy=delete, retention.ms at or above the declared value).
# apply-topics lives in a different job, so on a FIRST deploy none of the topics exist; a broker that auto-creates would give
# them cluster defaults (dev runs num.partitions=32) and the service would sit not-ready with a partition-count failure.
# Declared in scripts/kafka/topics.env (all four rebuildable from the input topics; none durable):
#   options.spx.market-commentary.minute      1 partition, delete, retention.ms=10368000000 (120 days)
#   options.spx.market-commentary.current     1 partition, delete, retention.ms=172800000 (2 days)
#   options.spx.market-commentary.events      1 partition, delete, retention.ms=10368000000 (120 days)
#   options.spx.market-commentary.rejections  1 partition, delete, retention.ms=10368000000 (120 days, at least the minute topic)
#
# CREATE if absent, then VERIFY every topic — including one just created — and fail closed. Never alters an
# existing topic's identity, policy or retention. Every CLI call's status is checked explicitly: ensure() runs in an `if`
# context, where bash ignores `set -e`, so nothing here may rely on errexit.
set -euo pipefail

MINUTE_TOPIC="${KAFKA_MARKET_COMMENTARY_MINUTE_TOPIC:-options.spx.market-commentary.minute}"
CURRENT_TOPIC="${KAFKA_MARKET_COMMENTARY_CURRENT_TOPIC:-options.spx.market-commentary.current}"
EVENTS_TOPIC="${KAFKA_MARKET_COMMENTARY_EVENTS_TOPIC:-options.spx.market-commentary.events}"
REJECTIONS_TOPIC="${KAFKA_MARKET_COMMENTARY_REJECTIONS_TOPIC:-options.spx.market-commentary.rejections}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
# The two minima the SERVICE verifies, which its own deployment freezes (1 on this single-broker estate).
MIN_RF="${MARKET_COMMENTARY_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${MARKET_COMMENTARY_MIN_INSYNC_REPLICAS:-1}"
MIN_MAX_MESSAGE_BYTES=1048576

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"   # extract(): the ONE tested config parser

echo "=== ensure-market-commentary-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

ensure() { # <topic> <retention.ms floor> [cleanup policy]
  local topic="$1" wantRet="$2" wantPolicy="${3:-delete}" out desc parts rf cfg policy retention maxBytes isr
  local extra=()
  if [ "$wantPolicy" = compact ]; then
    extra=(--config segment.bytes=16777216 --config max.compaction.lag.ms=300000)
  fi
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$wantPolicy, retention.ms=$wantRet)"
    if ! out=$(kt --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy="$wantPolicy" --config retention.ms="$wantRet" \
                  --config max.message.bytes="$MIN_MAX_MESSAGE_BYTES" --config min.insync.replicas="$MIN_ISR" ${extra[@]+"${extra[@]}"} 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (the service's boot verification requires exactly 1, and one partition is what gives readers a total order)."
    return 1
  fi
  rf=$(awk '{for (i = 1; i < NF; i++) if ($i == "ReplicationFactor:") { print $(i + 1); exit }}' <<<"$desc")
  case "${rf:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports ReplicationFactor='${rf:-unknown}', which is not a number."; return 1 ;;
  esac
  if [ "$rf" -lt "$MIN_RF" ]; then
    echo "FAIL: '$topic' has ReplicationFactor=$rf, expected >= $MIN_RF (the service verifies the same minimum)."
    return 1
  fi
  # --all, not the dynamic view: these are the EFFECTIVE values the service's own describeConfigs sees.
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  # extract() takes the FIRST whitespace/comma-bounded key=value, i.e. the topic's own value, never a
  # synonyms={... DEFAULT_CONFIG:log.cleanup.policy=...} entry; exact equality rejects "compact,delete".
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  if [ "${policy:-}" != "$wantPolicy" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly '$wantPolicy'."
    return 1
  fi
  # At or above the floor: R13's requirement is a floor, and -1 (infinite) satisfies any floor. A SHORTER
  # retention is refused, because the service rebuilds a session by replaying and cannot replay what was deleted.
  case "${retention:-}" in
    ''|*[!0-9-]*) echo "FAIL: '$topic' reports retention.ms='${retention:-<unreadable>}', expected >= $wantRet."; return 1 ;;
  esac
  if [ "$retention" != "-1" ] && { [ "$wantRet" = "-1" ] || [ "$retention" -lt "$wantRet" ]; }; then
    echo "FAIL: '$topic' has retention.ms=$retention, expected >= $wantRet (or -1)."
    return 1
  fi
  maxBytes=$(extract 'max\.message\.bytes' "$cfg")
  case "${maxBytes:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports max.message.bytes='${maxBytes:-<unreadable>}', expected >= $MIN_MAX_MESSAGE_BYTES."; return 1 ;;
  esac
  if [ "$maxBytes" -lt "$MIN_MAX_MESSAGE_BYTES" ]; then
    echo "FAIL: '$topic' has max.message.bytes=$maxBytes, expected >= $MIN_MAX_MESSAGE_BYTES (one input record's bound)."
    return 1
  fi
  isr=$(extract 'min\.insync\.replicas' "$cfg")
  case "${isr:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports min.insync.replicas='${isr:-<unreadable>}', expected >= $MIN_ISR."; return 1 ;;
  esac
  if [ "$isr" -lt "$MIN_ISR" ]; then
    echo "FAIL: '$topic' has min.insync.replicas=$isr, expected >= $MIN_ISR (the service verifies the same minimum)."
    return 1
  fi
  # A reset can recreate this topic through apply-topics with broker compaction defaults.
  # Reconcile cadence here on every sanctioned deploy, after identity/policy/retention verification.
  if [ "$wantPolicy" = compact ]; then
    if ! out=$(kc --entity-type topics --entity-name "$topic" --alter \
        --add-config segment.bytes=16777216,max.compaction.lag.ms=300000 2>&1); then
      echo "FAIL: could not configure recovery compaction for '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  echo "ok: '$topic' — partitions=1 rf=$rf cleanup.policy=$wantPolicy retention.ms=$retention (floor $wantRet) max.message.bytes=$maxBytes min.insync.replicas=$isr"
}

rc=0
if ! ensure "$MINUTE_TOPIC" 10368000000; then rc=1; fi
if ! ensure "$CURRENT_TOPIC" 172800000; then rc=1; fi
if ! ensure "$EVENTS_TOPIC" 10368000000; then rc=1; fi
if ! ensure "$REJECTIONS_TOPIC" 10368000000; then rc=1; fi
exit $rc
