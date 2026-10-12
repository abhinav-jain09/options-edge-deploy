#!/usr/bin/env bash
# ensure-market-structure-topics.sh — market-structure-service's three output topics must EXIST with their
# declared shape BEFORE the service rolls out (the same barrier as ensure-hedging-pressure-topics).
#
# Why: the service never relies on auto-creation, and its own R13 boot verification refuses readiness until every
# topic matches — existence, the exact partition count, cleanup.policy=delete, retention.ms at or above the floor and
# max.message.bytes >= 1 048 576. apply-topics lives in a different job, so on a FIRST deploy none of the three
# exists; a broker that auto-creates would give them cluster defaults (dev runs num.partitions=32), and the service
# would then sit not-ready with a partition-count failure. Declared in scripts/kafka/topics.env:
#   options.spx.market-structure.ui        1 partition, delete, retention.ms=604800000 (1 week)
#   options.spx.market-structure.sequence  1 partition, delete, retention.ms=172800000 (2 sessions)
#   options.spx.market-structure.dlq       1 partition, delete, retention.ms=604800000 (1 week)
#
# CREATE if absent, then VERIFY every topic — including one just created — and fail closed. Never alters an
# existing topic. Every CLI call's status is checked explicitly: ensure() runs in an `if` context, where bash
# ignores `set -e`, so nothing here may rely on errexit.
set -euo pipefail

UI_TOPIC="${KAFKA_MARKET_STRUCTURE_UI_TOPIC:-options.spx.market-structure.ui}"
SEQUENCE_TOPIC="${KAFKA_MARKET_STRUCTURE_SEQUENCE_TOPIC:-options.spx.market-structure.sequence}"
DLQ_TOPIC="${KAFKA_MARKET_STRUCTURE_DLQ_TOPIC:-options.spx.market-structure.dlq}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"   # extract(): the ONE tested config parser

echo "=== ensure-market-structure-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

ensure() { # <topic> <exact retention.ms>
  local topic="$1" wantRet="$2" out desc parts cfg policy retention
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=$wantRet)"
    if ! out=$(kt --create --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy=delete --config retention.ms="$wantRet" 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (the service's boot verification requires exactly 1, and one partition is what gives the page and the sequence readers a total order)."
    return 1
  fi
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  # extract() takes the FIRST whitespace/comma-bounded key=value, i.e. the topic's own value, never a
  # synonyms={... DEFAULT_CONFIG:log.cleanup.policy=...} entry; exact equality rejects "compact,delete".
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  if [ "${policy:-}" != "delete" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly 'delete'."
    return 1
  fi
  # At or above the floor: R13's requirement is a floor, and -1 (infinite) satisfies any floor. A SHORTER
  # retention is refused, because the service rebuilds a session by replaying and cannot replay what was deleted.
  if [ "${retention:-}" = "" ]; then
    echo "FAIL: '$topic' has no retention.ms of its own (it inherits the cluster default), expected >= $wantRet."
    return 1
  fi
  if [ "$retention" != "-1" ] && [ "$retention" -lt "$wantRet" ]; then
    echo "FAIL: '$topic' has retention.ms=$retention, expected >= $wantRet (or -1)."
    return 1
  fi
  echo "ok: '$topic' — partitions=1 cleanup.policy=delete retention.ms=$retention (floor $wantRet)"
}

rc=0
if ! ensure "$UI_TOPIC" 604800000; then rc=1; fi
if ! ensure "$SEQUENCE_TOPIC" 172800000; then rc=1; fi
if ! ensure "$DLQ_TOPIC" 604800000; then rc=1; fi
exit $rc
