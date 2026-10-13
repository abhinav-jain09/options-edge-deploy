#!/usr/bin/env bash
# ensure-early-auction-verdict-topics.sh — the EAV shadow ledger must EXIST with its declared shape
# BEFORE the service rolls out (the same barrier as ensure-market-structure-topics).
#
# ONE topic:
#   early-auction.verdicts.shadow   1 partition, cleanup.policy=delete, retention.ms=-1 (NEVER
#                                   deleted: the ledger is the service's evidence — predictions,
#                                   outcomes, misses, score traces, terminals — and the promotion
#                                   gate reads ALL of it; EARLY-AUCTION-VERDICT-STAGE1-GATE1.md
#                                   EAV-R0c/EAV-R2a), max.message.bytes >= 1 MiB.
#
# EXACT retention: -1 and nothing else. A finite retention silently deletes evidence the promotion
# gate needs, so "big enough" is not acceptable here the way a floor is for a rebuildable topic.
# EXACT partitions: one partition is the ledger's total order, and dev's broker runs
# num.partitions=32 with auto-create, which would otherwise win the race on a clean.
# CREATE if absent, then VERIFY — including a topic just created — and fail closed. Never alters an
# existing topic. ensure() runs in an `if` context where bash ignores `set -e`; nothing here relies
# on errexit.
set -euo pipefail

LEDGER_TOPIC="${KAFKA_EAV_LEDGER_TOPIC:-early-auction.verdicts.shadow}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${EAV_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${EAV_MIN_INSYNC_REPLICAS:-1}"
MIN_MAX_MESSAGE_BYTES=1048576

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"   # extract(): the ONE tested config parser

echo "=== ensure-early-auction-verdict-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

ensure() { # <topic>
  local topic="$1" out desc parts rf cfg policy retention maxBytes isr
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=-1)"
    if ! out=$(kt --create --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy=delete --config retention.ms=-1 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (the ledger's total order)."
    return 1
  fi
  rf=$(awk '{for (i = 1; i < NF; i++) if ($i == "ReplicationFactor:") { print $(i + 1); exit }}' <<<"$desc")
  case "${rf:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports ReplicationFactor='${rf:-unknown}', which is not a number."; return 1 ;;
  esac
  if [ "$rf" -lt "$MIN_RF" ]; then
    echo "FAIL: '$topic' has ReplicationFactor=$rf, expected >= $MIN_RF."
    return 1
  fi
  # --all: the EFFECTIVE values, which are what the service's own AdminClient describeConfigs sees.
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  if [ "${policy:-}" != "delete" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly 'delete'."
    return 1
  fi
  if [ "${retention:-}" != "-1" ]; then
    echo "FAIL: '$topic' has retention.ms='${retention:-<unreadable>}', expected exactly -1 — a finite retention silently deletes ledger evidence the promotion gate reads."
    return 1
  fi
  maxBytes=$(extract 'max\.message\.bytes' "$cfg")
  case "${maxBytes:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports max.message.bytes='${maxBytes:-<unreadable>}', expected >= $MIN_MAX_MESSAGE_BYTES."; return 1 ;;
  esac
  if [ "$maxBytes" -lt "$MIN_MAX_MESSAGE_BYTES" ]; then
    echo "FAIL: '$topic' has max.message.bytes=$maxBytes, expected >= $MIN_MAX_MESSAGE_BYTES."
    return 1
  fi
  isr=$(extract 'min\.insync\.replicas' "$cfg")
  case "${isr:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports min.insync.replicas='${isr:-<unreadable>}', expected >= $MIN_ISR."; return 1 ;;
  esac
  if [ "$isr" -lt "$MIN_ISR" ]; then
    echo "FAIL: '$topic' has min.insync.replicas=$isr, expected >= $MIN_ISR (the producer runs acks=all)."
    return 1
  fi
  echo "ok: '$topic' — partitions=1 rf=$rf cleanup.policy=delete retention.ms=-1 max.message.bytes=$maxBytes min.insync.replicas=$isr"
}

rc=0
ensure "$LEDGER_TOPIC" || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-early-auction-verdict-topics FAILED — the rollout must not proceed ==="
  exit 1
fi
echo "=== ensure-early-auction-verdict-topics OK ==="
