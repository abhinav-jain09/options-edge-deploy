#!/usr/bin/env bash
# ensure-es-trade-tools-topics.sh — es-trade-tools-service's seven Avro output topics must EXIST with their declared shape BEFORE the
# service is scaled up (the same first-deploy barrier as ensure-market-structure-topics.sh).
#
# The service creates an absent topic itself and verifies an existing one, refusing to run (FAILED_CONTRACT:OUTPUT_TOPIC_CONTRACT_VIOLATION) on any
# difference (PHASE-D D-R13; KafkaBrokerGuard.ensureOutputTopics). On dev the broker auto-creates at 32 partitions and a topic created there
# first is refused for ever, so the topics are ensured HERE, from the same declaration scripts/kafka/topics.env carries, before any scale-up.
# One partition each; retention.ms is compared for EQUALITY by the service, so the compact topics are -1 and the 90-day ones 7776000000:
#   futures.pf.columns           compact          -1            largest record 2 KiB
#   futures.pf.chart             compact          -1            16 KiB
#   futures.pf.events            compact,delete   7776000000    4 KiB
#   futures.volume-nodes         compact          -1            16 KiB
#   futures.flow.bars            compact,delete   7776000000    4 KiB
#   futures.flow.events          compact,delete   7776000000    2 KiB
#   futures.tradetools.state     compact          -1            512 KiB (a checkpoint chunk)
# max.message.bytes must be >= the largest record + 4096 (the service's own SIZE_BOUND), and the service's producer limit is 1 MiB, so a topic
# is CREATED at 1 048 576. All values are Avro (subjects com.optionsedge.contracts.tradetools.*, registered by ensure-fixed-id-schemas.sh).
# The topics are never versioned and never recreated here: Kafka cannot shrink a partition count, and a topic holding chart state is not
# something a barrier may destroy. A difference fails closed.
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${ES_TRADE_TOOLS_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${ES_TRADE_TOOLS_MIN_INSYNC_REPLICAS:-1}"
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"          # extract(): the ONE tested config parser
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"   # ensure_topic()

DAYS90=7776000000   # 90 * 24 * 60 * 60 * 1000, TradeToolsSettings.RETENTION_90_DAYS_MS
echo "=== ensure-es-trade-tools-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

rc=0
ensure_topic "${KAFKA_TT_PF_COLUMNS_TOPIC:-futures.pf.columns}"          compact        exact:-1        6144   || rc=1
ensure_topic "${KAFKA_TT_PF_CHART_TOPIC:-futures.pf.chart}"              compact        exact:-1        20480  || rc=1
ensure_topic "${KAFKA_TT_PF_EVENTS_TOPIC:-futures.pf.events}"            compact,delete "exact:$DAYS90" 8192   || rc=1
ensure_topic "${KAFKA_TT_VOLUME_NODES_TOPIC:-futures.volume-nodes}"      compact        exact:-1        20480  || rc=1
ensure_topic "${KAFKA_TT_FLOW_BARS_TOPIC:-futures.flow.bars}"            compact,delete "exact:$DAYS90" 8192   || rc=1
ensure_topic "${KAFKA_TT_FLOW_EVENTS_TOPIC:-futures.flow.events}"        compact,delete "exact:$DAYS90" 6144   || rc=1
ensure_topic "${KAFKA_TT_STATE_TOPIC:-futures.tradetools.state}"         compact        exact:-1        528384 || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-es-trade-tools-topics FAILED — the service must not be scaled up ==="
  exit 1
fi
echo "=== ensure-es-trade-tools-topics OK ==="
