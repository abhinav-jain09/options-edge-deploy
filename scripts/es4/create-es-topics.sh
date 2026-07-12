#!/usr/bin/env bash
# create-es-topics.sh — explicit creation + RECONCILIATION of the core es.* topics
# on the es4 broker.
#
# Policy: 4 partitions (org policy), RF=1 (single node). Cleanup semantics come from
# the org SSOT scripts/kafka/topics.env: topics listed there as COMPACTED get
# cleanup.policy=compact; everything else gets delete + 12h retention (org policy).
# Existing topics are RECONCILED (kafka-configs --alter is idempotent), so
# retention/cleanup drift is repaired on every run — creation is not the only guarantee.
#
# Naming: every ES topic carries the es. prefix (Gate-2 G6). Consumers get the prefix
# via TOPIC_PREFIX=es. — these names are <es.> + the values the GENERATED manifests
# carry (k8s/es4/services/*.yaml). Keep in sync when the renderer changes. Broker
# auto-create covers stragglers, but everything the manifests reference is listed here
# so its config is pinned.
#
# Runs ON the es4 box (invoked by bootstrap-es4.sh or the es4-deploy Jenkins job).

set -euo pipefail

BROKER=localhost:29092   # in-container listener via docker exec
PARTITIONS=4
RETENTION_MS=43200000    # 12h

# delete-cleanup topics (12h retention)
TOPICS_DELETE=(
  # feed outputs (es-feed on .252 produces these)
  es.options.databento.raw
  es.options.databento.events.raw
  es.options.opra.tcbbo
  es.options.hpsf.dlq
  es.options.databento.control
  es.options.marketdata.selection
  # mirrored from prod by MM2 (also created by MM2; pinned here for explicit config)
  es.underlying.es.trades
  # processing pipeline
  es.options.databento.strike-flow
  es.options.databento.strike-flow.strike.avro
  es.options.databento.gex.strike
  es.options.databento.gex.strike.history
  es.options.databento.pace
  es.options.databento.directional-pressure
  es.options.databento.display
  es.options.databento.display.volume.current
  es.options.databento.display.gamma.history
  es.options.databento.normalized
  es.delta-flow-session
  es.dealer-ledger-profile
  es.dealer-ledger-state
  es.dealer-ledger-signal-fired
  es.dealer-ledger-outcome-scored
  es.option-price-behavior-v2-by-option
  es.option-price-behavior-v2-session
  es.strike-liquidity-heatmap-dashboard
)

# compacted topics (per scripts/kafka/topics.env OPTIONS_EDGE_COMPACTED_TOPICS,
# plus the compacted-by-design calibration state)
TOPICS_COMPACT=(
  es.options.databento.pace.mission
  es.options.databento.market-pressure.mission
  es.options.databento.sandwich.mission
  es.options.databento.volume.state.compacted
  es.options.databento.maxpain
  es.dealer-ledger-calibration-state
)

kt() { docker exec es4-kafka kafka-topics --bootstrap-server "$BROKER" "$@"; }
kc() { docker exec es4-kafka kafka-configs --bootstrap-server "$BROKER" "$@"; }

ensure_topic() { # name cleanup(compact|delete)
  local t=$1 cleanup=$2 created=no
  if ! kt --describe --topic "$t" >/dev/null 2>&1; then
    kt --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
    created=yes
  fi
  # reconcile configs idempotently (repairs drift on every run)
  if [ "$cleanup" = compact ]; then
    kc --alter --entity-type topics --entity-name "$t" \
       --add-config "cleanup.policy=compact" >/dev/null
  else
    kc --alter --entity-type topics --entity-name "$t" \
       --add-config "cleanup.policy=delete,retention.ms=${RETENTION_MS}" >/dev/null
  fi
  # partitions can only grow; warn loudly on drift below policy
  local parts
  parts=$(kt --describe --topic "$t" 2>/dev/null | awk -F'PartitionCount: ' 'NR==1{print $2+0}')
  if [ "${parts:-0}" -lt "$PARTITIONS" ]; then
    echo "WARN: $t has $parts partitions (<$PARTITIONS) — raising (fail-closed)"
    kt --alter --topic "$t" --partitions "$PARTITIONS" >/dev/null
  fi
  echo "$t cleanup=$cleanup created=$created"
}

for t in "${TOPICS_DELETE[@]}";  do ensure_topic "$t" delete;  done
for t in "${TOPICS_COMPACT[@]}"; do ensure_topic "$t" compact; done
echo "topics reconciled: $(( ${#TOPICS_DELETE[@]} + ${#TOPICS_COMPACT[@]} ))"
