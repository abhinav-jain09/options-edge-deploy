#!/usr/bin/env bash
# create-es-topics.sh — explicit creation of the CORE es.* topics on the es4 broker.
#
# Policy: 4 partitions (org policy), RF=1 (single node), 12h retention (org policy —
# also the broker default here; stated explicitly on each core topic so a broker-default
# drift can never silently change them). auto.create.topics.enable=true covers the long
# tail (org pattern: gateway-safe-because-broker-autocreate), but the hot pipeline topics
# are created explicitly and idempotently here.
#
# Naming: every ES topic carries the es. prefix (Gate-2 G6). Consumers get the prefix
# automatically via TOPIC_PREFIX=es. (processing-common RuntimeProfileConfig), so these
# names are <es.> + the services' compiled defaults — e.g. the tcbbo topic keeps its
# legacy "opra" token because that is the consumers' default (cosmetic, documented).
#
# Runs ON the es4 box (invoked by bootstrap-es4.sh or the es4-deploy Jenkins job).

set -euo pipefail

BROKER=localhost:29092   # in-container listener via docker exec
PARTITIONS=4
RETENTION_MS=43200000    # 12h

TOPICS=(
  # feed outputs (es-feed on .252 produces these)
  es.options.databento.raw
  es.options.databento.events.raw
  es.options.opra.tcbbo
  es.options.hpsf.dlq
  es.options.databento.control
  es.options.marketdata.selection
  # mirrored from prod by MM2 (created by MM2 too; explicit here for config pinning)
  es.underlying.es.trades
  # processing pipeline
  es.options.databento.strike-flow
  es.options.databento.strike-flow.strike.avro
  es.options.databento.gex.strike
  es.options.databento.gex.history
  es.options.databento.pace
  es.options.databento.pace.mission
  es.options.databento.market-pressure.mission
  es.options.databento.sandwich.mission
  es.options.databento.directional-pressure
  es.options.databento.maxpain
  es.display
  es.display.volume.current
  es.display.volume.sandwich.current
  es.display.volume.sandwich.alerts
  es.options.databento.normalized
  es.options.databento.volume.state.compacted
)

created=0; existed=0
for t in "${TOPICS[@]}"; do
  if docker exec es4-kafka kafka-topics --bootstrap-server "$BROKER" --describe --topic "$t" >/dev/null 2>&1; then
    existed=$((existed+1))
  else
    docker exec es4-kafka kafka-topics --bootstrap-server "$BROKER" --create --topic "$t" \
      --partitions "$PARTITIONS" --replication-factor 1 \
      --config "retention.ms=$RETENTION_MS" >/dev/null
    echo "created $t"
    created=$((created+1))
  fi
done
echo "topics: created=$created existed=$existed total=${#TOPICS[@]}"
