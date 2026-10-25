#!/usr/bin/env bash
# ensure-break-classification-topics.sh — break-classifier-service's topics AND the broker bound it depends on must be right BEFORE the service is scaled up
# (the same first-deploy barrier as ensure-wave-cycle-topics.sh; Phase F). The service never creates a topic and its boot verification (BrokerGuard.verifyTopology)
# refuses to start (FAILED_CONTRACT) on any difference below.
#
#   BROKER   effective message.max.bytes >= 8388608 on every broker. Below that the barrier RAISES the cluster-wide dynamic default to 9437184
#            (kafka-configs --entity-type brokers --entity-default --alter --add-config message.max.bytes=9437184), idempotently, never lowering, and re-reads every
#            broker: a failed alter or a re-read that is still below the bound fails closed. THIS IS THE ONE ENVIRONMENT-LEVEL EFFECT OF THE ROLLOUT: every topic that
#            inherits the broker default may then carry larger records (no topic is changed by it, and no record grows because of it).
#   options.spx.break-classification             1 partition, delete, retention.ms EXACTLY 7776000000 (BreakMark / BreakOutcome / BreakStatus)
#   options.spx.break-classification.checkpoint  1 partition, compact, max.message.bytes >= 8388608 (created at 9437184); created at retention.ms=-1, not judged on an
#                                                existing topic; gets the recovery-topic compaction cadence after verification (it is rewritten in every transaction)
#   options.spx.market-structure.break-events    1 partition, delete, retention.ms >= 172800000 (X1 BreakParentRecord): created EMPTY, NO producer on main yet (Phase B/C)
#   options.spx.market-structure.swing-pivots    1 partition, delete, retention.ms >= 172800000 (X2 SwingPivotRecord): created EMPTY, NO producer on main yet (Phase B/C)
#   options.spx.market-structure.equal-levels    1 partition, delete, retention.ms >= 172800000 (EqualLevelRecord, contracts 0.44.0; T-5): written by the service ONLY while BREAK_EQUAL_LEVELS_PUBLISH
#                                                is true (off by default); the service verifies it at boot only then. Created here so turning the flag on needs no second barrier.
# The two input topics the service also verifies (options.indicators.bars with BREAK_SPX_BARS_PARTITIONS partitions, es.futures.footprint.bars with 1) belong to other
# services and are not created here. No tombstones, no versioned names.
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${BREAK_CLASSIFIER_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${BREAK_CLASSIFIER_MIN_INSYNC_REPLICAS:-1}"
MIN_BROKER_BYTES=8388608
RAISE_BROKER_BYTES=9437184
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

# broker_bytes_below_bound: prints every broker whose EFFECTIVE message.max.bytes is below the bound ("id=value"), and returns non-zero when a value cannot be read.
broker_bytes_below_bound() {
  local ids id cfg v
  if ! ids=$(kafka-broker-api-versions --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" 2>&1 | sed -n 's/.*(id: \([0-9][0-9]*\).*/\1/p' | sort -un) || [ -z "$ids" ]; then
    echo "FAIL: cannot list the brokers of $KAFKA_BOOTSTRAP_SERVERS" >&2; return 1
  fi
  for id in $ids; do
    if ! cfg=$(kc --entity-type brokers --entity-name "$id" --describe --all 2>&1); then
      echo "FAIL: could not read the config of broker $id:" >&2; printf '%s\n' "$cfg" >&2; return 1
    fi
    v=$(extract 'message\.max\.bytes' "$cfg")
    case "${v:-}" in
      ''|*[!0-9]*) echo "FAIL: broker $id reports message.max.bytes='${v:-<unreadable>}'" >&2; return 1 ;;
    esac
    if [ "$v" -lt "$MIN_BROKER_BYTES" ]; then echo "$id=$v"; fi
  done
}

echo "=== ensure-break-classification-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

if ! low=$(broker_bytes_below_bound); then
  echo "=== ensure-break-classification-topics FAILED — the service must not be scaled up ==="; exit 1
fi
if [ -n "$low" ]; then
  echo "raising the cluster-wide default message.max.bytes to $RAISE_BROKER_BYTES (below $MIN_BROKER_BYTES on: $(echo $low))"
  if ! out=$(kc --entity-type brokers --entity-default --alter --add-config "message.max.bytes=$RAISE_BROKER_BYTES" 2>&1); then
    echo "FAIL: could not raise message.max.bytes:"; printf '%s\n' "$out"
    echo "=== ensure-break-classification-topics FAILED — the service must not be scaled up ==="; exit 1
  fi
  if ! low=$(broker_bytes_below_bound) || [ -n "$low" ]; then
    echo "FAIL: message.max.bytes is still below $MIN_BROKER_BYTES after the raise: $(echo $low) (a per-broker override wins over the cluster default and is never touched here)."
    echo "=== ensure-break-classification-topics FAILED — the service must not be scaled up ==="; exit 1
  fi
  echo "ok: every broker reports message.max.bytes >= $MIN_BROKER_BYTES after the raise"
else
  echo "ok: every broker already reports message.max.bytes >= $MIN_BROKER_BYTES (nothing changed)"
fi

CHECKPOINT="${KAFKA_BREAK_CLASSIFICATION_CHECKPOINT_TOPIC:-options.spx.break-classification.checkpoint}"
rc=0
ensure_topic "${KAFKA_BREAK_CLASSIFICATION_TOPIC:-options.spx.break-classification}"      delete  exact:7776000000 1048576 || rc=1
ensure_topic "$CHECKPOINT"                                                               compact any:-1           8388608 yes "$RAISE_BROKER_BYTES" || rc=1
ensure_topic "${BREAK_PARENT_TOPIC:-options.spx.market-structure.break-events}"          delete  floor:172800000  1048576 || rc=1
ensure_topic "${BREAK_PIVOT_TOPIC:-options.spx.market-structure.swing-pivots}"           delete  floor:172800000  1048576 || rc=1
ensure_topic "${BREAK_EQUAL_LEVELS_TOPIC:-options.spx.market-structure.equal-levels}"      delete  floor:172800000  1048576 || rc=1
# The checkpoint is rewritten in every transaction: the small-segment compaction cadence of the other recovery topics (topic-shape.lib.sh topic_is_recovery_checkpoint),
# reconciled after verification because a topic the service or a reset created has the broker defaults (1 GiB segments, which compaction never cleans while active).
if [ "$rc" -eq 0 ]; then
  if ! out=$(kc --entity-type topics --entity-name "$CHECKPOINT" --alter --add-config segment.bytes=16777216,max.compaction.lag.ms=300000 2>&1); then
    echo "FAIL: could not configure checkpoint compaction for '$CHECKPOINT':"; printf '%s\n' "$out"; rc=1
  fi
fi
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-break-classification-topics FAILED — the service must not be scaled up ==="
  exit 1
fi
echo "=== ensure-break-classification-topics OK ==="
