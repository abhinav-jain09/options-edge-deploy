#!/usr/bin/env bash
# ensure-break-classification-topics.sh — break-classifier-service's topics must be right BEFORE the service is scaled up
# (the same first-deploy barrier as ensure-wave-cycle-topics.sh; Phase F). The service never creates a topic and its boot verification (BrokerGuard.verifyTopology)
# refuses to start (FAILED_CONTRACT) on any difference below.
#
#   NO BROKER-WIDE CHANGE (revision 2026-10-30). This barrier never reads or alters a broker config. It used to raise the cluster-wide dynamic default
#   message.max.bytes to 9437184, which pushed every inheriting topic's effective limit above 1 MiB and crash-looped the dev feed gateway (its ceiling check on the
#   es.futures.footprint* topics). The service now checks its OWN checkpoint topic's effective max.message.bytes, and Kafka accepts a topic-level max.message.bytes
#   larger than the broker default, so the checkpoint topic carries 9437184 as a TOPIC-LEVEL override and nothing else changes.
#   options.spx.break-classification             1 partition, delete, retention.ms EXACTLY 7776000000 (BreakMark / BreakOutcome / BreakStatus)
#   options.spx.break-classification.checkpoint  1 partition, compact, TOPIC-LEVEL max.message.bytes >= 8388608 (created at 9437184; an existing checkpoint whose topic-level
#                                                value is absent or below 8388608 is raised to 9437184 on this topic only, never lowered); created at retention.ms=-1, not
#                                                judged on an existing topic; gets the recovery-topic compaction cadence after verification (it is rewritten in every transaction)
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
MIN_CHECKPOINT_BYTES=8388608
CHECKPOINT_BYTES=9437184
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-break-classification-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

CHECKPOINT="${KAFKA_BREAK_CLASSIFICATION_CHECKPOINT_TOPIC:-options.spx.break-classification.checkpoint}"
rc=0
# An existing checkpoint must carry its own topic-level limit: the plain (non --all) describe lists only the topic's own overrides, so a value inherited from the broker
# default reads as absent here and is pinned, which keeps the topic correct whatever the broker default is or later becomes. Raise only; a larger value is left alone.
if grep -qxF "$CHECKPOINT" <<<"$LIST"; then
  if ! own=$(kc --entity-type topics --entity-name "$CHECKPOINT" --describe 2>&1); then
    echo "FAIL: could not read the config of '$CHECKPOINT':"; printf '%s\n' "$own"; rc=1
  else
    own_bytes=$(extract 'max\.message\.bytes' "$own")
    case "${own_bytes:-}" in
      ''|*[!0-9]*) own_bytes=0 ;;
    esac
    if [ "$rc" -eq 0 ] && [ "$own_bytes" -lt "$MIN_CHECKPOINT_BYTES" ]; then
      echo "pinning '$CHECKPOINT' topic-level max.message.bytes=$CHECKPOINT_BYTES (topic-level value was '${own_bytes}'; this topic only, no broker config is touched)"
      if ! out=$(kc --entity-type topics --entity-name "$CHECKPOINT" --alter --add-config "max.message.bytes=$CHECKPOINT_BYTES" 2>&1); then
        echo "FAIL: could not set max.message.bytes on '$CHECKPOINT':"; printf '%s\n' "$out"; rc=1
      fi
    fi
  fi
  if [ "$rc" -ne 0 ]; then   # fail-stop: nothing else is created or altered after a failed read/pin of the checkpoint limit
    echo "=== ensure-break-classification-topics FAILED — the service must not be scaled up ==="
    exit 1
  fi
fi
ensure_topic "${KAFKA_BREAK_CLASSIFICATION_TOPIC:-options.spx.break-classification}"      delete  exact:7776000000 1048576 || rc=1
ensure_topic "$CHECKPOINT"                                                               compact any:-1           "$MIN_CHECKPOINT_BYTES" yes "$CHECKPOINT_BYTES" || rc=1
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
