#!/usr/bin/env bash
# ensure-line-acceptance-topics.sh — line-acceptance-service's six topics must be right BEFORE the service is ever raised (VWAP-ACCEPTANCE-REJECTION design sections 9.1 and 15;
# the same first-deploy barrier as ensure-break-classification-topics.sh). DEV ONLY and DARK: the service is held at replicas 0 and this barrier runs from service-deploy
# SERVICE=line-acceptance; it refuses to run unless ENVIRONMENT is exactly "dev" (Jenkinsfile.service-deploy exports the job parameter).
#
#   NO BROKER-WIDE CHANGE. This barrier never reads or alters a broker config. The checkpoint and journal topics carry their own TOPIC-LEVEL max.message.bytes of 9437184.
#   options.spx.line-acceptance             1 partition, delete,  retention.ms EXACTLY 604800000 (7 days)
#   options.spx.line-acceptance.monitor     1 partition, compact, retention.ms -1 at creation (not judged on an existing topic)
#   options.spx.line-acceptance.status      1 partition, delete,  retention.ms EXACTLY -1: NOT compacted, no time limit (the K9b CalibrationStatus artifact: every version survives)
#   options.spx.line-acceptance.checkpoint  1 partition, compact, TOPIC-LEVEL max.message.bytes >= 8388608 (created at 9437184; an existing one whose topic-level value is absent or below
#                                           8388608 is raised to 9437184 on this topic only, never lowered); gets the recovery-topic compaction cadence after verification
#   options.spx.line-acceptance.journal     1 partition, delete,  retention.ms EXACTLY 604800000, TOPIC-LEVEL max.message.bytes >= 8388608 (same pin as the checkpoint)
#   options.spx.line-acceptance.dlq         1 partition, delete,  retention.ms EXACTLY 604800000
# An existing topic is never recreated or shrunk: any difference fails closed. The input topics (options.indicators.bars, es.options.indicators.snapshot.current, es.futures.footprint.bars,
# options.spx.market-structure.{tools,geometry,patterns,break-events}) belong to other services and are not created here. No tombstones, no versioned names.
# dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env (including max.message.bytes), and this barrier is the check that
# a deploy runs before the service is raised.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-line-acceptance-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); line-acceptance has no production topics — refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${LINE_ACCEPTANCE_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${LINE_ACCEPTANCE_MIN_INSYNC_REPLICAS:-1}"
MIN_LARGE_BYTES=8388608
LARGE_BYTES=9437184
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-line-acceptance-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

OUT="${KAFKA_LINE_ACCEPTANCE_TOPIC:-options.spx.line-acceptance}"
MONITOR="${KAFKA_LINE_ACCEPTANCE_MONITOR_TOPIC:-options.spx.line-acceptance.monitor}"
STATUS="${KAFKA_LINE_ACCEPTANCE_STATUS_TOPIC:-options.spx.line-acceptance.status}"
CHECKPOINT="${KAFKA_LINE_ACCEPTANCE_CHECKPOINT_TOPIC:-options.spx.line-acceptance.checkpoint}"
JOURNAL="${KAFKA_LINE_ACCEPTANCE_JOURNAL_TOPIC:-options.spx.line-acceptance.journal}"
DLQ="${KAFKA_LINE_ACCEPTANCE_DLQ_TOPIC:-options.spx.line-acceptance.dlq}"
rc=0
# An existing large-record topic must carry its own topic-level limit: the plain (non --all) describe lists only the topic's own overrides, so a value inherited from the broker
# default reads as absent here and is pinned, which keeps the topic correct whatever the broker default is or later becomes. Raise only; a larger value is left alone.
for t in "$CHECKPOINT" "$JOURNAL"; do
  if grep -qxF "$t" <<<"$LIST"; then
    if ! own=$(kc --entity-type topics --entity-name "$t" --describe 2>&1); then
      echo "FAIL: could not read the config of '$t':"; printf '%s\n' "$own"; rc=1
    else
      own_bytes=$(extract 'max\.message\.bytes' "$own")
      case "${own_bytes:-}" in
        ''|*[!0-9]*) own_bytes=0 ;;
      esac
      if [ "$own_bytes" -lt "$MIN_LARGE_BYTES" ]; then
        echo "pinning '$t' topic-level max.message.bytes=$LARGE_BYTES (topic-level value was '${own_bytes}'; this topic only, no broker config is touched)"
        if ! out=$(kc --entity-type topics --entity-name "$t" --alter --add-config "max.message.bytes=$LARGE_BYTES" 2>&1); then
          echo "FAIL: could not set max.message.bytes on '$t':"; printf '%s\n' "$out"; rc=1
        fi
      fi
    fi
  fi
done
if [ "$rc" -ne 0 ]; then   # fail-stop: nothing else is created or altered after a failed read/pin of a record limit
  echo "=== ensure-line-acceptance-topics FAILED — the service must not be raised ==="
  exit 1
fi
ensure_topic "$OUT"        delete  exact:604800000 1048576 || rc=1
ensure_topic "$MONITOR"    compact any:-1          1048576 || rc=1
ensure_topic "$STATUS"     delete  exact:-1        1048576 || rc=1
ensure_topic "$CHECKPOINT" compact any:-1          "$MIN_LARGE_BYTES" yes "$LARGE_BYTES" || rc=1
ensure_topic "$JOURNAL"    delete  exact:604800000 "$MIN_LARGE_BYTES" yes "$LARGE_BYTES" || rc=1
ensure_topic "$DLQ"        delete  exact:604800000 1048576 || rc=1
# The checkpoint is rewritten in every transaction: the small-segment compaction cadence of the other recovery topics (topic-shape.lib.sh topic_is_recovery_checkpoint),
# reconciled after verification because a topic the service or a reset created has the broker defaults (1 GiB segments, which compaction never cleans while active).
if [ "$rc" -eq 0 ]; then
  if ! out=$(kc --entity-type topics --entity-name "$CHECKPOINT" --alter --add-config segment.bytes=16777216,max.compaction.lag.ms=300000 2>&1); then
    echo "FAIL: could not configure checkpoint compaction for '$CHECKPOINT':"; printf '%s\n' "$out"; rc=1
  fi
fi
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-line-acceptance-topics FAILED — the service must not be raised ==="
  exit 1
fi
echo "=== ensure-line-acceptance-topics OK ==="
