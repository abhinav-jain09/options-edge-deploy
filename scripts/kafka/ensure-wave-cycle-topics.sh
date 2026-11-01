#!/usr/bin/env bash
# ensure-wave-cycle-topics.sh — wave-cycle-service's three Avro topics must EXIST with their declared shape BEFORE the service is scaled up
# (the same first-deploy barrier as ensure-market-structure-topics.sh; ELLIOTT-FIB-CYCLE-TOOLS E-R9.2).
#
#   options.spx.wave-cycle.waves     1 partition, delete, retention.ms >= 172800000 (WaveSet, key <symbol>|<timeframe>)
#   options.spx.wave-cycle.cycles    1 partition, delete, retention.ms >= 172800000 (CycleSet, same key)
#   options.spx.wave-cycle.recovery  1 partition, compact (the checkpoint: separate Avro records written in the output transaction); created at
#                                    retention.ms=-1 (topics.env), not judged on an existing topic because the service sets none and accepts the default
# max.message.bytes >= 1 048 576 on all three. The service checks waves and cycles only while WAVE_CYCLE_ENABLED is true and never creates
# them; it creates and verifies the recovery topic itself at every start. Here all three are ensured unconditionally so turning the flag on is a
# one-variable change, and a dev broker (num.partitions=32 auto-create) cannot win the race. No tombstones, no versioned names.
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${WAVE_CYCLE_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${WAVE_CYCLE_MIN_INSYNC_REPLICAS:-1}"
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-wave-cycle-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

rc=0
ensure_topic "${KAFKA_WAVE_CYCLE_WAVES_TOPIC:-options.spx.wave-cycle.waves}"       delete  floor:172800000 1048576 || rc=1
ensure_topic "${KAFKA_WAVE_CYCLE_CYCLES_TOPIC:-options.spx.wave-cycle.cycles}"     delete  floor:172800000 1048576 || rc=1
ensure_topic "${KAFKA_WAVE_CYCLE_RECOVERY_TOPIC:-options.spx.wave-cycle.recovery}" compact any:-1          1048576 || rc=1
# The recovery log is rewritten every fold with records of up to 960 KiB: it needs the same small-segment compaction cadence as
# market-structure's recovery topic (topic-shape.lib.sh topic_is_recovery_checkpoint), reconciled here after verification because a topic the
# service or a reset created has the broker defaults (1 GiB segments, which compaction never cleans while active).
if [ "$rc" -eq 0 ]; then
  if ! out=$(kc --entity-type topics --entity-name "${KAFKA_WAVE_CYCLE_RECOVERY_TOPIC:-options.spx.wave-cycle.recovery}" --alter \
      --add-config segment.bytes=16777216,max.compaction.lag.ms=300000 2>&1); then
    echo "FAIL: could not configure recovery compaction for the recovery topic:"; printf '%s\n' "$out"; rc=1
  fi
fi
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-wave-cycle-topics FAILED — the service must not be scaled up ==="
  exit 1
fi
echo "=== ensure-wave-cycle-topics OK ==="
