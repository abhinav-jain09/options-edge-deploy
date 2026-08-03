#!/usr/bin/env bash
# ensure-gamma-tilt-ledger.sh — the GMT Stage-1 evidence ledger must EXIST with the right shape
# before strike-intelligence (with the shadow flag on) is allowed to produce to it.
#
# Why a deploy barrier (GMT Gate-1 rev 11, Codex code-review c1-6): the ledger is append-only
# EVIDENCE — retention.ms=-1, exactly 1 partition (scorer co-partitioning + total order), pure
# delete policy (compaction would erase observations sharing symbol keys). A broker-auto-created
# or misshapen topic silently truncates the record the whole Stage-2 promotion decision reads.
# The service also refuses at boot (ensureLedgerTopic, defense-in-depth); this gate stops the
# deploy BEFORE a crash-looping pod ever ships.
#
# CREATE-only if absent; VERIFY (fail closed) if present.
set -euo pipefail

TOPIC="${STRIKE_INTEL_GAMMA_SHADOW_TOPIC:-strike-intel.gamma-tilt.shadow}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }

echo "=== ensure-gamma-tilt-ledger: $TOPIC on $KAFKA_BOOTSTRAP_SERVERS ==="

if ! kt --list 2>/dev/null | grep -qxF "$TOPIC"; then
  echo "creating '$TOPIC' (1 partition, RF=$RF, delete, retention.ms=-1)"
  kt --create --topic "$TOPIC" --partitions 1 --replication-factor "$RF" \
     --config cleanup.policy=delete --config retention.ms=-1
  exit 0
fi

PARTS=$(kt --describe --topic "$TOPIC" 2>/dev/null \
  | awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}')
if [ "${PARTS:-0}" != "1" ]; then
  echo "FAIL: '$TOPIC' has PartitionCount=${PARTS:-unknown}, expected 1."
  echo "      The scorer co-partitions against a single-partition clock and episode order is"
  echo "      only total on one partition."
  exit 1
fi

CFG=$(kc --entity-type topics --entity-name "$TOPIC" --describe 2>&1) || {
  echo "FAIL: cannot describe configs for '$TOPIC': $CFG"
  exit 1
}
if ! grep -qE 'retention\.ms=-1' <<<"$CFG"; then
  echo "FAIL: '$TOPIC' retention.ms is not -1 — an evidence ledger that silently drops its"
  echo "      record is worse than none. Fix the topic config, then re-run this deploy."
  exit 1
fi
if grep -qE 'cleanup\.policy=[^ ]*compact' <<<"$CFG"; then
  echo "FAIL: '$TOPIC' cleanup.policy contains compact — compaction erases append-only evidence"
  echo "      (observations share symbol keys). Must be pure delete."
  exit 1
fi
echo "ok: '$TOPIC' shape verified (1 partition, delete, retention.ms=-1)"
