#!/usr/bin/env bash
# roll-forward-avro-cutover-topics.sh — the DESTRUCTIVE half of the 2026-10-18 Avro roll-forward.
#
# Topics in this estate are ROLLED FORWARD: a topic that must change its serialization is DELETED
# AND RECREATED under its own name, and any versioned sibling that was minted is deleted (owner,
# 2026-10-18: "there is no version, it is always rolled forward ... and no version in the system",
# with explicit authorisation to wipe these topics).
#
# WHY A SCRIPT AND NOT A ONE-OFF COMMAND. The deletion is a PRECONDITION of the rollout, not a
# side effect of it: both topics are retention.ms=-1, and gamma-ladder-path-service reads
# options.databento.gex.oi-status as a Kafka Streams source with an AVRO serde and no explicit
# auto.offset.reset, so Streams replays from the beginning of the topic after any state loss. Roll
# the workloads onto a topic that still holds the old JSON and that replay feeds JSON to an Avro
# deserializer — the exact case the Kafka Serialization Rule forbids. An operation that a rollout
# depends on belongs in the repository, where it can be reviewed and repeated, and it has a
# fail-closed partner: scripts/kafka/verify-avro-rollforward.sh refuses to let the workloads roll
# while a topic still carries its pre-cutover topic id.
#
# WHAT IS LOST, so nobody runs this expecting otherwise: options.ibkr.gex.status was the only store
# of databento-gex-service's generation/epoch identity ledger (12,461,124 records / 1.31 GiB on prod,
# measured 2026-10-17). Wiping it makes the next mint start from zero and reissue numbers the old
# topic had spent. That is safe only because every consumer comparison is scoped to an OI-as-of date
# (options-edge-documents design/KAFKA-AVRO-CUTOVER-IBKR-GEX-STATUS.md §12.2).
#
# Shapes below MIRROR scripts/kafka/topics.env. apply-topics.sh remains the reconciler of record;
# this script exists for the one-time destructive step the reconciler will not do (it never deletes
# a topic), and it VERIFIES everything it creates before touching the versioned siblings.
#
# Usage:  roll-forward-avro-cutover-topics.sh <bootstrap-servers> <kafka-bin-dir>
# Fails closed: any unverifiable condition aborts before the versioned siblings are removed.
set -euo pipefail
BS="${1:-${KAFKA_BOOTSTRAP_SERVERS:-}}"
: "${BS:?usage: roll-forward-avro-cutover-topics.sh <bootstrap-servers> [kafka-bin-dir]}"
BIN="${2:-${KAFKA_BIN_DIR:-}}"
# The deploy images put the CLI on PATH without the .sh suffix; a developer Mac has a bin dir.
if [ -n "$BIN" ]; then
  KT="$BIN/kafka-topics.sh"
elif command -v kafka-topics >/dev/null 2>&1; then
  KT="kafka-topics"
elif command -v kafka-topics.sh >/dev/null 2>&1; then
  KT="kafka-topics.sh"
else
  echo "FAIL: no kafka-topics CLI found (pass a bin dir or set KAFKA_BIN_DIR)" >&2; exit 1
fi
kt() { "$KT" --bootstrap-server "$BS" "$@"; }

# topic|partitions|configs(comma separated)
SPECS=(
  "options.ibkr.gex.status|1|cleanup.policy=compact,retention.ms=-1,delete.retention.ms=172800000,min.insync.replicas=1"
  "options.databento.gex.oi-status|32|cleanup.policy=delete,retention.ms=-1,min.insync.replicas=1"
  "rates.databento.zn.options.raw|1|cleanup.policy=delete,retention.ms=86400000,min.insync.replicas=1"
  "rates.databento.zn.options.gex.strike|1|cleanup.policy=delete,retention.ms=86400000,min.insync.replicas=1"
)
# Versioned siblings to remove outright. Nothing reads these once the new images are live.
RETIRE=(
  "options.ibkr.gex.status.v2"
  "options.databento.gex.oi-status.v2"
  "rates.databento.zn.options.raw.v1"
  "rates.databento.zn.options.gex.strike.v1"
)

LIST="$(kt --list)"
has() { printf '%s\n' "$LIST" | grep -qx "$1"; }

echo "=== roll forward on $BS ==="
for spec in "${SPECS[@]}"; do
  IFS='|' read -r t parts cfgs <<<"$spec"
  if has "$t"; then
    echo "-- delete $t"
    kt --delete --topic "$t"
  else
    echo "-- $t absent, creating fresh"
  fi
done

# One wait for every deletion to settle, then create.
for spec in "${SPECS[@]}"; do
  IFS='|' read -r t parts cfgs <<<"$spec"
  for i in $(seq 1 60); do
    kt --list | grep -qx "$t" || break
    sleep 2
  done
  if kt --list | grep -qx "$t"; then
    echo "FAIL: $t still present 120s after delete"; exit 1
  fi
  args=(--create --topic "$t" --partitions "$parts" --replication-factor 1)
  IFS=',' read -ra kvs <<<"$cfgs"
  for kv in "${kvs[@]}"; do args+=(--config "$kv"); done
  echo "-- create $t ($parts partitions, $cfgs)"
  kt "${args[@]}"
done

echo "=== verify ==="
fail=0
for spec in "${SPECS[@]}"; do
  IFS='|' read -r t parts cfgs <<<"$spec"
  desc="$(kt --describe --topic "$t" | head -1)"
  got_parts="$(printf '%s' "$desc" | sed -nE 's/.*PartitionCount: ([0-9]+).*/\1/p')"
  [ "$got_parts" = "$parts" ] || { echo "FAIL $t: partitions $got_parts != $parts"; fail=1; }
  IFS=',' read -ra kvs <<<"$cfgs"
  for kv in "${kvs[@]}"; do
    printf '%s' "$desc" | grep -q -- "$kv" || { echo "FAIL $t: missing $kv in: $desc"; fail=1; }
  done
  echo "ok $t  $desc"
done
[ "$fail" -eq 0 ] || exit 1

echo "=== retire the versioned siblings ==="
LIST="$(kt --list)"
for t in "${RETIRE[@]}"; do
  if printf '%s\n' "$LIST" | grep -qx "$t"; then
    echo "-- delete $t"; kt --delete --topic "$t"
  else
    echo "-- $t already absent"
  fi
done
for t in "${RETIRE[@]}"; do
  for i in $(seq 1 60); do kt --list | grep -qx "$t" || break; sleep 2; done
  if kt --list | grep -qx "$t"; then echo "FAIL: $t still present"; exit 1; fi
done
echo "=== no versioned topic names remain ==="
kt --list | grep -E '\.v[0-9]+$' && { echo "FAIL: a versioned topic name survives"; exit 1; } || echo "confirmed: none"
echo "=== DONE on $BS ==="
