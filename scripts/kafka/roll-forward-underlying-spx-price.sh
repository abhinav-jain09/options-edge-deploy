#!/usr/bin/env bash
# roll-forward-underlying-spx-price.sh — the DESTRUCTIVE half of the underlying.spx.price Avro cutover.
#
# underlying.spx.price carried hand-built JSON (UnderlyingPriceEvent). Under the Kafka Serialization Rule and the owner
# ruling of 2026-10-18 ("there is no version, it is always rolled forward") it is DELETED AND RECREATED EMPTY under its
# own name; there is no .avro sibling and no dual-publish window. The retained JSON is deleted, never read by an Avro
# reader. Runbook: docs/UNDERLYING-SPX-PRICE-AVRO-CUTOVER.md (order of every step; this script is step 4 only).
#
# THE OPERATOR ASSERTS EXCLUSION. Nothing in Kafka proves a writer is absent, so --writers-and-readers-stopped must be
# passed by the operator who stopped the (old, JSON) feed and every reader first. A pre-cutover feed that is still
# running writes JSON into the fresh topic within seconds (found the hard way on 2026-10-18 for the gex status topic).
#
# Also refuses a bootstrap server that is not the named cluster's broker, re-checks the TopicId immediately before the delete,
# and fails if any consumer group still holds a committed offset for the recreated topic.
# Usage:
#   roll-forward-underlying-spx-price.sh --recreate --cluster dev|production --pre-cutover-id <TopicId> \
#       --writers-and-readers-stopped <bootstrap-servers> [kafka-bin-dir]
# The pre-cutover topic id is read off the LIVE broker by the operator (kafka-topics --describe) and passed in: a topic
# whose id is anything else is not the topic that held the JSON, and is refused (an already-recreated topic may hold live
# Avro). An absent topic is created fresh. Re-runnable after a failed create.
set -euo pipefail

usage() {
  echo "usage: roll-forward-underlying-spx-price.sh --recreate --cluster dev|production --pre-cutover-id <id> \\" >&2
  echo "         --writers-and-readers-stopped <bootstrap-servers> [kafka-bin-dir]" >&2
  exit 2
}
[ "${1:-}" = "--recreate" ] || usage
[ "${2:-}" = "--cluster" ] || usage
CLUSTER="${3:-}"
case "$CLUSTER" in dev|production) ;; *) usage ;; esac
[ "${4:-}" = "--pre-cutover-id" ] || usage
PRE_ID="${5:-}"; [ -n "$PRE_ID" ] || usage
[ "${6:-}" = "--writers-and-readers-stopped" ] || usage
BS="${7:-}"; [ -n "$BS" ] || usage
BIN="${8:-${KAFKA_BIN_DIR:-}}"

# --cluster is not a label: the bootstrap server must BE that cluster's broker, or a destructive command declared "dev" could
# wipe production (and the reverse). dev: the local broker (what pods dial) or the Mac's LAN address; production: the prod host.
case "$CLUSTER:$BS" in
  dev:localhost:19092|dev:host.docker.internal:19092|dev:192.168.100.102:19092) ;;
  production:192.168.100.252:9092) ;;
  *) echo "FATAL: bootstrap '$BS' is not the $CLUSTER cluster's broker; refusing (nothing was changed)" >&2; exit 2 ;;
esac

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$HERE/avro-rollforward.lib.sh"
KT="$(rf_resolve_cli "$BIN")"
KG="$(rf_resolve_cli_named kafka-consumer-groups "$BIN")"
kt() { "$KT" --bootstrap-server "$BS" "$@"; }

# The shape the live dev topic carried, read off the broker 2026-10-18 (topics.env: compacted, retention -1).
TOPIC=underlying.spx.price
PARTITIONS=1
CONFIGS="cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1"

echo "=== roll forward $TOPIC on $BS (cluster $CLUSTER) ==="
LIST="$(rf_list "$KT" "$BS")" || exit 1
if printf '%s\n' "$LIST" | grep -qx "$TOPIC"; then
  DESC="$(rf_describe "$KT" "$BS" "$TOPIC")" || exit 1
  CUR_ID="$(rf_topic_id "$DESC" "$TOPIC")" || exit 1
  if [ "$CUR_ID" != "$PRE_ID" ]; then
    echo "FAIL: $TOPIC carries id $CUR_ID, not the pre-cutover id $PRE_ID. It was already recreated and may hold live Avro;" >&2
    echo "      refusing to wipe it. Nothing was changed." >&2
    exit 1
  fi
  # Last look immediately before the delete: Kafka's delete takes a NAME, not a TopicId, so a topic another administrator
  # recreated since the check above would be deleted. The runbook freezes topic administration for the window; this
  # shrinks the race to the gap between this describe and the delete call.
  AGAIN="$(rf_topic_id "$(rf_describe "$KT" "$BS" "$TOPIC")" "$TOPIC")" || exit 1
  [ "$AGAIN" = "$PRE_ID" ] || { echo "FAIL: $TOPIC changed id to $AGAIN while checking; nothing deleted." >&2; exit 1; }
  echo "-- delete $TOPIC (id $CUR_ID)"
  kt --delete --topic "$TOPIC"
  settled=0
  for _ in $(seq 1 60); do
    cur="$(rf_list "$KT" "$BS")" || exit 1
    if ! printf '%s\n' "$cur" | grep -qx "$TOPIC"; then settled=1; break; fi
    sleep 2
  done
  [ "$settled" -eq 1 ] || { echo "FAIL: $TOPIC still present 120s after the delete; re-run once the broker settles." >&2; exit 1; }
else
  echo "-- $TOPIC absent, creating fresh"
fi

args=(--create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR:-1}")
# cleanup.policy carries a comma of its own, so the --config flags are spelled out rather than split from CONFIGS.
args+=(--config "cleanup.policy=compact,delete" --config retention.ms=-1 --config delete.retention.ms=3600000 \
       --config min.cleanable.dirty.ratio=0.01 --config segment.ms=3600000 --config min.insync.replicas=1)
echo "-- create $TOPIC ($PARTITIONS partition, $CONFIGS)"
kt "${args[@]}" || { echo "FAIL: $TOPIC was deleted but could not be created; it is absent. Re-run this command." >&2; exit 1; }

# Verify what was created, from the broker, not from the create command's exit code.
DESC="$(rf_describe "$KT" "$BS" "$TOPIC")" || exit 1
NEW_ID="$(rf_topic_id "$DESC" "$TOPIC")" || exit 1
[ "$NEW_ID" != "$PRE_ID" ] || { echo "FAIL: $TOPIC still has the pre-cutover id $PRE_ID" >&2; exit 1; }
printf '%s\n' "$DESC" | grep -q "PartitionCount: $PARTITIONS" || { echo "FAIL: partition count is not $PARTITIONS: $DESC" >&2; exit 1; }
rf_split_declared "$CONFIGS" | while IFS='=' read -r k want; do
  have="$(rf_config_value "$DESC" "$k")"
  [ "$have" = "$want" ] || { echo "FAIL: $TOPIC $k is '$have', declared '$want'" >&2; exit 1; }
done
# Committed offsets are keyed by topic NAME. Deleting the topic removes them from the (stopped) groups, but that is a broker
# behaviour, so it is verified, not assumed: a group that kept an offset for the name would resume beyond the new log end and
# silently skip the first Avro records.
GROUPS_OUT="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || { echo "FAIL: cannot read consumer groups on $BS:" >&2; printf '%s\n' "$GROUPS_OUT" >&2; exit 1; }
STALE="$(printf '%s\n' "$GROUPS_OUT" | awk -v t="$TOPIC" '$2 == t && $4 ~ /^[0-9]+$/ {print $1 ":" $3 "@" $4}')"
if [ -n "$STALE" ]; then
  echo "FAIL: consumer groups still hold committed offsets for $TOPIC (group:partition@offset):" >&2
  printf '%s\n' "$STALE" >&2
  echo "      reset them (kafka-consumer-groups --reset-offsets --to-earliest --execute, or delete the stopped group) before any reader starts." >&2
  exit 1
fi
echo "=== DONE: $TOPIC recreated empty (id $NEW_ID, was $PRE_ID). Next: deploy the Avro readers, then the Avro feed ==="
