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
# --verify runs ONLY the post-conditions (new id, shape, consumer-group offsets) and changes nothing: the recovery path when
# --recreate created the topic but then failed the consumer-group check (a re-run of --recreate would refuse the new id).
# PRODUCTION IS NOT ENABLED: the declared shape below was read off the dev broker; a production run needs its own recorded shape.
# Also refuses a bootstrap server that is not the named cluster's broker, re-checks the TopicId immediately before the delete,
# and fails if any consumer group still holds a committed offset for the recreated topic.
# Usage:
#   roll-forward-underlying-spx-price.sh --recreate|--verify --cluster dev|production --pre-cutover-id <TopicId> \
#       --writers-and-readers-stopped <bootstrap-servers> [kafka-bin-dir]
# The pre-cutover topic id is read off the LIVE broker by the operator (kafka-topics --describe) and passed in: a topic
# whose id is anything else is not the topic that held the JSON, and is refused (an already-recreated topic may hold live
# Avro). An absent topic is REFUSED unless the operator passes --topic-absent-after-failed-create (the delete succeeded, the create failed).
set -euo pipefail

usage() {
  echo "usage: roll-forward-underlying-spx-price.sh --recreate|--verify --cluster dev|production --pre-cutover-id <id> \\" >&2
  echo "         [--new-topic-id <id>   (REQUIRED for --verify: the id --recreate printed)] \\" >&2
  echo "         [--topic-absent-after-failed-create   (ONLY to finish a --recreate whose delete succeeded but whose create failed)] \\" >&2
  echo "         --writers-and-readers-stopped <bootstrap-servers> [kafka-bin-dir]" >&2
  exit 2
}
PHASE="${1:-}"
case "$PHASE" in --recreate|--verify) ;; *) usage ;; esac
shift
CLUSTER=""; PRE_ID=""; NEW_ID=""; STOPPED=""; ABSENT_OK=""; POS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="${2:-}"; shift 2 ;;
    --pre-cutover-id) PRE_ID="${2:-}"; shift 2 ;;
    --new-topic-id) NEW_ID="${2:-}"; shift 2 ;;
    --writers-and-readers-stopped) STOPPED=yes; shift ;;
    --topic-absent-after-failed-create) ABSENT_OK=yes; shift ;;
    *) POS+=("$1"); shift ;;
  esac
done
case "$CLUSTER" in dev|production) ;; *) usage ;; esac
[ -n "$PRE_ID" ] && [ "$STOPPED" = yes ] || usage
# --verify must name the topic that --recreate printed: "a different id from the old one" proves nothing about WHICH topic it is.
if [ "$PHASE" = "--verify" ]; then [ -n "$NEW_ID" ] || usage; elif [ -n "$NEW_ID" ]; then usage; fi
BS="${POS[0]:-}"; [ -n "$BS" ] || usage
BIN="${POS[1]:-${KAFKA_BIN_DIR:-}}"

# --cluster is not a label: the bootstrap server must BE that cluster's broker, or a destructive command declared "dev" could
# wipe production (and the reverse). dev: the local broker (what pods dial) or the Mac's LAN address; production: the prod host.
TOPIC_NAME=underlying.spx.price
case "$CLUSTER:$BS" in
  dev:localhost:19092|dev:host.docker.internal:19092|dev:192.168.100.102:19092) ;;
  production:192.168.100.252:9092)
    echo "FATAL: the production shape (replication factor, retention, cleanup policy) of $TOPIC_NAME has not been recorded; read it off the live" >&2
    echo "       production broker, add a production row here with its own verification, and review it before this script may touch production." >&2
    exit 2 ;;
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
REPLICATION_FACTOR=1
CONFIGS="cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1"

if [ "$PHASE" = "--recreate" ]; then
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
    # An absent topic is NOT a normal start state: it means somebody deleted it. The only legitimate cause is this script's own
    # earlier --recreate, whose delete succeeded and create failed; the operator says so explicitly (and saw that failure).
    [ "$ABSENT_OK" = yes ] || { echo "FAIL: $TOPIC does not exist. If a previous --recreate deleted it and then failed to create it, re-run with --topic-absent-after-failed-create; otherwise find out who deleted it first. Nothing was changed." >&2; exit 1; }
    echo "-- $TOPIC absent (recovering a failed create), creating fresh"
  fi

  args=(--create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR")
  # cleanup.policy carries a comma of its own, so the --config flags are spelled out rather than split from CONFIGS.
  args+=(--config "cleanup.policy=compact,delete" --config retention.ms=-1 --config delete.retention.ms=3600000 \
         --config min.cleanable.dirty.ratio=0.01 --config segment.ms=3600000 --config min.insync.replicas=1)
  echo "-- create $TOPIC ($PARTITIONS partition, $CONFIGS)"
  kt "${args[@]}" || { echo "FAIL: $TOPIC was deleted but could not be created; it is absent. Re-run this command with --topic-absent-after-failed-create added." >&2; exit 1; }
fi

# Verify what was created, from the broker, not from the create command's exit code.
DESC="$(rf_describe "$KT" "$BS" "$TOPIC")" || exit 1
FOUND_ID="$(rf_topic_id "$DESC" "$TOPIC")" || exit 1
echo "NEW TOPIC ID: $FOUND_ID (was $PRE_ID)"   # printed before any further check so a later failure can still be recovered with --verify
[ "$FOUND_ID" != "$PRE_ID" ] || { echo "FAIL: $TOPIC still has the pre-cutover id $PRE_ID" >&2; exit 1; }
[ -z "$NEW_ID" ] || [ "$FOUND_ID" = "$NEW_ID" ] || { echo "FAIL: $TOPIC has id $FOUND_ID, not the recreated topic $NEW_ID that --recreate reported" >&2; exit 1; }
# exact field match: `grep ReplicationFactor: 1` would also accept 11
printf '%s\n' "$DESC" | tr '\t' '\n' | grep -qx "ReplicationFactor: $REPLICATION_FACTOR" || { echo "FAIL: replication factor is not $REPLICATION_FACTOR: $DESC" >&2; exit 1; }
printf '%s\n' "$DESC" | tr '\t' '\n' | grep -qx "PartitionCount: $PARTITIONS" || { echo "FAIL: partition count is not $PARTITIONS: $DESC" >&2; exit 1; }
rf_split_declared "$CONFIGS" | while IFS='=' read -r k want; do
  have="$(rf_config_value "$DESC" "$k")"
  [ "$have" = "$want" ] || { echo "FAIL: $TOPIC $k is '$have', declared '$want'" >&2; exit 1; }
done
# The recreated topic must be EMPTY: a feed that was not actually stopped would already have written into it (log end > 0).
GO="$(rf_resolve_cli_named kafka-get-offsets "$BIN")"
ENDS="$("$GO" --bootstrap-server "$BS" --topic "$TOPIC" --time -1 2>&1)" || { echo "FAIL: cannot read end offsets of $TOPIC:" >&2; printf '%s\n' "$ENDS" >&2; exit 1; }
[ "$(printf '%s\n' "$ENDS" | awk -F: -v t="$TOPIC" '$1 == t {n++; if ($3 != 0) bad++} END {print (n == '"$PARTITIONS"' && bad == 0) ? "empty" : "not-empty"}')" = empty ] \
  || { echo "FAIL: $TOPIC is not empty (or its partitions could not be read): $ENDS — a writer is live; stop it and run --recreate again" >&2; exit 1; }

# Committed offsets are keyed by topic NAME. Deleting the topic removes them from the (stopped) groups, but that is a broker
# behaviour, so it is verified, not assumed: a group that kept an offset for the name would resume beyond the new log end and
# silently skip the first Avro records.
GROUPS_OUT="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || { echo "FAIL: cannot read consumer groups on $BS:" >&2; printf '%s\n' "$GROUPS_OUT" >&2; exit 1; }
STALE="$(printf '%s\n' "$GROUPS_OUT" | awk -v t="$TOPIC" '$2 == t && $4 ~ /^[0-9]+$/ {print $1 ":" $3 "@" $4}')"
if [ -n "$STALE" ]; then
  echo "FAIL: consumer groups still hold committed offsets for $TOPIC (group:partition@offset):" >&2
  printf '%s\n' "$STALE" >&2
  echo "      DELETE them (kafka-consumer-groups --delete-offsets --group <g> --topic $TOPIC, or delete the stopped group) before any reader starts;" >&2
  echo "      a reset to 0 still leaves a committed offset and is refused here." >&2
  exit 1
fi
echo "=== DONE: $TOPIC recreated empty. Next: deploy the Avro readers, then the Avro feed ==="
