#!/usr/bin/env bash
# roll-forward-es-amt-auction.sh — the DESTRUCTIVE half of the es.futures.auction Avro cutover ON DEV.
#
# The dev topic carried the SPX Auction Desk's JSON minute records (one 50-70 KB record per Globex/RTH minute, key <tradeDate>|<HH:mm>, 1 partition, plain
# delete, retention 7 calendar days), byte-mirrored from es4. Under the Kafka Serialization Rule and the owner ruling of 2026-10-18 ("there is no version, it is
# always rolled forward") it is DELETED AND RECREATED EMPTY under its own name; there is no sibling and no dual-publish window. The retained JSON is deleted,
# never read by an Avro reader. Runbook: docs/ES-AMT-AVRO-CUTOVER.md (order of every step; this script is the wipe step only).
#
# DEV ONLY. es4 and production are refused by name: es4 still runs the JSON writer and production still receives es4's JSON by mirror; their cutover is the owner's
# decision, not this script's. Every run needs the operator's exclusion attestation and the pre-cutover topic id.
#
# THE OPERATOR ASSERTS EXCLUSION. Nothing in Kafka proves a writer is absent, so --writers-and-readers-stopped (strict) or --avro-feed-live
# --json-writers-stopped must be passed by the operator. On dev the only JSON writer of the topic was the es4 -> dev MM1 mirror (Jenkinsfile.es-auction-mirror);
# it must be torn down (ACTION=teardown, TARGET 127.0.0.1:19092) before the wipe, and is never re-installed for the dev target (the job now refuses it). The Avro writer is
# dev's own es-amt-service. Dev has topic auto-creation on, so use the STRICT mode: es-amt-service and every reader at 0 for the wipe.
#
# --verify runs ONLY the post-conditions (new id, shape, consumer-group offsets) and changes nothing.
# Usage:
#   roll-forward-es-amt-auction.sh --recreate|--verify --cluster dev --pre-cutover-id <TopicId> \
#       --writers-and-readers-stopped|--avro-feed-live --json-writers-stopped <bootstrap-servers> [kafka-bin-dir]
set -euo pipefail

usage() {
  echo "usage: roll-forward-es-amt-auction.sh --recreate|--verify --cluster dev --pre-cutover-id <id> \\" >&2
  echo "         [--new-topic-id <id>   (REQUIRED for --verify: the id --recreate printed)] \\" >&2
  echo "         [--avro-feed-live --json-writers-stopped   (INSTEAD of --writers-and-readers-stopped: the AVRO feed is writing, readers are rolling; the operator attests no JSON writer is live)] \\" >&2
  echo "         [--topic-absent-after-failed-create   (ONLY to finish a --recreate whose delete succeeded but whose create failed)] \\" >&2
  echo "         --writers-and-readers-stopped (strict mode) <bootstrap-servers> [kafka-bin-dir]" >&2
  exit 2
}
PHASE="${1:-}"
case "$PHASE" in --recreate|--verify) ;; *) usage ;; esac
shift
CLUSTER=""; PRE_ID=""; NEW_ID=""; STOPPED=""; JSON_STOPPED=""; ABSENT_OK=""; AVRO_LIVE=""; POS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="${2:-}"; shift 2 ;;
    --pre-cutover-id) PRE_ID="${2:-}"; shift 2 ;;
    --new-topic-id) NEW_ID="${2:-}"; shift 2 ;;
    --writers-and-readers-stopped) STOPPED=yes; shift ;;
    --json-writers-stopped) JSON_STOPPED=yes; shift ;;
    --topic-absent-after-failed-create) ABSENT_OK=yes; shift ;;
    --avro-feed-live) AVRO_LIVE=yes; shift ;;
    *) POS+=("$1"); shift ;;
  esac
done
case "$CLUSTER" in
  dev) ;;
  es4|production) echo "FATAL: $CLUSTER is refused by name: this cutover is dev only (es4 still writes JSON and production still mirrors it). Nothing was changed." >&2; exit 2 ;;
  *) usage ;;
esac
[ -n "$PRE_ID" ] || usage
# The attestation must say what is TRUE of the mode: strict = every writer AND reader is stopped; --avro-feed-live = the AVRO feed
# (and still-rolling readers) are live, and the operator attests that NO JSON WRITER (the pre-cutover feed pod) is.
if [ "$AVRO_LIVE" = yes ]; then [ "$JSON_STOPPED" = yes ] && [ "$STOPPED" != yes ] || usage; else [ "$STOPPED" = yes ] && [ "$JSON_STOPPED" != yes ] || usage; fi
# --verify must name the topic that --recreate printed: "a different id from the old one" proves nothing about WHICH topic it is.
if [ "$PHASE" = "--verify" ]; then [ -n "$NEW_ID" ] || usage; elif [ -n "$NEW_ID" ]; then usage; fi
BS="${POS[0]:-}"; [ -n "$BS" ] || usage
BIN="${POS[1]:-${KAFKA_BIN_DIR:-}}"

# CLUSTER IDENTITY. The bootstrap allowlist below stops the usual mistake (a production address under --cluster dev). A FORWARDED address (a port-forward, a reassigned LAN address) is stopped by the pinned
# Kafka cluster id check right after the allowlist, and independently by the topic id: --pre-cutover-id is the TopicId read off the dev broker on the cutover day, a UUID that exists on exactly one cluster, and --recreate refuses (nothing deleted) unless the
# live topic carries it, re-checked immediately before the delete. A production or es4 es.futures.auction has a different TopicId, so the wipe cannot touch it (tested: "refuses a topic that
# is not the pre-cutover one").
# --cluster is not a label: the bootstrap server must BE that cluster's broker, or a destructive command declared "dev" could
# wipe production (and the reverse). dev: the local broker (what pods dial) or the Mac's LAN address; production: the prod host.
TOPIC_NAME=es.futures.auction
case "$CLUSTER:$BS" in
  dev:localhost:19092|dev:127.0.0.1:19092|dev:host.docker.internal:19092|dev:192.168.100.102:19092) ;;
  *) echo "FATAL: bootstrap '$BS' is not the $CLUSTER cluster's broker; refusing (nothing was changed)" >&2; exit 2 ;;
esac

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$HERE/avro-rollforward.lib.sh"
# BROKER IDENTITY, pinned. An address allowlist cannot tell the dev broker from a port-forward or a reassigned listener that reaches es4 or production, and a TopicId proves the topic, not the cluster. So
# before ANYTHING is changed the broker must report the dev Kafka cluster id below (read off 127.0.0.1:19092 with `kafka-cluster cluster-id`, 2026-11-02). There is no override: if the dev broker is ever
# re-formatted, this constant is changed in a reviewed PR.
DEV_CLUSTER_ID=ST9UcaxwRHWefoBXRdjTXg
KC_CLI="$(rf_resolve_cli_named kafka-cluster "$BIN")" || exit 1
CID_OUT="$("$KC_CLI" cluster-id --bootstrap-server "$BS" 2>&1)" || { echo "FATAL: cannot read the cluster id of $BS; refusing (nothing was changed): $CID_OUT" >&2; exit 2; }
LIVE_CID="$(printf '%s\n' "$CID_OUT" | awk '/^Cluster ID:/ {print $3}' | tail -1)"
[ "$LIVE_CID" = "$DEV_CLUSTER_ID" ] || { echo "FATAL: $BS reports Kafka cluster id '${LIVE_CID:-unreadable}', not the dev cluster $DEV_CLUSTER_ID; refusing (nothing was changed)" >&2; exit 2; }
# every sampled record must be Avro under the ONE fixed contract id 3201 (0x00000c81), not merely some Avro
export RF_EXPECT_ID_HEX=00000c81
KT="$(rf_resolve_cli "$BIN")"
KG="$(rf_resolve_cli_named kafka-consumer-groups "$BIN")"
kt() { "$KT" --bootstrap-server "$BS" "$@"; }

# The shape scripts/kafka/topics.env declares for it (OPTIONS_EDGE_TOPICS es.futures.auction:1, EXACT partitions) and the es-amt-service creates it with: 1 partition,
# replication 1, plain delete (the desk is never compacted: a compacted target also fails Jenkinsfile.es-auction-mirror's frozen assertion), retention.ms=604800000
# (7 CALENDAR days: what the gateway rebuilds its current + previous trade date from). Read off the live dev broker 2026-11-02.
TOPIC=es.futures.auction
PARTITIONS=1
REPLICATION_FACTOR=1
CONFIGS="cleanup.policy=delete,retention.ms=604800000"
CONFIG_ARGS=(--config cleanup.policy=delete --config retention.ms=604800000)

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
  args+=("${CONFIG_ARGS[@]}")
  echo "-- create $TOPIC ($PARTITIONS partition, $CONFIGS)"
  kt "${args[@]}" || { echo "FAIL: $TOPIC was deleted but could not be created; it is absent. Re-run this command with --topic-absent-after-failed-create added." >&2; exit 1; }
fi

# Verify what was created, from the broker, not from the create command's exit code.
DESC="$(rf_describe "$KT" "$BS" "$TOPIC")" || exit 1
FOUND_ID="$(rf_topic_id "$DESC" "$TOPIC")" || exit 1
RECOVER="Stop the writer, then run --recreate AGAIN with --pre-cutover-id $FOUND_ID (the id of THIS now-contaminated topic: a second recreate is bound to the topic it is about to delete)."
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
GO="$(rf_resolve_cli_named kafka-get-offsets "$BIN")"
ENDS="$("$GO" --bootstrap-server "$BS" --topic "$TOPIC" --time -1 2>&1)" || { echo "FAIL: cannot read end offsets of $TOPIC:" >&2; printf '%s\n' "$ENDS" >&2; exit 1; }
END_OFFSET="$(printf '%s\n' "$ENDS" | awk -F: -v t="$TOPIC" '$1 == t {n++; e = $3} END {print (n == '"$PARTITIONS"') ? e : "unreadable"}')"
case "$END_OFFSET" in unreadable|"") echo "FAIL: end offsets of $TOPIC could not be read: $ENDS" >&2; exit 1 ;; esac
if [ "$AVRO_LIVE" = yes ]; then
  # Jenkins cannot stop the feed, so the AVRO feed is writing while the topic is recreated (the order the 2026-10-18 roll-forward
  # settled on: the Avro writer first, then the wipe). Emptiness is therefore not required; what is required is that nothing in
  # the log is JSON: the first and the last record must be Confluent-framed Avro (or a tombstone). A sample, not a scan; the
  # guarantee that no JSON writer exists is the old feed pod being gone (runbook step 4), which the operator asserts.
  if [ "$END_OFFSET" -gt 0 ]; then
    CC="$(rf_resolve_cli_named kafka-console-consumer "$BIN")"
    EARLIEST="$("$GO" --bootstrap-server "$BS" --topic "$TOPIC" --time -2 2>&1 | awk -F: -v t="$TOPIC" '$1 == t {e = $3} END {print e}')"
    case "$EARLIEST" in ''|*[!0-9]*) echo "FAIL: cannot read the earliest offset of $TOPIC" >&2; exit 1 ;; esac
    # first retained record: a plain read; last record: walk BACK over transaction commit/abort markers (es-amt-service is a transactional writer: the offset below the high watermark is
    # normally a marker, which no consumer is handed). Fail CLOSED on anything not verifiably Avro under 3201.
    rc=0; rf_wire_is_avro "$CC" "$BS" "$TOPIC" 0 "$EARLIEST" || rc=$?
    [ "$rc" -eq 0 ] || { echo "FAIL: the first record (offset $EARLIEST) of $TOPIC is not verifiably Avro (verdict $rc: 1 = not Avro, a JSON writer is live; 2 = unreadable). $RECOVER" >&2; exit 1; }
    rc=0; rf_wire_is_avro_at_log_end "$CC" "$BS" "$TOPIC" 0 "$((END_OFFSET - 1))" "$EARLIEST" || rc=$?
    [ "$rc" -eq 0 ] || { echo "FAIL: the last delivered record of $TOPIC is not verifiably Avro (verdict $rc: 1 = not Avro, a JSON writer is live; 2 = nothing readable near the log end). $RECOVER" >&2; exit 1; }
  fi
elif [ "$END_OFFSET" != 0 ]; then
  echo "FAIL: $TOPIC is not empty (end offset $END_OFFSET): a writer is live. $RECOVER" >&2
  exit 1
fi

# Committed offsets are keyed by topic NAME. Deleting the topic removes them from the (stopped) groups, but that is a broker
# behaviour, so it is verified, not assumed: a group that kept an offset for the name would resume beyond the new log end and
# silently skip the first Avro records.
GROUPS_OUT="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || { echo "FAIL: cannot read consumer groups on $BS:" >&2; printf '%s\n' "$GROUPS_OUT" >&2; exit 1; }
# Strict mode: ANY committed offset is stale. With the Avro feed (and still-rolling readers) live, an offset INSIDE the new log is
# legitimate (a reader committed its position in the recreated topic); only an offset BEYOND the log end is the stale one.
STALE="$(printf '%s\n' "$GROUPS_OUT" | awk -v t="$TOPIC" -v live="$AVRO_LIVE" '$2 == t && $4 ~ /^[0-9]+$/ && (live != "yes" || $4 + 0 > $5 + 0) {print $1 ":" $3 "@" $4 " (log end " $5 ")"}')"
if [ -n "$STALE" ] && [ "$AVRO_LIVE" = yes ]; then
  # With readers still rolling (Jenkins cannot stop them) a group cannot be made inactive on demand. A regular consumer whose offset is
  # beyond the log end resets by its auto.offset.reset policy on its first poll; the cases that do not (a Streams GLOBAL store) are handled
  # in the reader (checkpoint discarded) and the per-reader gate is runbook step 8. So here it is reported, and step 8 decides.
  # The topic is a latest-value spot stream (each record supersedes the last), so a reader resuming at an in-range offset that was
  # committed before the recreation skips at most the first few records of the new log and loses no information.
  echo "WARN: consumer groups with a committed offset BEYOND the new log end (group:partition@offset): they reset on first poll; runbook step 8 verifies each reader:" >&2
  printf '%s\n' "$STALE" >&2
elif [ -n "$STALE" ]; then
  echo "FAIL: consumer groups still hold committed offsets for $TOPIC (group:partition@offset):" >&2
  printf '%s\n' "$STALE" >&2
  echo "      DELETE them (kafka-consumer-groups --delete-offsets --group <g> --topic $TOPIC, or delete the stopped group) before any reader starts;" >&2
  echo "      a reset to 0 still leaves a committed offset and is refused here." >&2
  exit 1
fi
echo "=== DONE: $TOPIC recreated empty. Next: the Avro readers are already rolled; start the dev es-amt-service (the Avro writer), then the readers ==="
