#!/usr/bin/env bash
# roll-forward-vix-option-inteligence.sh — the DESTRUCTIVE half of the options.spx.vix-option-inteligence-service.current Avro cutover.
#
# The topic carried vix-option-inteligence-service's JSON snapshot (one ~1.4 KB record per interval, key SYMBOL|sessionDate, 32 partitions, compacted,
# retention -1). Under the Kafka Serialization Rule and the owner ruling of 2026-10-18 ("there is no version, it is always rolled forward") it is DELETED
# AND RECREATED EMPTY under its own name; there is no sibling and no dual-publish window. The retained JSON is deleted, never read by an Avro reader.
# Runbook: docs/VIX-OPTION-INTELIGENCE-AVRO-CUTOVER.md (order of every step; this script is the wipe step only). Same guards and the same two attestation
# modes as roll-forward-vix-option-inteligence.sh.
#
# --cluster dev, es4 and production are executable; es4 carries the topic under the `es.` prefix (TOPIC_PREFIX), dev and production without it.
# Every run needs the operator's exclusion attestation and the pre-cutover topic id.
#
# THE OPERATOR ASSERTS EXCLUSION. Nothing in Kafka proves a writer is absent, so --writers-and-readers-stopped (strict) or --avro-feed-live
# --json-writers-stopped must be passed by the operator. The only JSON writer of the topic is vix-option-inteligence-service itself (one pod per
# cluster): it is rolled to the Avro image BEFORE the wipe (--avro-feed-live), or scaled to zero (strict).
#
# --verify runs ONLY the post-conditions (new id, shape, consumer-group offsets) and changes nothing.
# Usage:
#   roll-forward-vix-option-inteligence.sh --recreate|--verify --cluster dev --pre-cutover-id <TopicId> \
#       --writers-and-readers-stopped|--avro-feed-live --json-writers-stopped <bootstrap-servers> [kafka-bin-dir]
set -euo pipefail

usage() {
  echo "usage: roll-forward-vix-option-inteligence.sh --recreate|--verify --cluster dev --pre-cutover-id <id> \\" >&2
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
  dev|es4|production) ;;
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

# --cluster is not a label: the bootstrap server must BE that cluster's broker, or a destructive command declared "dev" could
# wipe production (and the reverse). dev: the local broker (what pods dial) or the Mac's LAN address; production: the prod host.
TOPIC_NAME=options.spx.vix-option-inteligence-service.current
case "$CLUSTER:$BS" in
  dev:localhost:19092|dev:127.0.0.1:19092|dev:host.docker.internal:19092|dev:192.168.100.102:19092) ;;
  es4:192.168.100.4:9092) ;;
  production:192.168.100.252:9092) ;;
  *) echo "FATAL: bootstrap '$BS' is not the $CLUSTER cluster's broker; refusing (nothing was changed)" >&2; exit 2 ;;
esac

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$HERE/avro-rollforward.lib.sh"
# every sampled record must be Avro under the ONE fixed contract id 3001 (0x00000bb9), not merely some Avro
export RF_EXPECT_ID_HEX=00000bb9
KT="$(rf_resolve_cli "$BIN")"
KG="$(rf_resolve_cli_named kafka-consumer-groups "$BIN")"
kt() { "$KT" --bootstrap-server "$BS" "$@"; }

# The shape scripts/kafka/ensure-vix-option-inteligence-topic.sh reconciles (read off the live dev broker 2026-10-31): 32 partitions, replication 1,
# cleanup.policy=compact, retention.ms=-1 (the keyed latest-value snapshot).
TOPIC=options.spx.vix-option-inteligence-service.current
[ "$CLUSTER" != es4 ] || TOPIC=es.$TOPIC
PARTITIONS=32
REPLICATION_FACTOR=1
CONFIGS="cleanup.policy=compact,retention.ms=-1"
CONFIG_ARGS=(--config cleanup.policy=compact --config retention.ms=-1)

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
  echo "-- create $TOPIC ($PARTITIONS partitions, $CONFIGS)"
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
GO="$(rf_resolve_cli_named kafka-get-offsets "$BIN")"
# One line per partition: <topic>:<partition>:<offset>. The topic has $PARTITIONS partitions and its records sit in the few that the
# SYMBOL|sessionDate key hashes to, so EVERY partition is read: a count other than $PARTITIONS is unreadable, never "empty".
ENDS="$("$GO" --bootstrap-server "$BS" --topic "$TOPIC" --time -1 2>&1)" || { echo "FAIL: cannot read end offsets of $TOPIC:" >&2; printf '%s\n' "$ENDS" >&2; exit 1; }
STARTS="$("$GO" --bootstrap-server "$BS" --topic "$TOPIC" --time -2 2>&1)" || { echo "FAIL: cannot read earliest offsets of $TOPIC:" >&2; printf '%s\n' "$STARTS" >&2; exit 1; }
# <partition>:<earliest>:<end> for every partition that holds at least one record; "unreadable" when either listing is not exactly $PARTITIONS well-formed lines
NONEMPTY="$(awk -F: -v t="$TOPIC" -v want="$PARTITIONS" '
  FNR == NR { if ($1 == t && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/) { se[$2] = $3; ns++ } next }
  $1 == t && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ { en[$2] = $3; ne++ }
  END {
    if (ns != want || ne != want) { print "unreadable"; exit }
    for (p = 0; p < want; p++) { if (!(p in se) || !(p in en)) { print "unreadable"; exit } }
    for (p = 0; p < want; p++) if (en[p] + 0 > se[p] + 0) print p ":" se[p] ":" en[p]
  }' <(printf '%s\n' "$STARTS") <(printf '%s\n' "$ENDS"))"
case "$NONEMPTY" in *unreadable*) echo "FAIL: offsets of $TOPIC could not be read for all $PARTITIONS partitions: $ENDS" >&2; exit 1 ;; esac
if [ "$AVRO_LIVE" = yes ]; then
  # Jenkins cannot stop the feed, so the AVRO feed is writing while the topic is recreated (the order the 2026-10-18 roll-forward
  # settled on: the Avro writer first, then the wipe). Emptiness is therefore not required; what is required is that nothing in
  # the log is JSON: in EVERY non-empty partition the first and the last record must be Confluent-framed Avro under id 3001 (or a
  # tombstone). A sample, not a scan; the guarantee that no JSON writer exists is the old pod being gone, which the operator asserts.
  if [ -n "$NONEMPTY" ]; then
    CC="$(rf_resolve_cli_named kafka-console-consumer "$BIN")"
    while IFS=: read -r part first end; do
      for off in "$first" "$((end - 1))"; do
        rc=0; rf_wire_is_avro "$CC" "$BS" "$TOPIC" "$part" "$off" || rc=$?
        # fail CLOSED: only an explicit Avro (or tombstone) verdict passes; "not Avro" and "could not be read" both stop the run
        [ "$rc" -eq 0 ] || { echo "FAIL: the record at partition $part offset $off of $TOPIC is not verifiably Avro (verdict $rc: 1 = not Avro, a JSON writer is live; 2 = unreadable). Resolve it and run --recreate again." >&2; exit 1; }
      done
    done <<< "$NONEMPTY"
  fi
elif [ -n "$NONEMPTY" ]; then
  echo "FAIL: $TOPIC is not empty (partition:earliest:end of the non-empty ones: $(printf '%s' "$NONEMPTY" | tr '\n' ' ')): a writer is live; stop it and run --recreate again (or, if the AVRO feed is the live writer, pass --avro-feed-live)" >&2
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
echo "=== DONE: $TOPIC recreated empty. Next: the Avro reader (feed gateway) is already rolled or rolls now; the Avro writer publishes ==="
