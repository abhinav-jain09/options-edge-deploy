#!/usr/bin/env bash
# verify-avro-rollforward.sh — the fail-closed barrier for the 2026-10-18 Avro roll-forward.
#
# WHAT IT PROVES, and why that is the right property.
#
# The Avro cutovers ROLLED THEIR TOPICS FORWARD: each topic was deleted and recreated under its own
# name, so the hand-built JSON it used to hold is gone. That deletion is load-bearing, not tidiness.
# gamma-ladder-path-service reads the oi-status topic as a Kafka Streams source with an AVRO serde
# and no explicit auto.offset.reset, so Streams' EARLIEST default replays the topic from its
# beginning after any state loss; on dev and production the topic is retention.ms=-1, so nothing
# would ever have aged the JSON out. Rolling the WORKLOADS forward onto a topic that still holds that
# JSON is exactly the case the Kafka Serialization Rule forbids ("never deploy an Avro consumer that
# can encounter retained JSON values"), and its symptom is a dead source or an empty panel, not an
# error.
#
# So this refuses to let that happen. For EVERY topic the named cluster declares
# (scripts/kafka/avro-rollforward.lib.sh), it requires the topic to EXIST, to carry the declared
# partition count and configs, and to carry a topic id that is NOT its pre-cutover id.
#
# ABSENT IS A FAILURE, not a pass. An earlier cut of this script admitted an absent topic on the
# reasoning that a topic which does not exist holds no JSON. True, and beside the point: absence also
# results from a half-finished or failed roll-forward, and a barrier that certifies "rolled forward"
# must not certify "nothing is there" (Codex round 2). The remedy is the same documented step either
# way — roll-forward-avro-cutover-topics.sh --recreate creates an absent topic.
#
# Usage: verify-avro-rollforward.sh --cluster dev|production|es4 <bootstrap-servers> [kafka-bin-dir]
# Exits non-zero on ANY unverifiable condition, a broker it cannot reach included.
set -euo pipefail

usage() {
  echo "usage: verify-avro-rollforward.sh --cluster dev|production|es4 <bootstrap-servers> [kafka-bin-dir]" >&2
  echo "       the cluster is REQUIRED: each broker carries a different topic set, and inferring it" >&2
  echo "       from what exists would make an absent topic look inapplicable" >&2
  exit 2
}
[ "${1:-}" = "--cluster" ] || usage
CLUSTER="${2:-}"; [ -n "$CLUSTER" ] || usage
BS="${3:-${KAFKA_BOOTSTRAP_SERVERS:-}}"; [ -n "$BS" ] || usage
BIN="${4:-${KAFKA_BIN_DIR:-}}"

# shellcheck source=/dev/null
. "$(dirname "${BASH_SOURCE[0]}")/avro-rollforward.lib.sh"

KT="$(rf_resolve_cli "$BIN")"
GO="$(rf_resolve_cli_named kafka-get-offsets "$BIN")"
CC="$(rf_resolve_cli_named kafka-console-consumer "$BIN")"
SPECS="$(rf_specs "$CLUSTER")"
LIST="$(rf_list "$KT" "$BS")"

echo "=== verify-avro-rollforward on $BS (cluster $CLUSTER) ==="
fail=0
while IFS='|' read -r -u 3 topic parts cfgs pre_id; do
  [ -n "$topic" ] || continue
  if ! printf '%s\n' "$LIST" | grep -qx "$topic"; then
    echo "FAIL: $topic does not exist on $BS. The roll-forward has not completed here — run" >&2
    echo "      scripts/kafka/roll-forward-avro-cutover-topics.sh --recreate --cluster $CLUSTER" >&2
    fail=1
    continue
  fi
  desc="$(rf_describe "$KT" "$BS" "$topic")" || { fail=1; continue; }
  id="$(rf_topic_id "$desc" "$topic")" || { fail=1; continue; }
  if [ "$pre_id" != "-" ] && [ "$id" = "$pre_id" ]; then
    echo "FAIL: $topic still carries its PRE-CUTOVER topic id $id — it is the original JSON-bearing" >&2
    echo "      topic and has NOT been rolled forward. Run" >&2
    echo "      scripts/kafka/roll-forward-avro-cutover-topics.sh --recreate --cluster $CLUSTER" >&2
    echo "      first; rolling the workloads now would replay JSON into an Avro deserializer." >&2
    fail=1
    continue
  fi
  # The SHAPE too, from the SAME describe (one broker call per topic): a topic recreated by broker
  # auto-create carries a new id and the cluster default,
  # which would pass an id-only check while being the wrong topic (32 partitions for a 1-partition
  # ledger read by assign(partition 0) hides every record that hashed elsewhere).
  got_parts="$(printf '%s' "$desc" | sed -nE 's/.*PartitionCount: ([0-9]+).*/\1/p')"
  if [ "$got_parts" != "$parts" ]; then
    echo "FAIL: $topic has $got_parts partition(s), declared $parts: $desc" >&2
    fail=1
    continue
  fi
  # EXACT key=value comparison, never a substring grep: an unanchored `grep -q retention.ms=-1`
  # also matches `retention.ms=-10`, and `min.insync.replicas=1` matches `...=10`, so the barrier
  # would certify the wrong broker setting (Codex). topic-config-parse.sh's extract() is not usable
  # here either — it parses the SPACE-separated `kafka-configs --describe` output and this is the
  # COMMA-separated Configs field of a topic describe, where `[^ ]*` swallows every later key.
  # rf_config_value handles both, including a value that itself contains a comma
  # (cleanup.policy=compact,delete).
  shape_ok=1
  while IFS='=' read -r want_key want_val; do
    [ -n "$want_key" ] || continue
    got="$(rf_config_value "$desc" "$want_key")"
    [ "$got" = "$want_val" ] \
      || { echo "FAIL: $topic has $want_key='$got', declared '$want_val': $desc" >&2; shape_ok=0; }
  done <<<"$(rf_split_declared "$cfgs")"
  [ "$shape_ok" -eq 1 ] || { fail=1; continue; }
  # THE WIRE FORMAT, which is the property the whole barrier is for. The topic id proves the
  # ORIGINAL JSON-bearing topic is gone; it does NOT prove nothing has written JSON since, and on
  # 2026-10-18 that gap was real — the dev topic was recreated at 15:10 and the still-running
  # pre-cutover image wrote 31 JSON heartbeats into it before the new image rolled. The id was
  # post-wipe throughout, so an id-only barrier passed over exactly the retained JSON the Kafka
  # Serialization Rule forbids. One record per NON-EMPTY partition, so an empty 32-partition topic
  # costs two offset queries rather than 32 consumer timeouts.
  parts_checked=0
  wire_ok=1
  # Captured with an explicit status check, NOT inlined as `<<<"$(rf_nonempty_partitions ...)"`:
  # command substitution discards the function's exit status, so an offsets failure would arrive as
  # empty output and read as "no non-empty partitions" — a pass over an unverified topic. The
  # barrier's own test caught this, which is the same class of bug as reading a `--list` failure as
  # absence.
  if ! nonempty="$(rf_nonempty_partitions "$GO" "$BS" "$topic" "$parts")"; then
    echo "FAIL: $topic — could not determine which partitions hold records, so the wire format is" >&2
    echo "      unverified. Refusing rather than assuming the topic is empty." >&2
    fail=1
    continue
  fi
  # BOTH ENDS of each non-empty partition, not just the earliest. An earliest-only check passes a
  # partition that begins with Avro and is followed by retained JSON — the exact shape a stale writer
  # produces when it starts writing AFTER the first good record (Codex). The start catches a writer
  # that was live when the topic was recreated; the end catches one that is live now.
  while IFS=: read -r pnum p_early p_late; do
    [ -n "$pnum" ] || continue
    for probe in "earliest:$p_early" "latest:$((p_late - 1))"; do
      label="${probe%%:*}"; off="${probe#*:}"
      [ "$off" -ge 0 ] 2>/dev/null || continue
      # `|| wrc=$?` and not a bare call: under `set -e` a non-zero return from the check would abort
      # the script before the diagnosis below could print — the failure would be a silent exit 1.
      wrc=0
      # The LATEST end is probed with the walk-back variant, not with a fixed read. The offset below
      # the high watermark is frequently a transaction commit marker, which no isolation level
      # delivers, so a fixed read there returns nothing and the topic would be failed as
      # unverifiable while every value in it is correct Avro — that refusal blocked the production
      # gamma rollout twice on 19 Oct. The earliest end keeps the fixed read: there, nothing
      # delivered means the record was compacted away, which is evidence of nothing.
      if [ "$label" = "latest" ]; then
        rf_wire_is_avro_at_log_end "$CC" "$BS" "$topic" "$pnum" "$off" "$p_early" || wrc=$?
      else
        rf_wire_is_avro "$CC" "$BS" "$topic" "$pnum" "$off" || wrc=$?
      fi
      case $wrc in
        0) parts_checked=$((parts_checked + 1)) ;;
        1) echo "FAIL: $topic partition $pnum holds a NON-AVRO value at its $label offset ($off)." >&2
           echo "      Something wrote to this topic after it was rolled forward. Stop that writer," >&2
           echo "      then re-run roll-forward-avro-cutover-topics.sh --recreate --cluster $CLUSTER" >&2
           wire_ok=0 ;;
        2) # Compaction can remove the record AT an offset; that is not evidence of anything.
           [ "$label" = "earliest" ] && continue
           echo "FAIL: $topic partition $pnum delivered NO record in the ${RF_LOG_END_PROBE_BACK:-8}" >&2
           echo "      offsets ending at its $label offset ($off), so the wire format at the log end" >&2
           echo "      is unverified. Transaction markers alone cannot fill that window; a writer" >&2
           echo "      that is down with an empty-transaction loop, or a probe window too small for" >&2
           echo "      this topic, can. Refusing rather than assuming Avro." >&2
           wire_ok=0 ;;
      esac
    done
  done <<<"$nonempty"
  if [ "$wire_ok" -ne 1 ]; then
    fail=1
    continue
  fi
  echo "ok  $topic rolled forward (topic id $id, $parts partition(s), $cfgs; $parts_checked log end(s) sampled, all Avro or tombstone)"
done 3<<<"$SPECS"

# Versioned topic names on the broker are REPORTED but do not fail this gate, and the reason is the
# cutover itself. While the rollout is in flight the OLD image is still running and still writing to
# its versioned sibling, so it recreates that topic within seconds of any deletion (dev's broker has
# auto-create on with num.partitions=32). A fatal sweep here would therefore block the one rollout
# that retires the sibling — unsatisfiable exactly when it is needed.
#
# The sibling is removed by `roll-forward-avro-cutover-topics.sh --retire-siblings` AFTER the new
# image is live and nothing writes to it. What stops a versioned name coming BACK is not this sweep
# but tests/test_no_versioned_topic_names.py, which fails if any declaration, regex or Kubernetes
# manifest names one, and contracts' KafkaTopicNames, which refuses one at resolution. Those cover
# the only two paths by which a versioned name can reach a client.
if versioned="$(printf '%s\n' "$LIST" | grep -E '\.v[0-9]+$' || true)"; [ -n "$versioned" ]; then
  printf '      %s\n' $versioned >&2
  if [ "${RF_ALLOW_VERSIONED_SIBLINGS:-0}" = "1" ]; then
    echo "NOTE: versioned topic name(s) present on $BS, ALLOWED by RF_ALLOW_VERSIONED_SIBLINGS=1." >&2
    echo "      That opt-out exists only for the cutover window itself, where the old image is still" >&2
    echo "      writing to its sibling and recreates it within seconds of any deletion. Retire them" >&2
    echo "      with --retire-siblings once the new image is live, then stop setting this." >&2
  else
    echo "FAIL: versioned topic name(s) present on $BS. Topics here are rolled forward under their" >&2
    echo "      own name and NO versioned topic may remain (owner, 2026-10-18). Retire them with" >&2
    echo "      roll-forward-avro-cutover-topics.sh --retire-siblings --cluster $CLUSTER." >&2
    echo "      During the cutover window itself, where the old image still recreates its sibling," >&2
    echo "      set RF_ALLOW_VERSIONED_SIBLINGS=1 deliberately for that one run." >&2
    fail=1
  fi
else
  echo "ok  no versioned topic names on $BS"
fi

if [ "$fail" -ne 0 ]; then
  echo "=== verify-avro-rollforward: FAILED ===" >&2
  exit 1
fi
echo "=== verify-avro-rollforward: OK ==="
