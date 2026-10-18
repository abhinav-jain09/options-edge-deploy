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
SPECS="$(rf_specs "$CLUSTER")"
LIST="$(rf_list "$KT" "$BS")"

echo "=== verify-avro-rollforward on $BS (cluster $CLUSTER) ==="
fail=0
while IFS='|' read -r topic parts cfgs pre_id; do
  [ -n "$topic" ] || continue
  if ! printf '%s\n' "$LIST" | grep -qx "$topic"; then
    echo "FAIL: $topic does not exist on $BS. The roll-forward has not completed here — run" >&2
    echo "      scripts/kafka/roll-forward-avro-cutover-topics.sh --recreate --cluster $CLUSTER" >&2
    fail=1
    continue
  fi
  id="$(rf_topic_id "$KT" "$BS" "$topic")" || { fail=1; continue; }
  if [ "$pre_id" != "-" ] && [ "$id" = "$pre_id" ]; then
    echo "FAIL: $topic still carries its PRE-CUTOVER topic id $id — it is the original JSON-bearing" >&2
    echo "      topic and has NOT been rolled forward. Run" >&2
    echo "      scripts/kafka/roll-forward-avro-cutover-topics.sh --recreate --cluster $CLUSTER" >&2
    echo "      first; rolling the workloads now would replay JSON into an Avro deserializer." >&2
    fail=1
    continue
  fi
  # The SHAPE too: a topic recreated by broker auto-create carries a new id and the cluster default,
  # which would pass an id-only check while being the wrong topic (32 partitions for a 1-partition
  # ledger read by assign(partition 0) hides every record that hashed elsewhere).
  desc="$("$KT" --bootstrap-server "$BS" --describe --topic "$topic" | head -1)"
  got_parts="$(printf '%s' "$desc" | sed -nE 's/.*PartitionCount: ([0-9]+).*/\1/p')"
  if [ "$got_parts" != "$parts" ]; then
    echo "FAIL: $topic has $got_parts partition(s), declared $parts: $desc" >&2
    fail=1
    continue
  fi
  shape_ok=1
  IFS=',' read -ra kvs <<<"$cfgs"
  for kv in "${kvs[@]}"; do
    printf '%s' "$desc" | grep -q -- "$kv" || { echo "FAIL: $topic is missing $kv: $desc" >&2; shape_ok=0; }
  done
  [ "$shape_ok" -eq 1 ] || { fail=1; continue; }
  echo "ok  $topic rolled forward (topic id $id, $parts partition(s), $cfgs)"
done <<<"$SPECS"

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
  echo "NOTE: versioned topic name(s) present on $BS — retire them with"
  echo "      roll-forward-avro-cutover-topics.sh --retire-siblings --cluster $CLUSTER once the new image is live:"
  printf '      %s\n' $versioned
else
  echo "ok  no versioned topic names on $BS"
fi

if [ "$fail" -ne 0 ]; then
  echo "=== verify-avro-rollforward: FAILED ===" >&2
  exit 1
fi
echo "=== verify-avro-rollforward: OK ==="
