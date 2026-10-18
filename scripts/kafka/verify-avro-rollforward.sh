#!/usr/bin/env bash
# verify-avro-rollforward.sh — the fail-closed barrier for the 2026-10-18 Avro roll-forward.
#
# WHAT IT PROVES, and why that is the right property.
#
# The Avro cutovers ROLLED THEIR TOPICS FORWARD: each topic was deleted and recreated under its own
# name, so the hand-built JSON it used to hold is gone. That deletion is load-bearing, not tidiness.
# gamma-ladder-path-service reads options.databento.gex.oi-status as a Kafka Streams source with an
# AVRO serde and no explicit auto.offset.reset, so Streams' EARLIEST default replays the topic from
# its beginning after any state loss; both topics are retention.ms=-1, so nothing would ever have
# aged the JSON out. Rolling the WORKLOADS forward onto a topic that still holds that JSON is exactly
# the case the Kafka Serialization Rule forbids ("never deploy an Avro consumer that can encounter
# retained JSON values"), and its symptom is a dead source or an empty panel, not an error.
#
# So this refuses to let that happen. A topic's TOPIC ID is assigned at creation and is a different
# value after any delete-and-recreate. The pre-cutover IDs below were read off the live brokers on
# 2026-10-18, BEFORE the wipe. If a live topic still carries its pre-cutover ID, the original
# JSON-bearing topic is still there and the workloads must not roll.
#
# One metadata read per topic, and it stays correct afterwards: the nightly clean-slate also deletes
# and recreates these topics, which mints another new ID and removes any JSON with it. There is no
# value this check can be satisfied by except "not the topic that held the JSON".
#
# Usage: verify-avro-rollforward.sh <bootstrap-servers> [kafka-bin-dir]
# Exits non-zero on ANY unverifiable condition, including a broker it cannot reach.
set -euo pipefail

BS="${1:-${KAFKA_BOOTSTRAP_SERVERS:-}}"
: "${BS:?usage: verify-avro-rollforward.sh <bootstrap-servers> [kafka-bin-dir]}"
BIN="${2:-${KAFKA_BIN_DIR:-}}"

if [ -n "$BIN" ]; then
  KT="$BIN/kafka-topics.sh"
elif command -v kafka-topics >/dev/null 2>&1; then
  KT="kafka-topics"            # the deploy images put the CLI on PATH without the .sh suffix
elif command -v kafka-topics.sh >/dev/null 2>&1; then
  KT="kafka-topics.sh"
else
  echo "FAIL: no kafka-topics CLI found (pass a bin dir or set KAFKA_BIN_DIR)" >&2
  exit 1
fi

# topic|pre-cutover topic id on dev|pre-cutover topic id on production
# Both are listed because this script runs against either broker and must not need to be told which.
ROLLED_FORWARD=(
  "options.ibkr.gex.status|yCF-rRJwQo-VVdaMHfiUkg|5OTMCxITTtWRiljc0z-kxA"
  "options.databento.gex.oi-status|YevJMOiKSCeVwF5LHm8x1A|75HJ4F03QFy7763nzdSYPA"
)

echo "=== verify-avro-rollforward on $BS ==="
if ! LIST="$("$KT" --bootstrap-server "$BS" --list 2>&1)"; then
  echo "FAIL: cannot list topics on $BS — refusing to certify an unverified cluster:" >&2
  printf '%s\n' "$LIST" >&2
  exit 1
fi

fail=0
for spec in "${ROLLED_FORWARD[@]}"; do
  IFS='|' read -r topic dev_id prod_id <<<"$spec"
  if ! printf '%s\n' "$LIST" | grep -qx "$topic"; then
    # Absent is fine: the ensure scripts create it, and a topic that does not exist holds no JSON.
    echo "ok  $topic absent (will be created by its ensure script)"
    continue
  fi
  desc="$("$KT" --bootstrap-server "$BS" --describe --topic "$topic" 2>/dev/null | head -1)"
  id="$(printf '%s\n' "$desc" | awk -F'\t' '{for(i=1;i<=NF;i++) if ($i ~ /^TopicId: /) {sub(/^TopicId: /,"",$i); print $i}}')"
  if [ -z "$id" ]; then
    echo "FAIL: could not read the topic id of $topic from: $desc" >&2
    fail=1
    continue
  fi
  if [ "$id" = "$dev_id" ] || [ "$id" = "$prod_id" ]; then
    echo "FAIL: $topic still carries its PRE-CUTOVER topic id $id — it is the original" >&2
    echo "      JSON-bearing topic and has NOT been rolled forward. Run" >&2
    echo "      scripts/kafka/roll-forward-avro-cutover-topics.sh against $BS first; rolling the" >&2
    echo "      workloads now would replay JSON into an Avro deserializer." >&2
    fail=1
    continue
  fi
  echo "ok  $topic rolled forward (topic id $id, not the pre-cutover id)"
done

# A versioned topic name must not exist at all: nothing creates, writes or reads one.
if versioned="$(printf '%s\n' "$LIST" | grep -E '\.v[0-9]+$' || true)"; [ -n "$versioned" ]; then
  echo "FAIL: versioned topic name(s) still present on $BS — topics are rolled forward under their" >&2
  echo "      own name and no versioned topic exists:" >&2
  printf '      %s\n' $versioned >&2
  fail=1
else
  echo "ok  no versioned topic names on $BS"
fi

if [ "$fail" -ne 0 ]; then
  echo "=== verify-avro-rollforward: FAILED ===" >&2
  exit 1
fi
echo "=== verify-avro-rollforward: OK ==="
