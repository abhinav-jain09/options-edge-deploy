#!/usr/bin/env bash
# verify-topic-partition-contract.sh — prove that every declared es4 topic has EXACTLY the
# partition count scripts/kafka/topics.env declares, at the moment the apps are still down.
#
# WHY THIS EXISTS (2026-08-02/03 incident, strike-flow-classifier processed 0 records for 3h):
# apply-topics.sh deliberately treats the declared partition count as a MINIMUM — extra partitions
# are normally harmless parallelism. That tolerance is what made an under-declared topic invisible:
#
#   19:19:19  apply-topics creates es.options.opra.tcbbo with numPartitions=4   (topics.env said :4)
#   19:22:39  the owning service's admin client widens the SAME topic 4 -> 32   (its real contract)
#   19:22:50  strike-flow-classifier's Streams, holding metadata from inside that window, creates
#             its 5 repartition/changelog topics at 4
#   19:23:08+ every later rebalance asks for those internal topics at 32 -> TOPIC_ALREADY_EXISTS
#             -> StreamsException "invalid partitions: expected: 32; actual: 4" -> StreamThread DEAD
#
# The pod stayed 1/1 Running with 0 restarts and the consumer group sat in PreparingRebalance
# forever. Nothing in the fleet-readiness path can see that: it is a green pod with a dead topology.
# Kafka cannot SHRINK a topic, so once the internal topics are built small the only repair is
# delete-the-internal-topics + bounce — i.e. an operator incident, every single time.
#
# The durable fix is to remove the window entirely: the contract must declare the REAL steady-state
# shape so nothing has to widen anything. This script is the guard that keeps it true. It runs
# AFTER topic reconciliation and BEFORE the apps are restored, which is the one moment where
# "declared == live" is an exact invariant rather than a race.
#
# FAIL-CLOSED. Every unknown is a failure:
#   * broker unreachable / describe fails      -> FAIL (never "assume the shape is fine")
#   * a declared topic missing from the broker -> FAIL (reconciliation did not do its job)
#   * an unparseable describe line             -> FAIL (an empty parse must not read as "match")
#   * live partitions != declared              -> FAIL, naming the topic and both numbers
#
# ESCAPE HATCH: ES4_ALLOW_PARTITION_DRIFT=true downgrades a mismatch to a loud warning. It exists
# so a live incident is never blocked by this check — but using it means topics.env is WRONG and
# must be corrected in the same session, otherwise the next clean-reset rebuilds the same trap.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHIM_DIR="$SCRIPT_DIR/kafka-cli-shim"
BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"   # in-container listener via the shim
ALLOW_DRIFT="${ES4_ALLOW_PARTITION_DRIFT:-false}"

[ -d "$SHIM_DIR" ] || { echo "verify-topic-partition-contract: missing $SHIM_DIR — cannot reach the es4 Kafka CLI" >&2; exit 1; }
[ -r "$SCRIPT_DIR/../kafka/topics.env" ] || { echo "verify-topic-partition-contract: missing scripts/kafka/topics.env (rsync scripts/ , not just scripts/es4)" >&2; exit 1; }

# shellcheck source=/dev/null
source "$SCRIPT_DIR/../kafka/topics.env"
: "${OPTIONS_EDGE_ES4_TOPICS:?OPTIONS_EDGE_ES4_TOPICS missing from topics.env}"

export PATH="$SHIM_DIR:$PATH"

# One describe of the whole broker instead of one per topic: ~85 sequential CLI calls through the
# docker-exec shim is minutes of clean-reset wall clock, and a per-topic loop cannot tell "topic
# absent" apart from "this one call failed".
ALL_TOPICS="$(kafka-topics --bootstrap-server "$BROKER" --describe 2>/dev/null | sed -n '/^Topic:/p')" || true
[ -n "$ALL_TOPICS" ] || { echo "verify-topic-partition-contract: broker describe returned nothing — treating as UNREACHABLE (fail-closed)" >&2; exit 1; }

mismatches=0
missing=0
checked=0

for entry in $OPTIONS_EDGE_ES4_TOPICS; do
  topic="${entry%%:*}"
  declared="${entry##*:}"
  checked=$((checked + 1))

  # Anchor both ends of the name. A bare grep for es.options.databento.strike-flow also matches
  # es.options.databento.strike-flow.strike.avro, which would compare the wrong topic's shape.
  line="$(printf '%s\n' "$ALL_TOPICS" | awk -v t="$topic" '$2 == t {print; exit}')"
  if [ -z "$line" ]; then
    echo "MISSING: declared topic $topic does not exist on the broker" >&2
    missing=$((missing + 1))
    continue
  fi

  live="$(printf '%s\n' "$line" | sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p')"
  if [ -z "$live" ]; then
    echo "UNPARSEABLE: cannot read PartitionCount for $topic from: $line" >&2
    mismatches=$((mismatches + 1))
    continue
  fi

  if [ "$live" != "$declared" ]; then
    echo "DRIFT: $topic declared=$declared live=$live" >&2
    mismatches=$((mismatches + 1))
  fi
done

echo "verify-topic-partition-contract: checked $checked declared topics (drift=$mismatches missing=$missing)"

if [ "$mismatches" -eq 0 ] && [ "$missing" -eq 0 ]; then
  exit 0
fi

cat >&2 <<EOF

es4 TOPIC PARTITION CONTRACT VIOLATED — $mismatches drifted, $missing missing.

A topic that is live-WIDER than declared is not cosmetic: apply-topics.sh created it at the small
declared size, something widened it afterwards, and any Kafka Streams app that read its metadata in
between has already built its repartition/changelog topics at the SMALL size. Those apps come up
1/1 Running and healthy and process nothing, forever.

FIX (do this, do not skip it):
  1. Read the real shape:  kafka-topics --bootstrap-server $BROKER --describe --topic <name>
  2. Correct that topic's entry in scripts/kafka/topics.env (OPTIONS_EDGE_ES4_TOPICS) to the REAL
     count and merge it — the contract is the fix, not the broker.
  3. Repair any app already wedged by it: scale the app to 0, delete its *-repartition and
     *-changelog topics, scale back to 1. Kafka cannot shrink a topic, so there is no gentler path.

To proceed anyway (incident only): ES4_ALLOW_PARTITION_DRIFT=true
EOF

if [ "$ALLOW_DRIFT" = "true" ]; then
  echo "ES4_ALLOW_PARTITION_DRIFT=true — continuing despite the violation above (topics.env is still wrong)" >&2
  exit 0
fi
exit 1
