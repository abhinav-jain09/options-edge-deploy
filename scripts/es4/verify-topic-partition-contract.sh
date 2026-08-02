#!/usr/bin/env bash
# verify-topic-partition-contract.sh <mode> — prove that scripts/kafka/topics.env declares the REAL
# partition shape of every es4 topic, so no Kafka Streams app can ever again build its internal
# topics against a transient smaller source.
#
# WHY THIS EXISTS (2026-08-02/03, strike-flow-classifier processed 0 records for 3h):
# apply-topics.sh treats the declared partition count as a MINIMUM — extra partitions are normally
# harmless parallelism. That tolerance is what made an under-declared topic invisible:
#
#   19:19:19  apply-topics creates es.options.opra.tcbbo with numPartitions=4   (topics.env said :4)
#   19:22:39  the owning service's admin client widens the SAME topic 4 -> 32   (its real contract)
#   19:22:50  strike-flow-classifier's Streams, holding metadata from inside that window, creates
#             its 5 repartition/changelog topics at 4
#   19:23:08+ every later rebalance asks for those internal topics at 32 -> TOPIC_ALREADY_EXISTS
#             -> StreamsException "invalid partitions: expected: 32; actual: 4" -> StreamThread DEAD
#
# The pod stayed 1/1 Running with 0 restarts and the consumer group sat in PreparingRebalance
# forever. Kafka cannot SHRINK a topic, so once the internal topics are built small the only repair
# is delete-the-internal-topics + bounce — an operator incident, every single time.
#
# ⭐TWO MODES, BECAUSE ONLY ONE OF THEM CAN SEE THE DEFECT.
#
#   created   Run right after topic reconciliation, apps still at 0. It compares topics that were
#             just CREATED FROM topics.env against topics.env, so on partition counts it is very
#             nearly a TAUTOLOGY: had `tcbbo:4` still been in the file, reconciliation would have
#             made 4 and this mode would have passed — it would NOT have caught the incident. Its
#             real job is narrower and still worth doing: prove reconciliation actually created
#             every declared topic, and catch a client that raced in and made one NARROWER.
#
#   steady    ⭐THE MODE WITH TEETH. Run after the apps are restored and have settled, and by the
#             create-topics action against a live es4. By then every owner has applied its own topic
#             contract, so the broker — not the file — supplies the numbers.
#
#             ⭐POLICY, NOT INFERENCE: for the es4 set, topics.env is the EXACT desired shape, not a
#             floor. A clean-reset recreates the world from this file, so a partition count the file
#             does not record is NOT preserved — it silently reverts on the next reset and rebuilds
#             the wedge. `live != declared` is therefore reported as a DISAGREEMENT to adjudicate,
#             with both remedies printed: record the widening in topics.env if it was deliberate, or
#             fix the topic if it was not. The audit does not claim to know which. (This is why
#             out-of-band widening of an es4 topic is not a supported operation: it does not
#             survive.) This is the state that existed at 19:22:39 on 2026-08-02.
#
# FAIL-CLOSED. Every unknown is a failure, in BOTH modes:
#   * describe exits non-zero, or returns nothing         -> FAIL (never "assume the shape is fine")
#   * a declared topic missing from the broker            -> FAIL (reconciliation did not do its job)
#   * an unparseable describe line                        -> FAIL (an empty parse is not a match)
#   * a malformed or duplicated topics.env entry          -> FAIL (before any broker call)
#   * live < declared                                     -> FAIL (in both modes)
#   * live > declared                                     -> FAIL in `steady` (exact-shape policy);
#                                                            reported but not adjudicated in
#                                                            `created`, where the owners have not
#                                                            applied their contracts yet
#
# ESCAPE HATCH: ES4_ALLOW_PARTITION_DRIFT=true downgrades ONLY validated wider-than-declared drift.
# It is the "yes, I widened it deliberately and I am about to record it" button — not a way to live
# with the disagreement.
# Missing topics, narrower topics, malformed output, malformed declarations and an unreachable
# broker stay fatal — those are not "drift I accept", they are "I do not know what is true".
# Using it means topics.env is WRONG and must be corrected in the same session.

set -euo pipefail

MODE="${1:-}"
case "$MODE" in
  created|steady) ;;
  *) echo "usage: verify-topic-partition-contract.sh <created|steady>" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHIM_DIR="$SCRIPT_DIR/kafka-cli-shim"
BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"   # in-container listener via the shim
ALLOW_DRIFT="${ES4_ALLOW_PARTITION_DRIFT:-false}"

[ -r "$SCRIPT_DIR/../kafka/topics.env" ] || { echo "verify-topic-partition-contract: missing scripts/kafka/topics.env (rsync scripts/ , not just scripts/es4)" >&2; exit 1; }

# The es4 HOST has no Kafka CLI — the shims in kafka-cli-shim/ proxy each call into the container.
# Prepend them only when no kafka-topics is already resolvable, so the same script is exercisable
# wherever a real (or test) CLI exists. On the box nothing named `kafka-topics` is on PATH (the
# tarball ships `kafka-topics.sh`), so the shim is always the one that gets used there.
if ! command -v kafka-topics >/dev/null 2>&1; then
  [ -d "$SHIM_DIR" ] || { echo "verify-topic-partition-contract: no kafka-topics on PATH and no $SHIM_DIR — cannot reach the es4 Kafka CLI" >&2; exit 1; }
  export PATH="$SHIM_DIR:$PATH"
fi

# shellcheck source=/dev/null
source "$SCRIPT_DIR/../kafka/topics.env"
: "${OPTIONS_EDGE_ES4_TOPICS:?OPTIONS_EDGE_ES4_TOPICS missing from topics.env}"

# --- validate the declaration BEFORE trusting it or querying Kafka ---------------------------
# A malformed entry must not silently become a topic named "" or a count of "". A DUPLICATE with a
# different count is worse than either: whichever line the reader happens to keep decides the shape,
# which is precisely the class of ambiguity that produced this incident.
declare -A DECLARED=()
for entry in $OPTIONS_EDGE_ES4_TOPICS; do
  if [[ ! "$entry" =~ ^([^:[:space:]]+):([1-9][0-9]*)$ ]]; then
    echo "MALFORMED declaration in OPTIONS_EDGE_ES4_TOPICS: '$entry' (want name:positive-integer)" >&2
    exit 1
  fi
  name="${BASH_REMATCH[1]}"
  count="${BASH_REMATCH[2]}"
  if [ -n "${DECLARED[$name]:-}" ] && [ "${DECLARED[$name]}" != "$count" ]; then
    echo "CONFLICTING duplicate declaration for $name: ${DECLARED[$name]} vs $count" >&2
    exit 1
  fi
  DECLARED[$name]="$count"
done

# One describe of the whole broker instead of ~99 sequential CLI calls through the docker-exec shim
# (minutes of clean-reset wall clock). Because that single call is now the ONLY evidence, its exit
# status is checked separately from its output: a CLI/shim/docker failure that still prints partial
# stdout must never read as a pass.
set +e
ALL_TOPICS_RAW="$(kafka-topics --bootstrap-server "$BROKER" --describe 2>&1)"
DESCRIBE_STATUS=$?
set -e
if [ "$DESCRIBE_STATUS" -ne 0 ]; then
  echo "verify-topic-partition-contract: broker describe exited $DESCRIBE_STATUS — treating as UNREACHABLE (fail-closed)" >&2
  printf '%s\n' "$ALL_TOPICS_RAW" | head -20 >&2
  exit 1
fi
ALL_TOPICS="$(printf '%s\n' "$ALL_TOPICS_RAW" | sed -n '/^Topic:/p')"
[ -n "$ALL_TOPICS" ] || { echo "verify-topic-partition-contract: broker describe returned no Topic: lines — treating as UNREACHABLE (fail-closed)" >&2; exit 1; }

fatal=0        # missing / narrower / unparseable — never tolerated
wider=0        # live > declared — the under-declaration signal
checked=0

for topic in "${!DECLARED[@]}"; do
  declared="${DECLARED[$topic]}"
  checked=$((checked + 1))

  # Anchor on the whole field. A substring match for es.options.databento.strike-flow would also
  # hit es.options.databento.strike-flow.strike.avro and compare the wrong topic's shape.
  line="$(printf '%s\n' "$ALL_TOPICS" | awk -v t="$topic" '$2 == t {print; exit}')"
  if [ -z "$line" ]; then
    echo "MISSING: declared topic $topic does not exist on the broker" >&2
    fatal=$((fatal + 1))
    continue
  fi

  live="$(printf '%s\n' "$line" | sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p')"
  if [ -z "$live" ]; then
    echo "UNPARSEABLE: cannot read PartitionCount for $topic from: $line" >&2
    fatal=$((fatal + 1))
    continue
  fi

  if [ "$live" -lt "$declared" ]; then
    # Kafka cannot shrink, so this means a racing client created it before reconciliation did.
    echo "NARROWER: $topic declared=$declared live=$live" >&2
    fatal=$((fatal + 1))
  elif [ "$live" -gt "$declared" ]; then
    echo "WIDER-THAN-DECLARED: $topic declared=$declared live=$live" >&2
    wider=$((wider + 1))
  fi
done

echo "verify-topic-partition-contract[$MODE]: checked $checked declared topics (wider-than-declared=$wider fatal=$fatal)"

if [ "$fatal" -gt 0 ]; then
  echo "es4 TOPIC CONTRACT: $fatal topic(s) missing, narrower than declared, or unreadable — fail-closed." >&2
  echo "ES4_ALLOW_PARTITION_DRIFT does NOT cover these: they mean the reconciliation did not produce" >&2
  echo "the declared world, not that the declared world is merely stale." >&2
  exit 1
fi

if [ "$wider" -eq 0 ]; then
  exit 0
fi

if [ "$MODE" = "created" ]; then
  # Right after reconciliation nothing should be wider than declared yet, but a topic that already
  # existed and was left alone legitimately can be. It is not proof of a contract defect at this
  # point in the reset — `steady` is where that judgement is made, with the owners' contracts applied.
  echo "NOTE: $wider topic(s) are wider than declared before the apps started; deferring the verdict to the steady-state audit" >&2
  exit 0
fi

cat >&2 <<EOF

es4 TOPIC PARTITION CONTRACT DISAGREES WITH THE BROKER ON $wider TOPIC(S).

Each one above is live-WIDER than scripts/kafka/topics.env declares. For the es4 set that file is the
EXACT desired shape, because a clean-reset rebuilds the world from it: a width it does not record is
not preserved. So this state cannot simply be left alone — ADJUDICATE it:

  (a) the widening was deliberate -> record the live count in topics.env (OPTIONS_EDGE_ES4_TOPICS)
      and merge, otherwise the next reset drops the topic back to the declared size; or
  (b) it was not deliberate -> the topic is wrong, and it was almost certainly widened by its owning
      service because the contract under-declared it.

Either way the failure mode if it is ignored is the same one this check exists for: the next reset
creates the topic at the SMALL declared size, the owner widens it seconds later, and any Kafka
Streams app that reads its metadata in between builds its repartition/changelog topics at the small
size. Those apps then come up 1/1 Running and healthy and process nothing, forever (2026-08-02:
strike-flow-classifier, 0 records for 3h).

If an app is already wedged, repair it per the runbook: scale that ONE app to 0, delete only the
internal topics whose names start with its Kafka Streams application.id (its *-repartition and
*-changelog topics — never a broker-wide wildcard), then scale back to 1.

To proceed anyway (deliberate widening, recording it now): ES4_ALLOW_PARTITION_DRIFT=true
EOF

if [ "$ALLOW_DRIFT" = "true" ]; then
  echo "ES4_ALLOW_PARTITION_DRIFT=true — continuing despite $wider wider-than-declared topic(s); topics.env still has to be updated or the next reset reverts them" >&2
  exit 0
fi
exit 1
