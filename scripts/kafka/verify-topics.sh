#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
EXPECTED_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"
for entry in $OPTIONS_EDGE_TOPICS; do
  topic="${entry%%:*}"
  expected_partitions="${entry##*:}"
  echo "Verifying $topic"
  description="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic")"
  echo "$description"
  partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
  replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
  if (( partitions < expected_partitions )); then
    echo "Topic $topic has partition count $partitions, expected at least $expected_partitions" >&2
    exit 1
  fi
  if (( partitions > expected_partitions )); then
    echo "Topic $topic has partition count $partitions, expected minimum $expected_partitions; accepting existing larger partition count."
  fi
  if (( replication_factor < EXPECTED_REPLICATION_FACTOR )); then
    echo "Topic $topic has replication factor $replication_factor, expected at least $EXPECTED_REPLICATION_FACTOR" >&2
    exit 1
  fi
  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --describe
done

# --- PURE-COMPACT enforcement, EVERY environment ---
# The pure-compact check below was written only for OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS,
# so the BASE list -- spx.basis.state and the OI anchor manifest -- was verified NOWHERE, on any
# environment. That is how spx.basis.state came to sit at cleanup.policy=delete on both dev and
# production while topics.env declared it pure compact, undetected: apply-topics reconciles the
# policy, but nothing ever checked that it had.
#
# The consequence is not theoretical. signal-follower's CacheConsumer seekToBeginning()s that
# topic with the comment "compacted: the CURRENT state is history" and then keeps only the
# STATE_CURRENT key. Without compaction it replays every basis update ever written on every
# restart, and that replay grows for as long as the topic exists.
#
# Unconditional on purpose. A contract that only holds on production is not a contract, and the
# environment where this was caught was dev.
# Resolved by TOPIC_SET, the same switch apply-topics.sh makes. Without this the SPX list would
# be verified against the es4 broker -- es4 declares its own topics and, today, NO pure-compact
# ones at all, so the correct es4 behaviour is to check nothing rather than to check the wrong
# cluster's contract. (es4 currently reaches apply-topics via scripts/es4/create-es-topics.sh and
# never runs this script, but a check that is only correct because nobody calls it is not correct.)
case "${TOPIC_SET:-}" in
  "")   PURE_COMPACT_LIST="${OPTIONS_EDGE_PURE_COMPACT_TOPICS:-}" ;;
  es4)  PURE_COMPACT_LIST="${OPTIONS_EDGE_ES4_PURE_COMPACT_TOPICS:-}" ;;
  *)    echo "FAIL: unknown TOPIC_SET='$TOPIC_SET' — refusing to verify against an unresolved declaration" >&2
        exit 1 ;;
esac

pure_compact_fail=0
pure_compact_policy() {
  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics \
    --entity-name "$1" --describe 2>/dev/null \
    | { grep -oE "(^|[[:space:],])cleanup\\.policy=[^ ]*" || true; } | head -1 | sed -E 's/.*cleanup\.policy=//'
}
for topic in $PURE_COMPACT_LIST; do
  if ! kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic" >/dev/null 2>&1; then
    echo "FAIL: pure-compact topic $topic is ABSENT" >&2
    pure_compact_fail=1
    continue
  fi
  policy="$(pure_compact_policy "$topic")"
  if [[ "$policy" != "compact" ]]; then
    echo "FAIL: topic $topic cleanup.policy='${policy:-<none, inherits the broker default>}' but topics.env" >&2
    echo "      declares it PURE COMPACT. 'delete' means it is never compacted, so a consumer that" >&2
    echo "      reads it from the beginning replays a log that grows without bound." >&2
    pure_compact_fail=1
  fi
done
if [[ "$pure_compact_fail" -ne 0 ]]; then
  echo "[verify-topics] pure-compact contract FAILED" >&2
  exit 1
fi
echo "[verify-topics] pure-compact contract OK ($(echo $PURE_COMPACT_LIST | wc -w | tr -d ' ') topic(s), TOPIC_SET='${TOPIC_SET:-default}')"

# --- COMPACTED enforcement, EVERY environment ---
# The pure-compact check above covers only the PURE-compact list. The ordinary COMPACTED
# list -- the topics that must be 'compact,delete' -- was verified NOWHERE, which is how
# es.options.indicators.snapshot.current came to sit at cleanup.policy=delete after the
# fleet had "provisioned" it: a declaration in topics.env had been glued to its neighbour
# into one nonsense token, so the topic was silently absent from the compacted list and
# es.drop.outcome silently lost the compaction it already had. Every CI validator passed.
#
# This closes the same class the comment above describes, for the other list, and it is
# what makes the contract survive the NEXT cleanup: cleanup-topics.sh either deletes these
# topics or shrinks their retention, and apply-topics.sh re-applies the policy immediately
# afterwards -- but nothing ever CHECKED that the re-application produced the declared
# policy. A partition change takes the same reconcile path and is covered by the same check.
case "${TOPIC_SET:-}" in
  "")   COMPACTED_LIST="${OPTIONS_EDGE_COMPACTED_TOPICS:-}" ;;
  es4)  COMPACTED_LIST="${OPTIONS_EDGE_ES4_COMPACTED_TOPICS:-}" ;;
esac

compacted_fail=0
for topic in $COMPACTED_LIST; do
  if ! kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic" >/dev/null 2>&1; then
    echo "FAIL: compacted topic $topic is ABSENT" >&2
    compacted_fail=1
    continue
  fi
  policy="$(pure_compact_policy "$topic")"
  case "$policy" in
    *compact*) ;;
    *)
      echo "FAIL: topic $topic cleanup.policy='${policy:-<none, inherits the broker default>}' but" >&2
      echo "      topics.env declares it COMPACTED. Without compaction the latest value per key is" >&2
      echo "      dropped once retention elapses, so a consumer that reads the topic for CURRENT" >&2
      echo "      state finds nothing." >&2
      compacted_fail=1
      ;;
  esac
done
if [[ "$compacted_fail" -ne 0 ]]; then
  echo "[verify-topics] compacted contract FAILED" >&2
  exit 1
fi
echo "[verify-topics] compacted contract OK ($(echo $COMPACTED_LIST | wc -w | tr -d ' ') topic(s), TOPIC_SET='${TOPIC_SET:-default}')"

# --- PROD-ONLY contract enforcement (VIX feed separation design §6.6, r2 finding 14) ---
# Same exact predicate as apply-topics.sh: ENVIRONMENT EXPLICITLY 'production' AND the
# default (non-es4) topic set. Unset/empty ENVIRONMENT fails CLOSED (checks skipped with
# a notice) — the load-kafka-settings.sh default is deliberately not trusted here.
# Under the predicate this stage FAILS (exit 1) on: a prod-only topic absent; partition
# count != declared for the exact-partition topics; cleanup.policy of a pure-compact
# topic not 'compact'; a retention override not matching. Everything else keeps the
# existing at-least/tolerant behavior above.
if [[ -z "${TOPIC_SET:-}" && "${ENVIRONMENT:-}" == "production" ]]; then
  echo "[verify-topics] ENVIRONMENT=production: enforcing the prod-only topic contract"
  prod_only_fail=0
  ALL_DECLARED_TOPICS="$OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"

  declared_partitions() {
    local t="$1" e
    for e in $ALL_DECLARED_TOPICS; do
      if [[ "${e%%:*}" == "$t" ]]; then echo "${e##*:}"; return 0; fi
    done
    return 1
  }
  describe_partitions() {
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$1" 2>/dev/null \
      | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p'
  }
  topic_config_value() {
    # First match wins: the dynamic per-topic config line, e.g. "cleanup.policy=compact".
    # No comma in the exclusion set — "compact,delete" must come back whole.
    kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics \
      --entity-name "$1" --describe 2>/dev/null | grep -oE "$2=[^ ]*" | head -1 | cut -d= -f2
  }

  for entry in ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}; do
    topic="${entry%%:*}"
    if ! kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic" >/dev/null 2>&1; then
      echo "FAIL: prod-only topic $topic is ABSENT" >&2
      prod_only_fail=1
    fi
  done

  for topic in ${OPTIONS_EDGE_PROD_ONLY_EXACT_PARTITION_TOPICS:-}; do
    if ! expected_exact="$(declared_partitions "$topic")"; then
      echo "FAIL: $topic is in the prod-only exact-partition list but has no declared partition count" >&2
      prod_only_fail=1
      continue
    fi
    actual="$(describe_partitions "$topic")"
    if [[ -z "$actual" ]]; then
      echo "FAIL: exact-partition topic $topic is ABSENT (expected EXACTLY $expected_exact partitions)" >&2
      prod_only_fail=1
    elif [[ "$actual" != "$expected_exact" ]]; then
      echo "FAIL: topic $topic has partitions=$actual but requires EXACTLY $expected_exact (single-partition read contract)" >&2
      prod_only_fail=1
    fi
  done

  for topic in ${OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS:-}; do
    policy="$(topic_config_value "$topic" 'cleanup\.policy')"
    if [[ "$policy" != "compact" ]]; then
      echo "FAIL: topic $topic cleanup.policy='${policy:-<none>}' but must be exactly 'compact' (pure compact — latest value survives forever)" >&2
      prod_only_fail=1
    fi
  done

  # Independent DELETE-policy enforcement for the non-compacted prod-only topics (Codex
  # round-1 finding 3): the shadow topic must be plain 'delete' — a compacted shadow
  # would silently keep last-value-per-key forever and no longer be the throwaway
  # differential stream design §6 declares. Any prod-only topic NOT in a compact list
  # must verify as cleanup.policy=delete.
  is_prod_only_compact() {
    local t="$1" c
    for c in ${OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS:-} ${OPTIONS_EDGE_PURE_COMPACT_TOPICS:-} ${OPTIONS_EDGE_COMPACTED_TOPICS:-}; do
      [[ "$t" == "$c" ]] && return 0
    done
    return 1
  }
  for entry in ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}; do
    topic="${entry%%:*}"
    if ! is_prod_only_compact "$topic"; then
      policy="$(topic_config_value "$topic" 'cleanup\.policy')"
      if [[ "$policy" != "delete" ]]; then
        echo "FAIL: topic $topic cleanup.policy='${policy:-<none>}' but must be exactly 'delete' (non-compacted prod-only topic)" >&2
        prod_only_fail=1
      fi
    fi
  done

  for entry in ${OPTIONS_EDGE_PROD_ONLY_TOPIC_RETENTION_OVERRIDES:-}; do
    topic="${entry%%=*}"
    expected_ms="${entry#*=}"
    actual_ms="$(topic_config_value "$topic" 'retention\.ms')"
    if [[ "$actual_ms" != "$expected_ms" ]]; then
      echo "FAIL: topic $topic retention.ms='${actual_ms:-<none>}' but the prod-only override declares '$expected_ms'" >&2
      prod_only_fail=1
    fi
  done

  if [[ "$prod_only_fail" -ne 0 ]]; then
    echo "[verify-topics] prod-only topic contract FAILED" >&2
    exit 1
  fi
  echo "[verify-topics] prod-only topic contract OK"
else
  echo "[verify-topics] prod-only contract checks SKIPPED (ENVIRONMENT='${ENVIRONMENT:-<unset>}' not explicitly 'production', or TOPIC_SET='${TOPIC_SET:-}' non-default) — fail-closed by design (VIX feed separation §6)"
fi
