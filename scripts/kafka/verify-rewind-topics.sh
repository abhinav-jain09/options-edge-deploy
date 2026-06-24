#!/usr/bin/env bash
# Exact-shape verification for the Off-Market Rewind Kafka topics.
#
# Codex flagged that the general scripts/kafka/verify-topics.sh accepts
# existing topics with MORE partitions than the manifest requests
# ("accepting existing larger partition count"). That's intentionally
# tolerant for normal topics but violates the Rewind journal invariant:
# options.rewind.journal MUST be EXACTLY 1 partition (the service's
# Journal.hydrate refuses to start otherwise — it uses single-partition
# ordering as its consistency model, plus cleanup.policy=compact for
# keyed-by-run_id state).
#
# This script does exact-shape verification for both rewind topics and
# is fail-closed. It MUST run on every rewind deploy, independent of
# whether APPLY_TOPICS is toggled — a topic-shape regression silently
# corrupts the journal.
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"

verify_exact() {
  local topic="$1"
  local expected_partitions="$2"
  local expected_cleanup_substr="$3"  # substring that MUST appear in cleanup.policy

  echo "Verifying rewind topic: $topic"

  if ! kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
       --list 2>/dev/null | grep -qx "$topic"; then
    echo "FAIL: topic $topic does not exist" >&2
    exit 1
  fi

  local describe
  describe="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                            --describe --topic "$topic")"
  echo "$describe"
  local partitions
  partitions="$(echo "$describe" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
  if [[ -z "$partitions" ]]; then
    echo "FAIL: could not parse PartitionCount for $topic" >&2
    exit 1
  fi
  if [[ "$partitions" != "$expected_partitions" ]]; then
    echo "FAIL: $topic has partitions=$partitions, MUST be exactly $expected_partitions" >&2
    echo "      (rewind service.Journal.hydrate enforces single-partition; resizing would corrupt journal ordering)" >&2
    exit 1
  fi

  # cleanup.policy check — only journal needs compact. Pass empty string
  # to skip the check (e.g. for audit which is delete-only).
  if [[ -n "$expected_cleanup_substr" ]]; then
    local config
    config="$(kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                             --entity-type topics --entity-name "$topic" \
                             --describe)"
    if ! echo "$config" | grep -q "cleanup.policy=[a-z,]*${expected_cleanup_substr}"; then
      echo "FAIL: $topic cleanup.policy MUST include '$expected_cleanup_substr'" >&2
      echo "$config" >&2
      exit 1
    fi
  fi

  echo "OK: $topic partitions=$partitions cleanup_policy_check=$expected_cleanup_substr"
}

# options.rewind.journal — single partition, must include 'compact' in
# cleanup.policy (Journal.hydrate uses compaction-keyed-by-run_id).
verify_exact "options.rewind.journal" 1 "compact"

# options.rewind.audit — single partition. cleanup.policy is delete
# (default); no specific substring required. Single partition for
# log-locality + key-by-run_id ordering even though audit is
# delete-cleanup.
verify_exact "options.rewind.audit" 1 ""

echo "Rewind topic shape verification PASSED"
