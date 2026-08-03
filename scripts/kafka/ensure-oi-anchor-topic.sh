#!/usr/bin/env bash
# ensure-oi-anchor-topic.sh — the anchor manifest topic must EXIST, with the right shape, before
# databento-gex-service is allowed to produce to it.
#
# Why this is a deploy barrier and not a note in a runbook. The topic is created by apply-topics,
# which lives in a DIFFERENT Jenkins job from the standalone service-deploy path. Two manually
# sequenced jobs are not an ordering guarantee: skip one, run it against the wrong environment, or
# have it fail quietly, and the service starts producing into a topic the broker auto-creates with
# cluster defaults -- partitioned, "compact,delete", 24h retention.
#
# That failure is silent and expensive. The manifest is keyed per session and its ENTIRE value is
# the difference against the next session's, so a 24h delete drops Friday's record before Monday
# can be compared against it, and the topic still looks configured the whole time. Auto-creation
# also lands multiple partitions, which breaks the publish-once backlog read that a single
# partition makes total.
#
# Fail closed: no topic, or the wrong shape, and the deploy stops.
set -euo pipefail

TOPIC="${KAFKA_OI_ANCHOR_MANIFEST_TOPIC:-options.databento.oi.anchor-manifest}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }

echo "=== ensure-oi-anchor-topic: $TOPIC on $KAFKA_BOOTSTRAP_SERVERS ==="

if ! kt --list 2>/dev/null | grep -qxF "$TOPIC"; then
  echo "FAIL: topic '$TOPIC' does not exist."
  echo "      Run the apply-topics job for THIS environment first, then re-run this deploy."
  echo "      Deploying now would let the broker auto-create it with cluster defaults, which"
  echo "      silently drops each session's record 24h later — exactly the data this captures."
  exit 1
fi

PARTS=$(kt --describe --topic "$TOPIC" 2>/dev/null \
  | awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}')
if [ "${PARTS:-0}" != "1" ]; then
  echo "FAIL: '$TOPIC' has PartitionCount=${PARTS:-unknown}, expected 1."
  echo "      Publish-once reads the whole topic back to decide whether a session already has a"
  echo "      manifest. That read is only total on a single partition."
  exit 1
fi

if ! CFG=$(kc --entity-type topics --entity-name "$TOPIC" --describe 2>&1); then
  echo "FAIL: could not read the config of '$TOPIC'. Refusing to deploy on an unverified topic."
  printf '%s\n' "$CFG"
  exit 1
fi

# The value is delimited by WHITESPACE, not by a comma -- "cleanup.policy=compact,delete" is ONE
# value with a comma inside it. The first version of this check excluded commas from the class, so
# it read "compact,delete" as "compact" and waved through the exact configuration it exists to
# reject. Whitespace-delimited, then compared for exact equality, so "compact,delete" and
# "delete,compact" both fail. Covered by ensure-oi-anchor-topic-parse-test.sh.
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"
policy=$(extract 'cleanup\.policy' "$CFG")
retention=$(extract 'retention\.ms' "$CFG")

# Read the broker's EFFECTIVE value, not just the explicit override: an unset retention.ms inherits
# the cluster default (24h here), and "absent" reads as fine while behaving as a delete.
if [ "${policy:-}" != "compact" ]; then
  echo "FAIL: '$TOPIC' has cleanup.policy='${policy:-<unset>}', expected 'compact' (PURE, no delete)."
  echo "      The delete half drops the record once retention elapses. topics.env declares this"
  echo "      topic in OPTIONS_EDGE_PURE_COMPACT_TOPICS for that reason."
  exit 1
fi
if [ "${retention:-}" != "-1" ]; then
  echo "FAIL: '$TOPIC' has retention.ms='${retention:-<unset, inherits cluster default>}', expected -1."
  exit 1
fi

echo "OK: $TOPIC — partitions=1 cleanup.policy=compact retention.ms=-1"
