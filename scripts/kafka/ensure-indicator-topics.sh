#!/usr/bin/env bash
# ensure-indicator-topics.sh — the indicator topics must have the DECLARED shape and policy before
# indicator-service is allowed to produce to them.
#
# Same barrier, same reason as ensure-oi-anchor-topic.sh: apply-topics lives in a DIFFERENT Jenkins
# job from the standalone service-deploy path, and two manually sequenced jobs are not an ordering
# guarantee. What makes this one necessary is not a hypothesis — it is measured:
#
#   * prod, 2026-08-11: options.indicators.snapshot.current had NO dynamic config at all. Not the
#     declared compaction, not the declared 7-day retention. The broker had auto-created it with
#     cluster defaults and it looked configured the whole time.
#   * es4, 2026-08-11: the same topic sat at cleanup.policy=delete because a topics.env declaration
#     had been glued to its neighbour into one nonsense token, so it was silently absent from the
#     compacted list. Every CI validator passed.
#
# snapshot.current is the CURRENT-state topic: the UI and the gateway read the latest value per key
# from it. Without compaction the latest value is dropped once retention elapses and the board goes
# blank while the topic still looks healthy. A wrong partition count is worse still — the service
# reads it with an explicit single-partition assign, so records that hash elsewhere are invisible,
# and that is exactly the shape (32 partitions, broker default) that crash-looped es4 for 20 hours.
#
# RECONCILES rather than only verifying. Refusing to deploy would leave the contract broken with no
# path to fix it except the full monolith pipeline; the declared values here are the same ones
# topics.env declares, so applying them is a no-op whenever the fleet already did its job. Partition
# count is NOT reconciled — Kafka cannot shrink a topic, and silently recreating one would destroy
# history. A partition mismatch therefore FAILS CLOSED and says what to do.
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to reconcile against an unknown cluster}"
PREFIX="${TOPIC_PREFIX:-}"
DRY="${DEPLOY_DRY_RUN:-false}"

kt() { kafka-topics  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }

# topic|partitions|cleanup.policy|retention.ms — byte-for-byte the topics.env declaration.
CONTRACT="
options.indicators.snapshot.current|1|compact,delete|604800000
options.indicators.bars|8|delete|3888000000
options.indicators.repartition|4|delete|
options.indicators.bootstrap.control|1|compact,delete|-1
"

echo "=== ensure-indicator-topics: prefix='${PREFIX}' on $KAFKA_BOOTSTRAP_SERVERS (dry_run=$DRY) ==="

fail=0
while IFS='|' read -r base parts policy retention; do
  [ -n "${base:-}" ] || continue
  topic="${PREFIX}${base}"

  if ! kt --list 2>/dev/null | grep -qxF "$topic"; then
    echo "FAIL: topic '$topic' does not exist."
    echo "      Run the apply-topics / create-topics job for THIS environment first. Deploying now"
    echo "      would let the broker auto-create it with cluster defaults — the 32-partition shape"
    echo "      that crash-looped es4 on MissingSourceTopicException and an invalid-partition"
    echo "      changelog for 20 hours on 2026-08-11."
    fail=1
    continue
  fi

  actual_parts=$(kt --describe --topic "$topic" 2>/dev/null \
    | awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}')
  if [ "${actual_parts:-0}" != "$parts" ]; then
    echo "FAIL: '$topic' has PartitionCount=${actual_parts:-unknown}, declared $parts."
    echo "      NOT reconciled on purpose: Kafka cannot shrink a topic, and recreating it would"
    echo "      destroy the bar history this service accumulates. Fix it deliberately."
    fail=1
    continue
  fi

  # The value is WHITESPACE-delimited, not comma-delimited: "cleanup.policy=compact,delete" is ONE
  # value with a comma inside it (the lesson ensure-oi-anchor-topic.sh records — a class that
  # excluded commas read "compact,delete" as "compact" and waved through what it existed to reject).
  if ! CFG=$(kc --entity-type topics --entity-name "$topic" --describe 2>&1); then
    echo "FAIL: could not read the config of '$topic'. Refusing to deploy on an unverified topic."
    printf '%s\n' "$CFG"
    fail=1
    continue
  fi
  cur_policy=$(printf '%s' "$CFG" | tr ' ' '\n' | sed -n 's/^cleanup\.policy=\(.*\)$/\1/p' | head -1)
  cur_ret=$(printf '%s' "$CFG" | tr ' ' '\n' | sed -n 's/^retention\.ms=\(.*\)$/\1/p' | head -1)

  # kafka-configs splits --add-config on COMMAS, so a value that CONTAINS a comma must be
  # bracketed or "cleanup.policy=compact,delete" is parsed as cleanup.policy=compact plus a bogus
  # config named "delete". apply-topics.sh solves it the same way (kafka_config_value).
  wrap() { case "$1" in \[*\]) printf '%s' "$1";; *,*) printf '[%s]' "$1";; *) printf '%s' "$1";; esac; }

  add=""
  [ "$cur_policy" != "$policy" ] && add="cleanup.policy=$(wrap "$policy")"
  if [ -n "$retention" ] && [ "$cur_ret" != "$retention" ]; then
    [ -n "$add" ] && add="$add,"
    add="${add}retention.ms=$retention"
  fi

  if [ -z "$add" ]; then
    echo "  OK   $topic  partitions=$parts cleanup.policy=$policy retention.ms=${retention:-<broker default>}"
    continue
  fi

  echo "  DRIFT $topic  policy='${cur_policy:-<none>}' retention='${cur_ret:-<none>}' -> $add"
  if [ "$DRY" = "true" ]; then
    echo "        DEPLOY_DRY_RUN=true — not applying."
    continue
  fi
  kc --entity-type topics --entity-name "$topic" --alter --add-config "$add"
  echo "        applied."
done <<< "$CONTRACT"

# --- STREAMS-INTERNAL topics ---------------------------------------------------
# The four topics above are the DECLARED ones. Kafka Streams also creates its own
# changelog and repartition topics, and those killed two environments:
#
#   es4  2026-08-11  indicator-state-changelog at 32 partitions -> invalid-partition
#                    changelog + MissingSourceTopicException, 235 restarts over 20 h
#   dev  2026-08-12  indicator-service-dev-indicator-state-changelog: expected 4,
#                    actual 32 -> Streams PENDING_ERROR then ERROR while the pod
#                    still reported RESTORING. Dead, not idle — and the health
#                    endpoint did not say so.
#
# Streams creates them correctly itself. They come out wrong when the NAME is
# touched first and the broker auto-creates it with cluster defaults (32 here), or
# when a topology's partition count changed and the old topic survived. Neither is
# visible in topics.env, so nothing checked them.
#
# FAIL CLOSED, never auto-delete: a changelog IS the durable store, and deleting it
# is a data-loss decision a human makes deliberately — exactly the stance taken for
# a declared-topic partition mismatch above.
: "${INDICATOR_APP_ID:?INDICATOR_APP_ID unset — cannot name the Streams-internal topics}"
CHANGELOG="${INDICATOR_APP_ID}-indicator-state-changelog"
EXPECT_PARTS=4   # the repartition topic's partition count: the aggregate reads it

if kt --list 2>/dev/null | grep -qxF "$CHANGELOG"; then
  cl_parts=$(kt --describe --topic "$CHANGELOG" 2>/dev/null     | awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}')
  if [ "${cl_parts:-0}" != "$EXPECT_PARTS" ]; then
    echo "FAIL: '$CHANGELOG' has PartitionCount=${cl_parts:-unknown}, expected $EXPECT_PARTS."
    echo "      Streams refuses to run against it: 'Existing internal topic ... has invalid"
    echo "      partitions'. The service will sit in ERROR while its health endpoint still"
    echo "      reports RESTORING, so it looks idle rather than dead."
    echo "      This is NOT auto-repaired: the changelog is the durable store. Delete it"
    echo "      deliberately (the state rebuilds from the checkpoint and the input), then"
    echo "      re-run this deploy:"
    echo "        kafka-topics --bootstrap-server \$KAFKA_BOOTSTRAP_SERVERS --delete --topic $CHANGELOG"
    fail=1
  else
    echo "  OK   $CHANGELOG  partitions=$cl_parts"
  fi
else
  echo "  OK   $CHANGELOG  absent — Streams will create it with the right shape"
fi

if [ "$fail" -ne 0 ]; then
  echo "=== ensure-indicator-topics: FAILED ==="
  exit 1
fi
echo "=== ensure-indicator-topics: OK ==="
