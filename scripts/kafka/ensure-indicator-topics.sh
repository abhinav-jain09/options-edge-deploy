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

kt() { "${KAFKA_TOPICS_CMD:-kafka-topics}"  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { "${KAFKA_CONFIGS_CMD:-kafka-configs}" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }

# topic|partitions|cleanup.policy|retention.ms — byte-for-byte the topics.env declaration.
CONTRACT="
options.indicators.snapshot.current|1|compact,delete|604800000
options.indicators.bars|8|delete|3888000000
options.indicators.repartition|4|delete|
options.indicators.bootstrap.control|1|compact,delete|-1
options.indicators.signals.current|1|compact,delete|604800000
options.indicators.signals.events|1|delete|3888000000
options.indicators.studies.bars|8|delete|3888000000
options.indicators.studies.current|1|compact,delete|604800000
"
# signals.current / signals.events: SIGNALS_V1 (indicator-service spec rev 13 §3) — the same
# producer, the same deploy path, the same two failure modes as snapshot.current/bars above; the
# shapes are the topics.env declarations (1 partition each, exact-partition; 7 d / 45 d).
# studies.bars / studies.current: STUDIES_V1 (Phase A, PHASE-A-INDICATORS-REQUIREMENT.md A-R10.1/A-R10.4) — written only with
# INDICATOR_STUDIES_ENABLED=true, declared and reconciled unconditionally so the flag flip is a one-variable change.

# ---- --recreate: the Avro cutover (INDICATOR-AVRO-CONVERSION-PLAN.md section 5) -------------------------
# Deletes and recreates, EMPTY, exactly the per-environment allow-list below at the shape topics.env declares,
# applied through the same functions apply-topics.sh uses (scripts/kafka/topic-shape.lib.sh), and verifies each
# topic's partitions, policy, retention and end offset 0. Refuses any other name. Run with indicator-service at
# 0 replicas and after reset-indicator-state.sh (the group offsets must be 0 BEFORE the state goes).
#   es4 (RECREATE_ENVIRONMENT=es4):  the six es.options.indicators.* source topics
#   dev / production:                 the six options.indicators.* base topics PLUS the four es.options.indicators.*
#                                     mirror targets, which hold retained JSON copies and are recreated empty before
#                                     any reader of them starts
if [ "${1:-}" = "--recreate" ]; then
  : "${RECREATE_ENVIRONMENT:?RECREATE_ENVIRONMENT unset (dev|production|es4) — the allow-list is environment-qualified}"
  : "${KAFKA_TOPIC_REPLICATION_FACTOR:?KAFKA_TOPIC_REPLICATION_FACTOR unset — source scripts/kafka/load-kafka-settings.sh}"
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  # shellcheck source=/dev/null
  source "${TOPICS_ENV:-$SCRIPT_DIR/topics.env}"
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/topic-shape.lib.sh"
  REPLICATION_FACTOR="$KAFKA_TOPIC_REPLICATION_FACTOR"
  RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"
  CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"
  MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
  BASE6="options.indicators.snapshot.current options.indicators.bars options.indicators.bootstrap.control options.indicators.signals.current options.indicators.signals.events options.indicators.repartition"
  MIRROR4="es.options.indicators.snapshot.current es.options.indicators.bars es.options.indicators.signals.current es.options.indicators.signals.events"
  case "$RECREATE_ENVIRONMENT" in
    es4)        TOPIC_SET=es4; ENVIRONMENT=es4; ALLOW=$(printf '%s\n' $BASE6 | sed 's/^/es./' | tr '\n' ' ') ;;
    dev)        TOPIC_SET=""; ENVIRONMENT=dev; ALLOW="$BASE6 $MIRROR4" ;;
    production) TOPIC_SET=""; ENVIRONMENT=production; ALLOW="$BASE6 $MIRROR4" ;;
    *) echo "FAIL: RECREATE_ENVIRONMENT must be dev, production or es4 (got '$RECREATE_ENVIRONMENT')"; exit 1 ;;
  esac
  export TOPIC_SET ENVIRONMENT
  topic_shape_resolve_declarations
  echo "=== ensure-indicator-topics --recreate: environment=$RECREATE_ENVIRONMENT on $KAFKA_BOOTSTRAP_SERVERS (dry_run=$DRY) ==="
  echo "allow-list ($(echo $ALLOW | wc -w | tr -d ' ') topics): $ALLOW"
  rfail=0
  for topic in $ALLOW; do
    parts=""
    for entry in $OPTIONS_EDGE_TOPICS; do [ "${entry%%:*}" = "$topic" ] && parts="${entry##*:}"; done
    if [ -z "$parts" ]; then echo "FAIL: $topic is not declared in topics.env for $RECREATE_ENVIRONMENT — refusing to recreate an undeclared topic"; rfail=1; continue; fi
    policy="$(topic_cleanup_policy "$topic")"; retention="$(topic_retention_ms "$topic")"
    echo "--- $topic -> partitions=$parts cleanup.policy=$policy retention.ms=$retention rf=$REPLICATION_FACTOR min.isr=$MIN_ISR"
    if [ "$DRY" = true ]; then echo "    DEPLOY_DRY_RUN=true — not deleting, not creating."; continue; fi
    if kt --list 2>/dev/null | grep -qxF "$topic"; then
      kt --delete --topic "$topic"
      wait_for_topic_absent "$topic" || { rfail=1; continue; }
      echo "    deleted"
    else
      echo "    absent"
    fi
    create_topic "$topic" "$parts" || { echo "FAIL: create of $topic failed"; rfail=1; continue; }
    wait_for_topic_shape "$topic" "$parts" "$REPLICATION_FACTOR" || { rfail=1; continue; }
    alter_topic_config "$topic" "$policy" || { echo "FAIL: config of $topic failed"; rfail=1; continue; }
    actual_parts=$(kt --describe --topic "$topic" 2>/dev/null | awk '!done { for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); done = 1 } }')
    CFG=$(kc --entity-type topics --entity-name "$topic" --describe 2>&1 || true)
    cur_policy=$(printf '%s' "$CFG" | tr ' ' '\n' | sed -n 's/^cleanup\.policy=\(.*\)$/\1/p' | head -1)
    cur_ret=$(printf '%s' "$CFG" | tr ' ' '\n' | sed -n 's/^retention\.ms=\(.*\)$/\1/p' | head -1)
    # every declared partition exactly once, each at end offset 0 (a missing partition line is a failure, not an empty set)
    ends=$("${KAFKA_GET_OFFSETS_CMD:-kafka-get-offsets}" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic "$topic" 2>/dev/null | awk -F: -v t="$topic" '$1 == t {print $2"="$3}' | sort -t= -k1,1n | tr '\n' ' ')
    want=$(for ((p = 0; p < parts; p++)); do printf '%s=0 ' "$p"; done)
    if [ "$actual_parts" != "$parts" ] || [ "$cur_policy" != "$policy" ] || [ "$cur_ret" != "$retention" ] || [ "$ends" != "$want" ]; then
      echo "FAIL: $topic after recreate: partitions=$actual_parts policy=$cur_policy retention=$cur_ret endOffsets=[${ends% }] (declared $parts/$policy/$retention, want [${want% }])"
      rfail=1; continue
    fi
    echo "    recreated empty at the declared shape (end offset 0 on every partition)"
  done
  if [ "$rfail" -ne 0 ]; then echo "=== ensure-indicator-topics --recreate: FAILED ==="; exit 1; fi
  echo "=== ensure-indicator-topics --recreate: OK ==="
  exit 0
fi

echo "=== ensure-indicator-topics: prefix='${PREFIX}' on $KAFKA_BOOTSTRAP_SERVERS (dry_run=$DRY) ==="

fail=0
while IFS='|' read -r base parts policy retention; do
  [ -n "${base:-}" ] || continue
  topic="${PREFIX}${base}"

  if ! kt --list 2>/dev/null | grep -qxF "$topic"; then
    # A missing DECLARED topic is created here with its declared shape — the same values the
    # fleet's apply-topics applies (--if-not-exists exactly as apply-topics.sh does, since the
    # admin client's own retry can answer TopicExistsException for a create that succeeded).
    # Refusing used to be the stance, but apply-topics lives in a different job and a FIRST deploy
    # of a new output (signals.current/.events, 2026-10-16) otherwise waits on that job while the
    # broker would auto-create the topic with cluster defaults at the first write — the very drift
    # this barrier exists to prevent. Partition count is still never changed on an EXISTING topic.
    echo "  MISSING $topic  -> create partitions=$parts cleanup.policy=$policy retention.ms=${retention:-<broker default>}"
    if [ "$DRY" = "true" ]; then
      echo "        DEPLOY_DRY_RUN=true — not creating."
      continue
    fi
    : "${KAFKA_TOPIC_REPLICATION_FACTOR:?KAFKA_TOPIC_REPLICATION_FACTOR unset — source scripts/kafka/load-kafka-settings.sh}"
    create_cfg=(--config "cleanup.policy=$policy")
    [ -n "$retention" ] && create_cfg+=(--config "retention.ms=$retention")
    kt --create --if-not-exists --topic "$topic" --partitions "$parts" \
       --replication-factor "$KAFKA_TOPIC_REPLICATION_FACTOR" "${create_cfg[@]}"
    if ! kt --list 2>/dev/null | grep -qxF "$topic"; then
      echo "FAIL: '$topic' still absent after create."
      fail=1
      continue
    fi
    echo "        created."
  fi

  actual_parts=$(kt --describe --topic "$topic" 2>/dev/null \
    | awk '!done { for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); done = 1 } }')
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
  cl_parts=$(kt --describe --topic "$CHANGELOG" 2>/dev/null     | awk '!done { for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); done = 1 } }')
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
