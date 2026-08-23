#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
: "${ENVIRONMENT:?ENVIRONMENT is required}"
if [[ "${KAFKA_CLEANUP_TOPICS:-false}" != "true" ]]; then
  echo "Kafka cleanup disabled. Set KAFKA_CLEANUP_TOPICS=true to enable."
  exit 0
fi
if [[ "$ENVIRONMENT" == "production" && "${ALLOW_PROD_KAFKA_CLEANUP:-false}" != "true" ]]; then
  echo "Refusing production Kafka cleanup without ALLOW_PROD_KAFKA_CLEANUP=true" >&2
  exit 1
fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"
# TOPIC_SET is REFUSED rather than handled. The approved list below is built from the default
# OPTIONS_EDGE_TOPICS with no topic-set switching, so under TOPIC_SET=es4 this script would iterate
# the SPX topics against an es4 broker -- deleting the wrong cluster's data.
#
# This refusal is deliberately the FIRST thing after the declarations are sourced. It used to sit
# below the approved-list construction, which was harmless only because nothing read the list in
# between -- and the prod-only merge added below has a predicate ("production AND the default topic
# set", the same one apply-topics.sh uses) whose second half is now true by construction instead of
# by reading order.
if [[ -n "${TOPIC_SET:-}" ]]; then
  echo "Refusing cleanup with TOPIC_SET='$TOPIC_SET': this script only handles the default topic set." >&2
  echo "Its approved-topic list comes from OPTIONS_EDGE_TOPICS and does not switch on TOPIC_SET." >&2
  exit 1
fi

# --- PROD-ONLY declarations are APPROVED on production, and only there --------------------------
# The approved list is what tells the KAFKA_DELETE_UNWANTED_TOPICS loop below which topics the repo
# actually declares. It was built from OPTIONS_EDGE_TOPICS alone, so every topic declared ONLY for
# production -- OPTIONS_EDGE_PROD_ONLY_TOPICS, which apply-topics.sh merges under exactly this
# predicate -- was matched by neither the approved list nor PROTECTED_TOPIC_REGEX, and a production
# cleanup with KAFKA_DELETE_UNWANTED_TOPICS=true deleted it as "unwanted": the one environment those
# topics exist on is the one environment that swept them.
#
# WHY IT MATTERS EVEN THOUGH APPLY-TOPICS RUNS LATER IN THE SAME BUILD. The Kafka Cleanup stage is
# gated on (DEPLOY_TARGET=all && KAFKA_CLEANUP_TOPICS && !DEPLOY_DRY_RUN) while the Kafka Topics
# stage that would recreate them is additionally gated on !SKIP_KAFKA_TOPICS. So a
# SKIP_KAFKA_TOPICS=true "code-only redeploy" with cleanup enabled deletes them and never recreates
# them, leaving the producing service's own auto-create to stamp broker defaults. Two of the three
# (es.futures.cvd.levels, options.es-cvd-spx-levels) are declared PURE COMPACT + retention.ms=-1 --
# a last-value-per-key view that plain-delete defaults silently destroy. That is the same trap the
# compaction comments in topics.env document, reached through the cleanup path instead.
#
# EXACT PREDICATE, matching apply-topics.sh: ENVIRONMENT EXPLICITLY 'production'. ENVIRONMENT is a
# hard requirement at the top of this script, so there is no unset case to fail closed on; anything
# other than 'production' leaves the prod-only topics unapproved, which is correct -- on dev they
# are genuinely undeclared and SHOULD be swept.
#
# The other PROD_ONLY_* sets (pure-compact, exact-partition, retention overrides) are deliberately
# NOT consulted: this script only ever asks "is this topic declared?", never what shape it should
# have. The prod-only RESET-PRESERVED list is already handled, by reset-preserved-topics.sh below.
#
# CONSEQUENCE, STATED ON PURPOSE: this merges into OPTIONS_EDGE_TOPICS, so on production the
# prod-only topics also become subject to the two DESTRUCTIVE modes below, exactly like every other
# declared topic -- delete-recreate deletes them, retention mode shrinks them, unless they are
# reset-preserved. That is the intended reading, not a side effect: "declared on production" has to
# mean one thing. All three are classified RESET-REBUILDABLE in topics.env (their producing services
# reconstruct them), which is the same classification their base-declared twins carry -- and the
# Kafka Topics stage restores the declared shape in the same build. Approving a topic for protection
# while exempting it from management would create a half-declared topic, which is the shape of the
# bug this change is fixing.
if [[ "$ENVIRONMENT" == "production" ]]; then
  echo "[cleanup-topics] ENVIRONMENT=production: prod-only topics are APPROVED (${OPTIONS_EDGE_PROD_ONLY_TOPICS:-<none>})"
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"
else
  echo "[cleanup-topics] ENVIRONMENT='$ENVIRONMENT': prod-only topics (${OPTIONS_EDGE_PROD_ONLY_TOPICS:-<none>}) are NOT approved here and remain sweepable -- they are declared for production only."
fi

approved_file="$(mktemp)"
trap 'rm -f "$approved_file"' EXIT
for entry in $OPTIONS_EDGE_TOPICS; do echo "${entry%%:*}" >> "$approved_file"; done

# Topics declared RESET-PRESERVED hold data that cannot be rebuilt, and this script's destructive
# modes must skip them.
#
# Driven by OPTIONS_EDGE_RESET_PRESERVED_TOPICS, NOT by retention.ms=-1. The two are different
# claims: retention=-1 says "do not age this out by time", preservation says "this cannot be
# reconstructed". options.spx.spot-vol-regime.current needs the first and must NOT get the second --
# it is live state its service republishes seconds after starting, and inferring preservation from
# retention would have made dev unable to wipe it.
#
# PROTECTED_TOPIC_REGEX does not cover this: it is consulted only when deleting UNWANTED topics.
# Both destructive paths below iterate the APPROVED list, where every preserved topic also appears.
#
# shellcheck source=/dev/null
. "$SCRIPT_DIR/reset-preserved-topics.sh"
durable_file="$(mktemp)"
trap 'rm -f "$approved_file" "$durable_file"' EXIT
printf '%s\n' $RESET_PRESERVED_TOPICS | sort -u > "$durable_file"
is_durable() { grep -qx "$1" "$durable_file"; }
if [[ -s "$durable_file" ]]; then
  echo "Durable topics exempt from destructive cleanup: $(tr '\n' ' ' < "$durable_file")"
fi

describe_topic() {
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$1" 2>/dev/null | sed -n '/^Topic:/p' || true
}

topic_id_from_description() {
  echo "$1" | head -1 | sed -n 's/.*TopicId: \([^[:space:]]*\).*/\1/p'
}

wait_for_topic_absent() {
  local topic="$1"
  local previous_topic_id="${2:-}"
  local attempts="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"

  if [[ -z "$previous_topic_id" ]]; then
    echo "Topic $topic was absent before cleanup; no delete wait needed."
    return 0
  fi

  for ((i = 1; i <= attempts; i++)); do
    description="$(describe_topic "$topic")"
    if [[ -z "$description" ]]; then
      return 0
    fi
    current_topic_id="$(topic_id_from_description "$description")"
    if [[ -n "$previous_topic_id" && -n "$current_topic_id" && "$current_topic_id" != "$previous_topic_id" ]]; then
      echo "Topic $topic was recreated while waiting for deletion; cleanup will let apply-topics validate/recreate it."
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for deleted topic to disappear: $topic" >&2
  describe_topic "$topic"
  return 1
}

if [[ "${KAFKA_DELETE_UNWANTED_TOPICS:-false}" == "true" ]]; then
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list | while read -r topic; do
    if [[ "$topic" =~ $PROTECTED_TOPIC_REGEX ]]; then
      echo "Keeping protected topic: $topic"
    elif grep -qx "$topic" "$approved_file"; then
      echo "Keeping approved topic: $topic"
    else
      echo "Deleting unwanted topic: $topic"
      previous_topic_id="$(topic_id_from_description "$(describe_topic "$topic")")"
      kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic"
      wait_for_topic_absent "$topic" "$previous_topic_id"
    fi
  done
fi
if [[ "${KAFKA_CLEANUP_MODE:-retention}" == "delete-recreate" ]]; then
  declare -A previous_topic_ids
  while read -r topic; do
    previous_topic_ids["$topic"]="$(topic_id_from_description "$(describe_topic "$topic")")"
  done < "$approved_file"
  while read -r topic; do
    if [[ -z "${previous_topic_ids[$topic]:-}" ]]; then
      echo "Approved app topic absent before cleanup: $topic"
      continue
    fi
    if is_durable "$topic"; then
      echo "Keeping DURABLE approved topic (retention.ms=-1): $topic"
      continue
    fi
    echo "Deleting approved app topic: $topic"
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic" || true
  done < "$approved_file"
  while read -r topic; do
    if is_durable "$topic"; then
      continue
    fi
    echo "Waiting for deleted approved app topic to disappear: $topic"
    wait_for_topic_absent "$topic" "${previous_topic_ids[$topic]:-}"
  done < "$approved_file"
else
  while read -r topic; do
    if is_durable "$topic"; then
      echo "Keeping DURABLE approved topic (retention.ms=-1), not shrinking: $topic"
      continue
    fi
    echo "Temporarily shrinking retention for: $topic"
    kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --alter --add-config retention.ms=1000 || true
  done < "$approved_file"
fi
