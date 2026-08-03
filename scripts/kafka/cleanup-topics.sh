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
approved_file="$(mktemp)"
trap 'rm -f "$approved_file"' EXIT
for entry in $OPTIONS_EDGE_TOPICS; do echo "${entry%%:*}" >> "$approved_file"; done

# Topics declared retention.ms=-1 are DURABLE: they hold observations that cannot be rebuilt, and
# this script's destructive modes must skip them.
#
# PROTECTED_TOPIC_REGEX does NOT cover this: it is consulted only when deleting UNWANTED topics.
# Both destructive paths below iterate the APPROVED list, where every durable topic also appears,
# so without this they delete exactly the records the -1 declaration promises to keep. For the
# anchor manifest that is unrecoverable in a way a rebuild cannot fix: re-fetching the settled
# open-interest print later returns a LATER print, never the one that was destroyed.
#
# Resolved for the ACTIVE environment: the prod-only sets merge ONLY when ENVIRONMENT is
# explicitly "production", the same condition apply-topics.sh uses. Deriving from every
# *_TOPIC_RETENTION_OVERRIDES line in topics.env instead would exempt underlying.vix.price --
# durable on production only -- from dev's retention cleanup too, quietly stranding dev data on a
# topic that is supposed to be swept.
#
# TOPIC_SET is REFUSED rather than handled. approved_file above is built from the default
# OPTIONS_EDGE_TOPICS with no topic-set switching, so under TOPIC_SET=es4 this script would iterate
# the SPX topics against an es4 broker -- deleting the wrong cluster's data. Reading the es4
# retention overrides here would have implied a capability the rest of the script does not have.
if [[ -n "${TOPIC_SET:-}" ]]; then
  echo "Refusing cleanup with TOPIC_SET='$TOPIC_SET': this script only handles the default topic set." >&2
  echo "Its approved-topic list comes from OPTIONS_EDGE_TOPICS and does not switch on TOPIC_SET." >&2
  exit 1
fi
DURABLE_OVERRIDES="${OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES:-}"
if [[ "${ENVIRONMENT:-}" == "production" ]]; then
  DURABLE_OVERRIDES="$DURABLE_OVERRIDES ${OPTIONS_EDGE_PROD_ONLY_TOPIC_RETENTION_OVERRIDES:-}"
fi
durable_file="$(mktemp)"
trap 'rm -f "$approved_file" "$durable_file"' EXIT
for entry in $DURABLE_OVERRIDES; do
  if [[ "${entry#*=}" == "-1" ]]; then echo "${entry%%=*}"; fi
done | sort -u > "$durable_file"
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
