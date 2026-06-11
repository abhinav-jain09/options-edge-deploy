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

wait_for_topic_absent() {
  local topic="$1"
  local attempts="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"

  for ((i = 1; i <= attempts; i++)); do
    if ! kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for deleted topic to disappear: $topic" >&2
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic" || true
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
      kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic"
      wait_for_topic_absent "$topic"
    fi
  done
fi
if [[ "${KAFKA_CLEANUP_MODE:-retention}" == "delete-recreate" ]]; then
  while read -r topic; do
    echo "Deleting approved app topic: $topic"
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic" || true
  done < "$approved_file"
  while read -r topic; do
    echo "Waiting for deleted approved app topic to disappear: $topic"
    wait_for_topic_absent "$topic"
  done < "$approved_file"
else
  while read -r topic; do
    echo "Temporarily shrinking retention for: $topic"
    kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --alter --add-config retention.ms=1000 || true
  done < "$approved_file"
fi
