#!/usr/bin/env bash
set -euo pipefail

# Problem this solves:
# A local Mac dev deployment accidentally used both README-canonical unprefixed
# Kafka topics (for example options.databento.raw) and stale dev-prefixed topics
# (for example dev.options.databento.raw). That split let different local
# services read/write different topic families, leaving old malformed Databento
# Avro records in the raw stream and causing RawToDisplayBridge failures such as
# "Not a valid schema field: underlying". This script removes only the local dev
# Databento/topic-family mess so Jenkins can recreate clean unprefixed topics.
# It must never be used against the remote production Kafka cluster.
#
# This script is only for cleanup after moving local dev to no-dev-prefix Kafka
# topics. It deletes stale topic families; it does not add, derive, or restore
# any dev. prefix behavior.

APPLY=false
LIST_ONLY=false
WAIT_SECONDS="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/kafka/cleanup-local-dev-databento-mixed-topics.sh [--dry-run|--apply|--list-only]

Purpose:
  Clean the local Mac dev Kafka cluster after mixed prefixed/unprefixed
  Databento topics were created. This script is intentionally local-dev only.

Defaults:
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  mode=--dry-run

Safety:
  - Refuses APP_PROFILE=prod or ENVIRONMENT=prod.
  - Refuses bootstrap servers containing the production host 192.168.100.252.
  - --apply only allows localhost, 127.0.0.1, host.docker.internal, or
    192.168.100.102 bootstrap hosts.
  - ALLOW_NON_LOCAL_DEV_KAFKA=true is accepted only for --dry-run/--list-only.
  - Does not delete replay topics.
  - --apply requires:
    CONFIRM_LOCAL_DEV_KAFKA_CLEANUP=DELETE_LOCAL_DEV_DATABENTO_TOPICS

Examples:
  scripts/kafka/cleanup-local-dev-databento-mixed-topics.sh --dry-run

  CONFIRM_LOCAL_DEV_KAFKA_CLEANUP=DELETE_LOCAL_DEV_DATABENTO_TOPICS \
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    scripts/kafka/cleanup-local-dev-databento-mixed-topics.sh --apply
USAGE
}

for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=true ;;
    --dry-run) APPLY=false ;;
    --list-only) LIST_ONLY=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

APP_PROFILE_LOWER="$(lower "${APP_PROFILE:-}")"
ENVIRONMENT_LOWER="$(lower "${ENVIRONMENT:-}")"
if [[ "$APP_PROFILE_LOWER" == "prod" || "$APP_PROFILE_LOWER" == "production" ]]; then
  echo "Refusing local cleanup when APP_PROFILE=$APP_PROFILE" >&2
  exit 1
fi
if [[ "$ENVIRONMENT_LOWER" == "prod" || "$ENVIRONMENT_LOWER" == "production" ]]; then
  echo "Refusing local cleanup when ENVIRONMENT=$ENVIRONMENT" >&2
  exit 1
fi

if [[ "$BOOTSTRAP_SERVERS" == *"192.168.100.252"* ]]; then
  echo "Refusing cleanup against production Kafka bootstrap: $BOOTSTRAP_SERVERS" >&2
  exit 1
fi

is_allowed_local_apply_bootstrap() {
  local bootstrap="$1"
  local entry host port
  IFS=',' read -r -a entries <<< "$bootstrap"
  for entry in "${entries[@]}"; do
    entry="${entry//[[:space:]]/}"
    host="${entry%%:*}"
    port="${entry##*:}"
    if [[ -z "$host" || "$host" == "$entry" || -z "$port" || "$port" == "$host" ]]; then
      return 1
    fi
    if [[ "$port" == *[!0-9]* ]]; then
      return 1
    fi
    case "$host" in
      localhost|127.0.0.1|host.docker.internal|192.168.100.102) ;;
      *) return 1 ;;
    esac
  done
  return 0
}

if [[ "$APPLY" == "true" ]]; then
  if ! is_allowed_local_apply_bootstrap "$BOOTSTRAP_SERVERS"; then
    echo "Refusing --apply because KAFKA_BOOTSTRAP_SERVERS is not on the local-dev allowlist: $BOOTSTRAP_SERVERS" >&2
    echo "Allowed --apply bootstrap hosts: localhost, 127.0.0.1, host.docker.internal, 192.168.100.102" >&2
    exit 1
  fi
elif [[ "$BOOTSTRAP_SERVERS" != *"localhost"* \
     && "$BOOTSTRAP_SERVERS" != *"127.0.0.1"* \
     && "$BOOTSTRAP_SERVERS" != *"host.docker.internal"* \
     && "$BOOTSTRAP_SERVERS" != *"192.168.100.102"* \
     && "${ALLOW_NON_LOCAL_DEV_KAFKA:-false}" != "true" ]]; then
  echo "Refusing non-local Kafka bootstrap without ALLOW_NON_LOCAL_DEV_KAFKA=true: $BOOTSTRAP_SERVERS" >&2
  exit 1
fi

if [[ "$APPLY" == "true" && "${CONFIRM_LOCAL_DEV_KAFKA_CLEANUP:-}" != "DELETE_LOCAL_DEV_DATABENTO_TOPICS" ]]; then
  echo "Refusing --apply without CONFIRM_LOCAL_DEV_KAFKA_CLEANUP=DELETE_LOCAL_DEV_DATABENTO_TOPICS" >&2
  exit 1
fi

KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-}"
if [[ -z "$KAFKA_TOPICS_BIN" ]]; then
  if command -v kafka-topics >/dev/null 2>&1; then
    KAFKA_TOPICS_BIN="$(command -v kafka-topics)"
  elif [[ -x "/Users/abhinav/development/confluent-7.3.1/bin/kafka-topics" ]]; then
    KAFKA_TOPICS_BIN="/Users/abhinav/development/confluent-7.3.1/bin/kafka-topics"
  elif [[ -x "/Users/abhinav/confluent-6.2.0/bin/kafka-topics" ]]; then
    KAFKA_TOPICS_BIN="/Users/abhinav/confluent-6.2.0/bin/kafka-topics"
  fi
fi

if [[ -z "$KAFKA_TOPICS_BIN" || ! -x "$KAFKA_TOPICS_BIN" ]]; then
  echo "kafka-topics is required. Set KAFKA_TOPICS_BIN=/path/to/kafka-topics or add it to PATH." >&2
  exit 1
fi

kafka_topics() {
  "$KAFKA_TOPICS_BIN" "$@"
}

run_cmd() {
  if [[ "$APPLY" == "true" ]]; then
    "$@"
  else
    printf '+ '
    printf '%q ' "$@"
    printf '\n'
  fi
}

topic_exists() {
  kafka_topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$1" >/dev/null 2>&1
}

wait_for_topic_deleted() {
  local topic="$1"
  if [[ "$APPLY" != "true" ]]; then
    return 0
  fi
  for ((i = 1; i <= WAIT_SECONDS; i++)); do
    if ! topic_exists "$topic"; then
      echo "Deleted topic: $topic"
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for topic deletion: $topic" >&2
  kafka_topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic" || true
  return 1
}

candidate_topics() {
  kafka_topics --bootstrap-server "$BOOTSTRAP_SERVERS" --list \
    | awk '
        /^dev\.options\.databento\./ { print; next }
        /^dev\.options\.marketdata\.selection$/ { print; next }
        /^dev\.options\.opra\./ { print; next }
        /^dev\.underlying\.(es\.trades|spx\.price)$/ { print; next }
        /^options\.databento\./ { print; next }
        /^options\.marketdata\.selection$/ { print; next }
        /^options\.opra\./ { print; next }
        /^underlying\.(es\.trades|spx\.price)$/ { print; next }
      ' \
    | grep -v '^options\.replay\.' \
    | grep -v '^underlying\.replay\.' \
    | sort -u
}

topics=()
while IFS= read -r topic; do
  topics+=("$topic")
done < <(candidate_topics)

echo "Kafka bootstrap: $BOOTSTRAP_SERVERS"
echo "Kafka topics binary: $KAFKA_TOPICS_BIN"
if [[ "${#topics[@]}" -eq 0 ]]; then
  echo "No local dev Databento mixed topics found."
  exit 0
fi

printf 'Matched topics:\n'
printf '  %s\n' "${topics[@]}"

if [[ "$LIST_ONLY" == "true" ]]; then
  exit 0
fi

if [[ "$APPLY" != "true" ]]; then
  echo
  echo "Dry run only. Re-run with --apply and CONFIRM_LOCAL_DEV_KAFKA_CLEANUP=DELETE_LOCAL_DEV_DATABENTO_TOPICS to delete these topics."
else
  echo
  echo "Final local-dev cleanup safety check:"
  echo "  bootstrap servers: $BOOTSTRAP_SERVERS"
  echo "  matched topic count: ${#topics[@]}"
  echo "  confirmation matched: yes"
  echo "  confirmation value: <redacted>"
  printf '  matched topics:\n'
  printf '    %s\n' "${topics[@]}"
fi

for topic in "${topics[@]}"; do
  echo "Deleting local dev topic: $topic"
  run_cmd "$KAFKA_TOPICS_BIN" --bootstrap-server "$BOOTSTRAP_SERVERS" --delete --topic "$topic"
done

for topic in "${topics[@]}"; do
  wait_for_topic_deleted "$topic"
done

if command -v kubectl >/dev/null 2>&1; then
  context="$(kubectl config current-context 2>/dev/null || true)"
  if [[ "$context" == "docker-desktop" ]]; then
    echo
    echo "Local Kubernetes context is docker-desktop."
    echo "After cleanup, redeploy/restart dev services through Jenkins so unprefixed topics are recreated cleanly."
  fi
fi
