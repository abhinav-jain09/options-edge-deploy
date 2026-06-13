#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-}"
APP_ID="${HPSF_STAGE_B_STREAMS_APPLICATION_ID:-options-edge-hpsf-stage-b-v2-1}"
DELETE_TIMEOUT_SECONDS="${HPSF_STAGE_B_INTERNAL_TOPIC_DELETE_TIMEOUT_SECONDS:-120}"

if [[ "$DRY_RUN" != "true" && -z "$BOOTSTRAP_SERVERS" ]]; then
  echo "KAFKA_BOOTSTRAP_SERVERS is required unless --dry-run is used" >&2
  exit 1
fi
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-DRY_RUN_BOOTSTRAP}"

run_cmd() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '+ '
    printf '%q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

list_repartition_topics() {
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --list \
    | awk -v prefix="${APP_ID}-" '$0 ~ ("^" prefix) && $0 ~ /-repartition$/ { print }' \
    | sort
}

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY RUN reset Stage B internal repartition topics for app.id=$APP_ID"
  run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --list
  run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --delete --topic "${APP_ID}-hpsf-stage-b-strike-flow-by-chain-repartition"
  exit 0
fi

mapfile -t topics < <(list_repartition_topics)
if [[ "${#topics[@]}" -eq 0 ]]; then
  echo "No HPSF Stage B repartition topics found for app.id=$APP_ID"
  exit 0
fi

for topic in "${topics[@]}"; do
  if [[ "$topic" != "$APP_ID"-* || "$topic" != *-repartition ]]; then
    echo "Refusing to delete unexpected topic name: $topic" >&2
    exit 1
  fi
  echo "Deleting stale HPSF Stage B internal repartition topic: $topic"
  run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --delete --topic "$topic" || true
done

for topic in "${topics[@]}"; do
  for ((i = 1; i <= DELETE_TIMEOUT_SECONDS; i++)); do
    if ! kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic" >/dev/null 2>&1; then
      echo "Deleted internal topic: $topic"
      break
    fi
    if [[ "$i" -eq "$DELETE_TIMEOUT_SECONDS" ]]; then
      echo "Timed out waiting for internal topic deletion: $topic" >&2
      kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic" || true
      exit 1
    fi
    sleep 1
  done
done
