#!/usr/bin/env bash
# Single source of truth for Kafka topic settings.
#
# Derives the replication settings from the rendered per-environment kustomize
# configmap so topic creation/verification always matches what actually gets
# deployed. No hard-coded replication factors to drift out of sync.
#
# Meant to be SOURCED (not executed), e.g.:
#   ENVIRONMENT=production . scripts/kafka/load-kafka-settings.sh
#
# Exports: KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_REPLICATION_FACTOR,
#          KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS, KAFKA_TOPIC_RETENTION_MS,
#          KAFKA_TOPIC_MESSAGE_TIMESTAMP_AFTER_MAX_MS
# Any value already set in the environment is preserved (explicit override wins).

: "${ENVIRONMENT:=production}"
_lks_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
_lks_root="$(cd "$_lks_dir/../.." && pwd)"
_lks_overlay="$_lks_root/k8s/overlays/${ENVIRONMENT}"

if [ ! -d "$_lks_overlay" ]; then
  echo "[kafka-settings] unknown ENVIRONMENT='${ENVIRONMENT}' (no overlay at $_lks_overlay)" >&2
  return 1 2>/dev/null || exit 1
fi

_lks_rendered="$(kubectl kustomize "$_lks_overlay")"

# Extract data.<key> from the rendered options-edge-config ConfigMap. Only that
# ConfigMap carries these keys, so a first-match grep is unambiguous.
_lks_cfg() {
  printf '%s\n' "$_lks_rendered" \
    | grep -E "^[[:space:]]*$1:[[:space:]]" \
    | head -1 \
    | sed -E "s/^[[:space:]]*$1:[[:space:]]*//; s/\"//g; s/[[:space:]]+\$//" || true
}

: "${KAFKA_BOOTSTRAP_SERVERS:=$(_lks_cfg KAFKA_BOOTSTRAP_SERVERS)}"
: "${KAFKA_TOPIC_REPLICATION_FACTOR:=$(_lks_cfg KAFKA_TOPIC_REPLICATION_FACTOR)}"
: "${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:=$(_lks_cfg KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS)}"
: "${KAFKA_TOPIC_RETENTION_MS:=$(_lks_cfg KAFKA_TOPIC_RETENTION_MS)}"
: "${KAFKA_TOPIC_MESSAGE_TIMESTAMP_AFTER_MAX_MS:=$(_lks_cfg KAFKA_TOPIC_MESSAGE_TIMESTAMP_AFTER_MAX_MS)}"
# Optional universal per-env retention cap (dev configmap sets 10h; prod leaves it
# unset). When set, the HPSF create/verify scripts and the Kafka Internal Topics
# stage cap every topic's retention.ms at this value. Empty => no cap (prod).
: "${KAFKA_MAX_RETENTION_MS:=$(_lks_cfg KAFKA_MAX_RETENTION_MS)}"
export KAFKA_BOOTSTRAP_SERVERS KAFKA_TOPIC_REPLICATION_FACTOR
export KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS KAFKA_TOPIC_RETENTION_MS
export KAFKA_TOPIC_MESSAGE_TIMESTAMP_AFTER_MAX_MS KAFKA_MAX_RETENTION_MS

: "${KAFKA_BOOTSTRAP_SERVERS:?could not derive KAFKA_BOOTSTRAP_SERVERS from ${ENVIRONMENT} configmap}"
: "${KAFKA_TOPIC_REPLICATION_FACTOR:?could not derive KAFKA_TOPIC_REPLICATION_FACTOR from ${ENVIRONMENT} configmap}"
: "${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:?could not derive KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS from ${ENVIRONMENT} configmap}"

echo "[kafka-settings] env=${ENVIRONMENT} RF=${KAFKA_TOPIC_REPLICATION_FACTOR} minISR=${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS} retention=${KAFKA_TOPIC_RETENTION_MS} timestampAfterMax=${KAFKA_TOPIC_MESSAGE_TIMESTAMP_AFTER_MAX_MS:-<none>} maxRetentionCap=${KAFKA_MAX_RETENTION_MS:-<none>} bootstrap=${KAFKA_BOOTSTRAP_SERVERS}"
