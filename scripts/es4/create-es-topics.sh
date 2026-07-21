#!/usr/bin/env bash
# create-es-topics.sh — explicit creation + RECONCILIATION of the core es.* topics
# on the es4 broker.
#
# Policy: 4 partitions (org policy), RF=1 (single node). Cleanup semantics come from
# the org SSOT scripts/kafka/topics.env: topics listed there as COMPACTED get
# cleanup.policy=compact; everything else gets delete + 12h retention (org policy).
# Existing topics are RECONCILED (kafka-configs --alter is idempotent), so
# retention/cleanup drift is repaired on every run — creation is not the only guarantee.
#
# Naming: every ES topic carries the es. prefix (Gate-2 G6). Consumers get the prefix
# via TOPIC_PREFIX=es. — these names are <es.> + the values the GENERATED manifests
# carry (k8s/es4/services/*.yaml). Keep in sync when the renderer changes. Broker
# auto-create covers stragglers, but everything the manifests reference is listed here
# so its config is pinned.
#
# Runs ON the es4 box (invoked by bootstrap-es4.sh or the es4-deploy Jenkins job).

set -euo pipefail

BROKER=localhost:29092   # in-container listener via docker exec
PARTITIONS=4
RETENTION_MS=43200000    # 12h

# delete-cleanup topics (12h retention)
# ---------------------------------------------------------------------------
# SINGLE SOURCE OF TRUTH: topic definitions live in scripts/kafka/topics.env and are applied by
# scripts/kafka/apply-topics.sh — the SAME script Jenkins runs for dev and prod. This file no longer
# declares topics; it only selects the es4 set and supplies the es4 broker + CLI shim.
#
# TOPIC_SET=es4 -> OPTIONS_EDGE_ES4_* (~45 es.* topics). The SPX options.* set is deliberately NOT
# applied here: those topics must never exist on the es4 broker.
#
# The Kafka CLI is not installed on the es4 host (Kafka runs as the es4-kafka container), so the
# shims in scripts/es4/kafka-cli-shim/ proxy each CLI call into it and apply-topics.sh runs unmodified.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHIM_DIR="$SCRIPT_DIR/kafka-cli-shim"
[ -d "$SHIM_DIR" ] || { echo "missing $SHIM_DIR — cannot reach the es4 Kafka CLI" >&2; exit 1; }
[ -r "$SCRIPT_DIR/../kafka/apply-topics.sh" ] || { echo "missing scripts/kafka/apply-topics.sh (rsync scripts/ , not just scripts/es4)" >&2; exit 1; }

PATH="$SHIM_DIR:$PATH" \
KAFKA_BOOTSTRAP_SERVERS="${BROKER:-localhost:9092}" \
KAFKA_TOPIC_REPLICATION_FACTOR="${ES4_REPLICATION_FACTOR:-1}" \
KAFKA_TOPIC_RETENTION_MS="${RETENTION_MS:-43200000}" \
TOPIC_SET=es4 \
  bash "$SCRIPT_DIR/../kafka/apply-topics.sh"

prune_retired_zero_dte_identity "es.options.spx.0dte.intelligence.current"
