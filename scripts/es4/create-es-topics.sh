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
OVERRIDES_GUARD="$SCRIPT_DIR/../ci/validate-declared-overrides-are-explicit.sh"
[ -r "$OVERRIDES_GUARD" ] || { echo "missing scripts/ci/validate-declared-overrides-are-explicit.sh (rsync scripts/ , not just scripts/es4)" >&2; exit 1; }

PATH="$SHIM_DIR:$PATH" \
KAFKA_BOOTSTRAP_SERVERS="${BROKER:-localhost:9092}" \
KAFKA_TOPIC_REPLICATION_FACTOR="${ES4_REPLICATION_FACTOR:-1}" \
KAFKA_TOPIC_RETENTION_MS="${RETENTION_MS:-43200000}" \
TOPIC_SET=es4 \
  bash "$SCRIPT_DIR/../kafka/apply-topics.sh"

# ---- the reconciliation actually took ----
# apply-topics.sh calls alter_topic_config UNCONDITIONALLY for every declared topic, so once this
# script has run, every topic in the es4 declaration must carry its retention override ON THE TOPIC.
# A topic sitting on the broker's default is a topic this stage did not reach: auto-created by a
# still-running producer, or created before it was declared. The agreement then looks fine — with no
# topic-level config, `--describe` shows no drift because it shows nothing — and it ends silently the
# moment a broker default moves.
#
# Run HERE and not only in CI, because es4 is the one environment CI cannot check: its declaration
# (OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES) and its broker are reachable only through the shims
# on this box, so the es4 arm of the guard is otherwise never exercised against a real broker. Same
# shim PATH, same in-container listener, same TOPIC_SET the reconciliation above used.
PATH="$SHIM_DIR:$PATH" TOPIC_SET=es4 bash "$OVERRIDES_GUARD" "$BROKER"

# ---- zero-orphan prune of the retired identity (One Service One Identity Rule) ----
# This call had NO `source` for the library that defines it: under `set -euo pipefail` that is a
# hard "command not found" failure at the very end of the script, so create-es-topics.sh could
# never complete. The sourcing was lost when topic creation was refactored to delegate to
# scripts/kafka/apply-topics.sh; the library's own header still says this caller "supplies
# docker-exec wrappers", which is exactly the half that went missing.
#
# Caught by tests/test_vix_option_inteligence_deploy.py, which had been failing on main — a red
# test that was describing a real breakage, not test drift.
#
# shellcheck source=/dev/null
source "$SCRIPT_DIR/../kafka/prune-retired-zero-dte-identity.lib.sh"
# The es4 host has no Kafka CLI — everything goes through the shims, so the wrappers must carry
# the shim PATH the same way the apply-topics.sh invocation above does.
prune_kt() { PATH="$SHIM_DIR:$PATH" kafka-topics --bootstrap-server "$BROKER" "$@"; }
prune_kg() { PATH="$SHIM_DIR:$PATH" kafka-consumer-groups --bootstrap-server "$BROKER" "$@"; }
prune_retired_zero_dte_identity "es.options.spx.0dte.intelligence.current"
