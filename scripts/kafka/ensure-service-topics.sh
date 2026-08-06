#!/usr/bin/env bash
# ensure-service-topics.sh — ONE pre-rollout Kafka-topic prerequisite gate for service-deploy.
#
# A service whose producer must reach a topic that EXISTS with a specific shape (partitions,
# cleanup.policy, retention) — before the broker can auto-create it with cluster defaults that
# silently drop data — declares that need with a single case line HERE, not with its own
# `when { SERVICE == 'x' }` stage in Jenkinsfile.service-deploy. The Jenkinsfile has exactly one
# 'Ensure topic prerequisites' stage that calls this dispatcher; adding a service never touches it.
#
# Each case delegates to the service's existing, individually-reviewed ensure script (the bespoke
# shape validation and its rationale live there). Runs BEFORE the rollout, while the service (if
# any) is still serving — nothing here needs an outage window.
#
# Contract: exit 0 = prerequisites satisfied (or none for this service); non-zero = fail the deploy
# BEFORE the pod rolls. This script OWNS its Kafka setup — it puts the Kafka CLI on PATH and sources
# load-kafka-settings.sh itself, but ONLY after confirming the service is mapped. A service with no
# topic prerequisites returns before any of that runs, so it is a TRUE no-op: an unrelated
# load-kafka-settings failure (it runs `kubectl kustomize`) can never block a service that has no
# topic to ensure. The caller must NOT pre-load Kafka settings (Codex P1).
set -euo pipefail

SERVICE="${1:?usage: ensure-service-topics.sh <service>}"
here="$(cd "$(dirname "$0")" && pwd)"

# True no-op fast-path: return BEFORE any Kafka setup for services without a topic prerequisite.
case "$SERVICE" in
  databento-gex|strike-intelligence|vix-option-inteligence) ;;
  *)
    echo "ensure-service-topics: no topic prerequisites for '$SERVICE' — nothing to do."
    exit 0
    ;;
esac

# Mapped service only: load the Kafka CLI + settings the ensure scripts assume (reached solely when
# there is real work to do).
export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
. "$here/load-kafka-settings.sh"

case "$SERVICE" in
  databento-gex)
    # OI anchor manifest: pure compact, retention.ms=-1 (the settled daily OI baseline; the
    # 'delete' half of a compact,delete default would drop each session's record 24h later).
    "$here/ensure-oi-anchor-topic.sh"
    ;;
  strike-intelligence)
    # GMT Stage-1 gamma-tilt shadow evidence ledger: 1 partition (scorer co-partitioning + total
    # episode order), delete, retention.ms=-1 (an evidence ledger must never drop records).
    "$here/ensure-gamma-tilt-ledger.sh"
    ;;
  vix-option-inteligence)
    # VIX current topic reconcile — the TOPIC phase only (the identity prune needs the outage
    # window and stays in its own stage). Runs while the service is still serving.
    RECONCILE_PHASE=topic "$here/ensure-vix-option-inteligence-topic.sh"
    ;;
esac
