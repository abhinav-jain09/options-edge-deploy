#!/usr/bin/env bash
# ensure-service-prereqs.sh — ONE pre-rollout prerequisite gate for service-deploy.
#
# Any service that needs something to be TRUE before its pod rolls — a Kafka topic that EXISTS with
# a specific shape (partitions, cleanup.policy, retention), or a ConfigMap key reconciled — declares
# it with a single case line HERE, not with its own `when { SERVICE == 'x' }` stage in
# Jenkinsfile.service-deploy. The Jenkinsfile has exactly one 'Ensure service prerequisites' stage
# that calls this dispatcher; adding a service never touches it.
#
# Each case delegates to the service's existing, individually-reviewed logic. Runs BEFORE the
# rollout, while the prior pod (if any) still serves — nothing here needs an outage window.
#
# Dry-run policy is PER-CONCERN and owned here:
#   - ConfigMap reconciles PREVIEW under dry-run (report the change, mutate nothing).
#   - Kafka topic ensures SKIP under dry-run (they create/verify topics and must never mutate).
#
# Contract: exit 0 = prerequisites satisfied (or none for this service); non-zero = fail the deploy
# BEFORE the pod rolls. A service with no prerequisite returns before any setup, so it is a TRUE
# no-op: an unrelated Kafka/kubectl failure can never block a service that has nothing to do.
set -euo pipefail

SERVICE="${1:?usage: ensure-service-prereqs.sh <service> [dry_run]}"
DRY="${2:-false}"
here="$(cd "$(dirname "$0")" && pwd)"
kafka="$here/../kafka"

case "$SERVICE" in
  feed-gateway)
    # The standalone slice carries only this workload, not the shared ConfigMap. Reconcile the one
    # gateway-owned selection key (IB_EXPIRY -> AUTO) before rolling the pod, so envFrom can never
    # revive a blank literal expiry and drop the entire live option chain. Previews under dry-run.
    current="$(kubectl -n options-edge get configmap options-edge-config -o jsonpath='{.data.IB_EXPIRY}')"
    if [ "$current" = "AUTO" ]; then
      echo "options-edge-config IB_EXPIRY is already AUTO."
    elif [ "$DRY" = "true" ]; then
      echo "[dry-run] would change options-edge-config IB_EXPIRY from [$current] to AUTO."
    else
      kubectl -n options-edge patch configmap options-edge-config --type=merge \
        --patch '{"data":{"IB_EXPIRY":"AUTO"}}'
      observed="$(kubectl -n options-edge get configmap options-edge-config -o jsonpath='{.data.IB_EXPIRY}')"
      [ "$observed" = "AUTO" ] || {
        echo "FATAL: IB_EXPIRY reconcile did not persist AUTO (observed [$observed])." >&2
        exit 1
      }
    fi
    ;;

  databento-gex|strike-intelligence|vix-option-inteligence)
    # Kafka topic-shape ensures. SKIP under dry-run — these create/verify topics and must never
    # mutate during a preview. Load the Kafka CLI + settings only here (a mapped, non-dry-run
    # service), so nothing Kafka-related runs for any other service.
    if [ "$DRY" = "true" ]; then
      echo "[dry-run] skipping topic prerequisite ensure for '$SERVICE'."
      exit 0
    fi
    export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
    . "$kafka/load-kafka-settings.sh"
    case "$SERVICE" in
      databento-gex)
        # OI anchor manifest: pure compact, retention.ms=-1 (the settled daily OI baseline; the
        # 'delete' half of a compact,delete default would drop each session's record 24h later).
        "$kafka/ensure-oi-anchor-topic.sh"
        ;;
      strike-intelligence)
        # GMT Stage-1 gamma-tilt shadow evidence ledger: 1 partition (scorer co-partitioning +
        # total episode order), delete, retention.ms=-1 (an evidence ledger must never drop records).
        "$kafka/ensure-gamma-tilt-ledger.sh"
        ;;
      vix-option-inteligence)
        # VIX current-topic reconcile — the TOPIC phase only (the identity prune needs the outage
        # window and stays in its own stage). Runs while the service is still serving.
        RECONCILE_PHASE=topic "$kafka/ensure-vix-option-inteligence-topic.sh"
        ;;
    esac
    ;;

  *)
    echo "ensure-service-prereqs: no pre-rollout prerequisites for '$SERVICE' — nothing to do."
    ;;
esac
