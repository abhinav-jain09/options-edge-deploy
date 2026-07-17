#!/usr/bin/env bash
          set -euo pipefail
          mkdir -p "$JENKINS_WORK_DIR"
          image_tag="${IMAGE_TAG:-}"
          if [ -n "${IMAGE_REGISTRY:-}" ]; then
            registry="$IMAGE_REGISTRY"
          else
            : "${OE_REGISTRY:?OE_REGISTRY must be set by the Resolve profile stage (oeProfile single source of truth)}"
            registry="$OE_REGISTRY"
          fi
          if [ -n "$image_tag" ] || [ "${ENVIRONMENT:-dev}" = "dev" ]; then
            if [ -z "$image_tag" ]; then
              image_tag=dev
            fi
            cat >"$JENKINS_WORK_DIR/options-edge-images.env" <<EOF
RAW_TO_DISPLAY_IMAGE=$registry/options-edge-raw-to-display:$image_tag
WEB_IMAGE=$registry/options-edge-web:$image_tag
DATABENTO_VOLUME_AGGREGATOR_IMAGE=$registry/options-edge-databento-volume-aggregator:$image_tag
DATABENTO_FEED_IMAGE=$registry/options-edge-databento-feed:$image_tag
DATABENTO_GEX_IMAGE=$registry/options-edge-databento-gex:$image_tag
OPTION_PRICE_BEHAVIOR_IMAGE=$registry/options-edge-option-price-behavior:$image_tag
DATABENTO_MISSION_SANDWICH_IMAGE=$registry/options-edge-databento-mission-sandwich:$image_tag
VOLUME_PACE_IMAGE=$registry/options-edge-volume-pace:$image_tag
DIRECTIONAL_PRESSURE_IMAGE=$registry/options-edge-directional-pressure:$image_tag
DATABENTO_GEX_HISTORY_IMAGE=$registry/options-edge-databento-gex-history:$image_tag
RAW_POSTGRES_WRITER_IMAGE=$registry/options-edge-raw-postgres-writer:$image_tag
PRESSURE_POSTGRES_WRITER_IMAGE=$registry/options-edge-pressure-postgres-writer:$image_tag
PIN_POSTGRES_WRITER_IMAGE=$registry/options-edge-pin-postgres-writer:$image_tag
PIN_FLOW_EXPLORER_IMAGE=$registry/options-edge-pin-flow-explorer:$image_tag
FEED_GATEWAY_IMAGE=$registry/options-edge-feed-gateway:$image_tag
HPSF_PROCESSING_IMAGE=$registry/options-edge-hpsf-processing:$image_tag
HPSF_POSTGRES_WRITER_IMAGE=$registry/options-edge-hpsf-postgres-writer:$image_tag
SPX_MISSION_CONTROL_IMAGE=$registry/options-edge-spx-mission-control:$image_tag
STRIKE_FLOW_CLASSIFIER_IMAGE=$registry/options-edge-strike-flow-classifier:$image_tag
DELTA_FLOW_IMAGE=$registry/options-edge-delta-flow:$image_tag
DEALER_LEDGER_IMAGE=$registry/options-edge-dealer-ledger:$image_tag
DEALER_LEDGER_CALIBRATION_IMAGE=$registry/options-edge-dealer-ledger-calibration:$image_tag
STRIKE_LIQUIDITY_HEATMAP_IMAGE=$registry/options-edge-strike-liquidity-heatmap:$image_tag
UNIFIED_SR_IMAGE=$registry/options-edge-unified-sr:$image_tag
STRIKE_INTELLIGENCE_IMAGE=$registry/options-edge-strike-intelligence:$image_tag
STRIKE_FLOW_AVRO_ADAPTER_IMAGE=$registry/options-edge-strike-flow-avro-adapter:$image_tag
GEX_DELTA_REDIS_WRITER_IMAGE=$registry/options-edge-gex-delta-redis-writer:$image_tag
IBKR_FEED_IMAGE=$registry/options-edge-ibkr-feed:$image_tag
DATABENTO_MAXPAIN_IMAGE=$registry/options-edge-databento-maxpain:$image_tag
STRIKE_INVASION_IMAGE=$registry/options-edge-strike-invasion:$image_tag
INVASION_POSTGRES_WRITER_IMAGE=$registry/options-edge-invasion-postgres-writer:$image_tag
SPREAD_SKEW_IMAGE=$registry/options-edge-spread-skew:$image_tag
REVERSAL_CONFIRMATION_IMAGE=$registry/options-edge-reversal-confirmation:$image_tag
SPREAD_SKEW_POSTGRES_WRITER_IMAGE=$registry/options-edge-spread-skew-postgres-writer:$image_tag
ES_OPEN_DIRECTION_IMAGE=$registry/options-edge-es-open-direction:$image_tag
ES_OPEN_DIRECTION_POSTGRES_WRITER_IMAGE=$registry/options-edge-es-open-direction-postgres-writer:$image_tag
REVERSAL_POSTGRES_WRITER_IMAGE=$registry/options-edge-reversal-postgres-writer:$image_tag
EOF
            # short-premium-agent renders in dev+production (a standalone service that runs on prod too),
            # NOT experiment. Emit its image var for a dev OR production tag-based resolve (mirrors
            # all_image_vars' dev+prod set), never for experiment.
            if [ "${ENVIRONMENT:-dev}" = "dev" ] || [ "${ENVIRONMENT:-dev}" = "production" ]; then
              cat >>"$JENKINS_WORK_DIR/options-edge-images.env" <<EOF
SHORT_PREMIUM_AGENT_IMAGE=$registry/options-edge-short-premium-agent:$image_tag
EOF
            fi
          else
            cat >"$JENKINS_WORK_DIR/options-edge-images.env" <<EOF
RAW_TO_DISPLAY_IMAGE=$RAW_TO_DISPLAY_IMAGE
WEB_IMAGE=$WEB_IMAGE
DATABENTO_VOLUME_AGGREGATOR_IMAGE=$DATABENTO_VOLUME_AGGREGATOR_IMAGE
DATABENTO_FEED_IMAGE=$DATABENTO_FEED_IMAGE
DATABENTO_GEX_IMAGE=$DATABENTO_GEX_IMAGE
OPTION_PRICE_BEHAVIOR_IMAGE=$OPTION_PRICE_BEHAVIOR_IMAGE
DATABENTO_MISSION_SANDWICH_IMAGE=$DATABENTO_MISSION_SANDWICH_IMAGE
VOLUME_PACE_IMAGE=$VOLUME_PACE_IMAGE
DIRECTIONAL_PRESSURE_IMAGE=$DIRECTIONAL_PRESSURE_IMAGE
DATABENTO_GEX_HISTORY_IMAGE=$DATABENTO_GEX_HISTORY_IMAGE
RAW_POSTGRES_WRITER_IMAGE=$RAW_POSTGRES_WRITER_IMAGE
PIN_POSTGRES_WRITER_IMAGE=$PIN_POSTGRES_WRITER_IMAGE
PIN_FLOW_EXPLORER_IMAGE=$PIN_FLOW_EXPLORER_IMAGE
PRESSURE_POSTGRES_WRITER_IMAGE=$PRESSURE_POSTGRES_WRITER_IMAGE
FEED_GATEWAY_IMAGE=$FEED_GATEWAY_IMAGE
HPSF_PROCESSING_IMAGE=$HPSF_PROCESSING_IMAGE
HPSF_POSTGRES_WRITER_IMAGE=$HPSF_POSTGRES_WRITER_IMAGE
SPX_MISSION_CONTROL_IMAGE=$SPX_MISSION_CONTROL_IMAGE
STRIKE_FLOW_CLASSIFIER_IMAGE=$STRIKE_FLOW_CLASSIFIER_IMAGE
DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE
DEALER_LEDGER_IMAGE=$DEALER_LEDGER_IMAGE
DEALER_LEDGER_CALIBRATION_IMAGE=$DEALER_LEDGER_CALIBRATION_IMAGE
STRIKE_LIQUIDITY_HEATMAP_IMAGE=$STRIKE_LIQUIDITY_HEATMAP_IMAGE
UNIFIED_SR_IMAGE=$UNIFIED_SR_IMAGE
STRIKE_INTELLIGENCE_IMAGE=$STRIKE_INTELLIGENCE_IMAGE
STRIKE_FLOW_AVRO_ADAPTER_IMAGE=$STRIKE_FLOW_AVRO_ADAPTER_IMAGE
GEX_DELTA_REDIS_WRITER_IMAGE=$GEX_DELTA_REDIS_WRITER_IMAGE
IBKR_FEED_IMAGE=$IBKR_FEED_IMAGE
DATABENTO_MAXPAIN_IMAGE=$DATABENTO_MAXPAIN_IMAGE
STRIKE_INVASION_IMAGE=$STRIKE_INVASION_IMAGE
INVASION_POSTGRES_WRITER_IMAGE=$INVASION_POSTGRES_WRITER_IMAGE
SPREAD_SKEW_IMAGE=$SPREAD_SKEW_IMAGE
REVERSAL_CONFIRMATION_IMAGE=$REVERSAL_CONFIRMATION_IMAGE
SPREAD_SKEW_POSTGRES_WRITER_IMAGE=$SPREAD_SKEW_POSTGRES_WRITER_IMAGE
ES_OPEN_DIRECTION_IMAGE=$ES_OPEN_DIRECTION_IMAGE
ES_OPEN_DIRECTION_POSTGRES_WRITER_IMAGE=$ES_OPEN_DIRECTION_POSTGRES_WRITER_IMAGE
REVERSAL_POSTGRES_WRITER_IMAGE=$REVERSAL_POSTGRES_WRITER_IMAGE
EOF
            # short-premium-agent renders in dev+production; on the promoted/branch-2 path (prod) its
            # image var is caller-provided (Jenkinsfile image-defaults -> oeProfile.image), so emit it
            # here too. Guarded to production so an experiment promoted resolve (which never renders it)
            # does not require the var under set -u.
            if [ "${ENVIRONMENT:-dev}" = "production" ]; then
              cat >>"$JENKINS_WORK_DIR/options-edge-images.env" <<EOF
SHORT_PREMIUM_AGENT_IMAGE=$SHORT_PREMIUM_AGENT_IMAGE
EOF
            fi
          fi
          . scripts/deploy/image-lock.sh
          . "$JENKINS_WORK_DIR/options-edge-images.env"
          apply_image_lock "${IMAGE_LOCK_FILE:-}" "${REQUIRE_IMAGE_LOCK:-false}" "$JENKINS_WORK_DIR/options-edge-images.env"
          echo "Resolved deployment images:"
          sed 's/^/  /' "$JENKINS_WORK_DIR/options-edge-images.env"
