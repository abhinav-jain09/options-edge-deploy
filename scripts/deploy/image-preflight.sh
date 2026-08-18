#!/usr/bin/env bash
          set -euo pipefail
          . "$JENKINS_WORK_DIR/options-edge-images.env"
          . "$JENKINS_WORK_DIR/options-edge-build.env"
          images="
            RAW_TO_DISPLAY_IMAGE=$RAW_TO_DISPLAY_IMAGE
            WEB_IMAGE=$WEB_IMAGE
            DATABENTO_VOLUME_AGGREGATOR_IMAGE=$DATABENTO_VOLUME_AGGREGATOR_IMAGE
            DATABENTO_FEED_IMAGE=$DATABENTO_FEED_IMAGE
            DATABENTO_SR3_FEED_IMAGE=$DATABENTO_SR3_FEED_IMAGE
            DATABENTO_GEX_IMAGE=$DATABENTO_GEX_IMAGE
            OPTION_PRICE_BEHAVIOR_IMAGE=$OPTION_PRICE_BEHAVIOR_IMAGE
            DATABENTO_MISSION_SANDWICH_IMAGE=$DATABENTO_MISSION_SANDWICH_IMAGE
            VOLUME_PACE_IMAGE=$VOLUME_PACE_IMAGE
            DIRECTIONAL_PRESSURE_IMAGE=$DIRECTIONAL_PRESSURE_IMAGE
            DATABENTO_GEX_HISTORY_IMAGE=$DATABENTO_GEX_HISTORY_IMAGE
            GAMMA_MIGRATION_IMAGE=$GAMMA_MIGRATION_IMAGE
            RAW_POSTGRES_WRITER_IMAGE=$RAW_POSTGRES_WRITER_IMAGE
            PIN_POSTGRES_WRITER_IMAGE=$PIN_POSTGRES_WRITER_IMAGE
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
            OPTION_TRUTH_ENGINE_IMAGE=$OPTION_TRUTH_ENGINE_IMAGE
            MARKET_CARRY_IMAGE=$MARKET_CARRY_IMAGE
            ES_SPX_ALIGN_IMAGE=$ES_SPX_ALIGN_IMAGE
            VIX_OPTION_INTELIGENCE_IMAGE=$VIX_OPTION_INTELIGENCE_IMAGE
            GREEK_MOVE_AUTHENTICITY_IMAGE=$GREEK_MOVE_AUTHENTICITY_IMAGE
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
            CLOSE_DIRECTION_IMAGE=$CLOSE_DIRECTION_IMAGE
            SPOT_VOL_REGIME_IMAGE=$SPOT_VOL_REGIME_IMAGE
            INDICATOR_SERVICE_IMAGE=$INDICATOR_SERVICE_IMAGE
            STOCK_GEX_IMAGE=$STOCK_GEX_IMAGE
            DROP_CLASSIFIER_IMAGE=$DROP_CLASSIFIER_IMAGE
            OI_SHADOW_IMAGE=$OI_SHADOW_IMAGE
            REVERSAL_POSTGRES_WRITER_IMAGE=$REVERSAL_POSTGRES_WRITER_IMAGE
          "
          case "${DEPLOY_TARGET:-all}" in
            all)
              ;;
            delta-flow-service)
              images="
                DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE
              "
              ;;
            dealer-ledger-service)
              images="
                DEALER_LEDGER_IMAGE=$DEALER_LEDGER_IMAGE
              "
              ;;
            strike-liquidity-heatmap-service)
              images="
                STRIKE_LIQUIDITY_HEATMAP_IMAGE=$STRIKE_LIQUIDITY_HEATMAP_IMAGE
              "
              ;;
            *)
              echo "Unsupported DEPLOY_TARGET: ${DEPLOY_TARGET}" >&2
              exit 1
              ;;
          esac

          # short-premium-agent renders in dev+production (a standalone service that runs on prod too),
          # NOT experiment. Preflight it for a dev OR production all-deploy so a missing image is caught
          # here (not later); experiment never renders it, so it is never preflighted there.
          if { [ "${ENVIRONMENT:-dev}" = "dev" ] || [ "${ENVIRONMENT:-dev}" = "production" ]; } && [ "${DEPLOY_TARGET:-all}" = "all" ]; then
            images="$images
            SHORT_PREMIUM_AGENT_IMAGE=${SHORT_PREMIUM_AGENT_IMAGE:-}
            SIGNAL_FOLLOWER_IMAGE=${SIGNAL_FOLLOWER_IMAGE:-}
            CONTEXT_TAPE_IMAGE=${CONTEXT_TAPE_IMAGE:-}
            MULTILEG_STRUCTURE_IMAGE=${MULTILEG_STRUCTURE_IMAGE:-}
            "
          fi

          image_exists_once() {
            local image="$1"
            local registry remainder repository reference tagged
            registry="${image%%/*}"
            remainder="${image#*/}"
            if [ "$registry" = "$image" ] || [ "$remainder" = "$image" ] || { [[ "$remainder" != *:* ]] && [[ "$remainder" != *@sha256:* ]]; }; then
              docker pull "$image" >/dev/null 2>&1
              return $?
            fi

            tagged="${remainder%@*}"
            repository="${tagged%:*}"
            if [[ "$remainder" == *@sha256:* ]]; then
              reference="${remainder##*@}"
            else
              reference="${remainder##*:}"
            fi
            for scheme in http https; do
              # Accept must include the multi-arch index/list media types, otherwise
              # the registry cannot content-negotiate a HEAD for an index-tagged image
              # and returns 404 even though the image exists.
              if curl -fsSI \
                -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
                -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
                -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
                -H 'Accept: application/vnd.oci.image.index.v1+json' \
                "$scheme://$registry/v2/$repository/manifests/$reference" >/dev/null 2>&1; then
                return 0
              fi
            done

            # Fallback (only reached if the registry HEAD check above could not run,
            # e.g. dev's host.docker.internal registry which the agent can't resolve
            # by curl). Use a plain pull so it matches the agent's own platform —
            # dev images are arm64, prod images are validated via the HEAD check.
            docker pull "$image" >/dev/null 2>&1
          }

          # Registry checks run from the deploy agent over the network to the
          # remote registry, which intermittently blips. Retry so a single
          # transient failure does not flag an existing image as "missing".
          image_exists() {
            local image="$1" attempt
            for attempt in 1 2 3 4 5 6; do
              if image_exists_once "$image"; then
                return 0
              fi
              sleep 3
            done
            return 1
          }

          buildx_builder_name=""
          cleanup_buildx_builder() {
            if [ -n "$buildx_builder_name" ]; then
              docker buildx rm "$buildx_builder_name" >/dev/null 2>&1 || true
            fi
          }
          trap cleanup_buildx_builder EXIT

          buildx_builder_arg=""
          # Trigger insecure buildkit config when the rendered images.env points at the
          # prod (or any) insecure registry from oeProfile. The registry host is derived
          # (single source of truth: oeProfile('production').registry, surfaced as
          # $OE_PROD_REGISTRY) — never literal.
          : "${OE_PROD_REGISTRY:?OE_PROD_REGISTRY must be set by the Resolve profile stage}"
          if grep -qF "=${OE_PROD_REGISTRY}/" "$JENKINS_WORK_DIR/options-edge-images.env"; then
            cat >"$JENKINS_WORK_DIR/buildkitd-insecure-registry.toml" <<EOF
[registry."${OE_PROD_REGISTRY}"]
  http = true
  insecure = true
EOF
            buildx_builder_name="options-edge-preflight-${BUILD_NUMBER:-$$}"
            docker buildx rm "$buildx_builder_name" >/dev/null 2>&1 || true
            docker buildx create --name "$buildx_builder_name" --config "$JENKINS_WORK_DIR/buildkitd-insecure-registry.toml" >/dev/null
            buildx_builder_arg="--builder $buildx_builder_name"
          fi

          inspect_image_platform() {
            local name="$1"
            local image="$2"
            local expected_platform="$3"
            local inspect_file inspect_json
            inspect_file="$JENKINS_WORK_DIR/imagetools-${name}.txt"
            inspect_json="$JENKINS_WORK_DIR/imagetools-${name}.json"
            local attempt
            for attempt in 1 2 3 4 5 6; do
              docker buildx imagetools inspect $buildx_builder_arg "$image" >"$inspect_file" 2>/dev/null && break
              [ "$attempt" = "6" ] && { echo "Unable to inspect image manifest with docker buildx imagetools: $name=$image" >&2; return 1; }
              sleep 3
            done
            for attempt in 1 2 3 4 5 6; do
              docker buildx imagetools inspect $buildx_builder_arg --format '{{json .}}' "$image" >"$inspect_json" 2>/dev/null && break
              [ "$attempt" = "6" ] && { echo "Unable to inspect image manifest JSON with docker buildx imagetools: $name=$image" >&2; return 1; }
              sleep 3
            done
            sed "s/^/  $name imagetools: /" "$inspect_file"
            if ! python3 - "$inspect_json" "$expected_platform" <<'PY'
import json
import sys

path, expected = sys.argv[1], sys.argv[2]
expected_os, expected_arch = expected.split("/", 1)
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)

def platform_matches(platform):
    return (
        platform.get("os") == expected_os
        and platform.get("architecture") == expected_arch
    )

manifest = data.get("manifest") or {}
for child in manifest.get("manifests") or []:
    if platform_matches(child.get("platform") or {}):
        sys.exit(0)

image = data.get("image") or {}
if image.get("os") == expected_os and image.get("architecture") == expected_arch:
    sys.exit(0)

sys.exit(1)
PY
            then
              echo "Image architecture mismatch for $name: expected manifest platform $expected_platform ($image)" >&2
              return 1
            fi
          }

          missing=0
          platform_mismatch=0
          while IFS='=' read -r name image; do
            name="$(echo "$name" | xargs)"
            image="$(echo "$image" | xargs)"
            [ -n "$name" ] || continue
            if [ -z "$image" ]; then
              echo "Missing image parameter: $name" >&2
              missing=1
              continue
            fi
            echo "Checking image manifest: $name=$image"
            if ! image_exists "$image"; then
              echo "Missing image manifest: $name=$image" >&2
              missing=1
              continue
            fi
            if [ "${ENVIRONMENT:-dev}" = "production" ]; then
              if ! inspect_image_platform "$name" "$image" "linux/amd64"; then
                platform_mismatch=1
              fi
            fi
          done <<EOF
$images
EOF

          if [ "$missing" != "0" ]; then
            echo "One or more requested images are missing; refusing to restart pods." >&2
            exit 1
          fi
          if [ "$platform_mismatch" != "0" ]; then
            echo "One or more production images are not linux/amd64; refusing to deploy to CentOS amd64 Kubernetes nodes." >&2
            exit 1
          fi
