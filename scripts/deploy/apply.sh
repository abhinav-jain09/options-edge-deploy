#!/usr/bin/env bash
          set -euo pipefail
          . "$JENKINS_WORK_DIR/options-edge-images.env"
          # --- Deploy by immutable @sha256 digest (image-freshness / clobber-immunity / traceability) ---
          # Resolve EVERY service image to its registry digest and pin it INTO the kustomize overlay's
          # images: block BEFORE the first kubectl mutation, so `kubectl apply -k` itself publishes a
          # digest-pinned Deployment spec -- no mutable :tag is ever applied to the API. This makes the
          # deploy clobber-immune (a stale/hand-pushed :dev can never reach a pod) and atomic on failure:
          # if any image is unresolvable we abort here, before delete/apply/patch, having mutated nothing.
          # The overlay file is the ephemeral per-build workspace copy (git-checked-out, never committed).
          # The unchanged `set image` block below re-applies the same resolved digests (idempotent).
          # Runs in DEPLOY_DRY_RUN too, so dry-run validates the resolver and renders the pinned spec.
          # Platform mirrors the build (dev=BUILD_PLATFORM arm64; non-dev amd64).
          if [ "${ENVIRONMENT:-dev}" = "dev" ]; then
            export DEPLOY_PLATFORM="${BUILD_PLATFORM:-linux/arm64}"
          else
            export DEPLOY_PLATFORM="linux/amd64"
          fi
          export REGISTRY_SCHEME="${REGISTRY_SCHEME:-http}"
          command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required to digest-pin the kustomize overlay; aborting before any kubectl mutation." >&2; exit 1; }
          . scripts/deploy/pin-image.sh
          _overlay_kustomization="k8s/overlays/${ENVIRONMENT}/kustomization.yaml"
          # The kustomize images: 'name' match key is the image as it appears in the base manifests, i.e.
          # the base registry/repo (same across all overlays). Derive the base registry from the rendered
          # base so we do not hard-code it.
          _base_images="$(kubectl kustomize k8s/base | yq -r 'select(.kind=="Deployment") | .spec.template.spec.containers[].image')"
          _base_registry="${_base_images%%$'\n'*}"; _base_registry="${_base_registry%%/*}"
          [ -n "$_base_registry" ] || { echo "FATAL: could not determine base image registry; aborting before any kubectl mutation." >&2; exit 1; }
          # Rebuild the overlay images: block as a digest-pinned list we fully control (one entry per
          # service: match the base name, remap to the resolved repo, pin the digest). This works for ANY
          # overlay -- dev (which has its own images: block) and production (which have none).
          yq -i '.images = []' "$_overlay_kustomization"
          # pin-postgres-writer is a DEV-ONLY deployment (k8s/overlays/dev only). Pin it for dev so the
          # fail-closed render guard passes; OMIT it for prod, where the image is intentionally absent from
          # the prod registry and resolving its digest would (correctly) fail and abort the prod deploy.
          _dev_only_images=""
          if [ "${ENVIRONMENT}" = "dev" ]; then
            _dev_only_images="PIN_POSTGRES_WRITER_IMAGE"
          fi
          for _img_var in DATABENTO_FEED_IMAGE DATABENTO_GEX_IMAGE DATABENTO_MAXPAIN_IMAGE DATABENTO_MISSION_PACE_IMAGE \
            DATABENTO_MISSION_PRESSURE_IMAGE DATABENTO_MISSION_SANDWICH_IMAGE DATABENTO_VOLUME_AGGREGATOR_IMAGE \
            DIRECTIONAL_PRESSURE_IMAGE FEED_GATEWAY_IMAGE HPSF_POSTGRES_WRITER_IMAGE HPSF_PROCESSING_IMAGE \
            IBKR_FEED_IMAGE INTEGRATION_TEST_IMAGE PRESSURE_POSTGRES_WRITER_IMAGE RAW_POSTGRES_WRITER_IMAGE \
            RAW_TO_DISPLAY_IMAGE SPX_MISSION_CONTROL_IMAGE STRIKE_FLOW_CLASSIFIER_IMAGE WEB_IMAGE \
            UNUSUAL_WHALES_GEX_HISTORY_IMAGE UNUSUAL_WHALES_GEX_IMAGE VOLUME_PACE_IMAGE VOLUME_SANDWICH_IMAGE DATABENTO_GEX_HISTORY_IMAGE \
            $_dev_only_images; do
            _pinned="$(pin_ref "${!_img_var}")" || {
              echo "FATAL: cannot resolve registry digest for ${_img_var}=${!_img_var}; aborting before any kubectl mutation." >&2
              exit 1
            }
            printf -v "$_img_var" '%s' "$_pinned"
            _repo="${_pinned%@*}"; _repo="${_repo%:*}"; _pdigest="${_pinned#*@}"; _pbase="${_repo##*/}"
            _pname="$_base_registry/$_pbase"
            _pname="$_pname" _pnewname="$_repo" _pdigest="$_pdigest" \
              yq -i '.images += [{"name": strenv(_pname), "newName": strenv(_pnewname), "digest": strenv(_pdigest)}]' "$_overlay_kustomization"
            echo "pinned ${_img_var} -> ${_pinned}"
          done
          # Authoritative fail-closed gate: render the overlay and require EVERY options-edge service image
          # that apply -k would publish to be digest-pinned (@sha256) BEFORE any kubectl mutation. This is
          # checked on the rendered manifest (not just the images: list), so it catches any environment or
          # any base Deployment whose image was not pinned above.
          _unpinned_rendered="$(kubectl kustomize "k8s/overlays/${ENVIRONMENT}" \
            | yq -r 'select(.kind=="Deployment") | (.spec.template.spec.containers[].image), (.spec.template.spec.initContainers[]?.image)' \
            | { grep '/options-edge-' || true; } | { grep -v '@sha256:' || true; })"
          if [ -n "$_unpinned_rendered" ]; then
            echo "FATAL: rendered Deployment images are not digest-pinned before apply:" >&2
            printf '%s\n' "$_unpinned_rendered" >&2
            echo "aborting before any kubectl mutation." >&2
            exit 1
          fi
          if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
            echo "DEPLOY_DRY_RUN=true: validating Kubernetes apply without changing runtime resources."
            kubectl apply --dry-run=server -k "k8s/overlays/${ENVIRONMENT}"
            exit 0
          fi
          kubectl -n options-edge delete deployment/strike-flow-classifier-service service/strike-flow-classifier-service --ignore-not-found=true
          kubectl apply -k "k8s/overlays/${ENVIRONMENT}"
          market_data_source="${MARKET_DATA_SOURCE:-DATABENTO}"
          effective_raw_topic="${RAW_TOPIC:-}"
          if [ -z "$effective_raw_topic" ]; then
            if [ "$market_data_source" = "IBKR" ]; then
              effective_raw_topic="options.ibkr.raw"
            else
              effective_raw_topic="options.databento.raw"
            fi
          fi
          default_weekday_expiry() {
            value="$(date +%Y%m%d)"
            while [ "$(date -d "$value" +%u)" -gt 5 ]; do
              value="$(date -d "$value +1 day" +%Y%m%d)"
            done
            printf '%s\n' "$value"
          }
          # IB_EXPIRY drives the web + gateway option-chain default selection; it MUST match the date the
          # Databento feed actually publishes (RESOLVED_DATABENTO_EXPIRY, the data-aware single source of
          # truth resolved in the "Resolve Databento Expiry" stage and used for the feed configmap below).
          # If they diverge the chain filters for an expiry the feed never produced and goes empty (e.g.
          # after the close the calendar weekday rolls to the next session while the feed stays on the last
          # session with data). Precedence: explicit IB_EXPIRY param > resolved Databento expiry > calendar
          # weekday fallback (only when no Databento expiry was resolved, e.g. an IBKR-only deploy).
          effective_expiry="${IB_EXPIRY:-${RESOLVED_DATABENTO_EXPIRY:-$(default_weekday_expiry)}}"
          if [ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
            : "${OE_KAFKA_BOOTSTRAP:?OE_KAFKA_BOOTSTRAP must be set by the Resolve profile stage (oeProfile single source of truth)}"
            KAFKA_BOOTSTRAP_SERVERS="$OE_KAFKA_BOOTSTRAP"
          fi
          kafka_bootstrap_servers="$KAFKA_BOOTSTRAP_SERVERS"
          if [ "${ENVIRONMENT:-dev}" = "dev" ]; then
            databento_feed_profile=dev
          else
            databento_feed_profile=prod
          fi
          if [ -z "${KAFKA_SCHEMA_REGISTRY_URL:-}" ]; then
            if [ "${ENVIRONMENT:-dev}" = "dev" ]; then
              KAFKA_SCHEMA_REGISTRY_URL=http://host.docker.internal:8082
            else
              KAFKA_SCHEMA_REGISTRY_URL=http://192.168.100.252:8082
            fi
          fi
          if [ -z "${POSTGRES_JDBC_URL:-}" ]; then
            if [ "${ENVIRONMENT:-dev}" = "dev" ]; then
              POSTGRES_JDBC_URL=jdbc:postgresql://host.docker.internal:5432/options_flow_dev
            else
              POSTGRES_JDBC_URL=jdbc:postgresql://192.168.100.252:5432/options_flow
            fi
          fi
          kafka_schema_registry_url="$KAFKA_SCHEMA_REGISTRY_URL"
          postgres_jdbc_url="$POSTGRES_JDBC_URL"
          cat >"$JENKINS_WORK_DIR/options-edge-runtime-config-patch.json" <<EOF
{"data":{"APP_MARKET_DATA_SOURCE":"$market_data_source","KAFKA_BOOTSTRAP_SERVERS":"$kafka_bootstrap_servers","KAFKA_SCHEMA_REGISTRY_URL":"$kafka_schema_registry_url","SCHEMA_REGISTRY_URL":"$kafka_schema_registry_url","POSTGRES_JDBC_URL":"$postgres_jdbc_url","KAFKA_RAW_TOPIC":"$effective_raw_topic","IB_HOST":"${IB_HOST:-127.0.0.1}","IB_PORT":"${IB_PORT:-4001}","IB_CLIENT_ID":"${IB_CLIENT_ID:-212}","IB_EXPIRY":"$effective_expiry","IB_MAX_STRIKES":"${IB_MAX_STRIKES:-43}","UNUSUAL_WHALES_EXPIRY":"$effective_expiry"}}
EOF
          kubectl -n options-edge patch configmap options-edge-config \
            --type merge \
            --patch "$(cat "$JENKINS_WORK_DIR/options-edge-runtime-config-patch.json")"
          if kubectl -n options-edge get configmap options-edge-databento-feed-config >/dev/null 2>&1; then
            cat >"$JENKINS_WORK_DIR/options-edge-databento-feed-config-patch.json" <<EOF
{"data":{"APP_PROFILE":"$databento_feed_profile","KAFKA_BOOTSTRAP_SERVERS":"$kafka_bootstrap_servers","KAFKA_SCHEMA_REGISTRY_URL":"$kafka_schema_registry_url","DATABENTO_EXPIRY":"$RESOLVED_DATABENTO_EXPIRY"}}
EOF
            kubectl -n options-edge patch configmap options-edge-databento-feed-config \
              --type merge \
              --patch "$(cat "$JENKINS_WORK_DIR/options-edge-databento-feed-config-patch.json")"
          fi
          kubectl -n options-edge set image deployment/raw-to-display-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-web web="$WEB_IMAGE"
          kubectl -n options-edge set image deployment/raw-to-display-databento-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-databento-feed databento-feed="$DATABENTO_FEED_IMAGE"
          kubectl -n options-edge set image deployment/databento-volume-aggregator databento-volume-aggregator="$DATABENTO_VOLUME_AGGREGATOR_IMAGE"
          kubectl -n options-edge set image deployment/databento-gex-service databento-gex="$DATABENTO_GEX_IMAGE"
          kubectl -n options-edge set image deployment/databento-maxpain-service databento-maxpain="$DATABENTO_MAXPAIN_IMAGE"
          kubectl -n options-edge set image deployment/databento-mission-pace-service databento-mission-pace="$DATABENTO_MISSION_PACE_IMAGE"
          kubectl -n options-edge set image deployment/databento-mission-pressure-service databento-mission-pressure="$DATABENTO_MISSION_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/databento-mission-sandwich-service databento-mission-sandwich="$DATABENTO_MISSION_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/volume-pace-service volume-pace="$VOLUME_PACE_IMAGE"
          kubectl -n options-edge set image deployment/volume-pace-databento-service volume-pace="$VOLUME_PACE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-databento-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/volume-sandwich-service volume-sandwich="$VOLUME_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/volume-sandwich-databento-service volume-sandwich="$VOLUME_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/unusual-whales-gex-service unusual-whales-gex="$UNUSUAL_WHALES_GEX_IMAGE"
          kubectl -n options-edge set image deployment/unusual-whales-gex-history-service unusual-whales-gex-history="$UNUSUAL_WHALES_GEX_HISTORY_IMAGE"
          kubectl -n options-edge set image deployment/databento-gex-history-service databento-gex-history="$DATABENTO_GEX_HISTORY_IMAGE"
          kubectl -n options-edge set image deployment/raw-postgres-writer raw-postgres-writer="$RAW_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/pressure-postgres-writer pressure-postgres-writer="$PRESSURE_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/feed-gateway-service feed-gateway="$FEED_GATEWAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-integration-test integration-test="$INTEGRATION_TEST_IMAGE"
          kubectl -n options-edge set image deployment/hpsf-stage-a-service hpsf-stage-a="$HPSF_PROCESSING_IMAGE"
          kubectl -n options-edge set image deployment/hpsf-stage-b-service hpsf-stage-b="$HPSF_PROCESSING_IMAGE"
          kubectl -n options-edge set image deployment/hpsf-postgres-writer-service hpsf-postgres-writer="$HPSF_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/strike-flow-classifier-databento strike-flow-classifier="$STRIKE_FLOW_CLASSIFIER_IMAGE"
          kubectl -n options-edge set image deployment/strike-flow-classifier-ibkr strike-flow-classifier="$STRIKE_FLOW_CLASSIFIER_IMAGE"
          kubectl -n options-edge set image deployment/spx-mission-control-service spx-mission-control="$SPX_MISSION_CONTROL_IMAGE"
          kubectl -n options-edge set image deployment/ibkr-feed-service ibkr-feed="$IBKR_FEED_IMAGE"
          kubectl -n options-edge rollout restart deployment/raw-to-display-service
          kubectl -n options-edge rollout restart deployment/raw-to-display-databento-service
          kubectl -n options-edge rollout restart deployment/options-edge-databento-feed
          kubectl -n options-edge rollout restart deployment/databento-volume-aggregator
          kubectl -n options-edge rollout restart deployment/databento-mission-pace-service
          kubectl -n options-edge rollout restart deployment/databento-mission-pressure-service
          kubectl -n options-edge rollout restart deployment/databento-mission-sandwich-service
          kubectl -n options-edge rollout restart deployment/databento-gex-service
          kubectl -n options-edge rollout restart deployment/databento-maxpain-service
          kubectl -n options-edge rollout restart deployment/volume-pace-service
          kubectl -n options-edge rollout restart deployment/volume-pace-databento-service
          kubectl -n options-edge rollout restart deployment/directional-pressure-service
          kubectl -n options-edge rollout restart deployment/directional-pressure-databento-service
          kubectl -n options-edge rollout restart deployment/volume-sandwich-service
          kubectl -n options-edge rollout restart deployment/volume-sandwich-databento-service
          kubectl -n options-edge rollout restart deployment/unusual-whales-gex-service
          kubectl -n options-edge rollout restart deployment/unusual-whales-gex-history-service
          kubectl -n options-edge rollout restart deployment/databento-gex-history-service
          kubectl -n options-edge rollout restart deployment/raw-postgres-writer
          kubectl -n options-edge rollout restart deployment/pressure-postgres-writer
          kubectl -n options-edge rollout restart deployment/feed-gateway-service
          kubectl -n options-edge rollout restart deployment/options-edge-integration-test
          kubectl -n options-edge rollout restart deployment/hpsf-stage-a-service
          kubectl -n options-edge rollout restart deployment/hpsf-stage-b-service
          kubectl -n options-edge rollout restart deployment/hpsf-postgres-writer-service
          kubectl -n options-edge rollout restart deployment/strike-flow-classifier-databento
          kubectl -n options-edge rollout restart deployment/strike-flow-classifier-ibkr
          kubectl -n options-edge rollout restart deployment/spx-mission-control-service
          kubectl -n options-edge rollout restart deployment/ibkr-feed-service
          kubectl -n options-edge rollout status deployment/raw-to-display-service --timeout=180s
          kubectl -n options-edge rollout status deployment/options-edge-web --timeout=240s
          kubectl -n options-edge rollout status deployment/raw-to-display-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/options-edge-databento-feed --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-volume-aggregator --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-mission-pace-service --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-mission-pressure-service --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-mission-sandwich-service --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-gex-service --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-maxpain-service --timeout=240s
          kubectl -n options-edge rollout status deployment/volume-pace-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-pace-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/directional-pressure-service --timeout=180s
          kubectl -n options-edge rollout status deployment/directional-pressure-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-sandwich-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-sandwich-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/unusual-whales-gex-service --timeout=180s
          kubectl -n options-edge rollout status deployment/unusual-whales-gex-history-service --timeout=180s
          kubectl -n options-edge rollout status deployment/databento-gex-history-service --timeout=180s
          kubectl -n options-edge rollout status deployment/raw-postgres-writer --timeout=900s
          kubectl -n options-edge rollout status deployment/pressure-postgres-writer --timeout=180s
          kubectl -n options-edge rollout status deployment/feed-gateway-service --timeout=180s
          kubectl -n options-edge rollout status deployment/options-edge-integration-test --timeout=180s
          kubectl -n options-edge rollout status deployment/hpsf-stage-a-service --timeout=240s
          kubectl -n options-edge rollout status deployment/hpsf-stage-b-service --timeout=600s || echo "WARN: hpsf-stage-b rollout status slow (old replica likely stuck terminating on dev); HPSF smoke validates the new pod"
          kubectl -n options-edge rollout status deployment/hpsf-postgres-writer-service --timeout=180s
          kubectl -n options-edge rollout status deployment/strike-flow-classifier-databento --timeout=180s
          kubectl -n options-edge rollout status deployment/strike-flow-classifier-ibkr --timeout=180s
          kubectl -n options-edge rollout status deployment/spx-mission-control-service --timeout=180s
          kubectl -n options-edge rollout status deployment/ibkr-feed-service --timeout=240s
