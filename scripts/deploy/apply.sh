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
          . scripts/deploy/image-lock.sh
          case "${DEPLOY_TARGET:-all}" in
            all)
              ;;
            delta-flow-service)
              DELTA_FLOW_IMAGE="$(pin_ref "$DELTA_FLOW_IMAGE")" || {
                echo "FATAL: cannot resolve registry digest for DELTA_FLOW_IMAGE=$DELTA_FLOW_IMAGE; aborting before any kubectl mutation." >&2
                exit 1
              }
              export DELTA_FLOW_IMAGE
              echo "pinned DELTA_FLOW_IMAGE -> $DELTA_FLOW_IMAGE"
              rewrite_images_env "$JENKINS_WORK_DIR/options-edge-images.env"
              _target_render="$JENKINS_WORK_DIR/options-edge-${ENVIRONMENT}-delta-flow.yaml"
              kubectl kustomize "k8s/overlays/${ENVIRONMENT}" \
                | yq '
                    select(
                      (.kind == "Namespace" and .metadata.name == "options-edge")
                      or (.kind == "ConfigMap" and .metadata.name == "options-edge-config")
                      or (.kind == "Service" and .metadata.name == "delta-flow-service")
                      or (.kind == "Deployment" and .metadata.name == "delta-flow-service")
                    )
                  ' >"$_target_render"
              yq -i '(. | select(.kind == "Deployment" and .metadata.name == "delta-flow-service").spec.template.spec.containers[] | select(.name == "delta-flow").image) = strenv(DELTA_FLOW_IMAGE)' "$_target_render"
              _unpinned_rendered="$(yq -r 'select(.kind=="Deployment") | (.spec.template.spec.containers[].image), (.spec.template.spec.initContainers[]?.image)' "$_target_render" \
                | { grep '/options-edge-' || true; } | { grep -v '@sha256:' || true; })"
              if [ -n "$_unpinned_rendered" ]; then
                echo "FATAL: rendered Delta Flow Deployment image is not digest-pinned before apply:" >&2
                printf '%s\n' "$_unpinned_rendered" >&2
                echo "aborting before any kubectl mutation." >&2
                exit 1
              fi
              if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
                echo "DEPLOY_DRY_RUN=true: validating Delta Flow targeted apply without changing runtime resources."
                kubectl apply --dry-run=server -f "$_target_render"
                exit 0
              fi
              kubectl apply -f "$_target_render"
              kubectl -n options-edge rollout status deployment/delta-flow-service --timeout=1260s
              scripts/deploy/verify-running-images.sh "$JENKINS_WORK_DIR/options-edge-images.env"
              exit 0
              ;;
            dealer-ledger-service)
              DEALER_LEDGER_IMAGE="$(pin_ref "$DEALER_LEDGER_IMAGE")" || {
                echo "FATAL: cannot resolve registry digest for DEALER_LEDGER_IMAGE=$DEALER_LEDGER_IMAGE; aborting before any kubectl mutation." >&2
                exit 1
              }
              export DEALER_LEDGER_IMAGE
              echo "pinned DEALER_LEDGER_IMAGE -> $DEALER_LEDGER_IMAGE"
              rewrite_images_env "$JENKINS_WORK_DIR/options-edge-images.env"
              _target_render="$JENKINS_WORK_DIR/options-edge-${ENVIRONMENT}-dealer-ledger.yaml"
              kubectl kustomize "k8s/overlays/${ENVIRONMENT}" \
                | yq '
                    select(
                      (.kind == "Namespace" and .metadata.name == "options-edge")
                      or (.kind == "ConfigMap" and .metadata.name == "options-edge-config")
                      or (.kind == "Service" and .metadata.name == "dealer-ledger-service")
                      or (.kind == "Deployment" and .metadata.name == "dealer-ledger-service")
                    )
                  ' >"$_target_render"
              yq -i '(. | select(.kind == "Deployment" and .metadata.name == "dealer-ledger-service").spec.template.spec.containers[] | select(.name == "dealer-ledger").image) = strenv(DEALER_LEDGER_IMAGE)' "$_target_render"
              _unpinned_rendered="$(yq -r 'select(.kind=="Deployment") | (.spec.template.spec.containers[].image), (.spec.template.spec.initContainers[]?.image)' "$_target_render" \
                | { grep '/options-edge-' || true; } | { grep -v '@sha256:' || true; })"
              if [ -n "$_unpinned_rendered" ]; then
                echo "FATAL: rendered Dealer Ledger Deployment image is not digest-pinned before apply:" >&2
                printf '%s\n' "$_unpinned_rendered" >&2
                echo "aborting before any kubectl mutation." >&2
                exit 1
              fi
              if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
                echo "DEPLOY_DRY_RUN=true: validating Dealer Ledger targeted apply without changing runtime resources."
                kubectl apply --dry-run=server -f "$_target_render"
                exit 0
              fi
              kubectl apply -f "$_target_render"
              kubectl -n options-edge rollout status deployment/dealer-ledger-service --timeout=1260s
              scripts/deploy/verify-running-images.sh "$JENKINS_WORK_DIR/options-edge-images.env"
              exit 0
              ;;
            strike-liquidity-heatmap-service)
              STRIKE_LIQUIDITY_HEATMAP_IMAGE="$(pin_ref "$STRIKE_LIQUIDITY_HEATMAP_IMAGE")" || {
                echo "FATAL: cannot resolve registry digest for STRIKE_LIQUIDITY_HEATMAP_IMAGE=$STRIKE_LIQUIDITY_HEATMAP_IMAGE; aborting before any kubectl mutation." >&2
                exit 1
              }
              export STRIKE_LIQUIDITY_HEATMAP_IMAGE
              echo "pinned STRIKE_LIQUIDITY_HEATMAP_IMAGE -> $STRIKE_LIQUIDITY_HEATMAP_IMAGE"
              rewrite_images_env "$JENKINS_WORK_DIR/options-edge-images.env"
              _target_render="$JENKINS_WORK_DIR/options-edge-${ENVIRONMENT}-strike-liquidity-heatmap.yaml"
              kubectl kustomize "k8s/overlays/${ENVIRONMENT}" \
                | yq '
                    select(
                      (.kind == "Namespace" and .metadata.name == "options-edge")
                      or (.kind == "ConfigMap" and .metadata.name == "options-edge-config")
                      or (.kind == "PersistentVolumeClaim" and .metadata.name == "strike-liquidity-heatmap-service-streams-state")
                      or (.kind == "Service" and .metadata.name == "strike-liquidity-heatmap-service")
                      or (.kind == "Deployment" and .metadata.name == "strike-liquidity-heatmap-service")
                    )
                  ' >"$_target_render"
              yq -i '(. | select(.kind == "Deployment" and .metadata.name == "strike-liquidity-heatmap-service").spec.template.spec.containers[] | select(.name == "strike-liquidity-heatmap").image) = strenv(STRIKE_LIQUIDITY_HEATMAP_IMAGE)' "$_target_render"
              _unpinned_rendered="$(yq -r 'select(.kind=="Deployment") | (.spec.template.spec.containers[].image), (.spec.template.spec.initContainers[]?.image)' "$_target_render" \
                | { grep '/options-edge-' || true; } | { grep -v '@sha256:' || true; })"
              if [ -n "$_unpinned_rendered" ]; then
                echo "FATAL: rendered Strike Liquidity Heatmap Deployment image is not digest-pinned before apply:" >&2
                printf '%s\n' "$_unpinned_rendered" >&2
                echo "aborting before any kubectl mutation." >&2
                exit 1
              fi
              if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
                echo "DEPLOY_DRY_RUN=true: validating Strike Liquidity Heatmap targeted apply without changing runtime resources."
                kubectl apply --dry-run=server -f "$_target_render"
                exit 0
              fi
              kubectl apply -f "$_target_render"
              kubectl -n options-edge rollout status deployment/strike-liquidity-heatmap-service --timeout=1260s
              scripts/deploy/verify-running-images.sh "$JENKINS_WORK_DIR/options-edge-images.env"
              exit 0
              ;;
            *)
              echo "Unsupported DEPLOY_TARGET: ${DEPLOY_TARGET}" >&2
              exit 1
              ;;
          esac
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
          # pin-postgres-writer now ships in k8s/base, so it renders in BOTH dev and prod and must be
          # digest-pinned in every environment (its prod image must exist in the prod registry before
          # promotion, enforced by Image Preflight). It is therefore in the main pin list below.
          for _img_var in DATABENTO_FEED_IMAGE DATABENTO_GEX_IMAGE OPTION_PRICE_BEHAVIOR_IMAGE \
            DATABENTO_MISSION_SANDWICH_IMAGE DATABENTO_VOLUME_AGGREGATOR_IMAGE \
            DIRECTIONAL_PRESSURE_IMAGE FEED_GATEWAY_IMAGE HPSF_POSTGRES_WRITER_IMAGE HPSF_PROCESSING_IMAGE \
            IBKR_FEED_IMAGE PIN_POSTGRES_WRITER_IMAGE PRESSURE_POSTGRES_WRITER_IMAGE RAW_POSTGRES_WRITER_IMAGE \
            RAW_TO_DISPLAY_IMAGE SPX_MISSION_CONTROL_IMAGE STRIKE_FLOW_CLASSIFIER_IMAGE WEB_IMAGE \
            VOLUME_PACE_IMAGE DATABENTO_GEX_HISTORY_IMAGE GAMMA_MIGRATION_IMAGE \
            DELTA_FLOW_IMAGE DEALER_LEDGER_IMAGE DEALER_LEDGER_CALIBRATION_IMAGE STRIKE_LIQUIDITY_HEATMAP_IMAGE UNIFIED_SR_IMAGE STRIKE_INTELLIGENCE_IMAGE OPTION_TRUTH_ENGINE_IMAGE MARKET_CARRY_IMAGE ES_SPX_ALIGN_IMAGE DATABENTO_SR3_FEED_IMAGE VIX_OPTION_INTELIGENCE_IMAGE STRIKE_FLOW_AVRO_ADAPTER_IMAGE GEX_DELTA_REDIS_WRITER_IMAGE DATABENTO_MAXPAIN_IMAGE \
            STRIKE_INVASION_IMAGE INVASION_POSTGRES_WRITER_IMAGE SPREAD_SKEW_IMAGE SPREAD_SKEW_POSTGRES_WRITER_IMAGE REVERSAL_CONFIRMATION_IMAGE CORRIDOR_GAUGE_IMAGE ES_OPEN_DIRECTION_IMAGE ES_OPEN_DIRECTION_POSTGRES_WRITER_IMAGE CLOSE_DIRECTION_IMAGE SPOT_VOL_REGIME_IMAGE INDICATOR_SERVICE_IMAGE STOCK_GEX_IMAGE DROP_CLASSIFIER_IMAGE OI_SHADOW_IMAGE REVERSAL_POSTGRES_WRITER_IMAGE GREEK_MOVE_AUTHENTICITY_IMAGE; do
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
          # short-premium-agent AND signal-follower render in the dev AND production overlays
          # (standalone services that run on prod too), but NOT experiment. Pin it for dev+production so both
          # `DEPLOY_TARGET=all` renders pass the digest gate below; experiment never renders it and this
          # block is skipped there (so pin_ref is never asked to resolve a non-existent experiment image).
          if [ "${ENVIRONMENT}" = "dev" ] || [ "${ENVIRONMENT}" = "production" ]; then
            for _img_var in SHORT_PREMIUM_AGENT_IMAGE SIGNAL_FOLLOWER_IMAGE CONTEXT_TAPE_IMAGE MULTILEG_STRUCTURE_IMAGE; do
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
          fi
          rewrite_images_env "$JENKINS_WORK_DIR/options-edge-images.env"
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
          # Reconcile-delete the timewarp replay workloads. They were removed from the overlay
          # (they deploy on-demand via replay-deploy / k8s/replay), but `kubectl apply -k` never
          # prunes resources dropped from a manifest — so a prior replay run would otherwise linger
          # through every full deploy. A full service deploy must NOT carry replay: delete them here
          # (idempotent, --ignore-not-found; a no-op in prod, which never had them).
          kubectl -n options-edge delete deployment/databento-timewarp-snapshot-replay job/databento-timewarp-replay --ignore-not-found=true
          # Reconcile-delete the long-retired versioned option-price-behavior deployment. The canonical
          # workload is `option-price-behavior-service` (ONE unversioned identity); the old separate
          # `option-price-behavior-service-v2` deployment is NOT in any overlay, so `apply -k` never prunes
          # it and it would run a duplicate forever. Delete it here (idempotent, deployer-SA-scoped).
          kubectl -n options-edge delete deployment/option-price-behavior-service-v2 --ignore-not-found=true
          # Reconcile-delete the retired standalone `pin-flow-explorer` workload. It was a standalone internal
          # web tool that has been dropped (rebuilt as an option-chain UI page instead), so it is no longer in
          # any overlay and `apply -k` will not prune it. Delete it here (idempotent, deployer-SA-scoped).
          kubectl -n options-edge delete deployment/pin-flow-explorer service/pin-flow-explorer --ignore-not-found=true
          # Reconcile-delete the pre-rename es-gex-spx-align-service. It was renamed to es-spx-align-service
          # in this same commit (One Service One Identity). Both byte-copy the ES-GEX book to the SAME
          # options.es-gex-spx-aligned topic, so leaving the old one running would put TWO producers on the
          # compacted book — their independent emitEventTimeMs stamps would fight the gateway's roll-forward
          # and flicker the live overlay. `apply -k` never prunes, so delete the old workload here as part of
          # the atomic cutover (idempotent, deployer-SA-scoped). Its rotated app-id changelog/repartition
          # topics are cleaned separately per docs/es-spx-align-cutover-runbook.md (never auto-deleted here).
          kubectl -n options-edge delete deployment/es-gex-spx-align-service --ignore-not-found=true
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
          # In AUTO mode the feed + gateway self-resolve + daily-roll the expiry from their embedded
          # MarketCalendar (same date), so IB_EXPIRY=AUTO keeps the gateway locked to the feed.
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
          databento_market_open_alignment="${DATABENTO_MARKET_OPEN_ALIGNMENT:-true}"
          kafka_schema_registry_url="$KAFKA_SCHEMA_REGISTRY_URL"
          postgres_jdbc_url="$POSTGRES_JDBC_URL"
          cat >"$JENKINS_WORK_DIR/options-edge-runtime-config-patch.json" <<EOF
{"data":{"APP_MARKET_DATA_SOURCE":"$market_data_source","KAFKA_BOOTSTRAP_SERVERS":"$kafka_bootstrap_servers","KAFKA_SCHEMA_REGISTRY_URL":"$kafka_schema_registry_url","SCHEMA_REGISTRY_URL":"$kafka_schema_registry_url","POSTGRES_JDBC_URL":"$postgres_jdbc_url","KAFKA_RAW_TOPIC":"$effective_raw_topic","IB_HOST":"${IB_HOST:-127.0.0.1}","IB_PORT":"${IB_PORT:-4001}","IB_CLIENT_ID":"${IB_CLIENT_ID:-212}","IB_EXPIRY":"$effective_expiry","IB_MAX_STRIKES":"${IB_MAX_STRIKES:-43}"}}
EOF
          kubectl -n options-edge patch configmap options-edge-config \
            --type merge \
            --patch "$(cat "$JENKINS_WORK_DIR/options-edge-runtime-config-patch.json")"
          if kubectl -n options-edge get configmap options-edge-databento-feed-config >/dev/null 2>&1; then
            cat >"$JENKINS_WORK_DIR/options-edge-databento-feed-config-patch.json" <<EOF
{"data":{"APP_PROFILE":"$databento_feed_profile","KAFKA_BOOTSTRAP_SERVERS":"$kafka_bootstrap_servers","KAFKA_SCHEMA_REGISTRY_URL":"$kafka_schema_registry_url","DATABENTO_EXPIRY":"$RESOLVED_DATABENTO_EXPIRY","DATABENTO_MARKET_OPEN_ALIGNMENT":"$databento_market_open_alignment","DATABENTO_USE_LIVE_REPLAY":"false"}}
EOF
            kubectl -n options-edge patch configmap options-edge-databento-feed-config \
              --type merge \
              --patch "$(cat "$JENKINS_WORK_DIR/options-edge-databento-feed-config-patch.json")"
          fi
          kubectl -n options-edge set image deployment/raw-to-display-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-web web="$WEB_IMAGE"
          kubectl -n options-edge set image deployment/raw-to-display-databento-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-databento-feed databento-feed="$DATABENTO_FEED_IMAGE"
          # databento-vix-feed (VIX feed separation): IMAGE REUSE — same image as the
          # SPX feed, different entrypoint. Renders in BOTH envs since the 2026-07-28
          # cutover commit (dev overlay added).
          kubectl -n options-edge set image deployment/databento-vix-feed databento-vix-feed="$DATABENTO_FEED_IMAGE"
          kubectl -n options-edge set image deployment/databento-volume-aggregator databento-volume-aggregator="$DATABENTO_VOLUME_AGGREGATOR_IMAGE"
          kubectl -n options-edge set image deployment/databento-gex-service databento-gex="$DATABENTO_GEX_IMAGE"
          kubectl -n options-edge set image deployment/option-price-behavior-service option-price-behavior="$OPTION_PRICE_BEHAVIOR_IMAGE"
          kubectl -n options-edge set image deployment/databento-mission-sandwich-service databento-mission-sandwich="$DATABENTO_MISSION_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/volume-pace-databento-service volume-pace="$VOLUME_PACE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-databento-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/databento-gex-history-service databento-gex-history="$DATABENTO_GEX_HISTORY_IMAGE"
          kubectl -n options-edge set image deployment/gamma-migration-service gamma-migration="$GAMMA_MIGRATION_IMAGE"
          kubectl -n options-edge set image deployment/raw-postgres-writer raw-postgres-writer="$RAW_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/pin-postgres-writer pin-postgres-writer="$PIN_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/pressure-postgres-writer pressure-postgres-writer="$PRESSURE_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/feed-gateway-service feed-gateway="$FEED_GATEWAY_IMAGE"
          kubectl -n options-edge set image deployment/hpsf-postgres-writer-service hpsf-postgres-writer="$HPSF_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/strike-flow-classifier-databento strike-flow-classifier="$STRIKE_FLOW_CLASSIFIER_IMAGE"
          kubectl -n options-edge set image deployment/delta-flow-service delta-flow="$DELTA_FLOW_IMAGE"
          kubectl -n options-edge set image deployment/dealer-ledger-service dealer-ledger="$DEALER_LEDGER_IMAGE"
          kubectl -n options-edge set image deployment/strike-liquidity-heatmap-service strike-liquidity-heatmap="$STRIKE_LIQUIDITY_HEATMAP_IMAGE"
          kubectl -n options-edge set image deployment/spx-mission-control-service spx-mission-control="$SPX_MISSION_CONTROL_IMAGE"
          kubectl -n options-edge set image deployment/unified-sr-service unified-sr="$UNIFIED_SR_IMAGE"
          kubectl -n options-edge set image deployment/strike-intelligence-service strike-intelligence="$STRIKE_INTELLIGENCE_IMAGE"
          kubectl -n options-edge set image deployment/option-truth-engine-service option-truth-engine="$OPTION_TRUTH_ENGINE_IMAGE"
          kubectl -n options-edge set image deployment/market-carry-service market-carry="$MARKET_CARRY_IMAGE"
          kubectl -n options-edge set image deployment/es-spx-align-service es-spx-align="$ES_SPX_ALIGN_IMAGE"
          kubectl -n options-edge set image deployment/databento-sr3-feed-service databento-sr3-feed="$DATABENTO_SR3_FEED_IMAGE"
          kubectl -n options-edge set image deployment/vix-option-inteligence-service vix-option-inteligence="$VIX_OPTION_INTELIGENCE_IMAGE"
          kubectl -n options-edge set image deployment/greek-move-authenticity-service greek-move-authenticity="$GREEK_MOVE_AUTHENTICITY_IMAGE"
          kubectl -n options-edge set image deployment/strike-flow-avro-adapter strike-flow-avro-adapter="$STRIKE_FLOW_AVRO_ADAPTER_IMAGE"
          kubectl -n options-edge set image deployment/gex-delta-redis-writer gex-delta-redis-writer="$GEX_DELTA_REDIS_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/ibkr-feed-service ibkr-feed="$IBKR_FEED_IMAGE"
          kubectl -n options-edge rollout restart deployment/raw-to-display-service
          kubectl -n options-edge rollout restart deployment/raw-to-display-databento-service
          kubectl -n options-edge rollout restart deployment/options-edge-databento-feed
          # databento-vix-feed: renders in BOTH envs since the 2026-07-28 cutover commit.
          kubectl -n options-edge rollout restart deployment/databento-vix-feed
          kubectl -n options-edge rollout restart deployment/databento-volume-aggregator
          kubectl -n options-edge rollout restart deployment/databento-mission-sandwich-service
          kubectl -n options-edge rollout restart deployment/databento-gex-service
          kubectl -n options-edge rollout restart deployment/option-price-behavior-service
          kubectl -n options-edge rollout restart deployment/volume-pace-databento-service
          kubectl -n options-edge rollout restart deployment/directional-pressure-service
          kubectl -n options-edge rollout restart deployment/directional-pressure-databento-service
          kubectl -n options-edge rollout restart deployment/databento-gex-history-service
          kubectl -n options-edge rollout restart deployment/raw-postgres-writer
          kubectl -n options-edge rollout restart deployment/pin-postgres-writer
          kubectl -n options-edge rollout restart deployment/pressure-postgres-writer
          kubectl -n options-edge rollout restart deployment/feed-gateway-service
          kubectl -n options-edge rollout restart deployment/hpsf-postgres-writer-service
          kubectl -n options-edge rollout restart deployment/strike-flow-classifier-databento
          kubectl -n options-edge rollout restart deployment/delta-flow-service
          kubectl -n options-edge rollout restart deployment/dealer-ledger-service
          kubectl -n options-edge rollout restart deployment/strike-liquidity-heatmap-service
          kubectl -n options-edge rollout restart deployment/spx-mission-control-service
          kubectl -n options-edge rollout restart deployment/unified-sr-service
          kubectl -n options-edge rollout restart deployment/strike-intelligence-service
          kubectl -n options-edge rollout restart deployment/option-truth-engine-service
          kubectl -n options-edge rollout restart deployment/market-carry-service
          kubectl -n options-edge rollout restart deployment/databento-sr3-feed-service
          kubectl -n options-edge rollout restart deployment/vix-option-inteligence-service
          kubectl -n options-edge rollout restart deployment/strike-flow-avro-adapter
          kubectl -n options-edge rollout restart deployment/gex-delta-redis-writer
          kubectl -n options-edge rollout restart deployment/ibkr-feed-service
          kubectl -n options-edge rollout restart deployment/databento-maxpain-service
          # context-tape renders in dev+production only (same category/guard as
          # short-premium-agent below). Restart so ConfigMap/Secret-only changes reach its
          # envFrom (the apply above does not roll pods on config-only changes).
          if [ "${ENVIRONMENT}" = "dev" ] || [ "${ENVIRONMENT}" = "production" ]; then
            kubectl -n options-edge rollout restart deployment/context-tape-service
            # multileg-structure: same dev+production guard and the same reason — its Kafka
            # bootstrap arrives via envFrom, which an apply alone does not roll pods for.
            kubectl -n options-edge rollout restart deployment/multileg-structure-service
          fi
          kubectl -n options-edge rollout status deployment/raw-to-display-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/options-edge-web --timeout=1260s
          kubectl -n options-edge rollout status deployment/raw-to-display-databento-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/options-edge-databento-feed --timeout=1260s
          # databento-vix-feed: production-only (see the set-image note above); at the
          # committed replicas:0 state `rollout status` returns success immediately.
          if [ "${ENVIRONMENT:-dev}" = "production" ]; then
            kubectl -n options-edge rollout status deployment/databento-vix-feed --timeout=1260s
          else
            echo "NOTICE: skipping rollout status deployment/databento-vix-feed — ENVIRONMENT='${ENVIRONMENT:-dev}' != production (this deployment renders only in the production overlay)"
          fi
          kubectl -n options-edge rollout status deployment/databento-volume-aggregator --timeout=1260s
          kubectl -n options-edge rollout status deployment/databento-mission-sandwich-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/databento-gex-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/option-price-behavior-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/volume-pace-databento-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/directional-pressure-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/directional-pressure-databento-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/databento-gex-history-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/raw-postgres-writer --timeout=1260s
          kubectl -n options-edge rollout status deployment/pin-postgres-writer --timeout=1260s
          kubectl -n options-edge rollout status deployment/pressure-postgres-writer --timeout=1260s
          kubectl -n options-edge rollout status deployment/feed-gateway-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/hpsf-postgres-writer-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/strike-flow-classifier-databento --timeout=1260s
          kubectl -n options-edge rollout status deployment/delta-flow-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/dealer-ledger-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/strike-liquidity-heatmap-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/spx-mission-control-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/unified-sr-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/strike-intelligence-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/option-truth-engine-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/market-carry-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/databento-sr3-feed-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/vix-option-inteligence-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/strike-flow-avro-adapter --timeout=1260s
          kubectl -n options-edge rollout status deployment/gex-delta-redis-writer --timeout=1260s
          kubectl -n options-edge rollout status deployment/ibkr-feed-service --timeout=1260s
          kubectl -n options-edge rollout status deployment/databento-maxpain-service --timeout=1260s
          # short-premium-agent renders in dev+production, so an unready/crashlooping rollout must fail
          # the all-deploy in both. Guarded to dev+production because this deployment does not exist in
          # experiment.
          if [ "${ENVIRONMENT}" = "dev" ] || [ "${ENVIRONMENT}" = "production" ]; then
            kubectl -n options-edge rollout status deployment/short-premium-agent-service --timeout=1260s
            # context-tape: same dev+production guard. An unready/crashlooping backfill must
            # fail the all-deploy, not leave it green with the pod NOT READY.
            kubectl -n options-edge rollout status deployment/context-tape-service --timeout=1260s
            # multileg-structure: a pod that cannot reach the broker must fail the all-deploy
            # rather than leave it green with the workload NOT READY.
            kubectl -n options-edge rollout status deployment/multileg-structure-service --timeout=1260s
          fi
          scripts/deploy/verify-running-images.sh "$JENKINS_WORK_DIR/options-edge-images.env"
          # Identity migration cleanup occurs only after the replacement has rolled out and
          # passed the digest verification above. A failed replacement therefore leaves the
          # legacy workload available for rollback instead of creating an outage.
          kubectl -n options-edge delete deployment zero-dte-intelligence-service --ignore-not-found
          kubectl -n options-edge delete service zero-dte-intelligence-service --ignore-not-found
