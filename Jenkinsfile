pipeline {
  agent any
  parameters {
    choice(name: 'ENVIRONMENT', choices: ['dev', 'staging', 'production'], description: 'Target environment')
    string(name: 'KUBECONFIG_FILE', defaultValue: '/home/options-edge/config/kubeconfig', description: 'Kubeconfig path on Jenkins agent')
    string(name: 'IMAGE_REGISTRY', defaultValue: '192.168.100.252:5000', description: 'Docker registry namespace used when IMAGE_TAG is set')
    string(name: 'IMAGE_TAG', defaultValue: '', description: 'Exact Docker tag to use for all runtime images. Empty keeps per-image parameters.')
    string(name: 'RAW_TO_DISPLAY_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-raw-to-display:dev', description: 'Raw-to-display image')
    string(name: 'DATABENTO_VOLUME_AGGREGATOR_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-databento-volume-aggregator:dev', description: 'Databento volume aggregator image')
    string(name: 'VOLUME_PACE_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-volume-pace:dev', description: 'Volume-pace image')
    string(name: 'DIRECTIONAL_PRESSURE_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-directional-pressure:dev', description: 'Directional-pressure image')
    string(name: 'VOLUME_SANDWICH_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-volume-sandwich:dev', description: 'Volume-sandwich image')
    string(name: 'UNUSUAL_WHALES_GEX_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-unusual-whales-gex:dev', description: 'Unusual Whales GEX image')
    string(name: 'UNUSUAL_WHALES_GEX_HISTORY_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-unusual-whales-gex-history:dev', description: 'Unusual Whales GEX history image')
    string(name: 'RAW_POSTGRES_WRITER_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-raw-postgres-writer:dev', description: 'Raw Postgres writer image')
    string(name: 'PRESSURE_POSTGRES_WRITER_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-pressure-postgres-writer:dev', description: 'Pressure Postgres writer image')
    string(name: 'FEED_GATEWAY_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-feed-gateway:dev', description: 'Feed gateway image')
    string(name: 'INTEGRATION_TEST_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-integration-test:dev', description: 'Integration-test image')
    string(name: 'HPSF_PROCESSING_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-hpsf-processing:dev', description: 'HPSF Stage A/B processing image')
    string(name: 'HPSF_POSTGRES_WRITER_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-hpsf-postgres-writer:dev', description: 'HPSF Postgres writer image')
    string(name: 'STRIKE_FLOW_CLASSIFIER_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-strike-flow-classifier:dev', description: 'Strike flow classifier image')
    string(name: 'IBKR_FEED_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-ibkr-feed:dev', description: 'IBKR feed image')
    string(name: 'UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID', defaultValue: 'options-edge-unusual-whales-api-key', description: 'Jenkins secret-text credential containing the Unusual Whales API key')
    choice(name: 'MARKET_DATA_SOURCE', choices: ['DATABENTO', 'IBKR'], description: 'Runtime raw market-data source for processors')
    string(name: 'RAW_TOPIC', defaultValue: '', description: 'Override raw topic. Empty uses source default.')
    string(name: 'IB_HOST', defaultValue: '127.0.0.1', description: 'IB Gateway/TWS host. IBKR feed uses hostNetwork, so localhost is the remote host.')
    string(name: 'IB_PORT', defaultValue: '4001', description: 'IB Gateway/TWS API port')
    string(name: 'IB_CLIENT_ID', defaultValue: '212', description: 'IBKR feed API client id')
    string(name: 'IB_EXPIRY', defaultValue: '20260615', description: 'IBKR option expiry/date')
    string(name: 'IB_MAX_STRIKES', defaultValue: '43', description: 'Max strikes around spot for IBKR feed')
    booleanParam(name: 'KAFKA_CLEANUP_TOPICS', defaultValue: false, description: 'Clean Kafka topics before deployment')
    booleanParam(name: 'KAFKA_DELETE_UNWANTED_TOPICS', defaultValue: false, description: 'Delete non-whitelisted topics')
    booleanParam(name: 'ALLOW_PROD_KAFKA_CLEANUP', defaultValue: false, description: 'Allow destructive Kafka cleanup in production')
  }
  environment {
    ENVIRONMENT = "${params.ENVIRONMENT ?: 'dev'}"
    KUBECONFIG = "${params.KUBECONFIG_FILE ?: '/home/options-edge/config/kubeconfig'}"
    REMOTE_APP_HOME = '/home/options-edge'
    IMAGE_REGISTRY = "${params.IMAGE_REGISTRY ?: '192.168.100.252:5000'}"
    IMAGE_TAG = "${params.IMAGE_TAG ?: ''}"
    RAW_TO_DISPLAY_IMAGE = "${params.RAW_TO_DISPLAY_IMAGE ?: '192.168.100.252:5000/options-edge-raw-to-display:dev'}"
    DATABENTO_VOLUME_AGGREGATOR_IMAGE = "${params.DATABENTO_VOLUME_AGGREGATOR_IMAGE ?: '192.168.100.252:5000/options-edge-databento-volume-aggregator:dev'}"
    VOLUME_PACE_IMAGE = "${params.VOLUME_PACE_IMAGE ?: '192.168.100.252:5000/options-edge-volume-pace:dev'}"
    DIRECTIONAL_PRESSURE_IMAGE = "${params.DIRECTIONAL_PRESSURE_IMAGE ?: '192.168.100.252:5000/options-edge-directional-pressure:dev'}"
    VOLUME_SANDWICH_IMAGE = "${params.VOLUME_SANDWICH_IMAGE ?: '192.168.100.252:5000/options-edge-volume-sandwich:dev'}"
    UNUSUAL_WHALES_GEX_IMAGE = "${params.UNUSUAL_WHALES_GEX_IMAGE ?: '192.168.100.252:5000/options-edge-unusual-whales-gex:dev'}"
    UNUSUAL_WHALES_GEX_HISTORY_IMAGE = "${params.UNUSUAL_WHALES_GEX_HISTORY_IMAGE ?: '192.168.100.252:5000/options-edge-unusual-whales-gex-history:dev'}"
    RAW_POSTGRES_WRITER_IMAGE = "${params.RAW_POSTGRES_WRITER_IMAGE ?: '192.168.100.252:5000/options-edge-raw-postgres-writer:dev'}"
    PRESSURE_POSTGRES_WRITER_IMAGE = "${params.PRESSURE_POSTGRES_WRITER_IMAGE ?: '192.168.100.252:5000/options-edge-pressure-postgres-writer:dev'}"
    FEED_GATEWAY_IMAGE = "${params.FEED_GATEWAY_IMAGE ?: '192.168.100.252:5000/options-edge-feed-gateway:dev'}"
    INTEGRATION_TEST_IMAGE = "${params.INTEGRATION_TEST_IMAGE ?: '192.168.100.252:5000/options-edge-integration-test:dev'}"
    HPSF_PROCESSING_IMAGE = "${params.HPSF_PROCESSING_IMAGE ?: '192.168.100.252:5000/options-edge-hpsf-processing:dev'}"
    HPSF_POSTGRES_WRITER_IMAGE = "${params.HPSF_POSTGRES_WRITER_IMAGE ?: '192.168.100.252:5000/options-edge-hpsf-postgres-writer:dev'}"
    STRIKE_FLOW_CLASSIFIER_IMAGE = "${params.STRIKE_FLOW_CLASSIFIER_IMAGE ?: '192.168.100.252:5000/options-edge-strike-flow-classifier:dev'}"
    IBKR_FEED_IMAGE = "${params.IBKR_FEED_IMAGE ?: '192.168.100.252:5000/options-edge-ibkr-feed:dev'}"
    MARKET_DATA_SOURCE = "${params.MARKET_DATA_SOURCE ?: 'DATABENTO'}"
    RAW_TOPIC = "${params.RAW_TOPIC ?: ''}"
    IB_HOST = "${params.IB_HOST ?: '127.0.0.1'}"
    IB_PORT = "${params.IB_PORT ?: '4001'}"
    IB_CLIENT_ID = "${params.IB_CLIENT_ID ?: '212'}"
    IB_EXPIRY = "${params.IB_EXPIRY ?: '20260615'}"
    IB_MAX_STRIKES = "${params.IB_MAX_STRIKES ?: '43'}"
    KAFKA_CLEANUP_TOPICS = "${params.KAFKA_CLEANUP_TOPICS ?: false}"
    KAFKA_DELETE_UNWANTED_TOPICS = "${params.KAFKA_DELETE_UNWANTED_TOPICS ?: false}"
    ALLOW_PROD_KAFKA_CLEANUP = "${params.ALLOW_PROD_KAFKA_CLEANUP ?: false}"
  }
  stages {
    stage('Validate') {
      steps {
        sh '''
          set -euo pipefail
          scripts/jenkins/enforce-main-branch.sh
          test "$REMOTE_APP_HOME" = "/home/options-edge"
          test ! -d /root/options-edge
          test ! -d /options-edge
          mkdir -p "$REMOTE_APP_HOME/tmp"
        '''
      }
    }
    stage('Render') {
      steps {
        sh 'kubectl kustomize k8s/overlays/${ENVIRONMENT} >"$REMOTE_APP_HOME/tmp/options-edge-${ENVIRONMENT}.yaml"'
      }
    }
    stage('Pause Runtime For Kafka Cleanup') {
      when {
        expression { return params.KAFKA_CLEANUP_TOPICS }
      }
      steps {
        sh '''
          set -euo pipefail
          /home/abhinav/ci/bin/app-control.sh options-edge stop || true
          for i in $(seq 1 30); do
            if ! ss -ltnp 2>/dev/null | grep -q ':8090'; then
              echo "options-edge Tomcat port 8090 is stopped."
              break
            fi
            echo "Waiting for options-edge Tomcat port 8090 to stop."
            sleep 2
          done
          if ss -ltnp 2>/dev/null | grep -q ':8090'; then
            echo "Timed out waiting for options-edge Tomcat port 8090 to stop before Kafka cleanup." >&2
            ss -ltnp 2>/dev/null | grep ':8090' || true
            exit 1
          fi
          kubectl -n options-edge scale deployment --all --replicas=0 || true
          for i in $(seq 1 60); do
            pod_count="$(kubectl -n options-edge get pods --no-headers 2>/dev/null | sed '/^$/d' | wc -l | tr -d ' ')"
            if [ "$pod_count" = "0" ]; then
              echo "All options-edge pods are stopped."
              exit 0
            fi
            echo "Waiting for options-edge pods to stop; remaining=$pod_count"
            kubectl -n options-edge get pods || true
            sleep 3
          done
          echo "Timed out waiting for options-edge pods to stop before Kafka cleanup." >&2
          kubectl -n options-edge get pods || true
          exit 1
        '''
      }
    }
    stage('Kafka Cleanup') {
      when {
        expression { return params.KAFKA_CLEANUP_TOPICS }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_CLEANUP_TOPICS="${KAFKA_CLEANUP_TOPICS}"
          export KAFKA_DELETE_UNWANTED_TOPICS="${KAFKA_DELETE_UNWANTED_TOPICS}"
          export ALLOW_PROD_KAFKA_CLEANUP="${ALLOW_PROD_KAFKA_CLEANUP}"
          export KAFKA_CLEANUP_MODE="${KAFKA_CLEANUP_MODE:-delete-recreate}"
          scripts/kafka/cleanup-topics.sh
        '''
      }
    }
    stage('Kafka Topics') {
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_TOPIC_REPLICATION_FACTOR=1
          export KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1
          export KAFKA_TOPIC_RETENTION_MS=86400000
          export KAFKA_RECREATE_MISMATCHED_TOPICS="${KAFKA_CLEANUP_TOPICS}"
          scripts/kafka/apply-topics.sh
          scripts/kafka/verify-topics.sh
          scripts/kafka/create-hpsf-topics.sh
          scripts/kafka/verify-hpsf-topics.sh
        '''
      }
    }
    stage('Reset HPSF Stage B Internal Topics') {
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export HPSF_STAGE_B_STREAMS_APPLICATION_ID="${HPSF_STAGE_B_STREAMS_APPLICATION_ID:-options-edge-hpsf-stage-b-v2-1}"

          kubectl -n options-edge scale deployment/hpsf-stage-b-service --replicas=0 || true
          for i in $(seq 1 60); do
            pod_count="$(kubectl -n options-edge get pods -l app.kubernetes.io/name=hpsf-stage-b-service --no-headers 2>/dev/null | sed '/^$/d' | wc -l | tr -d ' ')"
            if [ "$pod_count" = "0" ]; then
              echo "hpsf-stage-b-service pods are stopped."
              break
            fi
            echo "Waiting for hpsf-stage-b-service pods to stop; remaining=$pod_count"
            kubectl -n options-edge get pods -l app.kubernetes.io/name=hpsf-stage-b-service || true
            sleep 2
          done
          if [ "$pod_count" != "0" ]; then
            echo "Timed out waiting for hpsf-stage-b-service pods to stop before internal topic reset." >&2
            kubectl -n options-edge get pods -l app.kubernetes.io/name=hpsf-stage-b-service || true
            exit 1
          fi

          scripts/kafka/reset-hpsf-stage-b-internal-topics.sh
        '''
      }
    }
    stage('Resume Remote Apps') {
      when {
        expression { return params.KAFKA_CLEANUP_TOPICS }
      }
      steps {
        sh '''
          set -euo pipefail
          kubectl -n options-edge scale deployment/options-edge-databento-feed --replicas=1 || true
          kubectl -n options-edge rollout status deployment/options-edge-databento-feed --timeout=240s || true
          /home/abhinav/ci/bin/app-control.sh options-edge start
        '''
      }
    }
    stage('Unusual Whales Secret') {
      steps {
        withCredentials([string(credentialsId: params.UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID, variable: 'UNUSUAL_WHALES_API_KEY')]) {
          sh '''
            set -euo pipefail
            test -n "$UNUSUAL_WHALES_API_KEY"
            kubectl create namespace options-edge --dry-run=client -o yaml | kubectl apply -f -
            kubectl -n options-edge create secret generic options-edge-secrets \
              --from-literal=unusual-whales-api-key="$UNUSUAL_WHALES_API_KEY" \
              --dry-run=client -o yaml | kubectl apply -f -
          '''
        }
      }
    }
    stage('Resolve Images') {
      steps {
        sh '''
          set -euo pipefail
          mkdir -p "$REMOTE_APP_HOME/tmp"
          image_tag="${IMAGE_TAG:-}"
          registry="${IMAGE_REGISTRY:-192.168.100.252:5000}"
          if [ -n "$image_tag" ]; then
            cat >"$REMOTE_APP_HOME/tmp/options-edge-images.env" <<EOF
RAW_TO_DISPLAY_IMAGE=$registry/options-edge-raw-to-display:$image_tag
DATABENTO_VOLUME_AGGREGATOR_IMAGE=$registry/options-edge-databento-volume-aggregator:$image_tag
VOLUME_PACE_IMAGE=$registry/options-edge-volume-pace:$image_tag
DIRECTIONAL_PRESSURE_IMAGE=$registry/options-edge-directional-pressure:$image_tag
VOLUME_SANDWICH_IMAGE=$registry/options-edge-volume-sandwich:$image_tag
UNUSUAL_WHALES_GEX_IMAGE=$registry/options-edge-unusual-whales-gex:$image_tag
UNUSUAL_WHALES_GEX_HISTORY_IMAGE=$registry/options-edge-unusual-whales-gex-history:$image_tag
RAW_POSTGRES_WRITER_IMAGE=$registry/options-edge-raw-postgres-writer:$image_tag
PRESSURE_POSTGRES_WRITER_IMAGE=$registry/options-edge-pressure-postgres-writer:$image_tag
FEED_GATEWAY_IMAGE=$registry/options-edge-feed-gateway:$image_tag
INTEGRATION_TEST_IMAGE=$registry/options-edge-integration-test:$image_tag
HPSF_PROCESSING_IMAGE=$registry/options-edge-hpsf-processing:$image_tag
HPSF_POSTGRES_WRITER_IMAGE=$registry/options-edge-hpsf-postgres-writer:$image_tag
STRIKE_FLOW_CLASSIFIER_IMAGE=$registry/options-edge-strike-flow-classifier:$image_tag
IBKR_FEED_IMAGE=$registry/options-edge-ibkr-feed:$image_tag
EOF
          else
            cat >"$REMOTE_APP_HOME/tmp/options-edge-images.env" <<EOF
RAW_TO_DISPLAY_IMAGE=$RAW_TO_DISPLAY_IMAGE
DATABENTO_VOLUME_AGGREGATOR_IMAGE=$DATABENTO_VOLUME_AGGREGATOR_IMAGE
VOLUME_PACE_IMAGE=$VOLUME_PACE_IMAGE
DIRECTIONAL_PRESSURE_IMAGE=$DIRECTIONAL_PRESSURE_IMAGE
VOLUME_SANDWICH_IMAGE=$VOLUME_SANDWICH_IMAGE
UNUSUAL_WHALES_GEX_IMAGE=$UNUSUAL_WHALES_GEX_IMAGE
UNUSUAL_WHALES_GEX_HISTORY_IMAGE=$UNUSUAL_WHALES_GEX_HISTORY_IMAGE
RAW_POSTGRES_WRITER_IMAGE=$RAW_POSTGRES_WRITER_IMAGE
PRESSURE_POSTGRES_WRITER_IMAGE=$PRESSURE_POSTGRES_WRITER_IMAGE
FEED_GATEWAY_IMAGE=$FEED_GATEWAY_IMAGE
INTEGRATION_TEST_IMAGE=$INTEGRATION_TEST_IMAGE
HPSF_PROCESSING_IMAGE=$HPSF_PROCESSING_IMAGE
HPSF_POSTGRES_WRITER_IMAGE=$HPSF_POSTGRES_WRITER_IMAGE
STRIKE_FLOW_CLASSIFIER_IMAGE=$STRIKE_FLOW_CLASSIFIER_IMAGE
IBKR_FEED_IMAGE=$IBKR_FEED_IMAGE
EOF
          fi
          echo "Resolved deployment images:"
          sed 's/^/  /' "$REMOTE_APP_HOME/tmp/options-edge-images.env"
        '''
      }
    }
    stage('Image Preflight') {
      steps {
        sh '''
          set -euo pipefail
          . "$REMOTE_APP_HOME/tmp/options-edge-images.env"
          images="
            RAW_TO_DISPLAY_IMAGE=$RAW_TO_DISPLAY_IMAGE
            DATABENTO_VOLUME_AGGREGATOR_IMAGE=$DATABENTO_VOLUME_AGGREGATOR_IMAGE
            VOLUME_PACE_IMAGE=$VOLUME_PACE_IMAGE
            DIRECTIONAL_PRESSURE_IMAGE=$DIRECTIONAL_PRESSURE_IMAGE
            VOLUME_SANDWICH_IMAGE=$VOLUME_SANDWICH_IMAGE
            UNUSUAL_WHALES_GEX_IMAGE=$UNUSUAL_WHALES_GEX_IMAGE
            UNUSUAL_WHALES_GEX_HISTORY_IMAGE=$UNUSUAL_WHALES_GEX_HISTORY_IMAGE
            RAW_POSTGRES_WRITER_IMAGE=$RAW_POSTGRES_WRITER_IMAGE
            PRESSURE_POSTGRES_WRITER_IMAGE=$PRESSURE_POSTGRES_WRITER_IMAGE
            FEED_GATEWAY_IMAGE=$FEED_GATEWAY_IMAGE
            INTEGRATION_TEST_IMAGE=$INTEGRATION_TEST_IMAGE
            HPSF_PROCESSING_IMAGE=$HPSF_PROCESSING_IMAGE
            HPSF_POSTGRES_WRITER_IMAGE=$HPSF_POSTGRES_WRITER_IMAGE
            STRIKE_FLOW_CLASSIFIER_IMAGE=$STRIKE_FLOW_CLASSIFIER_IMAGE
            IBKR_FEED_IMAGE=$IBKR_FEED_IMAGE
          "

          image_exists() {
            local image="$1"
            local registry remainder repository tag
            registry="${image%%/*}"
            remainder="${image#*/}"
            if [ "$registry" = "$image" ] || [ "$remainder" = "$image" ] || [[ "$remainder" != *:* ]]; then
              docker pull "$image" >/dev/null 2>&1
              return $?
            fi

            repository="${remainder%:*}"
            tag="${remainder##*:}"
            for scheme in http https; do
              if curl -fsSI \
                -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
                -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
                "$scheme://$registry/v2/$repository/manifests/$tag" >/dev/null 2>&1; then
                return 0
              fi
            done

            docker pull "$image" >/dev/null 2>&1
          }

          missing=0
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
            fi
          done <<EOF
$images
EOF

          if [ "$missing" != "0" ]; then
            echo "One or more requested images are missing; refusing to restart pods." >&2
            exit 1
          fi
        '''
      }
    }
    stage('Deploy') {
      steps {
        sh '''
          set -euo pipefail
          . "$REMOTE_APP_HOME/tmp/options-edge-images.env"
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
          python3 - "$market_data_source" "$effective_raw_topic" "${IB_HOST:-127.0.0.1}" "${IB_PORT:-4001}" "${IB_CLIENT_ID:-212}" "${IB_EXPIRY:-20260615}" "${IB_MAX_STRIKES:-43}" "${IB_EXPIRY:-20260615}" >"$REMOTE_APP_HOME/tmp/options-edge-runtime-config-patch.json" <<'PY'
import json
import sys

keys = [
    "APP_MARKET_DATA_SOURCE",
    "KAFKA_RAW_TOPIC",
    "IB_HOST",
    "IB_PORT",
            "IB_CLIENT_ID",
            "IB_EXPIRY",
            "IB_MAX_STRIKES",
            "UNUSUAL_WHALES_EXPIRY",
]
print(json.dumps({"data": dict(zip(keys, sys.argv[1:]))}))
PY
          kubectl -n options-edge patch configmap options-edge-config \
            --type merge \
            --patch "$(cat "$REMOTE_APP_HOME/tmp/options-edge-runtime-config-patch.json")"
          kubectl -n options-edge set image deployment/raw-to-display-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/raw-to-display-databento-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/databento-volume-aggregator databento-volume-aggregator="$DATABENTO_VOLUME_AGGREGATOR_IMAGE"
          kubectl -n options-edge set image deployment/volume-pace-service volume-pace="$VOLUME_PACE_IMAGE"
          kubectl -n options-edge set image deployment/volume-pace-databento-service volume-pace="$VOLUME_PACE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-databento-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/volume-sandwich-service volume-sandwich="$VOLUME_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/volume-sandwich-databento-service volume-sandwich="$VOLUME_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/unusual-whales-gex-service unusual-whales-gex="$UNUSUAL_WHALES_GEX_IMAGE"
          kubectl -n options-edge set image deployment/unusual-whales-gex-history-service unusual-whales-gex-history="$UNUSUAL_WHALES_GEX_HISTORY_IMAGE"
          kubectl -n options-edge set image deployment/raw-postgres-writer raw-postgres-writer="$RAW_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/pressure-postgres-writer pressure-postgres-writer="$PRESSURE_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/feed-gateway-service feed-gateway="$FEED_GATEWAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-integration-test integration-test="$INTEGRATION_TEST_IMAGE"
          kubectl -n options-edge set image deployment/hpsf-stage-a-service hpsf-stage-a="$HPSF_PROCESSING_IMAGE"
          kubectl -n options-edge set image deployment/hpsf-stage-b-service hpsf-stage-b="$HPSF_PROCESSING_IMAGE"
          kubectl -n options-edge set image deployment/hpsf-postgres-writer-service hpsf-postgres-writer="$HPSF_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/strike-flow-classifier-service strike-flow-classifier="$STRIKE_FLOW_CLASSIFIER_IMAGE"
          kubectl -n options-edge set image deployment/ibkr-feed-service ibkr-feed="$IBKR_FEED_IMAGE"
          kubectl -n options-edge rollout restart deployment/raw-to-display-service
          kubectl -n options-edge rollout restart deployment/raw-to-display-databento-service
          kubectl -n options-edge rollout restart deployment/databento-volume-aggregator
          kubectl -n options-edge rollout restart deployment/volume-pace-service
          kubectl -n options-edge rollout restart deployment/volume-pace-databento-service
          kubectl -n options-edge rollout restart deployment/directional-pressure-service
          kubectl -n options-edge rollout restart deployment/directional-pressure-databento-service
          kubectl -n options-edge rollout restart deployment/volume-sandwich-service
          kubectl -n options-edge rollout restart deployment/volume-sandwich-databento-service
          kubectl -n options-edge rollout restart deployment/unusual-whales-gex-service
          kubectl -n options-edge rollout restart deployment/unusual-whales-gex-history-service
          kubectl -n options-edge rollout restart deployment/raw-postgres-writer
          kubectl -n options-edge rollout restart deployment/pressure-postgres-writer
          kubectl -n options-edge rollout restart deployment/feed-gateway-service
          kubectl -n options-edge rollout restart deployment/options-edge-integration-test
          kubectl -n options-edge rollout restart deployment/hpsf-stage-a-service
          kubectl -n options-edge rollout restart deployment/hpsf-stage-b-service
          kubectl -n options-edge rollout restart deployment/hpsf-postgres-writer-service
          kubectl -n options-edge rollout restart deployment/strike-flow-classifier-service
          kubectl -n options-edge rollout restart deployment/ibkr-feed-service
          kubectl -n options-edge rollout status deployment/raw-to-display-service --timeout=180s
          kubectl -n options-edge rollout status deployment/raw-to-display-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/databento-volume-aggregator --timeout=240s
          kubectl -n options-edge rollout status deployment/volume-pace-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-pace-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/directional-pressure-service --timeout=180s
          kubectl -n options-edge rollout status deployment/directional-pressure-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-sandwich-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-sandwich-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/unusual-whales-gex-service --timeout=180s
          kubectl -n options-edge rollout status deployment/unusual-whales-gex-history-service --timeout=180s
          kubectl -n options-edge rollout status deployment/raw-postgres-writer --timeout=900s
          kubectl -n options-edge rollout status deployment/pressure-postgres-writer --timeout=180s
          kubectl -n options-edge rollout status deployment/feed-gateway-service --timeout=180s
          kubectl -n options-edge rollout status deployment/options-edge-integration-test --timeout=180s
          kubectl -n options-edge rollout status deployment/hpsf-stage-a-service --timeout=240s
          kubectl -n options-edge rollout status deployment/hpsf-stage-b-service --timeout=240s
          kubectl -n options-edge rollout status deployment/hpsf-postgres-writer-service --timeout=180s
          kubectl -n options-edge rollout status deployment/strike-flow-classifier-service --timeout=180s
          kubectl -n options-edge rollout status deployment/ibkr-feed-service --timeout=240s
        '''
      }
    }
    stage('Kafka Internal Topics') {
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1
          export KAFKA_TOPIC_RETENTION_MS=86400000
          export KAFKA_STREAMS_INTERNAL_RETENTION_MS="${KAFKA_STREAMS_INTERNAL_RETENTION_MS:-86400000}"
          export KAFKA_STREAMS_INTERNAL_SEGMENT_MS="${KAFKA_STREAMS_INTERNAL_SEGMENT_MS:-3600000}"
          export KAFKA_CHANGELOG_RETENTION_MS="${KAFKA_CHANGELOG_RETENTION_MS:-86400000}"
          export KAFKA_CHANGELOG_DELETE_RETENTION_MS="${KAFKA_CHANGELOG_DELETE_RETENTION_MS:-3600000}"
          export KAFKA_CHANGELOG_MIN_CLEANABLE_DIRTY_RATIO="${KAFKA_CHANGELOG_MIN_CLEANABLE_DIRTY_RATIO:-0.01}"
          scripts/kafka/apply-internal-topic-configs.sh
        '''
      }
    }
    stage('Prometheus Scrapes') {
      steps {
        withCredentials([string(credentialsId: 'options-edge-remote-become-password', variable: 'BECOME_PASSWORD')]) {
          sh '''
            set -euo pipefail
            export NAMESPACE=options-edge
            export KUBECONFIG="${KUBECONFIG}"
            export REMOTE_APP_HOME="${REMOTE_APP_HOME}"
            scripts/monitoring/apply-prometheus-scrapes.sh
          '''
        }
      }
    }
    stage('Smoke') {
      steps {
        sh 'scripts/smoke/check-k8s-services.sh'
      }
    }
    stage('HPSF Smoke') {
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_TOPIC_REPLICATION_FACTOR=1
          export KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1
          export KUBECONFIG="${KUBECONFIG}"
          export NAMESPACE=options-edge
          export REQUIRE_LATEST_SIGNAL=false
          restore_stage_b_release_runtime() {
            kubectl -n "$NAMESPACE" set env deployment/hpsf-stage-b-service \
              HPSF_STREAMS_APPLICATION_ID=options-edge-hpsf-stage-b-v2-1 \
              HPSF_STAGE_B_EVALUATION_MODE- \
              HPSF_STAGE_B_PUNCTUATION_TYPE- \
              HPSF_ALLOW_DEBUG_EVALUATION_IN_LIVE- || true
            kubectl -n "$NAMESPACE" rollout restart deployment/hpsf-stage-b-service || true
            kubectl -n "$NAMESPACE" rollout status deployment/hpsf-stage-b-service --timeout=240s || true
          }
          trap 'rc=$?; restore_stage_b_release_runtime; exit $rc' EXIT
          scripts/smoke/check-hpsf-deployment.sh
          stage_b_smoke_app_id="options-edge-hpsf-stage-b-v2-1-smoke-${BUILD_NUMBER:-manual}"
          echo "Using isolated Stage B Kafka Streams application id for deploy smoke: ${stage_b_smoke_app_id}"
          kubectl -n "$NAMESPACE" set env deployment/hpsf-stage-b-service \
            HPSF_STREAMS_APPLICATION_ID="${stage_b_smoke_app_id}" \
            HPSF_STAGE_B_EVALUATION_MODE=SCHEDULED \
            HPSF_STAGE_B_PUNCTUATION_TYPE=WALL_CLOCK_TIME
          kubectl -n "$NAMESPACE" rollout restart deployment/hpsf-stage-b-service
          kubectl -n "$NAMESPACE" rollout status deployment/hpsf-stage-b-service --timeout=240s
          scripts/smoke/check-hpsf-stage-b-runtime.sh
        '''
      }
    }
  }
}
