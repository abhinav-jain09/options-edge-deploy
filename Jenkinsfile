pipeline {
  agent any
  parameters {
    choice(name: 'ENVIRONMENT', choices: ['dev', 'staging', 'production'], description: 'Target environment')
    string(name: 'KUBECONFIG_FILE', defaultValue: '/home/options-edge/config/kubeconfig', description: 'Kubeconfig path on Jenkins agent')
    string(name: 'RAW_TO_DISPLAY_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-raw-to-display:dev', description: 'Raw-to-display image')
    string(name: 'VOLUME_PACE_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-volume-pace:dev', description: 'Volume-pace image')
    string(name: 'DIRECTIONAL_PRESSURE_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-directional-pressure:dev', description: 'Directional-pressure image')
    string(name: 'VOLUME_SANDWICH_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-volume-sandwich:dev', description: 'Volume-sandwich image')
    string(name: 'RAW_POSTGRES_WRITER_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-raw-postgres-writer:dev', description: 'Raw Postgres writer image')
    string(name: 'PRESSURE_POSTGRES_WRITER_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-pressure-postgres-writer:dev', description: 'Pressure Postgres writer image')
    string(name: 'FEED_GATEWAY_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-feed-gateway:dev', description: 'Feed gateway image')
    string(name: 'INTEGRATION_TEST_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-integration-test:dev', description: 'Integration-test image')
    string(name: 'IBKR_FEED_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-ibkr-feed:dev', description: 'IBKR feed image')
    choice(name: 'MARKET_DATA_SOURCE', choices: ['DATABENTO', 'IBKR'], description: 'Runtime raw market-data source for processors')
    string(name: 'RAW_TOPIC', defaultValue: '', description: 'Override raw topic. Empty uses source default.')
    string(name: 'IB_HOST', defaultValue: '192.168.100.252', description: 'IB Gateway/TWS host reachable from Kubernetes pods')
    string(name: 'IB_PORT', defaultValue: '4001', description: 'IB Gateway/TWS API port')
    string(name: 'IB_CLIENT_ID', defaultValue: '212', description: 'IBKR feed API client id')
    string(name: 'IB_EXPIRY', defaultValue: '20260611', description: 'IBKR option expiry/date')
    string(name: 'IB_MAX_STRIKES', defaultValue: '60', description: 'Max strikes around spot for IBKR feed')
    booleanParam(name: 'KAFKA_CLEANUP_TOPICS', defaultValue: false, description: 'Clean Kafka topics before deployment')
    booleanParam(name: 'KAFKA_DELETE_UNWANTED_TOPICS', defaultValue: false, description: 'Delete non-whitelisted topics')
    booleanParam(name: 'ALLOW_PROD_KAFKA_CLEANUP', defaultValue: false, description: 'Allow destructive Kafka cleanup in production')
  }
  environment {
    ENVIRONMENT = "${params.ENVIRONMENT ?: 'dev'}"
    KUBECONFIG = "${params.KUBECONFIG_FILE ?: '/home/options-edge/config/kubeconfig'}"
    REMOTE_APP_HOME = '/home/options-edge'
    RAW_TO_DISPLAY_IMAGE = "${params.RAW_TO_DISPLAY_IMAGE ?: '192.168.100.252:5000/options-edge-raw-to-display:dev'}"
    VOLUME_PACE_IMAGE = "${params.VOLUME_PACE_IMAGE ?: '192.168.100.252:5000/options-edge-volume-pace:dev'}"
    DIRECTIONAL_PRESSURE_IMAGE = "${params.DIRECTIONAL_PRESSURE_IMAGE ?: '192.168.100.252:5000/options-edge-directional-pressure:dev'}"
    VOLUME_SANDWICH_IMAGE = "${params.VOLUME_SANDWICH_IMAGE ?: '192.168.100.252:5000/options-edge-volume-sandwich:dev'}"
    RAW_POSTGRES_WRITER_IMAGE = "${params.RAW_POSTGRES_WRITER_IMAGE ?: '192.168.100.252:5000/options-edge-raw-postgres-writer:dev'}"
    PRESSURE_POSTGRES_WRITER_IMAGE = "${params.PRESSURE_POSTGRES_WRITER_IMAGE ?: '192.168.100.252:5000/options-edge-pressure-postgres-writer:dev'}"
    FEED_GATEWAY_IMAGE = "${params.FEED_GATEWAY_IMAGE ?: '192.168.100.252:5000/options-edge-feed-gateway:dev'}"
    INTEGRATION_TEST_IMAGE = "${params.INTEGRATION_TEST_IMAGE ?: '192.168.100.252:5000/options-edge-integration-test:dev'}"
    IBKR_FEED_IMAGE = "${params.IBKR_FEED_IMAGE ?: '192.168.100.252:5000/options-edge-ibkr-feed:dev'}"
    MARKET_DATA_SOURCE = "${params.MARKET_DATA_SOURCE ?: 'DATABENTO'}"
    RAW_TOPIC = "${params.RAW_TOPIC ?: ''}"
    IB_HOST = "${params.IB_HOST ?: '192.168.100.252'}"
    IB_PORT = "${params.IB_PORT ?: '4001'}"
    IB_CLIENT_ID = "${params.IB_CLIENT_ID ?: '212'}"
    IB_EXPIRY = "${params.IB_EXPIRY ?: '20260611'}"
    IB_MAX_STRIKES = "${params.IB_MAX_STRIKES ?: '60'}"
    KAFKA_CLEANUP_TOPICS = "${params.KAFKA_CLEANUP_TOPICS ?: false}"
    KAFKA_DELETE_UNWANTED_TOPICS = "${params.KAFKA_DELETE_UNWANTED_TOPICS ?: false}"
    ALLOW_PROD_KAFKA_CLEANUP = "${params.ALLOW_PROD_KAFKA_CLEANUP ?: false}"
  }
  stages {
    stage('Validate') {
      steps {
        sh '''
          set -euo pipefail
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
          /home/abhinav/ci/bin/app-control.sh databento-feed stop || true
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
          /home/abhinav/ci/bin/app-control.sh databento-feed start
          /home/abhinav/ci/bin/app-control.sh options-edge start
        '''
      }
    }
    stage('Deploy') {
      steps {
        sh '''
          set -euo pipefail
          kubectl apply -k "k8s/overlays/${ENVIRONMENT}"
          effective_raw_topic="$RAW_TOPIC"
          if [ -z "$effective_raw_topic" ]; then
            if [ "$MARKET_DATA_SOURCE" = "IBKR" ]; then
              effective_raw_topic="options.ibkr.raw"
            else
              effective_raw_topic="options.databento.raw"
            fi
          fi
          python3 - "$MARKET_DATA_SOURCE" "$effective_raw_topic" "$IB_HOST" "$IB_PORT" "$IB_CLIENT_ID" "$IB_EXPIRY" "$IB_MAX_STRIKES" >"$REMOTE_APP_HOME/tmp/options-edge-runtime-config-patch.json" <<'PY'
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
]
print(json.dumps({"data": dict(zip(keys, sys.argv[1:]))}))
PY
          kubectl -n options-edge patch configmap options-edge-config \
            --type merge \
            --patch "$(cat "$REMOTE_APP_HOME/tmp/options-edge-runtime-config-patch.json")"
          kubectl -n options-edge set image deployment/raw-to-display-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/volume-pace-service volume-pace="$VOLUME_PACE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/volume-sandwich-service volume-sandwich="$VOLUME_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/raw-postgres-writer raw-postgres-writer="$RAW_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/pressure-postgres-writer pressure-postgres-writer="$PRESSURE_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/feed-gateway-service feed-gateway="$FEED_GATEWAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-integration-test integration-test="$INTEGRATION_TEST_IMAGE"
          kubectl -n options-edge set image deployment/ibkr-feed-service ibkr-feed="$IBKR_FEED_IMAGE"
          kubectl -n options-edge rollout restart deployment/raw-to-display-service
          kubectl -n options-edge rollout restart deployment/volume-pace-service
          kubectl -n options-edge rollout restart deployment/directional-pressure-service
          kubectl -n options-edge rollout restart deployment/volume-sandwich-service
          kubectl -n options-edge rollout restart deployment/raw-postgres-writer
          kubectl -n options-edge rollout restart deployment/pressure-postgres-writer
          kubectl -n options-edge rollout restart deployment/feed-gateway-service
          kubectl -n options-edge rollout restart deployment/options-edge-integration-test
          kubectl -n options-edge rollout restart deployment/ibkr-feed-service
          kubectl -n options-edge rollout status deployment/raw-to-display-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-pace-service --timeout=180s
          kubectl -n options-edge rollout status deployment/directional-pressure-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-sandwich-service --timeout=180s
          kubectl -n options-edge rollout status deployment/raw-postgres-writer --timeout=180s
          kubectl -n options-edge rollout status deployment/pressure-postgres-writer --timeout=180s
          kubectl -n options-edge rollout status deployment/feed-gateway-service --timeout=180s
          kubectl -n options-edge rollout status deployment/options-edge-integration-test --timeout=180s
          kubectl -n options-edge rollout status deployment/ibkr-feed-service --timeout=240s
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
  }
}
