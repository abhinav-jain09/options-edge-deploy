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
          kubectl -n options-edge set image deployment/raw-to-display-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/volume-pace-service volume-pace="$VOLUME_PACE_IMAGE"
          kubectl -n options-edge set image deployment/directional-pressure-service directional-pressure="$DIRECTIONAL_PRESSURE_IMAGE"
          kubectl -n options-edge set image deployment/volume-sandwich-service volume-sandwich="$VOLUME_SANDWICH_IMAGE"
          kubectl -n options-edge set image deployment/raw-postgres-writer raw-postgres-writer="$RAW_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/pressure-postgres-writer pressure-postgres-writer="$PRESSURE_POSTGRES_WRITER_IMAGE"
          kubectl -n options-edge set image deployment/feed-gateway-service feed-gateway="$FEED_GATEWAY_IMAGE"
          kubectl -n options-edge set image deployment/options-edge-integration-test integration-test="$INTEGRATION_TEST_IMAGE"
          kubectl -n options-edge rollout status deployment/raw-to-display-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-pace-service --timeout=180s
          kubectl -n options-edge rollout status deployment/directional-pressure-service --timeout=180s
          kubectl -n options-edge rollout status deployment/volume-sandwich-service --timeout=180s
          kubectl -n options-edge rollout status deployment/raw-postgres-writer --timeout=180s
          kubectl -n options-edge rollout status deployment/pressure-postgres-writer --timeout=180s
          kubectl -n options-edge rollout status deployment/feed-gateway-service --timeout=180s
          kubectl -n options-edge rollout status deployment/options-edge-integration-test --timeout=180s
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
