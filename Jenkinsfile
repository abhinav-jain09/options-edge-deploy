pipeline {
  agent { label 'built-in' }
  parameters {
    choice(name: 'ENVIRONMENT', choices: ['dev', 'staging', 'production'], description: 'Target environment')
    string(name: 'KUBECONFIG_FILE', defaultValue: '/home/options-edge/config/jenkins-deployer.kubeconfig', description: 'Jenkins deployer kubeconfig path on Jenkins agent')
    string(name: 'KUBECONFIG_ADMIN_FILE', defaultValue: '/home/options-edge/config/kubeconfig', description: 'Admin kubeconfig used only to bootstrap the Jenkins-only Kubernetes deploy guard')
    string(name: 'IMAGE_REGISTRY', defaultValue: '', description: 'Docker registry namespace used when IMAGE_TAG is set. Empty uses host.docker.internal:5001 for dev and 192.168.100.252:5000 for staging/production.')
    string(name: 'IMAGE_TAG', defaultValue: '', description: 'Exact Docker tag to use for all runtime images. Empty keeps per-image parameters.')
    string(name: 'BUILD_PLATFORM', defaultValue: '', description: 'Image platform. Empty defaults to linux/arm64 for dev and linux/amd64 for staging/production; staging/production always deploy linux/amd64.')
    string(name: 'KAFKA_BOOTSTRAP_SERVERS', defaultValue: '', description: 'Kafka bootstrap servers. Empty uses remote Kafka at 192.168.100.252:9092,9094,9096.')
    string(name: 'WEB_PUBLIC_URL', defaultValue: '', description: 'Public OptionsEdge web URL for smoke checks. Empty uses http://192.168.100.252:8090.')
    string(name: 'RAW_TO_DISPLAY_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-raw-to-display:dev', description: 'Raw-to-display image')
    string(name: 'DATABENTO_VOLUME_AGGREGATOR_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-databento-volume-aggregator:dev', description: 'Databento volume aggregator image')
    string(name: 'DATABENTO_MISSION_PACE_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-databento-mission-pace:dev', description: 'Databento mission pace image')
    string(name: 'DATABENTO_MISSION_PRESSURE_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-databento-mission-pressure:dev', description: 'Databento mission pressure image')
    string(name: 'DATABENTO_MISSION_SANDWICH_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-databento-mission-sandwich:dev', description: 'Databento mission sandwich image')
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
    string(name: 'SPX_MISSION_CONTROL_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-spx-mission-control:dev', description: 'SPX mission control image')
    string(name: 'STRIKE_FLOW_CLASSIFIER_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-strike-flow-classifier:dev', description: 'Strike flow classifier image')
    string(name: 'IBKR_FEED_IMAGE', defaultValue: '192.168.100.252:5000/options-edge-ibkr-feed:dev', description: 'IBKR feed image')
    string(name: 'UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID', defaultValue: 'options-edge-unusual-whales-api-key', description: 'Jenkins secret-text credential containing the Unusual Whales API key')
    choice(name: 'MARKET_DATA_SOURCE', choices: ['DATABENTO', 'IBKR'], description: 'Runtime raw market-data source for processors')
    string(name: 'RAW_TOPIC', defaultValue: '', description: 'Override raw topic. Empty uses source default.')
    string(name: 'IB_HOST', defaultValue: '127.0.0.1', description: 'IB Gateway/TWS host. IBKR feed uses hostNetwork, so localhost is the remote host.')
    string(name: 'IB_PORT', defaultValue: '4001', description: 'IB Gateway/TWS API port')
    string(name: 'IB_CLIENT_ID', defaultValue: '212', description: 'IBKR feed API client id')
    string(name: 'IB_EXPIRY', defaultValue: '', description: 'Option expiry/date. Empty uses the current weekday on the Jenkins agent.')
    string(name: 'IB_MAX_STRIKES', defaultValue: '43', description: 'Max strikes around spot for IBKR feed')
    booleanParam(name: 'KAFKA_CLEANUP_TOPICS', defaultValue: false, description: 'Clean Kafka topics before deployment')
    booleanParam(name: 'KAFKA_DELETE_UNWANTED_TOPICS', defaultValue: false, description: 'Delete non-whitelisted topics')
    booleanParam(name: 'ALLOW_PROD_KAFKA_CLEANUP', defaultValue: false, description: 'Allow destructive Kafka cleanup in production')
    booleanParam(name: 'SKIP_PRODUCTION_PROMOTION', defaultValue: false, description: 'Internal guard used by the manual production promotion build')
    booleanParam(name: 'DEPLOY_DRY_RUN', defaultValue: false, description: 'Validate render, image preflight, and server-side Kubernetes apply without mutating runtime resources.')
  }
  environment {
    ENVIRONMENT = "${params.ENVIRONMENT ?: 'dev'}"
    KUBECONFIG_FILE = "${(!params.KUBECONFIG_FILE || params.KUBECONFIG_FILE == '/var/jenkins_home/config/jenkins-deployer.kubeconfig') ? '/home/options-edge/config/jenkins-deployer.kubeconfig' : params.KUBECONFIG_FILE}"
    KUBECONFIG = "${(!params.KUBECONFIG_FILE || params.KUBECONFIG_FILE == '/var/jenkins_home/config/jenkins-deployer.kubeconfig') ? '/home/options-edge/config/jenkins-deployer.kubeconfig' : params.KUBECONFIG_FILE}"
    KUBECONFIG_ADMIN_FILE = "${(!params.KUBECONFIG_ADMIN_FILE || params.KUBECONFIG_ADMIN_FILE == '/var/jenkins_home/config/kubeconfig') ? '/home/options-edge/config/kubeconfig' : params.KUBECONFIG_ADMIN_FILE}"
    REMOTE_APP_HOME = '/home/options-edge'
    JENKINS_WORK_DIR = '.jenkins-tmp'
    PATH = "/var/jenkins_home/bin:${env.PATH}"
    IMAGE_REGISTRY = "${params.IMAGE_REGISTRY ?: ''}"
    IMAGE_TAG = "${params.IMAGE_TAG ?: ''}"
    BUILD_PLATFORM = "${params.BUILD_PLATFORM ?: ''}"
    KAFKA_BOOTSTRAP_SERVERS = "${params.KAFKA_BOOTSTRAP_SERVERS ?: ''}"
    WEB_PUBLIC_URL = "${params.WEB_PUBLIC_URL ?: ''}"
    RAW_TO_DISPLAY_IMAGE = "${params.RAW_TO_DISPLAY_IMAGE ?: '192.168.100.252:5000/options-edge-raw-to-display:dev'}"
    DATABENTO_VOLUME_AGGREGATOR_IMAGE = "${params.DATABENTO_VOLUME_AGGREGATOR_IMAGE ?: '192.168.100.252:5000/options-edge-databento-volume-aggregator:dev'}"
    DATABENTO_MISSION_PACE_IMAGE = "${params.DATABENTO_MISSION_PACE_IMAGE ?: '192.168.100.252:5000/options-edge-databento-mission-pace:dev'}"
    DATABENTO_MISSION_PRESSURE_IMAGE = "${params.DATABENTO_MISSION_PRESSURE_IMAGE ?: '192.168.100.252:5000/options-edge-databento-mission-pressure:dev'}"
    DATABENTO_MISSION_SANDWICH_IMAGE = "${params.DATABENTO_MISSION_SANDWICH_IMAGE ?: '192.168.100.252:5000/options-edge-databento-mission-sandwich:dev'}"
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
    SPX_MISSION_CONTROL_IMAGE = "${params.SPX_MISSION_CONTROL_IMAGE ?: '192.168.100.252:5000/options-edge-spx-mission-control:dev'}"
    STRIKE_FLOW_CLASSIFIER_IMAGE = "${params.STRIKE_FLOW_CLASSIFIER_IMAGE ?: '192.168.100.252:5000/options-edge-strike-flow-classifier:dev'}"
    IBKR_FEED_IMAGE = "${params.IBKR_FEED_IMAGE ?: '192.168.100.252:5000/options-edge-ibkr-feed:dev'}"
    MARKET_DATA_SOURCE = "${params.MARKET_DATA_SOURCE ?: 'DATABENTO'}"
    RAW_TOPIC = "${params.RAW_TOPIC ?: ''}"
    IB_HOST = "${params.IB_HOST ?: '127.0.0.1'}"
    IB_PORT = "${params.IB_PORT ?: '4001'}"
    IB_CLIENT_ID = "${params.IB_CLIENT_ID ?: '212'}"
    IB_EXPIRY = "${params.IB_EXPIRY ?: ''}"
    IB_MAX_STRIKES = "${params.IB_MAX_STRIKES ?: '43'}"
    KAFKA_CLEANUP_TOPICS = "${params.KAFKA_CLEANUP_TOPICS ?: false}"
    KAFKA_DELETE_UNWANTED_TOPICS = "${params.KAFKA_DELETE_UNWANTED_TOPICS ?: false}"
    ALLOW_PROD_KAFKA_CLEANUP = "${params.ALLOW_PROD_KAFKA_CLEANUP ?: false}"
    SKIP_PRODUCTION_PROMOTION = "${params.SKIP_PRODUCTION_PROMOTION ?: false}"
    DEPLOY_DRY_RUN = "${params.DEPLOY_DRY_RUN ?: false}"
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
          mkdir -p "$JENKINS_WORK_DIR"
          test -w "$JENKINS_WORK_DIR"
          case "${ENVIRONMENT:-dev}" in
            dev)
              effective_build_platform="${BUILD_PLATFORM:-linux/arm64}"
              ;;
            staging|production)
              effective_build_platform="linux/amd64"
              if [ -n "${BUILD_PLATFORM:-}" ] && [ "$BUILD_PLATFORM" != "linux/amd64" ]; then
                echo "BUILD_PLATFORM=$BUILD_PLATFORM is not allowed for ${ENVIRONMENT}; production Kubernetes nodes are CentOS amd64 and require linux/amd64." >&2
                exit 1
              fi
              ;;
            *)
              echo "Unsupported ENVIRONMENT for BUILD_PLATFORM resolution: ${ENVIRONMENT:-}" >&2
              exit 1
              ;;
          esac
          case "$effective_build_platform" in
            linux/arm64|linux/amd64) ;;
            *)
              echo "Unsupported BUILD_PLATFORM: $effective_build_platform" >&2
              exit 1
              ;;
          esac
          printf 'EFFECTIVE_BUILD_PLATFORM=%s\n' "$effective_build_platform" >"$JENKINS_WORK_DIR/options-edge-build.env"
          echo "Effective build/deploy image platform: $effective_build_platform"
        '''
      }
    }
    stage('Bootstrap Jenkins Kubernetes Guard') {
      steps {
        sh '''
          set -euo pipefail
          scripts/jenkins/bootstrap-kubernetes-deploy-guard.sh
        '''
      }
    }
    stage('Deploy to DEV') {
      when {
        expression { return env.ENVIRONMENT == 'dev' }
      }
      steps {
        timeout(time: 30, unit: 'MINUTES') {
          input message: 'Deploy OptionsEdge to DEV?', ok: 'Deploy to dev'
        }
      }
    }
    stage('Deploy to PRODUCTION') {
      when {
        expression { return env.ENVIRONMENT == 'production' && !params.SKIP_PRODUCTION_PROMOTION }
      }
      steps {
        timeout(time: 30, unit: 'MINUTES') {
          input message: 'Deploy OptionsEdge to PRODUCTION?', ok: 'Deploy to production'
        }
      }
    }
    stage('Render') {
      steps {
        sh 'kubectl kustomize k8s/overlays/${ENVIRONMENT} >"$JENKINS_WORK_DIR/options-edge-${ENVIRONMENT}.yaml"'
      }
    }
    stage('Pause Runtime For Kafka Cleanup') {
      when {
        expression { return params.KAFKA_CLEANUP_TOPICS && !params.DEPLOY_DRY_RUN }
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
        expression { return params.KAFKA_CLEANUP_TOPICS && !params.DEPLOY_DRY_RUN }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_BOOTSTRAP_SERVERS
          export KAFKA_CLEANUP_TOPICS="${KAFKA_CLEANUP_TOPICS}"
          export KAFKA_DELETE_UNWANTED_TOPICS="${KAFKA_DELETE_UNWANTED_TOPICS}"
          export ALLOW_PROD_KAFKA_CLEANUP="${ALLOW_PROD_KAFKA_CLEANUP}"
          export KAFKA_CLEANUP_MODE="${KAFKA_CLEANUP_MODE:-delete-recreate}"
          scripts/kafka/cleanup-topics.sh
        '''
      }
    }
    stage('Kafka Topics') {
      when {
        expression { return !params.DEPLOY_DRY_RUN }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_BOOTSTRAP_SERVERS
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
      when {
        expression { return !params.DEPLOY_DRY_RUN }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_BOOTSTRAP_SERVERS
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
        expression { return params.KAFKA_CLEANUP_TOPICS && !params.DEPLOY_DRY_RUN }
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
    stage('Verify OptionsEdge Web App') {
      steps {
        sh '''
          set -euo pipefail
          if [ "${ENVIRONMENT:-dev}" = "dev" ]; then
            WEB_PUBLIC_URL="${WEB_PUBLIC_URL:-http://192.168.100.252:8090}"
            curl -fsS --connect-timeout 5 --max-time 10 "$WEB_PUBLIC_URL/api/config" | grep -q '"provider"'
            curl -fsS --connect-timeout 5 --max-time 10 -o /dev/null "$WEB_PUBLIC_URL/"
            echo "OptionsEdge dev web app is healthy at $WEB_PUBLIC_URL/"
          else
            scripts/smoke/check-options-edge-web.sh
          fi
        '''
      }
    }
    stage('Unusual Whales Secret') {
      steps {
        withCredentials([string(credentialsId: params.UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID, variable: 'UNUSUAL_WHALES_API_KEY')]) {
          sh '''
            set -euo pipefail
            test -n "$UNUSUAL_WHALES_API_KEY"
            apply_args=""
            if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
              apply_args="--dry-run=server"
              echo "DEPLOY_DRY_RUN=true: validating secret manifests without changing Kubernetes."
            fi
            kubectl create namespace options-edge --dry-run=client -o yaml | kubectl apply $apply_args -f -
            kubectl -n options-edge create secret generic options-edge-secrets \
              --from-literal=unusual-whales-api-key="$UNUSUAL_WHALES_API_KEY" \
              --dry-run=client -o yaml | kubectl apply $apply_args -f -
            kubectl -n options-edge create secret generic options-edge-runtime-secrets \
              --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-Options#100}" \
              --from-literal=UNUSUAL_WHALES_API_KEY="$UNUSUAL_WHALES_API_KEY" \
              --dry-run=client -o yaml | kubectl apply $apply_args -f -
          '''
        }
      }
    }
    stage('Resolve Images') {
      steps {
        sh '''
          set -euo pipefail
          mkdir -p "$JENKINS_WORK_DIR"
          image_tag="${IMAGE_TAG:-}"
          if [ -n "${IMAGE_REGISTRY:-}" ]; then
            registry="$IMAGE_REGISTRY"
          elif [ "${ENVIRONMENT:-dev}" = "dev" ]; then
            registry=host.docker.internal:5001
          else
            registry=192.168.100.252:5000
          fi
          if [ -n "$image_tag" ] || [ "${ENVIRONMENT:-dev}" = "dev" ]; then
            if [ -z "$image_tag" ]; then
              image_tag=dev
            fi
            cat >"$JENKINS_WORK_DIR/options-edge-images.env" <<EOF
RAW_TO_DISPLAY_IMAGE=$registry/options-edge-raw-to-display:$image_tag
DATABENTO_VOLUME_AGGREGATOR_IMAGE=$registry/options-edge-databento-volume-aggregator:$image_tag
DATABENTO_MISSION_PACE_IMAGE=$registry/options-edge-databento-mission-pace:$image_tag
DATABENTO_MISSION_PRESSURE_IMAGE=$registry/options-edge-databento-mission-pressure:$image_tag
DATABENTO_MISSION_SANDWICH_IMAGE=$registry/options-edge-databento-mission-sandwich:$image_tag
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
SPX_MISSION_CONTROL_IMAGE=$registry/options-edge-spx-mission-control:$image_tag
STRIKE_FLOW_CLASSIFIER_IMAGE=$registry/options-edge-strike-flow-classifier:$image_tag
IBKR_FEED_IMAGE=$registry/options-edge-ibkr-feed:$image_tag
EOF
          else
            cat >"$JENKINS_WORK_DIR/options-edge-images.env" <<EOF
RAW_TO_DISPLAY_IMAGE=$RAW_TO_DISPLAY_IMAGE
DATABENTO_VOLUME_AGGREGATOR_IMAGE=$DATABENTO_VOLUME_AGGREGATOR_IMAGE
DATABENTO_MISSION_PACE_IMAGE=$DATABENTO_MISSION_PACE_IMAGE
DATABENTO_MISSION_PRESSURE_IMAGE=$DATABENTO_MISSION_PRESSURE_IMAGE
DATABENTO_MISSION_SANDWICH_IMAGE=$DATABENTO_MISSION_SANDWICH_IMAGE
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
SPX_MISSION_CONTROL_IMAGE=$SPX_MISSION_CONTROL_IMAGE
STRIKE_FLOW_CLASSIFIER_IMAGE=$STRIKE_FLOW_CLASSIFIER_IMAGE
IBKR_FEED_IMAGE=$IBKR_FEED_IMAGE
EOF
          fi
          echo "Resolved deployment images:"
          sed 's/^/  /' "$JENKINS_WORK_DIR/options-edge-images.env"
        '''
      }
    }
    stage('Image Preflight') {
      steps {
        sh '''
          set -euo pipefail
          . "$JENKINS_WORK_DIR/options-edge-images.env"
          . "$JENKINS_WORK_DIR/options-edge-build.env"
          images="
            RAW_TO_DISPLAY_IMAGE=$RAW_TO_DISPLAY_IMAGE
            DATABENTO_VOLUME_AGGREGATOR_IMAGE=$DATABENTO_VOLUME_AGGREGATOR_IMAGE
            DATABENTO_MISSION_PACE_IMAGE=$DATABENTO_MISSION_PACE_IMAGE
            DATABENTO_MISSION_PRESSURE_IMAGE=$DATABENTO_MISSION_PRESSURE_IMAGE
            DATABENTO_MISSION_SANDWICH_IMAGE=$DATABENTO_MISSION_SANDWICH_IMAGE
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
            SPX_MISSION_CONTROL_IMAGE=$SPX_MISSION_CONTROL_IMAGE
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

          inspect_image_platform() {
            local name="$1"
            local image="$2"
            local expected_platform="$3"
            local inspect_file
            inspect_file="$JENKINS_WORK_DIR/imagetools-${name}.txt"
            if ! docker buildx imagetools inspect "$image" >"$inspect_file"; then
              echo "Unable to inspect image manifest with docker buildx imagetools: $name=$image" >&2
              return 1
            fi
            sed "s/^/  $name imagetools: /" "$inspect_file"
            if ! grep -Eq "Platform:[[:space:]]*$expected_platform($|[[:space:]])|$expected_platform" "$inspect_file"; then
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
            if [ "${ENVIRONMENT:-dev}" = "production" ] || [ "${ENVIRONMENT:-dev}" = "staging" ]; then
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
            echo "One or more staging/production images are not linux/amd64; refusing to deploy to CentOS amd64 Kubernetes nodes." >&2
            exit 1
          fi
        '''
      }
    }
    stage('Deploy') {
      steps {
        sh '''
          set -euo pipefail
          . "$JENKINS_WORK_DIR/options-edge-images.env"
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
          effective_expiry="${IB_EXPIRY:-$(default_weekday_expiry)}"
          cat >"$JENKINS_WORK_DIR/options-edge-runtime-config-patch.json" <<EOF
{"data":{"APP_MARKET_DATA_SOURCE":"$market_data_source","KAFKA_RAW_TOPIC":"$effective_raw_topic","IB_HOST":"${IB_HOST:-127.0.0.1}","IB_PORT":"${IB_PORT:-4001}","IB_CLIENT_ID":"${IB_CLIENT_ID:-212}","IB_EXPIRY":"$effective_expiry","IB_MAX_STRIKES":"${IB_MAX_STRIKES:-43}","UNUSUAL_WHALES_EXPIRY":"$effective_expiry"}}
EOF
          kubectl -n options-edge patch configmap options-edge-config \
            --type merge \
            --patch "$(cat "$JENKINS_WORK_DIR/options-edge-runtime-config-patch.json")"
          if kubectl -n options-edge get configmap options-edge-databento-feed-config >/dev/null 2>&1; then
            cat >"$JENKINS_WORK_DIR/options-edge-databento-feed-config-patch.json" <<EOF
{"data":{"DATABENTO_EXPIRY":"$effective_expiry"}}
EOF
            kubectl -n options-edge patch configmap options-edge-databento-feed-config \
              --type merge \
              --patch "$(cat "$JENKINS_WORK_DIR/options-edge-databento-feed-config-patch.json")"
          fi
          kubectl -n options-edge set image deployment/raw-to-display-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/raw-to-display-databento-service raw-to-display="$RAW_TO_DISPLAY_IMAGE"
          kubectl -n options-edge set image deployment/databento-volume-aggregator databento-volume-aggregator="$DATABENTO_VOLUME_AGGREGATOR_IMAGE"
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
          kubectl -n options-edge rollout restart deployment/databento-volume-aggregator
          kubectl -n options-edge rollout restart deployment/databento-mission-pace-service
          kubectl -n options-edge rollout restart deployment/databento-mission-pressure-service
          kubectl -n options-edge rollout restart deployment/databento-mission-sandwich-service
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
          kubectl -n options-edge rollout restart deployment/strike-flow-classifier-databento
          kubectl -n options-edge rollout restart deployment/strike-flow-classifier-ibkr
          kubectl -n options-edge rollout restart deployment/spx-mission-control-service
          kubectl -n options-edge rollout restart deployment/ibkr-feed-service
          kubectl -n options-edge rollout status deployment/raw-to-display-service --timeout=180s
          kubectl -n options-edge rollout status deployment/raw-to-display-databento-service --timeout=180s
          kubectl -n options-edge rollout status deployment/databento-volume-aggregator --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-mission-pace-service --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-mission-pressure-service --timeout=240s
          kubectl -n options-edge rollout status deployment/databento-mission-sandwich-service --timeout=240s
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
          kubectl -n options-edge rollout status deployment/strike-flow-classifier-databento --timeout=180s
          kubectl -n options-edge rollout status deployment/strike-flow-classifier-ibkr --timeout=180s
          kubectl -n options-edge rollout status deployment/spx-mission-control-service --timeout=180s
          kubectl -n options-edge rollout status deployment/ibkr-feed-service --timeout=240s
        '''
      }
    }
    stage('Kafka Internal Topics') {
      when {
        expression { return !params.DEPLOY_DRY_RUN }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_BOOTSTRAP_SERVERS
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
      when {
        expression { return env.ENVIRONMENT != 'dev' && !params.DEPLOY_DRY_RUN }
      }
      steps {
        withCredentials([string(credentialsId: 'options-edge-remote-become-password', variable: 'BECOME_PASSWORD')]) {
          sh '''
            set -euo pipefail
            export NAMESPACE=options-edge
            export KUBECONFIG="${KUBECONFIG}"
            export REMOTE_APP_HOME="${REMOTE_APP_HOME}"
            export JENKINS_WORK_DIR="${JENKINS_WORK_DIR}"
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
      when {
        expression { return !params.DEPLOY_DRY_RUN }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
          export KAFKA_BOOTSTRAP_SERVERS
          export KAFKA_TOPIC_REPLICATION_FACTOR=1
          export KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1
          export KUBECONFIG="${KUBECONFIG}"
          export NAMESPACE=options-edge
          export REQUIRE_LATEST_SIGNAL=false
          scripts/kafka/create-hpsf-topics.sh
          scripts/kafka/verify-hpsf-topics.sh
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
          scripts/kafka/create-hpsf-topics.sh
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
    stage('Deploy To Production') {
      when {
        expression { return env.ENVIRONMENT != 'production' && !params.SKIP_PRODUCTION_PROMOTION }
      }
      steps {
        script {
          def promoteToProduction = false
          try {
            timeout(time: 30, unit: 'MINUTES') {
              input message: 'Dev deployment and smoke checks completed. Deploy the same build to PRODUCTION?', ok: 'Deploy to production'
            }
            promoteToProduction = true
          } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException ignored) {
            echo 'Production promotion was not approved; dev deployment remains complete.'
          }
          if (!promoteToProduction) {
            return
          }
          build job: env.JOB_NAME,
            wait: false,
            propagate: false,
            parameters: [
              string(name: 'ENVIRONMENT', value: 'production'),
              string(name: 'KUBECONFIG_FILE', value: env.KUBECONFIG),
              string(name: 'KUBECONFIG_ADMIN_FILE', value: env.KUBECONFIG_ADMIN_FILE),
              string(name: 'IMAGE_REGISTRY', value: params.IMAGE_REGISTRY),
              string(name: 'IMAGE_TAG', value: params.IMAGE_TAG),
              string(name: 'BUILD_PLATFORM', value: 'linux/amd64'),
              string(name: 'KAFKA_BOOTSTRAP_SERVERS', value: ''),
              string(name: 'WEB_PUBLIC_URL', value: params.WEB_PUBLIC_URL),
              string(name: 'RAW_TO_DISPLAY_IMAGE', value: params.RAW_TO_DISPLAY_IMAGE),
              string(name: 'DATABENTO_VOLUME_AGGREGATOR_IMAGE', value: params.DATABENTO_VOLUME_AGGREGATOR_IMAGE),
              string(name: 'DATABENTO_MISSION_PACE_IMAGE', value: params.DATABENTO_MISSION_PACE_IMAGE),
              string(name: 'DATABENTO_MISSION_PRESSURE_IMAGE', value: params.DATABENTO_MISSION_PRESSURE_IMAGE),
              string(name: 'DATABENTO_MISSION_SANDWICH_IMAGE', value: params.DATABENTO_MISSION_SANDWICH_IMAGE),
              string(name: 'VOLUME_PACE_IMAGE', value: params.VOLUME_PACE_IMAGE),
              string(name: 'DIRECTIONAL_PRESSURE_IMAGE', value: params.DIRECTIONAL_PRESSURE_IMAGE),
              string(name: 'VOLUME_SANDWICH_IMAGE', value: params.VOLUME_SANDWICH_IMAGE),
              string(name: 'UNUSUAL_WHALES_GEX_IMAGE', value: params.UNUSUAL_WHALES_GEX_IMAGE),
              string(name: 'UNUSUAL_WHALES_GEX_HISTORY_IMAGE', value: params.UNUSUAL_WHALES_GEX_HISTORY_IMAGE),
              string(name: 'RAW_POSTGRES_WRITER_IMAGE', value: params.RAW_POSTGRES_WRITER_IMAGE),
              string(name: 'PRESSURE_POSTGRES_WRITER_IMAGE', value: params.PRESSURE_POSTGRES_WRITER_IMAGE),
              string(name: 'FEED_GATEWAY_IMAGE', value: params.FEED_GATEWAY_IMAGE),
              string(name: 'INTEGRATION_TEST_IMAGE', value: params.INTEGRATION_TEST_IMAGE),
              string(name: 'HPSF_PROCESSING_IMAGE', value: params.HPSF_PROCESSING_IMAGE),
              string(name: 'HPSF_POSTGRES_WRITER_IMAGE', value: params.HPSF_POSTGRES_WRITER_IMAGE),
              string(name: 'SPX_MISSION_CONTROL_IMAGE', value: params.SPX_MISSION_CONTROL_IMAGE),
              string(name: 'STRIKE_FLOW_CLASSIFIER_IMAGE', value: params.STRIKE_FLOW_CLASSIFIER_IMAGE),
              string(name: 'IBKR_FEED_IMAGE', value: params.IBKR_FEED_IMAGE),
              string(name: 'UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID', value: params.UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID),
              string(name: 'MARKET_DATA_SOURCE', value: params.MARKET_DATA_SOURCE),
              string(name: 'RAW_TOPIC', value: params.RAW_TOPIC),
              string(name: 'IB_HOST', value: params.IB_HOST),
              string(name: 'IB_PORT', value: params.IB_PORT),
              string(name: 'IB_CLIENT_ID', value: params.IB_CLIENT_ID),
              string(name: 'IB_EXPIRY', value: params.IB_EXPIRY),
              string(name: 'IB_MAX_STRIKES', value: params.IB_MAX_STRIKES),
              booleanParam(name: 'KAFKA_CLEANUP_TOPICS', value: false),
              booleanParam(name: 'KAFKA_DELETE_UNWANTED_TOPICS', value: false),
              booleanParam(name: 'ALLOW_PROD_KAFKA_CLEANUP', value: false),
              booleanParam(name: 'SKIP_PRODUCTION_PROMOTION', value: true),
              booleanParam(name: 'DEPLOY_DRY_RUN', value: params.DEPLOY_DRY_RUN)
            ]
        }
      }
    }
  }
}
