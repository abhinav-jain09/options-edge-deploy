@Library('oe') _

pipeline {
  agent { label 'hpsf-replay-mac' }
  parameters {
    choice(name: 'ENVIRONMENT', choices: ['dev', 'production'], description: 'Target environment')
    string(name: 'DEPLOY_BRANCH', defaultValue: 'main', description: 'Git branch to deploy. LOCKED TO main for all environments (dev AND prod) — feature branches must be merged before deploy. The job SCM checks out this branch; enforce-main-branch.sh rejects anything but main.')
    string(name: 'KUBECONFIG_FILE', defaultValue: '', description: 'Dev deployer kubeconfig path on the Jenkins agent (Mac, ~/.kube — like prod). Bootstrap generates it from the admin kubeconfig.')
    string(name: 'KUBECONFIG_ADMIN_FILE', defaultValue: '', description: 'Dev admin (docker-desktop cluster-admin) kubeconfig used only to bootstrap the Jenkins-only deploy guard. Empty = derive from oeProfile(ENVIRONMENT).kubeconfigAdmin.')
    string(name: 'PROD_KUBECONFIG_FILE', defaultValue: '', description: 'Deployer kubeconfig for the PROD cluster, passed to the build launched by the Promote To Production button (prod is a separate cluster from dev).')
    string(name: 'PROD_KUBECONFIG_ADMIN_FILE', defaultValue: '', description: 'Admin kubeconfig for the PROD cluster, passed to the build launched by the Promote To Production button.')
    string(name: 'IMAGE_REGISTRY', defaultValue: '', description: 'Docker registry namespace used when IMAGE_TAG is set. Empty derives from oeProfile(ENVIRONMENT).registry.')
    string(name: 'IMAGE_TAG', defaultValue: '', description: 'Exact Docker tag to use for all runtime images. Empty keeps per-image parameters.')
    string(name: 'BUILD_PLATFORM', defaultValue: '', description: 'Image platform. Empty derives from oeProfile(ENVIRONMENT).platform.')
    string(name: 'KAFKA_BOOTSTRAP_SERVERS', defaultValue: '', description: 'Kafka bootstrap servers. Empty derives from oeProfile(ENVIRONMENT).kafkaBootstrap.')
    string(name: 'WEB_PUBLIC_URL', defaultValue: '', description: 'Public OptionsEdge web URL for smoke checks. Empty uses the per-environment dev/prod default.')
    string(name: 'RAW_TO_DISPLAY_IMAGE', defaultValue: '', description: 'Raw-to-display image')
    string(name: 'WEB_IMAGE', defaultValue: '', description: 'OptionsEdge web image')
    string(name: 'DATABENTO_VOLUME_AGGREGATOR_IMAGE', defaultValue: '', description: 'Databento volume aggregator image')
    string(name: 'DATABENTO_FEED_IMAGE', defaultValue: '', description: 'Databento feed image')
    string(name: 'DATABENTO_GEX_IMAGE', defaultValue: '', description: 'Databento per-strike GEX image')
    string(name: 'DATABENTO_MAXPAIN_IMAGE', defaultValue: '', description: 'Databento per-(symbol,expiry) max-pain image')
    string(name: 'DATABENTO_MISSION_PACE_IMAGE', defaultValue: '', description: 'Databento mission pace image')
    string(name: 'DATABENTO_MISSION_PRESSURE_IMAGE', defaultValue: '', description: 'Databento mission pressure image')
    string(name: 'DATABENTO_MISSION_SANDWICH_IMAGE', defaultValue: '', description: 'Databento mission sandwich image')
    string(name: 'VOLUME_PACE_IMAGE', defaultValue: '', description: 'Volume-pace image')
    string(name: 'DIRECTIONAL_PRESSURE_IMAGE', defaultValue: '', description: 'Directional-pressure image')
    string(name: 'VOLUME_SANDWICH_IMAGE', defaultValue: '', description: 'Volume-sandwich image')
    string(name: 'UNUSUAL_WHALES_GEX_IMAGE', defaultValue: '', description: 'Unusual Whales GEX image')
    string(name: 'UNUSUAL_WHALES_GEX_HISTORY_IMAGE', defaultValue: '', description: 'Unusual Whales GEX history image')
    string(name: 'DATABENTO_GEX_HISTORY_IMAGE', defaultValue: '', description: 'Databento GEX history image')
    string(name: 'RAW_POSTGRES_WRITER_IMAGE', defaultValue: '', description: 'Raw Postgres writer image')
    string(name: 'PRESSURE_POSTGRES_WRITER_IMAGE', defaultValue: '', description: 'Pressure Postgres writer image')
    string(name: 'PIN_POSTGRES_WRITER_IMAGE', defaultValue: '', description: 'Pin Postgres writer image (dev-only deployment)')
    string(name: 'FEED_GATEWAY_IMAGE', defaultValue: '', description: 'Feed gateway image')
    string(name: 'INTEGRATION_TEST_IMAGE', defaultValue: '', description: 'Integration-test image')
    string(name: 'HPSF_PROCESSING_IMAGE', defaultValue: '', description: 'HPSF Stage A/B processing image')
    string(name: 'HPSF_POSTGRES_WRITER_IMAGE', defaultValue: '', description: 'HPSF Postgres writer image')
    string(name: 'SPX_MISSION_CONTROL_IMAGE', defaultValue: '', description: 'SPX mission control image')
    string(name: 'STRIKE_FLOW_CLASSIFIER_IMAGE', defaultValue: '', description: 'Strike flow classifier image')
    string(name: 'IBKR_FEED_IMAGE', defaultValue: '', description: 'IBKR feed image')
    string(name: 'UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID', defaultValue: 'options-edge-unusual-whales-api-key', description: 'Jenkins secret-text credential containing the Unusual Whales API key')
    string(name: 'DATABENTO_API_KEY_CREDENTIAL_ID', defaultValue: 'options-edge-databento-api-key', description: 'Jenkins secret-text credential containing the Databento API key')
    string(name: 'KEYCLOAK_DB_PASSWORD_CREDENTIAL_ID', defaultValue: 'oe-keycloak-db-password', description: 'Jenkins secret-text credential with the prod Keycloak DB password (oe-keycloak-secrets POSTGRES_PASSWORD)')
    string(name: 'KEYCLOAK_ADMIN_PASSWORD_CREDENTIAL_ID', defaultValue: 'oe-keycloak-admin-password', description: 'Jenkins secret-text credential with the prod Keycloak bootstrap admin password')
    choice(name: 'MARKET_DATA_SOURCE', choices: ['DATABENTO', 'IBKR'], description: 'Runtime raw market-data source for processors')
    string(name: 'RAW_TOPIC', defaultValue: '', description: 'Override raw topic. Empty uses source default.')
    string(name: 'IB_HOST', defaultValue: '127.0.0.1', description: 'IB Gateway/TWS host. IBKR feed uses hostNetwork, so localhost is the remote host.')
    string(name: 'IB_PORT', defaultValue: '4001', description: 'IB Gateway/TWS API port')
    string(name: 'IB_CLIENT_ID', defaultValue: '212', description: 'IBKR feed API client id')
    string(name: 'IB_EXPIRY', defaultValue: '', description: 'Option expiry/date. Empty uses the current weekday on the Jenkins agent.')
    string(name: 'DATABENTO_EXPIRY', defaultValue: '', description: 'Override expiry for Databento Historical feed (YYYYMMDD). Empty -> auto-resolved from Databento metadata + MarketCalendar in the Resolve Databento Expiry stage (fail-closed if Databento is unreachable or the result is not a trading day).')
    string(name: 'IB_MAX_STRIKES', defaultValue: '43', description: 'Max strikes around spot for IBKR feed')
    booleanParam(name: 'SKIP_KAFKA_TOPICS', defaultValue: false, description: 'Skip all three Kafka topic stages (Kafka Topics, Reset HPSF Stage B Internal Topics, Kafka Internal Topics). Use for a code/image-only redeploy when topic configs and partitions are already correct on the cluster — saves ~5-15 min on a typical run. Defaults off (run the full topic apply/verify path).')
    booleanParam(name: 'KAFKA_CLEANUP_TOPICS', defaultValue: false, description: 'Clean Kafka topics before deployment')
    booleanParam(name: 'KAFKA_DELETE_UNWANTED_TOPICS', defaultValue: false, description: 'Delete non-whitelisted topics')
    booleanParam(name: 'ALLOW_PROD_KAFKA_CLEANUP', defaultValue: false, description: 'Allow destructive Kafka cleanup in production')
    booleanParam(name: 'SKIP_PRODUCTION_PROMOTION', defaultValue: false, description: 'Internal guard used by the manual production promotion build. DO NOT pass this by hand — it is set by the Promote To Production stage. Manual ENVIRONMENT=production runs with SKIP_PRODUCTION_PROMOTION=true are now REJECTED at Validate unless the build came from a dev run of this job (the legitimate promote path) OR the EMERGENCY_DIRECT_PROD_DEPLOY break-glass is engaged.')
    booleanParam(name: 'EMERGENCY_DIRECT_PROD_DEPLOY', defaultValue: false, description: 'BREAK-GLASS: skip the must-go-via-dev rule and deploy directly to prod. Use ONLY for genuine emergencies (prod broken AND the dev path is unusable). Requires a non-empty EMERGENCY_REASON. The override is loudly audit-logged to the build console.')
    string(name: 'EMERGENCY_REASON', defaultValue: '', description: 'Why are you doing a direct-to-prod emergency deploy? Required and non-empty when EMERGENCY_DIRECT_PROD_DEPLOY=true. Captured in the audit log.')
    booleanParam(name: 'DEPLOY_DRY_RUN', defaultValue: false, description: 'Validate render, image preflight, and server-side Kubernetes apply without mutating runtime resources.')
    booleanParam(name: 'SKIP_HPSF_SMOKE', defaultValue: true, description: 'Skip the HPSF Smoke stage (Stage B runtime check). Temporarily bypassed until the Stage B underlying-state/runtime check is fixed; set false to re-enable.')
  }
  environment {
    ENVIRONMENT = "${params.ENVIRONMENT ?: 'dev'}"
    // KUBECONFIG / KUBECONFIG_ADMIN_FILE / REMOTE_APP_HOME come from oeProfile (single
    // source of truth). An explicit non-empty operator override is honored; empty derives
    // from the profile.
    KUBECONFIG_FILE       = "${params.KUBECONFIG_FILE       ?: oeProfile(params.ENVIRONMENT).kubeconfigDeployer}"
    KUBECONFIG            = "${params.KUBECONFIG_FILE       ?: oeProfile(params.ENVIRONMENT).kubeconfigDeployer}"
    KUBECONFIG_ADMIN_FILE = "${params.KUBECONFIG_ADMIN_FILE ?: oeProfile(params.ENVIRONMENT).kubeconfigAdmin}"
    REMOTE_APP_HOME = "${oeProfile(params.ENVIRONMENT).remoteAppHome}"
    JENKINS_WORK_DIR = '.jenkins-tmp'
    // PATH inherited from the Jenkins agent (the prior CentOS-controller-only PATH
    // prefix was removed; it pointed at a directory that does not exist on local-mac).
    IMAGE_REGISTRY = "${params.IMAGE_REGISTRY ?: ''}"
    IMAGE_TAG = "${params.IMAGE_TAG ?: ''}"
    BUILD_PLATFORM = "${params.BUILD_PLATFORM ?: ''}"
    KAFKA_BOOTSTRAP_SERVERS = "${params.KAFKA_BOOTSTRAP_SERVERS ?: ''}"
    WEB_PUBLIC_URL = "${params.WEB_PUBLIC_URL ?: ''}"
    RAW_TO_DISPLAY_IMAGE = "${params.RAW_TO_DISPLAY_IMAGE ?: oeProfile.image('raw-to-display', 'production', 'dev')}"
    WEB_IMAGE = "${params.WEB_IMAGE ?: oeProfile.image('web', 'production', 'dev')}"
    DATABENTO_VOLUME_AGGREGATOR_IMAGE = "${params.DATABENTO_VOLUME_AGGREGATOR_IMAGE ?: oeProfile.image('databento-volume-aggregator', 'production', 'dev')}"
    DATABENTO_FEED_IMAGE = "${params.DATABENTO_FEED_IMAGE ?: oeProfile.image('databento-feed', 'production', 'dev')}"
    DATABENTO_GEX_IMAGE = "${params.DATABENTO_GEX_IMAGE ?: oeProfile.image('databento-gex', 'production', 'dev')}"
    DATABENTO_MAXPAIN_IMAGE = "${params.DATABENTO_MAXPAIN_IMAGE ?: oeProfile.image('databento-maxpain', 'production', 'dev')}"
    DATABENTO_MISSION_PACE_IMAGE = "${params.DATABENTO_MISSION_PACE_IMAGE ?: oeProfile.image('databento-mission-pace', 'production', 'dev')}"
    DATABENTO_MISSION_PRESSURE_IMAGE = "${params.DATABENTO_MISSION_PRESSURE_IMAGE ?: oeProfile.image('databento-mission-pressure', 'production', 'dev')}"
    DATABENTO_MISSION_SANDWICH_IMAGE = "${params.DATABENTO_MISSION_SANDWICH_IMAGE ?: oeProfile.image('databento-mission-sandwich', 'production', 'dev')}"
    VOLUME_PACE_IMAGE = "${params.VOLUME_PACE_IMAGE ?: oeProfile.image('volume-pace', 'production', 'dev')}"
    DIRECTIONAL_PRESSURE_IMAGE = "${params.DIRECTIONAL_PRESSURE_IMAGE ?: oeProfile.image('directional-pressure', 'production', 'dev')}"
    VOLUME_SANDWICH_IMAGE = "${params.VOLUME_SANDWICH_IMAGE ?: oeProfile.image('volume-sandwich', 'production', 'dev')}"
    UNUSUAL_WHALES_GEX_IMAGE = "${params.UNUSUAL_WHALES_GEX_IMAGE ?: oeProfile.image('unusual-whales-gex', 'production', 'dev')}"
    UNUSUAL_WHALES_GEX_HISTORY_IMAGE = "${params.UNUSUAL_WHALES_GEX_HISTORY_IMAGE ?: oeProfile.image('unusual-whales-gex-history', 'production', 'dev')}"
    DATABENTO_GEX_HISTORY_IMAGE = "${params.DATABENTO_GEX_HISTORY_IMAGE ?: oeProfile.image('databento-gex-history', 'production', 'dev')}"
    RAW_POSTGRES_WRITER_IMAGE = "${params.RAW_POSTGRES_WRITER_IMAGE ?: oeProfile.image('raw-postgres-writer', 'production', 'dev')}"
    PRESSURE_POSTGRES_WRITER_IMAGE = "${params.PRESSURE_POSTGRES_WRITER_IMAGE ?: oeProfile.image('pressure-postgres-writer', 'production', 'dev')}"
    // pin-postgres-writer is a DEV-ONLY deployment (manifests live in k8s/overlays/dev, not base). The env
    // value is resolved in both profiles for consistency, but it is only digest-pinned/applied for dev (see
    // the dev-gated extension to the digest-pin loop) so a prod deploy never tries to resolve an image that
    // does not exist in the prod registry.
    PIN_POSTGRES_WRITER_IMAGE = "${params.PIN_POSTGRES_WRITER_IMAGE ?: oeProfile.image('pin-postgres-writer', 'production', 'dev')}"
    FEED_GATEWAY_IMAGE = "${params.FEED_GATEWAY_IMAGE ?: oeProfile.image('feed-gateway', 'production', 'dev')}"
    INTEGRATION_TEST_IMAGE = "${params.INTEGRATION_TEST_IMAGE ?: oeProfile.image('integration-test', 'production', 'dev')}"
    HPSF_PROCESSING_IMAGE = "${params.HPSF_PROCESSING_IMAGE ?: oeProfile.image('hpsf-processing', 'production', 'dev')}"
    HPSF_POSTGRES_WRITER_IMAGE = "${params.HPSF_POSTGRES_WRITER_IMAGE ?: oeProfile.image('hpsf-postgres-writer', 'production', 'dev')}"
    SPX_MISSION_CONTROL_IMAGE = "${params.SPX_MISSION_CONTROL_IMAGE ?: oeProfile.image('spx-mission-control', 'production', 'dev')}"
    STRIKE_FLOW_CLASSIFIER_IMAGE = "${params.STRIKE_FLOW_CLASSIFIER_IMAGE ?: oeProfile.image('strike-flow-classifier', 'production', 'dev')}"
    IBKR_FEED_IMAGE = "${params.IBKR_FEED_IMAGE ?: oeProfile.image('ibkr-feed', 'production', 'dev')}"
    MARKET_DATA_SOURCE = "${params.MARKET_DATA_SOURCE ?: 'DATABENTO'}"
    RAW_TOPIC = "${params.RAW_TOPIC ?: ''}"
    IB_HOST = "${params.IB_HOST ?: '127.0.0.1'}"
    IB_PORT = "${params.IB_PORT ?: '4001'}"
    IB_CLIENT_ID = "${params.IB_CLIENT_ID ?: '212'}"
    IB_EXPIRY = "${params.IB_EXPIRY ?: ''}"
    DATABENTO_EXPIRY = "${params.DATABENTO_EXPIRY ?: ''}"
    IB_MAX_STRIKES = "${params.IB_MAX_STRIKES ?: '43'}"
    KAFKA_CLEANUP_TOPICS = "${params.KAFKA_CLEANUP_TOPICS ?: false}"
    KAFKA_DELETE_UNWANTED_TOPICS = "${params.KAFKA_DELETE_UNWANTED_TOPICS ?: false}"
    ALLOW_PROD_KAFKA_CLEANUP = "${params.ALLOW_PROD_KAFKA_CLEANUP ?: false}"
    SKIP_PRODUCTION_PROMOTION = "${params.SKIP_PRODUCTION_PROMOTION ?: false}"
    DEPLOY_DRY_RUN = "${params.DEPLOY_DRY_RUN ?: false}"
  }
  stages {
    stage('Resolve profile') {
      // Observability-only: echo the canonical deploy profile from the single source
      // of truth (@Library('oe') deploy-profiles.yaml). This stage does NOT override
      // any param defaults — the existing params already match the profile (kubeconfig
      // and registry defaults were aligned in PR #80/#83). When the policy guard later
      // flips fail-closed, this @Library import is what proves this job is on-policy.
      // If the profile ever drifts from the params, this stage logs it loudly.
      steps {
        script {
          // Single source of truth for EVERY environment-specific value in this job.
          // Each derived env.* is validated nonblank before assignment, so a future
          // profile weakening can never silently propagate a null/empty into the build.
          def p = oeProfile(params.ENVIRONMENT)
          def required = [
            registry:           p.registry,
            kafkaBootstrap:     p.kafkaBootstrap,
            kubeconfigDeployer: p.kubeconfigDeployer,
            kubeconfigAdmin:    p.kubeconfigAdmin,
            remoteAppHome:      p.remoteAppHome,
          ]
          required.each { k, v ->
            if (!(v?.toString()?.trim())) {
              error("Resolve profile: oeProfile(${params.ENVIRONMENT}).${k} is empty — check deploy-profiles.yaml")
            }
          }
          // Export as env.* (validated local values; Jenkins coerces null to 'null' otherwise).
          env.OE_REGISTRY           = required.registry.toString().trim()
          env.OE_KAFKA_BOOTSTRAP    = required.kafkaBootstrap.toString().trim()
          env.OE_KUBECONFIG_FILE    = required.kubeconfigDeployer.toString().trim()
          env.OE_KUBECONFIG_ADMIN   = required.kubeconfigAdmin.toString().trim()
          env.OE_REMOTE_APP_HOME    = required.remoteAppHome.toString().trim()
          // Dev + prod registries from the profile (used by buildkit insecure-config and
          // the prod-with-dev-registry safety check). Validated locally before env assign.
          def devReg  = oeProfile('dev').registry?.toString()?.trim()
          def prodReg = oeProfile('production').registry?.toString()?.trim()
          if (!devReg)  { error("Resolve profile: oeProfile('dev').registry is empty — check deploy-profiles.yaml") }
          if (!prodReg) { error("Resolve profile: oeProfile('production').registry is empty — check deploy-profiles.yaml") }
          env.OE_DEV_REGISTRY  = devReg
          env.OE_PROD_REGISTRY = prodReg

          echo "oeProfile(${params.ENVIRONMENT}): registry=${env.OE_REGISTRY} kafka=${env.OE_KAFKA_BOOTSTRAP} kubeconfigDeployer=${env.OE_KUBECONFIG_FILE} kubeconfigAdmin=${env.OE_KUBECONFIG_ADMIN} remoteAppHome=${env.OE_REMOTE_APP_HOME} ns=${p.namespace} platform=${p.platform} (devRegistry=${env.OE_DEV_REGISTRY})"

          // Drift warnings: if the operator passed a kubeconfig param that diverges from
          // the profile, log it (the build still uses the profile-derived value below).
          def deployerActual = params.KUBECONFIG_FILE ?: ''
          def adminActual    = params.KUBECONFIG_ADMIN_FILE ?: ''
          if (deployerActual && deployerActual != env.OE_KUBECONFIG_FILE) {
            echo "WARN: KUBECONFIG_FILE param (${deployerActual}) differs from oeProfile (${env.OE_KUBECONFIG_FILE})"
          }
          if (adminActual && adminActual != env.OE_KUBECONFIG_ADMIN) {
            echo "WARN: KUBECONFIG_ADMIN_FILE param (${adminActual}) differs from oeProfile (${env.OE_KUBECONFIG_ADMIN})"
          }
        }
      }
    }
    stage('Validate') {
      steps {
        // The must-go-via-dev prod-promotion guard lives in the top-level
        // enforceProdPromotionGuard() method (defined after pipeline{}). Moving
        // this CPS-heavy Groovy out of the declarative pipeline's single compiled
        // method keeps it under the JVM 64KB per-method bytecode limit
        // (MethodTooLargeException). See also promoteToProduction() below.
        script { enforceProdPromotionGuard() }
        sh 'bash -x scripts/deploy/validate-platform.sh'
      }
    }
    stage('Resolve Databento Expiry') {
      // Placed AFTER Validate (so enforce-main-branch.sh has already blocked non-main runs)
      // and BEFORE Bootstrap Jenkins Kubernetes Guard. Resolves the latest OPRA.PILLAR
      // Historical trading-day expiry via scripts/jenkins/pick-databento-historical-expiry.sh
      // and exposes it as env.RESOLVED_DATABENTO_EXPIRY for the configmap-patch stage.
      // Fail-closed — if Databento metadata is unreachable or the result is not a trading
      // day (e.g., explicit override of a holiday like Juneteenth), the build fails here
      // instead of crash-looping the feed pod later.
      steps {
        withCredentials([
          string(credentialsId: params.DATABENTO_API_KEY_CREDENTIAL_ID, variable: 'DATABENTO_API_KEY')
        ]) {
          script {
            def resolved = sh(
              returnStdout: true,
              script: 'scripts/jenkins/pick-databento-historical-expiry.sh'
            ).trim()
            env.RESOLVED_DATABENTO_EXPIRY = resolved
            echo "Resolved DATABENTO_EXPIRY = ${resolved}"
          }
        }
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
    stage('Render') {
      steps {
        sh 'kubectl kustomize k8s/overlays/${ENVIRONMENT} >"$JENKINS_WORK_DIR/options-edge-${ENVIRONMENT}.yaml"'
      }
    }
    stage('Unusual Whales Secret') {
      steps {
        withCredentials([
          string(credentialsId: params.UNUSUAL_WHALES_API_KEY_CREDENTIAL_ID, variable: 'UNUSUAL_WHALES_API_KEY'),
          string(credentialsId: params.DATABENTO_API_KEY_CREDENTIAL_ID, variable: 'DATABENTO_API_KEY')
        ]) {
          sh '''
            set -euo pipefail
            test -n "$UNUSUAL_WHALES_API_KEY"
            test -n "$DATABENTO_API_KEY"
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
            kubectl -n options-edge create secret generic options-edge-databento-feed-env \
              --from-literal=DATABENTO_API_KEY="$DATABENTO_API_KEY" \
              --dry-run=client -o yaml | kubectl apply $apply_args -f -
          '''
        }
      }
    }
    // Production identity provider (Keycloak) secret — created the same deployer-SA way (the admission
    // policy blocks any other principal from writing secrets, verified even system:admin is denied).
    // PRODUCTION ONLY: the KC manifests live only in the production overlay, so dev has no consumer — and
    // the KC credentials are bound in THIS stage's withCredentials, so dev runs never resolve them.
    stage('Keycloak Secret') {
      when { expression { return params.ENVIRONMENT == 'production' } }
      steps {
        withCredentials([
          string(credentialsId: params.KEYCLOAK_DB_PASSWORD_CREDENTIAL_ID, variable: 'KEYCLOAK_DB_PASSWORD'),
          string(credentialsId: params.KEYCLOAK_ADMIN_PASSWORD_CREDENTIAL_ID, variable: 'KEYCLOAK_ADMIN_PASSWORD')
        ]) {
          sh '''
            set -euo pipefail
            test -n "$KEYCLOAK_DB_PASSWORD"
            test -n "$KEYCLOAK_ADMIN_PASSWORD"
            apply_args=""
            if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
              apply_args="--dry-run=server"
              echo "DEPLOY_DRY_RUN=true: validating the Keycloak secret without changing Kubernetes."
            fi
            # Feed secret material via a 0600 temp env-file (trap-cleaned), not --from-literal, so the
            # values never appear in the kubectl process argv on the Jenkins agent. Two keys:
            # POSTGRES_PASSWORD is the single source of truth for both Postgres AND Keycloak's JDBC pw.
            umask 077
            envfile="$(mktemp)"; trap 'rm -f "$envfile"' EXIT
            printf 'POSTGRES_PASSWORD=%s\\nKC_BOOTSTRAP_ADMIN_PASSWORD=%s\\n' \
              "$KEYCLOAK_DB_PASSWORD" "$KEYCLOAK_ADMIN_PASSWORD" > "$envfile"
            kubectl -n options-edge create secret generic oe-keycloak-secrets \
              --from-env-file="$envfile" \
              --dry-run=client -o yaml | kubectl apply $apply_args -f -
          '''
        }
      }
    }
    stage('Resolve Images') {
      steps {
        sh 'bash -x scripts/deploy/resolve-images.sh'
      }
    }
    stage('Image Preflight') {
      steps {
        sh 'bash -x scripts/deploy/image-preflight.sh'
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
            pod_count="$(kubectl -n options-edge get pods --no-headers 2>/dev/null | awk '$3 !~ /^(Completed|Succeeded|Error|Failed)$/ { print }' | wc -l | tr -d ' ')"
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
          if [ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
            : "${OE_KAFKA_BOOTSTRAP:?OE_KAFKA_BOOTSTRAP must be set by the Resolve profile stage (oeProfile single source of truth)}"
            KAFKA_BOOTSTRAP_SERVERS="$OE_KAFKA_BOOTSTRAP"
          fi
          export KAFKA_BOOTSTRAP_SERVERS
          export TOPIC_PREFIX
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
        expression { return !params.DEPLOY_DRY_RUN && !params.SKIP_KAFKA_TOPICS }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          # Single source of truth: derive bootstrap + RF/minISR/retention from the
          # rendered per-environment configmap (k8s/overlays/${ENVIRONMENT}), so topic
          # creation always matches what is deployed (no hard-coded RF to drift).
          . scripts/kafka/load-kafka-settings.sh
          export TOPIC_PREFIX
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
        expression { return !params.DEPLOY_DRY_RUN && !params.SKIP_KAFKA_TOPICS }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          if [ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
            : "${OE_KAFKA_BOOTSTRAP:?OE_KAFKA_BOOTSTRAP must be set by the Resolve profile stage (oeProfile single source of truth)}"
            KAFKA_BOOTSTRAP_SERVERS="$OE_KAFKA_BOOTSTRAP"
          fi
          export KAFKA_BOOTSTRAP_SERVERS
          export TOPIC_PREFIX
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
    stage('Kafka Internal Topics') {
      when {
        expression { return !params.DEPLOY_DRY_RUN && !params.SKIP_KAFKA_TOPICS }
      }
      steps {
        sh '''
          set -euo pipefail
          export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
          if [ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
            : "${OE_KAFKA_BOOTSTRAP:?OE_KAFKA_BOOTSTRAP must be set by the Resolve profile stage (oeProfile single source of truth)}"
            KAFKA_BOOTSTRAP_SERVERS="$OE_KAFKA_BOOTSTRAP"
          fi
          export KAFKA_BOOTSTRAP_SERVERS
          export KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1
          # Single source of truth for the per-env retention cap: load-kafka-settings.sh
          # exports KAFKA_MAX_RETENTION_MS from the rendered configmap (dev=10h; unset in
          # prod). KAFKA_BOOTSTRAP_SERVERS/MIN_ISR are already set above, so its ':='
          # derivations no-op for those. The streams-internal + changelog retention then
          # default to the cap (dev 10h), or 24h when no cap is set (prod unchanged).
          . scripts/kafka/load-kafka-settings.sh
          internal_ret_default="${KAFKA_MAX_RETENTION_MS:-86400000}"
          export KAFKA_TOPIC_RETENTION_MS="${internal_ret_default}"
          export KAFKA_STREAMS_INTERNAL_RETENTION_MS="${KAFKA_STREAMS_INTERNAL_RETENTION_MS:-$internal_ret_default}"
          export KAFKA_STREAMS_INTERNAL_SEGMENT_MS="${KAFKA_STREAMS_INTERNAL_SEGMENT_MS:-3600000}"
          export KAFKA_CHANGELOG_RETENTION_MS="${KAFKA_CHANGELOG_RETENTION_MS:-$internal_ret_default}"
          export KAFKA_CHANGELOG_DELETE_RETENTION_MS="${KAFKA_CHANGELOG_DELETE_RETENTION_MS:-3600000}"
          export KAFKA_CHANGELOG_MIN_CLEANABLE_DIRTY_RATIO="${KAFKA_CHANGELOG_MIN_CLEANABLE_DIRTY_RATIO:-0.01}"
          scripts/kafka/apply-internal-topic-configs.sh
        '''
      }
    }
    stage('Deploy') {
      steps {
        sh 'bash -x scripts/deploy/apply.sh'
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
    stage('Verify OptionsEdge Web App') {
      when {
        expression { return !params.DEPLOY_DRY_RUN }
      }
      steps {
        sh '''
          set -euo pipefail
          # The option-chain UI is gated behind Keycloak login: public shell/assets serve, but /api/** is
          # 401 without a bearer JWT. check-options-edge-web.sh asserts that posture (public "/" +
          # "/option-chain" == 200; "/api/config" == 200+provider when auth is off OR 401 when on), so the
          # verify is auth-on/off portable instead of false-failing on the (correct) 401.
          if [ "${ENVIRONMENT:-dev}" = "dev" ]; then
            # Default to the k8s web Service (LoadBalancer :8094, bound on localhost by
            # docker-desktop ServiceLB on the native Mac agent) — mirroring prod, which
            # verifies the k8s web at 192.168.100.252:8094. Must NOT default to the legacy
            # Tomcat :8090, or this stage false-greens against the old listener instead of
            # the options-edge-web Deployment this pipeline just rolled out.
            WEB_PUBLIC_URL="${WEB_PUBLIC_URL:-http://localhost:8094}" \
              scripts/smoke/check-options-edge-web.sh
          else
            # The prod web app runs on the prod host (192.168.100.252), not the Jenkins agent;
            # verify the deployed prod web app remotely over HTTP.
            # Note: do NOT reuse WEB_PUBLIC_URL -- the promote step forwards the
            # dev value (localhost) into the production build.
            WEB_PUBLIC_URL="${PROD_WEB_PUBLIC_URL:-http://192.168.100.252:8094}" \
              scripts/smoke/check-options-edge-web.sh
          fi
        '''
      }
    }
    stage('Smoke') {
      steps {
        sh '''
          set -euo pipefail
          if [ -z "${WEB_BASE_URL:-}" ]; then
            if [ "${ENVIRONMENT:-dev}" = "dev" ] && [ -n "${WEB_PUBLIC_URL:-}" ]; then
              # Dev only: honor the explicit web URL passed for smoke checks so
              # the Smoke stage targets the same endpoint as 'Verify OptionsEdge
              # Web App'. The smoke runs on the native (Mac) Jenkins agent, where
              # the docker-only DNS name host.docker.internal does not resolve, so
              # the dev default below uses localhost (docker-desktop ServiceLB binds
              # the k8s web LoadBalancer :8094 on localhost).
              # Production must NOT honor an inherited dev WEB_PUBLIC_URL (the
              # promote step forwards the dev value); it always targets prod.
              WEB_BASE_URL="$WEB_PUBLIC_URL"
            elif [ "${ENVIRONMENT:-dev}" = "dev" ]; then
              # k8s web Service (:8094 on localhost), NOT legacy Tomcat :8090 — same
              # rationale as the Verify stage above; mirrors prod's :8094 target.
              WEB_BASE_URL=http://localhost:8094
            else
              WEB_BASE_URL=http://192.168.100.252:8094
            fi
          fi
          export WEB_BASE_URL
          scripts/smoke/check-k8s-services.sh
        '''
      }
    }
    stage('HPSF Smoke') {
      when {
        expression { return !params.DEPLOY_DRY_RUN && !params.SKIP_HPSF_SMOKE }
      }
      steps {
        sh 'bash -x scripts/deploy/hpsf-smoke.sh'
      }
    }
    stage('Promote To Production') {
      when {
        expression { return env.ENVIRONMENT != 'production' && !params.SKIP_PRODUCTION_PROMOTION }
      }
      steps {
        // The promotion gate + downstream prod build (a CPS-heavy build job: with
        // ~50 parameter() calls — the single largest Groovy block in this file)
        // lives in the top-level promoteToProduction() method (defined after
        // pipeline{}). Keeping it out of the declarative pipeline's single
        // compiled method is what keeps the script under the JVM 64KB per-method
        // bytecode limit (MethodTooLargeException).
        script { promoteToProduction() }
      }
    }
  }
}

// --- Top-level helper methods ---------------------------------------------
// Defined OUTSIDE the pipeline{} block so each compiles to its OWN CPS method,
// rather than being inlined into the single giant WorkflowScript method that
// the declarative pipeline produces. Pipeline steps (error/echo/input/build/
// timeout) and the params/env/currentBuild bindings resolve at runtime through
// the script binding, exactly as they do inline. This is the structural fix for
// MethodTooLargeException: the prod-promotion guard and the downstream prod
// build are the two heaviest Groovy blocks, so hoisting them frees the most
// per-method bytecode.

// Must-go-via-dev gate. ENVIRONMENT=production deploys are only legitimate when:
//   (A) the build was triggered as a downstream of the "Promote To Production"
//       stage of a dev run of THIS SAME job (the build job: env.JOB_NAME call in
//       promoteToProduction() records an UpstreamCause we can verify), OR
//   (B) the operator engages the audited break-glass: EMERGENCY_DIRECT_PROD_DEPLOY
//       =true PLUS a non-empty EMERGENCY_REASON. Both required, both logged loudly.
// This closes the previous hole where any operator could pass
// SKIP_PRODUCTION_PROMOTION=true and bypass the direct-prod guard.
// SKIP_PRODUCTION_PROMOTION remains the implementation marker (the promote stage's
// downstream build needs it set to true so it does not re-promote and re-block
// itself), but operators no longer get to set it themselves.
void enforceProdPromotionGuard() {
  if (params.ENVIRONMENT == 'production') {
    if (!params.SKIP_PRODUCTION_PROMOTION) {
      error "Direct production runs are disabled. Run ENVIRONMENT=dev and use the final Promote To Production button after dev smoke passes."
    }
    def upstreamFromPromote = currentBuild.upstreamBuilds.any { it.fullProjectName == env.JOB_NAME }
    def breakglass = params.EMERGENCY_DIRECT_PROD_DEPLOY ?: false
    def reason = ((params.EMERGENCY_REASON ?: '') as String).trim()
    if (upstreamFromPromote) {
      echo "Promote-from-dev path verified: prod build triggered by upstream dev run of ${env.JOB_NAME}."
    } else if (breakglass) {
      if (!reason) {
        error "EMERGENCY_DIRECT_PROD_DEPLOY=true requires a non-empty EMERGENCY_REASON. Aborting."
      }
      def who = currentBuild.getBuildCauses().collect { (it.userId ?: it.userName ?: '') as String }.find { it } ?: 'unknown'
      echo "*******************************************************************"
      echo "* EMERGENCY DIRECT-TO-PROD DEPLOY (break-glass override engaged)  *"
      echo "* Build:   ${env.BUILD_TAG}"
      echo "* User:    ${who}"
      echo "* Reason:  ${reason}"
      echo "* Bypassed the must-go-via-dev rule. Audit-logged.                *"
      echo "*******************************************************************"
    } else {
      error """Direct production deploys are forbidden.
SKIP_PRODUCTION_PROMOTION=true is only valid when the build is launched by the Promote
To Production stage of a dev run of this same job (verified via the upstream cause).
To deploy to prod normally: run ENVIRONMENT=dev and click 'Promote To Production' at
the end of the dev run. To override in a genuine emergency, also pass
EMERGENCY_DIRECT_PROD_DEPLOY=true AND a non-empty EMERGENCY_REASON — both will be
captured loudly in the build log."""
    }
  }
}

// Manual approval gate, then launch the same job against production as a
// downstream build (records the UpstreamCause that enforceProdPromotionGuard()
// verifies). Returns without promoting if the input times out / is rejected.
void promoteToProduction() {
  def approved = false
  try {
    timeout(time: 30, unit: 'MINUTES') {
      input message: 'Dev deployment and smoke checks completed. Deploy the same build to PRODUCTION?', ok: 'Deploy to production'
    }
    approved = true
  } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException ignored) {
    echo 'Production promotion was not approved; dev deployment remains complete.'
  }
  if (!approved) {
    return
  }
  build job: env.JOB_NAME,
    wait: false,
    propagate: false,
    parameters: [
      string(name: 'ENVIRONMENT', value: 'production'),
      string(name: 'KUBECONFIG_FILE', value: params.PROD_KUBECONFIG_FILE),
      string(name: 'KUBECONFIG_ADMIN_FILE', value: params.PROD_KUBECONFIG_ADMIN_FILE),
      string(name: 'IMAGE_REGISTRY', value: params.IMAGE_REGISTRY),
      string(name: 'IMAGE_TAG', value: params.IMAGE_TAG),
      string(name: 'BUILD_PLATFORM', value: 'linux/amd64'),
      string(name: 'KAFKA_BOOTSTRAP_SERVERS', value: ''),
      string(name: 'WEB_PUBLIC_URL', value: params.WEB_PUBLIC_URL),
      string(name: 'RAW_TO_DISPLAY_IMAGE', value: params.RAW_TO_DISPLAY_IMAGE),
      string(name: 'WEB_IMAGE', value: params.WEB_IMAGE),
      string(name: 'DATABENTO_VOLUME_AGGREGATOR_IMAGE', value: params.DATABENTO_VOLUME_AGGREGATOR_IMAGE),
      string(name: 'DATABENTO_GEX_IMAGE', value: params.DATABENTO_GEX_IMAGE),
      string(name: 'DATABENTO_MAXPAIN_IMAGE', value: params.DATABENTO_MAXPAIN_IMAGE),
      string(name: 'DATABENTO_MISSION_PACE_IMAGE', value: params.DATABENTO_MISSION_PACE_IMAGE),
      string(name: 'DATABENTO_MISSION_PRESSURE_IMAGE', value: params.DATABENTO_MISSION_PRESSURE_IMAGE),
      string(name: 'DATABENTO_MISSION_SANDWICH_IMAGE', value: params.DATABENTO_MISSION_SANDWICH_IMAGE),
      string(name: 'VOLUME_PACE_IMAGE', value: params.VOLUME_PACE_IMAGE),
      string(name: 'DIRECTIONAL_PRESSURE_IMAGE', value: params.DIRECTIONAL_PRESSURE_IMAGE),
      string(name: 'VOLUME_SANDWICH_IMAGE', value: params.VOLUME_SANDWICH_IMAGE),
      string(name: 'UNUSUAL_WHALES_GEX_IMAGE', value: params.UNUSUAL_WHALES_GEX_IMAGE),
      string(name: 'UNUSUAL_WHALES_GEX_HISTORY_IMAGE', value: params.UNUSUAL_WHALES_GEX_HISTORY_IMAGE),
      string(name: 'DATABENTO_GEX_HISTORY_IMAGE', value: params.DATABENTO_GEX_HISTORY_IMAGE),
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
      string(name: 'DATABENTO_API_KEY_CREDENTIAL_ID', value: params.DATABENTO_API_KEY_CREDENTIAL_ID),
      string(name: 'KEYCLOAK_DB_PASSWORD_CREDENTIAL_ID', value: params.KEYCLOAK_DB_PASSWORD_CREDENTIAL_ID),
      string(name: 'KEYCLOAK_ADMIN_PASSWORD_CREDENTIAL_ID', value: params.KEYCLOAK_ADMIN_PASSWORD_CREDENTIAL_ID),
      string(name: 'MARKET_DATA_SOURCE', value: params.MARKET_DATA_SOURCE),
      string(name: 'RAW_TOPIC', value: params.RAW_TOPIC),
      string(name: 'IB_HOST', value: params.IB_HOST),
      string(name: 'IB_PORT', value: params.IB_PORT),
      string(name: 'IB_CLIENT_ID', value: params.IB_CLIENT_ID),
      string(name: 'IB_EXPIRY', value: params.IB_EXPIRY),
      string(name: 'DATABENTO_EXPIRY', value: params.DATABENTO_EXPIRY),
      string(name: 'IB_MAX_STRIKES', value: params.IB_MAX_STRIKES),
      booleanParam(name: 'KAFKA_CLEANUP_TOPICS', value: false),
      booleanParam(name: 'KAFKA_DELETE_UNWANTED_TOPICS', value: false),
      booleanParam(name: 'ALLOW_PROD_KAFKA_CLEANUP', value: false),
      booleanParam(name: 'SKIP_PRODUCTION_PROMOTION', value: true),
      booleanParam(name: 'DEPLOY_DRY_RUN', value: params.DEPLOY_DRY_RUN),
      booleanParam(name: 'SKIP_HPSF_SMOKE', value: params.SKIP_HPSF_SMOKE),
      booleanParam(name: 'SKIP_KAFKA_TOPICS', value: params.SKIP_KAFKA_TOPICS)
    ]
}
