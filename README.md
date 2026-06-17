# OptionsEdge Deploy

Kubernetes deployment repo for OptionsEdge services.

Owns:
- Kubernetes manifests
- environment overlays
- Kafka topic operations
- smoke-test scripts
- image tag manifests
- deployment Jenkins pipeline

This repo deploys applications after `options-edge-infra` has prepared Docker/Kubernetes on the remote server.

## Local Jenkins Dev Invariants

The normal dev deployment runs from the MacBook Jenkins instance, not from the remote production server.

Do not change these defaults without intentionally migrating Jenkins and updating the guard script in the same tested change:

- Jenkins URL: `http://localhost:8085`
- Dev Kubernetes: local Docker Desktop Kubernetes
- Dev Jenkins kubeconfig: `/var/jenkins_home/config/jenkins-deployer.kubeconfig`
- Dev Jenkins admin kubeconfig: `/var/jenkins_home/config/kubeconfig`
- Dev image registry: `host.docker.internal:5001`
- Dev Kafka: `host.docker.internal:9092`
- Dev OptionsEdge web smoke URL inside Jenkins: `http://host.docker.internal:8090`
- Same dev OptionsEdge web app from the Mac browser: `http://localhost:8090`

The remote server values are production values and must not become the dev defaults:

- Production registry: `192.168.100.252:5000`
- Production web app: `http://192.168.100.252:8090`
- Production/remote kubeconfig path: `/home/options-edge/config/...`

Build `#264` failed because the Jenkinsfile drifted back to `/home/options-edge/config/kubeconfig`, which does not exist in local Jenkins. The `scripts/jenkins/enforce-local-dev-defaults.sh` rule now runs in the Jenkins `Validate` stage and blocks this kind of drift before any deploy, Kafka topic, or smoke-test step runs.

## Kafka Topic Namespace

The dev overlay intentionally leaves `TOPIC_PREFIX` empty. Databento live feed
services publish and consume the shared remote topics such as
`options.databento.raw`, `options.databento.display`, and
`options.databento.strike-flow`. Jenkins topic creation and smoke checks must
validate those same unprefixed topics; do not add a dev-only fallback that
rewrites them to `dev.options.*` unless the live feed and gateway are migrated
at the same time.

## Jenkins Deployment Flow

The deployment Jenkins job is designed for a dev-first, manual-production flow:

1. Start the job with `ENVIRONMENT=dev` for the normal path.
2. Jenkins renders and deploys the dev Kubernetes overlay automatically. Dev deployment must not require a manual approval button.
3. Jenkins runs the smoke checks against the dev deployment.
4. The final `Deploy To Production` stage pauses with a manual `Deploy to production` button.
5. Pressing that button starts a separate production deployment build with the same image and runtime parameters.

The production promotion build sets `ENVIRONMENT=production` and `SKIP_PRODUCTION_PROMOTION=true`, so it deploys production directly after the normal preflight checks and does not create another promotion loop.

Direct production runs are also protected: if a job is started with `ENVIRONMENT=production` manually, Jenkins pauses at `Manual Production Approval` and requires the `Deploy to production` button before applying the production overlay.

Production promotion intentionally disables Kafka cleanup flags in the triggered production build:

- `KAFKA_CLEANUP_TOPICS=false`
- `KAFKA_DELETE_UNWANTED_TOPICS=false`
- `ALLOW_PROD_KAFKA_CLEANUP=false`

If production Kafka cleanup is ever needed, run that as a separate, explicit maintenance operation.

Expected dev stage order:

1. `Validate`
2. `Bootstrap Jenkins Kubernetes Guard`
3. `Render`
4. `Unusual Whales Secret`
5. `Resolve Images`
6. `Image Preflight`
7. Optional Kafka cleanup stages, only when cleanup flags are enabled
8. `Kafka Topics`
9. `Reset HPSF Stage B Internal Topics`
10. `Kafka Internal Topics`
11. `Deploy`
12. Optional `Resume Remote Apps`, only when cleanup flags are enabled
13. Optional `Prometheus Scrapes`, skipped in dev
14. `Verify OptionsEdge Web App`
15. `Smoke`
16. `HPSF Smoke`
17. `Promote To Production`, manual button only

`Manual Production Approval` appears only for direct `ENVIRONMENT=production` runs. It must not block dev.
