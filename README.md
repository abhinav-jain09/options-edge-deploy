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
