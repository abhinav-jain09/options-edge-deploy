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

## Environment Boundary

The local Mac is the dev environment. The remote machine
`192.168.100.252` is the production environment.

Do not treat a remote `APP_PROFILE=dev`, image tag `:dev`, or Jenkins
parameter default as permission to change local dev behavior for a production
issue. If the problem is observed on `192.168.100.252`, scope the fix,
validation, and deployment path as production/remote work.

## Local Mac Dev Invariants

The normal dev deployment runs on the local Mac, not on the remote production server.

Do not change these defaults without intentionally migrating Jenkins and updating the guard script in the same tested change:

- Jenkins URL: `http://localhost:8085`
- Dev Kubernetes: local Docker Desktop Kubernetes
- Dev Jenkins kubeconfig: `/var/jenkins_home/config/jenkins-deployer.kubeconfig`
- Dev Jenkins admin kubeconfig: `/var/jenkins_home/config/kubeconfig`
- Dev image registry: `host.docker.internal:5001`
- Dev Kafka: `host.docker.internal:9092`
- Dev Kafka topic prefix: empty/unprefixed
- Dev OptionsEdge web smoke URL inside Jenkins: `http://localhost:8094` (k8s Service `options-edge-web`, LoadBalancer bound on the Mac's localhost by docker-desktop ServiceLB)
- Dev OptionsEdge web app from the Mac browser: `http://localhost:8094` (the legacy `:8090` Docker container has been removed)

## Remote Production Invariants

The remote server values are production values and must not become local dev
defaults. Changes for `192.168.100.252` must be treated as production changes:

- Production registry: `192.168.100.252:5000`
- Production Kafka: `192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096`
- Production Kafka topic prefix: empty/unprefixed
- Production web app: `http://192.168.100.252:8094` (k8s Service `options-edge-web`, LoadBalancer on prod cluster). Public via cloudflared tunnel: `https://fullfunding.nl`.
- Production/remote kubeconfig path: `/home/options-edge/config/...`

Build `#264` failed because the Jenkinsfile drifted back to `/home/options-edge/config/kubeconfig`, which does not exist in local Jenkins. The `scripts/jenkins/enforce-local-dev-defaults.sh` rule now runs in the Jenkins `Validate` stage and blocks this kind of drift before any deploy, Kafka topic, or smoke-test step runs.

## Kafka Topic Namespace

Kafka topic namespace changes must follow the environment boundary above.
Local Mac dev and remote production both use unprefixed topic names such as
`options.databento.raw`. They are separated by Kafka bootstrap servers, not by a
topic prefix: local dev uses `host.docker.internal:9092`; production uses
`192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096`. Do not fix a
remote production topic issue by changing the local Mac dev namespace.

## Jenkins Deployment Flow

The deployment Jenkins job is designed for a dev-first, manual-production flow:

1. Start the job with `ENVIRONMENT=dev` for the normal path.
2. Jenkins renders and deploys the dev Kubernetes overlay automatically. Dev deployment must not require a manual approval button.
3. Jenkins runs the smoke checks against the dev deployment.
4. The final `Promote To Production` stage pauses with a manual `Deploy to production` button.
5. Pressing that button starts a separate production deployment build with the same image and runtime parameters.

The production promotion build sets `ENVIRONMENT=production` and `SKIP_PRODUCTION_PROMOTION=true`, so it deploys production directly after the normal preflight checks and does not create another promotion loop.

Direct production runs are blocked. Production must be promoted from a successful dev deployment.

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
17. `Promote To Production`, the only production manual button

Direct `ENVIRONMENT=production` runs are blocked. Production deployment must start from a successful dev run through the final `Promote To Production` button, so production approval never appears before dev deployment.
