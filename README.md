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

## Jenkins Deployment Flow

The deployment Jenkins job is designed for a dev-first, manual-production flow:

1. Start the job with `ENVIRONMENT=dev` for the normal path.
2. Jenkins renders and deploys the dev Kubernetes overlay automatically.
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
