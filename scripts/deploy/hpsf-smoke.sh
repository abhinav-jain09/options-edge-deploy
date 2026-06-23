#!/usr/bin/env bash
# HPSF Smoke stage shell body (extracted from Jenkinsfile to keep the
# declarative pipeline's single compiled CPS method under the JVM 64KB
# per-method bytecode limit; mirrors the apply.sh / resolve-images.sh
# extraction pattern). Invoked via `sh 'bash -x scripts/deploy/hpsf-smoke.sh'`;
# relies on the environment exported by the pipeline (KUBECONFIG, BUILD_NUMBER,
# ENVIRONMENT, etc.). Gated by the stage `when` (skipped on dry-run / when
# SKIP_HPSF_SMOKE).
set -euo pipefail
export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
# Single source of truth: derive bootstrap + RF/minISR from the rendered
# per-environment configmap (k8s/overlays/${ENVIRONMENT}).
. scripts/kafka/load-kafka-settings.sh
export TOPIC_PREFIX
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
kubectl -n "$NAMESPACE" rollout status deployment/hpsf-stage-b-service --timeout=600s || echo "WARN: hpsf-stage-b rollout status slow (old replica likely stuck terminating on dev); runtime check below validates the new pod"
scripts/smoke/check-hpsf-stage-b-runtime.sh
