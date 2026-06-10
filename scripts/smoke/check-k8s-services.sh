#!/usr/bin/env bash
set -euo pipefail
: "${KUBECONFIG:?KUBECONFIG is required}"
NAMESPACE="${NAMESPACE:-options-edge}"

check_deployment() {
  local deployment="$1"
  local local_port="$2"
  echo "Checking rollout for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s

  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8080" >/tmp/"$deployment"-port-forward.log 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  sleep 3
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_integration_test() {
  local deployment="options-edge-integration-test"
  local local_port="18082"
  echo "Checking live health for $deployment"
  local pod
  pod="$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name="$deployment" -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "$NAMESPACE" port-forward "pod/$pod" "${local_port}:8080" >/tmp/"$deployment"-port-forward.log 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  sleep 3
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  echo "Running final UI/data-path synthetic check"
  curl -fsS "http://127.0.0.1:${local_port}/api/check/once"
  echo
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_deployment raw-to-display-service 18080
check_deployment volume-pace-service 18081
check_deployment directional-pressure-service 18084
check_deployment volume-sandwich-service 18083
check_deployment raw-postgres-writer 18085
check_deployment pressure-postgres-writer 18086
check_integration_test
