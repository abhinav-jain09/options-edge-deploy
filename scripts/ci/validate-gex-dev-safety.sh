#!/usr/bin/env bash
# Prevent configuration regressions that can make all dev per-strike GEX disappear.
set -euo pipefail
cd "$(dirname "$0")/../.."

command -v kubectl >/dev/null 2>&1 || {
  echo "FATAL: kubectl is required" >&2
  exit 1
}

rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT
kubectl kustomize k8s/overlays/dev >"$rendered"

env_value() {
  local name="$1"
  awk -v target="$name" '
    $1 == "-" && $2 == "name:" { current=$3; value="" }
    $1 == "value:" && current == target {
      gsub(/"/, "", $2)
      print $2
      exit
    }
  ' "$rendered"
}

assert_value() {
  local name="$1" expected="$2" actual
  actual="$(env_value "$name")"
  if [ "$actual" != "$expected" ]; then
    echo "FAIL: dev databento-gex $name must be '$expected', got '${actual:-unset}'" >&2
    exit 1
  fi
}

assert_value DATABENTO_GEX_DYNAMIC_CARRY_ENABLED true
assert_value DATABENTO_GEX_CARRY_STATIC_FALLBACK_ENABLED true
assert_value DATABENTO_GEX_STALE_AFTER_MS 900000
assert_value GEX_FLOW_READINESS_GATE false

echo "dev GEX safety invariants passed"
