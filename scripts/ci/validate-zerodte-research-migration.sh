#!/usr/bin/env bash
# Every 0DTE research-migration declaration (deploy/zerodte/research-migration/<env>.yaml) is read with the SAME strict YAML subset the Job's
# loader and the attestation port share (scripts/ci/zerodte_attestation.py) and held to its exact schema: the keys, their types, their domains
# (fromVersion 6, toVersion 7, a 64-hex calendarVersion, expectedSessionsAhead 0..400, an operator id). A declaration is a reviewed statement
# of what a migration run may do; a file that only loads is not one.
set -euo pipefail
cd "$(dirname "$0")/../.."
fail=0
for f in deploy/zerodte/research-migration/*.yaml; do
  [ -e "$f" ] || { echo "FAIL: no migration declaration under deploy/zerodte/research-migration/"; exit 1; }
  env="$(basename "$f" .yaml)"
  case "$env" in dev|production) : ;; *) echo "FAIL: $f is not named after an environment (dev|production)"; fail=1; continue ;; esac
  if out="$(python3 scripts/ci/zerodte_attestation.py migration "$f" 2>&1)"; then
    echo "ok   $f ($out)"
  else
    echo "FAIL: $f — $out"; fail=1
  fi
done
[ "$fail" -eq 0 ] && { echo "=== validate-zerodte-research-migration: OK ==="; exit 0; }
echo "=== validate-zerodte-research-migration: FAILED ==="; exit 1
