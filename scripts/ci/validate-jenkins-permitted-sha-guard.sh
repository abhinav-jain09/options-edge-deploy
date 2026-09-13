#!/usr/bin/env bash
# Every Jenkins deploy job carries the canonical permitted-commit guard (PERMITTED_SHA +
# PERMITTED_SHA_GUARD_VERSION parameters, scripts/jenkins/permitted-sha-guard.sh in a canonical
# 'Permitted commit guard' stage) BEFORE any stage that builds, publishes, applies, rolls, restarts or
# alters a target — Deployment Permission Rule. Every Jenkinsfile* must be classified in
# scripts/ci/jenkins-permitted-sha-scope.txt. The validator (shared with the gateway, processing and
# web repositories) documents its rules and limits; its mutation suite runs from the tests.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec python3 scripts/jenkins/validate-jenkinsfile-guard.py --root . --manifest scripts/ci/jenkins-permitted-sha-scope.txt "$@"
