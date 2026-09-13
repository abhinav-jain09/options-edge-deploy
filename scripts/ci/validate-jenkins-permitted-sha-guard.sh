#!/usr/bin/env bash
# Every Jenkins deploy job carries the permitted-commit guard (PERMITTED_SHA parameter +
# scripts/jenkins/permitted-sha-guard.sh in a 'Permitted commit guard' stage) BEFORE any stage that
# builds, publishes, applies, rolls, restarts or alters a target — Deployment Permission Rule.
# Every Jenkinsfile* must be classified in scripts/ci/jenkins-permitted-sha-scope.txt.
# The checks, and what they cannot prove, are documented in validate_jenkins_permitted_sha_guard.py.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec python3 scripts/ci/validate_jenkins_permitted_sha_guard.py --root . "$@"
