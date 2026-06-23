#!/usr/bin/env bash
# Validate stage shell body (extracted from Jenkinsfile to keep the declarative
# pipeline's single compiled CPS method under the JVM 64KB per-method bytecode
# limit; mirrors the apply.sh / resolve-images.sh extraction pattern). Invoked
# via `sh 'bash -x scripts/deploy/validate-platform.sh'`; relies on the
# environment exported by the pipeline (ENVIRONMENT, BUILD_PLATFORM,
# REMOTE_APP_HOME, OE_REMOTE_APP_HOME, JENKINS_WORK_DIR).
set -euo pipefail
scripts/jenkins/enforce-main-branch.sh
scripts/jenkins/enforce-local-dev-defaults.sh
test "$REMOTE_APP_HOME" = "$OE_REMOTE_APP_HOME"
test ! -d /root/options-edge
test ! -d /options-edge
mkdir -p "$JENKINS_WORK_DIR"
test -w "$JENKINS_WORK_DIR"
case "${ENVIRONMENT:-dev}" in
  dev)
    effective_build_platform="${BUILD_PLATFORM:-linux/arm64}"
    ;;
  production)
    effective_build_platform="linux/amd64"
    if [ -n "${BUILD_PLATFORM:-}" ] && [ "$BUILD_PLATFORM" != "linux/amd64" ]; then
      echo "BUILD_PLATFORM=$BUILD_PLATFORM is not allowed for ${ENVIRONMENT}; production Kubernetes nodes are CentOS amd64 and require linux/amd64." >&2
      exit 1
    fi
    ;;
  *)
    echo "Unsupported ENVIRONMENT for BUILD_PLATFORM resolution: ${ENVIRONMENT:-}" >&2
    exit 1
    ;;
esac
case "$effective_build_platform" in
  linux/arm64|linux/amd64) ;;
  *)
    echo "Unsupported BUILD_PLATFORM: $effective_build_platform" >&2
    exit 1
    ;;
esac
printf 'EFFECTIVE_BUILD_PLATFORM=%s\n' "$effective_build_platform" >"$JENKINS_WORK_DIR/options-edge-build.env"
echo "Effective build/deploy image platform: $effective_build_platform"
