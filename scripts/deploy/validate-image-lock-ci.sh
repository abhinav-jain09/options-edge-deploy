#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

bash -n scripts/deploy/apply.sh \
  scripts/deploy/image-lock.sh \
  scripts/deploy/image-preflight.sh \
  scripts/deploy/pin-image.sh \
  scripts/deploy/resolve-images.sh \
  scripts/deploy/verify-running-images.sh

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat >"$tmpdir/good-lock.env" <<'EOF'
OPTIONS_EDGE_IMAGE_LOCK_FORMAT=1
OPTIONS_EDGE_IMAGE_LOCK_SOURCE_REPO=git@example/options-edge-processing.git
OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT=0123456789abcdef0123456789abcdef01234567
OPTIONS_EDGE_IMAGE_LOCK_BUILD_URL=https://jenkins.example/job/options-edge-processing/1/
OPTIONS_EDGE_IMAGE_LOCK_PLATFORM=linux/arm64
OPTIONS_EDGE_IMAGE_LOCK_TAG=dev
DELTA_FLOW_IMAGE=host.docker.internal:5001/options-edge-delta-flow:dev@sha256:1111111111111111111111111111111111111111111111111111111111111111
DELTA_FLOW_IMAGE_GIT_COMMIT=0123456789abcdef0123456789abcdef01234567
DELTA_FLOW_IMAGE_JENKINS_BUILD_URL=https://jenkins.example/job/options-edge-processing/1/
EOF

cat >"$tmpdir/duplicate-lock.env" <<'EOF'
OPTIONS_EDGE_IMAGE_LOCK_FORMAT=1
DELTA_FLOW_IMAGE=repo/options-edge-delta-flow:dev@sha256:1111111111111111111111111111111111111111111111111111111111111111
DELTA_FLOW_IMAGE=repo/options-edge-delta-flow:dev@sha256:2222222222222222222222222222222222222222222222222222222222222222
EOF

cat >"$tmpdir/unknown-lock.env" <<'EOF'
OPTIONS_EDGE_IMAGE_LOCK_FORMAT=1
NOT_A_REAL_IMAGE=repo/options-edge-delta-flow:dev@sha256:1111111111111111111111111111111111111111111111111111111111111111
EOF

cat >"$tmpdir/images.env" <<'EOF'
DELTA_FLOW_IMAGE=old
EOF

DEPLOY_TARGET=delta-flow-service bash -c \
  '. scripts/deploy/image-lock.sh; apply_image_lock "$0" true "$1"' \
  "$tmpdir/good-lock.env" "$tmpdir/images.env" >/tmp/image-lock-good.out

grep -q '@sha256:1111111111111111111111111111111111111111111111111111111111111111' "$tmpdir/images.env"

if DEPLOY_TARGET=delta-flow-service bash -c \
  '. scripts/deploy/image-lock.sh; apply_image_lock "$0" true "$1"' \
  "$tmpdir/duplicate-lock.env" "$tmpdir/images.env" >/tmp/image-lock-duplicate.out 2>&1; then
  echo "FATAL: duplicate image-lock key unexpectedly passed" >&2
  exit 1
fi

if DEPLOY_TARGET=delta-flow-service bash -c \
  '. scripts/deploy/image-lock.sh; apply_image_lock "$0" true "$1"' \
  "$tmpdir/unknown-lock.env" "$tmpdir/images.env" >/tmp/image-lock-unknown.out 2>&1; then
  echo "FATAL: unknown image-lock key unexpectedly passed" >&2
  exit 1
fi

cat >"$tmpdir/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

case "$*" in
  "-n options-edge get deployments -o json")
    cat "$FAKE_DEPLOYMENTS_JSON"
    ;;
  "-n options-edge get pods -o json")
    cat "$FAKE_PODS_JSON"
    ;;
  *)
    command kubectl "$@"
    ;;
esac
EOF
chmod +x "$tmpdir/kubectl"

cat >"$tmpdir/expected.env" <<'EOF'
DELTA_FLOW_IMAGE=host.docker.internal:5001/options-edge-delta-flow:dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
GEX_DELTA_REDIS_WRITER_IMAGE=host.docker.internal:5001/options-edge-gex-delta-redis-writer:dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
EOF

cat >"$tmpdir/swapped.env" <<'EOF'
DELTA_FLOW_IMAGE=host.docker.internal:5001/options-edge-delta-flow:dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
GEX_DELTA_REDIS_WRITER_IMAGE=host.docker.internal:5001/options-edge-gex-delta-redis-writer:dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF

cat >"$tmpdir/deployments.json" <<'EOF'
{
  "items": [
    {
      "metadata": {"name": "delta-flow-service"},
      "spec": {
        "template": {
          "spec": {
            "containers": [
              {
                "name": "delta-flow",
                "image": "host.docker.internal:5001/options-edge-delta-flow:dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              }
            ]
          }
        }
      }
    },
    {
      "metadata": {"name": "gex-delta-redis-writer"},
      "spec": {
        "template": {
          "spec": {
            "containers": [
              {
                "name": "gex-delta-redis-writer",
                "image": "host.docker.internal:5001/options-edge-gex-delta-redis-writer:dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
              }
            ]
          }
        }
      }
    }
  ]
}
EOF

cat >"$tmpdir/pods.json" <<'EOF'
{
  "items": [
    {
      "metadata": {
        "name": "delta-flow-service-abc",
        "labels": {"app.kubernetes.io/name": "delta-flow-service"}
      },
      "spec": {
        "containers": [
          {
            "name": "delta-flow",
            "image": "host.docker.internal:5001/options-edge-delta-flow:dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
          }
        ]
      },
      "status": {
        "phase": "Running",
        "containerStatuses": [
          {
            "name": "delta-flow",
            "imageID": "docker-pullable://host/options-edge-delta-flow@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
          }
        ]
      }
    },
    {
      "metadata": {
        "name": "gex-delta-redis-writer-abc",
        "labels": {"app.kubernetes.io/name": "gex-delta-redis-writer"}
      },
      "spec": {
        "containers": [
          {
            "name": "gex-delta-redis-writer",
            "image": "host.docker.internal:5001/options-edge-gex-delta-redis-writer:dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
          }
        ]
      },
      "status": {
        "phase": "Running",
        "containerStatuses": [
          {
            "name": "gex-delta-redis-writer",
            "imageID": "docker-pullable://host/options-edge-gex-delta-redis-writer@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
          }
        ]
      }
    }
  ]
}
EOF

FAKE_DEPLOYMENTS_JSON="$tmpdir/deployments.json" \
  FAKE_PODS_JSON="$tmpdir/pods.json" \
  PATH="$tmpdir:$PATH" \
  DEPLOY_TARGET=all \
  scripts/deploy/verify-running-images.sh "$tmpdir/expected.env" >/tmp/verify-images-good.out

if FAKE_DEPLOYMENTS_JSON="$tmpdir/deployments.json" \
  FAKE_PODS_JSON="$tmpdir/pods.json" \
  PATH="$tmpdir:$PATH" \
  DEPLOY_TARGET=all \
  scripts/deploy/verify-running-images.sh "$tmpdir/swapped.env" >/tmp/verify-images-swapped.out 2>&1; then
  echo "FATAL: swapped deployment/container image mapping unexpectedly passed" >&2
  exit 1
fi

FAKE_DEPLOYMENTS_JSON="$tmpdir/deployments.json" \
  FAKE_PODS_JSON="$tmpdir/pods.json" \
  PATH="$tmpdir:$PATH" \
  DEPLOY_TARGET=delta-flow-service \
  scripts/deploy/verify-running-images.sh "$tmpdir/expected.env" >/tmp/verify-images-targeted.out

kubectl kustomize k8s/base >/tmp/options-edge-kustomize-base.yaml

echo "deploy image-lock CI validation passed"
