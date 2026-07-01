#!/usr/bin/env bash
set -euo pipefail

images_env="${1:?usage: verify-running-images.sh <options-edge-images.env>}"
namespace="${NAMESPACE:-options-edge}"

[ -f "$images_env" ] || { echo "FATAL: expected images env not found: $images_env" >&2; exit 1; }

deployments_json="$(mktemp)"
pods_json="$(mktemp)"
trap 'rm -f "$deployments_json" "$pods_json"' EXIT

kubectl -n "$namespace" get deployments -o json >"$deployments_json"
kubectl -n "$namespace" get pods -o json >"$pods_json"

python3 - "$images_env" "$deployments_json" "$pods_json" "${DEPLOY_TARGET:-all}" <<'PY'
import json
import re
import sys

images_env, deployments_path, pods_path, deploy_target = sys.argv[1:5]
target_deployments = None
if deploy_target == "delta-flow-service":
    target_deployments = {"delta-flow-service"}

expected = {}
with open(images_env, "r", encoding="utf-8") as handle:
    for line in handle:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, image = line.split("=", 1)
        if image:
            expected[name] = image

expected_digests = {
    image.split("@", 1)[1]
    for image in expected.values()
    if "@sha256:" in image
}
if not expected_digests:
    print("FATAL: expected image env contains no digest-pinned images", file=sys.stderr)
    sys.exit(1)

def digest_from_image(image: str) -> str:
    if "@sha256:" not in image:
        return ""
    return image.split("@", 1)[1]

def interesting(image: str) -> bool:
    return "/options-edge-" in image

with open(deployments_path, "r", encoding="utf-8") as handle:
    deployments = json.load(handle)
with open(pods_path, "r", encoding="utf-8") as handle:
    pods = json.load(handle)

errors = []
rows = []
for item in deployments.get("items", []):
    deployment = item.get("metadata", {}).get("name", "")
    if target_deployments is not None and deployment not in target_deployments:
        continue
    spec = item.get("spec", {}).get("template", {}).get("spec", {})
    containers = list(spec.get("initContainers") or []) + list(spec.get("containers") or [])
    for container in containers:
        name = container.get("name", "")
        image = container.get("image", "")
        if not interesting(image):
            continue
        digest = digest_from_image(image)
        rows.append((deployment, name, digest or "UNPINNED", image))
        if not digest:
            errors.append(f"deployment/{deployment} container/{name} is not digest-pinned: {image}")
        elif digest not in expected_digests:
            errors.append(f"deployment/{deployment} container/{name} digest is not in expected image env: {image}")

for item in pods.get("items", []):
    metadata = item.get("metadata", {})
    labels = metadata.get("labels", {}) or {}
    pod_app = labels.get("app.kubernetes.io/name", "")
    if target_deployments is not None and pod_app not in target_deployments:
        continue
    if metadata.get("deletionTimestamp"):
        continue
    phase = item.get("status", {}).get("phase", "")
    if phase in {"Succeeded", "Failed"}:
        continue
    pod = metadata.get("name", "")
    spec_containers = {
        c.get("name", ""): c.get("image", "")
        for c in item.get("spec", {}).get("containers", []) or []
    }
    status_containers = item.get("status", {}).get("containerStatuses", []) or []
    for status in status_containers:
        name = status.get("name", "")
        image = spec_containers.get(name, "")
        if not interesting(image):
            continue
        spec_digest = digest_from_image(image)
        if not spec_digest:
            errors.append(f"pod/{pod} container/{name} spec image is not digest-pinned: {image}")
        elif spec_digest not in expected_digests:
            errors.append(f"pod/{pod} container/{name} spec digest is not in expected image env: {image}")
        image_id = status.get("imageID", "")
        id_match = re.search(r"sha256:[0-9a-fA-F]{64}", image_id)
        id_digest = id_match.group(0) if id_match else ""
        if id_digest and id_digest not in expected_digests and spec_digest in expected_digests:
            print(
                f"WARN: pod/{pod} container/{name} runtime imageID digest {id_digest} "
                f"differs from expected spec digest {spec_digest}; runtime may report a platform manifest digest.",
                file=sys.stderr,
            )

print("Verified digest-pinned deployment images:")
for deployment, container, digest, image in sorted(rows):
    print(f"  {deployment}/{container} {digest} {image}")

if errors:
    print("FATAL: running image verification failed:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)
    sys.exit(1)
PY
