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

IMAGE_MAP = {
    ("raw-to-display-service", "raw-to-display"): "RAW_TO_DISPLAY_IMAGE",
    ("options-edge-web", "web"): "WEB_IMAGE",
    ("raw-to-display-databento-service", "raw-to-display"): "RAW_TO_DISPLAY_IMAGE",
    ("options-edge-databento-feed", "databento-feed"): "DATABENTO_FEED_IMAGE",
    ("databento-volume-aggregator", "databento-volume-aggregator"): "DATABENTO_VOLUME_AGGREGATOR_IMAGE",
    ("databento-gex-service", "databento-gex"): "DATABENTO_GEX_IMAGE",
    ("databento-maxpain-service", "databento-maxpain"): "DATABENTO_MAXPAIN_IMAGE",
    ("option-price-behavior-service", "option-price-behavior"): "OPTION_PRICE_BEHAVIOR_IMAGE",
    ("databento-mission-sandwich-service", "databento-mission-sandwich"): "DATABENTO_MISSION_SANDWICH_IMAGE",
    ("volume-pace-databento-service", "volume-pace"): "VOLUME_PACE_IMAGE",
    ("directional-pressure-service", "directional-pressure"): "DIRECTIONAL_PRESSURE_IMAGE",
    ("directional-pressure-databento-service", "directional-pressure"): "DIRECTIONAL_PRESSURE_IMAGE",
    ("databento-gex-history-service", "databento-gex-history"): "DATABENTO_GEX_HISTORY_IMAGE",
    ("raw-postgres-writer", "raw-postgres-writer"): "RAW_POSTGRES_WRITER_IMAGE",
    ("pin-postgres-writer", "pin-postgres-writer"): "PIN_POSTGRES_WRITER_IMAGE",
    ("pressure-postgres-writer", "pressure-postgres-writer"): "PRESSURE_POSTGRES_WRITER_IMAGE",
    ("feed-gateway-service", "feed-gateway"): "FEED_GATEWAY_IMAGE",
    ("hpsf-postgres-writer-service", "hpsf-postgres-writer"): "HPSF_POSTGRES_WRITER_IMAGE",
    ("strike-flow-classifier-databento", "strike-flow-classifier"): "STRIKE_FLOW_CLASSIFIER_IMAGE",
    ("delta-flow-service", "delta-flow"): "DELTA_FLOW_IMAGE",
    ("strike-liquidity-heatmap-service", "strike-liquidity-heatmap"): "STRIKE_LIQUIDITY_HEATMAP_IMAGE",
    ("dealer-ledger-service", "dealer-ledger"): "DEALER_LEDGER_IMAGE",
    ("dealer-ledger-calibration-scorer", "dealer-ledger-calibration-scorer"): "DEALER_LEDGER_CALIBRATION_IMAGE",
    ("dealer-ledger-calibration-accumulator", "dealer-ledger-calibration-accumulator"): "DEALER_LEDGER_CALIBRATION_IMAGE",
    ("spx-mission-control-service", "spx-mission-control"): "SPX_MISSION_CONTROL_IMAGE",
    ("unified-sr-service", "unified-sr"): "UNIFIED_SR_IMAGE",
    ("strike-intelligence-service", "strike-intelligence"): "STRIKE_INTELLIGENCE_IMAGE",
    ("strike-flow-avro-adapter", "strike-flow-avro-adapter"): "STRIKE_FLOW_AVRO_ADAPTER_IMAGE",
    ("gex-delta-redis-writer", "gex-delta-redis-writer"): "GEX_DELTA_REDIS_WRITER_IMAGE",
    ("ibkr-feed-service", "ibkr-feed"): "IBKR_FEED_IMAGE",
}

if deploy_target == "all":
    target_deployments = None
elif deploy_target == "delta-flow-service":
    target_deployments = {"delta-flow-service"}
elif deploy_target == "strike-liquidity-heatmap-service":
    target_deployments = {"strike-liquidity-heatmap-service"}
elif deploy_target == "dealer-ledger-service":
    target_deployments = {"dealer-ledger-service"}
elif deploy_target == "dealer-ledger-calibration":
    target_deployments = {"dealer-ledger-calibration-scorer", "dealer-ledger-calibration-accumulator"}
else:
    print(f"FATAL: unsupported DEPLOY_TARGET={deploy_target}", file=sys.stderr)
    sys.exit(1)

expected = {}
with open(images_env, "r", encoding="utf-8") as handle:
    for line in handle:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, image = line.split("=", 1)
        if image:
            expected[name] = image

def digest_from_image(image: str) -> str:
    if "@sha256:" not in image:
        return ""
    return image.split("@", 1)[1]

def _norm(ref: str) -> str:
    # Compare by DIGEST when both refs are digest-pinned: `repo:tag@sha256:X` and
    # `repo@sha256:X` are the same immutable image. The tag-less form appears on pods
    # pinned via the kustomize `newName+digest` path (repo@digest) while the env file
    # carries repo:tag@digest — so both loops (deployment specs AND running pods) must
    # normalize before comparing, else an identical image reads as a mismatch.
    if "@" in ref:
        repo_tag, digest = ref.split("@", 1)
        repo = repo_tag.rsplit(":", 1)[0] if ":" in repo_tag.split("/")[-1] else repo_tag
        return f"{repo}@{digest}"
    return ref

with open(deployments_path, "r", encoding="utf-8") as handle:
    deployments = json.load(handle)
with open(pods_path, "r", encoding="utf-8") as handle:
    pods = json.load(handle)

errors = []
rows = []
checked = set()
present_deployment_names = {
    item.get("metadata", {}).get("name", "")
    for item in deployments.get("items", [])
}

def expected_image_for(deployment, container):
    var_name = IMAGE_MAP.get((deployment, container), "")
    if not var_name:
        return "", ""
    image = expected.get(var_name, "")
    if not image:
        errors.append(f"deployment/{deployment} container/{container} maps to {var_name}, but {var_name} is empty/missing")
        return var_name, ""
    if "@sha256:" not in image:
        errors.append(f"{var_name} is not digest-pinned: {image}")
    return var_name, image

for item in deployments.get("items", []):
    deployment = item.get("metadata", {}).get("name", "")
    if target_deployments is not None and deployment not in target_deployments:
        continue
    spec = item.get("spec", {}).get("template", {}).get("spec", {})
    containers = list(spec.get("initContainers") or []) + list(spec.get("containers") or [])
    for container in containers:
        name = container.get("name", "")
        image = container.get("image", "")
        mapping_key = (deployment, name)
        if mapping_key not in IMAGE_MAP:
            continue
        checked.add(mapping_key)
        var_name, expected_image = expected_image_for(deployment, name)
        if not expected_image:
            continue
        rows.append((deployment, name, var_name, expected_image, image))
        if _norm(image) != _norm(expected_image):
            errors.append(
                f"deployment/{deployment} container/{name} image mismatch for {var_name}: "
                f"expected {expected_image}, got {image}"
            )

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
        mapping_key = (pod_app, name)
        if mapping_key not in IMAGE_MAP:
            continue
        image = spec_containers.get(name, "")
        var_name, expected_image = expected_image_for(pod_app, name)
        if not expected_image:
            continue
        if _norm(image) != _norm(expected_image):
            errors.append(
                f"pod/{pod} container/{name} spec image mismatch for deployment/{pod_app} {var_name}: "
                f"expected {expected_image}, got {image}"
            )
        spec_digest = digest_from_image(image)
        expected_digest = digest_from_image(expected_image)
        image_id = status.get("imageID", "")
        id_match = re.search(r"sha256:[0-9a-fA-F]{64}", image_id)
        id_digest = id_match.group(0) if id_match else ""
        if id_digest and expected_digest and id_digest != expected_digest and spec_digest == expected_digest:
            print(
                f"WARN: pod/{pod} container/{name} runtime imageID digest {id_digest} "
                f"differs from expected spec digest {expected_digest}; runtime may report a platform manifest digest.",
                file=sys.stderr,
            )

for deployment, container in sorted(IMAGE_MAP):
    if target_deployments is not None and deployment not in target_deployments:
        continue
    if deployment in present_deployment_names and (deployment, container) not in checked:
        errors.append(f"deployment/{deployment} exists but mapped container/{container} was not found")

print("Verified exact deployment image mapping:")
for deployment, container, var_name, expected_image, actual_image in sorted(rows):
    digest = digest_from_image(actual_image) or "UNPINNED"
    print(f"  {deployment}/{container} {var_name} {digest}")
    if actual_image != expected_image:
        print(f"    expected: {expected_image}")
        print(f"    actual:   {actual_image}")

if errors:
    print("FATAL: running image verification failed:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)
    sys.exit(1)
PY
