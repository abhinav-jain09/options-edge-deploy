#!/usr/bin/env bash
# validate-feed-config-explicit.sh — DBP-R4 configuration-drift gate.
#
# THE BUG THIS EXISTS TO PREVENT: on 2026-07-24 `es-feed` mounted only its API-key Secret, so every
# tuning knob silently fell back to the feed library's CODE DEFAULTS — including
# DATABENTO_USE_LIVE_REPLAY=true and DATABENTO_PUBLISH_INTERVAL_MS=250. The pod then requested a
# multi-hour statistics replay on every start and was killed 8 times by its own liveness probe,
# abandoning a Databento session mid-replay each time.
#
# WHAT IT CHECKS: for every Deployment manifest that runs the options-edge-databento-feed image, the
# FULLY RENDERED environment must set each mandatory key explicitly — resolving `env` (which wins)
# over `envFrom` configMapRef, exactly as Kubernetes does. "The value exists in some configmap" is
# not enough: it only counts if the pod actually mounts that configmap.
#
# Absence is the failure mode, so this gate is about presence. Two keys additionally have a REQUIRED
# VALUE because the requirement fixes them (DBP-R1/R3).
set -euo pipefail
cd "$(dirname "$0")/../.."

FEED_IMAGE_SUBSTR="options-edge-databento-feed"

MANDATORY_KEYS="
DATABENTO_USE_LIVE_REPLAY
DATABENTO_REPLAY_START_MINUTES
DATABENTO_STATISTICS_REPLAY_LOCAL_TIME
DATABENTO_STATISTICS_REPLAY_TIMEZONE
DATABENTO_STATISTICS_REPLAY_MAX_LOOKBACK_HOURS
DATABENTO_ENABLE_STATISTICS
DATABENTO_ENABLE_CBBO
DATABENTO_ENABLE_TCBBO
DATABENTO_ENABLE_TRADES
DATABENTO_CBBO_SCHEMA
DATABENTO_TCBBO_SCHEMA
DATABENTO_PUBLISH_INTERVAL_MS
DATABENTO_RECONNECT_INITIAL_SECONDS
DATABENTO_RECONNECT_MAX_SECONDS
DATABENTO_RECONNECT_RESET_SECONDS
DATABENTO_MARKET_HOURS_ENABLED
DATABENTO_FEED_LIVENESS_SESSION
DATABENTO_FEED_LIVENESS_STALE_SECONDS
DATABENTO_FEED_LIVENESS_STARTUP_GRACE_SECONDS
"

# Deployments that must satisfy the gate, paired with the configmap files whose data may be mounted.
# Kept as an explicit list so a NEW deployment of this image is a deliberate addition here (the
# alternative — globbing — would let a new deployment pass silently, which is the original bug).
TARGETS="
k8s/es4/services/es-feed.yaml|k8s/es4/es4-feed-config.yaml
k8s/es4/prod/es-feed.yaml|k8s/es4/prod/es-feed-config.yaml
"

python3 - "$FEED_IMAGE_SUBSTR" "$MANDATORY_KEYS" "$TARGETS" <<'PY'
import sys, yaml, pathlib
image_substr, keys_blob, targets_blob = sys.argv[1], sys.argv[2], sys.argv[3]
mandatory = [k for k in keys_blob.split() if k]
required_values = {
    # DBP-R1: replay must be off; DBP-R3: cadence matches prod SPX (USER decision 2026-07-25).
    "DATABENTO_USE_LIVE_REPLAY": "false",
    "DATABENTO_PUBLISH_INTERVAL_MS": "2000",
}
failures, checked = [], 0
for line in targets_blob.split():
    if "|" not in line:
        continue
    dep_path, cm_path = line.split("|", 1)
    dep = yaml.safe_load(pathlib.Path(dep_path).read_text())
    cms = {}
    cm_doc = yaml.safe_load(pathlib.Path(cm_path).read_text())
    cms[cm_doc["metadata"]["name"]] = {k: str(v) for k, v in (cm_doc.get("data") or {}).items()}

    spec = dep["spec"]["template"]["spec"]
    for c in spec["containers"]:
        if image_substr not in c.get("image", ""):
            continue
        checked += 1
        # Resolve exactly as Kubernetes does: envFrom sources first (in order), then `env` wins.
        rendered = {}
        for src in c.get("envFrom", []) or []:
            ref = src.get("configMapRef")
            if not ref:
                continue  # secretRef contents are not visible here and carry no tuning knobs
            name = ref["name"]
            if name not in cms:
                failures.append(f"{dep_path}: mounts configMapRef '{name}' but this gate has no file for it — add it to TARGETS")
                continue
            rendered.update(cms[name])
        for e in c.get("env", []) or []:
            if "value" in e:
                rendered[e["name"]] = str(e["value"])
            else:
                rendered[e["name"]] = "<from-ref>"
        missing = [k for k in mandatory if k not in rendered]
        if missing:
            failures.append(f"{dep_path} ({c['name']}): {len(missing)} mandatory key(s) NOT explicitly set -> would inherit a library default: {', '.join(missing)}")
        for k, want in required_values.items():
            got = rendered.get(k)
            if got is not None and got != want:
                failures.append(f"{dep_path} ({c['name']}): {k}={got!r} but the requirement fixes it at {want!r}")
if checked == 0:
    failures.append("no feed containers were checked — TARGETS or the image substring is wrong")
for f in failures:
    print("FAIL:", f)
print(f"checked {checked} feed container(s)")
sys.exit(1 if failures else 0)
PY
echo "=== validate-feed-config-explicit: OK ==="
