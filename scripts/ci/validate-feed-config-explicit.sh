#!/usr/bin/env bash
# validate-feed-config-explicit.sh — DBP-R4 configuration-drift gate.
#
# THE BUG THIS EXISTS TO PREVENT: on 2026-07-24 `es-feed` mounted only its API-key Secret, so every
# tuning knob silently fell back to the feed library's CODE DEFAULTS — including
# DATABENTO_USE_LIVE_REPLAY=true and DATABENTO_PUBLISH_INTERVAL_MS=250. The pod then requested a
# multi-hour statistics replay on every start and was killed 8 times by its own liveness probe,
# abandoning a Databento session mid-replay each time.
#
# WHAT IT CHECKS: for every Deployment manifest running the feed image, the FULLY RENDERED
# environment must set each mandatory key explicitly — resolving `envFrom` sources in order and then
# `env` (which wins), exactly as Kubernetes does. "The value exists in some ConfigMap" is not
# enough: it counts only if the pod actually mounts that ConfigMap.
#
# SCOPE: es-feed deployments are ENFORCED. Other feed deployments (the SPX feed) carry pre-existing
# gaps of the same kind — real and worth seeing, but not introduced by this requirement — so they are
# REPORTED as warnings. A gate that is expected to be red teaches people to ignore it.
set -euo pipefail
cd "$(dirname "$0")/../.."

python3 - <<'PY'
import sys, yaml, pathlib

IMAGE_SUBSTR = "options-edge-databento-feed"

# Names verified against options-edge-databento-feed/src/options_edge_databento_feed/config.py.
MANDATORY = [
    "DATABENTO_USE_LIVE_REPLAY",
    "DATABENTO_REPLAY_START_MINUTES",
    "DATABENTO_STATISTICS_REPLAY_LOCAL_TIME",
    "DATABENTO_STATISTICS_REPLAY_TIMEZONE",
    "DATABENTO_STATISTICS_REPLAY_MAX_LOOKBACK_HOURS",
    "DATABENTO_ENABLE_STATISTICS",
    "DATABENTO_ENABLE_CBBO",
    "DATABENTO_ENABLE_TCBBO",
    "DATABENTO_ENABLE_TRADES",
    "DATABENTO_CBBO_SCHEMA",
    "DATABENTO_TCBBO_SCHEMA",
    "DATABENTO_PUBLISH_INTERVAL_MS",
    "DATABENTO_RECONNECT_INITIAL_SECONDS",
    "DATABENTO_RECONNECT_MAX_SECONDS",
    "DATABENTO_RECONNECT_RESET_SECONDS",
    "DATABENTO_MARKET_HOURS_ENABLED",
    "DATABENTO_FEED_LIVENESS_SESSION",
    "DATABENTO_FEED_LIVENESS_STALE_SECONDS",
    "DATABENTO_FEED_LIVENESS_STARTUP_GRACE_SECONDS",
]

# es-feed-specific fixed values. DBP-R1 fixes replay off; DBP-R3 fixes the cadence (USER decision
# 2026-07-25). Deliberately NOT applied to the SPX feed, which is tuned per environment and has its
# own history: prod and dev both moved to 5000 ms on 2026-07-29 (CPU saturation on prod, the 0DTE
# repartition hot partition on dev), while es-feed keeps 2000 ms and its own ESM-R23 ladder.
REQUIRED_VALUES = {
    "DATABENTO_USE_LIVE_REPLAY": "false",
    "DATABENTO_PUBLISH_INTERVAL_MS": "2000",
}

def docs_of(path):
    try:
        return [d for d in yaml.safe_load_all(path.read_text()) if isinstance(d, dict)]
    except Exception:
        return []

manifests = sorted(pathlib.Path("k8s").rglob("*.yaml"))

# Index every ConfigMap this repo defines. A manifest mounting one we do not define is a failure:
# the gate cannot prove those keys are set.
configmaps = {}
for f in manifests:
    for d in docs_of(f):
        if d.get("kind") == "ConfigMap":
            name = (d.get("metadata") or {}).get("name")
            if name:
                configmaps.setdefault(name, {}).update(
                    {k: str(v) for k, v in (d.get("data") or {}).items()})

failures, warnings, checked, enforced = [], [], 0, 0

for f in manifests:
    for dep in docs_of(f):
        if dep.get("kind") != "Deployment":
            continue
        pod = ((dep.get("spec") or {}).get("template") or {}).get("spec") or {}
        for c in pod.get("containers") or []:
            if IMAGE_SUBSTR not in (c.get("image") or ""):
                continue
            checked += 1
            where = f"{f} ({c.get('name')})"
            strict = ((dep.get("metadata") or {}).get("name") == "es-feed"
                      or c.get("name") == "es-feed")
            if strict:
                enforced += 1
            sink = failures if strict else warnings

            rendered = {}
            for src in c.get("envFrom") or []:
                if "configMapRef" in src:
                    name = src["configMapRef"]["name"]
                    if name not in configmaps:
                        sink.append(f"{where}: mounts configMapRef '{name}' which no manifest under k8s/ defines")
                        continue
                    rendered.update(configmaps[name])
                # secretRef contents are correctly absent from the repo, so a Secret can never be
                # shown to supply a tuning knob. Tuning belongs in a ConfigMap; credentials in the
                # Secret. A knob only a Secret might set is therefore still "not explicit".
            for e in c.get("env") or []:
                # `valueFrom` is an explicit intent even though the literal is not visible here.
                rendered[e["name"]] = str(e["value"]) if "value" in e else "<valueFrom>"

            missing = [k for k in MANDATORY if k not in rendered]
            if missing:
                sink.append(f"{where}: {len(missing)} mandatory key(s) NOT explicitly set -> would inherit a library default: {', '.join(missing)}")
            if strict:
                for k, want in REQUIRED_VALUES.items():
                    got = rendered.get(k)
                    if got is not None and got not in (want, "<valueFrom>"):
                        failures.append(f"{where}: {k}={got!r} but the requirement fixes it at {want!r}")

if checked == 0:
    failures.append("no feed containers were discovered — the image substring is wrong")
if enforced == 0:
    failures.append("no es-feed container was discovered — the gate would enforce nothing")

for w in warnings:
    print("WARN (pre-existing, not enforced by this requirement):", w)
for x in failures:
    print("FAIL:", x)
print(f"checked {checked} feed container(s); {enforced} enforced as es-feed")
sys.exit(1 if failures else 0)
PY
echo "=== validate-feed-config-explicit: OK ==="
