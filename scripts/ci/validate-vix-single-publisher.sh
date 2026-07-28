#!/usr/bin/env bash
# AT-MOST-ONE VIX PUBLISHER ASSERTION (VIX feed separation design rev 4 §7; VFS-R7,
# r1 finding 13 / r2 finding 8; USER-accepted interim ownership model:
# at-most-one-by-declared-config + bounded-gap).
#
# Renders the PRODUCTION overlay and computes two ownership predicates:
#   * SPX-side owner      <=> the options-edge-databento-feed Deployment has
#                             DATABENTO_VIX_PRICE_ENABLED=true OR an AUTO_VIX_MONTHLY
#                             entry inside its DATABENTO_EXTRA_INSTRUMENTS JSON;
#   * standalone owner    <=> the databento-vix-feed Deployment has spec.replicas > 0
#                             AND KAFKA_VIX_PRICE_TOPIC missing-or-equal
#                             underlying.vix.price (missing = the real topic, the code
#                             default).
# FAIL <=> both predicates true. Malformed or duplicated env entries, unparseable
# JSON, an unreadable replicas field, or EITHER Deployment missing from the render
# FAIL CLOSED (Codex round-1 finding 1: a render this assertion cannot fully see is
# never proof of at-most-one). Truthiness and expiry-mode matching mirror the REAL
# feed parser (config.py:49 truthy set incl. "y", case-insensitive;
# config.py:143 expiry_mode strip().upper()) — finding 2.
#
# Every planned rollout state passes: PR-2 default (standalone replicas 0 + SPX
# enabled), shadow (replicas 1 + shadow topic + SPX enabled), cutover-intermediate
# (SPX disabled, standalone 0->1), steady state, rollback (standalone 0 + real topic +
# SPX re-enabled). Each is an explicit case in tests/test_vix_single_publisher.py.
#
# Wired into: (i) PR CI + the service-deploy validation stage via
# scripts/ci/validate-services.sh (Jenkinsfile.service-deploy runs it before apply),
# (ii) the monolith path via scripts/deploy/validate-platform.sh (its stage precedes
# the deploy stage).
#
# Usage:
#   validate-vix-single-publisher.sh                # render k8s/overlays/production
#   validate-vix-single-publisher.sh <render.yaml>  # evaluate a pre-rendered file
#                                                     (the test suite's fixture path)
# Requires: yq + python3 (and kubectl when it renders the overlay itself).
set -euo pipefail
cd "$(dirname "$0")/../.."

command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "FATAL: python3 is required" >&2; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RENDER="${1:-}"
if [ -z "$RENDER" ]; then
  command -v kubectl >/dev/null 2>&1 || { echo "FATAL: kubectl (kustomize) is required to render the production overlay" >&2; exit 1; }
  kubectl kustomize k8s/overlays/production >"$TMP/render.yaml" || { echo "FATAL: k8s/overlays/production does not render" >&2; exit 1; }
  RENDER="$TMP/render.yaml"
fi
[ -f "$RENDER" ] || { echo "FATAL: render file not found: $RENDER" >&2; exit 1; }

# yq handles the multi-doc YAML -> JSON conversion; python3 evaluates the predicates
# (JSON parsing of DATABENTO_EXTRA_INSTRUMENTS must fail closed, which bash cannot do
# robustly). eval-all wraps all docs into one JSON array.
yq ea -o=json '[.]' "$RENDER" >"$TMP/render.json" || { echo "FATAL: render is not parseable YAML" >&2; exit 1; }

python3 - "$TMP/render.json" <<'PY'
import json
import sys

SPX_DEPLOYMENT = "options-edge-databento-feed"
STANDALONE_DEPLOYMENT = "databento-vix-feed"
REAL_TOPIC = "underlying.vix.price"

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    docs = json.load(handle)


def die(msg: str) -> None:
    print(f"FAIL (fail-closed): {msg}", file=sys.stderr)
    sys.exit(1)


def find_deployment(name: str):
    found = [
        d for d in docs
        if isinstance(d, dict)
        and d.get("kind") == "Deployment"
        and (d.get("metadata") or {}).get("name") == name
    ]
    if len(found) > 1:
        die(f"render contains {len(found)} Deployment docs named {name} — ambiguous ownership")
    if not found:
        # FAIL CLOSED: both Deployments are required in the production render. A render
        # missing one is not proof of at-most-one — it means the assertion cannot see
        # the publisher topology it exists to check (a renamed/dropped workload must be
        # a loud failure, never a silent owner=false).
        die(f"required Deployment '{name}' is MISSING from the production render")
    return found[0]


def env_value(deployment, name: str, key: str):
    """Return (present, value) for env var `key`; fail closed on duplicates/valueFrom."""
    spec = (((deployment.get("spec") or {}).get("template") or {}).get("spec") or {})
    containers = list(spec.get("initContainers") or []) + list(spec.get("containers") or [])
    hits = []
    for container in containers:
        for entry in container.get("env") or []:
            if not isinstance(entry, dict):
                die(f"{name}: malformed env entry (not a mapping): {entry!r}")
            if entry.get("name") == key:
                hits.append(entry)
    if len(hits) > 1:
        die(f"{name}: env {key} appears {len(hits)} times — duplicated env entries are ambiguous")
    if not hits:
        return False, None
    entry = hits[0]
    if "valueFrom" in entry or "value" not in entry:
        die(f"{name}: env {key} is not a statically readable literal value")
    value = entry["value"]
    if not isinstance(value, str):
        die(f"{name}: env {key} value is not a string: {value!r}")
    return True, value


# --- SPX-side ownership predicate --------------------------------------------------
# Truthiness mirrors the REAL feed parser (config.py:49): {"1","true","yes","on","y"},
# case-insensitive — the assertion must never call "disabled" what the runtime would
# happily enable (Codex round-1 finding 2).
TRUTHY = {"1", "true", "yes", "on", "y"}

spx = find_deployment(SPX_DEPLOYMENT)
enabled_present, enabled_value = env_value(spx, SPX_DEPLOYMENT, "DATABENTO_VIX_PRICE_ENABLED")
enabled = enabled_present and enabled_value.strip().lower() in TRUTHY

extra_present, extra_value = env_value(spx, SPX_DEPLOYMENT, "DATABENTO_EXTRA_INSTRUMENTS")
has_vix_instrument = False
if extra_present and extra_value.strip():
    try:
        instruments = json.loads(extra_value)
    except (ValueError, TypeError) as exc:
        die(f"{SPX_DEPLOYMENT}: DATABENTO_EXTRA_INSTRUMENTS is unparseable JSON ({exc})")
    if not isinstance(instruments, list):
        die(f"{SPX_DEPLOYMENT}: DATABENTO_EXTRA_INSTRUMENTS JSON is not a list")
    for instrument in instruments:
        if not isinstance(instrument, dict):
            die(f"{SPX_DEPLOYMENT}: DATABENTO_EXTRA_INSTRUMENTS entry is not an object: {instrument!r}")
        # expiry_mode is normalized strip().upper() by the runtime (config.py:143):
        # a lowercase "auto_vix_monthly" subscribes VIX just the same.
        expiry_mode = instrument.get("expiry_mode")
        if isinstance(expiry_mode, str) and expiry_mode.strip().upper() == "AUTO_VIX_MONTHLY":
            has_vix_instrument = True

spx_owner = enabled or has_vix_instrument
spx_detail = (
    f"{SPX_DEPLOYMENT}: DATABENTO_VIX_PRICE_ENABLED="
    f"{enabled_value if enabled_present else '<absent>'}, "
    f"AUTO_VIX_MONTHLY instrument={'yes' if has_vix_instrument else 'no'}"
)

# --- standalone ownership predicate ------------------------------------------------
standalone = find_deployment(STANDALONE_DEPLOYMENT)
replicas = (standalone.get("spec") or {}).get("replicas")
if not isinstance(replicas, int) or isinstance(replicas, bool):
    die(
        f"{STANDALONE_DEPLOYMENT}: spec.replicas is unreadable "
        f"({replicas!r}) — the ownership predicate requires an explicit integer"
    )
topic_present, topic_value = env_value(standalone, STANDALONE_DEPLOYMENT, "KAFKA_VIX_PRICE_TOPIC")
real_topic = (not topic_present) or topic_value.strip() == REAL_TOPIC
standalone_owner = replicas > 0 and real_topic
standalone_detail = (
    f"{STANDALONE_DEPLOYMENT}: replicas={replicas}, KAFKA_VIX_PRICE_TOPIC="
    f"{topic_value if topic_present else '<absent -> ' + REAL_TOPIC + ' (code default)'}"
)

# --- verdict -----------------------------------------------------------------------
print(f"SPX-side owner:   {spx_owner}  ({spx_detail})")
print(f"standalone owner: {standalone_owner}  ({standalone_detail})")
if spx_owner and standalone_owner:
    print(
        "FAIL: BOTH the in-process SPX VIX block and the standalone databento-vix-feed "
        f"would publish the real {REAL_TOPIC} (at-most-one violated — VFS-R7).",
        file=sys.stderr,
    )
    sys.exit(1)
print("OK: at most one declared VIX publisher for underlying.vix.price")
PY
