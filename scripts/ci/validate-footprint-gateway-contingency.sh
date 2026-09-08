#!/usr/bin/env bash
# ES Footprint deployment contingency (G-R8), enforced on the manifests rather than trusted.
#
# Two facts about this subsystem are only true if two SEPARATE manifests agree, and nothing else
# in CI or on the deploy path noticed when they stopped agreeing:
#
#   1. The record ceiling is ONE value. The gateway drops any record above its own ceiling, so a
#      producer allowed to emit larger records loses those records SILENTLY at the relay — the
#      producer is happy, the topic has the bytes, and the page never sees them.
#   2. G-R8's memory inequality. The relay's extra working set lives OUTSIDE the heap, so a
#      container whose limit equals its -Xmx is OOM-killed on native growth the heap never sees.
#      The rule is limit - Xmx >= 512 MiB, and it must hold wherever the relay is ENABLED.
#
# Both are read out of the SELECTED Deployment and container with yq, not grepped out of the file:
# a text scan would also match an init container, a sidecar, a second document, or a commented
# example, and the first version of this guard took whichever `limits:` block came first — which
# would have validated a sidecar's memory while the gateway itself ran without headroom.
# yq is already required on this same Jenkins path by validate-dealer-ledger-fire-quality.sh.
#
# Refusals carry a STABLE identifier (E_*). The mutation suite asserts those, so the prose can be
# reworded without breaking the suite, and a case can never be "killed" by an unrelated failure.
set -euo pipefail
cd "$(dirname "$0")/../.."

command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }

fail() { printf 'FOOTPRINT CONTINGENCY [%s]: %s\n' "$1" "$2" >&2; exit 1; }

GW=k8s/es4/services/es-feed-gateway.yaml
GW_DEPLOYMENT=es-feed-gateway
GW_CONTAINER=feed-gateway
PROD=k8s/es4/services/es-cvd.yaml
PROD_DEPLOYMENT=es-cvd-service
PROD_CONTAINER=es-cvd

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

# The one container, rendered. Everything below reads THIS, never the file.
container() {   # $1 = manifest, $2 = deployment, $3 = container, $4 = output path
    yq "select(.kind == \"Deployment\" and .metadata.name == \"$2\")
        | .spec.template.spec.containers[] | select(.name == \"$3\")" "$1" > "$4"
    [ -s "$4" ] || fail E_CONTAINER "no container '$3' in Deployment '$2' of $1"
    # Two Deployments of the same name, or two containers of the same name, would make every
    # question below ambiguous. yq concatenates them, so count the documents it produced.
    local docs; docs=$(yq 'select(. != null) | .name' "$4" | grep -c . || true)
    [ "$docs" = "1" ] || fail E_CONTAINER "container '$3' in Deployment '$2' of $1 resolved $docs times"
}

# Exactly one env entry of this name, and its value. Emptiness is asserted by COUNT, never by a
# missing value: an entry present with an empty value and an entry absent are different states.
env_count() { yq -r "[.env[] | select(.name == \"$2\")] | length" "$1"; }
env_value() { yq -r "[.env[] | select(.name == \"$2\") | .value] | .[-1] // \"\"" "$1"; }

require_one() {   # $1 = container file, $2 = var, $3 = which manifest (for the message)
    local n; n=$(env_count "$1" "$2")
    [ "$n" = "1" ] || fail E_ENV_COUNT "$3 declares $2 $n times in container $GW_CONTAINER; exactly one entry decides it"
}

mib() {   # a Kubernetes quantity in Mi, or empty when this guard cannot compare it
    printf '%s' "$1" | awk '
        /^[0-9]+Mi$/ { sub(/Mi$/, ""); print; next }
        /^[0-9]+Gi$/ { sub(/Gi$/, ""); print $0 * 1024; next }
        { print "" }'
}

container "$GW"   "$GW_DEPLOYMENT"   "$GW_CONTAINER"   "$tmp/gw"
container "$PROD" "$PROD_DEPLOYMENT" "$PROD_CONTAINER" "$tmp/prod"

# ---- the flag is a DECISION, never a parse failure -------------------------------------------
# Anything other than exactly one entry holding exactly true or false is refused: a missing,
# malformed or duplicated flag would otherwise exempt the manifest from every check below, and a
# pair ordered false-then-true would deploy an ENABLED relay past a guard that stood down.
require_one "$tmp/gw" GATEWAY_ES_FOOTPRINT_ENABLED "$GW"
enabled=$(env_value "$tmp/gw" GATEWAY_ES_FOOTPRINT_ENABLED)
case "$enabled" in
    true) ;;
    false) printf 'footprint relay is disabled in %s: nothing to check\n' "$GW"; exit 0 ;;
    *) fail E_FLAG_VALUE "GATEWAY_ES_FOOTPRINT_ENABLED is '$enabled' in $GW; it must be exactly true or false" ;;
esac

# ---- (1) one ceiling, both sides --------------------------------------------------------------
require_one "$tmp/gw"   GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES "$GW"
require_one "$tmp/prod" FOOTPRINT_MAX_RECORD_BYTES            "$PROD"
gw_bytes=$(env_value "$tmp/gw"   GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
pr_bytes=$(env_value "$tmp/prod" FOOTPRINT_MAX_RECORD_BYTES)
[ "$gw_bytes" = "$pr_bytes" ] \
    || fail E_CEILING_MISMATCH "the record ceiling differs: gateway $gw_bytes vs producer $pr_bytes; a producer allowed to emit larger records loses them silently at the relay"

# ---- (2) G-R8: limit - Xmx >= 512 MiB ---------------------------------------------------------
limit_raw=$(yq -r '.resources.limits.memory // ""' "$tmp/gw")
[ -n "$limit_raw" ] || fail E_NO_LIMIT "container $GW_CONTAINER in $GW declares no resources.limits.memory"
limit=$(mib "$limit_raw")
[ -n "$limit" ] || fail E_LIMIT_UNIT "memory limit '$limit_raw' is neither Mi nor Gi; this guard cannot compare it"

require_one "$tmp/gw" JAVA_TOOL_OPTIONS "$GW"
xmx_raw=$(env_value "$tmp/gw" JAVA_TOOL_OPTIONS)
xmx=$(printf '%s' "$xmx_raw" | sed -n 's/.*-Xmx\([0-9]*\)m.*/\1/p')
[ -n "$xmx" ] || fail E_NO_XMX "no -Xmx found in JAVA_TOOL_OPTIONS ('$xmx_raw')"

headroom=$(( limit - xmx ))
[ "$headroom" -ge 512 ] \
    || fail E_HEADROOM "native headroom is ${headroom} MiB (limit ${limit} MiB - Xmx ${xmx} MiB); the relay's working set is OUTSIDE the heap and G-R8 requires at least 512 MiB"

printf 'footprint contingency OK: ceiling %s on both sides; headroom %s MiB (limit %s, Xmx %s)\n' \
    "$gw_bytes" "$headroom" "$limit" "$xmx"
