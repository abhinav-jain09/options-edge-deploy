#!/usr/bin/env bash
# ES Footprint deployment contingency (G-R8), enforced on the manifests rather than trusted.
#
# Two facts about this subsystem are only true if two SEPARATE manifests agree, and nothing else
# in CI notices when they stop agreeing:
#
#   1. The record ceiling is ONE value. The gateway drops any record above its own ceiling, so a
#      producer allowed to emit larger records loses those records SILENTLY at the relay — the
#      producer is happy, the topic has the bytes, and the page never sees them.
#   2. G-R8's memory inequality. The relay's extra working set lives OUTSIDE the heap, so a
#      container whose limit equals its -Xmx is OOM-killed on native growth the heap never sees.
#      The rule is limit - Xmx >= 512 MiB, and it must hold wherever the relay is ENABLED.
#
# Both are checked only where the relay is actually on: a manifest with the flag off is not
# holding anything up, and demanding the headroom there would be noise.
set -euo pipefail
cd "$(dirname "$0")/../.."

fail() { printf 'FOOTPRINT CONTINGENCY: %s\n' "$*" >&2; exit 1; }

# The value of an env var in a manifest: the `value:` line that FOLLOWS its `- name:` line.
# The value of an env var: the `value:` line that FOLLOWS its `- name:` line. Taking $2 alone
# truncates a value with spaces to its first word — which is how "-Xms256m -Xmx1536m" read as
# "-Xms256m" and this check reported a missing -Xmx that was right there.
env_value() {
    awk -v want="$2" '
        $1 == "-" && $2 == "name:" { name = $3; next }
        $1 == "value:" && name == want {
            sub(/^ *value: */, ""); gsub(/^"|"$/, ""); print; exit
        }
    ' "$1"
}

# How many ACTIVE entries declare this name. Two entries make the manifest ambiguous, and which
# one Kubernetes honours is not something this guard should be guessing at.
env_count() {
    awk -v want="$2" '
        $1 == "-" && $2 == "name:" && $3 == want { n++ }
        END { print n + 0 }
    ' "$1"
}

mib() { printf '%s' "$1" | awk '{ if ($0 ~ /Mi$/) { sub(/Mi$/, ""); print $0 } else if ($0 ~ /Gi$/) { sub(/Gi$/, ""); print $0 * 1024 } else { print "" } }'; }

GW=k8s/es4/services/es-feed-gateway.yaml
PROD=k8s/es4/services/es-cvd.yaml
[ -f "$GW" ] || fail "$GW is missing"
[ -f "$PROD" ] || fail "$PROD is missing"

# The exemption must be a DECISION, not a parse failure. Anything other than exactly one entry
# holding exactly `true` or `false` is refused: a missing, malformed or duplicated flag would
# otherwise exempt the manifest from every check below, and a pair ordered false-then-true would
# deploy an ENABLED relay past a guard that read the first value and stood down.
count=$(env_count "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
[ "$count" = "1" ] \
    || fail "$GW declares GATEWAY_ES_FOOTPRINT_ENABLED $count times; exactly one entry decides whether the relay is on"
enabled=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
case "$enabled" in
    true) ;;
    false)
        printf 'footprint relay is disabled in %s: nothing to check\n' "$GW"
        exit 0
        ;;
    *) fail "GATEWAY_ES_FOOTPRINT_ENABLED is '$enabled' in $GW; it must be exactly true or false" ;;
esac

# ---- (1) one ceiling, both sides -------------------------------------------------------------
gw_bytes=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
pr_bytes=$(env_value "$PROD" FOOTPRINT_MAX_RECORD_BYTES)
[ -n "$gw_bytes" ] || fail "the relay is enabled but $GW sets no GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES"
[ -n "$pr_bytes" ] || fail "the relay is enabled but $PROD sets no FOOTPRINT_MAX_RECORD_BYTES"
[ "$gw_bytes" = "$pr_bytes" ] \
    || fail "the record ceiling differs: gateway $gw_bytes vs producer $pr_bytes. A producer allowed to emit larger records loses them silently at the relay"

# ---- (2) G-R8: limit - Xmx >= 512 MiB ---------------------------------------------------------
limit_raw=$(awk '/^ *limits:/ { inlim = 1; next } inlim && /memory:/ { v = $2; gsub(/"/, "", v); print v; exit }' "$GW")
[ -n "$limit_raw" ] || fail "no memory limit found in $GW"
limit=$(mib "$limit_raw")
[ -n "$limit" ] || fail "memory limit '$limit_raw' is neither Mi nor Gi; this check cannot compare it"

xmx_raw=$(env_value "$GW" JAVA_TOOL_OPTIONS)
xmx=$(printf '%s' "$xmx_raw" | sed -n 's/.*-Xmx\([0-9]*\)m.*/\1/p')
[ -n "$xmx" ] || fail "no -Xmx found in JAVA_TOOL_OPTIONS ('$xmx_raw')"

headroom=$(( limit - xmx ))
[ "$headroom" -ge 512 ] \
    || fail "G-R8 native headroom is ${headroom} MiB (limit ${limit} MiB - Xmx ${xmx} MiB); the relay's working set is OUTSIDE the heap and needs >= 512 MiB"

printf 'footprint contingency OK: ceiling %s on both sides; headroom %s MiB (limit %s, Xmx %s)\n' \
    "$gw_bytes" "$headroom" "$limit" "$xmx"
