#!/usr/bin/env bash
# Mutation test for validate-footprint-gateway-contingency.sh: each guard must REFUSE its own
# violation. A guard nobody has watched fail is a guard nobody knows is wired up.
set -euo pipefail
cd "$(dirname "$0")/../.."

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
GUARD=scripts/ci/validate-footprint-gateway-contingency.sh
GW=k8s/es4/services/es-feed-gateway.yaml
PROD=k8s/es4/services/es-cvd.yaml

run_against() {   # $1 = gateway manifest, $2 = producer manifest -> exit code of the guard
    local d="$WORK/case"; rm -rf "$d"; mkdir -p "$d/scripts/ci" "$d/k8s/es4/services"
    cp "$GUARD" "$d/scripts/ci/"
    cp "$1" "$d/k8s/es4/services/es-feed-gateway.yaml"
    cp "$2" "$d/k8s/es4/services/es-cvd.yaml"
    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh >/dev/null 2>&1 ) && echo 0 || echo $?
}

expect() {   # $1 = expected code, $2 = label, $3.. = manifests
    local want="$1" label="$2"; shift 2
    local got; got=$(run_against "$@")
    if [ "$got" != "$want" ]; then
        printf 'MUTATION SURVIVED: %s (guard exited %s, expected %s)\n' "$label" "$got" "$want" >&2
        exit 1
    fi
    printf '  killed: %s\n' "$label"
}

echo "baseline"
expect 0 "the manifests as committed pass" "$GW" "$PROD"

echo "mutations"
# 1. the two ceilings diverge
sed 's/^\( *\)value: "262144"/\1value: "524288"/' "$PROD" > "$WORK/prod-bigger.yaml"
expect 1 "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"

# 2. the producer stops declaring a ceiling at all
grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
expect 1 "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"

# 3. the limit is lowered back to the heap size (the incident this rule exists for)
sed 's/^\( *\)memory: "2560Mi"/\1memory: "1536Mi"/' "$GW" > "$WORK/gw-tight.yaml"
expect 1 "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"

# 4. the heap is raised into the headroom instead
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$GW" > "$WORK/gw-fat-heap.yaml"
expect 1 "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"

# 5. exactly at the boundary is ACCEPTED — the rule is >=, and a guard that refuses its own
#    boundary would force every future budget to overshoot it
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2048m/' "$GW" > "$WORK/gw-boundary.yaml"
expect 0 "exactly 512 MiB of headroom is legal" "$WORK/gw-boundary.yaml" "$PROD"

# 6. relay off: the guard has nothing to hold up and says so. Flip the flag's OWN value line —
#    a blanket s/"true"/"false"/ hits the first unrelated flag in the file and passes for the
#    wrong reason, which is exactly what the first version of this case did.
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "false") }
     { print }' "$GW" > "$WORK/gw-off.yaml"
grep -A1 GATEWAY_ES_FOOTPRINT_ENABLED "$WORK/gw-off.yaml" | grep -q 'value: "false"' \
    || { echo "the disable mutation did not take: the case would pass for the wrong reason" >&2; exit 1; }
expect 0 "relay disabled: not checked" "$WORK/gw-off.yaml" "$PROD"

# 6b. and with the relay off the guard is not merely quiet — it is quiet BECAUSE of the flag:
#     the same manifest with a broken ceiling still passes while disabled, and fails once enabled
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES" { sub(/262144/, "999999") }
     { print }' "$WORK/gw-off.yaml" > "$WORK/gw-off-broken.yaml"
expect 0 "relay disabled hides a ceiling mismatch (by design)" "$WORK/gw-off-broken.yaml" "$PROD"
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/false/, "true") }
     { print }' "$WORK/gw-off-broken.yaml" > "$WORK/gw-on-broken.yaml"
expect 1 "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"

echo "every guard refused its own violation"
