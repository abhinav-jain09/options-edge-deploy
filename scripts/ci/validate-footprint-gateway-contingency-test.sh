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

OUT="$WORK/out"

run_against() {   # $1 = gateway manifest, $2 = producer manifest -> exit code; output in $OUT
    local d="$WORK/case"; rm -rf "$d"; mkdir -p "$d/scripts/ci" "$d/k8s/es4/services"
    cp "$GUARD" "$d/scripts/ci/"
    cp "$1" "$d/k8s/es4/services/es-feed-gateway.yaml"
    cp "$2" "$d/k8s/es4/services/es-cvd.yaml"
    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh ) > "$OUT" 2>&1 && echo 0 || echo $?
}

# A refusal is only a kill if it is THIS refusal. Asserting the exit code alone lets an unrelated
# failure — a typo in the guard, a missing file, a parse error — count as every mutation being
# caught, which is the failure mode a mutation suite exists to rule out.
expect() {   # $1 = expected code, $2 = expected message substring ("" when passing), $3 = label, $4.. = manifests
    local want="$1" msg="$2" label="$3"; shift 3
    local got; got=$(run_against "$@")
    if [ "$got" != "$want" ]; then
        printf 'MUTATION SURVIVED: %s (guard exited %s, expected %s)\n' "$label" "$got" "$want" >&2
        sed 's/^/    | /' "$OUT" >&2
        exit 1
    fi
    if [ -n "$msg" ] && ! grep -qF -- "$msg" "$OUT"; then
        printf 'WRONG REASON: %s exited %s but not for the reason under test\n' "$label" "$got" >&2
        printf '    expected to contain: %s\n' "$msg" >&2
        sed 's/^/    | /' "$OUT" >&2
        exit 1
    fi
    printf '  killed: %s\n' "$label"
}

echo "baseline"
expect 0 "footprint contingency OK" "the manifests as committed pass" "$GW" "$PROD"

echo "mutations"
# 1. the two ceilings diverge
sed 's/^\( *\)value: "262144"/\1value: "524288"/' "$PROD" > "$WORK/prod-bigger.yaml"
expect 1 "the record ceiling differs" "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"

# 2. the producer stops declaring a ceiling at all
grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
expect 1 "sets no FOOTPRINT_MAX_RECORD_BYTES" "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"

# 3. the limit is lowered back to the heap size (the incident this rule exists for)
sed 's/^\( *\)memory: "2560Mi"/\1memory: "1536Mi"/' "$GW" > "$WORK/gw-tight.yaml"
expect 1 "G-R8 native headroom is 0 MiB" "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"

# 4. the heap is raised into the headroom instead
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$GW" > "$WORK/gw-fat-heap.yaml"
expect 1 "G-R8 native headroom is 384 MiB" "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"

# 5. exactly at the boundary is ACCEPTED — the rule is >=, and a guard that refuses its own
#    boundary would force every future budget to overshoot it
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2048m/' "$GW" > "$WORK/gw-boundary.yaml"
expect 0 "headroom 512 MiB" "exactly 512 MiB of headroom is legal" "$WORK/gw-boundary.yaml" "$PROD"

# 6. relay off: the guard has nothing to hold up and says so. Flip the flag's OWN value line —
#    a blanket s/"true"/"false"/ hits the first unrelated flag in the file and passes for the
#    wrong reason, which is exactly what the first version of this case did.
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "false") }
     { print }' "$GW" > "$WORK/gw-off.yaml"
grep -A1 GATEWAY_ES_FOOTPRINT_ENABLED "$WORK/gw-off.yaml" | grep -q 'value: "false"' \
    || { echo "the disable mutation did not take: the case would pass for the wrong reason" >&2; exit 1; }
expect 0 "footprint relay is disabled" "relay disabled: not checked" "$WORK/gw-off.yaml" "$PROD"

# 6b. and with the relay off the guard is not merely quiet — it is quiet BECAUSE of the flag:
#     the same manifest with a broken ceiling still passes while disabled, and fails once enabled
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES" { sub(/262144/, "999999") }
     { print }' "$WORK/gw-off.yaml" > "$WORK/gw-off-broken.yaml"
expect 0 "footprint relay is disabled" "relay disabled hides a ceiling mismatch (by design)" "$WORK/gw-off-broken.yaml" "$PROD"
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/false/, "true") }
     { print }' "$WORK/gw-off-broken.yaml" > "$WORK/gw-on-broken.yaml"
expect 1 "the record ceiling differs" "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"

# ---- the enablement flag itself must be a decision, never a parse failure -------------------
# Each of these would, before the fix, have EXEMPTED the manifest from every check above.
awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" { skip = 2 }
     skip > 0 { skip--; next } { print }' "$GW" > "$WORK/gw-noflag.yaml"
expect 1 "declares GATEWAY_ES_FOOTPRINT_ENABLED 0 times" "the flag is missing entirely" "$WORK/gw-noflag.yaml" "$PROD"

awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "TRUE") }
     { print }' "$GW" > "$WORK/gw-badflag.yaml"
expect 1 "it must be exactly true or false" "the flag is not a canonical boolean" "$WORK/gw-badflag.yaml" "$PROD"

# false FIRST, then true: Kubernetes takes the last, the old guard read the first and stood down
awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" {
         print "        - name: GATEWAY_ES_FOOTPRINT_ENABLED"; print "          value: \"false\"" }
     { print }' "$GW" > "$WORK/gw-dupflag.yaml"
expect 1 "declares GATEWAY_ES_FOOTPRINT_ENABLED 2 times" "the flag is declared twice" "$WORK/gw-dupflag.yaml" "$PROD"

echo "every guard refused its own violation, for its own reason"
