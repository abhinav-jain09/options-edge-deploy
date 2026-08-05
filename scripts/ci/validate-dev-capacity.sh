#!/usr/bin/env bash
# validate-dev-capacity.sh — the dev CPU budget is load-bearing, so it gets a gate.
#
# The dev node (docker-desktop) has 10 allocatable CPUs; kube-system takes 950m and jenkins 500m,
# leaving 8550m for options-edge. databento-gex-service's dev overlay deliberately reserves 2 full
# CPUs, and without deliberate headroom the rendered fleet exceeds that budget — which is exactly
# how GEX ended up Pending on "Insufficient cpu" on 2026-08-05 with no bad code anywhere.
#
# Three things have to hold together, so all three are asserted:
#   1. every capacity target renders at replicas 0, AND
#   2. is in dev-cleanup's DISABLED_DEV — the overlay alone is undone by `dev-cleanup start`, which
#      scales every deployment outside that list back to 1 (this omission already undid the fix once)
#   3. the rendered fleet fits, counting replicas, AND leaves room for the largest RollingUpdate
#      surge pod — a rollout whose surge cannot schedule deadlocks in service-deploy.sh's wait.
#
# Uses yq (provisioned by the deploy-validation workflow); deliberately no Python YAML dependency.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

BUDGET_MCPU=8550
CAPACITY_TARGETS=(dealer-ledger-service unified-sr-service)
CLEANUP=scripts/ops/dev-cleanup.sh
RENDER=$(mktemp); trap 'rm -f "$RENDER"' EXIT

RECREATE_PATCH=k8s/overlays/dev/dev-recreate-strategy-patch.yaml
recreate_targets=$(yq eval-all 'select(.spec.strategy.type=="Recreate") | .metadata.name' "$RECREATE_PATCH" 2>/dev/null || true)

echo "=== validate-dev-capacity: dev fleet must fit ${BUDGET_MCPU}m ==="
kubectl kustomize k8s/overlays/dev > "$RENDER"

# yq turns the multi-doc render into ONE json array; python's STDLIB json does the arithmetic.
# Deliberately no PyYAML — the deploy-validation workflow provisions kubectl and yq only.
yq -o=json eval-all '[select(.kind=="Deployment")]' "$RENDER" > "$RENDER.json"

report=$(python3 - "$RENDER.json" "$BUDGET_MCPU" "${CAPACITY_TARGETS[@]}" <<'PYEOF'
import json, sys

path, budget = sys.argv[1], int(sys.argv[2])
targets = sys.argv[3:]
deployments = json.load(open(path))

def mcpu(dep):
    """CPU millicores for ONE pod of this deployment, summed over its containers."""
    total = 0
    for c in dep["spec"]["template"]["spec"]["containers"]:
        v = ((c.get("resources") or {}).get("requests") or {}).get("cpu")
        if not v:
            continue
        v = str(v)
        total += int(v[:-1]) if v.endswith("m") else int(float(v) * 1000)
    return total

by_name = {d["metadata"]["name"]: d for d in deployments}

for name in targets:
    dep = by_name.get(name)
    if dep is None:
        print(f"FAIL|capacity target '{name}' is not in the rendered dev overlay at all.")
        sys.exit(0)
    reps = dep["spec"].get("replicas", 1)
    if reps != 0:
        print(f"FAIL|'{name}' renders replicas={reps} in the dev overlay, expected 0.|"
              f"It is a capacity target: without it databento-gex-service cannot schedule.")
        sys.exit(0)

# Steady-state demand counts REPLICAS: a 2-replica deployment asks for twice as much.
active = 0
surge, surge_name = 0, "none"
for dep in deployments:
    reps = dep["spec"].get("replicas", 1)
    if reps == 0:
        continue
    pod = mcpu(dep)
    active += pod * reps
    if dep["spec"].get("strategy", {}).get("type", "RollingUpdate") == "RollingUpdate" and pod > surge:
        surge, surge_name = pod, dep["metadata"]["name"]

if active > budget:
    print(f"FAIL|the rendered dev fleet requests {active}m, over the {budget}m budget.|"
          f"Something will sit Pending on 'Insufficient cpu' — most likely databento-gex-service, "
          f"whose 2-CPU reservation is the largest single request.")
    sys.exit(0)

headroom = budget - active
if surge > headroom:
    print(f"FAIL|'{surge_name}' rolls with RollingUpdate and needs {surge}m for its surge pod, but "
          f"only {headroom}m is free.|service-deploy.sh would apply the new template and then wait "
          f"on a rollout that can never schedule. Give it strategy Recreate on dev, or free more CPU.")
    sys.exit(0)

print(f"OK|dev fleet: {active}m of {budget}m ({headroom}m headroom); "
      f"largest RollingUpdate surge {surge}m ({surge_name})")
PYEOF
)
rm -f "$RENDER.json"

if [ "${report%%|*}" = "FAIL" ]; then
  printf '%s\n' "$report" | tr '|' '\n' | sed '1s/^FAIL$/FAIL:/;1!s/^/      /'
  exit 1
fi

# The overlay half is only half the invariant: `dev-cleanup start` scales every deployment outside
# DISABLED_DEV back to 1, which already undid this fix once.
for svc in "${CAPACITY_TARGETS[@]}"; do
  if ! grep -q "DISABLED_DEV='.*\b${svc}\b" "$CLEANUP"; then
    echo "FAIL: '$svc' renders at 0 but is NOT in DISABLED_DEV in $CLEANUP."
    echo "      do_start() scales every deployment outside that list to 1, silently undoing this."
    exit 1
  fi
done

# 5) no Jenkins path may apply a capacity-relevant Deployment straight from k8s/base — that
#    defaults `strategy` back to RollingUpdate and silently reintroduces a surge that cannot fit.
for jf in Jenkinsfile*; do
  [ -f "$jf" ] || continue
  while read -r base_dep; do
    [ -n "$base_dep" ] || continue
    svc=$(yq eval '.metadata.name' "k8s/base/$base_dep" 2>/dev/null || true)
    [ -n "$svc" ] || continue
    if printf '%s' "$recreate_targets" | grep -qx "$svc"; then
      echo "FAIL: $jf applies k8s/base/$base_dep directly, but '$svc' is pinned to Recreate on dev."
      echo "      k8s/base omits strategy, so applying it defaults back to RollingUpdate and its"
      echo "      surge pod stops fitting the headroom. Apply the dev-rendered Deployment instead."
      exit 1
    fi
  done <<< "$(grep -ohE 'kubectl apply -f k8s/base/[a-z0-9-]+-deployment\.yaml' "$jf" 2>/dev/null | sed 's|.*k8s/base/||')"
done

echo "${report#OK|}; capacity targets pinned at 0 in both places; no base-apply bypass of a Recreate-pinned service"
echo "=== validate-dev-capacity: OK ==="
