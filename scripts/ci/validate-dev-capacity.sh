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
#   3. the rendered fleet FITS, counting replicas.
# It deliberately does NOT assert that a rollout surge plus a concurrent operational Job also fits —
# see the note at the end of the Python block for why that invariant is not holdable here. It
# reports those numbers instead, so the tightness is visible rather than implied.
#
# Uses yq (provisioned by the deploy-validation workflow); deliberately no Python YAML dependency.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

BUDGET_MCPU=8550
CAPACITY_TARGETS=(dealer-ledger-service unified-sr-service pressure-postgres-writer)
CLEANUP=scripts/ops/dev-cleanup.sh
RENDER=$(mktemp); trap 'rm -f "$RENDER"' EXIT

echo "=== validate-dev-capacity: dev fleet must fit ${BUDGET_MCPU}m ==="
kubectl kustomize k8s/overlays/dev > "$RENDER"

# yq turns the multi-doc render into ONE json array; python's STDLIB json does the arithmetic.
# Deliberately no PyYAML — the deploy-validation workflow provisions kubectl and yq only.
yq -o=json eval-all '[select(.kind=="Deployment" or .kind=="CronJob")]' "$RENDER" > "$RENDER.json"

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

cronjobs = [d for d in deployments if d["kind"] == "CronJob"]
deployments = [d for d in deployments if d["kind"] == "Deployment"]
by_name = {d["metadata"]["name"]: d for d in deployments}


def job_mcpu(cj):
    """CPU for one pod of a CronJob's job template."""
    spec = cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    total = 0
    for c in spec.get("containers", []):
        v = ((c.get("resources") or {}).get("requests") or {}).get("cpu")
        if not v:
            continue
        v = str(v)
        total += int(v[:-1]) if v.endswith("m") else int(float(v) * 1000)
    return total

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

# A rollout can overlap an operational Job. Every dev CronJob is suspend: true, but they are
# triggered on demand (Jenkinsfile.kafka-cleanup) and the cleanup Job may run for 1800s — long
# enough to sit under a whole rollout. The headroom has to cover BOTH.
job, job_name = 0, "none"
for cj in cronjobs:
    c = job_mcpu(cj)
    if c > job:
        job, job_name = c, cj["metadata"]["name"]

# NOT asserted, deliberately, and the reason is written down rather than left as a green tick.
# A RollingUpdate surge pod and an on-demand Job both compete for the same headroom, and dev also
# carries an unsuspendable-on-demand replay Job asking 500m (k8s/replay/dev). Covering the worst
# case — surge + the largest Job — would need ~750m free, which this 10-CPU node cannot give
# without disabling services that are genuinely in use (the ES pair, for one, is deliberately
# started after the close by dev-cleanup's OVERNIGHT_SET). So the gate guarantees STEADY STATE and
# REPORTS the rollout picture, which is the honest boundary of what it can promise.
note = "fits" if surge + job <= headroom else f"TIGHT: a rollout overlapping that Job needs {surge + job}m"
print(f"OK|dev fleet: {active}m of {budget}m ({headroom}m headroom); "
      f"largest RollingUpdate surge {surge}m ({surge_name}) + largest CronJob {job}m ({job_name}) -> {note}")
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

echo "${report#OK|}; capacity targets pinned at 0 in both places"
echo "=== validate-dev-capacity: OK ==="
