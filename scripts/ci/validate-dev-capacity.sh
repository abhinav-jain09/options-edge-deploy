#!/usr/bin/env bash
# validate-dev-capacity.sh — the dev CPU budget is load-bearing, so it gets a gate.
#
# The dev node (docker-desktop) has 10 allocatable CPUs; kube-system takes 950m and jenkins 500m,
# leaving 8550m for options-edge. databento-gex-service's dev overlay deliberately reserves 2 full
# CPUs, and without deliberate headroom the rendered fleet exceeds that budget — which is exactly
# how GEX ended up Pending on "Insufficient cpu" on 2026-08-05 with no bad code anywhere.
#
# Two capacity targets are held at 0 to buy that headroom. Both halves have to hold together: the
# overlay alone is undone by `dev-cleanup start`, which scales every deployment outside DISABLED_DEV
# back to 1. That omission already undid this fix once, so this script asserts BOTH, plus the budget
# itself — a future patch that quietly raises a request now fails CI instead of production.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

BUDGET_MCPU=8550
CAPACITY_TARGETS=(dealer-ledger-service unified-sr-service)
CLEANUP=scripts/ops/dev-cleanup.sh

echo "=== validate-dev-capacity: dev fleet must fit ${BUDGET_MCPU}m ==="

render=$(kubectl kustomize k8s/overlays/dev)

# 1) every capacity target renders at replicas 0
for svc in "${CAPACITY_TARGETS[@]}"; do
  reps=$(printf '%s' "$render" | python3 -c "
import sys, yaml
name = sys.argv[1]
found = 'MISSING'
for doc in yaml.safe_load_all(sys.stdin.read()):
    if doc and doc.get('kind') == 'Deployment' and doc['metadata']['name'] == name:
        found = doc['spec'].get('replicas', 1)
print(found)
" "$svc")
  if [ "$reps" != "0" ]; then
    echo "FAIL: '$svc' renders replicas=$reps in the dev overlay, expected 0."
    echo "      It is a capacity target: without it databento-gex-service cannot schedule."
    exit 1
  fi
done

# 2) and is pinned by the daily lifecycle, or `dev-cleanup start` scales it back to 1
for svc in "${CAPACITY_TARGETS[@]}"; do
  if ! grep -q "DISABLED_DEV='.*\b${svc}\b" "$CLEANUP"; then
    echo "FAIL: '$svc' renders at 0 but is NOT in DISABLED_DEV in $CLEANUP."
    echo "      do_start() scales every deployment outside that list to 1, silently undoing this."
    exit 1
  fi
done

# 3) the active fleet fits the budget
active=$(printf '%s' "$render" | python3 -c "
import sys, yaml
total = 0
for doc in yaml.safe_load_all(sys.stdin.read()):
    if not doc or doc.get('kind') != 'Deployment': continue
    if doc['spec'].get('replicas', 1) == 0: continue
    for c in doc['spec']['template']['spec']['containers']:
        v = ((c.get('resources') or {}).get('requests') or {}).get('cpu')
        if not v: continue
        total += int(v[:-1]) if str(v).endswith('m') else int(float(v) * 1000)
print(total)
")
if [ "$active" -gt "$BUDGET_MCPU" ]; then
  echo "FAIL: the rendered dev fleet requests ${active}m, over the ${BUDGET_MCPU}m budget."
  echo "      Something will sit Pending on 'Insufficient cpu' — most likely databento-gex-service,"
  echo "      whose 2-CPU reservation is the largest single request."
  exit 1
fi

echo "dev fleet: ${active}m of ${BUDGET_MCPU}m ($((BUDGET_MCPU - active))m headroom); capacity targets pinned at 0 in both places"
echo "=== validate-dev-capacity: OK ==="
