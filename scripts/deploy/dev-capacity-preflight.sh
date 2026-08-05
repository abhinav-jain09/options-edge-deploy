#!/usr/bin/env bash
# dev-capacity-preflight.sh — refuse a dev rollout that cannot finish.
#
# Usage: dev-capacity-preflight.sh <rendered-manifest.yaml> [label-for-messages]
#
# A RollingUpdate needs its surge pod to SCHEDULE before the old one goes away. The dev node is
# 10 CPUs and headroom is deliberately thin (scripts/ci/validate-dev-capacity.sh asserts the static
# side), so a rollout that starts while an operational Job holds CPU waits on a pod that can never be
# scheduled — and a cleanup Job may outlive ROLLOUT_TIMEOUT, leaving the service half-rolled.
#
# `need` comes from the CANDIDATE manifest, never from the live Deployment: a change that RAISES a
# request is exactly the case that must be caught, and reading the server would evaluate the old
# value. `kubectl create --dry-run=client -o json` converts it locally without touching the API, so
# the surge maths is structural (replicas x maxSurge, with Kubernetes' percentage rounding) rather
# than a text scan assuming one pod each. Only free capacity is queried from the cluster.
#
# Fails OPEN on a query it cannot run (no kubectl, unreadable node) — this is a guard against a
# known scheduling trap, not an authorisation gate, and must never wedge a deploy on a parse error.
set -euo pipefail

RENDER="${1:?usage: dev-capacity-preflight.sh <rendered-manifest.yaml> [label]}"
LABEL="${2:-this deploy}"

command -v kubectl >/dev/null 2>&1 || exit 0
[ -f "$RENDER" ] || exit 0

need=$(kubectl create -f "$RENDER" --dry-run=client -o json 2>/dev/null | python3 -c "
import json, math, sys

# STRUCTURAL, not a text scan: surge is a Kubernetes calculation, not 'one pod each'.
#   replicas: 0        -> nothing rolls, no surge
#   Recreate           -> old pods go first, no surge pod
#   maxSurge: 0        -> explicitly no surge (raw-postgres-writer relies on this)
#   maxSurge: N        -> N extra pods
#   maxSurge: 'P%'     -> ceil(replicas * P / 100), the Kubernetes rounding
#   default            -> 25%, i.e. 1 pod at replicas: 1
decoder = json.JSONDecoder()
text = sys.stdin.read().strip()
docs, idx = [], 0
while idx < len(text):
    obj, end = decoder.raw_decode(text, idx)
    docs.append(obj)
    idx = end
    while idx < len(text) and text[idx] in ' \\t\\r\\n':
        idx += 1

need = 0
for d in docs:
    for it in (d.get('items') or [d]):
        if it.get('kind') != 'Deployment':
            continue
        spec = it['spec']
        replicas = spec.get('replicas', 1)
        if replicas == 0:
            continue
        strategy = spec.get('strategy') or {}
        if strategy.get('type', 'RollingUpdate') != 'RollingUpdate':
            continue
        raw = (strategy.get('rollingUpdate') or {}).get('maxSurge', '25%')
        if isinstance(raw, str) and raw.endswith('%'):
            surge_pods = math.ceil(replicas * int(raw[:-1]) / 100)
        else:
            surge_pods = int(raw)
        if surge_pods <= 0:
            continue
        pod = 0
        for c in spec['template']['spec']['containers']:
            v = ((c.get('resources') or {}).get('requests') or {}).get('cpu')
            if not v:
                continue
            v = str(v)
            pod += int(v[:-1]) if v.endswith('m') else int(float(v) * 1000)
        need = max(need, pod * surge_pods)
print(need)
") || exit 0

[ "${need:-0}" -gt 0 ] || exit 0

free=$(kubectl get node -o json 2>/dev/null | python3 -c "
import json, subprocess, sys
try:
    node = json.load(sys.stdin)['items'][0]
except Exception:
    raise SystemExit(1)
cap = node['status']['allocatable']['cpu']
cap = int(cap[:-1]) if cap.endswith('m') else int(float(cap) * 1000)
pods = json.loads(subprocess.run(['kubectl','get','pods','-A','-o','json'],
                                 capture_output=True, text=True).stdout)
used = 0
for p in pods['items']:
    if p['status'].get('phase') not in ('Running', 'Pending'):
        continue
    for c in p['spec']['containers']:
        v = ((c.get('resources') or {}).get('requests') or {}).get('cpu')
        if not v:
            continue
        v = str(v)
        used += int(v[:-1]) if v.endswith('m') else int(float(v) * 1000)
print(cap - used)
") || exit 0

[ -n "${free:-}" ] || exit 0

if [ "$free" -lt "$need" ]; then
  echo "REFUSING TO DEPLOY: $LABEL needs ${need}m for its largest RollingUpdate surge pod, but only ${free}m is free on the dev node." >&2
  echo "  Something is holding CPU — most likely an operational Job (kafka-changelog-cleanup, or the 500m replay Job)." >&2
  echo "  Applying now would leave the rollout waiting on a pod that can never schedule, then time out half-rolled." >&2
  echo "  Check:  kubectl -n options-edge get pods --field-selector=status.phase=Running | grep -iE 'job|cleanup|replay'" >&2
  echo "  Then re-run once it finishes." >&2
  exit 1
fi
echo "capacity preflight: ${need}m surge needed, ${free}m free — proceeding"
