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
# value. Only free capacity is queried from the cluster.
#
# Fails OPEN on a query it cannot run (no kubectl, unreadable node) — this is a guard against a
# known scheduling trap, not an authorisation gate, and must never wedge a deploy on a parse error.
set -euo pipefail

RENDER="${1:?usage: dev-capacity-preflight.sh <rendered-manifest.yaml> [label]}"
LABEL="${2:-this deploy}"

command -v kubectl >/dev/null 2>&1 || exit 0
[ -f "$RENDER" ] || exit 0

need=$(python3 - "$RENDER" <<'PYEOF'
import sys

# Deliberately not a YAML parse: the renders here are kubectl/kustomize output, and this only needs
# each Deployment's strategy and container cpu requests. Tracks indentation to stay inside the
# containers block of the doc it is reading.
docs = open(sys.argv[1]).read().split('\n---\n')
need = 0
for doc in docs:
    if 'kind: Deployment' not in doc:
        continue
    if 'type: Recreate' in doc:
        continue
    pod, in_requests, indent = 0, False, None
    for line in doc.splitlines():
        stripped = line.strip()
        if stripped == 'requests:':
            in_requests, indent = True, len(line) - len(line.lstrip())
            continue
        if in_requests:
            cur = len(line) - len(line.lstrip())
            if stripped and cur <= indent:
                in_requests = False
            elif stripped.startswith('cpu:'):
                v = stripped.split(':', 1)[1].strip().strip('"\'')
                pod += int(v[:-1]) if v.endswith('m') else int(float(v) * 1000)
    need = max(need, pod)
print(need)
PYEOF
) || exit 0

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
