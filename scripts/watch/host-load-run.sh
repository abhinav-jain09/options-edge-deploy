#!/usr/bin/env bash
# Fetch the nodes (and whatever usage the cluster will tell us) and run host-load.py, as ONE command the deliverer can wrap.
#
# Usage comes from the metrics API (metrics-server) when the cluster has one. Without it host-load.py still checks node CONDITIONS (Ready,
# Memory/Disk/PID pressure) and says in its output that usage was not measured - a visible limitation, never a silent "healthy". Node access needs
# k8s/security/jenkins-fleet-watch-node-reader.yaml; without it `get nodes` is Forbidden and this exits 2, which pages as "could not check".
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/kube-reachable.sh"
oe_kube_reachable || exit 2
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
if ! kubectl --request-timeout=20s get nodes -o json > "$dir/nodes.json" 2> "$dir/err"; then
  echo "ERROR: kubectl get nodes failed: $(tr '\n' ' ' < "$dir/err" | cut -c1-300)"
  exit 2
fi
args=(--nodes "$dir/nodes.json")
if kubectl --request-timeout=20s get --raw /apis/metrics.k8s.io/v1beta1/nodes > "$dir/metrics.json" 2> /dev/null; then
  args+=(--metrics "$dir/metrics.json")
fi
python3 "$HERE/host-load.py" "${args[@]}" "$@"
