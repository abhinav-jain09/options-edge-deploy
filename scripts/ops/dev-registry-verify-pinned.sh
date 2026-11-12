#!/usr/bin/env bash
# dev-registry-verify-pinned.sh — can every image dev pins be PULLED? (GETs, not HEAD: see the .py)
# Run it after anything that touches the registry. The prune runs it for you.
set -uo pipefail
export PATH="$PATH:/opt/homebrew/bin:/usr/local/bin"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KCTX="${KCTX-docker-desktop}"
kubectl ${KCTX:+--context "$KCTX"} -n options-edge get deploy,statefulset,daemonset,cronjob,job -o json 2>/dev/null \
  | REG="${REG:-http://localhost:5001}" python3 "$HERE/dev-registry-verify-pinned.py"
