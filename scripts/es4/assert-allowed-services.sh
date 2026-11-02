#!/usr/bin/env bash
# Refuses to deploy anything to es4 that is not in k8s/es4/allowed-services.txt.
#   assert-allowed-services.sh service <name>   - <name> must be listed (and have a manifest)
#   assert-allowed-services.sh manifests        - every k8s/es4/services/*.yaml must be listed
# Exit 1 with a clear message otherwise. Run from the repo root (the Jenkins workspace).
set -euo pipefail
cd "$(dirname "$0")/../.."
LIST=k8s/es4/allowed-services.txt
allowed() { grep -vE '^[[:space:]]*(#|$)' "$LIST" | tr -d '[:blank:]'; }
refuse() { echo "REFUSED: $1 is not an es4 service. Only ES-related services may run on es4; the allowed set is $LIST." >&2; exit 1; }
[ -s "$LIST" ] || { echo "REFUSED: $LIST is missing or empty" >&2; exit 1; }
case "${1:-}" in
  service)
    name="${2:?service name required}"
    allowed | grep -Fxq -- "$name" || refuse "$name"
    ;;
  manifests)
    bad=0
    for f in k8s/es4/services/*.yaml; do
      n="$(basename "$f" .yaml)"
      allowed | grep -Fxq -- "$n" || { echo "REFUSED: $f is not an allowed es4 service ($LIST)" >&2; bad=1; }
    done
    [ "$bad" = 0 ] || exit 1
    ;;
  *) echo "usage: $0 service <name> | manifests" >&2; exit 2 ;;
esac
echo "es4 allowlist ok (${1}${2:+ $2})"
