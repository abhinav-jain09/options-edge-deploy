#!/usr/bin/env bash
# cleanup-held-zero-test.sh - the clean-reset restore must never raise a held deployment.
# cleanup-es4.sh restores every captured replica count, so a snapshot taken while a held service ran (or a
# stale one that survived an interrupted run) would undo an owner hold. HELD_AT_ZERO is the list the restore,
# the verification and the readiness wait all skip. This pins that list, its use in all three loops, and that
# every ES4_KEEP_DOWN service that has ever run is either listed or was already at 0 when captured.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
C="$HERE/cleanup-es4.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

eval "$(sed -n '/^HELD_AT_ZERO=/p;/^held_zero()/p' "$C")"
for n in es-feed strike-liquidity-heatmap-service; do
  held_zero "$n" && ok "$n is held at 0 by a reset" || bad "$n is NOT in HELD_AT_ZERO"
done
for n in es-amt-service es-feed-gateway strike-liquidity-heatmap-service-x es-fee; do
  held_zero "$n" && bad "$n must not match (exact names only)" || ok "$n is not held (exact match)"
done
[ "$(grep -c 'if held_zero "\$name"; then' "$C")" -ge 1 ] && ok "restore loop uses held_zero" || bad "restore loop does not use held_zero"
[ "$(grep -c 'if held_zero "\$name"; then reps=0; fi' "$C")" = 2 ] && ok "verification and readiness loops use held_zero" || bad "verification/readiness loops do not both use held_zero"
if grep -n 'if \[ "\$name" = "es-feed" \]; then' "$C" | grep -v "^$" >/dev/null; then bad "a bare es-feed-only check is still in a restore loop"; else ok "no es-feed-only check left in the restore loops"; fi
grep -q 'strike-liquidity-heatmap-service' "$HERE/render_es4_manifests.py" && ok "also listed in ES4_KEEP_DOWN (renderer)" || bad "missing from ES4_KEEP_DOWN"
echo
[ "$fails" -eq 0 ] && { echo "=== cleanup-held-zero: OK ==="; exit 0; }
echo "=== cleanup-held-zero: $fails problem(s) ===" >&2; exit 1
