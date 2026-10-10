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
# --- the phase=RESTORED resume path (Codex r2): an older build restored a held service to 1 and was killed before
# clearing state; the resume must force it to 0, verify, and fail closed when it cannot.
eval "$(sed -n '/^force_held_zero()/,/^}/p' "$C")"
die() { echo "DIE: $*"; return 1; }
log() { :; }
W="$(cd "$(mktemp -d)" && pwd -P)"; trap 'find "$W" -mindepth 1 -delete 2>/dev/null; rmdir "$W" 2>/dev/null' EXIT
cat > "$W/kc" <<'SH'
#!/usr/bin/env bash
# fake kubectl: state in $FAKE/<deploy>=<replicas>; FAKE_FAIL=get|scale|stick simulates failures
d=""; for a in "$@"; do case "$a" in deploy/*) d="${a#deploy/}" ;; esac; done
echo "$*" >> "$FAKE/calls"
case "$1" in
  get)
    n="$3"; [ "$FAKE_FAIL" = get ] && { echo "boom" >&2; exit 1; }
    if [ "$4" = "--ignore-not-found" ]; then [ -f "$FAKE/$n" ] && echo "deployment.apps/$n"; exit 0; fi
    [ -f "$FAKE/$3" ] && cat "$FAKE/$3" || echo ERR ;;
  scale) [ "$FAKE_FAIL" = scale ] && exit 1; [ "$FAKE_FAIL" = stick ] || echo 0 > "$FAKE/$d" ;;
esac
SH
chmod +x "$W/kc"; export FAKE="$W/fake"; mkdir -p "$FAKE"
KC="$W/kc"
echo 1 > "$FAKE/strike-liquidity-heatmap-service"; echo 1 > "$FAKE/es-feed"
FAKE_FAIL=none; ( set -e; force_held_zero ) >/dev/null 2>&1
[ "$(cat "$FAKE/strike-liquidity-heatmap-service")" = 0 ] && ok "RESTORED resume: a heatmap left at 1 by an older build is forced to 0" || bad "heatmap still at $(cat "$FAKE/strike-liquidity-heatmap-service")"
[ "$(cat "$FAKE/es-feed")" = 1 ] && ok "es-feed is left to its own pod-level check (not touched here)" || bad "force_held_zero touched es-feed"
: > "$FAKE/calls"; ( set -e; force_held_zero ) >/dev/null 2>&1
! grep -q "^scale" "$FAKE/calls" && ok "already at 0: nothing is scaled" || bad "scaled a deployment that was already 0"
rm -f "$FAKE/strike-liquidity-heatmap-service"
( set -e; force_held_zero ) >/dev/null 2>&1 && ok "a missing Deployment is not an error" || bad "missing Deployment failed the resume"
echo 1 > "$FAKE/strike-liquidity-heatmap-service"
FAKE_FAIL=get; export FAKE_FAIL; out=$( ( force_held_zero ) 2>&1 ); echo "$out" | grep -q "DIE: .*query failed" && ok "an API failure fails closed (state would not be cleared)" || bad "API failure not fatal: $out"
for bad_count in "" "not-a-replica" "-1" "1.5"; do
  printf '%s' "$bad_count" > "$FAKE/strike-liquidity-heatmap-service"; FAKE_FAIL=none; export FAKE_FAIL
  : > "$FAKE/calls"; out=$( ( force_held_zero ) 2>&1 )
  if echo "$out" | grep -q "DIE: .*unreadable" && ! grep -q "^scale" "$FAKE/calls"; then ok "a malformed current count ('$bad_count') fails closed and scales nothing"; else bad "malformed count '$bad_count' accepted: $out"; fi
done
echo 1 > "$FAKE/strike-liquidity-heatmap-service"
FAKE_FAIL=scale; export FAKE_FAIL; out=$( ( force_held_zero ) 2>&1 ); echo "$out" | grep -q "DIE: could not force" && ok "a scale failure fails closed" || bad "scale failure not fatal: $out"
FAKE_FAIL=stick; export FAKE_FAIL; out=$( ( force_held_zero ) 2>&1 ); echo "$out" | grep -q "DIE: .*still at" && ok "a scale that does not take fails closed (verified, not assumed)" || bad "unverified scale accepted: $out"
grep -q '^      force_held_zero$' "$C" && ok "the RESTORED resume path calls force_held_zero before clearing state" || bad "RESTORED path does not call force_held_zero"

[ "$(grep -c 'if held_zero "\$name"; then' "$C")" -ge 1 ] && ok "restore loop uses held_zero" || bad "restore loop does not use held_zero"
[ "$(grep -c 'if held_zero "\$name"; then reps=0; fi' "$C")" = 2 ] && ok "verification and readiness loops use held_zero" || bad "verification/readiness loops do not both use held_zero"
if grep -n 'if \[ "\$name" = "es-feed" \]; then' "$C" | grep -v "^$" >/dev/null; then bad "a bare es-feed-only check is still in a restore loop"; else ok "no es-feed-only check left in the restore loops"; fi
grep -q 'strike-liquidity-heatmap-service' "$HERE/render_es4_manifests.py" && ok "also listed in ES4_KEEP_DOWN (renderer)" || bad "missing from ES4_KEEP_DOWN"
echo
[ "$fails" -eq 0 ] && { echo "=== cleanup-held-zero: OK ==="; exit 0; }
echo "=== cleanup-held-zero: $fails problem(s) ===" >&2; exit 1
