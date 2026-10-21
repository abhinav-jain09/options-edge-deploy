#!/usr/bin/env bash
# Regression fixture for the MS_IGNORE_RECOVERY_ROLLOVER block in service-deploy.sh (BEGIN/END markers): the W0 recovery-method rollover flag
# is injected into the RENDER from MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER=true, for SERVICE=market-structure only, never twice, never into
# git, and the RTH_ROLLOVER_GATE that follows it in the real script refuses the injected flag inside the New York regular session exactly as it
# refuses a committed one. Both blocks are extracted from the real script (never copies); `date` is shimmed; yq is real.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

INJECT="$(sed -n '/^# BEGIN MS_IGNORE_RECOVERY_ROLLOVER/,/^# END MS_IGNORE_RECOVERY_ROLLOVER$/p' "$HERE/service-deploy.sh")"
GATE="$(sed -n '/^# BEGIN RTH_ROLLOVER_GATE/,/^# END RTH_ROLLOVER_GATE$/p' "$HERE/service-deploy.sh")"
[ -n "$INJECT" ] && [ -n "$GATE" ] || { echo "FATAL: BEGIN/END markers not found in service-deploy.sh" >&2; exit 1; }
grep -q 'exit 1' <<<"$INJECT" || { echo "FATAL: the extracted inject block never exits non-zero" >&2; exit 1; }
# the inject block must come BEFORE the gate in the real script, or the gate would never see the injected flag
[ "$(grep -n 'BEGIN MS_IGNORE_RECOVERY_ROLLOVER' "$HERE/service-deploy.sh" | cut -d: -f1)" -lt "$(grep -n 'BEGIN RTH_ROLLOVER_GATE' "$HERE/service-deploy.sh" | cut -d: -f1)" ] \
  && ok "the inject block precedes the RTH gate in service-deploy.sh" || bad "the inject block must precede the RTH gate"
# ... and before the dry-run exit, so a dry run shows the injected render
[ "$(grep -n 'BEGIN MS_IGNORE_RECOVERY_ROLLOVER' "$HERE/service-deploy.sh" | cut -d: -f1)" -lt "$(grep -n '^if \[ "\$DEPLOY_DRY_RUN" = "true" \]' "$HERE/service-deploy.sh" | head -1 | cut -d: -f1)" ] \
  && ok "the inject block precedes the dry-run exit" || bad "the inject block must precede the dry-run exit"

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"
cat >"$T/bin/date" <<'SHIM'
#!/usr/bin/env bash
[ "${TZ:-}" = "America/New_York" ] || { echo "date shim: expected TZ=America/New_York" >&2; exit 97; }
printf '%s\n' "$FAKE_NY"
SHIM
chmod +x "$T/bin/date"

SRC="$ROOT/k8s/services/market-structure/overlays/production/manifest.yaml"
cp "$SRC" "$T/real.yaml"
before_env="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].env[].name' "$T/real.yaml" | grep -v '^---$' | tr '\n' ' ')"

run_inject() { # <render> <service> <flag>
  OUT="$(cd "$T" && PATH="$T/bin:$PATH" RENDER="$1" SERVICE="$2" MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER="$3" bash -c "$INJECT" 2>&1)"; RC=$?
}
envnames() { yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].env[].name' "$1" | grep -v '^---$' | tr '\n' ' '; }

echo "== parameter false / unset: the render is untouched =="
cp "$SRC" "$T/a.yaml"; run_inject "$T/a.yaml" market-structure false
{ [ "$RC" -eq 0 ] && cmp -s "$T/a.yaml" "$SRC"; } && ok "false leaves the render byte-identical" || bad "false changed the render (rc=$RC): $OUT"
cp "$SRC" "$T/a.yaml"; OUT="$(cd "$T" && PATH="$T/bin:$PATH" RENDER="$T/a.yaml" SERVICE=market-structure bash -c "$INJECT" 2>&1)"; RC=$?
{ [ "$RC" -eq 0 ] && cmp -s "$T/a.yaml" "$SRC"; } && ok "unset leaves the render byte-identical" || bad "unset changed the render (rc=$RC)"

echo "== parameter true, SERVICE=market-structure: injected, everything else kept =="
cp "$SRC" "$T/b.yaml"; run_inject "$T/b.yaml" market-structure true
val="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].env[] | select(.name == "MARKET_STRUCTURE_IGNORE_RECOVERY") | .value' "$T/b.yaml" | grep -v '^---$')"
{ [ "$RC" -eq 0 ] && [ "$val" = true ] && [ "$(envnames "$T/b.yaml")" = "${before_env}MARKET_STRUCTURE_IGNORE_RECOVERY " ]; } \
  && ok "MARKET_STRUCTURE_IGNORE_RECOVERY=true appended; every other env var kept in order" || bad "injection wrong (rc=$RC val=$val): $OUT"
[ "$(grep -c MARKET_STRUCTURE_IGNORE_RECOVERY "$SRC")" = 0 ] && ok "the manifest in git carries no rollover flag" || bad "the committed manifest carries the rollover flag"
run_inject "$T/b.yaml" market-structure true
[ "$RC" -eq 1 ] && grep -q 'already sets MARKET_STRUCTURE_IGNORE_RECOVERY' <<<"$OUT" && ok "a second injection is refused" || bad "second injection not refused (rc=$RC)"

echo "== parameter true, any other service: refused =="
cp "$SRC" "$T/c.yaml"; run_inject "$T/c.yaml" indicator-service true
{ [ "$RC" -eq 1 ] && grep -q 'for SERVICE=market-structure only' <<<"$OUT" && cmp -s "$T/c.yaml" "$SRC"; } && ok "refused for indicator-service, render untouched" || bad "other service not refused (rc=$RC)"

echo "== the RTH gate sees the injected flag =="
run_gate() { OUT="$(cd "$T" && PATH="$T/bin:$PATH" FAKE_NY="$2" RENDER="$1" bash -c "$GATE" 2>&1)"; RC=$?; }
run_gate "$T/b.yaml" "2 1030 2026-10-13 10:30:00 EDT"
{ [ "$RC" -eq 1 ] && grep -q 'FATAL: Deployment(s) market-structure-service carry MARKET_STRUCTURE_IGNORE_RECOVERY=true' <<<"$OUT"; } \
  && ok "injected flag refused inside the regular session" || bad "gate did not refuse the injected flag (rc=$RC): $OUT"
run_gate "$T/b.yaml" "2 1601 2026-10-13 16:01:00 EDT"
[ "$RC" -eq 0 ] && grep -q 'rollover gate:' <<<"$OUT" && ok "injected flag proceeds after the 16:00 close" || bad "gate refused after the close (rc=$RC): $OUT"
run_gate "$T/real.yaml" "2 1030 2026-10-13 10:30:00 EDT"
[ "$RC" -eq 0 ] && ok "the committed render (no flag) is never gated" || bad "gate refused a render without the flag"

[ "$fails" -eq 0 ] && echo "PASS" || { echo "$fails FAILED"; exit 1; }
