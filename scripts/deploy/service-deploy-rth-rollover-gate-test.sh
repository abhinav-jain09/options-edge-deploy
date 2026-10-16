#!/usr/bin/env bash
# Regression fixture for the RTH_ROLLOVER_GATE in service-deploy.sh (BEGIN/END RTH_ROLLOVER_GATE markers):
# a rendered Deployment carrying MARKET_STRUCTURE_IGNORE_RECOVERY=true must NOT be applied while the New York
# regular session is open (Mon-Fri 09:30:00-15:59:59 America/New_York), with no override; outside it, and
# whenever the flag is absent or not "true", the deploy proceeds. The block is extracted from the real
# script (never a copy), `date` is shimmed on PATH so no case depends on the wall clock, and `yq` is real.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

GATE="$(sed -n '/^# BEGIN RTH_ROLLOVER_GATE/,/^# END RTH_ROLLOVER_GATE$/p' "$HERE/service-deploy.sh")"
[ -n "$GATE" ] || { echo "FATAL: BEGIN/END RTH_ROLLOVER_GATE markers not found in service-deploy.sh" >&2; exit 1; }
grep -q 'exit 1' <<<"$GATE" || { echo "FATAL: the extracted gate block never exits non-zero" >&2; exit 1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"
# The shim answers the gate's one `date` call with FAKE_NY ("<dow> <HHMM> <stamp...>"); anything else is a bug.
cat >"$T/bin/date" <<'SHIM'
#!/usr/bin/env bash
[ "${TZ:-}" = "America/New_York" ] || { echo "date shim: expected TZ=America/New_York, got '${TZ:-}'" >&2; exit 97; }
printf '%s\n' "$FAKE_NY"
SHIM
chmod +x "$T/bin/date"

render() { # $1 = file, $2 = flag value or "" for absent, $3 = container list key (containers|initContainers)
  local val="$2" key="${3:-containers}"
  {
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: unrelated\n---\n'
    printf 'apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: market-structure-service\nspec:\n  template:\n    spec:\n      %s:\n        - name: c\n          env:\n            - name: MARKET_STRUCTURE_PATTERNS_ENABLED\n              value: "false"\n' "$key"
    if [ -n "$val" ]; then printf '            - name: MARKET_STRUCTURE_IGNORE_RECOVERY\n              value: "%s"\n' "$val"; fi
  } >"$1"
}

# run <render-file> <FAKE_NY>: prints rc, stdout+stderr captured in $OUT
run() {
  OUT="$(cd "$T" && PATH="$T/bin:$PATH" FAKE_NY="$2" RENDER="$1" bash -c "$GATE" 2>&1)"; RC=$?
}

render "$T/on.yaml" true
render "$T/off.yaml" false
render "$T/absent.yaml" ""
render "$T/init.yaml" true initContainers

echo "== flag TRUE inside the regular session: refused, nothing applied, no override =="
for ny in "2 1030 2026-10-13 10:30:00 EDT" "1 0930 2026-10-12 09:30:00 EDT" "5 1559 2026-10-16 15:59:59 EDT" "3 1200 2026-01-14 12:00:00 EST"; do
  run "$T/on.yaml" "$ny"
  if [ "$RC" -eq 1 ] && grep -q 'FATAL: Deployment(s) market-structure-service carry MARKET_STRUCTURE_IGNORE_RECOVERY=true' <<<"$OUT" \
     && grep -q 'Nothing was applied' <<<"$OUT" && grep -q 'there is no override' <<<"$OUT"; then ok "refused at NY '$ny'"; else bad "expected refusal at NY '$ny' (rc=$RC): $OUT"; fi
done

echo "== flag TRUE in an initContainer inside the session: refused =="
run "$T/init.yaml" "2 1030 2026-10-13 10:30:00 EDT"
[ "$RC" -eq 1 ] && grep -q 'FATAL' <<<"$OUT" && ok "initContainer flag refused" || bad "initContainer flag not refused (rc=$RC): $OUT"

echo "== flag TRUE outside the regular session: proceeds, and says so =="
for ny in "2 0929 2026-10-13 09:29:59 EDT" "2 1600 2026-10-13 16:00:00 EDT" "2 0745 2026-10-13 07:45:00 EDT" "6 1200 2026-10-17 12:00:00 EDT" "7 1030 2026-10-18 10:30:00 EDT" "1 2207 2026-10-12 22:07:00 EDT"; do
  run "$T/on.yaml" "$ny"
  if [ "$RC" -eq 0 ] && grep -q 'rollover gate: market-structure-service carry MARKET_STRUCTURE_IGNORE_RECOVERY=true' <<<"$OUT" && ! grep -q FATAL <<<"$OUT"; then ok "proceeds at NY '$ny'"; else bad "expected proceed at NY '$ny' (rc=$RC): $OUT"; fi
done

echo "== flag false or absent inside the session: not gated, silent =="
for f in off absent; do
  run "$T/$f.yaml" "2 1030 2026-10-13 10:30:00 EDT"
  if [ "$RC" -eq 0 ] && [ -z "$OUT" ]; then ok "$f: silent pass-through"; else bad "$f: expected silent rc=0, got rc=$RC: $OUT"; fi
done

echo "== mutation: a gate that never refuses must fail this test =="
MUT="$(sed 's/exit 1$/true/' <<<"$GATE")"
OUT="$(cd "$T" && PATH="$T/bin:$PATH" FAKE_NY="2 1030 2026-10-13 10:30:00 EDT" RENDER="$T/on.yaml" bash -c "$MUT" 2>&1)"; RC=$?
[ "$RC" -eq 0 ] && ok "mutant (exit 1 -> true) is detectably different (rc=0 where the real gate gives 1)" || bad "mutant still refused — the refusal assertion proves nothing"

echo
if [ "$fails" -eq 0 ]; then echo "service-deploy-rth-rollover-gate-test: ALL PASS"; else echo "service-deploy-rth-rollover-gate-test: $fails FAILURE(S)"; exit 1; fi
