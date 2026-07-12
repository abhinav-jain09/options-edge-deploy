#!/usr/bin/env bash
# es-predown.sh — scale the OVERNIGHT ES-tracking services to 0 at ~09:17 ET, just before the 09:30 SPX
# open. The overnight ES-open-direction session is over; feed-gateway + web STAY UP for the SPX day
# session (this script does NOT touch them). Mirror of the dev-cleanup.sh ESDOWN slot for prod.
#
# Runs ON the prod host over SSH from Jenkinsfile.es-predown (the SAME Jenkins->SSH model as
# morning-autostart / offhours-clean-slate). The jenkins-only admission policy denies scale to every
# principal except the jenkins-deployer SA, so scale ops impersonate it (--as).
#
# NON-DESTRUCTIVE: only scales the two ES Deployments to 0. Never touches topics/PVCs/DB/logs, and never
# touches feed-gateway/web or any other service. Trading-day guard no-ops on holidays.
set -uo pipefail

NS="${K8S_NAMESPACE:-options-edge}"
KUBECTL_AS="${KUBECTL_AS:-system:serviceaccount:options-edge:jenkins-deployer}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
EXPECTED_ENV="${EXPECTED_ENV:-prod}"
ENABLED="${ENABLED:-true}"                        # false -> dry log only, no scaling
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CALENDAR_DIR="${CALENDAR_DIR:-$(cd "$SCRIPT_DIR/../jenkins" 2>/dev/null && pwd || echo "$SCRIPT_DIR/../jenkins")}"
# The overnight ES services that shut down before the open. Keep in sync with dev-cleanup.sh ES_DOWN_SET.
ES_DOWN_SET="${ES_DOWN_SET:-es-open-direction-service es-open-direction-postgres-writer}"

log()  { printf '%s %s\n' "$(date '+%FT%T%z')" "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }
discord() { [ -n "${DISCORD_WEBHOOK_URL:-}" ] || return 0
  local payload; payload=$(MSG="$1" python3 -c 'import json,os;print(json.dumps({"content":os.environ["MSG"]}))' 2>/dev/null) || return 0
  curl -s -m10 -H 'Content-Type: application/json' -d "$payload" "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 || true; }
kc() { kubectl -n "$NS" --as="$KUBECTL_AS" "$@"; }
kcr() { kubectl -n "$NS" "$@"; }

have kubectl || { log "FATAL: kubectl not found"; exit 1; }
have python3 || { log "FATAL: python3 required for the trading-day guard"; exit 1; }
kcr get deploy >/dev/null 2>&1 || { log "FATAL: cannot reach k8s namespace $NS"; exit 1; }

NOW_ET="$(TZ=America/New_York date '+%Y-%m-%d %H:%M %Z')"
log "=== ES pre-open down start (env=$EXPECTED_ENV ENABLED=$ENABLED) $NOW_ET ==="

# TRADING-DAY GUARD (fail-closed): no-op on weekends/holidays.
TRADING=$(CALENDAR_DIR="$CALENDAR_DIR" python3 - <<'PY'
import os, sys
sys.path.insert(0, os.environ["CALENDAR_DIR"])
try:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from market_calendar import MarketCalendar
    today = datetime.now(ZoneInfo("America/New_York")).date()
    print("yes" if MarketCalendar().is_trading_day(today) else "no")
except Exception as e:
    print("err:" + str(e))
PY
)
case "$TRADING" in
  yes) log "trading-day guard: today IS a trading day — proceeding" ;;
  no)  log "trading-day guard: NOT a trading day — no-op"; exit 0 ;;
  *)   log "FATAL: trading-day guard failed: $TRADING"; exit 1 ;;
esac

down=0; skip=0; failed=0
for d in $ES_DOWN_SET; do
  if ! kcr get deploy "$d" >/dev/null 2>&1; then log "  (absent, skipped): $d"; skip=$((skip+1)); continue; fi
  if [ "$ENABLED" = "true" ]; then
    if kc scale deploy "$d" --replicas=0 >/dev/null 2>&1; then log "  down: $d"; down=$((down+1)); else log "  WARN scale-to-0 failed: $d"; failed=$((failed+1)); fi
  else
    log "  [dry] would scale $d -> 0"; down=$((down+1))
  fi
done

log "=== ES pre-open down complete (down=$down skipped=$skip failed=$failed) ==="
if [ "$failed" -eq 0 ]; then
  discord "🌙 ES pre-open down ($EXPECTED_ENV) — scaled **${down}** overnight ES service(s) to 0 before the open (feed-gateway/web untouched). · ${NOW_ET}"
else
  discord "⚠️ ES pre-open down ($EXPECTED_ENV) — down=${down} FAILED=${failed}. CHECK: kubectl -n $NS get deploy · ${NOW_ET}"
fi
[ "$failed" -eq 0 ]
