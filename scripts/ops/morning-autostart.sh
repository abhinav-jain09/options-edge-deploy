#!/usr/bin/env bash
# morning-autostart.sh — bring the prod OptionsEdge stack back UP on trading-day
# mornings. The inverse of scripts/ops/offhours-clean-slate.sh's scale-DOWN: it
# enforces the intended daytime replica state — every part-of=options-edge
# Deployment scaled to 1 EXCEPT an intentional keep-at-0 list (scaled to 0).
#
# WHY A HOST SCRIPT (not a native k8s CronJob): the cluster enforces a
# ValidatingAdmissionPolicy (options-edge-jenkins-only-workloads) that DENIES
# apps/deployments/scale to every principal except the jenkins-deployer SA, and
# denies batch/Job creation entirely — so a native CronJob's controller-created
# Job is rejected. The real trigger is Jenkinsfile.morning-autostart, which runs
# this ON the host over SSH and impersonates jenkins-deployer for the scale ops
# (the SAME model as Jenkinsfile.offhours-clean-slate / kafka-reset).
#
# NON-DESTRUCTIVE: only scales Deployments; never touches topics, PVCs, DB or logs.
# TRADING-DAY GUARD: no-ops on weekends/holidays (market closed -> prod stays down),
# using the vendored MarketCalendar. The cron is already Mon-Fri; this adds holidays.
set -uo pipefail

log()  { printf '%s %s\n' "$(date '+%FT%T%z')" "$*"; }
discord() { [ -n "${DISCORD_WEBHOOK_URL:-}" ] || return 0
  local payload
  payload=$(MSG="$1" python3 -c 'import json,os;print(json.dumps({"content":os.environ["MSG"]}))' 2>/dev/null) || return 0
  curl -s -m10 -H 'Content-Type: application/json' -d "$payload" "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 || true; }
die()  { log "FATAL: $*"; discord "❌ Morning autostart ABORTED (${EXPECTED_ENV:-?}): $*"; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

NS="${K8S_NAMESPACE:-options-edge}"
SELECTOR="${APP_SELECTOR:-app.kubernetes.io/part-of=options-edge}"
# The jenkins-deployer SA is the ONLY principal the admission policy lets scale
# Deployments. --as runs every scale op as it (same as offhours-clean-slate).
KUBECTL_AS="${KUBECTL_AS:-system:serviceaccount:options-edge:jenkins-deployer}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
EXPECTED_ENV="${EXPECTED_ENV:-prod}"
ENABLED="${ENABLED:-true}"                       # false -> dry log only, no scaling
ROLLOUT_WAIT="${ROLLOUT_WAIT:-120}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CALENDAR_DIR="${CALENDAR_DIR:-$(cd "$SCRIPT_DIR/../jenkins" 2>/dev/null && pwd || echo "$SCRIPT_DIR/../jenkins")}"

# INTENTIONAL keep-at-0 set (enforced DOWN each morning). Space-separated.
#   declared replicas:0 in the prod overlay:
#     hpsf-stage-a-service, hpsf-stage-b-service,
#     volume-sandwich-service, volume-sandwich-databento-service
#   4 USER-directed off (also set replicas:0 in the prod overlay): volume-pace-service
#   (IBKR variant), strike-flow-classifier-ibkr (IBKR out of scope),
#   options-edge-integration-test (no continuous tests in prod), spx-mission-control-service
#   (2026-07-09 USER kept mission-sandwich UP; SUPERSEDED 2026-07-30 — USER: keep it OFF),
#   ibkr-feed-service REMOVED from KEEP_DOWN 2026-07-29: it is now the priority-1 SPX+VIX
#   index source for BOTH envs (IBKR-SPX-INDEX-PRIORITY-SPOT-REQUIREMENT §3/§12) — the
#   single IB client on dev fans out to both clusters, so autostart MUST bring it up.
#   spread-skew-service + spread-skew-postgres-writer (2026-07-26 USER hold:
#   disabled in every environment until further notice).
#   databento-vix-feed (VIX feed separation PR-2): pre-evidence default; the shadow
#   commit removes this — replicas:1 and a KEEP_DOWN entry are mutually exclusive
#   (autostart would scale the shadow to 0 at 06:15 mid-session; design §5).
#   databento-maxpain-service REMOVED from KEEP_DOWN 2026-08-01 (USER: "turn on prod
#   only") — prod overlay patches replicas:1; dev stays down via dev-cleanup DISABLED_DEV.
KEEP_DOWN="${KEEP_DOWN:-hpsf-stage-a-service hpsf-stage-b-service volume-sandwich-service volume-sandwich-databento-service volume-pace-service volume-pace-databento-service strike-flow-classifier-ibkr options-edge-integration-test spx-mission-control-service short-premium-agent-service spread-skew-service spread-skew-postgres-writer directional-pressure-databento-service databento-mission-sandwich-service directional-pressure-service option-truth-engine-service ibkr-feed-service}"

kc()  { kubectl -n "$NS" --as="$KUBECTL_AS" "$@"; }   # impersonated (scale ops are policy-gated)
kcr() { kubectl -n "$NS" "$@"; }                       # read-only

have kubectl || die "kubectl not found"
have python3 || die "python3 required for the trading-day guard"
kcr get deploy >/dev/null 2>&1 || die "cannot reach k8s namespace $NS"

NOW_ET="$(TZ=America/New_York date '+%Y-%m-%d %H:%M %Z')"
log "=== morning autostart start (env=$EXPECTED_ENV ENABLED=$ENABLED) $NOW_ET ==="

# --- TRADING-DAY GUARD (fail-closed): no-op on weekends/holidays -----------------
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
  no)  log "trading-day guard: NOT a trading day — no-op (prod stays down)"
       discord "🌙 Morning autostart ($EXPECTED_ENV) — non-trading day, prod left down · ${NOW_ET}"; exit 0 ;;
  *)   die "trading-day guard failed: $TRADING" ;;
esac

# --- MUTUAL EXCLUSION with the off-hours clean-slate wipe -----------------------
# The off-hours wipe (scripts/ops/offhours-clean-slate.sh) deletes/purges Kafka
# topics + PVCs + DB while apps are at 0. If a manual/delayed live wipe overlapped
# this 06:15 run we could scale apps to 1 MID-WIPE (corrupt/half-empty state). Both
# jobs now contend the SAME lock file: whoever holds it wins, the other backs off.
# We hold it for our whole run (offhours' own `flock -n 9` on this file then exits).
SHARED_LOCK="${SHARED_LOCK:-/tmp/offhours-clean-slate.lock}"
if have flock; then
  exec 9>"$SHARED_LOCK" || die "cannot open shared lock $SHARED_LOCK"
  if ! flock -n 9; then
    log "off-hours clean-slate (or another autostart) holds $SHARED_LOCK — refusing to scale into a wipe"
    discord "🔒 Morning autostart ($EXPECTED_ENV) — off-hours job holds the lock; skipped this run · ${NOW_ET}"
    exit 0
  fi
  log "acquired shared lock $SHARED_LOCK (mutually exclusive with off-hours wipe)"
else
  log "WARN: flock not available — shared lock skipped (overlap with off-hours wipe not prevented)"
fi

# --- enforce morning replica state: up-set -> 1, keep-down set -> 0 --------------
ALL=$(kcr get deploy -l "$SELECTOR" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sed '/^$/d' | sort)
[ -n "$ALL" ] || die "no deployments matched selector $SELECTOR"

up=0; kept=0; failed=0; UPLIST=""
for name in $ALL; do
  target=1
  for k in $KEEP_DOWN; do [ "$name" = "$k" ] && { target=0; break; }; done
  if [ "$ENABLED" = "true" ]; then
    if kc scale deploy "$name" --replicas="$target" >/dev/null 2>&1; then
      if [ "$target" = 1 ]; then up=$((up+1)); UPLIST="$UPLIST $name"; else kept=$((kept+1)); fi
    else
      failed=$((failed+1)); log "WARN scale to $target failed: $name"
    fi
  else
    log "[dry] would set $name -> replicas=$target"
    if [ "$target" = 1 ]; then up=$((up+1)); else kept=$((kept+1)); fi
  fi
done
log "replica enforcement: up=$up kept-down=$kept failed=$failed"

# --- best-effort readiness wait (never fail the job on a slow rollout) -----------
if [ "$ENABLED" = "true" ] && [ "$up" -gt 0 ]; then
  deadline=$(( $(date +%s) + ROLLOUT_WAIT ))
  while :; do
    notready=$(kcr get deploy -l "$SELECTOR" \
        -o jsonpath='{range .items[*]}{.spec.replicas}{"/"}{.status.readyReplicas}{"\n"}{end}' 2>/dev/null \
      | awk -F'/' '$1+0>0 && $2+0<$1+0{c++} END{print c+0}')
    [ "${notready:-0}" -eq 0 ] && { log "all scaled-up deployments Ready"; break; }
    [ "$(date +%s)" -ge "$deadline" ] && { log "WARN: ${notready} deployments not Ready after ${ROLLOUT_WAIT}s"; break; }
    sleep 5
  done

  # SELF-HEAL boot-order stragglers (parity with dev-cleanup's start path).
  # On a box reboot k3s starts the pods while Kafka (systemd) is still coming up, so
  # Streams/runtime services die at startup with "Timeout expired while fetching topic
  # metadata" / "Connection refused" / "timeout creating source topic ...". Their MAIN
  # THREAD dies but the container stays Running, so kubelet never restarts them and they
  # sit 0/1 FOREVER — an env that looks "up" but silently has no close-direction,
  # delta-flow, strike-intelligence or heatmap. Bounce whatever is still short of its
  # desired replicas; by now Kafka is up, so the restart succeeds.
  healed=0
  for d in $(kcr get deploy -l "$SELECTOR" -o name 2>/dev/null | sed 's#.*/##'); do
    skip=0
    for k in $KEEP_DOWN; do [ "$d" = "$k" ] && skip=1 && break; done
    [ "$skip" = 1 ] && continue
    des=$(kcr get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null)
    rr=$(kcr get deploy "$d" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
    if [ "${des:-0}" -gt 0 ] && [ "${rr:-0}" -lt "${des:-0}" ]; then
      log "boot-order straggler: $d (${rr:-0}/${des}) — restarting"
      kc rollout restart deploy/"$d" >/dev/null 2>&1 && healed=$((healed+1))
    fi
  done
  [ "$healed" -gt 0 ] && log "self-heal restarted ${healed} straggler(s)" || log "self-heal: no stragglers"
fi

if [ "$failed" -eq 0 ]; then
  discord "☀️ Morning autostart ($EXPECTED_ENV) — brought up **${up}**, kept **${kept}** intentionally down. ✅ · ${NOW_ET}"
else
  discord "⚠️ Morning autostart ($EXPECTED_ENV) — up=${up} kept=${kept} FAILED=${failed}. CHECK: kubectl -n $NS get deploy · ${NOW_ET}"
fi
log "=== morning autostart complete (up=$up kept=$kept failed=$failed) ==="
[ "$failed" -eq 0 ]
