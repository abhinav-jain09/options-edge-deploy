#!/usr/bin/env bash
# verify-prod-tunnel.sh — prove the LIVE prod Cloudflare tunnel still matches the reviewed repo copy,
# and that the WebSocket route actually reaches the feed gateway.
#
# WHY THIS EXISTS
# ---------------
# Twice in one day a host config drifted from the repo and cost hours of market-time outage,
# because nothing ever compared the two:
#   * es4  — infra/es4/docker-compose.yml set auto-create=false on 2026-07-12 (#656); the LIVE
#            broker at /home/es4/infra was still "true" on 2026-07-31. Three clean-resets failed.
#   * prod — /etc/cloudflared/options-edge-stable.yml routed fullfunding.nl/ws/events at the k3s
#            ServiceLB (:8091) instead of the gateway NodePort. klipper-lb passes plain HTTP but
#            drops the WebSocket upgrade, so the board went dark for ~2h during market hours while
#            every component tested healthy on its own.
#
# The lesson both times: a file that only exists on a box is a file nobody reviews. This script is
# the cheap check that would have caught either in seconds.
#
# CHECKS
#   1. live tunnel config == repo copy (ignoring comments/blank lines)
#   2. /ws/events routes to a NodePort, never to the :8091 ServiceLB
#   3. the NodePort in the config is the one feed-gateway-service actually advertises
#   4. an unauthenticated WS upgrade returns 401 — proving the route reaches the gateway's auth
#      layer rather than dying in a proxy (1006/000 would mean it never got there)
#
# USAGE
#   scripts/ops/verify-prod-tunnel.sh                     # from the prod box
#   PROD_SSH="sshpass -p … ssh user@192.168.100.252" scripts/ops/verify-prod-tunnel.sh   # remote
set -uo pipefail

REPO_COPY="${REPO_COPY:-$(cd "$(dirname "$0")/../.." && pwd)/infra/prod/cloudflared/options-edge-stable.yml}"
LIVE_PATH="${LIVE_PATH:-/etc/cloudflared/options-edge-stable.yml}"
PROD_SSH="${PROD_SSH:-}"
GW_SVC="${GW_SVC:-feed-gateway-service}"
NS="${NS:-options-edge}"
# Dual-domain matrix (fullfunding.nl -> bleadingoptions.com migration): every public hostname is
# probed. Override APEX_HOSTS/ES_HOSTS/AUTH_HOSTS (space-separated) if a domain is retired.
APEX_HOSTS="${APEX_HOSTS:-fullfunding.nl bleadingoptions.com}"
ES_HOSTS="${ES_HOSTS:-es.fullfunding.nl es.bleadingoptions.com}"
AUTH_HOSTS="${AUTH_HOSTS:-auth.fullfunding.nl auth.bleadingoptions.com}"

run() { if [ -n "$PROD_SSH" ]; then $PROD_SSH "$1"; else bash -c "$1"; fi; }
strip() { grep -vE '^\s*#|^\s*$' | sed 's/[[:space:]]*#.*$//' | sed 's/[[:space:]]*$//'; }
fail=0
note() { echo "  $*"; }
bad()  { echo "  FAIL: $*" >&2; fail=1; }

[ -f "$REPO_COPY" ] || { echo "FATAL: repo copy not found at $REPO_COPY" >&2; exit 2; }

# 1. live vs repo -----------------------------------------------------------------------------
live_raw="$(run "cat '$LIVE_PATH' 2>/dev/null")"
if [ -z "$live_raw" ]; then
  bad "could not read the live config at $LIVE_PATH"
else
  if diff -q <(printf '%s\n' "$live_raw" | strip) <(strip < "$REPO_COPY") >/dev/null 2>&1; then
    note "OK   live tunnel config matches the repo copy"
  else
    bad "live tunnel config DRIFTED from $REPO_COPY"
    diff <(strip < "$REPO_COPY") <(printf '%s\n' "$live_raw" | strip) | head -20 | sed 's/^/       /'
  fi
fi

# 2 + 3. the /ws/events routes (every apex hostname) -------------------------------------------
# Each apex hostname must route /ws/events at the prod gateway NodePort — never the :8091
# ServiceLB (2026-07-31 outage). The es.* hostnames route to the es4 box (.4:30091), whose
# NodePort belongs to the other cluster, so they are covered by the live 401 probes below.
ws_target_for() {  # $1 = hostname; prints that block's /ws/events service target
  printf '%s\n' "$live_raw" | awk -v h="$1" '
    $0 ~ "hostname: "h"$" { inhost=1; next }
    inhost && /path: \/ws\/events/ { inpath=1; next }
    inpath && /service:/ { sub(/.*service:[[:space:]]*/, ""); sub(/[[:space:]]*#.*/, ""); print; exit }
    inhost && /hostname:/ { inhost=0 }'
}
want_np="$(run "kubectl -n $NS get svc $GW_SVC -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null")"
[ -n "$want_np" ] || note "WARN could not read $GW_SVC nodePort (kubectl unavailable from here) — skipping that check"
for h in $APEX_HOSTS; do
  ws_target="$(ws_target_for "$h")"
  note "     $h/ws/events -> ${ws_target:-<unset>}"
  case "$ws_target" in
    *:8091*) bad "$h/ws/events points at the :8091 ServiceLB — klipper-lb DROPS the WebSocket upgrade (2026-07-31 outage)" ;;
    *:3[0-9][0-9][0-9][0-9]*) note "OK   $h/ws/events uses a NodePort" ;;
    *) bad "$h/ws/events target '$ws_target' is neither a NodePort nor a recognised form" ;;
  esac
  if [ -n "$want_np" ]; then
    case "$ws_target" in
      *":$want_np"*) note "OK   NodePort $want_np matches $GW_SVC" ;;
      *) bad "$GW_SVC advertises NodePort $want_np but $h routes to '$ws_target' — the Service was likely recreated" ;;
    esac
  fi
done

# 4. do the routes actually reach each gateway's auth layer? -----------------------------------
# 401 proves the upgrade crossed Cloudflare AND the tunnel AND reached the gateway's auth check;
# 1006/000 means it died in a proxy on the way. Covers both domains and both clusters (apex=prod,
# es.*=es4). A brand-new hostname failing here usually means its DNS CNAME is not live yet.
for h in $APEX_HOSTS $ES_HOSTS; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 \
          -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
          -H "Sec-WebSocket-Key: $(head -c16 /dev/urandom | base64)" -H 'Sec-WebSocket-Version: 13' \
          "https://$h/ws/events" 2>/dev/null)"
  case "$code" in
    401) note "OK   $h: unauthenticated upgrade -> 401 (route reaches the gateway auth layer)" ;;
    000) bad "$h: unauthenticated upgrade got NO response — DNS, the tunnel, or the hop to the gateway is broken" ;;
    *)   bad "$h: unauthenticated upgrade -> $code (expected 401); the route may not reach the gateway" ;;
  esac
done

# 5. auth hostnames: OIDC discovery must serve, admin surfaces must be edge-blocked -------------
for h in $AUTH_HOSTS; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h/realms/optionsedge/.well-known/openid-configuration" 2>/dev/null)"
  case "$code" in
    200) note "OK   $h: OIDC discovery -> 200" ;;
    *)   bad "$h: OIDC discovery -> $code (expected 200)" ;;
  esac
  for path in /admin /realms/master; do
    code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h$path" 2>/dev/null)"
    case "$code" in
      404) note "OK   $h$path -> 404 (edge-blocked)" ;;
      *)   bad "$h$path -> $code (expected 404 — the admin surface must stay edge-blocked)" ;;
    esac
  done
done

[ "$fail" -eq 0 ] && echo "  prod tunnel VERIFIED" || echo "  prod tunnel has PROBLEMS (see above)" >&2
exit "$fail"
