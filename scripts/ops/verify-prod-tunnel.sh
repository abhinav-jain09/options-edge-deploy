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
# Contract expectations (flip EXPECTED_ISSUER at Phase 2; shrink hosts/origins at Phase 3).
# KC_HOSTNAME pins the issuer, so BOTH auth hostnames must report this exact value.
EXPECTED_ISSUER="${EXPECTED_ISSUER:-https://auth.fullfunding.nl/realms/optionsedge}"
EXPECTED_WS_ORIGINS="${EXPECTED_WS_ORIGINS:-https://fullfunding.nl https://bleadingoptions.com}"
GW_DEPLOY="${GW_DEPLOY:-feed-gateway-service}"

run() { if [ -n "$PROD_SSH" ]; then $PROD_SSH "$1"; else bash -c "$1"; fi; }
strip() { grep -vE '^\s*#|^\s*$' | sed 's/[[:space:]]*#.*$//' | sed 's/[[:space:]]*$//'; }
fail=0
note() { echo "  $*"; }
bad()  { echo "  FAIL: $*" >&2; fail=1; }

# Literal (non-regex) hostname match, and all state resets at every new ingress list item, so a
# malformed block (path with no service) or a metacharacter host can never borrow a later block's
# service line. Run with --selftest to exercise the fixtures.
ws_target_for() {  # $1 = hostname; $2 = config text; prints that host's /ws/events service target
  printf '%s\n' "$2" | awk -v h="$1" '
    function fieldval(line, key,    v) { v=line; sub(".*" key ":[[:space:]]*", "", v);
      sub(/[[:space:]]*#.*/, "", v); sub(/[[:space:]]+$/, "", v); return v }
    /^[[:space:]]*-[[:space:]]*hostname:/ { cur=fieldval($0, "hostname"); haspath=0; next }
    /^[[:space:]]*-[[:space:]]*service:/  { cur=""; haspath=0; next }   # hostless catch-all item
    /path:[[:space:]]*\/ws\/events([[:space:]]|$)/ { if (cur == h) haspath=1; next }
    /service:/ { if (cur == h && haspath) { print fieldval($0, "service"); exit } haspath=0 }'
}

selftest() {
  local fixture expect got rc=0
  run_case() {  # host, expected, config
    got="$(ws_target_for "$1" "$3")"
    if [ "$got" = "$2" ]; then echo "  OK   selftest: $4"
    else echo "  FAIL selftest: $4 (host=$1 expected='$2' got='$got')" >&2; rc=1; fi
  }
  fixture='ingress:
  - hostname: fullfundingXnl
    path: /ws/events
    service: http://evil:1111
  - hostname: fullfunding.nl
    path: /ws/events
    service: http://good:30097
  - hostname: fullfunding.nl
    service: http://web:8094
  - hostname: es.fullfunding.nl
    path: /ws/events
    service: http://es:30091
  - service: http_status:404'
  run_case fullfunding.nl "http://good:30097" "$fixture" "literal match skips lookalike host"
  run_case fullfundingXnl "http://evil:1111" "$fixture" "lookalike host resolves to its own block"
  run_case es.fullfunding.nl "http://es:30091" "$fixture" "es host not confused with apex"
  fixture='ingress:
  - hostname: a.example
    path: /ws/events
  - hostname: b.example
    path: /ws/events
    service: http://b:30001
  - service: http_status:404'
  run_case a.example "" "$fixture" "missing service yields empty, never a later block"
  run_case b.example "http://b:30001" "$fixture" "block after malformed one still resolves"
  fixture='ingress:
  - hostname: dup.example
    path: /ws/events
    service: http://first:30001
  - hostname: dup.example
    path: /ws/events
    service: http://second:30002
  - service: http_status:404'
  run_case dup.example "http://first:30001" "$fixture" "duplicate blocks: first wins (cloudflared order)"
  fixture='ingress:
  - hostname: dot.metachar+host.example
    path: /ws/events
    service: http://meta:30003
  - service: http_status:404'
  run_case "dot.metachar+host.example" "http://meta:30003" "$fixture" "regex metacharacters in host are literal"
  run_case "dotXmetachar+hostYexample" "" "$fixture" "dots are not wildcards"
  fixture='ingress:
  - hostname: auth.example
    path: /admin
    service: http_status:404
  - hostname: auth.example
    service: http://kc:8080'
  run_case auth.example "" "$fixture" "non-ws path rule never reports its service"
  return $rc
}
case "${1:-}" in --selftest) selftest; exit $? ;; esac

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

want_np="$(run "kubectl -n $NS get svc $GW_SVC -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null")"
[ -n "$want_np" ] || note "WARN could not read $GW_SVC nodePort (kubectl unavailable from here) — skipping that check"
for h in $APEX_HOSTS; do
  ws_target="$(ws_target_for "$h" "$live_raw")"
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

# 6. the changed security contracts, proven at their source --------------------------------------
# 6a. WS_ALLOWED_ORIGINS as actually deployed (an unauthenticated handshake 401s before the origin
#     check runs, so the HTTP probes above cannot see the allow-list; the Deployment env can).
#     Covers the prod cluster only — the es4 allow-list lives on the other cluster's kubeconfig.
deployed_origins="$(run "kubectl -n $NS get deploy $GW_DEPLOY -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"WS_ALLOWED_ORIGINS\")].value}' 2>/dev/null")"
if [ -z "$deployed_origins" ]; then
  note "WARN could not read $GW_DEPLOY WS_ALLOWED_ORIGINS (kubectl unavailable from here) — skipping"
else
  note "     deployed WS_ALLOWED_ORIGINS=$deployed_origins"
  for o in $EXPECTED_WS_ORIGINS; do
    case ",$deployed_origins," in
      *",$o,"*) note "OK   allow-list carries $o" ;;
      *) bad "deployed WS_ALLOWED_ORIGINS is missing '$o'" ;;
    esac
  done
fi

# 6b. the OIDC issuer value (status 200 alone proves nothing about WHICH issuer is being served).
for h in $AUTH_HOSTS; do
  iss="$(curl -s -m 20 "https://$h/realms/optionsedge/.well-known/openid-configuration" 2>/dev/null \
        | grep -o '"issuer" *: *"[^"]*"' | head -1 | sed -E 's/.*: *"//; s/"$//')"
  if [ "$iss" = "$EXPECTED_ISSUER" ]; then note "OK   $h issuer = $iss"
  else bad "$h issuer = '${iss:-<none>}' (expected $EXPECTED_ISSUER)"; fi
done

# 6c. the web surface itself: page serves, and the REST API refuses anonymous callers.
for h in $APEX_HOSTS $ES_HOSTS; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h/" 2>/dev/null)"
  [ "$code" = "200" ] && note "OK   $h/ -> 200" || bad "$h/ -> $code (expected 200)"
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h/api/config" 2>/dev/null)"
  [ "$code" = "401" ] && note "OK   $h/api/config unauthenticated -> 401" || bad "$h/api/config unauthenticated -> $code (expected 401)"
done

# 6d. OPTIONAL end-to-end origin proof: with a real bearer (grab one from a browser session's WS
#     token request), an allowed Origin completes the upgrade (101) and a hostile one must not.
if [ -n "${WS_PROBE_TOKEN:-}" ]; then
  for probe in "https://bleadingoptions.com 101" "https://evil.invalid not101"; do
    o="${probe% *}"; want="${probe#* }"
    code="$(curl -s -o /dev/null -w '%{http_code}' -m 8 \
            -H 'Connection: Upgrade' -H 'Upgrade: websocket' -H "Origin: $o" \
            -H "Authorization: Bearer $WS_PROBE_TOKEN" \
            -H "Sec-WebSocket-Key: $(head -c16 /dev/urandom | base64)" -H 'Sec-WebSocket-Version: 13' \
            "https://fullfunding.nl/ws/events" 2>/dev/null)"
    if [ "$want" = "101" ]; then
      [ "$code" = "101" ] && note "OK   authenticated upgrade with Origin $o -> 101" || bad "authenticated upgrade with Origin $o -> $code (expected 101)"
    else
      [ "$code" != "101" ] && note "OK   authenticated upgrade with hostile Origin $o rejected ($code)" || bad "authenticated upgrade with hostile Origin $o was ACCEPTED"
    fi
  done
else
  note "     (set WS_PROBE_TOKEN=<bearer> to also prove the origin allow-list end-to-end)"
fi

[ "$fail" -eq 0 ] && echo "  prod tunnel VERIFIED" || echo "  prod tunnel has PROBLEMS (see above)" >&2
exit "$fail"
