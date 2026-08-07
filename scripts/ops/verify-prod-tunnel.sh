#!/usr/bin/env bash
# verify-prod-tunnel.sh — prove the LIVE prod Cloudflare tunnel still matches the reviewed repo copy,
# and that every public hostname behaves per the current migration phase.
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
# PHASES (fullfunding.nl -> bleadingoptions.com migration; docs/domain-migration-bleadingoptions.md)
#   --phase dual      both domains serve everything (Phase 1 acceptance gate; default)
#   --phase redirect  new domain serves; every OLD hostname 307/308-redirects, host-mapped, with
#                     path+query preserved (Phase 2 acceptance gate)
#   --phase retired   like redirect, but the old origins must be GONE from both gateway
#                     allow-lists (Phase 3 acceptance gate)
#
# MODES
#   default           GATE mode: every check that cannot run (e.g. kubectl unreachable) FAILS —
#                     an unverifiable contract is a failed contract at phase acceptance.
#   --network-only    diagnostic mode: kube-dependent checks downgrade to warnings so the network
#                     surface can be probed from a box without cluster credentials.
#   --selftest        run the embedded ingress-parser fixtures and exit.
#
# USAGE
#   scripts/ops/verify-prod-tunnel.sh [--phase dual|redirect|retired] [--network-only]
#   PROD_SSH="ssh user@192.168.100.252" scripts/ops/verify-prod-tunnel.sh   # remote prod reads
#   ES4_SSH="ssh user@192.168.100.4"    …                                    # remote es4 reads
set -uo pipefail

PHASE="${TUNNEL_PHASE:-dual}"
NETWORK_ONLY=0
SELFTEST=0
while [ $# -gt 0 ]; do
  case "$1" in
    --phase) PHASE="${2:?--phase needs dual|redirect|retired}"; shift 2 ;;
    --network-only) NETWORK_ONLY=1; shift ;;
    --selftest) SELFTEST=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
case "$PHASE" in dual|redirect|retired) ;; *) echo "bad --phase '$PHASE'" >&2; exit 2 ;; esac

REPO_COPY="${REPO_COPY:-$(cd "$(dirname "$0")/../.." && pwd)/infra/prod/cloudflared/options-edge-stable.yml}"
LIVE_PATH="${LIVE_PATH:-/etc/cloudflared/options-edge-stable.yml}"
PROD_SSH="${PROD_SSH:-}"
ES4_SSH="${ES4_SSH:-ssh abhinav@192.168.100.4}"
GW_SVC="${GW_SVC:-feed-gateway-service}"
GW_DEPLOY="${GW_DEPLOY:-feed-gateway-service}"
ES4_GW_DEPLOY="${ES4_GW_DEPLOY:-es-feed-gateway}"
NS="${NS:-options-edge}"
ES4_LAN_ORIGIN="${ES4_LAN_ORIGIN:-http://192.168.100.4:30080}"

# Per-phase expectations. Every default is overridable, but the phase picks coherent defaults so
# the gate cannot silently run with a stale matrix. KC_HOSTNAME pins ONE issuer for all auth hosts.
case "$PHASE" in
  dual)
    SERVE_APEX_DEFAULT="fullfunding.nl bleadingoptions.com"
    SERVE_ES_DEFAULT="es.fullfunding.nl es.bleadingoptions.com"
    SERVE_AUTH_DEFAULT="auth.fullfunding.nl auth.bleadingoptions.com"
    REDIR_HOSTS_DEFAULT=""
    EXPECTED_ISSUER_DEFAULT="https://auth.fullfunding.nl/realms/optionsedge"
    PROD_ORIGINS_DEFAULT="https://fullfunding.nl https://bleadingoptions.com"
    ES4_ORIGINS_DEFAULT="https://es.fullfunding.nl https://es.bleadingoptions.com $ES4_LAN_ORIGIN"
    ;;
  redirect)
    SERVE_APEX_DEFAULT="bleadingoptions.com"
    SERVE_ES_DEFAULT="es.bleadingoptions.com"
    SERVE_AUTH_DEFAULT="auth.bleadingoptions.com"
    REDIR_HOSTS_DEFAULT="fullfunding.nl es.fullfunding.nl auth.fullfunding.nl"
    EXPECTED_ISSUER_DEFAULT="https://auth.bleadingoptions.com/realms/optionsedge"
    # Old origins stay trusted during the redirect soak; Phase 3 removes them.
    PROD_ORIGINS_DEFAULT="https://fullfunding.nl https://bleadingoptions.com"
    ES4_ORIGINS_DEFAULT="https://es.fullfunding.nl https://es.bleadingoptions.com $ES4_LAN_ORIGIN"
    ;;
  retired)
    SERVE_APEX_DEFAULT="bleadingoptions.com"
    SERVE_ES_DEFAULT="es.bleadingoptions.com"
    SERVE_AUTH_DEFAULT="auth.bleadingoptions.com"
    REDIR_HOSTS_DEFAULT="fullfunding.nl es.fullfunding.nl auth.fullfunding.nl"
    EXPECTED_ISSUER_DEFAULT="https://auth.bleadingoptions.com/realms/optionsedge"
    PROD_ORIGINS_DEFAULT="https://bleadingoptions.com"
    ES4_ORIGINS_DEFAULT="https://es.bleadingoptions.com $ES4_LAN_ORIGIN"
    ;;
esac
SERVE_APEX="${SERVE_APEX:-$SERVE_APEX_DEFAULT}"
SERVE_ES="${SERVE_ES:-$SERVE_ES_DEFAULT}"
SERVE_AUTH="${SERVE_AUTH:-$SERVE_AUTH_DEFAULT}"
REDIR_HOSTS="${REDIR_HOSTS:-$REDIR_HOSTS_DEFAULT}"
EXPECTED_ISSUER="${EXPECTED_ISSUER:-$EXPECTED_ISSUER_DEFAULT}"
EXPECTED_PROD_ORIGINS="${EXPECTED_PROD_ORIGINS:-$PROD_ORIGINS_DEFAULT}"
EXPECTED_ES4_ORIGINS="${EXPECTED_ES4_ORIGINS:-$ES4_ORIGINS_DEFAULT}"

redirect_target_for() {  # old host -> new host (host-mapped)
  case "$1" in
    fullfunding.nl) echo "bleadingoptions.com" ;;
    es.fullfunding.nl) echo "es.bleadingoptions.com" ;;
    auth.fullfunding.nl) echo "auth.bleadingoptions.com" ;;
    *) echo "" ;;
  esac
}

run()     { if [ -n "$PROD_SSH" ]; then $PROD_SSH "$1"; else bash -c "$1"; fi; }
run_es4() { if [ -n "$ES4_SSH" ]; then $ES4_SSH "$1"; else bash -c "$1"; fi; }
strip() { grep -vE '^\s*#|^\s*$' | sed 's/[[:space:]]*#.*$//' | sed 's/[[:space:]]*$//'; }
fail=0
note() { echo "  $*"; }
bad()  { echo "  FAIL: $*" >&2; fail=1; }
# A check that cannot run is a FAILED check at phase acceptance; --network-only downgrades it.
unavailable() { if [ "$NETWORK_ONLY" = "1" ]; then note "WARN $* (network-only mode: skipped)"; else bad "$* — unverifiable contract fails the gate (use --network-only for a network-surface-only diagnostic)"; fi; }

# --- ingress parser (literal hostname compare; ALL state resets on EVERY list item, because a
# hostless rule is legal and matches every hostname — it must never lend its service line) -------
ws_target_for() {  # $1 = hostname; $2 = config text; prints that host's /ws/events service target
  printf '%s\n' "$2" | awk -v h="$1" '
    function fieldval(line, key,    v) { v=line; sub(".*" key ":[[:space:]]*", "", v);
      sub(/[[:space:]]*#.*/, "", v); sub(/[[:space:]]+$/, "", v); return v }
    /^[[:space:]]*-([[:space:]]|$)/ { cur=""; haspath=0 }              # every list item resets
    /^[[:space:]]*-[[:space:]]*hostname:/ { cur=fieldval($0, "hostname"); next }
    /path:[[:space:]]*\/ws\/events([[:space:]]|$)/ { if (cur == h) haspath=1; next }
    /service:/ { if (cur == h && haspath) { print fieldval($0, "service"); exit } haspath=0 }'
}

selftest() {
  local fixture got rc=0
  run_case() {  # host, expected, config, label
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
  - hostname: a.example
    path: /ws/events
  - path: /health
    service: http://borrowed:39999
  - service: http_status:404'
  run_case a.example "" "$fixture" "hostless rule (legal, matches all hosts) never lends its service"
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
[ "$SELFTEST" = "1" ] && { selftest; exit $?; }

[ -f "$REPO_COPY" ] || { echo "FATAL: repo copy not found at $REPO_COPY" >&2; exit 2; }
note "phase=$PHASE $([ "$NETWORK_ONLY" = "1" ] && echo '(network-only diagnostic)')"

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

# 2 + 3. the /ws/events routes (every SERVING apex hostname) -----------------------------------
# Each serving apex hostname must route /ws/events at the prod gateway NodePort — never the :8091
# ServiceLB (2026-07-31 outage). The es.* hostnames route to the es4 box (.4:30091), whose
# NodePort belongs to the other cluster, so they are covered by the live 401 probes below.
want_np="$(run "kubectl -n $NS get svc $GW_SVC -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null")"
[ -n "$want_np" ] || unavailable "could not read $GW_SVC nodePort"
for h in $SERVE_APEX; do
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
# 1006/000 means it died in a proxy on the way. Covers both clusters (apex=prod, es.*=es4). A
# brand-new hostname failing here usually means its DNS CNAME is not live yet.
for h in $SERVE_APEX $SERVE_ES; do
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

# 5. auth hostnames: OIDC discovery must serve the EXACT expected issuer; admin edge-blocked ----
# (status 200 alone proves nothing about WHICH issuer is being served)
for h in $SERVE_AUTH; do
  disc="$(curl -s -m 20 "https://$h/realms/optionsedge/.well-known/openid-configuration" 2>/dev/null)"
  iss="$(printf '%s' "$disc" | grep -o '"issuer" *: *"[^"]*"' | head -1 | sed -E 's/.*: *"//; s/"$//')"
  if [ "$iss" = "$EXPECTED_ISSUER" ]; then note "OK   $h issuer = $iss"
  else bad "$h issuer = '${iss:-<none>}' (expected $EXPECTED_ISSUER)"; fi
  for path in /admin /realms/master; do
    code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h$path" 2>/dev/null)"
    case "$code" in
      404) note "OK   $h$path -> 404 (edge-blocked)" ;;
      *)   bad "$h$path -> $code (expected 404 — the admin surface must stay edge-blocked)" ;;
    esac
  done
done

# 6. gateway origin allow-lists, proven at their source on BOTH clusters ------------------------
# An unauthenticated handshake 401s before the origin check runs (verified empirically), so HTTP
# probes cannot see the allow-list; the Deployment env can. EXACT set equality: an unexpected
# extra origin (worst case '*') is as much a failure as a missing one.
origin_set_check() {  # label, deployed-csv, expected space-list
  local label="$1" csv="$2" expected="$3"
  if [ -z "$csv" ]; then unavailable "could not read $label WS_ALLOWED_ORIGINS"; return; fi
  note "     $label WS_ALLOWED_ORIGINS=$csv"
  local deployed_sorted expected_sorted
  deployed_sorted="$(printf '%s' "$csv" | tr ',' '\n' | sed 's/^ *//; s/ *$//' | grep -v '^$' | sort -u)"
  expected_sorted="$(printf '%s\n' $expected | sort -u)"
  if [ "$deployed_sorted" = "$expected_sorted" ]; then
    note "OK   $label allow-list matches the expected set exactly"
  else
    bad "$label allow-list differs from the expected set"
    diff <(printf '%s\n' "$expected_sorted") <(printf '%s\n' "$deployed_sorted") | sed 's/^</       missing: /; s/^>/       unexpected: /' | grep -v '^---' | sed 's/^/  /'
  fi
}
prod_csv="$(run "kubectl -n $NS get deploy $GW_DEPLOY -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"WS_ALLOWED_ORIGINS\")].value}' 2>/dev/null")"
origin_set_check "prod $GW_DEPLOY" "$prod_csv" "$EXPECTED_PROD_ORIGINS"
es4_csv="$(run_es4 "KC=\$(command -v kubectl); sudo -n env KUBECONFIG=/etc/rancher/k3s/k3s.yaml \$KC -n $NS get deploy $ES4_GW_DEPLOY -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"WS_ALLOWED_ORIGINS\")].value}' 2>/dev/null")"
origin_set_check "es4 $ES4_GW_DEPLOY" "$es4_csv" "$EXPECTED_ES4_ORIGINS"

# 7. the web surface itself: page serves, and the REST API refuses anonymous callers ------------
for h in $SERVE_APEX $SERVE_ES; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h/" 2>/dev/null)"
  [ "$code" = "200" ] && note "OK   $h/ -> 200" || bad "$h/ -> $code (expected 200)"
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h/api/config" 2>/dev/null)"
  [ "$code" = "401" ] && note "OK   $h/api/config unauthenticated -> 401" || bad "$h/api/config unauthenticated -> $code (expected 401)"
done

# 8. redirect phases: every OLD hostname must 307/308 host-mapped with path+query preserved -----
for h in $REDIR_HOSTS; do
  target_host="$(redirect_target_for "$h")"
  if [ -z "$target_host" ]; then bad "no redirect mapping defined for $h"; continue; fi
  hdrs="$(curl -s -o /dev/null -D - -m 20 "https://$h/board?x=1" 2>/dev/null)"
  code="$(printf '%s' "$hdrs" | head -1 | awk '{print $2}')"
  loc="$(printf '%s' "$hdrs" | grep -i '^location:' | head -1 | sed -E 's/^[Ll]ocation:[[:space:]]*//; s/\r$//')"
  want="https://$target_host/board?x=1"
  case "$code" in
    307|308) note "OK   $h -> $code" ;;
    *) bad "$h -> ${code:-<none>} (expected 307/308 redirect)" ;;
  esac
  if [ "$loc" = "$want" ]; then note "OK   $h Location preserves host-map + path + query ($loc)"
  else bad "$h Location = '${loc:-<none>}' (expected $want)"; fi
done

[ "$fail" -eq 0 ] && echo "  prod tunnel VERIFIED (phase=$PHASE)" || echo "  prod tunnel has PROBLEMS (see above)" >&2
exit "$fail"
