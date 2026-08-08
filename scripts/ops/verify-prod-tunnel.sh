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
#   SSHOPTS="-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3"
#   timeout 600 env PROD_SSH="ssh $SSHOPTS user@192.168.100.252" ES4_SSH="ssh $SSHOPTS user@192.168.100.4" \
#     scripts/ops/verify-prod-tunnel.sh --phase …
#   (BatchMode+ConnectTimeout stop prompts/handshake hangs; ServerAlive kills stalled established
#   streams; the outer `timeout 600` is the whole-gate deadline — a gate that never returns is a
#   gate that failed open)
set -uo pipefail

PHASE="${TUNNEL_PHASE:-dual}"
NETWORK_ONLY=0
SELFTEST=0
PRECHECK=0
while [ $# -gt 0 ]; do
  case "$1" in
    --phase) PHASE="${2:?--phase needs dual|redirect|retired}"; shift 2 ;;
    --network-only) NETWORK_ONLY=1; shift ;;
    --precheck) PRECHECK=1; shift ;;   # skip ONLY the redirect section; output stamped PRECHECK
    --selftest) SELFTEST=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
case "$PHASE" in dual|redirect|retired) ;; *) echo "bad --phase '$PHASE'" >&2; exit 2 ;; esac
if [ "$NETWORK_ONLY" = "1" ] && [ "$PRECHECK" = "1" ]; then
  echo "FATAL: --network-only --precheck is meaningless — precheck exists to prove the kube/runtime contracts, which network-only skips" >&2
  exit 2
fi

REPO_COPY="${REPO_COPY:-$(cd "$(dirname "$0")/../.." && pwd)/infra/prod/cloudflared/options-edge-stable.yml}"
LIVE_PATH="${LIVE_PATH:-/etc/cloudflared/options-edge-stable.yml}"
PROD_SSH="${PROD_SSH:-}"
ES4_SSH="${ES4_SSH:-ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 abhinav@192.168.100.4}"
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
    EXPECTED_REDIRECT_STATUS_DEFAULT=""
    EXPECTED_ISSUER_DEFAULT="https://auth.fullfunding.nl/realms/optionsedge"
    PROD_ORIGINS_DEFAULT="https://fullfunding.nl https://bleadingoptions.com"
    ES4_ORIGINS_DEFAULT="https://es.fullfunding.nl https://es.bleadingoptions.com $ES4_LAN_ORIGIN"
    PRIMARY_APEX_DEFAULT="fullfunding.nl"; PRIMARY_ES_DEFAULT="es.fullfunding.nl"
    ABSENT_TUNNEL_HOSTS_DEFAULT=""
    KC_REDIRECTS_DEFAULT="https://fullfunding.nl/* https://es.fullfunding.nl/* https://bleadingoptions.com/* https://es.bleadingoptions.com/* http://192.168.100.4:30080/* http://192.168.100.252:8094/* http://192.168.100.103:8094/*"
    KC_WEBORIGINS_DEFAULT="https://fullfunding.nl https://es.fullfunding.nl https://bleadingoptions.com https://es.bleadingoptions.com http://192.168.100.4:30080 http://192.168.100.252:8094 http://192.168.100.103:8094"
    KC_POSTLOGOUT_DEFAULT="https://fullfunding.nl/* https://es.fullfunding.nl/* https://bleadingoptions.com/* https://es.bleadingoptions.com/*"
    ;;
  redirect)
    SERVE_APEX_DEFAULT="bleadingoptions.com"
    SERVE_ES_DEFAULT="es.bleadingoptions.com"
    SERVE_AUTH_DEFAULT="auth.bleadingoptions.com"
    REDIR_HOSTS_DEFAULT="fullfunding.nl es.fullfunding.nl auth.fullfunding.nl"
    EXPECTED_REDIRECT_STATUS_DEFAULT="307"   # soak on temporary; 'retired' demands the promoted 308
    EXPECTED_ISSUER_DEFAULT="https://auth.bleadingoptions.com/realms/optionsedge"
    # Old origins stay trusted during the redirect soak; Phase 3 removes them.
    PROD_ORIGINS_DEFAULT="https://fullfunding.nl https://bleadingoptions.com"
    ES4_ORIGINS_DEFAULT="https://es.fullfunding.nl https://es.bleadingoptions.com $ES4_LAN_ORIGIN"
    PRIMARY_APEX_DEFAULT="bleadingoptions.com"; PRIMARY_ES_DEFAULT="es.bleadingoptions.com"
    ABSENT_TUNNEL_HOSTS_DEFAULT=""
    KC_REDIRECTS_DEFAULT="https://fullfunding.nl/* https://es.fullfunding.nl/* https://bleadingoptions.com/* https://es.bleadingoptions.com/* http://192.168.100.4:30080/* http://192.168.100.252:8094/* http://192.168.100.103:8094/*"
    KC_WEBORIGINS_DEFAULT="https://fullfunding.nl https://es.fullfunding.nl https://bleadingoptions.com https://es.bleadingoptions.com http://192.168.100.4:30080 http://192.168.100.252:8094 http://192.168.100.103:8094"
    KC_POSTLOGOUT_DEFAULT="https://fullfunding.nl/* https://es.fullfunding.nl/* https://bleadingoptions.com/* https://es.bleadingoptions.com/*"
    ;;
  retired)
    SERVE_APEX_DEFAULT="bleadingoptions.com"
    SERVE_ES_DEFAULT="es.bleadingoptions.com"
    SERVE_AUTH_DEFAULT="auth.bleadingoptions.com"
    REDIR_HOSTS_DEFAULT="fullfunding.nl es.fullfunding.nl auth.fullfunding.nl"
    EXPECTED_REDIRECT_STATUS_DEFAULT="308"
    EXPECTED_ISSUER_DEFAULT="https://auth.bleadingoptions.com/realms/optionsedge"
    PROD_ORIGINS_DEFAULT="https://bleadingoptions.com"
    ES4_ORIGINS_DEFAULT="https://es.bleadingoptions.com $ES4_LAN_ORIGIN"
    PRIMARY_APEX_DEFAULT="bleadingoptions.com"; PRIMARY_ES_DEFAULT="es.bleadingoptions.com"
    # Retirement must be provable in the config itself, not hidden behind the edge redirects.
    ABSENT_TUNNEL_HOSTS_DEFAULT="fullfunding.nl es.fullfunding.nl auth.fullfunding.nl"
    KC_REDIRECTS_DEFAULT="https://bleadingoptions.com/* https://es.bleadingoptions.com/* http://192.168.100.4:30080/* http://192.168.100.252:8094/* http://192.168.100.103:8094/*"
    KC_WEBORIGINS_DEFAULT="https://bleadingoptions.com https://es.bleadingoptions.com http://192.168.100.4:30080 http://192.168.100.252:8094 http://192.168.100.103:8094"
    KC_POSTLOGOUT_DEFAULT="https://bleadingoptions.com/* https://es.bleadingoptions.com/*"
    ;;
esac
SERVE_APEX="${SERVE_APEX-$SERVE_APEX_DEFAULT}"
SERVE_ES="${SERVE_ES-$SERVE_ES_DEFAULT}"
SERVE_AUTH="${SERVE_AUTH-$SERVE_AUTH_DEFAULT}"
REDIR_HOSTS="${REDIR_HOSTS-$REDIR_HOSTS_DEFAULT}"   # dash (not :-) so REDIR_HOSTS="" is a real override
EXPECTED_ISSUER="${EXPECTED_ISSUER:-$EXPECTED_ISSUER_DEFAULT}"
EXPECTED_PROD_ORIGINS="${EXPECTED_PROD_ORIGINS:-$PROD_ORIGINS_DEFAULT}"
EXPECTED_ES4_ORIGINS="${EXPECTED_ES4_ORIGINS:-$ES4_ORIGINS_DEFAULT}"
EXPECTED_REDIRECT_STATUS="${EXPECTED_REDIRECT_STATUS:-$EXPECTED_REDIRECT_STATUS_DEFAULT}"
PRIMARY_APEX="${PRIMARY_APEX:-$PRIMARY_APEX_DEFAULT}"
PRIMARY_ES="${PRIMARY_ES:-$PRIMARY_ES_DEFAULT}"
ABSENT_TUNNEL_HOSTS="${ABSENT_TUNNEL_HOSTS-$ABSENT_TUNNEL_HOSTS_DEFAULT}"
EXPECTED_KC_REDIRECTS="${EXPECTED_KC_REDIRECTS:-$KC_REDIRECTS_DEFAULT}"
EXPECTED_KC_WEBORIGINS="${EXPECTED_KC_WEBORIGINS:-$KC_WEBORIGINS_DEFAULT}"
EXPECTED_KC_POSTLOGOUT="${EXPECTED_KC_POSTLOGOUT:-$KC_POSTLOGOUT_DEFAULT}"
# Normalize every host set first: whitespace-only values would satisfy -n yet expand to zero loop
# iterations — the exact fail-open the guards exist to stop.
trimset() { printf '%s' "$1" | xargs 2>/dev/null || true; }
SERVE_APEX="$(trimset "$SERVE_APEX")"; SERVE_ES="$(trimset "$SERVE_ES")"; SERVE_AUTH="$(trimset "$SERVE_AUTH")"
REDIR_HOSTS="$(trimset "$REDIR_HOSTS")"; ABSENT_TUNNEL_HOSTS="$(trimset "$ABSENT_TUNNEL_HOSTS")"

# The serving surface is mandatory in EVERY mode — an inherited SERVE_*='' must never suppress
# the page/WS/discovery/admin probes and still print a passing stamp.
[ -n "$SERVE_APEX" ] && [ -n "$SERVE_ES" ] && [ -n "$SERVE_AUTH" ] \
  || { echo "FATAL: empty SERVE_* set — the serving surface cannot be skipped in any mode" >&2; exit 2; }

# In acceptance mode the sets must be EXACTLY the phase defaults: a subset override (e.g. only the
# new apex) would silently shrink the matrix and still stamp VERIFIED. ALLOW_SET_OVERRIDES=1 is
# the named escape for deliberate diagnostics — its runs are stamped as such below.
require_default() {  # name, got, phase-default — exact match or FATAL
  local got want
  got="$(trimset "$2")"; want="$(trimset "$3")"
  if [ "$got" != "$want" ]; then
    echo "FATAL: $1 overridden ('$got' != phase default '$want') — set ALLOW_SET_OVERRIDES=1 for a deliberately reduced diagnostic run (its stamp will say so)" >&2
    exit 2
  fi
}
if [ "$NETWORK_ONLY" != "1" ] && [ "${ALLOW_SET_OVERRIDES:-0}" != "1" ]; then
  # Enforced in PRECHECK too — precheck's only concession is not RUNNING the redirect probes;
  # its serving/absence matrices must be the real ones or its green means nothing.
  require_default SERVE_APEX "$SERVE_APEX" "$SERVE_APEX_DEFAULT"
  require_default SERVE_ES "$SERVE_ES" "$SERVE_ES_DEFAULT"
  require_default SERVE_AUTH "$SERVE_AUTH" "$SERVE_AUTH_DEFAULT"
  require_default ABSENT_TUNNEL_HOSTS "$ABSENT_TUNNEL_HOSTS" "$ABSENT_TUNNEL_HOSTS_DEFAULT"
  [ "$PRECHECK" != "1" ] && require_default REDIR_HOSTS "$REDIR_HOSTS" "$REDIR_HOSTS_DEFAULT"
fi
if [ "$PRECHECK" = "1" ]; then
  # Precheck exists for exactly one moment: the redirect phase before its rules are created.
  [ "$PHASE" = "redirect" ] || { echo "FATAL: --precheck is only meaningful with --phase redirect" >&2; exit 2; }
  REDIR_HOSTS=""   # the ONLY exemption precheck grants
else
  if [ "$PHASE" != "dual" ] && [ -z "$REDIR_HOSTS" ]; then
    echo "FATAL: empty REDIR_HOSTS in $PHASE acceptance mode — use --precheck for the pre-redirect run" >&2; exit 2
  fi
  if [ "$PHASE" = "retired" ] && [ -z "$ABSENT_TUNNEL_HOSTS" ]; then
    echo "FATAL: empty ABSENT_TUNNEL_HOSTS in retired acceptance mode" >&2; exit 2
  fi
fi

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
note "phase=$PHASE $([ "$NETWORK_ONLY" = "1" ] && echo '(network-only diagnostic)')$([ "$PRECHECK" = "1" ] && echo '(PRECHECK: redirect section skipped)')"

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

# 1b. retirement is proven in the config text, not hidden behind the edge redirects --------------
for h in $ABSENT_TUNNEL_HOSTS; do
  h_re="$(printf '%s' "$h" | sed 's/\./\\./g')"
  if printf '%s\n' "$live_raw" | grep -qiE "hostname:[[:space:]]*[\"']?$h_re[\"']?([[:space:]]|\$)"; then
    bad "retired hostname '$h' still has ingress rules in the LIVE tunnel config"
  else
    note "OK   retired hostname '$h' absent from the live tunnel config"
  fi
  if grep -qiE "hostname:[[:space:]]*[\"']?$h_re[\"']?([[:space:]]|\$)" "$REPO_COPY"; then
    bad "retired hostname '$h' still has ingress rules in the REPO canonical config"
  else
    note "OK   retired hostname '$h' absent from the repo canonical config"
  fi
done

# 1c. retired: the legacy traefik Ingress must not route the old auth hostname either ------------
if [ -n "$ABSENT_TUNNEL_HOSTS" ]; then
  ING_FILE="${ING_FILE:-$(cd "$(dirname "$0")/../.." && pwd)/k8s/keycloak/keycloak-ingress.yaml}"
  if [ ! -r "$ING_FILE" ]; then
    bad "repo keycloak-ingress.yaml not readable at $ING_FILE — cannot prove ingress retirement"
  else
    grep -qiE "host:[[:space:]]*[\"']?auth\.fullfunding\.nl[\"']?([[:space:]]|\$)" "$ING_FILE"
    case $? in
      0) bad "repo keycloak-ingress.yaml still routes auth.fullfunding.nl after retirement" ;;
      1) note "OK   repo keycloak-ingress.yaml carries no old auth host" ;;
      *) bad "grep error reading $ING_FILE — cannot prove ingress retirement" ;;
    esac
  fi
  live_ing_hosts="$(run "kubectl --request-timeout=${K8S_TIMEOUT:-20s} -n $NS get ingress oe-keycloak -o jsonpath='{.spec.rules[*].host}' 2>/dev/null")"
  if [ -z "$live_ing_hosts" ]; then
    unavailable "could not read the live oe-keycloak Ingress host set"
  elif printf '%s' "$live_ing_hosts" | grep -qi "auth\.fullfunding\.nl"; then
    bad "live oe-keycloak Ingress still routes auth.fullfunding.nl after retirement ($live_ing_hosts)"
  else
    note "OK   live oe-keycloak Ingress carries no old auth host ($live_ing_hosts)"
  fi
fi

# 2 + 3. the /ws/events routes (every SERVING apex hostname) -----------------------------------
# Each serving apex hostname must route /ws/events at the prod gateway NodePort — never the :8091
# ServiceLB (2026-07-31 outage). The es.* hostnames route to the es4 box (.4:30091), whose
# NodePort belongs to the other cluster, so they are covered by the live 401 probes below.
want_np="$(run "kubectl --request-timeout=${K8S_TIMEOUT:-20s} -n $NS get svc $GW_SVC -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null")"
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

# --request-timeout bounds every k8s call: ConnectTimeout only bounds the ssh handshake, and an
# acceptance gate that can hang on a wedged apiserver does not fail closed.
K8S_TIMEOUT="${K8S_TIMEOUT:-20s}"
# --request-timeout bounds each API request; exec additionally needs --pod-running-timeout (its own
# default is 1 minute of waiting for a running pod). Neither bounds a stalled established stream —
# that is what the ssh ServerAlive options in the documented examples and the recommended outer
# `timeout 600 …` invocation are for.
KEXEC_OPTS="--request-timeout=$K8S_TIMEOUT --pod-running-timeout=$K8S_TIMEOUT"
prod_kubectl() { run "kubectl --request-timeout=$K8S_TIMEOUT -n $NS $1 2>/dev/null"; }
prod_kexec()   { run "kubectl $KEXEC_OPTS -n $NS exec $1 2>/dev/null"; }
es4_kubectl()  { run_es4 "KC=\$(command -v kubectl); sudo -n env KUBECONFIG=/etc/rancher/k3s/k3s.yaml \$KC --request-timeout=$K8S_TIMEOUT -n $NS $1 2>/dev/null"; }
k8s_on() { if [ "$1" = "prod" ]; then prod_kubectl "$2"; else es4_kubectl "$2"; fi; }

rollout_settled() {  # cluster, deploy — a COMPLETE rollout, not a mixed-version snapshot:
  # generation observed, and desired == total == updated == ready == available ('total' catches a
  # lingering old replica that would otherwise supply 'ready' while the new pod supplies
  # 'updated'). 0-replica deploys settle trivially.
  local fields
  fields="$(k8s_on "$1" "get deploy $2 -o jsonpath='{.metadata.generation} {.status.observedGeneration} {.spec.replicas} {.status.replicas} {.status.updatedReplicas} {.status.readyReplicas} {.status.availableReplicas}'")"
  [ -z "$fields" ] && { unavailable "could not read $1/$2 rollout state"; return 1; }
  set -- $fields
  local gen="${1:-0}" ogen="${2:-0}" want="${3:-0}" total="${4:-0}" updated="${5:-0}" ready="${6:-0}" avail="${7:-0}"
  if [ "$gen" != "$ogen" ]; then
    bad "$1/$2 rollout not observed yet (generation=$gen observed=$ogen) — effective env below may be stale"; return 0
  fi
  if [ "$want" = "0" ] && [ "$total" = "0" ]; then return 0; fi
  if [ "$want" = "$total" ] && [ "$want" = "$updated" ] && [ "$want" = "$ready" ] && [ "$want" = "$avail" ]; then return 0; fi
  bad "$1/$2 rollout unsettled (spec=$want total=$total updated=$updated ready=$ready available=$avail) — a mixed-version state; effective env below may be stale"
  return 0
}


# 5. auth hostnames: OIDC discovery must serve the EXACT expected issuer; admin edge-blocked ----
# (status 200 alone proves nothing about WHICH issuer is being served)
# Keycloak must be SETTLED before discovery is trusted: a RollingUpdate briefly runs old- and
# new-issuer pods side by side and one request can hit either — the mixed-issuer hazard Phase 2
# declares. Settlement precedes the probes so a rollout finishing on a misconfigured pod cannot
# slip in after a lucky early discovery hit.
rollout_settled prod "${KC_DEPLOY:-oe-keycloak}"
for h in $SERVE_AUTH; do
  disc="$(curl -s -m 20 -w '\n%{http_code}' "https://$h/realms/optionsedge/.well-known/openid-configuration" 2>/dev/null)"
  disc_code="$(printf '%s' "$disc" | tail -1)"
  disc_body="$(printf '%s' "$disc" | sed '$d')"
  if [ "$disc_code" != "200" ]; then
    bad "$h OIDC discovery -> ${disc_code:-<none>} (expected 200)"
  fi
  iss="$(printf '%s' "$disc_body" | python3 -c "
import json, sys
try: print(json.load(sys.stdin).get('issuer', ''))
except Exception: pass" 2>/dev/null)"
  if [ "$iss" = "$EXPECTED_ISSUER" ]; then note "OK   $h discovery 200, issuer = $iss"
  else bad "$h issuer = '${iss:-<unparsable>}' (expected $EXPECTED_ISSUER; discovery must be valid JSON)"; fi
  for path in /admin /realms/master; do
    code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h$path" 2>/dev/null)"
    case "$code" in
      404) note "OK   $h$path -> 404 (edge-blocked)" ;;
      *)   bad "$h$path -> $code (expected 404 — the admin surface must stay edge-blocked)" ;;
    esac
  done
done

# 6. security contracts proven in the RUNNING workloads on BOTH clusters ------------------------
# An unauthenticated handshake 401s before the origin check runs (verified empirically), so HTTP
# probes cannot see these values; the running pod's effective environment can (kubectl exec
# printenv — a template read alone could report a config an unfinished rollout is not serving).
eff_env() {  # cluster, deploy, container, var — the RUNNING container's effective value
  k8s_on "$1" "exec --pod-running-timeout=$K8S_TIMEOUT deploy/$2 -c $3 -- printenv $4"
}

env_must_equal() {  # cluster, deploy, container, var, expected
  local got
  got="$(eff_env "$1" "$2" "$3" "$4")"
  if [ -z "$got" ]; then unavailable "could not read effective $4 from $1/$2"; return; fi
  if [ "$got" = "$5" ]; then note "OK   $1/$2 $4 = $got"
  else bad "$1/$2 $4 = '$got' (expected '$5')"; fi
}

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

rollout_settled prod "$GW_DEPLOY"
origin_set_check "prod $GW_DEPLOY" "$(eff_env prod "$GW_DEPLOY" feed-gateway WS_ALLOWED_ORIGINS)" "$EXPECTED_PROD_ORIGINS"
env_must_equal prod "$GW_DEPLOY" feed-gateway WS_AUTH_ISSUER_URI "$EXPECTED_ISSUER"
rollout_settled es4 "$ES4_GW_DEPLOY"
origin_set_check "es4 $ES4_GW_DEPLOY" "$(eff_env es4 "$ES4_GW_DEPLOY" feed-gateway WS_ALLOWED_ORIGINS)" "$EXPECTED_ES4_ORIGINS"
env_must_equal es4 "$ES4_GW_DEPLOY" feed-gateway WS_AUTH_ISSUER_URI "$EXPECTED_ISSUER"

# 6b. web runtime URLs — RuntimeProfileConfig injects these envs into the page, so the browser's
# API/WS/auth targets are exactly what the running web pods carry. Primary hosts follow the phase.
WEB_DEPLOY="${WEB_DEPLOY:-options-edge-web}"
ES4_WEB_DEPLOY="${ES4_WEB_DEPLOY:-es-web}"
rollout_settled prod "$WEB_DEPLOY"
env_must_equal prod "$WEB_DEPLOY" web VITE_AUTH_ISSUER "$EXPECTED_ISSUER"
env_must_equal prod "$WEB_DEPLOY" web VITE_API_BASE_URL "https://$PRIMARY_APEX"
env_must_equal prod "$WEB_DEPLOY" web VITE_WS_URL "wss://$PRIMARY_APEX/ws/events"
env_must_equal prod "$WEB_DEPLOY" web APP_FEED_GATEWAY_WS_URL "wss://$PRIMARY_APEX/ws/events"
env_must_equal prod "$WEB_DEPLOY" web VITE_MISSION_CONTROL_URL "https://$PRIMARY_APEX"
env_must_equal prod "$WEB_DEPLOY" web VITE_REPLAY_ORCHESTRATOR_URL "https://$PRIMARY_APEX"
rollout_settled es4 "$ES4_WEB_DEPLOY"
env_must_equal es4 "$ES4_WEB_DEPLOY" web VITE_AUTH_ISSUER "$EXPECTED_ISSUER"
env_must_equal es4 "$ES4_WEB_DEPLOY" web VITE_API_BASE_URL "https://$PRIMARY_ES"
env_must_equal es4 "$ES4_WEB_DEPLOY" web VITE_WS_URL "wss://$PRIMARY_ES/ws/events"
env_must_equal es4 "$ES4_WEB_DEPLOY" web APP_FEED_GATEWAY_WS_URL "wss://$PRIMARY_ES/ws/events"
env_must_equal es4 "$ES4_WEB_DEPLOY" web VITE_MISSION_CONTROL_URL "https://$PRIMARY_ES"
env_must_equal es4 "$ES4_WEB_DEPLOY" web VITE_REPLAY_ORCHESTRATOR_URL "https://$PRIMARY_ES"
if [ "$PHASE" != "dual" ]; then
  # WebNav's compiled default still says the old domain; from Phase 2 the env override is
  # mandatory on BOTH web deployments (es-web renders the same nav).
  env_must_equal prod "$WEB_DEPLOY" web APP_ES_OPTIONS_URL "https://$PRIMARY_ES"
  env_must_equal es4 "$ES4_WEB_DEPLOY" web APP_ES_OPTIONS_URL "https://$PRIMARY_ES"
fi
# spx-mission-control declares 0 replicas — no pod to exec, so its desired template is the
# strongest available check (a stale issuer there boots broken on the next scale-up).
MC_DEPLOY="${MC_DEPLOY:-spx-mission-control-service}"
mc_template_env() { prod_kubectl "get deploy $MC_DEPLOY -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"$1\")].value}'"; }
mc_check() {  # var, expected
  local got; got="$(mc_template_env "$1")"
  if [ -z "$got" ]; then unavailable "could not read $MC_DEPLOY $1 template"
  elif [ "$got" = "$2" ]; then note "OK   $MC_DEPLOY (template, 0 replicas) $1 = $got"
  else bad "$MC_DEPLOY (template) $1 = '$got' (expected $2)"; fi
}
mc_check MISSION_AUTH_ISSUER_URI "$EXPECTED_ISSUER"
mc_check MISSION_AUTH_JWK_SET_URI "$EXPECTED_ISSUER/protocol/openid-connect/certs"

# 6c. the LIVE Keycloak client's redirect/origin/logout sets (kcadm in-pod; --import-realm skips
# existing realms, so the configmap alone can silently diverge from what login actually enforces).
# The permanent master-realm admin is 'abhinav' (the bootstrap 'admin' account is DISABLED; its
# password was re-synced to the KC_BOOTSTRAP_ADMIN_PASSWORD secret on 2026-07-18 — see
# docs/keycloak-prod.md). Override KC_VERIFY_USER to use a dedicated read-only verifier account.
KC_VERIFY_USER="${KC_VERIFY_USER:-abhinav}"
case "$KC_VERIFY_USER" in
  *[!a-zA-Z0-9._-]*|"") echo "FATAL: KC_VERIFY_USER '$KC_VERIFY_USER' outside [a-zA-Z0-9._-] — refusing to interpolate into a remote shell" >&2; exit 2 ;;
esac
run_stdin() {  # like run(), but forwards OUR stdin to the remote command (ssh passes stdin through)
  if [ -n "$PROD_SSH" ]; then $PROD_SSH "$1"; else bash -c "$1"; fi
}
kc_sets_check() {
  command -v python3 >/dev/null || { unavailable "python3 needed to parse the Keycloak client"; return; }
  local pw client
  pw="$(prod_kubectl "get secret oe-keycloak-secrets -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}'" | base64 -d)"
  [ -z "$pw" ] && { unavailable "could not read the Keycloak admin secret"; return; }
  # Password travels via stdin end-to-end — never interpolated into shell text, so any character
  # (apostrophes included) is safe; kcadm receives it through the pod-side shell variable.
  client="$(printf '%s' "$pw" | run_stdin "kubectl $KEXEC_OPTS -n $NS exec -i deploy/oe-keycloak -- sh -c 'IFS= read -r KC_PW; /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user $KC_VERIFY_USER --password \"\$KC_PW\" >/dev/null 2>&1 && /opt/keycloak/bin/kcadm.sh get clients -r optionsedge -q clientId=options-edge-web 2>/dev/null'")"
  [ -z "$client" ] && { unavailable "could not read the live options-edge-web client via kcadm"; return; }
  printf '%s' "$client" | python3 -c "
import json, sys
# EVERYTHING inside the guard: a [null] element, a non-dict client, or non-list fields must all
# surface as a FAIL line (stdout, exit 0 — the caller's detector reads the message, not the status).
try:
    arr = json.load(sys.stdin)
    if not isinstance(arr, list) or len(arr) != 1:
        raise ValueError('expected exactly 1 matching client, got %r' % (len(arr) if isinstance(arr, list) else type(arr).__name__))
    c = arr[0]
    if not isinstance(c, dict):
        raise ValueError('client entry is %s, not an object' % type(c).__name__)
    red = c.get('redirectUris', []); wo = c.get('webOrigins', [])
    attrs = c.get('attributes', {}) or {}
    if not isinstance(red, list) or not isinstance(wo, list) or not isinstance(attrs, dict):
        raise ValueError('client fields have unexpected types')
    pl = [u for u in str(attrs.get('post.logout.redirect.uris', '')).split('##') if u]
    def out(name, vals): print(name + '\t' + '\t'.join(sorted(str(v) for v in vals)))
    out('REDIRECTS', red)
    out('WEBORIGINS', wo)
    out('POSTLOGOUT', pl)
except Exception as e:
    print('  FAIL: live Keycloak client response unusable: %s' % e)
" | while IFS=$'\t' read -r name rest; do
    case "$name" in *FAIL:*) echo "$name" >&2; continue ;; esac
    live_sorted="$(printf '%s' "$rest" | tr '\t' '\n' | sort -u)"
    case "$name" in
      REDIRECTS) expected="$EXPECTED_KC_REDIRECTS" ;;
      WEBORIGINS) expected="$EXPECTED_KC_WEBORIGINS" ;;
      POSTLOGOUT) expected="$EXPECTED_KC_POSTLOGOUT" ;;
    esac
    expected_sorted="$(printf '%s\n' $expected | sort -u)"
    if [ "$live_sorted" = "$expected_sorted" ]; then
      echo "  OK   live Keycloak client $name matches the expected set exactly"
    else
      echo "  FAIL: live Keycloak client $name differs from the expected set" >&2
      diff <(printf '%s\n' "$expected_sorted") <(printf '%s\n' "$live_sorted") | sed 's/^</       missing: /; s/^>/       unexpected: /' | grep -v '^---' | sed 's/^/  /'
    fi
  done
}
# kc_sets_check pipes through a subshell, so $fail cannot mutate inside it; detect on its output.
kc_out="$(kc_sets_check 2>&1)"
[ -n "$kc_out" ] && printf '%s\n' "$kc_out"
printf '%s\n' "$kc_out" | grep -q 'FAIL' && fail=1
# Completeness: all three client sets must have been judged (a crashed/truncated parser producing
# fewer rows must not read as green).
kc_rows="$(printf '%s\n' "$kc_out" | grep -c 'live Keycloak client .* \(matches\|differs\)')"
if ! printf '%s\n' "$kc_out" | grep -q 'FAIL' && [ "$kc_rows" != "3" ]; then
  if printf '%s\n' "$kc_out" | grep -q 'WARN'; then :; else
    bad "Keycloak client verification produced $kc_rows/3 judgements — parser output truncated"
  fi
fi

# 6d. the dark req realm's client must follow the domain too (Phase 3 renames it) ----------------
# Exact per-field sets: the callback URI and the origin are DIFFERENT values and each list must
# match its expected set exactly (extra entries are a failure, substrings prove nothing).
case "$PHASE" in retired) req_host="req.bleadingoptions.com" ;; *) req_host="req.fullfunding.nl" ;; esac
EXPECTED_REQ_REDIRECTS="${EXPECTED_REQ_REDIRECTS:-https://$req_host/oidc-callback}"
EXPECTED_REQ_WEBORIGINS="${EXPECTED_REQ_WEBORIGINS:-https://$req_host}"
req_raw="$(prod_kubectl "exec --pod-running-timeout=$K8S_TIMEOUT deploy/oe-keycloak -- sh -c '/opt/keycloak/bin/kcadm.sh get clients -r req -q clientId=bugzilla-web 2>/dev/null'")"
if [ -z "$req_raw" ]; then
  unavailable "could not read the req realm's bugzilla-web client"
  req_out=""
else
if ! command -v python3 >/dev/null; then
  unavailable "python3 needed to parse the req realm client"
  req_out=""
else
req_out="$(printf '%s' "$req_raw" | python3 -c "
import json, sys
try:
    arr = json.load(sys.stdin)
    if not isinstance(arr, list) or len(arr) != 1:
        raise ValueError('expected exactly 1 bugzilla-web client, got %r' % (len(arr) if isinstance(arr, list) else type(arr).__name__))
    c = arr[0]
    if not isinstance(c, dict) or not isinstance(c.get('redirectUris', []), list) or not isinstance(c.get('webOrigins', []), list):
        raise ValueError('client fields have unexpected types')
    print('REQ_REDIRECTS\t' + '\t'.join(sorted(str(v) for v in c.get('redirectUris', []))))
    print('REQ_WEBORIGINS\t' + '\t'.join(sorted(str(v) for v in c.get('webOrigins', []))))
except Exception as e:
    print('  FAIL: req realm client response unusable: %s' % e)")"
fi
fi
if [ -z "$req_out" ]; then
  :  # unavailable already reported above (or python3 missing — the kc_sets_check gate covers that)
elif printf '%s\n' "$req_out" | grep -q 'FAIL'; then
  printf '%s\n' "$req_out"; fail=1
else
  while IFS=$'\t' read -r name rest; do
    live_sorted="$(printf '%s' "$rest" | tr '\t' '\n' | sort -u)"
    case "$name" in
      REQ_REDIRECTS) expected="$EXPECTED_REQ_REDIRECTS"; label="req redirectUris" ;;
      REQ_WEBORIGINS) expected="$EXPECTED_REQ_WEBORIGINS"; label="req webOrigins" ;;
      *) continue ;;
    esac
    expected_sorted="$(printf '%s\n' $expected | sort -u)"
    if [ "$live_sorted" = "$expected_sorted" ]; then
      note "OK   live $label matches the expected set exactly"
    else
      bad "live $label differs from the expected set"
      diff <(printf '%s\n' "$expected_sorted") <(printf '%s\n' "$live_sorted") | sed 's/^</       missing: /; s/^>/       unexpected: /' | grep -v '^---' | sed 's/^/  /'
    fi
  done <<EOF_REQ
$req_out
EOF_REQ
  req_rows="$(printf '%s\n' "$req_out" | grep -cE '^(REQ_REDIRECTS|REQ_WEBORIGINS)\b')"
  [ "$req_rows" = "2" ] || bad "req client verification produced $req_rows/2 records — parser output truncated"
fi

# 6e. the repo realm IMPORT FILE must carry the same sets (parity is a gate, not a hope) ---------
REALM_CM="${REALM_CM:-$(cd "$(dirname "$0")/../.." && pwd)/k8s/keycloak/keycloak-realm-configmap.yaml}"
cm_out="$(python3 - "$REALM_CM" <<'PYCM'
import sys
try:
    import yaml, json
    docs = list(yaml.safe_load_all(open(sys.argv[1])))
    cm = next(d for d in docs if d and d.get('kind') == 'ConfigMap')
    realm = json.loads(cm['data']['optionsedge-realm.json'])
    req = json.loads(cm['data']['req-realm.json'])
    webs = [c for c in realm['clients'] if isinstance(c, dict) and c.get('clientId') == 'options-edge-web']
    if len(webs) != 1:
        raise ValueError('expected exactly 1 options-edge-web client in the import file, got %d' % len(webs))
    web = webs[0]
    wattrs = web.get('attributes', {})
    if (not isinstance(web.get('redirectUris', []), list) or not isinstance(web.get('webOrigins', []), list)
            or not isinstance(wattrs, dict) or not isinstance(wattrs.get('post.logout.redirect.uris', ''), str)):
        raise ValueError('options-edge-web fields have unexpected types')
    bz_all = [c for c in req['clients'] if isinstance(c, dict) and c.get('clientId') == 'bugzilla-web']
    if len(bz_all) != 1:
        raise ValueError('expected exactly 1 bugzilla-web client in the import file, got %d' % len(bz_all))
    bz = bz_all[0]
    if not isinstance(bz.get('redirectUris', []), list) or not isinstance(bz.get('webOrigins', []), list):
        raise ValueError('bugzilla-web fields have unexpected types')
    pl = [u for u in web.get('attributes', {}).get('post.logout.redirect.uris', '').split('##') if u]
    print('CM_REDIRECTS	' + '	'.join(sorted(web.get('redirectUris', []))))
    print('CM_WEBORIGINS	' + '	'.join(sorted(web.get('webOrigins', []))))
    print('CM_POSTLOGOUT	' + '	'.join(sorted(pl)))
    print('CM_REQ_REDIRECTS	' + '	'.join(sorted(str(v) for v in bz.get('redirectUris', []))))
    print('CM_REQ_WEBORIGINS	' + '	'.join(sorted(str(v) for v in bz.get('webOrigins', []))))
except Exception as e:
    print('  FAIL: realm configmap unusable: %s' % e)
PYCM
)"
if [ -z "$cm_out" ]; then
  bad "realm configmap parser produced no output — cannot prove import-file parity"
elif printf '%s\n' "$cm_out" | grep -q 'FAIL'; then
  printf '%s\n' "$cm_out"; fail=1
else
  while IFS=$'\t' read -r name rest; do
    cm_sorted="$(printf '%s' "$rest" | tr '\t' '\n' | sort -u)"
    case "$name" in
      CM_REDIRECTS) expected="$EXPECTED_KC_REDIRECTS"; label="redirectUris" ;;
      CM_WEBORIGINS) expected="$EXPECTED_KC_WEBORIGINS"; label="webOrigins" ;;
      CM_POSTLOGOUT) expected="$EXPECTED_KC_POSTLOGOUT"; label="post-logout" ;;
      CM_REQ_REDIRECTS) expected="$EXPECTED_REQ_REDIRECTS"; label="req redirectUris" ;;
      CM_REQ_WEBORIGINS) expected="$EXPECTED_REQ_WEBORIGINS"; label="req webOrigins" ;;
      *) continue ;;
    esac
    expected_sorted="$(printf '%s\n' $expected | sort -u)"
    if [ "$cm_sorted" = "$expected_sorted" ]; then
      note "OK   realm configmap $label matches the expected set exactly"
    else
      echo "  FAIL: realm configmap $label differs from the expected set" >&2; fail=1
      diff <(printf '%s\n' "$expected_sorted") <(printf '%s\n' "$cm_sorted") | sed 's/^</       missing: /; s/^>/       unexpected: /' | grep -v '^---' | sed 's/^/  /'
    fi
  done <<EOF_CM
$cm_out
EOF_CM
  cm_rows="$(printf '%s\n' "$cm_out" | grep -cE '^CM_(REDIRECTS|WEBORIGINS|POSTLOGOUT|REQ_REDIRECTS|REQ_WEBORIGINS)\b')"
  [ "$cm_rows" = "5" ] || bad "realm configmap verification produced $cm_rows/5 records — parser output truncated"
fi

# 7. the web surface itself: page serves, and the REST API refuses anonymous callers ------------
for h in $SERVE_APEX $SERVE_ES; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h/" 2>/dev/null)"
  [ "$code" = "200" ] && note "OK   $h/ -> 200" || bad "$h/ -> $code (expected 200)"
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$h/api/config" 2>/dev/null)"
  [ "$code" = "401" ] && note "OK   $h/api/config unauthenticated -> 401" || bad "$h/api/config unauthenticated -> $code (expected 401)"
done

# 8. redirect phases: every OLD hostname, BOTH schemes, exact lifecycle status ------------------
# redirect soak expects 307 (recallable); retired expects the promoted 308. Accepting either would
# let a premature permanent redirect — or a forgotten promotion — pass silently.
for h in $REDIR_HOSTS; do
  target_host="$(redirect_target_for "$h")"
  if [ -z "$target_host" ]; then bad "no redirect mapping defined for $h"; continue; fi
  # FULL matrix: both schemes x both methods. Only POST proves the method-preservation the
  # 307/308 policy exists for — a method- or scheme-scoped rule could pass GET https while
  # mangling POST http (Keycloak's token endpoint is the load-bearing POST, so the auth host is
  # additionally probed on its real token path). STRICT everywhere: the http*:// Single Redirect
  # matches plain HTTP at the edge BEFORE any https upgrade, so a same-host 301/302 on the http
  # leg FAILS — it would rewrite POST to GET before the cross-domain hop.
  probe_paths="/board?x=1"
  [ "$h" = "auth.fullfunding.nl" ] && probe_paths="$probe_paths /realms/optionsedge/protocol/openid-connect/token?x=1"
  for ppath in $probe_paths; do
    want="https://$target_host$ppath"
    for scheme in https http; do
      for method in GET POST; do
        hdrs="$(curl -s -o /dev/null -D - -m 20 -X "$method" "$scheme://$h$ppath" 2>/dev/null)"
        code="$(printf '%s' "$hdrs" | head -1 | awk '{print $2}')"
        loc="$(printf '%s' "$hdrs" | grep -i '^location:' | head -1 | sed -E 's/^[Ll]ocation:[[:space:]]*//; s/\r$//')"
        if [ "$code" = "$EXPECTED_REDIRECT_STATUS" ]; then note "OK   $method $scheme://$h$ppath -> $code"
        else bad "$method $scheme://$h$ppath -> ${code:-<none>} (expected exactly $EXPECTED_REDIRECT_STATUS in phase $PHASE; a method/scheme-scoped rule may be interfering)"; fi
        if [ "$loc" = "$want" ]; then note "OK   $method $scheme://$h$ppath Location preserves host-map + path + query"
        else bad "$method $scheme://$h$ppath Location = '${loc:-<none>}' (expected $want)"; fi
      done
    done
  done
done

stamp="VERIFIED"
[ "${ALLOW_SET_OVERRIDES:-0}" = "1" ] && stamp="REDUCED-MATRIX DIAGNOSTIC PASSED (host sets overridden — NOT acceptance)"
[ "$PRECHECK" = "1" ] && stamp="PRECHECK PASSED (redirect section deliberately skipped — NOT the full gate)"
[ "$NETWORK_ONLY" = "1" ] && stamp="NETWORK-ONLY DIAGNOSTIC PASSED (kube/runtime contracts skipped — NOT acceptance)"
[ "$fail" -eq 0 ] && echo "  prod tunnel $stamp (phase=$PHASE)" || echo "  prod tunnel has PROBLEMS (see above)" >&2
exit "$fail"
