#!/usr/bin/env bash
set -euo pipefail

# Liveness + auth-posture smoke for the OptionsEdge web app (k8s Deployment, served on :8094).
#
# The option-chain UI is gated behind Keycloak login (SecurityConfig): the SPA shell + assets are PUBLIC,
# but every /api/** route requires a valid bearer JWT (401 without one). So this check is auth-on/off
# portable and asserts a real posture:
#   * public routes "/" and "/option-chain" MUST be 200 (a blanket-401 / ingress misconfig fails here), and
#   * "/api/config" MUST be either 200 with a "provider" body (auth disabled) OR 401 (auth enabled);
#     any other status or a connection failure fails.
#
# DEEP (authenticated) verification — opt-in: when OPTIONS_EDGE_SMOKE_CLIENT_SECRET is set, the check also
# obtains a Keycloak token via the client_credentials grant (client OPTIONS_EDGE_SMOKE_CLIENT_ID against
# OPTIONS_EDGE_SMOKE_TOKEN_URL, or {OPTIONS_EDGE_SMOKE_ISSUER}/protocol/openid-connect/token) and asserts
# /api/config returns 200 + "provider" WITH the bearer. This proves the protected API actually serves valid
# config behind auth — not just that it 401s. With no secret configured it degrades to the posture check, so
# it is safe to ship before the CI identity exists. The secret is read only from the environment (a Jenkins
# credential) and is never logged.

WEB_PUBLIC_URL="${WEB_PUBLIC_URL:-http://localhost:8094}"
WEB_PUBLIC_URL="${WEB_PUBLIC_URL%/}"
TIMEOUT_SECONDS="${OPTIONS_EDGE_WEB_TIMEOUT_SECONDS:-120}"

SMOKE_CLIENT_ID="${OPTIONS_EDGE_SMOKE_CLIENT_ID:-options-edge-smoke}"
SMOKE_TOKEN_URL="${OPTIONS_EDGE_SMOKE_TOKEN_URL:-}"
if [ -z "$SMOKE_TOKEN_URL" ] && [ -n "${OPTIONS_EDGE_SMOKE_ISSUER:-}" ]; then
  SMOKE_TOKEN_URL="${OPTIONS_EDGE_SMOKE_ISSUER%/}/protocol/openid-connect/token"
fi

# Read the client secret and decide deep mode with xtrace OFF so `bash -x` can never echo the secret value
# (the only operations that touch the raw secret; curl gets it via a 0600 --config file, never argv). The
# length test uses ${#var} so even the trace shows a count, not the value. Restore the caller's xtrace after.
__xtrace_was_on=0; case $- in *x*) __xtrace_was_on=1 ;; esac
set +x
SMOKE_CLIENT_SECRET="${OPTIONS_EDGE_SMOKE_CLIENT_SECRET:-}"
DEEP=0
if [ "${#SMOKE_CLIENT_SECRET}" -ne 0 ]; then
  if [ -z "$SMOKE_TOKEN_URL" ]; then
    echo "OPTIONS_EDGE_SMOKE_CLIENT_SECRET is set but neither OPTIONS_EDGE_SMOKE_TOKEN_URL nor OPTIONS_EDGE_SMOKE_ISSUER is — cannot obtain a token." >&2
    exit 2
  fi
  DEEP=1
fi
[ "$__xtrace_was_on" = 1 ] && set -x

# Sensitive material (client secret, bearer token) is passed to curl via 0600 --config files in this dir,
# NEVER on the command line (argv is world-readable via `ps`). Removed on any exit.
SECURE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/oe-smoke.XXXXXX")"
chmod 700 "$SECURE_DIR"
trap 'rm -rf "$SECURE_DIR"' EXIT

http_code() {
  # http_code <url> [body_out_file] -> prints the HTTP status (000 on connection failure)
  local code
  code="$(curl -s -o "${2:-/dev/null}" -w '%{http_code}' --connect-timeout 5 --max-time 15 "$1" 2>/dev/null)" || code="000"
  printf '%s' "${code:-000}"
}

posture_ok() {
  # Public SPA shell + a public route must serve (guards against a blanket-401 / misrouted deployment).
  [ "$(http_code "$WEB_PUBLIC_URL/")" = "200" ] || return 1
  [ "$(http_code "$WEB_PUBLIC_URL/option-chain")" = "200" ] || return 1
  # API posture: 200 + "provider" (auth disabled) OR 401 (auth enabled). Anything else is unhealthy.
  local body code
  body="$(mktemp)"
  code="$(http_code "$WEB_PUBLIC_URL/api/config" "$body")"
  case "$code" in
    200) grep -q '"provider"' "$body" || { rm -f "$body"; return 1; } ;;
    401) ;; # auth enabled — /api/config correctly demands a bearer JWT
    *) rm -f "$body"; return 1 ;;
  esac
  rm -f "$body"
  return 0
}

# Fetch a client_credentials access token. The secret goes into a 0600 curl --config file (never argv),
# and the value-bearing lines are written with xtrace OFF in a subshell so `bash -x` can't echo them.
# Prints the access token (empty on any failure); never echoes the secret or token.
smoke_token() {
  local cfg resp
  cfg="$SECURE_DIR/token.curl"
  ( set +x
    umask 077
    {
      printf 'url = "%s"\n' "$SMOKE_TOKEN_URL"
      printf 'data = "grant_type=client_credentials"\n'
      printf 'data = "client_id=%s"\n' "$SMOKE_CLIENT_ID"
      printf 'data-urlencode = "client_secret=%s"\n' "$SMOKE_CLIENT_SECRET"
    } > "$cfg"
  )
  resp="$(curl -s --connect-timeout 5 --max-time 15 --config "$cfg" 2>/dev/null)" || resp=""
  rm -f "$cfg"
  printf '%s' "$resp" | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("access_token",""))
except Exception: print("")' 2>/dev/null || true
}

authenticated_config_ok() {
  local token cfg body code
  token="$(smoke_token)"
  [ -n "$token" ] || return 1
  cfg="$SECURE_DIR/api.curl"
  ( set +x
    umask 077
    printf 'header = "Authorization: Bearer %s"\n' "$token" > "$cfg"
  )
  body="$(mktemp "$SECURE_DIR/body.XXXXXX")"
  code="$(curl -s -o "$body" -w '%{http_code}' --connect-timeout 5 --max-time 15 \
    --config "$cfg" "$WEB_PUBLIC_URL/api/config")" || code="000"
  rm -f "$cfg"
  if [ "$code" = "200" ] && grep -q '"provider"' "$body"; then rm -f "$body"; return 0; fi
  rm -f "$body"
  return 1
}

deadline=$((SECONDS + TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if posture_ok && { [ "$DEEP" = "0" ] || authenticated_config_ok; }; then
    if [ "$DEEP" = "1" ]; then
      echo "OptionsEdge web app healthy at $WEB_PUBLIC_URL/ (public shell 200; authenticated /api/config returned valid config)"
    else
      echo "OptionsEdge web app healthy at $WEB_PUBLIC_URL/ (public shell 200; /api/config posture OK)"
    fi
    exit 0
  fi
  sleep 2
done

echo "OptionsEdge web app did not reach a healthy posture at $WEB_PUBLIC_URL before timeout." >&2
[ "$DEEP" = "1" ] && echo "(deep auth verification was enabled via OPTIONS_EDGE_SMOKE_CLIENT_SECRET; token client=$SMOKE_CLIENT_ID url=$SMOKE_TOKEN_URL)" >&2
echo "Endpoint statuses:" >&2
for path in / /option-chain /api/config; do
  echo "  ${path} -> $(http_code "$WEB_PUBLIC_URL$path")" >&2
done
exit 1
