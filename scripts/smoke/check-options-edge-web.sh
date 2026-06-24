#!/usr/bin/env bash
set -euo pipefail

# Liveness + auth-posture smoke for the OptionsEdge web app (k8s Deployment; dev :8090, prod :8094).
#
# The option-chain UI is gated behind Keycloak login (SecurityConfig): the SPA shell + assets are PUBLIC,
# but every /api/** route requires a valid bearer JWT (401 without one). An unauthenticated smoke therefore
# must NOT expect /api/config to return 200 — it correctly returns 401 once login is enabled. This check is
# auth-on/off portable and still asserts a real posture:
#   * public routes "/" and "/option-chain" MUST be 200 (a blanket-401 / ingress misconfig fails here), and
#   * "/api/config" MUST be either 200 with a "provider" body (auth disabled) OR 401 (auth enabled);
#     any other status or a connection failure fails.
# It deliberately does NOT obtain a token: that needs a dedicated CI identity and belongs to a separate
# authenticated integration smoke, not this deploy liveness/posture check.

WEB_PUBLIC_URL="${WEB_PUBLIC_URL:-http://localhost:8090}"
WEB_PUBLIC_URL="${WEB_PUBLIC_URL%/}"
TIMEOUT_SECONDS="${OPTIONS_EDGE_WEB_TIMEOUT_SECONDS:-120}"

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

deadline=$((SECONDS + TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if posture_ok; then
    echo "OptionsEdge web app healthy at $WEB_PUBLIC_URL/ (public shell 200; /api/config posture OK)"
    exit 0
  fi
  sleep 2
done

echo "OptionsEdge web app did not reach a healthy posture at $WEB_PUBLIC_URL before timeout." >&2
echo "Endpoint statuses:" >&2
for path in / /option-chain /api/config; do
  echo "  ${path} -> $(http_code "$WEB_PUBLIC_URL$path")" >&2
done
exit 1
