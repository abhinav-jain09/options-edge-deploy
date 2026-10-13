#!/usr/bin/env bash
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WATCHDOG="$HERE/oe-tunnel-watchdog.sh"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "$TEST_DIR"' EXIT

# Stub external effects while running the actual watchdog in a child bash process.
curl() {
  local arg url="" follow=no
  for arg in "$@"; do
    [ "$arg" = -L ] && follow=yes
    case "$arg" in https://*|http://*) url="$arg" ;; esac
  done
  case "$url:$TEST_CASE" in
    "$PUBLIC_URL/ws/events:ws_down") [ -e "$TEST_DIR/restarted" ] && printf 401 || printf 502 ;;
    "$PUBLIC_URL/ws/events:"*) printf 401 ;;
    "$WS_ORIGIN/ws/events:ws_origin_down") printf 503 ;;
    "$WS_ORIGIN/ws/events:"*) printf 401 ;;
    "$PUBLIC_URL:redirect") [ "$follow" = yes ] && printf 200 || printf 302 ;;
    "$PUBLIC_URL:tunnel_down") [ -e "$TEST_DIR/restarted" ] && printf 200 || printf 502 ;;
    "$PUBLIC_URL:origin_down") printf 502 ;;
    "$PUBLIC_URL:ws_down"|"$PUBLIC_URL:ws_origin_down") printf 200 ;;
    "$ORIGIN_URL:origin_down") printf 503 ;;
    "$ORIGIN_URL:"*) printf 200 ;;
    *) printf 000 ;;
  esac
}
systemctl() {
  printf '%s\n' "$*" >> "$TEST_DIR/restarts"
  : > "$TEST_DIR/restarted"
}
logger() { :; }
sleep() { :; }
export -f curl systemctl logger sleep

PUBLIC_URL=https://example.test
ORIGIN_URL=http://origin.test
WS_ORIGIN=http://ws-origin.test
export PUBLIC_URL ORIGIN_URL WS_ORIGIN TEST_DIR

run_case() {
  TEST_CASE="$1"
  export TEST_CASE
  : > "$TEST_DIR/restarts"
  rm -f "$TEST_DIR/restarted"
  bash "$WATCHDOG" > "$TEST_DIR/output" 2>&1 || {
    printf 'FAIL %s: watchdog exited nonzero\n' "$TEST_CASE" >&2
    sed -n '1,8p' "$TEST_DIR/output" >&2
    exit 1
  }
  local actual
  actual="$(wc -l < "$TEST_DIR/restarts" | tr -d ' ')"
  [ "$actual" = "$2" ] || {
    printf 'FAIL %s: %s restarts, expected %s\n' "$TEST_CASE" "$actual" "$2" >&2
    exit 1
  }
}

run_case redirect 0
run_case tunnel_down 1
run_case origin_down 0
run_case ws_down 1
run_case ws_origin_down 0
echo 'oe-tunnel-watchdog-test: PASS'
