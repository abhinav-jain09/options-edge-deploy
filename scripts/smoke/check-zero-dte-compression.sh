#!/usr/bin/env bash
# Post-rollout smoke for the hash-pinned ZDCE shadow runtime. The deployed manifest is the
# authority for enabled/disabled state: disabled must fail closed; enabled must reach BACKFILL
# (after topic/schema/transaction initialization) or LIVE and satisfy the frozen contract.
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
EXPECTED_SHA256="${EXPECTED_SHA256:-d774ed59ca3148e6f9b36accfc0a4744d43fc1f6bf36cdaa30d4ae899f419d82}"
KUBECTL=(kubectl -n "$NAMESPACE")

if [[ -n "${KUBECONFIG:-}" ]]; then
  KUBECTL=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE")
fi

deployed_enabled="$("${KUBECTL[@]}" get deployment context-tape-service \
  -o jsonpath='{range .spec.template.spec.containers[?(@.name=="context-tape")].env[*]}{.name}={.value}{"\n"}{end}' \
  | awk -F= '$1 == "ZERO_DTE_COMPRESSION_ENABLED" { print $2 }')"
EXPECTED_ENABLED="${EXPECTED_ENABLED:-$deployed_enabled}"
case "$EXPECTED_ENABLED" in
  true|1) EXPECTED_ENABLED=1 ;;
  false|0) EXPECTED_ENABLED=0 ;;
  *) echo "FATAL: cannot resolve deployed ZERO_DTE_COMPRESSION_ENABLED" >&2; exit 1 ;;
esac

choose_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

api_port="$(choose_port)"
health_port="$(choose_port)"
while [[ "$health_port" == "$api_port" ]]; do health_port="$(choose_port)"; done
log_file="${WORK_DIR:-/tmp}/zero-dte-compression-port-forward.log"
"${KUBECTL[@]}" port-forward service/context-tape-service \
  "$api_port:8134" "$health_port:8135" >"$log_file" 2>&1 &
forward_pid=$!
health_body=""
api_body=""
cleanup() {
  kill "$forward_pid" >/dev/null 2>&1 || true
  wait "$forward_pid" >/dev/null 2>&1 || true
  [[ -z "$health_body" ]] || rm -f "$health_body"
  [[ -z "$api_body" ]] || rm -f "$api_body"
}
trap cleanup EXIT

metrics=""
for attempt in $(seq 1 30); do
  if metrics="$(curl -fsS --max-time 5 "http://127.0.0.1:$health_port/metrics" 2>/dev/null)"; then
    break
  fi
  if ! kill -0 "$forward_pid" >/dev/null 2>&1; then
    echo "FATAL: context-tape port-forward exited before the compression smoke" >&2
    cat "$log_file" >&2 || true
    exit 1
  fi
  sleep 1
done

grep -qx "zero_dte_compression_enabled $EXPECTED_ENABLED" <<<"$metrics" || {
  echo "FATAL: runtime enabled metric disagrees with the deployed manifest" >&2
  exit 1
}
if [[ "$EXPECTED_ENABLED" -eq 0 ]]; then
  health_body="$(mktemp)"
  health_code="$(curl -sS --max-time 10 -o "$health_body" -w '%{http_code}' \
    "http://127.0.0.1:$health_port/health/compression")"
  [[ "$health_code" == "404" ]] && grep -qx 'DISABLED' "$health_body" || {
    echo "FATAL: disabled compression runtime did not fail closed: HTTP $health_code $(cat "$health_body")" >&2
    exit 1
  }
  echo "  compression: disabled by reviewed deployment gate ✓"
  exit 0
fi
failures="$(awk '$1 == "zero_dte_compression_loop_failures_total" { print $2 }' <<<"$metrics")"
[[ "${failures:-}" =~ ^[0-9]+$ ]] && [[ "$failures" -eq 0 ]] || {
  echo "FATAL: ZDCE runtime reports loop failures: ${failures:-missing}" >&2
  exit 1
}

health_body="$(mktemp)"
health_code=""
for attempt in $(seq 1 30); do
  health_code="$(curl -sS --max-time 10 -o "$health_body" -w '%{http_code}' \
    "http://127.0.0.1:$health_port/health/compression")"
  if [[ "$health_code" == "200" ]] || grep -qx 'NOT_READY:BACKFILL' "$health_body"; then
    break
  fi
  if grep -Eq '^NOT_READY:(RETRYING|FAILED|STOPPED)$' "$health_body"; then
    break
  fi
  sleep 1
done
case "$health_code" in
  200)
    grep -qx 'READY:LIVE' "$health_body" || {
      echo "FATAL: compression health returned 200 with an invalid body: $(cat "$health_body")" >&2
      exit 1
    }
    api_body="$(mktemp)"
    api_code="$(curl -sS --max-time 15 -o "$api_body" -w '%{http_code}' \
      "http://127.0.0.1:$api_port/api/context-tape/compression")"
    [[ "$api_code" == "200" ]] || {
      echo "FATAL: LIVE compression runtime returned API status $api_code" >&2
      cat "$api_body" >&2 || true
      exit 1
    }
    python3 - "$api_body" "$EXPECTED_SHA256" <<'PY'
import json
import sys

body = json.load(open(sys.argv[1], encoding="utf-8"))
expected = sys.argv[2]
assert body.get("schemaVersion") == "zdce.context-tape-view.1", body.get("schemaVersion")
assert body.get("modelVersion") == "zdce113-candidate-v1", body.get("modelVersion")
assert body.get("artifactSha256") == expected, body.get("artifactSha256")
assert body.get("authority") == "SHADOW_NOT_FOR_TRADING", body.get("authority")
assert body.get("state") == "LIVE" and body.get("ready") is True, (body.get("state"), body.get("ready"))
assert body.get("supportedHorizonsMinutes") == [1, 3, 5], body.get("supportedHorizonsMinutes")
assert body.get("unsupportedHorizonsMinutes") == [10, 30], body.get("unsupportedHorizonsMinutes")
assert isinstance(body.get("points"), list) and body["points"], "LIVE response has no points"
PY
    echo "  compression: LIVE contract + artifact digest verified ✓"
    ;;
  503)
    if ! grep -qx 'NOT_READY:BACKFILL' "$health_body"; then
      echo "FATAL: compression endpoint is unavailable in an unexpected state: $(cat "$health_body")" >&2
      exit 1
    fi
    echo "  compression: enabled and operational; awaiting current-session data ($(cat "$health_body")) ✓"
    ;;
  *)
    echo "FATAL: compression health returned HTTP $health_code: $(cat "$health_body")" >&2
    exit 1
    ;;
esac
