#!/usr/bin/env bash
# Post-rollout gate for broker-execution-service: the stored execution destination must be selectable by the running
# configuration. The destination record carries a fingerprint of the broker configuration (host, port, client id,
# limits); a deploy that changes it used to leave every order request failing SELECTED_DESTINATION_UNAVAILABLE with the
# pod looking healthy. /health reports {"destination":"OK"|<reason code>,"destinationMode":"PAPER"|"LIVE"|...}.
# A Paper destination that is not OK fails the deploy; a Live destination that is not OK only warns (a Live gateway that is
# down is an operator matter and must never block shipping a fix).
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
RETRIES="${DESTINATION_RETRIES:-36}"
DELAY="${DESTINATION_RETRY_DELAY:-5}"
KUBECTL=(kubectl -n "$NAMESPACE")
if [[ -n "${KUBECONFIG:-}" ]]; then KUBECTL=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE"); fi

fetch_health() {
  "${KUBECTL[@]}" exec deploy/broker-execution-service -- bash -c \
    'exec 3<>/dev/tcp/127.0.0.1/${SERVER_PORT:-8131}; printf "GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3; cat <&3' 2>/dev/null | tr -d '\r' | tail -n 1
}

last=""
for attempt in $(seq 1 "$RETRIES"); do
  last="$(fetch_health || true)"
  destination="$(printf '%s' "$last" | sed -n 's/.*"destination":"\([^"]*\)".*/\1/p')"
  mode="$(printf '%s' "$last" | sed -n 's/.*"destinationMode":"\([^"]*\)".*/\1/p')"
  if [[ "$destination" == "OK" ]]; then
    echo "  destination: OK ($mode) ✓"
    exit 0
  fi
  echo "  destination: ${destination:-no answer} ($mode) attempt $attempt/$RETRIES, retrying …"
  sleep "$DELAY"
done

if [[ "$mode" == "LIVE" ]]; then
  echo "  destination: WARNING — Live destination is ${destination:-unreachable}; not failing the deploy (operator matter)." >&2
  exit 0
fi
echo "  destination: FAIL — the stored destination cannot be selected by the running configuration (${destination:-no answer}); last health: $last" >&2
exit 1
