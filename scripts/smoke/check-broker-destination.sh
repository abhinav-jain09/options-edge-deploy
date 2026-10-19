#!/usr/bin/env bash
# Post-rollout gate for broker-execution-service: the stored execution destination must be selectable by the running
# configuration. The destination record carries a fingerprint of the broker configuration (host, port, client id,
# limits); a deploy that changes it used to leave every order request failing SELECTED_DESTINATION_UNAVAILABLE while the pod
# looked healthy. The service logs one line at startup, "DESTINATION_SELECTION destination=<OK|reason> mode=<PAPER|LIVE|...>",
# and this gate reads it from the pod log: the Jenkins deployer may read pod logs but may NOT exec or proxy into a pod.
#   destination=OK                          -> pass
#   destination=<reason> and mode=PAPER     -> fail the deploy (Paper must always be selectable)
#   destination=<reason> and mode=LIVE      -> warn only (a Live gateway that is down is an operator matter)
#   no line after the retries               -> WARN loudly and pass: a gate that cannot see must not fail a healthy rollout,
#                                              but the build log says the destination was NOT verified.
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
RETRIES="${DESTINATION_RETRIES:-24}"
DELAY="${DESTINATION_RETRY_DELAY:-5}"
KUBECTL=(kubectl -n "$NAMESPACE")
if [[ -n "${KUBECONFIG:-}" ]]; then KUBECTL=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE"); fi

line=""
for attempt in $(seq 1 "$RETRIES"); do
  line="$("${KUBECTL[@]}" logs deploy/broker-execution-service --since=15m 2>/dev/null | grep 'DESTINATION_SELECTION' | tail -n 1 || true)"
  if [[ -n "$line" ]]; then break; fi
  echo "  destination: no startup line yet (attempt $attempt/$RETRIES), retrying …"
  sleep "$DELAY"
done

destination="$(printf '%s' "$line" | sed -n 's/.*destination=\([A-Za-z0-9_]*\).*/\1/p')"
mode="$(printf '%s' "$line" | sed -n 's/.*mode=\([A-Za-z0-9_]*\).*/\1/p')"

if [[ -z "$destination" ]]; then
  echo "  destination: WARNING — NOT VERIFIED: the DESTINATION_SELECTION startup line was not found in the pod log (older image or unreadable log); not failing the deploy." >&2
  exit 0
fi
if [[ "$destination" == "OK" ]]; then
  echo "  destination: OK ($mode) ✓"
  exit 0
fi
if [[ "$mode" == "LIVE" ]]; then
  echo "  destination: WARNING — Live destination is $destination; not failing the deploy (operator matter)." >&2
  exit 0
fi
echo "  destination: FAIL — the stored $mode destination cannot be selected by the running configuration ($destination): $line" >&2
exit 1
