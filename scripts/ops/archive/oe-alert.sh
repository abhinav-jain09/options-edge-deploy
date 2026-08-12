# oe-alert.sh — the ONE implementation of alert delivery. Sourced, never executed.
#
# WHY THIS FILE EXISTS
# The delivery contract below was not obvious and was got wrong twice; it is asserted by
# test-alert-delivery.sh against a controlled local endpoint. A second copy-paste of it into a new
# script would be correct on the day it was pasted and free to rot afterwards — which is the same
# class of defect as two environments archiving different topic sets (see oe-topics.env). One
# definition, every caller. 2026-08-12.
#
# THE CONTRACT (each clause is a bug that actually happened)
#   -f          curl must FAIL on non-2xx. Without it a rejected payload (HTTP 400) exits 0 and a
#               lost alert is logged as "delivered".
#   explicit 2xx  -f does NOT fail on 3xx. Without this check a redirect is logged as "delivered"
#               and the alert is lost in silence.
#   no -L       following redirects is what made rc=52/rc=7 masquerade as unrelated failures and
#               let the assertion go green while guarding nothing.
#   never fatal alert() returns 0 even when delivery fails. The CONDITION that triggered the alert
#               is the caller's to act on; a broken webhook must not also suppress the caller's own
#               non-zero exit.
#
# The log lines are part of the contract too — test-alert-delivery.sh matches them with grep -F.
# Do not reword "(alert delivered to Discord, HTTP N)" or "ALERT DELIVERY FAILED — curl rc=N http=N;".

# Callers define their own log() that tees to their own logfile. Use it when present so alert
# output lands in the same file as the condition it describes; fall back to stdout when not.
_oe_alert_log() {
  if declare -F log >/dev/null 2>&1; then log "$@"
  else echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"
  fi
}

# ⚠️ Write oe-ops.env with a plain, NON-TTY transfer. `ssh -tt` allocates a pseudo-terminal and the
# TTY line discipline rewrites \n as \r\n — which silently injected a CR into the webhook URL and
# produced a permanent HTTP 401 that the old `curl -s` reported as "delivered". Verify by md5, not
# by eye. 2026-08-08.
#
# An explicitly-passed DISCORD_WEBHOOK_URL wins over the file: sourcing unconditionally would
# overwrite it, which both blocks an operator override and makes the delivery-failure paths
# untestable (every test would silently hit the real webhook and pass).
oe_load_webhook() {
  [ -n "${DISCORD_WEBHOOK_URL:-}" ] && return 0
  local _d _envf
  _d="${OE_OPS_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
  for _envf in "$_d/oe-ops.env" /etc/oe-ops.env; do
    # shellcheck source=/dev/null
    [ -r "$_envf" ] && { . "$_envf"; break; }
  done
  return 0
}

alert() {
  oe_load_webhook
  if [ -z "${DISCORD_WEBHOOK_URL:-}" ]; then
    _oe_alert_log "  (no DISCORD_WEBHOOK_URL in /etc/oe-ops.env — alert NOT delivered, log only)"
    return 0
  fi
  local payload http crc ok
  payload=$(printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps({"content": sys.stdin.read()}))' 2>/dev/null)
  if [ -z "$payload" ]; then
    _oe_alert_log "  (ALERT DELIVERY FAILED — could not encode the payload; the condition below still stands)"
    return 0
  fi
  http=$(curl -fsS -m 10 -o /dev/null -w '%{http_code}' \
           -H 'Content-Type: application/json' -d "$payload" "$DISCORD_WEBHOOK_URL" 2>/dev/null)
  crc=$?
  ok=false
  if [ "$crc" -eq 0 ]; then
    case "$http" in 2??) ok=true ;; esac
  fi
  if [ "$ok" = true ]; then
    _oe_alert_log "  (alert delivered to Discord, HTTP $http)"
  else
    _oe_alert_log "  (ALERT DELIVERY FAILED — curl rc=$crc http=${http:-none}; the condition below still stands)"
  fi
  return 0
}
