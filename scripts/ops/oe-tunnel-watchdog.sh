#!/usr/bin/env bash
# oe-tunnel-watchdog.sh — restart the Cloudflare tunnel when it is running but no longer serving.
#
# THE FAILURE THIS EXISTS FOR (prod, 2026-08-04)
# ----------------------------------------------
# Services were scaled down off-hours, so the web origin 192.168.100.252:8094 disappeared.
# cloudflared logged 9,688 "connection refused" against it, then:
#     16:25  WRN Serve tunnel error error="control stream encountered a failure"
#     16:30  INF Unregistered tunnel connection connIndex=0 / connIndex=3
# and it never came back on its own. systemd's Restart=always did NOT help, because a broken
# control stream does not kill the process — cloudflared stays "active" while serving nothing.
# The tunnel only recovered when someone restarted it by hand at 16:31.
#
# So "is the service active?" is the wrong question. This asks the only one that matters:
# does the public URL actually answer? If it does not, but the ORIGIN is healthy, the tunnel is
# the broken part and restarting it is the fix.
#
# The origin check is what keeps this safe: during a deliberate shutdown the origin is down too,
# so the tunnel is left alone and the log says why. It only ever acts on tunnel-shaped failures.
#
# Runs as root from a systemd timer, so it needs no polkit grant and covers EVERY path that can
# leave the tunnel stranded — off-hours scale-down, a manual bring-down, or a reboot — not just
# the autostart path.
#
# ⚠️ PUBLIC_URL MUST TRACK THE LIVE PUBLIC DOMAIN. 2026-08-08: fullfunding.nl was retired
# (migration Phase 3) but this file still probed it. The retired host answers 404 while the origin
# answers 200, which is exactly this script's "tunnel is broken" signature — so it restarted
# cloudflared every 2 minutes, and each restart drains for ~30s, i.e. a rolling outage of the NEW
# domain caused by the watchdog itself. Whenever the public hostname changes, change it here in
# the same window. This file lives on the host at /usr/local/sbin/oe-tunnel-watchdog.sh; keep the
# two in step (docs/domain-migration-bleadingoptions.md).
set -uo pipefail
PUBLIC_URL="${PUBLIC_URL:-https://bleadingoptions.com}"
ORIGIN_URL="${ORIGIN_URL:-http://192.168.100.252:8094}"
UNIT="${UNIT:-options-edge-cloudflared-stable}"
TAG="oe-tunnel-watchdog"
log() { logger -t "$TAG" -- "$*"; echo "[$(date -Is)] $*"; }

code() { curl -s -o /dev/null -w '%{http_code}' -m 15 "$1" 2>/dev/null; }

pub="$(code "$PUBLIC_URL")"
[ "$pub" = "200" ] && exit 0          # healthy: the overwhelmingly common case, stay silent

org="$(code "$ORIGIN_URL")"
if [ "$org" != "200" ]; then
  log "public=$pub origin=$org — origin is DOWN too, so this is a deliberate shutdown, not a tunnel fault. Leaving $UNIT alone."
  exit 0
fi

# Origin serves but the public URL does not => the tunnel is the broken link.
log "public=$pub but origin=200 — tunnel is not serving a healthy origin. Restarting $UNIT."
systemctl restart "$UNIT"

for i in $(seq 1 12); do
  sleep 5
  pub="$(code "$PUBLIC_URL")"
  if [ "$pub" = "200" ]; then
    log "recovered after ${i} checks: $PUBLIC_URL -> 200"
    exit 0
  fi
done
log "STILL BROKEN after restart: $PUBLIC_URL -> $pub (origin 200). Needs a human."
exit 1
