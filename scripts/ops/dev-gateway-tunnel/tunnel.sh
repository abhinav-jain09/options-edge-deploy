#!/bin/bash
# Reverse tunnel: the dev machine's 127.0.0.1:<port> -> this Mac's IB Gateway / TWS on 127.0.0.1:<port>, for the ports
# 4001 (Gateway live) 4002 (Gateway paper) 7496 (TWS live) 7497 (TWS paper). The dev pods reach it as host.docker.internal:<port>.
#
# Restart-proof : run by launchd (RunAtLoad + KeepAlive); this script itself never exits.
# Outage-proof  : ssh keepalives (dead peer detected in ~30 s), a supervisor loop that reconnects with a capped backoff, and a
#                 pre-connect cleanup of a stale remote sshd that still holds the ports after an ungraceful drop.
# Route         : the home network (192.168.100.102) is used whenever it answers; otherwise Tailscale (100.117.21.102). While on
#                 Tailscale it checks every 5 s and switches back to the LAN the moment the LAN is reachable again. A LAN address that
#                 answers but is not the dev machine (another network) fails the pinned host key and is skipped for 60 s.
# Market-aware: the tunnel only runs while the SPX/SPXW options session (the code's SpxSessionCalendar, see market.sh) is open, plus a
# small lead/lag. Closed (nights between 16:00 and 20:15 ET, weekends, exchange holidays): no tunnel, nothing to connect to. The window
# opening is acted on to the second. `touch <state>.force-on` keeps the tunnel up regardless; MARKET_AWARE=0 turns the calendar off.
set -u
LAN_HOST=${LAN_HOST:-192.168.100.102}
TS_HOST=${TS_HOST:-100.117.21.102}
KEYALIAS=${KEYALIAS:-dev-oe.tail4fa2fd.ts.net}      # the dev machine's host key in known_hosts, whichever address reaches it
REMOTE_USER=${REMOTE_USER:-abhinav}
PORTS=${PORTS:-"4001 4002 7496 7497"}
LOG=${LOG:-$HOME/Library/Logs/ib-gateway-reverse-tunnel.log}
STATE=${STATE:-$HOME/Library/dev-gateway-tunnel/state}
MARKET_AWARE=${MARKET_AWARE:-1}
. "$(cd "$(dirname "$0")" && pwd)/market.sh"
now() { if [ -r "$STATE.now" ]; then cat "$STATE.now"; else date +%s; fi; }   # `<state>.now` holding an epoch overrides the clock (test hook)
SSHOPTS=(-o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 -o TCPKeepAlive=yes
         -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=yes -o HostKeyAlias="$KEYALIAS" -o ControlMaster=no -o ControlPath=none)
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }
up()  { if [ "$1" = "$LAN_HOST" ] && [ -e "$STATE.lan-down" ]; then return 1; fi; /usr/bin/nc -z -G 1 "$1" 22 >/dev/null 2>&1; }   # `touch <state>.lan-down` treats the LAN as down (forces Tailscale; also the test hook)
rports=(); for p in $PORTS; do rports+=(-R "127.0.0.1:$p:127.0.0.1:$p"); done
clean_remote() {   # an ssh session that died without a goodbye keeps its listeners on the dev machine; only a listener owned by sshd is touched
  ssh "${SSHOPTS[@]}" "$REMOTE_USER@$1" "for p in $PORTS; do for pid in \$(/usr/sbin/lsof -nP -iTCP:\$p -sTCP:LISTEN -t 2>/dev/null); do case \"\$(/bin/ps -o comm= -p \$pid)\" in *sshd*) kill \$pid;; esac; done; done" >/dev/null 2>&1
}
market_gate() {   # 0 = the tunnel may run now; 1 = the market is closed (MKT_NEXT = when the next window starts)
  [ "$MARKET_AWARE" = 1 ] || { MKT_STATE=open; MKT_END=9999999999; return 0; }
  if [ -e "$STATE.force-on" ]; then MKT_STATE=forced; MKT_END=9999999999; return 0; fi
  market_eval "$(now)"; [ "$MKT_STATE" != closed ]
}
market_still_open() {   # cheap per-check: the calendar is re-read only when the window ended or the force file went away
  [ "$MARKET_AWARE" = 1 ] || return 0
  [ -e "$STATE.force-on" ] && return 0
  if [ "$MKT_STATE" = forced ] || [ "$(now)" -ge "$MKT_END" ]; then market_gate; return $?; fi
  return 0
}
backoff=1; lan_bad_until=0; noroute=''; mkt_closed_logged=''
/usr/bin/pkill -u "$(id -u)" -f "ssh .* -N .*-R 127.0.0.1:${PORTS%% *}:127.0.0.1:" 2>/dev/null   # an orphan from a supervisor that died hard
rm -f "$STATE"   # a marker left by a supervisor that was killed must not read as "up"
log "supervisor start pid $$ ports: $PORTS lan=$LAN_HOST tailscale=$TS_HOST"
while true; do
  if ! market_gate; then
    rm -f "$STATE"
    if [ "$mkt_closed_logged" != "$MKT_NEXT" ]; then log "market closed: no tunnel until $(et_text "$MKT_NEXT") (SPX options session per SpxSessionCalendar; touch state.force-on to override)"; mkt_closed_logged=$MKT_NEXT; fi
    w=$(( MKT_NEXT - $(now) )); [ "$w" -gt 30 ] && w=30; [ "$w" -lt 1 ] && w=1; sleep "$w"; continue
  fi
  if [ -n "$mkt_closed_logged" ]; then log "market window opened ($MKT_STATE): connecting"; mkt_closed_logged=''; fi
  host=""; route=""
  if [ "$(date +%s)" -ge "$lan_bad_until" ] && up "$LAN_HOST"; then host=$LAN_HOST; route=lan
  elif up "$TS_HOST"; then host=$TS_HOST; route=tailscale; fi
  if [ -z "$host" ]; then [ -z "${noroute:-}" ] && log "no route (LAN and Tailscale both down): probing every second, connecting the moment one answers"; noroute=1; sleep 1; continue; fi
  noroute=''; 
  clean_remote "$host"
  log "connecting via $route ($host)"
  ssh "${SSHOPTS[@]}" -N "${rports[@]}" "$REMOTE_USER@$host" & spid=$!
  started=$SECONDS; echo "$route $host $(date +%s)" > "$STATE"; lanfail=0; tsfail=0
  while kill -0 "$spid" 2>/dev/null; do
    sleep 2
    market_still_open || { log "market closed: dropping the tunnel (next window $(et_text "$MKT_NEXT"))"; kill "$spid" 2>/dev/null; break; }
    if [ "$route" = tailscale ]; then
      if [ "$(date +%s)" -ge "$lan_bad_until" ] && up "$LAN_HOST"; then log "LAN is reachable again: switching from Tailscale to LAN"; kill "$spid" 2>/dev/null; break; fi
      if up "$TS_HOST"; then tsfail=0; else tsfail=$((tsfail + 1)); if [ "$tsfail" -ge 2 ]; then log "Tailscale lost: dropping the tunnel to reconnect the moment a route answers"; kill "$spid" 2>/dev/null; break; fi; fi
    else
      if up "$LAN_HOST"; then lanfail=0; else lanfail=$((lanfail + 1)); if [ "$lanfail" -ge 2 ]; then log "LAN lost: dropping the tunnel to fail over"; kill "$spid" 2>/dev/null; break; fi; fi
    fi
  done
  wait "$spid" 2>/dev/null; rc=$?; ran=$((SECONDS - started)); rm -f "$STATE"
  log "tunnel via $route ended after ${ran}s (ssh exit $rc)"
  if [ "$route" = lan ] && [ "$ran" -lt 20 ] && [ "$rc" -eq 255 ]; then lan_bad_until=$(( $(date +%s) + 60 )); log "LAN route failed fast (wrong host, auth or key): skipping it for 60 s"; fi
  if [ "$ran" -ge 20 ]; then backoff=1; else sleep "$backoff"; backoff=$(( backoff + 1 > 5 ? 5 : backoff + 1 )); fi   # a tunnel that lived reconnects at once; one that fails fast retries after 1, 2, 3, 4, 5 s, never longer
done
