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
set -u
LAN_HOST=${LAN_HOST:-192.168.100.102}
TS_HOST=${TS_HOST:-100.117.21.102}
KEYALIAS=${KEYALIAS:-dev-oe.tail4fa2fd.ts.net}      # the dev machine's host key in known_hosts, whichever address reaches it
REMOTE_USER=${REMOTE_USER:-abhinav}
PORTS=${PORTS:-"4001 4002 7496 7497"}
LOG=${LOG:-$HOME/Library/Logs/ib-gateway-reverse-tunnel.log}
STATE=${STATE:-$HOME/Library/dev-gateway-tunnel/state}
SSHOPTS=(-o BatchMode=yes -o ConnectTimeout=8 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -o TCPKeepAlive=yes
         -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=yes -o HostKeyAlias="$KEYALIAS" -o ControlMaster=no -o ControlPath=none)
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }
up()  { if [ "$1" = "$LAN_HOST" ] && [ -e "$STATE.lan-down" ]; then return 1; fi; /usr/bin/nc -z -G 3 "$1" 22 >/dev/null 2>&1; }   # `touch <state>.lan-down` treats the LAN as down (forces Tailscale; also the test hook)
rports=(); for p in $PORTS; do rports+=(-R "127.0.0.1:$p:127.0.0.1:$p"); done
clean_remote() {   # an ssh session that died without a goodbye keeps its listeners on the dev machine; only a listener owned by sshd is touched
  ssh "${SSHOPTS[@]}" "$REMOTE_USER@$1" "for p in $PORTS; do for pid in \$(/usr/sbin/lsof -nP -iTCP:\$p -sTCP:LISTEN -t 2>/dev/null); do case \"\$(/bin/ps -o comm= -p \$pid)\" in *sshd*) kill \$pid;; esac; done; done" >/dev/null 2>&1
}
backoff=2; lan_bad_until=0
/usr/bin/pkill -u "$(id -u)" -f "ssh .* -N .*-R 127.0.0.1:${PORTS%% *}:127.0.0.1:" 2>/dev/null   # an orphan from a supervisor that died hard
log "supervisor start pid $$ ports: $PORTS lan=$LAN_HOST tailscale=$TS_HOST"
while true; do
  host=""; route=""
  if [ "$(date +%s)" -ge "$lan_bad_until" ] && up "$LAN_HOST"; then host=$LAN_HOST; route=lan
  elif up "$TS_HOST"; then host=$TS_HOST; route=tailscale; fi
  if [ -z "$host" ]; then log "no route (LAN and Tailscale both down); retrying in ${backoff}s"; sleep "$backoff"; backoff=$(( backoff * 2 > 30 ? 30 : backoff * 2 )); continue; fi
  clean_remote "$host"
  log "connecting via $route ($host)"
  ssh "${SSHOPTS[@]}" -N "${rports[@]}" "$REMOTE_USER@$host" & spid=$!
  started=$SECONDS; echo "$route $host $(date +%s)" > "$STATE"; lanfail=0
  while kill -0 "$spid" 2>/dev/null; do
    sleep 5
    if [ "$route" = tailscale ] && [ "$(date +%s)" -ge "$lan_bad_until" ] && up "$LAN_HOST"; then log "LAN is reachable again: switching from Tailscale to LAN"; kill "$spid" 2>/dev/null; break; fi
    if [ "$route" = lan ]; then
      if up "$LAN_HOST"; then lanfail=0; else lanfail=$((lanfail + 1)); if [ "$lanfail" -ge 3 ]; then log "LAN lost: dropping the tunnel to fail over to Tailscale"; kill "$spid" 2>/dev/null; break; fi; fi
    fi
  done
  wait "$spid" 2>/dev/null; rc=$?; ran=$((SECONDS - started)); rm -f "$STATE"
  log "tunnel via $route ended after ${ran}s (ssh exit $rc)"
  if [ "$route" = lan ] && [ "$ran" -lt 20 ] && [ "$rc" -eq 255 ]; then lan_bad_until=$(( $(date +%s) + 60 )); log "LAN route failed fast (wrong host, auth or key): skipping it for 60 s"; fi
  if [ "$ran" -ge 60 ]; then backoff=2; else sleep "$backoff"; backoff=$(( backoff * 2 > 30 ? 30 : backoff * 2 )); fi
done
