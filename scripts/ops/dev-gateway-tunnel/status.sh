#!/bin/bash
# Shows the current route and whether the dev machine really has the forwarded ports.
D="$HOME/Library/dev-gateway-tunnel"; LOG="$HOME/Library/Logs/ib-gateway-reverse-tunnel.log"
. "$D/market.sh"
if [ -e "$D/state.force-on" ]; then echo "market: FORCED ON (state.force-on exists; remove it to follow the calendar)"; else market_eval "$(date +%s)"; case $MKT_STATE in open) echo "market: OPEN, the tunnel window ends $(et_text $MKT_END)";; closed) echo "market: CLOSED, no tunnel until $(et_text $MKT_NEXT)";; *) echo "market: date outside the calendar table, tunnel kept up";; esac; fi
if [ -s "$D/state" ]; then read -r route host since < "$D/state"; echo "tunnel UP via $route ($host) for $(( $(date +%s) - since )) s"; elif [ "${MKT_STATE:-}" = closed ] && [ ! -e "$D/state.force-on" ]; then echo "tunnel DOWN by design: the market is closed"; else echo "tunnel DOWN (supervisor is retrying)"; fi
for h in 192.168.100.102 100.117.21.102; do printf "%-16s port 22: " $h; /usr/bin/nc -z -G 3 $h 22 >/dev/null 2>&1 && echo reachable || echo no answer; done
echo "--- last log lines:"; tail -6 "$LOG" 2>/dev/null
