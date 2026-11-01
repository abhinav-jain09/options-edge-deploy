#!/bin/bash
# Shows the current route and whether the dev machine really has the forwarded ports.
D="$HOME/Library/dev-gateway-tunnel"; LOG="$HOME/Library/Logs/ib-gateway-reverse-tunnel.log"
if [ -s "$D/state" ]; then read -r route host since < "$D/state"; echo "tunnel UP via $route ($host) for $(( $(date +%s) - since )) s"; else echo "tunnel DOWN (supervisor is retrying)"; fi
for h in 192.168.100.102 100.117.21.102; do printf "%-16s port 22: " $h; /usr/bin/nc -z -G 3 $h 22 >/dev/null 2>&1 && echo reachable || echo no answer; done
echo "--- last log lines:"; tail -6 "$LOG" 2>/dev/null
