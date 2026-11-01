# IB Gateway / TWS tunnel: this Mac -> the dev machine

IB Gateway (Paper) and TWS (Live) run on **this Mac**; the dev services run on the dev machine and still connect to
`host.docker.internal:4001|4002|7496|7497`. A **reverse** ssh tunnel makes those four ports on the dev machine's loopback
land on this Mac's `127.0.0.1:<same port>`, so no dev deployment setting changes. The IB API sees the clients as `127.0.0.1`,
so Gateway/TWS can keep "allow connections from localhost only".

| Port | What |
|---|---|
| 4001 | IB Gateway, live |
| 4002 | IB Gateway, paper (also the market-data feed) |
| 7496 | TWS, live |
| 7497 | TWS, paper |

## Behaviour (all tested 2026-11-01)

- **Restart-proof:** `com.optionsedge.ib-gateway-reverse-tunnel` LaunchAgent (`RunAtLoad`, `KeepAlive`), wrapped in `caffeinate -i` so an idle Mac does not sleep. It starts at login; kill -9 of the supervisor or of ssh brings it back within seconds.
- **Immediate:** with no route it probes both addresses every second and connects the moment one answers (0.4 s measured); a tunnel that was up reconnects at once, one that fails fast retries after 1, 2, 3, 4, 5 s and never waits longer. The dev feed retries its own connection every 3 s, so data resumes within about 3 s of the Gateway being up. (The dev broker's own retry delay grows from 1 s to 60 s after a long outage.)
- **Outage-proof:** ssh keepalives (dead peer noticed in ~10 s), route checks every 2 s, and a pre-connect cleanup of a stale remote `sshd` that still holds the ports after an ungraceful drop (only a listener owned by `sshd` is ever killed).
- **Route:** home network `192.168.100.102` whenever its ssh port answers, else Tailscale `100.117.21.102`. While on Tailscale it re-checks the LAN every 2 s and moves back as soon as it answers; on the LAN, 2 failed 2 s checks fail over to Tailscale (3.7 s measured), and the switch back to the LAN took 4.2 s. A LAN address that answers but is another machine fails the pinned host key (`HostKeyAlias dev-oe.tail4fa2fd.ts.net`, `StrictHostKeyChecking yes`) and is skipped for 60 s.
- **Force Tailscale** (also the test hook): `touch ~/Library/dev-gateway-tunnel/state.lan-down`; remove the file to go back.

## Install / operate

```bash
mkdir -p ~/Library/dev-gateway-tunnel && cp tunnel.sh status.sh ~/Library/dev-gateway-tunnel/ && chmod +x ~/Library/dev-gateway-tunnel/*.sh
sed "s#__HOME__#$HOME#g" com.optionsedge.ib-gateway-reverse-tunnel.plist.template > ~/Library/LaunchAgents/com.optionsedge.ib-gateway-reverse-tunnel.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.optionsedge.ib-gateway-reverse-tunnel.plist
~/Library/dev-gateway-tunnel/status.sh          # route, reachability, last log lines   (log: ~/Library/Logs/ib-gateway-reverse-tunnel.log)
launchctl bootout gui/$(id -u)/com.optionsedge.ib-gateway-reverse-tunnel   # stop
```

The old laptop->dev *pull* tunnel (`com.optionsedge.ib-gateway-live-tunnel`, `-L 4001/4002`) must stay disabled: it would hold the laptop's 4001/4002 and block the local Gateway.

## Things the tunnel cannot do for you

- One IBKR session per login: the dev machine's Gateway must stay logged out, and Paper and Live need separate sessions.
- A Live login needs your 2FA; IBKR restarts Gateway nightly and re-authenticates weekly.
- The Mac must stay awake and logged in (the LaunchAgent runs in your session; lid-close sleeps a laptop unless on AC with a display).
- Over Tailscale the round trip is 70-280 ms: fine for streaming, slower for order placement and big chain subscriptions.
- The dev machine's `sshd` must stay reachable on port 22 over the LAN or Tailscale.
