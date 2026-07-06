# vix-bridge — mirror VIX from prod Kafka into dev

Dev has no IBKR feed, so it has no `underlying.vix.price`. This bridge live-mirrors that one topic
from **prod** Kafka into **dev** Kafka, so the dev pipeline/gateway gets a real VIX.

## Why this is simple
`underlying.vix.price` is **plain JSON** (`StringSerializer` key + value in ibkr-feed's
`KafkaIndexPricePublisher`), **not** Confluent-Avro. No schema ids to align across clusters — the
bridge just forwards the raw key/value bytes (verified byte-exact). 1 partition.

## What it does
- Assigns the source partition directly (no consumer group → nothing to go stale on dev's nightly wipe).
- On start, **primes** from the last retained record so dev immediately has the *current* VIX
  (matters off-hours / right after a dev Kafka wipe, when no new tick may arrive for a while).
- Then tails forever, forwarding each new record (key, value, timestamp) to dev.
- Re-sending the latest value on restart is harmless — the gateway is last-value-wins for VIX.

## Files
| file | what |
|---|---|
| `vix_bridge.py` | core: stateless prod→dev mirror of one topic (bytes in/out) |
| `run-vix-bridge.sh` | wrapper: ensures the venv, sets prod/dev defaults, runs it |
| `com.optionsedge.vixbridge.plist` | launchd daemon (KeepAlive) to run it continuously |

## Run
**Foreground (test):**
```bash
SOURCE_BOOTSTRAP=192.168.100.252:9092 TARGET_BOOTSTRAP=localhost:19092 ./run-vix-bridge.sh
```
**As a daemon:**
```bash
cp com.optionsedge.vixbridge.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.optionsedge.vixbridge.plist
# logs: ~/oe-ops/logs/vix-bridge.log
```

## Prerequisite
Prod must actually be **producing** VIX — i.e. the VIX-only `ibkr-feed`
(`IBKR_CHAIN_ENABLED=false`, connected to prod's IB Gateway) deployed to prod. Until then the prod
topic is empty and the bridge idles (it forwards nothing, no error). Reuses the pure-python venv
from `scripts/kafka-archive` (`~/.oe-kafka-archive-venv`, `kafka-python`).
