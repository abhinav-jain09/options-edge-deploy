# Broker heap bump on `.2` (experiment Mac)

Manual ops runbook. The Kafka broker on `.2` is a **native Mac install** under
`~/kafka-options-edge/` — not a Kubernetes Deployment and not git-tracked. So
heap changes can't go through Jenkins / `kubectl set image` like everything
else on `.2`. This doc lists the exact steps.

## Why this bump

The broker on `.2` was running with `-Xms512m -Xmx1g`. Under market-hours load
across ~22 service pods publishing/consuming, G1GC stalled the broker often
enough to trigger overlapping `OFFSET_COMMIT` responses arriving out of order
to clients. Multiple Streams services (gex, strike-flow, unified-sr) crashed
**synchronised in time** with `CorrelationIdMismatchException` — definitive
broker-side stall, not a client-side race.

The paired processing-side fix (10× less commit pressure) shipped as PRs
#179 / #180 / #181 (experiment / dev / main). This doc covers the broker-side
fix on `.2`.

## What changes

Edit one line in `~/kafka-options-edge/run-kafka-foreground.sh`:

```diff
- export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms512m -Xmx1g}"
+ export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms2g -Xmx4g}"
```

That's it on disk. The broker then needs a restart so the JVM picks up the
new heap.

## Exact commands (paste on `.2`'s shell)

```bash
# 1. Edit the launch wrapper.
sed -i.bak \
  's|-Xms512m -Xmx1g|-Xms2g -Xmx4g|' \
  ~/kafka-options-edge/run-kafka-foreground.sh

# 2. Verify the diff before restart.
diff ~/kafka-options-edge/run-kafka-foreground.sh \
     ~/kafka-options-edge/run-kafka-foreground.sh.bak
# expect a single-line change on the KAFKA_HEAP_OPTS line.

# 3. Restart the broker via launchd.
#    The launchd label is `local.options-edge.kafka` (per
#    ~/Library/LaunchAgents/local.options-edge.kafka.plist).
launchctl unload  ~/Library/LaunchAgents/local.options-edge.kafka.plist
launchctl load -w ~/Library/LaunchAgents/local.options-edge.kafka.plist

# 4. Verify the new heap is in effect.
ps -eo pid,args | grep '[k]afka\.Kafka' | grep -oE 'Xms[0-9a-z]+ -Xmx[0-9a-z]+'
# expect:  Xms2g -Xmx4g
```

## Verify it took

```bash
# Broker accepts client connections again
~/kafka-options-edge/current/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:19092 | head -1

# Streams pods rebalance and resume consuming (give them ~30s after restart)
/Applications/Docker.app/Contents/Resources/bin/kubectl -n options-edge \
  get pods --no-headers | awk '{print $3}' | sort | uniq -c
# expect: most/all 'Running', none 'CrashLoopBackOff'

# A real test — watch for new records on the strike-sr output topic during
# market hours; this is the canary that the unified-sr pipeline is healthy:
~/oe-native/confluent-7.7.1/bin/kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic options.spx.strike-sr.current \
  --max-messages 3
```

## Expected impact

| | Before | After |
|---|---|---|
| Broker max heap | 1 GB | 4 GB |
| GC pause behaviour | Frequent stalls under load | Headroom; G1GC `MaxGCPauseMillis=20` target is hit cleanly |
| Streams services synchronised crashes | Every few hours during market hours | None expected |

The bump is conservative — `.2` has plenty of RAM, and 4 GB is well below
typical production Kafka broker sizing.

## Rolling back

```bash
mv ~/kafka-options-edge/run-kafka-foreground.sh.bak \
   ~/kafka-options-edge/run-kafka-foreground.sh
launchctl unload  ~/Library/LaunchAgents/local.options-edge.kafka.plist
launchctl load -w ~/Library/LaunchAgents/local.options-edge.kafka.plist
```

## Why this isn't a Jenkins / kubectl change

Per the Jenkins-only deployment rule, runtime changes go through Jenkins.
The Kafka broker on `.2` is a native host install (not a k8s Deployment, not
a container image), so there's nothing for Jenkins to deploy — its config
lives on the host's filesystem, not in any image. This is the documented
exception.

Production (`.252`) and dev (`localhost:19092` Mac install on a different host)
have their own broker configs and aren't touched by this runbook.
