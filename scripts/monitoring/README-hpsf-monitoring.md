# HPSF V2.1 Monitoring

HPSF V2.1 is Databento-based and signal-only. Monitoring must never depend on IBKR order services or any live order path.

## Required Metrics

- `hpsf.trades.classified.count`
- `hpsf.trades.unknown.count`
- `hpsf.signals.no_trade.count`
- `hpsf.signals.buy_call_early.count`
- `hpsf.signals.buy_put_early.count`
- `hpsf.evaluation.duration.ms`
- `hpsf.active_strikes.count`
- `hpsf.vwap.spx_equivalent`
- `hpsf.kafka.lag`
- `hpsf.state.strike_bucket.keys`
- `hpsf.state.old_bucket_keys.deleted.count`
- `hpsf.changelog.restore.duration.ms`
- `hpsf.dlq.count`
- `hpsf.writer.dlq.count`

Some Java exporters normalize dots to underscores. Alert rules therefore accept both dotted and underscore metric names where practical.

## Scrape Targets

- `hpsf-postgres-writer-service:8080/metrics`
- `feed-gateway-service:8091/metrics` when gateway metrics are enabled
- Stage A and Stage B processing metrics should be added to Prometheus after the processing runtime exposes an HTTP metrics endpoint. Until then, Kafka lag, topic verification, and pod/process health are the runtime smoke checks.

## Alerts

Alert definitions live in `scripts/monitoring/hpsf-alert-rules.yaml` and cover:

- Kafka disk pressure above 75 percent.
- OPRA lag too high.
- Underlying ES/SPX lag too high.
- Evaluation duration above interval.
- HPSF DLQ increasing fast.
- Writer DLQ nonzero.
- Postgres writer unhealthy.
- Changelog restore duration above 60 seconds.
- Stale data gates active too long.
- Old bucket keys not deleted.

## RF=1 Warning

Abhinav's current Kafka cluster uses RF=1 and `min.insync.replicas=1`. This is a capacity constraint, not high availability. A broker loss can lose HPSF topic data. Runbooks and dashboards must not claim HA until the cluster is upgraded.
