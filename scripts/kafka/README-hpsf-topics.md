# HPSF Kafka Topics

`create-hpsf-topics.sh` creates the HPSF V2.1 Databento/HPSF topics for Abhinav's current two-broker Kafka cluster.

Cluster constraint:

- `replication.factor=1`
- `min.insync.replicas=1`
- `compression.type=lz4`

RF=1 has no broker-failure durability. If the broker owning a partition is lost, that partition is unavailable until the broker/data returns.

## Dry Run

```bash
scripts/kafka/create-hpsf-topics.sh --dry-run
```

## Apply

```bash
export PATH="/home/confluent/confluent-8.2.1/bin:$PATH"
export KAFKA_BOOTSTRAP_SERVERS="192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096"
export KAFKA_TOPIC_REPLICATION_FACTOR=1
export KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1
scripts/kafka/create-hpsf-topics.sh
```

## Verify

```bash
scripts/kafka/verify-hpsf-topics.sh
```

The verification script checks partition count, replication factor, `cleanup.policy`, `retention.ms`, `min.insync.replicas`, and `compression.type` for every required HPSF topic.

Critical retention rules:

- `options.opra.tcbbo`, `options.opra.trades`, `options.opra.quotes`: 1 day.
- `options.hpsf.strike-flow`, `options.hpsf.strike-score`: 2 days, compacted plus delete.
- `options.hpsf.signal`: 30 days, delete-only, not compacted.
- `options.hpsf.latest-signal`: 30 days, compacted plus delete.
- `options.hpsf.dlq` and `options.hpsf.writer-dlq`: 30 days.
