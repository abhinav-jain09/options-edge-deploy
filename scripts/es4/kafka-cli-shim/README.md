# es4 Kafka CLI shim

`scripts/kafka/apply-topics.sh` — the SINGLE topic-applier Jenkins uses for dev and prod — invokes the
Kafka CLI directly (`kafka-topics`, `kafka-consumer-groups`, `kafka-configs`, `kafka-broker-api-versions`,
`kafka-reassign-partitions`). On the es4 box those binaries do **not** exist on the host: Kafka runs as
the `es4-kafka` Docker container and the CLI lives inside it.

Rather than fork a second topic-applier for es4 (which is what `scripts/es4/create-es-topics.sh` was,
and it drifted from `topics.env`), this directory provides thin PATH shims that proxy each CLI call
into the container. es4 then runs the *same* `apply-topics.sh` as every other environment:

```sh
PATH="$REPO/scripts/es4/kafka-cli-shim:$PATH" \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC_REPLICATION_FACTOR=1 \
TOPIC_SET=es4 \
  bash scripts/kafka/apply-topics.sh
```

`TOPIC_SET=es4` selects `OPTIONS_EDGE_ES4_*` from `topics.env`, so es4 gets only its ~45 `es.*`
topics — never the ~52 SPX `options.*` topics, which must not exist on that broker.
