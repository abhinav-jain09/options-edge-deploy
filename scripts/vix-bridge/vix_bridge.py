#!/usr/bin/env python3
"""
vix_bridge.py — live-mirror a single JSON topic (underlying.vix.price) from a SOURCE Kafka
(production) into a TARGET Kafka (dev), so dev gets VIX even though it has no IBKR feed.

WHY a plain copy works: underlying.vix.price is String key + String JSON value (StringSerializer
in ibkr-feed's KafkaIndexPricePublisher) — NOT Confluent-Avro. There are no schema ids to align
across clusters, so we just forward the raw key/value bytes. It stays format-agnostic (bytes in,
bytes out) so it keeps working even if the payload format ever changes.

Design — stateless, restart-safe, no consumer group:
  * assign() the source partitions directly (no group offsets to manage / go stale on dev's daily wipe)
  * on startup, seek each partition to end-1 so dev immediately gets the CURRENT VIX (important
    off-hours / right after a dev Kafka wipe, when no new tick may arrive for a while)
  * then tail forever, forwarding every new record (same key, value, timestamp) to the target
  * re-sending the latest value on restart is harmless — the gateway is last-value-wins for VIX

Usage:
  vix_bridge.py --source <prodhost:9092> --target <devhost:19092> [--topic underlying.vix.price]
                [--no-prime] [--poll-ms 1000]
"""
import argparse, sys, time
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def ensure_target_topic(bootstrap, topic, partitions):
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap.split(","), request_timeout_ms=10000)
    except Exception as e:
        print(f"[vix-bridge] WARN could not open admin on target ({type(e).__name__}: {e}); "
              f"assuming topic exists", flush=True)
        return
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"[vix-bridge] created target topic {topic} ({partitions}p rf1)", flush=True)
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"[vix-bridge] WARN create_topics({topic}): {type(e).__name__}: {e}", flush=True)
    finally:
        admin.close()


def run(source, target, topic, prime, poll_ms):
    consumer = KafkaConsumer(
        bootstrap_servers=source.split(","),
        enable_auto_commit=False,
        key_deserializer=lambda b: b, value_deserializer=lambda b: b,   # raw bytes, no decode
        fetch_max_bytes=10485760, request_timeout_ms=30000,
    )
    parts = None
    for _ in range(30):
        parts = consumer.partitions_for_topic(topic)
        if parts:
            break
        print(f"[vix-bridge] source topic {topic} not visible yet; retrying...", flush=True)
        time.sleep(2)
    if not parts:
        print(f"[vix-bridge] ERROR source topic {topic} not found on {source}", flush=True)
        sys.exit(1)

    tps = [TopicPartition(topic, p) for p in sorted(parts)]
    consumer.assign(tps)

    end = consumer.end_offsets(tps)
    if prime:
        # start from the last retained record per partition -> dev gets the current VIX at once
        for tp in tps:
            consumer.seek(tp, max(0, end[tp] - 1))
        print(f"[vix-bridge] priming from latest retained record; end offsets="
              f"{ {tp.partition: end[tp] for tp in tps} }", flush=True)
    else:
        consumer.seek_to_end(*tps)
        print("[vix-bridge] starting live at end (no prime)", flush=True)

    ensure_target_topic(target, topic, len(tps))
    producer = KafkaProducer(
        bootstrap_servers=target.split(","),
        acks="all", linger_ms=10,   # no compression: VIX msgs are tiny + infrequent, and the
                                     # pure-python venv has no lz4/snappy codec
        key_serializer=lambda b: b, value_serializer=lambda b: b,
    )

    forwarded = 0
    last_beat = time.monotonic()
    print(f"[vix-bridge] mirroring {topic}: {source} -> {target}", flush=True)
    while True:
        batch = consumer.poll(timeout_ms=poll_ms)
        for tp, recs in batch.items():
            for r in recs:
                producer.send(topic, key=r.key, value=r.value,
                              timestamp_ms=r.timestamp if r.timestamp and r.timestamp > 0 else None,
                              partition=tp.partition if tp.partition < len(tps) else None)
                forwarded += 1
        if batch:
            producer.flush()
        now = time.monotonic()
        if now - last_beat >= 60:
            print(f"[vix-bridge] heartbeat: forwarded={forwarded}", flush=True)
            last_beat = now


def main():
    ap = argparse.ArgumentParser(description="Mirror underlying.vix.price from prod Kafka to dev Kafka")
    ap.add_argument("--source", required=True, help="source (prod) bootstrap, e.g. 192.168.100.252:9092")
    ap.add_argument("--target", required=True, help="target (dev) bootstrap, e.g. localhost:19092")
    ap.add_argument("--topic", default="underlying.vix.price")
    ap.add_argument("--no-prime", action="store_true", help="don't seed the latest retained value on start")
    ap.add_argument("--poll-ms", type=int, default=1000)
    a = ap.parse_args()
    while True:
        try:
            run(a.source, a.target, a.topic, not a.no_prime, a.poll_ms)
        except KeyboardInterrupt:
            print("[vix-bridge] stopped", flush=True)
            return
        except Exception as e:
            print(f"[vix-bridge] ERROR {type(e).__name__}: {e}; reconnecting in 5s", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
