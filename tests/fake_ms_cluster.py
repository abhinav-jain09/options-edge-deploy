#!/usr/bin/env python3
"""A stateful stand-in for the Kafka CLIs and kubectl that scripts/kafka/roll-forward-market-structure.sh drives, dispatched by the name it is invoked
as (kafka-topics[.sh], kafka-configs, kafka-get-offsets, kafka-consumer-groups, kafka-console-consumer, kafka-transactions, kubectl).  State is the JSON
file named by MS_CLUSTER_STATE; every invocation is appended to MS_CLUSTER_CALLS so a test can assert on what was (not) called."""
import json
import os
import random
import string
import sys

state_path = os.environ["MS_CLUSTER_STATE"]
state = json.load(open(state_path))
argv = sys.argv[1:]
name = os.path.basename(sys.argv[0]).removesuffix(".sh")
with open(os.environ["MS_CLUSTER_CALLS"], "a") as f:
    f.write(name + " " + " ".join(argv) + "\n")
topics = state["topics"]


def save():
    json.dump(state, open(state_path, "w"))


def arg(flag, default=None):
    return argv[argv.index(flag) + 1] if flag in argv else default


def new_id():
    return "".join(random.choice(string.ascii_letters + string.digits + "_-") for _ in range(22))


if name == "kafka-topics":
    t = arg("--topic")
    if "--list" in argv:
        if state.get("list_fails"):
            print("broker down", file=sys.stderr); sys.exit(1)
        print("\n".join(sorted(topics)))
    elif "--describe" in argv:
        if t not in topics:
            print("Error: topic not found", file=sys.stderr); sys.exit(1)
        d = topics[t]
        cfg = ",".join(f"{k}={v}" for k, v in d["configs"].items())
        print(f"Topic: {t}\tTopicId: {d['id']}\tPartitionCount: {d['parts']}\tReplicationFactor: {d['rf']}\tConfigs: {cfg}")
    elif "--delete" in argv:
        topics.pop(t, None); save()
    elif "--create" in argv:
        if state.get("create_fails") == t:
            print("create refused", file=sys.stderr); sys.exit(1)
        if t in topics and "--if-not-exists" in argv:
            sys.exit(0)
        cfg = {}
        for i, a in enumerate(argv):
            if a == "--config":
                k, _, v = argv[i + 1].partition("=")
                cfg[k] = v
        topics[t] = {"id": new_id(), "parts": int(arg("--partitions")), "rf": int(arg("--replication-factor")), "configs": cfg,
                     "end": {str(p): 0 for p in range(int(arg("--partitions")))}, "first": {}}
        if state.get("writer_after_create") == t:
            topics[t]["end"]["0"] = 3   # a JSON writer that is still alive
        save()
    sys.exit(0)
if name == "kafka-configs":
    t = arg("--entity-name")
    if "--alter" in argv:
        add = arg("--add-config")
        for kv in add.split(","):
            k, _, v = kv.partition("=")
            topics[t]["configs"][k] = v
        save(); sys.exit(0)
    d = topics[t]
    print(f"All configs for topic {t} are:")
    for k, v in d["configs"].items():
        print(f"  {k}={v} sensitive=false synonyms={{DYNAMIC_TOPIC_CONFIG:{k}={v}}}")
    sys.exit(0)
if name == "kafka-get-offsets":
    t = arg("--topic")
    if t not in topics:
        print("unknown topic", file=sys.stderr); sys.exit(1)
    drift = state.get("bars_moves_per_read") and t == "options.indicators.bars"
    for p in range(topics[t]["parts"]):
        if "-2" in argv:
            print(f"{t}:{p}:{topics[t]['first'].get(str(p), 0)}")
        else:
            end = topics[t]["end"][str(p)]
            if drift and p == 0:
                topics[t]["end"][str(p)] = end + 1; save()
            print(f"{t}:{p}:{end}")
    sys.exit(0)
if name == "kafka-consumer-groups":
    print("GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG")
    for row in state.get("groups", []):
        print(row)
    sys.exit(0)
if name == "kafka-console-consumer":
    t = arg("--topic")
    kind = state.get("wire", {}).get(t, "avro")
    if kind == "json":
        sys.stdout.write('{"symbol":"SPX"}')
    elif kind == "avro":
        sys.stdout.buffer.write(b"\x00\x00\x00\x08\xa1avro-bytes")
    sys.exit(0)
if name == "kafka-transactions":
    cmd = argv[argv.index("--bootstrap-server") + 2]
    own = state["producers"]["own"]
    if cmd == "describe":
        print("CoordinatorId  TransactionalId  ProducerId  ProducerEpoch  TransactionState  TransactionTimeoutMs  CurrentTransactionStartTimeMs  TransactionDurationMs  TopicPartitions")
        print(f"0  {arg('--transactional-id')}  {own}  3  Empty  60000  None  None  None")
    elif cmd == "describe-producers":
        t = arg("--topic")
        print("ProducerId  ProducerEpoch  LatestCoordinatorEpoch  LastSequence  LastTimestamp  CurrentTransactionStartOffset")
        for pid, ts in state["producers"].get("by_topic", {}).get(t, []):
            print(f"{pid}  3  0  -1  {ts}  None")
    elif cmd == "list":
        print("TransactionalId  Coordinator  ProducerId  TransactionState")
        print(f"market-structure-service-dev  0  {own}  Empty")
    sys.exit(0)
if name == "kubectl":
    if argv[:1] == ["get"] and "--raw" in argv:
        path = argv[argv.index("--raw") + 1]
        if path.endswith("/health/ready"):
            print(json.dumps({"state": state["service_state"], "error": ""}, separators=(",", ":")))
        elif path.endswith("/metrics"):
            for k, v in state["metrics"].items():
                print(f"market_structure_{k} {v}")
            if state.get("metrics_move"):
                state["metrics"]["records_consumed_total"] += 1; save()
        sys.exit(0)
    if "-A" in argv:
        print(json.dumps(state["pods"]))
        sys.exit(0)
    if "-o" in argv and "name" in argv:
        print("\n".join("pod/" + n for n in state["running_pods"]))
        sys.exit(0)
sys.exit(1)
