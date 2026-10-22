#!/usr/bin/env python3
"""A stateful stand-in for kafka-topics / kafka-configs / kafka-get-offsets / kafka-broker-api-versions, dispatched by
the name it is invoked as (symlink or copy per name).  State lives in the JSON file named by FAKE_KAFKA_STATE so one
test run sees its own deletes and creates; every invocation is appended to FAKE_KAFKA_CALLS."""
import json, os, sys

state_path = os.environ["FAKE_KAFKA_STATE"]
with open(state_path) as f:
    state = json.load(f)   # {"topics": {name: {"partitions": n, "rf": n, "configs": {...}, "end": n}}}
argv = sys.argv[1:]
name = os.path.basename(sys.argv[0])
with open(os.environ["FAKE_KAFKA_CALLS"], "a") as f:
    f.write(name + " " + " ".join(argv) + "\n")


def arg(flag, default=None):
    return argv[argv.index(flag) + 1] if flag in argv else default


def save():
    with open(state_path, "w") as f:
        json.dump(state, f)


topics = state["topics"]
if name == "kafka-topics":
    if "--list" in argv:
        print("\n".join(sorted(topics)))
    elif "--describe" in argv and "--topic" not in argv:
        for t, d in sorted(topics.items()):
            cfg = ",".join(f"{k}={v}" for k, v in d["configs"].items())
            print(f"Topic: {t}\tTopicId: x\tPartitionCount: {d['partitions']}\tReplicationFactor: {d['rf']}\tConfigs: {cfg}")
    elif "--describe" in argv:
        t = arg("--topic")
        if t in topics:
            d = topics[t]
            print(f"Topic: {t}\tTopicId: x\tPartitionCount: {d['partitions']}\tReplicationFactor: {d['rf']}\tConfigs: ")
    elif "--delete" in argv:
        topics.pop(arg("--topic"), None)
        save()
    elif "--create" in argv:
        t = arg("--topic")
        cfg = {}
        for i, a in enumerate(argv):
            if a == "--config":
                k, _, v = argv[i + 1].partition("=")
                cfg[k] = v.strip("[]")
        topics[t] = {"partitions": int(arg("--partitions")), "rf": int(arg("--replication-factor")), "configs": cfg, "end": 0}
        save()
    sys.exit(0)
if name == "kafka-configs" and arg("--entity-type") == "brokers":
    # state["brokers"] = {id: {config: value}} (effective values); state["broker_pinned"] = [ids whose own dynamic override wins over a cluster default];
    # FAKE_KAFKA_BROKER_ALTER_FAILS=1 makes the cluster-default alter fail.
    brokers = state.setdefault("brokers", {})
    if "--describe" in argv:
        b = brokers.get(arg("--entity-name"))
        if b is None:
            print("Broker not found", file=sys.stderr)
            sys.exit(1)
        print(f"All configs for broker {arg('--entity-name')} are:")
        for k, v in b.items():
            print(f"  {k}={v} sensitive=false synonyms={{DYNAMIC_DEFAULT_BROKER_CONFIG:{k}={v}}}")
    elif "--alter" in argv and "--entity-default" in argv:
        if os.environ.get("FAKE_KAFKA_BROKER_ALTER_FAILS"):
            print("alter refused", file=sys.stderr)
            sys.exit(1)
        k, _, v = arg("--add-config").partition("=")
        for bid, b in brokers.items():
            if bid not in state.get("broker_pinned", []):
                b[k] = v
        save()
    sys.exit(0)
if name == "kafka-configs":
    t = arg("--entity-name")
    if "--describe" in argv:
        cfg = topics.get(t, {}).get("configs", {})
        print(f"Dynamic configs for topic {t} are:")
        print("  " + " ".join(f"{k}={v}" for k, v in cfg.items()))
    elif "--alter" in argv:
        add = arg("--add-config")
        # split on commas outside [brackets]
        parts, depth, cur = [], 0, ""
        for ch in add:
            if ch == "[": depth += 1
            if ch == "]": depth -= 1
            if ch == "," and depth == 0:
                parts.append(cur); cur = ""
            else:
                cur += ch
        parts.append(cur)
        for p in parts:
            k, _, v = p.partition("=")
            topics[t]["configs"][k] = v.strip("[]")
        save()
    sys.exit(0)
if name == "kafka-get-offsets":
    t = arg("--topic")
    drop = os.environ.get("FAKE_KAFKA_OFFSETS_DROP_PARTITION")   # "<topic>:<p>" — that partition line is not printed
    for p in range(topics[t]["partitions"]):
        if drop == f"{t}:{p}":
            continue
        print(f"{t}:{p}:{topics[t]['end']}")
    sys.exit(0)
if name == "kafka-broker-api-versions":
    for bid in state.get("brokers", {"1": {}}):
        print(f"b:{bid} (id: {bid} rack: null) -> (")
    sys.exit(0)
sys.exit(1)
