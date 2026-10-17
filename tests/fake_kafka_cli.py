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
    print("b:1 (id: 1 rack: null) -> (")
    sys.exit(0)
sys.exit(1)
