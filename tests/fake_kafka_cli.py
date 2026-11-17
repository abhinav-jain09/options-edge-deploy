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
# `--describe --all` on a REAL broker prints every config, one per line, with the effective value
# first and its SYNONYMS after -- including keys the topic never set, which come back as the broker
# DEFAULT rather than being absent:
#   min.compaction.lag.ms=0 sensitive=false synonyms={DEFAULT_CONFIG:log.cleaner.min.compaction.lag.ms=0}
# The bare "Dynamic configs" line below models the plain --describe and omits anything unset, so a
# guard that reads an effective value could not be tested against it at all. It also never exercised
# the trap the synonym carries: log.cleaner.min.compaction.lag.ms ENDS WITH the key being read, so a
# parser without a left boundary counts two hits for one value. Modelled here so the barriers are
# tested against output shaped like the broker's, not like a convenience.
# One simplification, stated rather than hidden (Codex round 11, P3): the synonym NAME for
# retention.ms and segment.ms is rendered as log.retention.ms / log.roll.ms, while a 4.3.0 broker
# derives both from its hours defaults (log.retention.hours, log.roll.hours) and reports those -- the
# ms properties themselves default to null. The effective VALUES below are the broker's, and no
# barrier in this repo compares a synonym name for these two keys; the key whose synonym name is
# load-bearing is min.compaction.lag.ms, whose log.cleaner.min.compaction.lag.ms is exact and is the
# trap the gamma-zone guard is tested against.
ALL_DEFAULTS = {
    "cleanup.policy": ("delete", "log.cleanup.policy"),
    "retention.ms": ("604800000", "log.retention.ms"),
    "retention.bytes": ("-1", "log.retention.bytes"),
    "max.message.bytes": ("1048588", "message.max.bytes"),
    "min.insync.replicas": ("1", "min.insync.replicas"),
    "min.compaction.lag.ms": ("0", "log.cleaner.min.compaction.lag.ms"),
    "max.compaction.lag.ms": ("9223372036854775807", "log.cleaner.max.compaction.lag.ms"),
    "min.cleanable.dirty.ratio": ("0.5", "log.cleaner.min.cleanable.ratio"),
    "delete.retention.ms": ("86400000", "log.cleaner.delete.retention.ms"),
    "segment.ms": ("604800000", "log.roll.ms"),
    "segment.bytes": ("1073741824", "log.segment.bytes"),
}

if name == "kafka-configs":
    t = arg("--entity-name")
    if "--describe" in argv and "--all" in argv:
        cfg = topics.get(t, {}).get("configs", {})
        # FAKE_KAFKA_DESCRIBE_OMITS=<key[,key...]>: leave those keys out of the answer entirely.
        #
        # This exists because the realism above took something away. While `--all` printed only the
        # keys a topic had SET, a test could model "the describe output does not carry this key" by
        # simply not setting it -- and that is how ensure-line-health-topics-test.sh reached its
        # guard for an unreadable lag. A real broker never answers that way: `--describe --all`
        # prints every key, an unset one as the broker DEFAULT. So with the realistic answer the
        # absent-key case became unreachable, the guard that refuses it stopped being exercised,
        # and the test that named it was asserting against a run that now succeeds. A truncated or
        # garbled answer is still a thing that happens, so it needs its own knob rather than a
        # fixture that no broker would produce.
        omit = {k for k in os.environ.get("FAKE_KAFKA_DESCRIBE_OMITS", "").split(",") if k}
        print(f"All configs for topic {t} are:")
        for k in sorted((set(ALL_DEFAULTS) | set(cfg)) - omit):
            d, syn = ALL_DEFAULTS.get(k, (None, None))
            if k in cfg:
                # A topic-level override does NOT hide the broker default: the real broker lists
                # BOTH, the override first. Recorded from the live dev broker 2026-11-17 for the
                # very key the gamma-zone guard reads:
                #   min.compaction.lag.ms=604800000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:
                #   min.compaction.lag.ms=604800000, DEFAULT_CONFIG:log.cleaner.min.compaction.lag.ms=0}
                # Emitting only the DYNAMIC synonym made the trap vanish exactly when a test
                # configured the value it was testing — so the synonym regression test passed
                # without the synonym ever reaching the parser (Codex round 1, P2).
                syns = [f"DYNAMIC_TOPIC_CONFIG:{k}={cfg[k]}"]
                if syn is not None:
                    syns.append(f"DEFAULT_CONFIG:{syn}={d}")
                print(f"  {k}={cfg[k]} sensitive=false synonyms={{{', '.join(syns)}}}")
            else:
                print(f"  {k}={d} sensitive=false synonyms={{DEFAULT_CONFIG:{syn}={d}}}")
    elif "--describe" in argv:
        cfg = topics.get(t, {}).get("configs", {})
        print(f"Dynamic configs for topic {t} are:")
        print("  " + " ".join(f"{k}={v}" for k, v in cfg.items()))
    elif "--alter" in argv:
        add = arg("--add-config")
        if os.environ.get("FAKE_KAFKA_TOPIC_MAXBYTES_ALTER_FAILS") and "max.message.bytes" in add:
            print("topic alter refused", file=sys.stderr)
            sys.exit(1)
        # An ACCEPTED alter is not an APPLIED one: the CLI exits 0 and the broker's effective value
        # does not move. Barriers that set a config and then read it back exist for exactly this,
        # and without a way to produce it the read-back can only ever be tested as a tautology.
        if os.environ.get("FAKE_KAFKA_TOPIC_ALTER_IS_A_NO_OP"):
            sys.exit(0)
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
