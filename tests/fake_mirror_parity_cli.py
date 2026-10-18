#!/usr/bin/env python3
"""Scenario-driven stand-in for kafka-get-offsets and kafka-console-consumer, dispatched by the name it is invoked as,
for tests/test_indicator_mirror_parity_gate.py. The scenario (FAKE_PARITY_SCENARIO, JSON) describes one source topic
and one target topic:

  {"source": {"<p>": {"begin": 0, "end": 902, "records": [0, 2, 4, ...]}},   # committed data-record offsets
   "target": {"<p>": [449, 451]},            # target end offset per kafka-get-offsets call (last value sticks)
   "begin_sequence": {"<p>": [0, 50]},       # optional: earliest offset per --time -2 call (retention advancing)
   "consumer": {"exit": 0, "error": null, "total_off_by": 0}}   # optional consumer misbehaviour

Call counters live in FAKE_PARITY_STATE so the sequences advance across invocations; every call is appended to
FAKE_PARITY_CALLS. The console-consumer stand-in reproduces the real tool's measured behaviour (2026-10-18): it
prints "Offset:<n>" per record to stdout, "Processed a total of N messages" to stderr, an idle timeout as
"[ERROR] ... TimeoutException" on stderr, and it EXITS 0 on every error unless the scenario says otherwise."""
import json, os, sys, time

argv = sys.argv[1:]
name = os.path.basename(sys.argv[0])
with open(os.environ["FAKE_PARITY_CALLS"], "a") as f:
    f.write(name + " " + " ".join(argv) + "\n")
scenario = json.load(open(os.environ["FAKE_PARITY_SCENARIO"]))
state_path = os.environ["FAKE_PARITY_STATE"]
state = json.load(open(state_path)) if os.path.exists(state_path) else {}


def arg(flag, default=None):
    return argv[argv.index(flag) + 1] if flag in argv else default


def nxt(key, seq):
    """Return seq[counter] and advance the counter; the last value sticks."""
    i = state.get(key, 0)
    state[key] = i + 1
    json.dump(state, open(state_path, "w"))
    return seq[min(i, len(seq) - 1)]


src = scenario["source"]
tgt = scenario["target"]
topic = arg("--topic")
bootstrap = arg("--bootstrap-server")
is_source = bootstrap == os.environ["FAKE_SRC"]

if name == "kafka-get-offsets":
    if not is_source and "target_raw" in scenario and "--partitions" not in argv:
        print("\n".join(scenario["target_raw"])); sys.exit(0)   # verbatim rows: lets a test feed partial or malformed answers
    parts = [arg("--partitions")] if "--partitions" in argv else sorted(src if is_source else tgt, key=int)
    for p in parts:
        if is_source:
            if arg("--time") == "-2":
                seq = scenario.get("begin_sequence", {}).get(p)
                val = nxt(f"begin:{p}", seq) if seq else src[p]["begin"]
            else:
                val = src[p]["end"]
        else:
            val = nxt(f"tend:{p}", tgt[p])
        print(f"{topic}:{p}:{val}")
    sys.exit(0)

if name == "kafka-console-consumer":
    c = scenario.get("consumer", {})
    if not is_source:
        print("[ERROR] stand-in: the gate must count on the SOURCE", file=sys.stderr); sys.exit(3)
    if arg("--isolation-level") != "read_committed" or "auto.offset.reset=none" not in argv:
        print("[ERROR] stand-in: read_committed and auto.offset.reset=none are required", file=sys.stderr); sys.exit(3)
    p = arg("--partition")
    offset = int(arg("--offset"))
    maxm = int(arg("--max-messages"))
    part = src[p]
    # the broker's TRUE earliest offset is part["begin"]; begin_sequence only shapes what --time -2 reports, so a
    # scenario can report a stale earliest offset the broker no longer holds
    if offset < part["begin"] or offset > part["end"]:
        print("[ERROR] Error processing message, terminating consumer process:  (kafka.tools.ConsoleConsumer$)", file=sys.stderr)
        print(f"org.apache.kafka.clients.consumer.OffsetOutOfRangeException: Fetch position FetchPosition{{offset={offset}}} is out of range for partition {topic}-{p}", file=sys.stderr)
        print("Processed a total of 0 messages", file=sys.stderr)
        sys.exit(c.get("exit", 0))
    out = [o for o in part["records"] if o >= offset][:maxm]
    for o in out:
        print(f"Offset:{o}")
    if c.get("error"):
        print("[ERROR] Error processing message, terminating consumer process:  (kafka.tools.ConsoleConsumer$)", file=sys.stderr)
        print(f"org.apache.kafka.common.errors.{c['error']}", file=sys.stderr)
    elif len(out) < maxm:
        print("[2026-10-18 05:09:11,239] ERROR Error processing message, terminating consumer process:  (kafka.tools.ConsoleConsumer$)", file=sys.stderr)
        print("org.apache.kafka.common.errors.TimeoutException", file=sys.stderr)
    if c.get("hang_s"):   # a live topic never idles: the consumer keeps running until the gate's wall-time bound kills it
        sys.stdout.flush(); time.sleep(c["hang_s"])
    print(f"Processed a total of {len(out) + c.get('total_off_by', 0)} messages", file=sys.stderr)
    sys.exit(c.get("exit", 0))

print(f"stand-in: unknown tool {name}", file=sys.stderr)
sys.exit(2)
