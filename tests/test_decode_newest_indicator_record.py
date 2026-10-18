"""Executes scripts/kafka/decode-newest-indicator-record.sh (the mirror install's decode gate) against stand-ins for
kafka-topics, kafka-get-offsets, kafka-console-consumer, kafka-avro-console-consumer and curl.

Why (2026-10-18): the gate failed every NON-empty partition and passed every empty one — `--formatter-property`
is unknown to Confluent 7.3.1's console consumer, and `end - 1` is a commit marker on a transactional topic. The
stand-in console consumer reproduces a transactional partition: records at even offsets, markers at odd ones,
and read_committed semantics (markers never returned)."""
import json, os, pathlib, stat, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/decode-newest-indicator-record.sh"

STUB = r'''#!/usr/bin/env python3
import json, os, sys
name = os.path.basename(sys.argv[0]); argv = sys.argv[1:]
sc = json.load(open(os.environ["DECODE_SCENARIO"]))
with open(os.environ["DECODE_CALLS"], "a") as f: f.write(name + " " + " ".join(argv) + "\n")
def arg(flag, d=None): return argv[argv.index(flag) + 1] if flag in argv else d
topic = arg("--topic"); parts = sc["partitions"]   # {"p": {"begin": b, "end": e, "records": {offset: hexframe}}}
if name == "kafka-topics":
    print(f"Topic: {topic}\tTopicId: x\tPartitionCount: {len(parts)}\tReplicationFactor: 1\tConfigs: "); sys.exit(0)
if name == "kafka-get-offsets":
    key = "begin" if arg("--time") == "-2" else "end"
    for p in sorted(parts, key=int): print(f"{topic}:{p}:{parts[p][key]}")
    sys.exit(0)
if name in ("kafka-console-consumer", "kafka-avro-console-consumer"):
    if arg("--isolation-level") != "read_committed" or "auto.offset.reset=none" not in argv:
        print("stand-in: read_committed and auto.offset.reset=none required", file=sys.stderr); sys.exit(3)
    if "--formatter-property" in argv:
        print("formatter-property is not a recognized option", file=sys.stderr); sys.exit(1)
    p = arg("--partition"); off = int(arg("--offset")); part = parts[p]
    if off < part["begin"] or off > part["end"]:
        print("OffsetOutOfRangeException", file=sys.stderr); sys.exit(1)
    recs = sorted((int(k), v) for k, v in part["records"].items() if int(k) >= off)
    if not recs: sys.exit(0)   # idle timeout: nothing printed, exit 0 (measured behaviour)
    o, frame = recs[0]
    if name == "kafka-console-consumer":
        sys.stdout.buffer.write(bytes.fromhex(frame) + b"payload\n")
    else:
        if sc.get("avro_fails"): sys.exit(1)
        print(json.dumps({"symbol": "ES", "offset": o}))
    sys.exit(0)
if name == "curl":
    url = argv[-1]; code = sc.get("registry", {}).get(url.rsplit("/", 1)[-1], "404")
    sys.stdout.write(code); sys.exit(0)
sys.exit(2)
'''


def frame(schema_id):
    return "00" + format(schema_id, "08x")


def tx_partition(begin, end, schema_id=2001):
    """A transactional partition: records at even offsets, markers at odd ones, end offset after a marker."""
    return {"begin": begin, "end": end, "records": {str(o): frame(schema_id) for o in range(begin, end, 2)}}


def run(partitions, registry=None, avro_fails=False):
    d = pathlib.Path(tempfile.mkdtemp(prefix="decode-gate-"))
    kbin = d / "kbin"; kbin.mkdir()
    for n in ("kafka-topics", "kafka-get-offsets", "kafka-console-consumer", "kafka-avro-console-consumer", "curl"):
        (kbin / n).write_text(STUB); os.chmod(kbin / n, 0o755)
    (d / "scenario.json").write_text(json.dumps({"partitions": partitions, "registry": registry or {"2001": "200", "2004": "200"}, "avro_fails": avro_fails}))
    calls = d / "calls.log"; calls.write_text("")
    env = dict(os.environ, PATH=f"{kbin}:{os.environ['PATH']}", DECODE_SCENARIO=str(d / "scenario.json"), DECODE_CALLS=str(calls))
    r = subprocess.run(["bash", str(SCRIPT), "broker:9092", "http://registry:8082", "es.options.indicators.snapshot.current", str(kbin)],
                       capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text()


class DecodeGateTest(unittest.TestCase):
    def test_a_transactional_partition_whose_last_offset_is_a_marker_decodes(self):
        rc, out, calls = run({"0": tx_partition(0, 1658)})
        self.assertEqual(rc, 0, out)
        self.assertIn("schema id 2001 resolves", out)
        self.assertIn("--offset 1594", calls, "the read starts 64 offsets before the end")
        self.assertNotIn("--formatter-property", calls)

    def test_a_short_partition_starts_at_its_earliest_offset(self):
        rc, out, calls = run({"0": tx_partition(100, 120)})
        self.assertEqual(rc, 0, out)
        self.assertIn("--offset 100", calls)

    def test_an_empty_partition_is_reported_and_passes(self):
        rc, out, calls = run({"0": {"begin": 0, "end": 0, "records": {}}})
        self.assertEqual(rc, 0, out)
        self.assertIn("-0: empty", out)
        self.assertNotIn("kafka-console-consumer", calls)

    def test_eight_partitions_each_gated_and_one_bad_magic_byte_fails(self):
        parts = {str(p): tx_partition(0, 20 + 2 * p) for p in range(8)}
        rc, out, _ = run(parts)
        self.assertEqual(rc, 0, out)
        self.assertEqual(out.count("resolves"), 8)
        parts["5"]["records"]["0"] = "7b2273796d"   # JSON '{"sym' where the frame should be
        parts["5"]["records"] = {"0": "7b2273796d"}
        rc, out, _ = run(parts)
        self.assertEqual(rc, 1, out)
        self.assertIn("snapshot.current-5", out)
        self.assertIn("no Confluent magic byte", out)

    def test_a_foreign_schema_id_fails(self):
        rc, out, _ = run({"0": tx_partition(0, 10, schema_id=61)})
        self.assertEqual(rc, 1, out)
        self.assertIn("not one of the family's fixed ids", out)

    def test_an_id_the_target_registry_lacks_fails(self):
        rc, out, _ = run({"0": tx_partition(0, 10)}, registry={"2001": "404"})
        self.assertEqual(rc, 1, out)
        self.assertIn("does not resolve", out)

    def test_a_value_that_does_not_deserialize_fails(self):
        rc, out, _ = run({"0": tx_partition(0, 10)}, avro_fails=True)
        self.assertEqual(rc, 1, out)
        self.assertIn("does not deserialize", out)

    def test_no_record_within_the_window_fails_loud(self):
        # only markers in the window: a consumer that reads nothing must not pass the gate
        rc, out, _ = run({"0": {"begin": 0, "end": 10, "records": {}}})
        self.assertEqual(rc, 1, out)
        self.assertIn("no committed record read", out)


if __name__ == "__main__":
    unittest.main()
