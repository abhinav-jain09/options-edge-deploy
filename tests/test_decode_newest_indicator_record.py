"""Executes scripts/kafka/decode-newest-indicator-record.sh (the mirror install's decode gate) against stand-ins for
kafka-topics, kafka-get-offsets, kafka-console-consumer, kafka-avro-console-consumer and curl.

Why (2026-10-18): the gate failed every NON-empty partition and passed every empty one — `--formatter-property`
is unknown to Confluent 7.3.1's console consumer, and `end - 1` is a commit marker on a transactional topic. The
stand-in console consumer reproduces a transactional partition (records at even offsets, markers at odd ones,
read_committed semantics: markers never returned) and the Avro stand-in decodes the SAME bytes the raw stand-in
serves: a record whose payload is not valid Avro fails to deserialize, whatever its frame says."""
import json, os, pathlib, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/decode-newest-indicator-record.sh"

STUB = r'''#!/usr/bin/env python3
import json, os, sys
name = os.path.basename(sys.argv[0]); argv = sys.argv[1:]
sc = json.load(open(os.environ["DECODE_SCENARIO"]))
with open(os.environ["DECODE_CALLS"], "a") as f: f.write(name + " " + " ".join(argv) + "\n")
def arg(flag, d=None): return argv[argv.index(flag) + 1] if flag in argv else d
topic = arg("--topic"); parts = sc["partitions"]   # {"p": {"begin": b, "end": e, "records": {offset: {"frame": hex, "avro_ok": bool}}}}
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
    p = arg("--partition"); off = int(arg("--offset")); maxm = int(arg("--max-messages", "1")); part = parts[p]
    if off < part["begin"] or off > part["end"]:
        print("OffsetOutOfRangeException", file=sys.stderr); sys.exit(1)
    recs = sorted((int(k), v) for k, v in part["records"].items() if int(k) >= off)[:maxm]
    if not recs: sys.exit(0)   # idle timeout: nothing printed, exit 0 (measured behaviour)
    if name == "kafka-console-consumer":
        if "print.offset=true" in argv:
            for o, _ in recs[:sc.get("lister_prints", 10**9)]: print(f"Offset:{o}")
            if sc.get("lister_exit"): sys.stdout.flush(); sys.exit(sc["lister_exit"])
        else:
            for o, r in recs: sys.stdout.buffer.write(bytes.fromhex(r["frame"]) + b"payload\n")
    else:
        o, r = recs[0]
        if not (r["frame"].startswith("00") and r["avro_ok"]): sys.exit(1)   # the same bytes must be valid Avro
        print(json.dumps({"symbol": "ES", "offset": o}))
    sys.exit(0)
if name == "curl":
    url = argv[-1]; code = sc.get("registry", {}).get(url.rsplit("/", 1)[-1], "404")
    sys.stdout.write(code); sys.exit(0)
sys.exit(2)
'''


def frame(schema_id):
    return "00" + format(schema_id, "08x")


def avro(schema_id=2001):
    return {"frame": frame(schema_id), "avro_ok": True}


def tx_partition(begin, end, schema_id=2001):
    """A transactional partition: records at even offsets, markers at odd ones, end offset after a marker."""
    return {"begin": begin, "end": end, "records": {str(o): avro(schema_id) for o in range(begin, end, 2)}}


def run(partitions, registry=None, empty_ok=None, **scenario_extra):
    d = pathlib.Path(tempfile.mkdtemp(prefix="decode-gate-"))
    kbin = d / "kbin"; kbin.mkdir()
    for n in ("kafka-topics", "kafka-get-offsets", "kafka-console-consumer", "kafka-avro-console-consumer", "curl"):
        (kbin / n).write_text(STUB); os.chmod(kbin / n, 0o755)
    (d / "scenario.json").write_text(json.dumps({"partitions": partitions, "registry": registry or {"2001": "200", "2004": "200"}, **scenario_extra}))
    calls = d / "calls.log"; calls.write_text("")
    env = dict(os.environ, PATH=f"{kbin}:{os.environ['PATH']}", DECODE_SCENARIO=str(d / "scenario.json"), DECODE_CALLS=str(calls))
    if empty_ok is not None:
        env["DECODE_EMPTY_OK"] = empty_ok
    r = subprocess.run(["bash", str(SCRIPT), "broker:9092", "http://registry:8082", "es.options.indicators.snapshot.current", str(kbin)],
                       capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text()


class DecodeGateTest(unittest.TestCase):
    def test_a_transactional_partition_whose_last_offset_is_a_marker_decodes_its_newest_record(self):
        # 829 records + 829 markers (end 1658): the newest committed record is at offset 1656
        rc, out, calls = run({"0": tx_partition(0, 1658)})
        self.assertEqual(rc, 0, out)
        self.assertIn("offset 1656 (newest committed): Avro, schema id 2001 resolves", out)
        self.assertIn("--offset 1594 --max-messages 64", calls, "the window starts 64 offsets before the end")
        self.assertIn("--offset 1656 --max-messages 1", calls, "the newest record is read on its own")
        self.assertNotIn("--formatter-property", calls)

    def test_the_newest_record_is_validated_not_an_older_one(self):
        # older records are Avro, the newest is JSON: the gate must fail on the newest
        part = tx_partition(0, 20)
        part["records"]["18"] = {"frame": "7b2273796d", "avro_ok": False}   # '{"sym…'
        rc, out, _ = run({"0": part})
        self.assertEqual(rc, 1, out)
        self.assertIn("offset 18: no Confluent magic byte", out)

    def test_a_batched_transaction_picks_the_last_record_before_the_marker(self):
        # three records per transaction: offsets 0,1,2 then marker 3; 4,5,6 then marker 7 → newest is 6
        recs = {str(o): avro() for o in (0, 1, 2, 4, 5, 6)}
        rc, out, _ = run({"0": {"begin": 0, "end": 8, "records": recs}})
        self.assertEqual(rc, 0, out)
        self.assertIn("offset 6 (newest committed)", out)

    def test_a_framed_record_whose_payload_is_not_avro_fails_to_deserialize(self):
        part = tx_partition(0, 10)
        part["records"]["8"] = {"frame": frame(2001), "avro_ok": False}
        rc, out, _ = run({"0": part})
        self.assertEqual(rc, 1, out)
        self.assertIn("offset 8: the value does not deserialize", out)

    def test_a_lister_that_prints_an_older_offset_then_fails_does_not_certify_that_record(self):
        rc, out, _ = run({"0": tx_partition(0, 20)}, lister_prints=3, lister_exit=1)
        self.assertEqual(rc, 1, out)
        self.assertIn("listing the committed records of [0, 20) failed (consumer exit status 1)", out)
        self.assertNotIn("(newest committed)", out)

    def test_a_short_partition_starts_at_its_earliest_offset(self):
        rc, out, calls = run({"0": tx_partition(100, 120)})
        self.assertEqual(rc, 0, out)
        self.assertIn("--offset 100 --max-messages 64", calls)

    def test_an_empty_partition_fails_unless_the_caller_declares_its_source_empty(self):
        rc, out, calls = run({"0": {"begin": 0, "end": 0, "records": {}}})
        self.assertEqual(rc, 1, out)
        self.assertIn("is empty but the caller did not declare its source empty", out)
        rc, out, calls = run({"0": {"begin": 0, "end": 0, "records": {}}}, empty_ok="0")
        self.assertEqual(rc, 0, out)
        self.assertIn("-0: empty, and its source holds no committed record", out)
        self.assertNotIn("kafka-console-consumer", calls)

    def test_eight_partitions_each_gated_and_one_bad_magic_byte_fails(self):
        parts = {str(p): tx_partition(0, 20 + 2 * p) for p in range(8)}
        rc, out, _ = run(parts)
        self.assertEqual(rc, 0, out)
        self.assertEqual(out.count("(newest committed)"), 8)
        parts["5"]["records"] = {"0": {"frame": "7b2273796d", "avro_ok": False}}
        rc, out, _ = run(parts)
        self.assertEqual(rc, 1, out)
        self.assertIn("snapshot.current-5 offset 0: no Confluent magic byte", out)

    def test_a_foreign_schema_id_fails(self):
        rc, out, _ = run({"0": tx_partition(0, 10, schema_id=61)})
        self.assertEqual(rc, 1, out)
        self.assertIn("not one of the family's fixed ids", out)

    def test_an_id_the_target_registry_lacks_fails(self):
        rc, out, _ = run({"0": tx_partition(0, 10)}, registry={"2001": "404"})
        self.assertEqual(rc, 1, out)
        self.assertIn("does not resolve", out)

    def test_no_record_within_the_window_fails_loud(self):
        # a non-empty offset range that holds only markers: a consumer that lists nothing must not pass the gate
        rc, out, _ = run({"0": {"begin": 0, "end": 10, "records": {}}})
        self.assertEqual(rc, 1, out)
        self.assertIn("no committed record read in [0, 10)", out)


if __name__ == "__main__":
    unittest.main()
