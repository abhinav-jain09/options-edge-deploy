"""scripts/kafka/spx-price-mirror/verify-target-record.sh against stand-ins for kafka-get-offsets, kafka-console-consumer,
kafka-avro-console-consumer and curl: the gate passes only for a Confluent-framed record whose id resolves to the canonical
subject ON THE TARGET REGISTRY and that decodes; JSON, an id the target registry maps to another subject, an undecodable
record and an empty topic each fail."""
import json, os, pathlib, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/spx-price-mirror/verify-target-record.sh"
SUBJECT = "com.optionsedge.contracts.hpsf.UnderlyingPrice"

STUB = r'''#!/usr/bin/env python3
import json, os, sys
name = os.path.basename(sys.argv[0]); argv = sys.argv[1:]
sc = json.load(open(os.environ["SC"]))
def arg(f, d=None): return argv[argv.index(f) + 1] if f in argv else d
topic = arg("--topic")
if name == "kafka-get-offsets":
    key = "begin" if arg("--time") == "-2" else "end"
    for p in sorted(sc["parts"], key=int): print(f"{topic}:{p}:{sc['parts'][p][key]}")
elif name == "kafka-console-consumer":
    r = sc["parts"][arg("--partition")]
    off = int(arg("--offset"))
    if off == r["begin"]: frame = r.get("first", r["frame"])
    elif off == r["end"] - 1: frame = r["frame"]
    else: sys.exit(0)
    sys.stdout.buffer.write(bytes.fromhex(frame) + b"payload\n")
elif name == "kafka-avro-console-consumer":
    if sc["decodes"]: print(json.dumps({"symbol": "SPX"}))
elif name == "curl":
    i = argv[-1].split("/")[-2]
    print(json.dumps([{"subject": sc["ids"][i], "version": 1}], separators=(",", ":")) if i in sc["ids"] else "{}")
'''


def run(parts, ids, decodes=True, allow_empty=False, check_first=False):
    d = pathlib.Path(tempfile.mkdtemp(prefix="verify-spx-"))
    kbin = d / "kbin"; kbin.mkdir()
    for n in ("kafka-get-offsets", "kafka-console-consumer", "kafka-avro-console-consumer", "curl"):
        (kbin / n).write_text(STUB); os.chmod(kbin / n, 0o755)
    (d / "sc.json").write_text(json.dumps({"parts": parts, "ids": ids, "decodes": decodes}))
    env = dict(os.environ, PATH=f"{kbin}:{os.environ['PATH']}", SC=str(d / "sc.json"), ALLOW_EMPTY="true" if allow_empty else "false", CHECK_FIRST="true" if check_first else "false")
    r = subprocess.run(["bash", str(SCRIPT), "b:9092", "http://reg:8081", "es.underlying.spx.price", str(kbin)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


def part(frame, begin=0, end=10, first=None):
    return {"begin": begin, "end": end, "frame": frame, **({"first": first} if first else {})}


class VerifyTargetRecord(unittest.TestCase):
    def test_framed_record_whose_id_resolves_on_the_target_passes(self):
        rc, out = run({"0": part("0000000009")}, {"9": SUBJECT})
        self.assertEqual(rc, 0, out)
        self.assertIn("Avro id 9 resolves", out)

    def test_empty_partitions_are_skipped_but_data_partitions_are_all_checked(self):
        rc, out = run({"0": part("0000000009", 5, 5), "1": part("7b22", 0, 3)}, {"9": SUBJECT})
        self.assertNotEqual(rc, 0, out)
        self.assertIn("not Confluent-framed", out)

    def test_json_fails(self):
        rc, out = run({"0": part("7b227379")}, {})
        self.assertNotEqual(rc, 0)
        self.assertIn("a JSON writer", out)

    def test_an_id_that_means_another_subject_on_the_target_fails(self):
        rc, out = run({"0": part("000000004d")}, {"77": "some.other.Record"})
        self.assertNotEqual(rc, 0)
        self.assertIn("does not resolve", out)

    def test_an_undecodable_record_fails(self):
        rc, out = run({"0": part("0000000009")}, {"9": SUBJECT}, decodes=False)
        self.assertNotEqual(rc, 0)
        self.assertIn("could not decode", out)

    def test_earliest_record_is_fully_decoded_not_just_byte_zero(self):
        # framed (byte 0 = 00) but its id means another subject on the registry, or it does not decode
        bad_id = {"0": part("0000000009", first="00000000aa")}
        rc, out = run(bad_id, {"9": SUBJECT, "170": "some.other.Record"}, check_first=True)
        self.assertNotEqual(rc, 0)
        self.assertIn("@0: schema id 170 does not resolve", out)

    def test_a_tombstone_newest_record_fails(self):
        rc, out = run({"0": part("6e756c6c")}, {})   # the console consumer prints a null value as the text null
        self.assertNotEqual(rc, 0)
        self.assertIn("tombstone", out)

    def test_retained_json_before_avro_is_caught_only_by_the_earliest_record_check(self):
        mixed = {"0": part("0000000009", first="7b22")}
        self.assertEqual(run(mixed, {"9": SUBJECT})[0], 0, "the newest-record check alone cannot see it")
        rc, out = run(mixed, {"9": SUBJECT}, check_first=True)
        self.assertNotEqual(rc, 0)
        self.assertIn("@0: not Confluent-framed", out)

    def test_an_empty_topic_fails_unless_the_caller_allows_it(self):
        self.assertNotEqual(run({"0": part("00", 4, 4)}, {})[0], 0)
        self.assertEqual(run({"0": part("00", 4, 4)}, {}, allow_empty=True)[0], 0)


if __name__ == "__main__":
    unittest.main()
