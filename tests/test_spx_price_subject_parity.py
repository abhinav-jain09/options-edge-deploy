"""scripts/kafka/spx-price-mirror/assert-subject-parity.sh against two stand-in registries: the mirror never registers, so the subject
must be on both, in the same shape, while the ids are free to differ."""
import json, os, subprocess, unittest
from pathlib import Path

from tests.stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/spx-price-mirror/assert-subject-parity.sh"
SCHEMA = (ROOT / "tests/fixtures/underlying-price/UnderlyingPrice.avsc").read_text()
SUBJECT = "com.optionsedge.contracts.hpsf.UnderlyingPrice"


def run(a: Registry, b: Registry):
    for r in (a, b):
        r.configs.setdefault(SUBJECT, "FULL")
    (s1, u1), (s2, u2) = serve(a), serve(b)
    try:
        r = subprocess.run(["bash", str(SCRIPT), u1, u2], capture_output=True, text=True)
    finally:
        s1.shutdown(); s2.shutdown()
    return r.returncode, r.stdout + r.stderr


class SubjectParity(unittest.TestCase):
    def test_same_shape_with_different_ids_passes_and_registers_nothing(self):
        a, b = Registry(), Registry()
        a.register(SUBJECT, SCHEMA, id=77)
        b.register(SUBJECT, SCHEMA, id=9)
        rc, out = run(a, b)
        self.assertEqual(rc, 0, out)
        self.assertIn("source id 77", out)
        self.assertIn("target id 9", out)
        self.assertFalse([c for c in b.calls if c.startswith(("POST", "PUT"))])

    def test_compatibility_must_be_full_on_both_registries(self):
        for which in (0, 1):
            a, b = Registry(), Registry()
            a.register(SUBJECT, SCHEMA, id=77); b.register(SUBJECT, SCHEMA, id=9)
            a.configs[SUBJECT] = "FULL"; b.configs[SUBJECT] = "FULL"
            [a, b][which].configs[SUBJECT] = "BACKWARD"
            rc, out = run(a, b)
            self.assertNotEqual(rc, 0, out)
            self.assertIn("FULL", out)

    def test_compatibility_not_configured_on_the_subject_fails(self):
        a, b = Registry(), Registry()
        a.register(SUBJECT, SCHEMA, id=77); b.register(SUBJECT, SCHEMA, id=9)
        a.configs[SUBJECT] = "FULL"   # b has no subject-level config (the global default is not enough)
        (s1, u1), (s2, u2) = serve(a), serve(b)
        try:
            r = subprocess.run(["bash", str(SCRIPT), u1, u2], capture_output=True, text=True)
        finally:
            s1.shutdown(); s2.shutdown()
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("FULL compatibility", r.stdout)

    def test_subject_absent_on_the_target_fails(self):
        a, b = Registry(), Registry()
        a.register(SUBJECT, SCHEMA, id=77)
        rc, out = run(a, b)
        self.assertNotEqual(rc, 0)
        self.assertIn("absent on target", out)

    def test_a_different_shape_fails(self):
        a, b = Registry(), Registry()
        a.register(SUBJECT, SCHEMA, id=77)
        other = json.loads(SCHEMA); other["fields"] = other["fields"][:-1]
        b.register(SUBJECT, json.dumps(other), id=9)
        rc, out = run(a, b)
        self.assertNotEqual(rc, 0)
        self.assertIn("differs", out)


STUB = r'''#!/usr/bin/env python3
import json, os, sys
name = os.path.basename(sys.argv[0]); argv = sys.argv[1:]
sc = json.load(open(os.environ["SC"]))
arg = lambda f: argv[argv.index(f) + 1] if f in argv else None
t = arg("--topic")
if name == "kafka-get-offsets":
    k = "begin" if arg("--time") == "-2" else "end"
    print(f"{t}:0:{sc[k]}")
else:
    off = int(arg("--offset"))
    sys.stdout.buffer.write(bytes.fromhex(sc["frames"][str(off)]) + b"x\\n")
'''


def retained(a, b, frames, begin=0, end=10):
    for r in (a, b):
        r.configs.setdefault(SUBJECT, "FULL")
    import tempfile
    d = Path(tempfile.mkdtemp()); (d / "sc.json").write_text(json.dumps({"begin": begin, "end": end, "frames": frames}))
    for n in ("kafka-get-offsets", "kafka-console-consumer"):
        (d / n).write_text(STUB); os.chmod(d / n, 0o755)
    (s1, u1), (s2, u2) = serve(a), serve(b)
    try:
        r = subprocess.run(["bash", str(SCRIPT), u1, u2, "b:9092", "underlying.spx.price", str(d)], capture_output=True, text=True, env=dict(os.environ, SC=str(d / "sc.json")))
    finally:
        s1.shutdown(); s2.shutdown()
    return r.returncode, r.stdout + r.stderr


class RetainedRecords(unittest.TestCase):
    def regs(self, extra_source_schema=None):
        a, b = Registry(), Registry()
        a.register(SUBJECT, SCHEMA, id=77)
        b.register(SUBJECT, SCHEMA, id=9)
        return a, b

    def test_earliest_and_newest_ids_present_on_the_target_pass(self):
        a, b = self.regs()
        rc, out = retained(a, b, {"0": "000000004d", "9": "000000004d"})
        self.assertEqual(rc, 0, out)
        self.assertIn("schema id 77: present", out)

    def test_an_older_version_on_the_source_missing_on_the_target_fails(self):
        a, b = Registry(), Registry()
        old = json.loads(SCHEMA); old["fields"] = old["fields"][:-1]
        a.register(SUBJECT, json.dumps(old), id=70)
        a.register(SUBJECT, SCHEMA, id=77)
        b.register(SUBJECT, SCHEMA, id=9)
        rc, out = retained(a, b, {"0": "0000000046", "9": "000000004d"})
        self.assertNotEqual(rc, 0)
        self.assertIn("schema id 70 has no byte-identical schema", out)

    def test_json_at_the_start_of_the_source_fails(self):
        a, b = self.regs()
        rc, out = retained(a, b, {"0": "7b227379", "9": "000000004d"})
        self.assertNotEqual(rc, 0)
        self.assertIn("not Confluent-framed", out)


if __name__ == "__main__":
    unittest.main()
