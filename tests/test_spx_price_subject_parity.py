"""scripts/kafka/spx-price-mirror/assert-subject-parity.sh against two stand-in registries: the mirror never registers, so the subject
must be on both, in the same shape, while the ids are free to differ."""
import json, os, subprocess, unittest
from pathlib import Path

from stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/spx-price-mirror/assert-subject-parity.sh"
SCHEMA = (ROOT / "tests/fixtures/underlying-price/UnderlyingPrice.avsc").read_text()
SUBJECT = "com.optionsedge.contracts.hpsf.UnderlyingPrice"


def run(a: Registry, b: Registry):
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


if __name__ == "__main__":
    unittest.main()
