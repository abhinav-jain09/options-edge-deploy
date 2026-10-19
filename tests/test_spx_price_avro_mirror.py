"""scripts/kafka/spx-price-mirror/SpxPriceAvroMirror.java: the decode-and-re-encode step of the underlying.spx.price mirror,
run for real (java single-file launch, the Confluent 7.3.1 serializer jars) against two stand-in registries whose ids for the
canonical subject DIFFER. A byte copy would carry the source id into a registry where it means another schema (or none);
the mirror must emit the TARGET id with the identical payload, and must refuse JSON, unknown shapes and foreign records."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests.stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "scripts/kafka/spx-price-mirror/SpxPriceAvroMirror.java"
SCHEMA = (ROOT / "tests/fixtures/underlying-price/UnderlyingPrice.avsc").read_text()
SUBJECT = "com.optionsedge.contracts.hpsf.UnderlyingPrice"
CONFLUENT = Path(os.environ.get("CONFLUENT", "/Users/abhinav/development/confluent-7.3.1"))
CP = ":".join(str(CONFLUENT / "share/java" / d / "*") for d in ("kafka", "kafka-serde-tools", "confluent-common"))
HAVE_JAVA = shutil.which("java") is not None and (CONFLUENT / "share/java/kafka-serde-tools").is_dir()


_FIX = {}


def fixture(registry_url: str, schema_text: str = SCHEMA) -> bytes:
    """One value serialized by the PRODUCTION Confluent serializer (tests/java/SerializeFixture.java) against the given registry."""
    if "dir" not in _FIX:
        _FIX["dir"] = tempfile.mkdtemp(prefix="spx-fixture-")
        c = subprocess.run(["javac", "-proc:none", "-cp", CP, "-d", _FIX["dir"], str(ROOT / "tests/java/SerializeFixture.java")],
                           capture_output=True, text=True, timeout=120)
        assert c.returncode == 0, c.stderr
    sf = Path(_FIX["dir"]) / "schema.avsc"
    sf.write_text(schema_text)
    r = subprocess.run(["java", "-cp", f"{_FIX['dir']}:{CP}", "SerializeFixture", registry_url, str(sf)], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
    return bytes.fromhex(r.stdout.strip())


def reframe(value: bytes, schema_id: int) -> bytes:
    return b"\x00" + schema_id.to_bytes(4, "big") + value[5:]


def transcode(src_url: str, tgt_url: str, value: bytes):
    r = subprocess.run(["java", "-cp", CP, str(SOURCE), "transcode", src_url, tgt_url, value.hex()],
                       capture_output=True, text=True, timeout=120)
    return r.returncode, r.stdout.strip(), r.stderr


@unittest.skipUnless(HAVE_JAVA, "needs java and the Confluent 7.3.1 serializer jars (CONFLUENT=...)")
class SpxPriceAvroMirrorTest(unittest.TestCase):
    def setUp(self):
        self.src, self.tgt = Registry(), Registry()
        # the two registries hand out DIFFERENT ids for the same canonical schema
        self.src.register(SUBJECT, SCHEMA, id=77)
        self.tgt.register("unrelated-subject", json.dumps({"type": "string"}), id=3)
        self.tgt.register(SUBJECT, SCHEMA, id=9)
        (self.s1, self.src_url), (self.s2, self.tgt_url) = serve(self.src), serve(self.tgt)

    def tearDown(self):
        self.s1.shutdown()
        self.s2.shutdown()

    def test_the_target_id_replaces_the_source_id_and_the_payload_is_unchanged(self):
        value = fixture(self.src_url)
        self.assertEqual(value[:5], b"\x00\x00\x00\x00M")
        rc, out, err = transcode(self.src_url, self.tgt_url, value)
        self.assertEqual(rc, 0, err)
        self.assertEqual(bytes.fromhex(out), fixture(self.tgt_url), "identical to what the production serializer writes against the target")
        self.assertEqual(bytes.fromhex(out)[:5], b"\x00\x00\x00\x00\x09")
        self.assertEqual(bytes.fromhex(out)[5:], value[5:], "payload unchanged")

    def test_json_is_refused_not_copied(self):
        rc, out, err = transcode(self.src_url, self.tgt_url, b'{"symbol":"SPX"}')
        self.assertNotEqual(rc, 0)
        self.assertEqual(out, "")

    def test_a_frame_whose_id_the_source_registry_does_not_know_is_refused(self):
        rc, out, _ = transcode(self.src_url, self.tgt_url, reframe(fixture(self.src_url), 78))
        self.assertNotEqual(rc, 0)
        self.assertEqual(out, "")

    def test_a_schema_the_target_has_not_registered_is_refused_and_nothing_is_registered(self):
        bare = Registry()
        server, bare_url = serve(bare)
        try:
            rc, out, _ = transcode(self.src_url, bare_url, fixture(self.src_url))
        finally:
            server.shutdown()
        self.assertNotEqual(rc, 0)
        self.assertEqual(out, "")
        self.assertEqual(bare.subjects, {}, "the mirror never registers")

    def test_a_foreign_record_on_the_source_is_refused(self):
        other = json.dumps({"type": "record", "name": "Other", "namespace": "x", "fields": [{"name": "symbol", "type": "string"}]})
        self.src.register("x.Other", other, id=80)
        rc, out, _ = transcode(self.src_url, self.tgt_url, fixture(self.src_url, other))
        self.assertNotEqual(rc, 0)
        self.assertEqual(out, "")


@unittest.skipUnless(HAVE_JAVA, "needs java and the Confluent 7.3.1 serializer jars (CONFLUENT=...)")
class MirrorStepBehaviour(unittest.TestCase):
    """SpxPriceAvroMirror.step through kafka-clients MockConsumer/MockProducer (no broker): key bytes, headers, source timestamp,
    re-encoded value, send -> flush -> commit order, and the stops (tombstone, JSON, failed produce) that must not commit."""

    def test_step_preserves_key_headers_timestamp_and_commits_after_flush(self):
        src, tgt = Registry(), Registry()
        src.register(SUBJECT, SCHEMA, id=77)
        tgt.register("unrelated-subject", json.dumps({"type": "string"}), id=3)
        tgt.register(SUBJECT, SCHEMA, id=9)
        (s1, u1), (s2, u2) = serve(src), serve(tgt)
        try:
            with tempfile.TemporaryDirectory() as d:
                c = subprocess.run(["javac", "-proc:none", "-cp", CP, "-d", d, str(SOURCE), str(ROOT / "tests/java/SpxPriceAvroMirrorStepTest.java")],
                                   capture_output=True, text=True, timeout=120)
                self.assertEqual(c.returncode, 0, c.stderr)
                r = subprocess.run(["java", "-cp", f"{d}:{CP}", "SpxPriceAvroMirrorStepTest", u1, u2, fixture(u1).hex(), fixture(u2).hex()],
                                   capture_output=True, text=True, timeout=120)
                self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
                self.assertIn("OK", r.stdout)
        finally:
            s1.shutdown()
            s2.shutdown()


class MirrorSourceContract(unittest.TestCase):
    def test_the_mirror_never_registers(self):
        text = SOURCE.read_text()
        self.assertIn('"auto.register.schemas", false', text)
        self.assertIn('"use.latest.version", false', text)
        self.assertIn("RecordNameStrategy", text)
        self.assertLess(text.index("producer.flush()"), text.index("consumer.commitSync()"))


if __name__ == "__main__":
    unittest.main()
