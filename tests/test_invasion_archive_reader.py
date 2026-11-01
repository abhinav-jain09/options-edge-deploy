"""scripts/ops/archive/StrikeInvasionEventsArchiveReader.java decodes the Confluent-framed Avro records of
options.spx.strike-invasion.events (TWELVE record types) into the JSON-era archive line (the retired writer's
{seq,ts,type,strike,spot,direction,data}), a tombstone into the text null, and fails loud (exit 2) on anything that is
not one of the records. Records are generated through the real serde for every event type (tests/fixtures/invasion-archive
/EncodeFixture.java), together with an expected line assembled by hand, independently of the contracts renderer.

Needs a JDK 21+ and the reader's classpath (Kafka clients, Avro, Confluent serializer/registry client, Jackson, the
options-edge-contracts jar with StrikeInvasionAvro): OE_INVASION_READER_CP, else OE_INDICATOR_READER_CP (the archive
deploy builds one classpath for all readers). A machine without it FAILS this test; the release boundary is never skipped."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
READER = ROOT / "scripts/ops/archive/StrikeInvasionEventsArchiveReader.java"
FIX = ROOT / "tests/fixtures/invasion-archive/EncodeFixture.java"
JAVA = os.environ.get("OE_INVASION_READER_JAVA", os.environ.get("OE_INDICATOR_READER_JAVA", "java"))
TOPIC = "options.spx.strike-invasion.events"
SCOPE = "invasion-archive-reader-test"
TYPES = ["NON_FINITE_SPOT_DROPPED", "OUT_OF_ORDER_SPOT_DROPPED", "NON_FINITE_STRIKE_DROPPED", "SESSION_RESET",
         "CANDIDATE_INVALIDATED", "STAGE1_RETIRED", "STATE", "STAGE1", "INVASION", "STAGE2_TIMEOUT", "DEFENSE_BROKEN",
         "STAGE2_VERDICT"]


def reader_classpath() -> str:
    cp = os.environ.get("OE_INVASION_READER_CP") or os.environ.get("OE_INDICATOR_READER_CP")
    if not cp:
        raise AssertionError("OE_INVASION_READER_CP (or OE_INDICATOR_READER_CP) from the guarded latest-main contracts build is required")
    return cp


class InvasionArchiveReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cp = reader_classpath()
        cls.tmp = Path(tempfile.mkdtemp())
        cls.foreign_avsc = cls.tmp / "other.avsc"
        cls.foreign_avsc.write_text('{"type":"record","name":"Other","namespace":"x","fields":[{"name":"a","type":"long"}]}')
        enc = subprocess.run([JAVA, "-cp", cls.cp, str(FIX), SCOPE, TOPIC, str(cls.foreign_avsc)], capture_output=True, text=True)
        assert enc.returncode == 0, enc.stderr[-2000:]
        rows = [l.split("\t") for l in enc.stdout.splitlines()]
        cls.foreign = next(r for r in rows if r[0] == "FOREIGN")
        cls.events = [r for r in rows if r[0].startswith("SPX:")]
        assert len(cls.events) == 12, enc.stdout[-1500:]

    def run_reader(self, file_lines, *extra):
        f = self.tmp / "in.tsv"
        f.write_text("\n".join(file_lines) + "\n")
        return subprocess.run([JAVA, "-cp", self.cp, str(READER), "--registry", "mock://" + SCOPE, "--topic", TOPIC,
                               "--partition", "0", "--from", "41", "--mock-extra-schema", str(self.foreign_avsc), *extra,
                               "--decode-file", str(f)], capture_output=True, text=True)

    def test_every_event_type_is_its_json_era_line(self):
        dec = self.run_reader([f"{r[0]}\t{r[1]}" for r in self.events])
        self.assertEqual(dec.returncode, 0, dec.stderr[-2000:])
        out = dec.stdout.rstrip("\n").split("\n")
        self.assertEqual(len(out), 12)
        for r, line in zip(self.events, out):
            ts, part, key, value = line.split("\t")
            self.assertEqual((ts, part, key), ("CreateTime:0", "Partition:0", r[0]))
            self.assertEqual(value, r[2], r[0])
            parsed = json.loads(value)
            self.assertEqual(list(parsed)[:7], ["seq", "ts", "type", "strike", "spot", "direction", "data"])
        self.assertEqual([json.loads(l.split("\t")[3])["type"] for l in out], TYPES)

    def test_an_absent_optional_payload_key_is_left_out_like_the_json_writer(self):
        dec = self.run_reader([f"{r[0]}\t{r[1]}" for r in self.events])
        v = {json.loads(l.split("\t")[3])["type"]: json.loads(l.split("\t")[3]) for l in dec.stdout.splitlines()}
        self.assertNotIn("grade", v["STAGE2_VERDICT"]["data"])   # the 12th event (even seq) carries none of its optional keys
        self.assertIn("outcome", v["STAGE2_VERDICT"]["data"])

    def test_print_offset_adds_only_the_offset_column(self):
        dec = self.run_reader([f"{r[0]}\t{r[1]}" for r in self.events[:2]], "--print-offset")
        rows = [l.split("\t") for l in dec.stdout.rstrip("\n").split("\n")]
        self.assertEqual([r[2] for r in rows], ["Offset:41", "Offset:42"])
        self.assertEqual(len(rows[0]), 5)

    def test_a_tombstone_is_the_text_null(self):
        dec = self.run_reader([f"{self.events[0][0]}\t{self.events[0][1]}", "SPX:STATE\tnull"])
        self.assertEqual(dec.returncode, 0, dec.stderr[-500:])
        self.assertEqual(dec.stdout.rstrip("\n").split("\n")[1], "CreateTime:0\tPartition:0\tSPX:STATE\tnull")

    def test_a_value_that_is_not_an_event_record_fails_loud_and_publishes_nothing(self):
        json_value = self.events[0][2].encode().hex()   # the retired JSON line itself
        cases = {"json": "SPX:STATE\t" + json_value,
                 "garbage": "SPX:STATE\t" + "0000000001ff",
                 "foreign schema (registered and resolvable)": f"SPX:STATE\t{self.foreign[1]}"}
        for name, line in cases.items():
            dec = self.run_reader([line])
            self.assertEqual(dec.returncode, 2, f"{name}: {dec.stderr[-500:]}")
            self.assertEqual(dec.stdout, "", f"{name}: nothing is published for an undecodable range")
            self.assertIn("FAILED", dec.stderr)
        mixed = self.run_reader([f"{self.events[0][0]}\t{self.events[0][1]}", "SPX:STATE\t" + json_value])
        self.assertEqual(mixed.returncode, 2)
        self.assertEqual(mixed.stdout, "", "a failure part-way publishes nothing of the range")

    def test_usage_errors_exit_64(self):
        r = subprocess.run([JAVA, "-cp", self.cp, str(READER), "--topic", TOPIC], capture_output=True, text=True)
        self.assertEqual(r.returncode, 64)


if __name__ == "__main__":
    unittest.main()
