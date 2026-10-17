"""scripts/ops/archive/IndicatorArchiveReader.java decodes a Confluent-framed IndicatorBar into the owner's JSON line,
byte-identical to the JSON-era archive line of the same record; a value that is not the record fails loud.

Needs a JDK 21+ and the reader's classpath (Kafka clients, Avro, the Confluent serializer and registry client, and
the options-edge-contracts jar): set OE_INDICATOR_READER_CP to it (and OE_INDICATOR_READER_JAVA to the java binary),
an explicit classpath from the guarded latest-main checkout is required; a machine without it FAILS
this test (the archive's release boundary is never skipped)."""
from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
READER = ROOT / "scripts/ops/archive/IndicatorArchiveReader.java"
FIX = ROOT / "tests/fixtures/indicator-archive"
JAVA = os.environ.get("OE_INDICATOR_READER_JAVA", "java")


def reader_classpath() -> str:
    """Use the classpath from the exact guarded latest-main build, never guess a cached release."""
    cp = os.environ.get("OE_INDICATOR_READER_CP")
    if not cp:
        raise AssertionError("OE_INDICATOR_READER_CP from the guarded latest-main contracts checkout is required")
    return cp


CP = None


class IndicatorArchiveReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global CP
        CP = reader_classpath()

    def test_decoded_line_is_the_json_era_archive_line(self):
        scope = "indicator-archive-reader-test"
        enc = subprocess.run([JAVA, "-cp", CP, str(FIX / "EncodeFixture.java"), scope, "options.indicators.bars",
                              str(FIX / "observed-spx-1m-bar.json")], capture_output=True, text=True)
        self.assertEqual(enc.returncode, 0, enc.stderr[-2000:])
        lines = [l for l in enc.stdout.splitlines() if l.strip()]
        framed = [l for l in lines if not l.startswith("JSON\t")]
        expected_json = next(l for l in lines if l.startswith("JSON\t"))[len("JSON\t"):]
        f = Path(tempfile.mkdtemp()) / "framed.tsv"
        f.write_text("\n".join(framed) + "\n")
        dec = subprocess.run([JAVA, "-cp", CP, str(READER), "--registry", "mock://" + scope, "--topic", "options.indicators.bars",
                              "--partition", "3", "--from", "41", "--decode-file", str(f)], capture_output=True, text=True)
        self.assertEqual(dec.returncode, 0, dec.stderr[-2000:])
        out = dec.stdout.rstrip("\n").split("\n")
        self.assertEqual(len(out), 1)
        ts, part, key, json = out[0].split("\t")
        self.assertEqual(ts, "CreateTime:0")
        self.assertEqual(part, "Partition:3")
        self.assertEqual(key, "SPX|1m")
        self.assertEqual(json, expected_json, "the archive line is the owner's JSON of the record, as before the conversion")
        self.assertIn('"final":', json)
        self.assertNotIn('"isFinal"', json)
        # --print-offset adds the console consumer's Offset column, nothing else
        dec2 = subprocess.run([JAVA, "-cp", CP, str(READER), "--registry", "mock://" + scope, "--topic", "options.indicators.bars",
                               "--partition", "3", "--from", "41", "--print-offset", "--decode-file", str(f)], capture_output=True, text=True)
        self.assertEqual(dec2.stdout.rstrip("\n").split("\t")[2], "Offset:41")

    def test_a_value_that_is_not_the_record_fails_loud(self):
        f = Path(tempfile.mkdtemp()) / "framed.tsv"
        f.write_text("SPX|1m\t" + "7b2273796d626f6c223a22535058227d" + "\n")   # {"symbol":"SPX"} — JSON, no magic byte
        dec = subprocess.run([JAVA, "-cp", CP, str(READER), "--registry", "mock://indicator-archive-reader-test-2", "--topic",
                              "options.indicators.bars", "--decode-file", str(f)], capture_output=True, text=True)
        self.assertEqual(dec.returncode, 2, dec.stderr[-500:])
        self.assertEqual(dec.stdout, "", "nothing is published for an undecodable range")
        self.assertIn("FAILED", dec.stderr)


if __name__ == "__main__":
    unittest.main()
