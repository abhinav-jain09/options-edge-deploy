"""scripts/ops/archive/SpotArchiveReader.java decodes a Confluent-framed underlying.spx.price UnderlyingPrice into the
JSON-era archive line (key order, CPython repr(float) numbers, size null), a tombstone into the text null, and fails
loud (exit 2) on anything that is not the record. Prices are generated through the real serializer
(tests/fixtures/spot-archive/EncodeFixture.java); the expected text comes from CPython itself: json.dumps of the
same dict, whose floats are float.__repr__.

Needs a JDK 21+ and the reader's classpath (Kafka clients, Avro, Confluent serializer/registry client, Jackson, the
options-edge-contracts jar with UnderlyingPriceAvro): OE_SPOT_READER_CP, else OE_INDICATOR_READER_CP (the archive deploy
builds one classpath for both). A machine without it FAILS this test; the release boundary is never skipped."""
from __future__ import annotations

import json
import os
import random
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
READER = ROOT / "scripts/ops/archive/SpotArchiveReader.java"
FIX = ROOT / "tests/fixtures/spot-archive/EncodeFixture.java"
RANDOM_N = 3000
JAVA = os.environ.get("OE_SPOT_READER_JAVA", os.environ.get("OE_INDICATOR_READER_JAVA", "java"))
TOPIC = "underlying.spx.price"

# The gateway's SpxPriceJsonTest golden bit patterns (price within decimal(38,18)), covering the plain/scientific edges.
GOLDEN = [
    0x40bca70000000000, 0x40b77c8000000000, 0x40bd4bdf97edc7ef, 0x3fd3333333333334, 0x3ee4f8b588e368f1,
    0x3f1a36e2eb1c432d, 0x3f1a36371ea531a8, 0x4132d68700000000, 0x416312cfe0000000, 0x416312d000000000,
    0x41678c29dccccccd, 0x430c6bf526340000, 0x4341c37937e07fff, 0x4341c37937e08000, 0x434aa535d3d0c000,
    0x40fe240c9fcb689f, 0x40bb580000000001, 0x40bb57ffffffffff, 0x4341c37937e08001, 0x40b388028f5c28f6,
    0x40b387ffffffffff, 0x40b98f35c28f5c29, 0x3f50624dd2f1a9fc, 0x3f202c9dedbc309d, 0x42dc12218377de66,
    0x43e158e460913d00, 0x43e0000000000000, 0x3c570ef54646d497, 0x3c670ef54646d497, 0x437b69b4ba630f35,
    0x4011666666666666, 0x3fd3333333333333, 0x4005666666666666, 0x3ff0000000000001, 0xc3e158e460913d00,
    0xbee4f8b588e368f1, 0xbfd3333333333334, 0x43a757aa16a03500, 0x43a757aa16a03501, 0x43a757aa16a034ff,
    0x43abc16d674ec800, 0x40953692db3e40c5, 0x43d695c5011fad06, 0x40845f6b8916f673, 0x40a9b666da99389b,
    0x40b1d6ec0e2c647e, 0x4394de21ad97a616, 0x408983581fc9dbd4, 0x40bd11ab56ee1320, 0x409f649b17dd5e04,
]
# literal anchors, so the oracle itself is pinned to what the gateway's golden table states
ANCHORS = {0x40bca70000000000: "7335.0", 0x3ee4f8b588e368f1: "1e-05", 0x4341c37937e08000: "1e+16",
           0x434aa535d3d0c000: "1.5e+16", 0x3fd3333333333334: "0.30000000000000004", 0x416312d000000000: "10000000.0",
           0x43e0000000000000: "9.223372036854776e+18", 0x4341c37937e07fff: "9999999999999998.0"}


def reader_classpath() -> str:
    cp = os.environ.get("OE_SPOT_READER_CP") or os.environ.get("OE_INDICATOR_READER_CP")
    if not cp:
        raise AssertionError("OE_SPOT_READER_CP (or OE_INDICATOR_READER_CP) from the guarded latest-main contracts build is required")
    return cp


def expected_line(bits: int, seq: int) -> str:
    p = struct.unpack(">d", struct.pack(">Q", bits))[0]
    return json.dumps({"schemaVersion": 1, "symbol": "SPX", "eventTime": "2026-10-19T14:30:00.123456Z", "price": p,
                       "size": None, "source": "NATIVE_SPX_INDEX", "quality": "LIVE_INDEX", "canonicalSpot": p,
                       "spotEventTimeNs": 1792420200123456000, "feedEpochId": "epoch-7", "epochSeq": seq,
                       "spotQuality": "LIVE_INDEX"}, separators=(",", ":"))


class SpotArchiveReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cp = reader_classpath()
        cls.tmp = Path(tempfile.mkdtemp())
        cls.foreign_avsc = cls.tmp / "other.avsc"
        cls.foreign_avsc.write_text('{"type":"record","name":"Other","namespace":"x","fields":[{"name":"a","type":"long"}]}')
        enc = subprocess.run([JAVA, "-cp", cls.cp, str(FIX), "spot-archive-reader-test", TOPIC, str(cls.foreign_avsc)]
                             + [format(b, "x") for b in GOLDEN + cls.random_bits()],
                             capture_output=True, text=True)
        assert enc.returncode == 0, enc.stderr[-2000:]
        cls.framed = [l for l in enc.stdout.splitlines() if l.startswith("SPX\t")]
        cls.foreign = next(l for l in enc.stdout.splitlines() if l.startswith("FOREIGN\t"))
        assert len(cls.framed) == len(GOLDEN) + RANDOM_N

    @staticmethod
    def random_bits():
        """RANDOM_N seeded doubles inside decimal(38,18): |x| < 1e20, at most 18 fractional digits (so the decimal is exact)."""
        rng = random.Random(20261019)
        out = []
        while len(out) < RANDOM_N:
            digits = rng.randint(0, 18)
            x = round(rng.uniform(-1, 1) * 10 ** rng.randint(-6, 19), digits)
            if x != 0 and abs(x) < 1e20 and len(repr(abs(x)).split(".")[-1]) <= 18 and "e" not in repr(x):
                out.append(struct.unpack(">Q", struct.pack(">d", x))[0])
        return out

    def run_reader(self, file_lines, *extra):
        f = self.tmp / "in.tsv"
        f.write_text("\n".join(file_lines) + "\n")
        return subprocess.run([JAVA, "-cp", self.cp, str(READER), "--registry", "mock://spot-archive-reader-test", "--topic", TOPIC,
                               "--partition", "3", "--from", "41", "--mock-extra-schema", str(self.foreign_avsc), *extra, "--decode-file", str(f)], capture_output=True, text=True)

    def test_every_golden_price_is_the_json_era_line(self):
        dec = self.run_reader(self.framed[:len(GOLDEN)])
        self.assertEqual(dec.returncode, 0, dec.stderr[-2000:])
        out = dec.stdout.rstrip("\n").split("\n")
        self.assertEqual(len(out), len(GOLDEN))
        for i, (bits, line) in enumerate(zip(GOLDEN, out), 1):
            ts, part, key, value = line.split("\t")
            self.assertEqual((ts, part, key), ("CreateTime:0", "Partition:3", "SPX"))
            self.assertEqual(value, expected_line(bits, i), hex(bits))
            if bits in ANCHORS:
                self.assertIn('"price":%s,' % ANCHORS[bits], value)
                self.assertIn('"canonicalSpot":%s,' % ANCHORS[bits], value)
        self.assertTrue(out[0].split("\t")[3].startswith('{"schemaVersion":1,"symbol":"SPX","eventTime":'))
        self.assertIn('"size":null,', out[0])

    def test_random_doubles_in_the_decimal_domain_equal_cpython_repr(self):
        bits = self.random_bits()
        dec = self.run_reader(self.framed[len(GOLDEN):])
        self.assertEqual(dec.returncode, 0, dec.stderr[-2000:])
        out = dec.stdout.rstrip("\n").split("\n")
        self.assertEqual(len(out), RANDOM_N)
        for i, (b, line) in enumerate(zip(bits, out), len(GOLDEN) + 1):
            self.assertEqual(line.split("\t")[3], expected_line(b, i), hex(b))

    def test_print_offset_adds_only_the_offset_column(self):
        dec = self.run_reader(self.framed[:2], "--print-offset")
        rows = [l.split("\t") for l in dec.stdout.rstrip("\n").split("\n")]
        self.assertEqual([r[2] for r in rows], ["Offset:41", "Offset:42"])
        self.assertEqual(len(rows[0]), 5)

    def test_a_tombstone_is_the_text_null(self):
        dec = self.run_reader([self.framed[0], "SPX\tnull"])
        self.assertEqual(dec.returncode, 0, dec.stderr[-500:])
        self.assertEqual(dec.stdout.rstrip("\n").split("\n")[1], "CreateTime:0\tPartition:3\tSPX\tnull")

    def test_a_value_that_is_not_the_record_fails_loud_and_publishes_nothing(self):
        json_value = '{"schemaVersion":1,"symbol":"SPX","price":7335.0}'.encode().hex()
        cases = {"json": "SPX\t" + json_value,
                 "garbage": "SPX\t" + "0000000001ff",              # framed with the contracts id, undecodable body
                 "foreign schema (registered and resolvable)": self.foreign}
        for name, line in cases.items():
            dec = self.run_reader([line])
            self.assertEqual(dec.returncode, 2, f"{name}: {dec.stderr[-500:]}")
            self.assertEqual(dec.stdout, "", f"{name}: nothing is published for an undecodable range")
            self.assertIn("FAILED", dec.stderr)
        mixed = self.run_reader([self.framed[0], "SPX\t" + json_value])
        self.assertEqual(mixed.returncode, 2, "one bad value fails the whole range")
        self.assertEqual(mixed.stdout, "", "all-or-nothing: the valid line before the bad one is not published either")


if __name__ == "__main__":
    unittest.main()
