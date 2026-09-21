"""Tests for scripts/ops/vol-premium-open-reference-capture.py.

Every case here follows the Negative-Test Sufficiency Rule: it states the property, and a
companion case removes the thing the property names and shows the assertion goes red. The one
that matters most is the OPTION-MODEL exclusion - the index topic carries two different
quantities, and a capture that scored against both would report an offset for a series nobody
asked about, silently, forever.
"""
from __future__ import annotations

import datetime as dt
import gzip
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "vol-premium-open-reference-capture.py"
ET = ZoneInfo("America/New_York")

_spec = importlib.util.spec_from_file_location("orc", SCRIPT)
orc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(orc)

DAY = "2026-09-18"
OPEN = dt.datetime.combine(dt.date.fromisoformat(DAY), dt.time(9, 30), ET)


def _iso(offset_s: float) -> str:
    return (OPEN + dt.timedelta(seconds=offset_s)).astimezone(dt.timezone.utc).isoformat().replace(
        "+00:00", "Z")


def _write(root: Path, topic: str, rows: list) -> None:
    folder = root / topic / f"dt={DAY}"
    folder.mkdir(parents=True, exist_ok=True)
    with gzip.open(folder / f"{topic}.p0.0-1.jsonl.gz", "wt") as handle:
        for row in rows:
            handle.write(f"CreateTime:0\tPartition:0\tKEY\t{json.dumps(row)}\n")


def _index(offset_s: float, price: float, source="IBKR_INDEX", field="LAST", quality="LIVE"):
    return {"symbol": "SPX", "price": price, "source": source, "priceField": field,
            "quality": quality, "eventTime": _iso(offset_s)}


def _es(offset_s: float, equivalent: float, state="PROJECTED"):
    return {"symbol": "ES", "spxEquivalent": equivalent, "spxBasisState": state,
            "eventTime": _iso(offset_s)}


def _fixture(root: Path, *, index_rows=None, es_rows=None) -> None:
    """A session shaped like the real ones: the index silent across the bell, ES present."""
    if index_rows is None:
        index_rows = [_index(-302, 7650.0)] + [
            _index(300 + 10 * i, 7650.0 + i) for i in range(40)]
    if es_rows is None:
        es_rows = [_es(-25 + i, 7647.0) for i in range(20)] + [
            _es(300 + 10 * i, 7650.0 + i - 3.0) for i in range(40)]
    _write(root, orc.INDEX, index_rows)
    _write(root, orc.ES, es_rows)
    _write(root, orc.BASIS, [{"level": 69.7, "levelKind": "MEASURED"}])


class OpenReferenceCaptureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: None)      # nothing is removed; the tmp dir is left for inspection

    # --- the freeze, and the reference that survives it ---------------------------------------
    def test_the_freeze_is_measured_and_slot_zero_has_nothing(self) -> None:
        _fixture(self.tmp)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["indexTicksInWindow"], 0,
                         "the window must be empty; that is the whole reason this capture exists")
        self.assertAlmostEqual(got["freezeSeconds"], 602.0, places=0)
        self.assertEqual(got["esTicksInWindow"], 20)
        self.assertEqual(got["esBasisStatesInWindow"], ["PROJECTED"])
        self.assertTrue(got["accepted"])

    def test_an_admissible_index_tick_in_the_window_is_counted(self) -> None:
        """The companion to the case above: if the window were NOT empty, the count must say so.
        Without this, `indexTicksInWindow == 0` would also be produced by a reader that never
        looked."""
        rows = [_index(-302, 7650.0), _index(-10, 7651.0)] + [
            _index(300 + 10 * i, 7650.0 + i) for i in range(40)]
        _fixture(self.tmp, index_rows=rows)
        self.assertEqual(orc.capture(str(self.tmp), DAY)["indexTicksInWindow"], 1)

    # --- the offset, which is the question the >=55 sessions exist to answer -------------------
    def test_the_offset_is_measured_against_the_true_index_only(self) -> None:
        got = None
        _fixture(self.tmp)
        got = orc.capture(str(self.tmp), DAY)
        self.assertAlmostEqual(got["offsetPoints"], -3.0, places=3)
        self.assertGreater(got["offsetPairs"], 0)

    def test_option_model_rows_are_excluded_from_the_offset(self) -> None:
        """The index topic carries IBKR_OPTION_MODEL rows too - a DIFFERENT quantity (bug 368).
        Here they sit 20 points away; if the capture scored them the offset would move. This is
        the case that fails if the source filter is ever dropped."""
        rows = [_index(-302, 7650.0)]
        for i in range(40):
            rows.append(_index(300 + 10 * i, 7650.0 + i))
            # WITHIN the 2 s staleness limit of the ES row at 300+10i, deliberately: at +5 s the
            # staleness guard excluded them and this case passed with the source filter DELETED -
            # it was named for the filter and satisfied by something else entirely.
            rows.append(_index(301 + 10 * i, 7670.0 + i,
                               source="IBKR_OPTION_MODEL", field="OPTION_MODEL_UNDERLYING"))
        _fixture(self.tmp, index_rows=rows)
        self.assertAlmostEqual(orc.capture(str(self.tmp), DAY)["offsetPoints"], -3.0, places=3,
                               msg="option-model rows leaked into the offset")

    # --- a session that cannot answer the question is REJECTED, and says why -------------------
    def test_a_session_without_an_es_reference_is_not_accepted(self) -> None:
        _fixture(self.tmp, es_rows=[_es(300 + 10 * i, 7650.0 + i - 3.0) for i in range(40)])
        got = orc.capture(str(self.tmp), DAY)
        self.assertFalse(got["accepted"])
        self.assertEqual(got["rejectedBecause"], "no ES reference in the window")

    def test_a_session_whose_offset_cannot_be_measured_is_not_accepted(self) -> None:
        """ES present in the window, but nothing to score it against afterwards."""
        _fixture(self.tmp, index_rows=[_index(-302, 7650.0)],
                 es_rows=[_es(-25 + i, 7647.0) for i in range(20)])
        got = orc.capture(str(self.tmp), DAY)
        self.assertFalse(got["accepted"])
        self.assertEqual(got["rejectedBecause"], "offset not measurable")

    # --- the count the decision rests on cannot be inflated -------------------------------------
    def test_a_second_run_of_the_same_session_is_not_appended_twice(self) -> None:
        _fixture(self.tmp)
        out = self.tmp / "ledger.jsonl"
        for _ in range(2):
            subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                            "--archive-root", str(self.tmp), "--out", str(out)],
                           capture_output=True, text=True, check=True)
        self.assertEqual(len(out.read_text().strip().split("\n")), 1,
                         "a repeated capture would double the denominator of the >=55 count")

    def test_a_different_session_does_append(self) -> None:
        """The companion: the guard above must refuse a DUPLICATE, not every write."""
        _fixture(self.tmp)
        out = self.tmp / "ledger.jsonl"
        subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                        "--archive-root", str(self.tmp), "--out", str(out)], check=True,
                       capture_output=True)
        other = "2026-09-17"
        for topic in (orc.INDEX, orc.ES, orc.BASIS):
            src = self.tmp / topic / f"dt={DAY}"
            dst = self.tmp / topic / f"dt={other}"
            dst.mkdir(parents=True, exist_ok=True)
            for f in src.iterdir():
                (dst / f.name).write_bytes(f.read_bytes())
        subprocess.run([sys.executable, str(SCRIPT), "--session", other,
                        "--archive-root", str(self.tmp), "--out", str(out)], check=True,
                       capture_output=True)
        self.assertEqual(len(out.read_text().strip().split("\n")), 2)

    # --- refusals ------------------------------------------------------------------------------
    def test_a_bad_session_date_refuses_with_64(self) -> None:
        r = subprocess.run([sys.executable, str(SCRIPT), "--session", "18-09-2026",
                            "--archive-root", str(self.tmp)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 64)
        self.assertIn("not a session date", r.stderr)

    def test_a_missing_archive_root_refuses_with_66(self) -> None:
        r = subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                            "--archive-root", str(self.tmp / "nope")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 66)
        self.assertIn("no archive root", r.stderr)


if __name__ == "__main__":
    unittest.main()
