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
            _index(300 + i, 7650.0 + i / 10.0) for i in range(orc.MIN_OFFSET_PAIRS + 50)]
    if es_rows is None:
        # fresh at the open (the last tick is 1 s before it), and enough post-open pairs to clear
        # the MIN_OFFSET_PAIRS floor
        es_rows = [_es(-20 + i, 7647.0) for i in range(20)] + [
            _es(300 + i, 7650.0 + i / 10.0 - 3.0) for i in range(orc.MIN_OFFSET_PAIRS + 50)]
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
        self.assertLessEqual(got["esReferenceAgeMsAtOpen"], orc.PAIR_MAX_AGE_S * 1000)
        self.assertEqual(got["esBasisStatesInWindow"], ["PROJECTED"])
        self.assertTrue(got["accepted"])

    def test_an_admissible_index_tick_in_the_window_is_counted(self) -> None:
        """The companion to the case above: if the window were NOT empty, the count must say so.
        Without this, `indexTicksInWindow == 0` would also be produced by a reader that never
        looked."""
        rows = [_index(-302, 7650.0), _index(-10, 7651.0)] + [
            _index(300 + i, 7650.0 + i / 10.0) for i in range(orc.MIN_OFFSET_PAIRS + 50)]
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
        for i in range(orc.MIN_OFFSET_PAIRS + 50):
            rows.append(_index(300 + i, 7650.0 + i / 10.0))
            # WITHIN the 2 s staleness limit of the ES row at 300+10i, deliberately: at +5 s the
            # staleness guard excluded them and this case passed with the source filter DELETED -
            # it was named for the filter and satisfied by something else entirely.
            rows.append(_index(300.5 + i, 7670.0 + i / 10.0,
                               source="IBKR_OPTION_MODEL", field="OPTION_MODEL_UNDERLYING"))
            # An IBKR_INDEX row that is NOT the LAST field. Without this the case could not tell a
            # deleted `priceField == "LAST"` predicate from a kept one: every bad row differed in
            # BOTH attributes, so removing one filter changed nothing.
            rows.append(_index(300.7 + i, 7610.0 + i / 10.0,
                               source="IBKR_INDEX", field="BID"))
        _fixture(self.tmp, index_rows=rows)
        self.assertAlmostEqual(orc.capture(str(self.tmp), DAY)["offsetPoints"], -3.0, places=3,
                               msg="option-model rows leaked into the offset")

    # --- a session that cannot answer the question is REJECTED, and says why -------------------
    def test_a_session_without_an_es_reference_is_not_accepted(self) -> None:
        _fixture(self.tmp, es_rows=[_es(300 + i, 7650.0 + i / 10.0 - 3.0)
                                    for i in range(orc.MIN_OFFSET_PAIRS + 50)])
        got = orc.capture(str(self.tmp), DAY)
        self.assertFalse(got["accepted"])
        self.assertEqual(got["rejectedBecause"], "no ES reference in the window")

    def test_a_session_whose_offset_cannot_be_measured_is_not_accepted(self) -> None:
        """ES present in the window, but nothing to score it against afterwards."""
        _fixture(self.tmp, index_rows=[_index(-302, 7650.0)],
                 es_rows=[_es(-20 + i, 7647.0) for i in range(20)])
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

    # --- the new rules, each with the case that removes what it names -------------------------
    def test_a_stale_es_reference_at_the_open_is_not_accepted(self) -> None:
        """An ES tick inside the 30 s window but 25 s before the bell is not a reference FOR THE
        OPEN. Accepting it would put a 25-second-old price into slot 0 under the name of an
        observation."""
        _fixture(self.tmp, es_rows=[_es(-25, 7647.0)] + [
            _es(300 + i, 7650.0 + i / 10.0 - 3.0) for i in range(orc.MIN_OFFSET_PAIRS + 50)])
        got = orc.capture(str(self.tmp), DAY)
        self.assertFalse(got["accepted"])
        self.assertIn("ms old at the open", got["rejectedBecause"])
        self.assertEqual(got["esTicksInWindow"], 1, "it IS in the window; that is the point")

    def test_a_fresh_es_reference_at_the_open_is_accepted(self) -> None:
        """The companion: the freshness rule must reject a STALE reference, not every reference."""
        _fixture(self.tmp)
        self.assertTrue(orc.capture(str(self.tmp), DAY)["accepted"])

    def test_one_coincident_pair_is_not_a_session_statistic(self) -> None:
        _fixture(self.tmp, index_rows=[_index(-302, 7650.0), _index(300, 7650.0)],
                 es_rows=[_es(-1, 7647.0), _es(299.5, 7647.0)])
        got = orc.capture(str(self.tmp), DAY)
        self.assertFalse(got["accepted"])
        self.assertIn("offset pairs", got["rejectedBecause"])
        self.assertEqual(got["offsetPairs"], 1)

    def test_option_model_rows_do_not_count_as_index_ticks_or_shorten_the_freeze(self) -> None:
        """The real 2026-09-18 shape: the pre-open series is entirely IBKR_OPTION_MODEL and the
        index itself never prints before the bell. Counting both as one series would report a
        non-empty window and a freeze that did not happen."""
        rows = [_index(-302, 7650.0, source="IBKR_OPTION_MODEL",
                       field="OPTION_MODEL_UNDERLYING"),
                _index(-5, 7651.0, source="IBKR_OPTION_MODEL", field="OPTION_MODEL_UNDERLYING")]
        rows += [_index(300 + i, 7650.0 + i / 10.0) for i in range(orc.MIN_OFFSET_PAIRS + 50)]
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["indexTicksInWindow"], 0)
        self.assertEqual(got["optionModelTicksInWindow"], 1)
        self.assertIsNone(got["freezeSeconds"], "the index never printed before the open")
        self.assertEqual(got["anySourceLastBeforeOpenSource"], "IBKR_OPTION_MODEL")

    def test_a_record_from_another_session_is_not_scored(self) -> None:
        """A row stored under dt=<session> whose own event time is a different New York day is
        rejected, not scored against this 09:30. The partition name is storage, not evidence."""
        foreign = dict(_index(300, 7650.0))
        foreign["eventTime"] = "2026-09-17T13:35:00Z"
        rows = [_index(-302, 7650.0), foreign] + [
            _index(300 + i, 7650.0 + i / 10.0) for i in range(orc.MIN_OFFSET_PAIRS + 50)]
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["indexForeignDateRecords"], 1)

    def test_a_november_session_uses_the_new_york_open_after_the_dst_change(self) -> None:
        """09:30 in New York is 14:30 UTC in November and 13:30 UTC in September. A capture that
        hard-coded either would score the wrong five minutes for half the year."""
        day = "2026-11-17"
        open_et = dt.datetime.combine(dt.date.fromisoformat(day), dt.time(9, 30), ET)
        self.assertEqual(open_et.astimezone(dt.timezone.utc).hour, 14)

        def iso(offset_s):
            return (open_et + dt.timedelta(seconds=offset_s)).astimezone(
                dt.timezone.utc).isoformat().replace("+00:00", "Z")

        idx = [{"price": 7650.0, "source": "IBKR_INDEX", "priceField": "LAST", "quality": "LIVE",
                "eventTime": iso(-302)}]
        idx += [{"price": 7650.0 + i / 10.0, "source": "IBKR_INDEX", "priceField": "LAST",
                 "quality": "LIVE", "eventTime": iso(300 + i)}
                for i in range(orc.MIN_OFFSET_PAIRS + 50)]
        es = [{"spxEquivalent": 7647.0, "spxBasisState": "PROJECTED", "eventTime": iso(-1)}]
        es += [{"spxEquivalent": 7650.0 + i / 10.0 - 3.0, "spxBasisState": "PROJECTED",
                "eventTime": iso(300 + i)} for i in range(orc.MIN_OFFSET_PAIRS + 50)]
        for topic, rows in ((orc.INDEX, idx), (orc.ES, es), (orc.BASIS, [{"level": 1.0}])):
            folder = self.tmp / topic / f"dt={day}"
            folder.mkdir(parents=True, exist_ok=True)
            with gzip.open(folder / "x.jsonl.gz", "wt") as handle:
                for row in rows:
                    handle.write(f"CreateTime:0\tPartition:0\tK\t{json.dumps(row)}\n")
        got = orc.capture(str(self.tmp), day)
        self.assertTrue(got["accepted"], got["rejectedBecause"])
        self.assertEqual(got["indexForeignDateRecords"], 0)

    def test_a_non_numeric_price_is_skipped_not_fatal(self) -> None:
        """A malformed but parseable value must cost its own row, not the day's capture: losing
        exactly the days whose archive was odd would bias the forward sample."""
        bad = dict(_index(400, 0.0))
        bad["price"] = "7650.0"
        rows = [_index(-302, 7650.0), bad] + [
            _index(300 + i, 7650.0 + i / 10.0) for i in range(orc.MIN_OFFSET_PAIRS + 50)]
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)      # must not raise
        self.assertTrue(got["accepted"], got["rejectedBecause"])

    def test_the_record_carries_no_absolute_archive_path(self) -> None:
        _fixture(self.tmp)
        self.assertNotIn("archiveRoot", orc.capture(str(self.tmp), DAY))


if __name__ == "__main__":
    unittest.main()