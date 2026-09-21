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
# A full session's scoreable window is 385 minutes (09:35..16:00); fixtures place one pair a
# minute across all of it, so coverage and span are both complete unless a case breaks them.
MINUTES = 385


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
            _index(300 + 60 * i, 7650.0 + i / 10.0)
            for i in range(MINUTES)]
    if es_rows is None:
        # fresh at the open (the last tick is 1 s before it), and enough post-open pairs to clear
        # the MIN_OFFSET_PAIRS floor
        es_rows = [_es(-20 + i, 7647.0) for i in range(20)] + [
            _es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0)
            for i in range(MINUTES)]
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
            _index(300 + 60 * i, 7650.0 + i / 10.0)
            for i in range(MINUTES)]
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
        for i in range(MINUTES):
            rows.append(_index(300 + 60 * i, 7650.0 + i / 10.0))
            # WITHIN the 2 s staleness limit of the ES row at 300+10i, deliberately: at +5 s the
            # staleness guard excluded them and this case passed with the source filter DELETED -
            # it was named for the filter and satisfied by something else entirely.
            rows.append(_index(300.5 + 60 * i, 7670.0 + i / 10.0,
                               source="IBKR_OPTION_MODEL", field="OPTION_MODEL_UNDERLYING"))
            # An IBKR_OPTION_MODEL row whose priceField IS "LAST". Without it, deleting only the
            # `source` predicate changed nothing, because every bad row was also excluded by the
            # field predicate - the case named two filters and tested one.
            rows.append(_index(300.6 + 60 * i, 7690.0 + i / 10.0,
                               source="IBKR_OPTION_MODEL", field="LAST"))
            # An IBKR_INDEX row that is NOT the LAST field. Without this the case could not tell a
            # deleted `priceField == "LAST"` predicate from a kept one: every bad row differed in
            # BOTH attributes, so removing one filter changed nothing.
            rows.append(_index(300.7 + 60 * i, 7610.0 + i / 10.0,
                               source="IBKR_INDEX", field="BID"))
        _fixture(self.tmp, index_rows=rows)
        self.assertAlmostEqual(orc.capture(str(self.tmp), DAY)["offsetPoints"], -3.0, places=3,
                               msg="option-model rows leaked into the offset")

    # --- a session that cannot answer the question is REJECTED, and says why -------------------
    def test_a_session_without_an_es_reference_is_not_accepted(self) -> None:
        _fixture(self.tmp, es_rows=[_es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0)
                                    for i in range(MINUTES)])
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
    def test_a_second_run_of_the_same_session_is_not_published_twice(self) -> None:
        _fixture(self.tmp)
        out = self.tmp / "ledger"
        for _ in range(2):
            subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                            "--archive-root", str(self.tmp), "--out", str(out)],
                           capture_output=True, text=True, check=True)
        published = sorted(p.name for p in (out / "accepted").iterdir())
        self.assertEqual(published, [f"{DAY}.json"],
                         "a repeated capture would double the denominator of the >=55 count")
        # COUNTING FILES CANNOT SEE AN OVERWRITE. The file is marked, and the marker must survive.
        marked = out / "accepted" / f"{DAY}.json"
        marked.write_text(marked.read_text() + "SENTINEL\n")
        subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                        "--archive-root", str(self.tmp), "--out", str(out)],
                       capture_output=True, text=True, check=True)
        self.assertIn("SENTINEL", marked.read_text(),
                      "the capture republished a session it had already published")

    def test_concurrent_captures_publish_one_complete_record(self) -> None:
        """Six captures started together. Only one may publish, and what is published must be a
        WHOLE record - the previous protocol created the destination empty and filled it
        afterwards, so a reader could count an empty file as a captured session."""
        _fixture(self.tmp)
        out = self.tmp / "ledger"
        procs = [subprocess.Popen([sys.executable, str(SCRIPT), "--session", DAY,
                                   "--archive-root", str(self.tmp), "--out", str(out)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                 for _ in range(6)]
        for proc in procs:
            proc.wait()
        published = sorted(p.name for p in (out / "accepted").iterdir())
        self.assertEqual(published, [f"{DAY}.json"], f"six concurrent captures wrote {published}")
        body = (out / "accepted" / f"{DAY}.json").read_text()
        self.assertTrue(body.strip(), "the published file is empty")
        self.assertEqual(json.loads(body)["session"], DAY)

    def test_a_rejected_session_is_not_in_the_accepted_ledger(self) -> None:
        """A session that cannot answer the question is recorded - silence would make the count
        look better than the data - but a consumer that counts the accepted directory must not
        reach 55 on it."""
        _fixture(self.tmp, es_rows=[_es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0)
                                    for i in range(MINUTES)])
        out = self.tmp / "ledger"
        subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                        "--archive-root", str(self.tmp), "--out", str(out)],
                       capture_output=True, check=True)
        self.assertFalse((out / "accepted").exists() and list((out / "accepted").iterdir()))
        self.assertEqual([p.name for p in (out / "rejected").iterdir()], [f"{DAY}.json"])

    def test_a_distinct_usable_session_is_published_as_accepted(self) -> None:
        """The companion to the duplicate guard. The earlier version of this case copied one
        session's events into another partition, where they were all correctly filtered as
        foreign - so it proved only that a REJECTED session is persisted."""
        _fixture(self.tmp)
        out = self.tmp / "ledger"
        subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                        "--archive-root", str(self.tmp), "--out", str(out)],
                       capture_output=True, check=True)
        other = "2026-09-17"
        other_open = dt.datetime.combine(dt.date.fromisoformat(other), dt.time(9, 30), ET)

        def iso(offset_s):
            return (other_open + dt.timedelta(seconds=offset_s)).astimezone(
                dt.timezone.utc).isoformat().replace("+00:00", "Z")

        idx = [{"price": 7650.0, "source": "IBKR_INDEX", "priceField": "LAST",
                "quality": "LIVE", "eventTime": iso(-302)}]
        idx += [{"price": 7650.0 + i / 10.0, "source": "IBKR_INDEX", "priceField": "LAST",
                 "quality": "LIVE", "eventTime": iso(300 + 60 * i)} for i in range(MINUTES)]
        es = [{"spxEquivalent": 7647.0, "spxBasisState": "PROJECTED", "eventTime": iso(-1)}]
        es += [{"spxEquivalent": 7650.0 + i / 10.0 - 3.0, "spxBasisState": "PROJECTED",
                "eventTime": iso(300 + 60 * i)} for i in range(MINUTES)]
        for topic, rows in ((orc.INDEX, idx), (orc.ES, es), (orc.BASIS, [{"level": 1.0}])):
            folder = self.tmp / topic / f"dt={other}"
            folder.mkdir(parents=True, exist_ok=True)
            with gzip.open(folder / "x.jsonl.gz", "wt") as handle:
                for row in rows:
                    handle.write(f"CreateTime:0\tPartition:0\tK\t{json.dumps(row)}\n")
        subprocess.run([sys.executable, str(SCRIPT), "--session", other,
                        "--archive-root", str(self.tmp), "--out", str(out)],
                       capture_output=True, check=True)
        self.assertEqual(sorted(p.name for p in (out / "accepted").iterdir()),
                         [f"{other}.json", f"{DAY}.json"])

    def test_a_duplicated_archive_row_is_not_counted_twice(self) -> None:
        """A replayed row describes the same instant twice. Counting it twice clears a floor it
        should not and weights the median toward whatever was duplicated."""
        rows = [_index(-302, 7650.0)]
        for i in range(MINUTES):
            rows.append(_index(300 + 60 * i, 7650.0 + i / 10.0))
            rows.append(_index(300 + 60 * i, 7650.0 + i / 10.0))      # the same observation again
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["offsetDuplicateRowsSkipped"], MINUTES)
        self.assertEqual(got["offsetPairs"], MINUTES)

    def test_pairs_bunched_into_a_few_minutes_do_not_cover_the_session(self) -> None:
        """Enough pairs, all inside a short burst: a per-session statistic needs the session."""
        rows = [_index(-302, 7650.0)]
        es_rows = [_es(-1, 7647.0)]
        for i in range(MINUTES):
            rows.append(_index(300 + i, 7650.0 + i / 1000.0))
            es_rows.append(_es(300 + i, 7650.0 + i / 1000.0 - 3.0))
        _fixture(self.tmp, index_rows=rows, es_rows=es_rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertGreaterEqual(got["offsetPairs"], MINUTES)
        self.assertFalse(got["accepted"])
        self.assertIn("scoreable minutes", got["rejectedBecause"])


    # --- the session, not part of it -----------------------------------------------------------
    def test_a_half_day_is_judged_against_its_own_close(self) -> None:
        """A 13:00 close leaves 205 scoreable minutes. A sound sample of one must pass, where a
        fixed 195-minute floor would have failed it while passing the first 195 minutes of a full
        day."""
        rows = [_index(-302, 7650.0)] + [
            _index(300 + 60 * i, 7650.0 + i / 10.0) for i in range(205)]
        es_rows = [_es(-1, 7647.0)] + [
            _es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0) for i in range(205)]
        _fixture(self.tmp, index_rows=rows, es_rows=es_rows)
        got = orc.capture(str(self.tmp), DAY, "13:00")
        self.assertTrue(got["accepted"], got["rejectedBecause"])
        self.assertEqual(got["scoreableMinutes"], 205)

    def test_the_first_half_of_a_session_does_not_describe_the_session(self) -> None:
        """Plenty of pairs, full minute-by-minute coverage of the morning, nothing after lunch."""
        rows = [_index(-302, 7650.0)] + [
            _index(300 + 60 * i, 7650.0 + i / 10.0) for i in range(200)]
        es_rows = [_es(-1, 7647.0)] + [
            _es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0) for i in range(200)]
        _fixture(self.tmp, index_rows=rows, es_rows=es_rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertFalse(got["accepted"])
        self.assertIn("spans", got["rejectedBecause"])

    def test_rows_after_the_close_are_not_scored(self) -> None:
        """A topic that keeps publishing after the bell must not extend the session."""
        rows = [_index(-302, 7650.0)] + [
            _index(300 + 60 * i, 7650.0 + i / 10.0) for i in range(MINUTES)]
        # ES ROWS AFTER THE CLOSE TOO. With only index rows there, the 2 s pairing limit excluded
        # them anyway and the case passed with the close bound DELETED - named for the bound,
        # satisfied by the staleness rule. The pairs below are coincident, so only the bound can
        # exclude them.
        rows += [_index(60 * (400 + i), 9999.0) for i in range(30)]
        es_rows = [_es(-1, 7647.0)] + [
            _es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0) for i in range(MINUTES)]
        es_rows += [_es(60 * (400 + i), 9999.0 - 3.0) for i in range(30)]
        _fixture(self.tmp, index_rows=rows, es_rows=es_rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["offsetCoveredMinutes"], MINUTES)
        self.assertAlmostEqual(got["offsetPoints"], -3.0, places=3,
                               msg="post-close rows leaked into the offset")

    def test_two_updates_at_one_timestamp_are_two_observations(self) -> None:
        """A correction shares its predecessor's timestamp. Treating the clock as the identity
        would discard it as a duplicate, deflating the pair count and biasing the median."""
        rows = [_index(-302, 7650.0)]
        for i in range(MINUTES):
            rows.append(_index(300 + 60 * i, 7650.0 + i / 10.0))
            rows.append(_index(300 + 60 * i, 7650.0 + i / 10.0 + 0.25))   # same instant, revised
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["offsetDuplicateRowsSkipped"], 0,
                         "a revision at the same instant is not a duplicate")
        self.assertEqual(got["offsetPairs"], 2 * MINUTES)


    def test_a_timestamp_without_an_offset_is_not_an_instant(self) -> None:
        """`2026-09-18T13:35:00` with no Z and no +HH:MM would be read in the HOST's timezone, so
        the same archive would land in different New York sessions depending on which machine ran
        the capture. It is rejected and counted, not guessed at."""
        naive = dict(_index(400, 7651.0))
        naive["eventTime"] = "2026-09-18T13:35:00"
        rows = [_index(-302, 7650.0), naive] + [
            _index(300 + 60 * i, 7650.0 + i / 10.0) for i in range(MINUTES)]
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["indexUndatedRecords"], 1)
        self.assertEqual(got["offsetPairs"], MINUTES, "the undated row was scored")


    def test_a_session_cannot_be_published_under_two_verdicts(self) -> None:
        """A partial archive rejects; a fuller one would accept. os.link into accepted/ says
        nothing about rejected/, so without a per-SESSION claim the ledger would hold two
        disagreeing records for one denominator candidate."""
        out = self.tmp / "ledger"
        _fixture(self.tmp, es_rows=[_es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0)
                                    for i in range(MINUTES)])          # no reference -> rejected
        subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                        "--archive-root", str(self.tmp), "--out", str(out)],
                       capture_output=True, check=True)
        self.assertEqual([p.name for p in (out / "rejected").iterdir()], [f"{DAY}.json"])
        _fixture(self.tmp)                                              # now it would be accepted
        subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                        "--archive-root", str(self.tmp), "--out", str(out)],
                       capture_output=True, check=True)
        accepted = list((out / "accepted").iterdir()) if (out / "accepted").exists() else []
        self.assertEqual(accepted, [], "the session was published a second time, under the other "
                                       "verdict")

    def test_a_symlinked_output_component_is_refused(self) -> None:
        """`.staging -> /elsewhere` would redirect the write outside --out entirely."""
        out = self.tmp / "ledger"
        elsewhere = self.tmp / "elsewhere"
        elsewhere.mkdir(parents=True)
        out.mkdir(parents=True)
        (out / ".staging").symlink_to(elsewhere)
        _fixture(self.tmp)
        r = subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                            "--archive-root", str(self.tmp), "--out", str(out)],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 73)
        self.assertIn("symlink", r.stderr)
        self.assertEqual(list(elsewhere.iterdir()), [], "it wrote through the link")

    def test_a_tick_exactly_at_the_close_is_outside_the_session(self) -> None:
        """The scoreable universe is 385 minutes; a 16:00 tick bucketed as a 386th contradicted
        the record's own scoreableMinutes and could push a borderline session over the floor."""
        rows = [_index(-302, 7650.0)] + [
            _index(300 + 60 * i, 7650.0 + i / 10.0) for i in range(MINUTES)]
        rows.append(_index(390 * 60, 8000.0))                            # exactly 16:00
        es_rows = [_es(-1, 7647.0)] + [
            _es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0) for i in range(MINUTES)]
        es_rows.append(_es(390 * 60, 8000.0 - 3.0))
        _fixture(self.tmp, index_rows=rows, es_rows=es_rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertEqual(got["offsetCoveredMinutes"], MINUTES)
        self.assertLessEqual(got["offsetCoveredMinutes"], got["scoreableMinutes"])


    def test_a_claim_without_a_record_publishes_THE_CLAIM_not_a_recomputation(self) -> None:
        """Killed between the per-session claim and the verdict link, the session had a marker and
        no record, and every retry reported success - it could never enter the >=55 set although
        it was usable.

        The repair must publish the record that was ALREADY DECIDED. Seeding a claim and rerunning
        against the same archive proves nothing: an implementation that ignored the claim and
        recomputed would pass identically. So the archive is changed between the crash and the
        retry - ES loses its reference, which recomputes as REJECTED - and the published record
        must still be the accepted claim, byte for byte."""
        _fixture(self.tmp)
        out = self.tmp / "ledger"
        (out / ".published").mkdir(parents=True)
        decided = orc.capture(str(self.tmp), DAY)
        self.assertTrue(decided["accepted"])
        seeded = json.dumps(decided, sort_keys=True)
        (out / ".published" / f"{DAY}.json").write_text(seeded + "\n")

        # the archive as the retry finds it: no ES reference in the window any more
        _fixture(self.tmp, es_rows=[_es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0)
                                    for i in range(MINUTES)])
        self.assertFalse(orc.capture(str(self.tmp), DAY)["accepted"],
                         "the retry must recompute differently, or this case proves nothing")

        r = subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                            "--archive-root", str(self.tmp), "--out", str(out)],
                           capture_output=True, text=True, check=True)
        self.assertIn("claimed but never published", r.stderr)
        self.assertEqual([p.name for p in (out / "accepted").iterdir()], [f"{DAY}.json"],
                         "the repair published a recomputation, not the claim")
        self.assertFalse((out / "rejected").exists() and list((out / "rejected").iterdir()))
        published = (out / "accepted" / f"{DAY}.json").read_text().strip()
        self.assertEqual(json.loads(published), json.loads(seeded))

    def test_a_completed_session_is_left_alone(self) -> None:
        """The companion: the repair must fire only for a claim with NO record, never re-link one
        that is already published."""
        _fixture(self.tmp)
        out = self.tmp / "ledger"
        subprocess.run([sys.executable, str(SCRIPT), "--session", DAY, "--archive-root",
                        str(self.tmp), "--out", str(out)], capture_output=True, check=True)
        marked = out / "accepted" / f"{DAY}.json"
        marked.write_text(marked.read_text() + "SENTINEL\n")
        r = subprocess.run([sys.executable, str(SCRIPT), "--session", DAY, "--archive-root",
                            str(self.tmp), "--out", str(out)], capture_output=True, text=True,
                           check=True)
        self.assertIn("already published", r.stderr)
        self.assertIn("SENTINEL", marked.read_text())

    def test_a_ledger_subdirectory_that_is_a_symlink_INSIDE_out_is_still_refused(self) -> None:
        """The earlier case pointed .staging outside --out, so the containment check alone caught
        it and the case passed with the symlink check deleted. This one points INSIDE, where only
        the symlink check can refuse it: a directory this script creates must not be a link."""
        out = self.tmp / "ledger"
        (out / "real").mkdir(parents=True)
        (out / ".staging").symlink_to(out / "real")
        _fixture(self.tmp)
        r = subprocess.run([sys.executable, str(SCRIPT), "--session", DAY,
                            "--archive-root", str(self.tmp), "--out", str(out)],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 73)
        self.assertIn("symlink", r.stderr)
        self.assertEqual(list((out / "real").iterdir()), [])


    def test_a_session_of_stale_index_prints_is_not_accepted(self) -> None:
        """The window demanded quality == "LIVE" while the offset scored every IBKR_INDEX/LAST
        row, so a session of nothing but stale prints was accepted with an offset measured against
        prices that were not live."""
        rows = [_index(-302, 7650.0)] + [
            _index(300 + 60 * i, 7650.0 + i / 10.0, quality="DELAYED") for i in range(MINUTES)]
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertFalse(got["accepted"])
        self.assertEqual(got["offsetPairs"], 0)

    def test_a_single_stale_print_does_not_contaminate_a_live_session(self) -> None:
        """The companion: the quality bar must drop the stale ROW, not the session."""
        stale = _index(400, 9999.0, quality="DELAYED")
        rows = [_index(-302, 7650.0), stale] + [
            _index(300 + 60 * i, 7650.0 + i / 10.0) for i in range(MINUTES)]
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)
        self.assertTrue(got["accepted"], got["rejectedBecause"])
        self.assertAlmostEqual(got["offsetPoints"], -3.0, places=3)

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
            _es(300 + 60 * i, 7650.0 + i / 10.0 - 3.0)
            for i in range(MINUTES)])
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
        self.assertIn("scoreable minutes", got["rejectedBecause"])
        self.assertEqual(got["offsetPairs"], 1)

    def test_option_model_rows_do_not_count_as_index_ticks_or_shorten_the_freeze(self) -> None:
        """The real 2026-09-18 shape: the pre-open series is entirely IBKR_OPTION_MODEL and the
        index itself never prints before the bell. Counting both as one series would report a
        non-empty window and a freeze that did not happen."""
        rows = [_index(-302, 7650.0, source="IBKR_OPTION_MODEL",
                       field="OPTION_MODEL_UNDERLYING"),
                _index(-5, 7651.0, source="IBKR_OPTION_MODEL", field="OPTION_MODEL_UNDERLYING")]
        rows += [_index(300 + i, 7650.0 + i / 10.0) for i in range(MINUTES + 50)]
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
            _index(300 + 60 * i, 7650.0 + i / 10.0)
            for i in range(MINUTES)]
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
                 "quality": "LIVE", "eventTime": iso(300 + 60 * i)}
                for i in range(MINUTES)]
        es = [{"spxEquivalent": 7647.0, "spxBasisState": "PROJECTED", "eventTime": iso(-1)}]
        es += [{"spxEquivalent": 7650.0 + i / 10.0 - 3.0, "spxBasisState": "PROJECTED",
                "eventTime": iso(300 + 60 * i)} for i in range(MINUTES)]
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
            _index(300 + 60 * i, 7650.0 + i / 10.0)
            for i in range(MINUTES)]
        _fixture(self.tmp, index_rows=rows)
        got = orc.capture(str(self.tmp), DAY)      # must not raise
        self.assertTrue(got["accepted"], got["rejectedBecause"])

    def test_the_record_carries_no_absolute_archive_path(self) -> None:
        _fixture(self.tmp)
        self.assertNotIn("archiveRoot", orc.capture(str(self.tmp), DAY))


if __name__ == "__main__":
    unittest.main()