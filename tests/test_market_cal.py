# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""scripts/ops/dev-gateway-tunnel/market_cal.py — the calendar arithmetic the tunnel supervisor runs on.

It replaced five `/bin/date -j` calls in market.sh, which could only ever work on a Mac. The facts below
are read off the committed table (spx-session-calendar.csv), so a table edit that moves a session shows
up here as a failure rather than as a tunnel that is up on a holiday or down during a session.
"""
from __future__ import annotations

import datetime
import os
import subprocess
import sys
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parent.parent
HELPER = REPO / "scripts/ops/dev-gateway-tunnel/market_cal.py"
TABLE = REPO / "scripts/ops/dev-gateway-tunnel/spx-session-calendar.csv"
ET = ZoneInfo("America/New_York")


def _epoch(y, mo, d, h, mi):
    return int(datetime.datetime(y, mo, d, h, mi, tzinfo=ET).timestamp())


def _eval(epoch, lead=0, lag=0, table=None):
    env = dict(os.environ, MKT_LEAD_MIN=str(lead), MKT_LAG_MIN=str(lag),
               CAL_FILE=str(table or TABLE))
    r = subprocess.run([sys.executable, str(HELPER), "eval", str(epoch)],
                       capture_output=True, text=True, env=env, timeout=60)
    if r.returncode != 0:
        raise AssertionError("market_cal eval failed: rc=%d %s%s" % (r.returncode, r.stdout, r.stderr))
    state, end, nxt = r.stdout.split()
    return state, int(end), int(nxt)


class MarketCal(unittest.TestCase):
    def test_the_table_the_assertions_below_are_read_from_is_the_committed_one(self):
        # Every expectation here is a fact of this table. If it is not the table the supervisor reads,
        # the assertions are about nothing.
        text = TABLE.read_text()
        self.assertIn("2020-01-01,RANGE,2027-12-31", text, "the supported range moved")
        self.assertIn("2026-11-26,CLOSED,", text, "Thanksgiving 2026 is no longer a closure")
        self.assertIn("2026-11-27,EARLY_CLOSE,", text, "the day after Thanksgiving is no longer a half day")

    def test_a_weekday_session_is_open_between_the_evening_before_and_the_cash_close(self):
        # Tuesday 2026-11-10: open from 20:15 ET Monday, closed at 16:00 ET Tuesday.
        self.assertEqual("open", _eval(_epoch(2026, 11, 9, 20, 15))[0])
        self.assertEqual("closed", _eval(_epoch(2026, 11, 9, 20, 14))[0])
        self.assertEqual("open", _eval(_epoch(2026, 11, 10, 15, 59))[0])
        self.assertEqual("closed", _eval(_epoch(2026, 11, 10, 16, 0))[0])

    def test_the_sunday_evening_open_is_the_monday_session(self):
        # Sunday is not a trading day, yet 20:15 Sunday opens Monday's session — the rule that makes the
        # "day before" test ask about Sundays at all.
        self.assertEqual("open", _eval(_epoch(2026, 11, 8, 20, 15))[0])
        self.assertEqual("closed", _eval(_epoch(2026, 11, 8, 12, 0))[0])

    def test_a_saturday_is_closed_all_day(self):
        for hour in (0, 9, 13, 20, 23):
            self.assertEqual("closed", _eval(_epoch(2026, 11, 7, hour, 0))[0],
                             "Saturday %02d:00 must be closed" % hour)

    def test_a_holiday_is_not_a_session_and_the_half_day_closes_at_1300(self):
        # Thanksgiving 2026-11-26 is CLOSED: nothing opens on the Wednesday evening for it.
        self.assertEqual("closed", _eval(_epoch(2026, 11, 25, 20, 15))[0])
        # 2026-11-27 is an EARLY_CLOSE, so its session ends at 13:00 ET, not 16:00.
        self.assertEqual("open", _eval(_epoch(2026, 11, 27, 12, 59))[0])
        self.assertEqual("closed", _eval(_epoch(2026, 11, 27, 13, 0))[0])

    def test_the_lead_and_lag_widen_the_window_at_both_ends(self):
        open_at = _epoch(2026, 11, 9, 20, 15)
        close_at = _epoch(2026, 11, 10, 16, 0)
        self.assertEqual("closed", _eval(open_at - 300)[0])
        self.assertEqual("open", _eval(open_at - 300, lead=5)[0], "MKT_LEAD_MIN must open the window early")
        self.assertEqual("closed", _eval(close_at + 60)[0])
        self.assertEqual("open", _eval(close_at + 60, lag=20)[0], "MKT_LAG_MIN must hold the window open")
        state, end, _ = _eval(close_at + 60, lag=20)
        self.assertEqual(close_at + 20 * 60, end, "MKT_END must include the lag")

    def test_outside_the_table_the_answer_is_unknown_so_the_tunnel_stays_up(self):
        state, end, nxt = _eval(_epoch(2028, 6, 1, 12, 0))
        self.assertEqual("unknown", state)
        self.assertEqual(9999999999, end, "the caller keeps the tunnel up until this sentinel passes")
        state_before, _, _ = _eval(_epoch(2019, 6, 1, 12, 0))
        self.assertEqual("unknown", state_before)

    def test_a_closed_day_reports_when_the_next_window_starts(self):
        state, end, nxt = _eval(_epoch(2026, 11, 7, 12, 0))      # Saturday
        self.assertEqual(("closed", 0), (state, end))
        self.assertEqual(_epoch(2026, 11, 8, 20, 15), nxt, "the next window is Sunday 20:15 for Monday")

    def test_a_table_without_a_range_row_is_refused_not_guessed(self):
        bad = Path(os.environ.get("TMPDIR", "/tmp")) / "market_cal_no_range.csv"
        bad.write_text("# no RANGE row here\n2026-11-26,CLOSED,Thanksgiving\n")
        self.addCleanup(bad.unlink)
        env = dict(os.environ, CAL_FILE=str(bad), MKT_LEAD_MIN="0", MKT_LAG_MIN="0")
        r = subprocess.run([sys.executable, str(HELPER), "eval", "1794000000"],
                           capture_output=True, text=True, env=env, timeout=60)
        self.assertNotEqual(0, r.returncode, "a table with no RANGE row must be refused")
        self.assertIn("no RANGE row", r.stderr)

    def test_a_non_integer_lead_is_refused_not_treated_as_zero(self):
        env = dict(os.environ, CAL_FILE=str(TABLE), MKT_LEAD_MIN="five", MKT_LAG_MIN="0")
        r = subprocess.run([sys.executable, str(HELPER), "eval", "1794000000"],
                           capture_output=True, text=True, env=env, timeout=60)
        self.assertNotEqual(0, r.returncode)
        self.assertIn("MKT_LEAD_MIN", r.stderr)

    def test_ettext_prints_the_new_york_wall_time(self):
        r = subprocess.run([sys.executable, str(HELPER), "ettext", str(_epoch(2026, 11, 9, 20, 15))],
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual("Mon 2026-11-09 20:15 ET", r.stdout.strip())

    def test_market_sh_no_longer_reaches_for_date_arithmetic(self):
        # The reason this helper exists. tests/test_close_board_session_resolution.py enforces it
        # repo-wide; this says it about the one file that regressed.
        #
        # CODE only, with comments stripped exactly as that guard strips them. Asserting on the raw text
        # instead failed on market.sh's own comment explaining why `/bin/date -j` was removed — a test
        # that claimed "no date arithmetic" while really checking "the words never appear".
        sh = (REPO / "scripts/ops/dev-gateway-tunnel/market.sh").read_text()
        code = "\n".join(line.split("#", 1)[0] for line in sh.splitlines())
        self.assertNotIn("date -j", code, "the BSD parsing form is back in market.sh's code")
        self.assertNotIn("date -v", code, "the BSD arithmetic form is back in market.sh's code")
        self.assertIn("market_cal.py", code, "market.sh must delegate the arithmetic to this helper")
        # and the stripping itself is not vacuous: the explanatory comment is still there to strip
        self.assertIn("/bin/date -j", sh, "the comment recording why this moved should not be deleted")


class MarketShWrapper(unittest.TestCase):
    """market.sh's side: it must survive a helper that cannot answer, and it must be deployable.

    The fail-open branch is the one that decides whether a tooling failure costs a day of dev data, and
    the install command is the one that decides whether the helper is on the host at all.
    """

    SH = REPO / "scripts/ops/dev-gateway-tunnel/market.sh"
    DIR = REPO / "scripts/ops/dev-gateway-tunnel"

    def _source(self, snippet, helper=None):
        script = ". %s\n%s\n" % (self.SH, snippet)
        env = dict(os.environ)
        if helper is not None:
            env["MKT_CAL_PY"] = helper
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env, timeout=60)

    def test_a_helper_that_cannot_answer_keeps_the_tunnel_up(self):
        # "closed" here would DROP the tunnel on a missing file or a broken python; the documented bias is
        # the other way. Verified through market.sh itself, not by reading it.
        r = self._source('market_eval 1794000000; echo "RESULT $MKT_STATE $MKT_END $MKT_NEXT"',
                         helper="/nonexistent/market_cal.py")
        self.assertIn("RESULT unknown 9999999999 0", r.stdout,
                      "a helper that cannot answer must read as unknown with the sentinel end")
        self.assertIn("could not answer", r.stderr, "and it must say so on stderr")

    def test_a_helper_that_prints_rubbish_is_refused_not_believed(self):
        bad = Path(os.environ.get("TMPDIR", "/tmp")) / "market_cal_rubbish.py"
        bad.write_text("print('open not-a-number 0')\n")
        self.addCleanup(bad.unlink)
        r = self._source('market_eval 1794000000; echo "RESULT $MKT_STATE $MKT_END $MKT_NEXT"', helper=str(bad))
        self.assertIn("RESULT unknown 9999999999 0", r.stdout,
                      "a non-numeric end must not be assigned to MKT_END")

    def test_a_helper_that_invents_a_state_is_refused(self):
        # The numeric check alone would let "sideways 0 0" through: the state whitelist is what refuses a
        # word market_eval's callers do not handle. Both validations need their own case.
        bad = Path(os.environ.get("TMPDIR", "/tmp")) / "market_cal_badstate.py"
        bad.write_text("print('sideways 0 0')\n")
        self.addCleanup(bad.unlink)
        r = self._source('market_eval 1794000000; echo "RESULT $MKT_STATE $MKT_END $MKT_NEXT"', helper=str(bad))
        self.assertIn("RESULT unknown 9999999999 0", r.stdout,
                      "a state outside open|closed|unknown must not be assigned to MKT_STATE")

    def test_a_working_helper_is_used(self):
        # The negative tests above would also pass if market_eval ALWAYS said unknown.
        r = self._source('market_eval 1794186700; echo "RESULT $MKT_STATE"')
        self.assertIn("RESULT open", r.stdout, r.stderr)

    def test_the_readme_install_ships_every_file_the_tunnel_needs_at_runtime(self):
        # This command said only `cp tunnel.sh status.sh` from the day the tunnel became market-aware
        # (#1440): market.sh and the table reached the one host by hand, so a fresh install per the README
        # would have sourced a missing market.sh. market_cal.py made it three files.
        lines = [l for l in (self.DIR / "README.md").read_text().splitlines()
                 if "cp " in l and "dev-gateway-tunnel/" in l]
        self.assertEqual(1, len(lines), "expected exactly one install `cp` line in the README, got %d" % len(lines))
        install = lines[0]
        for name, why in (("tunnel.sh", "the supervisor itself"),
                          ("status.sh", "the operator command the README documents"),
                          ("market.sh", "tunnel.sh and status.sh source it"),
                          ("market_cal.py", "market.sh runs it for every evaluation"),
                          ("spx-session-calendar.csv", "market_cal.py reads it")):
            self.assertIn(name, install, "the install command must ship %s — %s" % (name, why))

        # Derived, so the NEXT runtime file added is caught without anyone remembering this test: any
        # sibling whose name appears in the CODE of a shipped script must itself be shipped.
        referenced = set()
        for script in ("tunnel.sh", "status.sh", "market.sh"):
            code = "\n".join(l.split("#", 1)[0]
                              for l in (self.DIR / script).read_text(errors="replace").splitlines())
            for sibling in self.DIR.iterdir():
                if sibling.name == script or sibling.name in ("README.md", "market-test.sh") \
                        or sibling.name.startswith("."):
                    continue
                if sibling.name in code:
                    referenced.add(sibling.name)
        self.assertIn("market_cal.py", referenced,
                      "the derivation found no reference to market_cal.py — it is checking nothing")
        self.assertEqual([], sorted(n for n in referenced if n not in install),
                         "the README install command omits files the runtime scripts reference")


if __name__ == "__main__":
    unittest.main()
