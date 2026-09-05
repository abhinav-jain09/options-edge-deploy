"""Which session the close chain freezes, and the portability rule that failure taught.

THE INCIDENT (2026-09-01, stockgex-close-board build #33). The morning catch-up cron fired at
01:00 ET and was dead one second later:

    date: invalid option -- 'v'

The wrapper asked for yesterday with `date -v-1d`, the BSD spelling, reasoning that "the agents
are Macs". The agent IS a Mac — but its PATH resolves `date` to GNU coreutils, where `-v` is not
a flag. Nothing was published and nothing was damaged (the guard held), but the ONE run whose job
is to heal a lost night could not run at all, and the evening runs stayed green the whole time,
so the chain looked healthy.

Two tests, therefore: the behaviour (with a movable clock, so it is not asserting the
implementation back to itself), and the CLASS — no shell script in this repo may do date
arithmetic through `date`, in either dialect, because which dialect answers is a property of the
host's PATH and not of anything we control.
"""
import datetime
import pathlib
import re
import subprocess
import sys
import unittest

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts" / "ops"))

import et_session  # noqa: E402


def _at(iso):
    return datetime.datetime.fromisoformat(iso).replace(tzinfo=et_session.ET)


class SessionResolution(unittest.TestCase):

    def test_the_evening_run_freezes_today(self):
        # 16:10 ET, the first cron: the close just happened, and it is today's board.
        self.assertEqual(et_session.session_for(_at("2026-09-01T16:10")),
                         datetime.date(2026, 9, 1))

    def test_the_morning_catchup_heals_yesterday(self):
        # 01:00 ET, the second cron. Today has not closed; the night to heal is the one behind.
        self.assertEqual(et_session.session_for(_at("2026-09-01T01:00")),
                         datetime.date(2026, 8, 31))

    def test_the_boundary_is_noon_and_it_is_exclusive(self):
        self.assertEqual(et_session.session_for(_at("2026-09-01T11:59")),
                         datetime.date(2026, 8, 31), "11:59 is still catch-up")
        self.assertEqual(et_session.session_for(_at("2026-09-01T12:00")),
                         datetime.date(2026, 9, 1), "noon is not")

    def test_catchup_walks_back_over_month_and_year_ends(self):
        self.assertEqual(et_session.session_for(_at("2026-09-01T01:00")),
                         datetime.date(2026, 8, 31))
        self.assertEqual(et_session.session_for(_at("2026-01-01T01:00")),
                         datetime.date(2025, 12, 31))

    def test_catchup_on_a_monday_names_the_weekend_and_that_is_correct(self):
        # Monday 01:00 ET names Sunday. The wrapper's calendar check then exits NOT_A_TRADING_DAY,
        # harmlessly — the alternative (guessing Friday here) would silently freeze a session two
        # days old under today's date.
        self.assertEqual(et_session.session_for(_at("2026-08-31T01:00")),
                         datetime.date(2026, 8, 30))

    def test_the_cli_prints_hour_and_session_for_the_shell_to_split(self):
        out = subprocess.run([sys.executable, str(REPO / "scripts/ops/et_session.py"),
                              "--now", "2026-09-01T01:00"],
                             capture_output=True, text=True, check=True).stdout.strip()
        self.assertEqual(out, "01 2026-08-31")
        hour, session = out.split(" ")          # exactly how the wrapper reads it
        self.assertTrue(int(hour) < et_session.CATCHUP_BEFORE_HOUR)
        self.assertRegex(session, r"^\d{4}-\d{2}-\d{2}$")


class NoShellDateArithmetic(unittest.TestCase):
    """`date -v…` (BSD) and `date -d …` (GNU) are both bets on the host's PATH. Neither is allowed.

    Reading the clock is fine — `date +%s`, `date +%F`, `date -u +…` mean the same thing
    everywhere. It is ARITHMETIC and PARSING that diverge, and those are what this forbids.
    """

    # `date -v…` in any unit; `date -d <arg>`; and the -j/-r parsing forms BSD alone has.
    FORBIDDEN = re.compile(r"\bdate\s+(-v|-d\s|-j\b|-r\s)")

    def test_no_script_does_date_arithmetic_through_the_shell(self):
        offenders = []
        for path in sorted((REPO / "scripts").rglob("*.sh")):
            for n, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
                code = line.split("#", 1)[0]
                if self.FORBIDDEN.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{n}: {line.strip()}")
        self.assertEqual(offenders, [], "\n".join(
            ["date arithmetic in shell is not portable across the agents' PATHs — this is the",
             "2026-09-01 catch-up failure. Use python3 (scripts/ops/et_session.py is the example):"]
            + offenders))

    def test_the_close_board_wrapper_resolves_its_session_through_the_helper(self):
        wrapper = (REPO / "scripts/ops/stock-gex-close-board.sh").read_text()
        self.assertIn("scripts/ops/et_session.py", wrapper,
                      "the wrapper must ask the helper, not the shell, which session it is for")


if __name__ == "__main__":
    unittest.main()
