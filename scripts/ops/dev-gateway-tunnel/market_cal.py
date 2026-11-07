#!/usr/bin/env python3
"""The calendar arithmetic behind market.sh — a port of app.kafka.SpxSessionCalendar's session window.

WHY THIS IS PYTHON AND NOT ``date``. market.sh used ``/bin/date -j`` (the BSD parsing form) for every
calendar step. The absolute path dodges the hazard that killed the 2026-09-01 catch-up — a GNU
``date`` earlier on ``$PATH``, see scripts/ops/et_session.py — but it only moves it: ``/bin/date`` on
Linux IS GNU date, where ``-j`` is a hard error, so the tunnel supervisor could not run anywhere but a
Mac. tests/test_close_board_session_resolution.py forbids shell date arithmetic for exactly this
reason, and it had been failing on market.sh since #1440 went in behind an already-red gate.

The rules, unchanged from market.sh and from the Java class it ports:
  * no session on weekends or on an exchange holiday (a CLOSED row);
  * a trading date D is open from 20:15 ET on the calendar day before it, when that day is a Sunday or
    itself a trading day, and otherwise from 09:30 ET on D;
  * until D's cash close: 16:00 ET, or 13:00 ET on an EARLY_CLOSE half day;
  * the tunnel window is that session plus MKT_LEAD_MIN before the open and MKT_LAG_MIN after the close;
  * outside the table's range the answer is "unknown" and the caller keeps the tunnel up — an idle ssh
    costs nothing, while guessing "closed" could cost a day of data.

Dates are compared as ISO strings, which market.sh did with [[ > ]] and which for YYYY-MM-DD is the same
ordering as the calendar.

  market_cal.py eval <epoch-seconds>   -> "<open|closed|unknown> <end-epoch> <next-epoch>"
  market_cal.py ettext <epoch-seconds> -> "Mon 2026-11-09 20:15 ET"
"""
from __future__ import annotations

import datetime
import os
import sys
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UNKNOWN_END = 9999999999          # the sentinel market.sh has always used for "do not re-ask soon"
LOOK_AHEAD_DAYS = 11              # a long holiday weekend needs up to 5 days; 11 is market.sh's own bound


def _load(path):
    """CLOSED days, EARLY_CLOSE days and the RANGE row, read exactly as market.sh's greps read them."""
    closed, early, rng = set(), set(), None
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            f = line.split(",")
            if len(f) < 2:
                continue
            day, kind = f[0], f[1]
            if kind == "RANGE":
                if len(f) < 3:
                    raise SystemExit("market_cal: the RANGE row of %s has no end date" % path)
                rng = (day, f[2])          # fields 1 and 3 of the ,RANGE, row, as market.sh cut them
            elif kind == "CLOSED":
                closed.add(day)
            elif kind == "EARLY_CLOSE":
                early.add(day)
    if rng is None:
        raise SystemExit("market_cal: %s carries no RANGE row" % path)
    return closed, early, rng


class Calendar:
    def __init__(self, path):
        self.closed, self.early, (self.range_from, self.range_to) = _load(path)

    def supported(self, d: datetime.date) -> bool:
        return self.range_from <= d.isoformat() <= self.range_to

    def trading(self, d: datetime.date) -> bool:
        return d.isoweekday() <= 5 and self.supported(d) and d.isoformat() not in self.closed

    def _at(self, d: datetime.date, hour: int, minute: int) -> int:
        # Seconds pinned to :00. market.sh had to say so explicitly because `date -j` would otherwise
        # borrow the current clock's seconds; here it is simply what the constructor does.
        return int(datetime.datetime(d.year, d.month, d.day, hour, minute, tzinfo=ET).timestamp())

    def close_epoch(self, d: datetime.date) -> int:
        return self._at(d, 13, 0) if d.isoformat() in self.early else self._at(d, 16, 0)

    def open_epoch(self, d: datetime.date) -> int:
        before = d - datetime.timedelta(days=1)
        if before.isoweekday() == 7 or self.trading(before):
            return self._at(before, 20, 15)
        return self._at(d, 9, 30)


def _eval(cal: Calendar, now: int, lead: int, lag: int):
    """market.sh's market_eval: (state, end, next)."""
    d = datetime.datetime.fromtimestamp(now, ET).date()
    if not cal.supported(d):
        return "unknown", UNKNOWN_END, 0
    for _ in range(LOOK_AHEAD_DAYS):
        if cal.trading(d):
            o = cal.open_epoch(d) - lead * 60
            c = cal.close_epoch(d) + lag * 60
            if o <= now < c:
                return "open", c, 0
            if now < o:
                return "closed", 0, o
        d += datetime.timedelta(days=1)
    return "closed", 0, 0


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit("market_cal: %s=%r is not an integer" % (name, raw))


def main(argv):
    if len(argv) != 3:
        raise SystemExit(__doc__.strip().splitlines()[-2].strip())
    cmd, arg = argv[1], argv[2]
    try:
        epoch = int(arg)
    except ValueError:
        raise SystemExit("market_cal: %r is not an epoch in seconds" % arg)
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.environ.get("CAL_FILE") or os.path.join(here, "spx-session-calendar.csv")
    if cmd == "ettext":
        print(datetime.datetime.fromtimestamp(epoch, ET).strftime("%a %Y-%m-%d %H:%M ET"))
        return 0
    if cmd != "eval":
        raise SystemExit("market_cal: unknown command %r (eval|ettext)" % cmd)
    state, end, nxt = _eval(Calendar(path), epoch, _int_env("MKT_LEAD_MIN", 5), _int_env("MKT_LAG_MIN", 20))
    print("%s %d %d" % (state, end, nxt))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
