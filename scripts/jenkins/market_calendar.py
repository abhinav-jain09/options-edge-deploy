# VENDORED — DO NOT EDIT HERE.
# source: options-edge-databento-feed src/options_edge_databento_feed/market_calendar.py
# repo commit: 330c54a   vendored: 2026-07-25   sha256: a2e56aed93df18bbee65c0e08507ddb2a73da895f6a28f3f35eb4b8a8ffe2507
# The platform-canonical calendar (python mirror of Java app.web.MarketCalendar); the feed
# liveness gate uses it, so the watcher agrees with the fleet on what "open" means.
# Drift check: tests/test_freshness_watch.py compares this file against the source when the
# feed repo is present locally, and FAILS on divergence — re-vendor instead of editing.
"""US equity-options market calendar, anchored to America/New_York.

Python mirror of app.web.MarketCalendar (Java) so the feed agrees with the rest
of the platform on what "the market is open" means. All decisions are made on
the absolute instant converted to Eastern time -- the host/container's local
timezone is never used, so this behaves identically whether the process runs in
a UTC container, an Amsterdam laptop, or anywhere else.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
NORMAL_CLOSE = time(16, 0)
EARLY_CLOSE = time(13, 0)

_MON = 0
_THU = 3
_SAT = 5
_SUN = 6


def _easter_sunday(year: int) -> date:
    # Anonymous Gregorian computus.
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    ll = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ll) // 451
    month = (h + ll - 7 * m + 114) // 31
    day = ((h + ll - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _observed(holiday: date) -> date:
    if holiday.weekday() == _SAT:
        return holiday - timedelta(days=1)
    if holiday.weekday() == _SUN:
        return holiday + timedelta(days=1)
    return holiday


def globex_full_closure_dates(year: int) -> frozenset[date]:
    """CME Globex FULL-closure days (24h shutdown).

    Globex's weekly cycle is Sunday 6pm ET -> Friday 5pm ET, with a daily ~1h maintenance break.
    On most US-equity holidays Globex still trades a shortened session (e.g. Independence Day
    observed = ~12:15 ET early close, but ES still runs premarket). Only a handful of dates are
    truly closed all day: New Year's Day, Good Friday, and Christmas Day. We intentionally do NOT
    model early-close cutoff times here -- ES subscriptions can survive early-close windows
    without producing spurious errors (Databento just goes quiet when Globex halts).
    """
    days: set[date] = set()
    for y in (year - 1, year, year + 1):
        days.add(_observed(date(y, 1, 1)))              # New Year's Day
        days.add(_easter_sunday(y) - timedelta(days=2)) # Good Friday
        days.add(_observed(date(y, 12, 25)))            # Christmas Day
    return frozenset(days)


def parse_date_set(raw: str) -> frozenset[date]:
    """Parse 'YYYY-MM-DD,YYYY-MM-DD' or 'YYYY-MM-DD=13:00:00,...' (early-close form)."""
    out: set[date] = set()
    for token in (raw or "").replace(";", ",").split(","):
        token = token.strip()
        if not token:
            continue
        day_part = token.split("=", 1)[0].strip()
        try:
            out.add(date.fromisoformat(day_part))
        except ValueError:
            continue
    return frozenset(out)


class MarketCalendar:
    def __init__(
        self,
        extra_holidays: frozenset[date] = frozenset(),
        extra_early_closes: frozenset[date] = frozenset(),
    ) -> None:
        self._extra_holidays = set(extra_holidays)
        self._extra_early_closes = set(extra_early_closes)

    def _full_holidays(self, year: int) -> set[date]:
        holidays: set[date] = set()
        for y in (year - 1, year, year + 1):
            holidays.add(_observed(date(y, 1, 1)))          # New Year's Day
            holidays.add(_nth_weekday(y, 1, _MON, 3))       # MLK Jr.
            holidays.add(_nth_weekday(y, 2, _MON, 3))       # Presidents' Day
            holidays.add(_easter_sunday(y) - timedelta(days=2))  # Good Friday
            holidays.add(_last_weekday(y, 5, _MON))         # Memorial Day
            holidays.add(_observed(date(y, 6, 19)))         # Juneteenth
            holidays.add(_observed(date(y, 7, 4)))          # Independence Day
            holidays.add(_nth_weekday(y, 9, _MON, 1))       # Labor Day
            holidays.add(_nth_weekday(y, 11, _THU, 4))      # Thanksgiving
            holidays.add(_observed(date(y, 12, 25)))        # Christmas Day
        return holidays

    def is_weekend(self, day: date) -> bool:
        return day.weekday() >= _SAT

    def is_holiday(self, day: date) -> bool:
        return day in self._extra_holidays or day in self._full_holidays(day.year)

    def is_trading_day(self, day: date) -> bool:
        return not self.is_weekend(day) and not self.is_holiday(day)

    def next_trading_date(self, day: date) -> date:
        """The next trading day strictly AFTER ``day`` (skips weekends and full holidays).

        This is the NEXT-SESSION (1DTE) expiry for the SPXW chain: SPXW expires every trading
        day, so the next session is simply the next open day — never a naive ``day + 1``.
        Bounded walk so a mis-specified calendar cannot loop forever."""
        probe = day + timedelta(days=1)
        for _ in range(30):
            if self.is_trading_day(probe):
                return probe
            probe = probe + timedelta(days=1)
        raise ValueError(f"no trading day within 30 days after {day}")

    def is_early_close(self, day: date) -> bool:
        if not self.is_trading_day(day):
            return False
        if day in self._extra_early_closes:
            return True
        thanksgiving = _nth_weekday(day.year, 11, _THU, 4)
        day_before_july4 = _observed(date(day.year, 7, 4)) - timedelta(days=1)
        return (
            day == day_before_july4
            or day == thanksgiving + timedelta(days=1)
            or (day.month == 12 and day.day == 24)
        )

    def close_time(self, day: date) -> time:
        return EARLY_CLOSE if self.is_early_close(day) else NORMAL_CLOSE

    def _now_et(self, now: datetime | None) -> datetime:
        if now is None:
            return datetime.now(MARKET_TZ)
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(MARKET_TZ)

    def is_open(self, now: datetime | None = None) -> bool:
        et = self._now_et(now)
        day = et.date()
        if not self.is_trading_day(day):
            return False
        return MARKET_OPEN <= et.time() < self.close_time(day)

    def current_trading_date(
        self, now: datetime | None = None, roll_after: time | None = None
    ) -> date:
        """ET trading date: today if a trading day, else the most recent trading day.

        When ``roll_after`` is set (e.g. the CME Globex session-open time, 18:00 ET), the trade
        date advances to the NEXT trading day once the ET wall-clock passes that time. The near-24h
        GLBX session that opens in the evening belongs to the next calendar trade date, so a 0DTE
        feed must roll at session open rather than waiting for midnight ET (which would freeze the
        chain on the just-expired contracts for the whole evening session). Weekends/holidays are
        skipped forward. ``roll_after=None`` keeps the legacy midnight-ET / most-recent behavior."""
        et = self._now_et(now)
        day = et.date()
        if roll_after is not None:
            # Session-aware (GLBX): past the session-open boundary the active session belongs to the
            # next trade date; before it we stay on today. Either way a non-trading candidate skips
            # FORWARD to the next trading day — never backward — so the resolved date is monotonic
            # across a weekend/holiday and _apply_auto_rollover can't flip-flop (e.g. Fri 18:00 -> Mon,
            # and Sat/Sun stay Mon rather than snapping back to Fri).
            if et.time() >= roll_after:
                day += timedelta(days=1)
            while not self.is_trading_day(day):
                day += timedelta(days=1)
            return day
        while not self.is_trading_day(day):
            day -= timedelta(days=1)
        return day

    def next_open(self, now: datetime | None = None) -> datetime:
        et = self._now_et(now)
        day = et.date()
        if self.is_trading_day(day):
            today_open = datetime.combine(day, MARKET_OPEN, MARKET_TZ)
            if et < today_open:
                return today_open
        day += timedelta(days=1)
        while not self.is_trading_day(day):
            day += timedelta(days=1)
        return datetime.combine(day, MARKET_OPEN, MARKET_TZ)

    def seconds_until_next_open(self, now: datetime | None = None) -> float:
        et = self._now_et(now)
        return max(0.0, (self.next_open(et) - et).total_seconds())


# ── CME Globex session model (liveness gating for GLBX feeds) ────────────────────────────────
# Weekly cycle: Sunday 18:00 ET -> Friday 17:00 ET, with a daily 17:00-18:00 ET maintenance
# break Mon-Thu, closed on globex_full_closure_dates. Early-close half-days are treated as OPEN
# (Databento just goes quiet at the halt; the liveness staleness window is judged against a
# session that may deliver thin data anyway — see monitoring.evaluate_liveness's stale window).
# This deliberately mirrors the approximation documented on globex_full_closure_dates.
GLOBEX_OPEN = time(18, 0)
GLOBEX_CLOSE = time(17, 0)


def is_globex_open(now_et: datetime, extra_closures: frozenset[date] = frozenset()) -> bool:
    """True while CME Globex is trading (ES futures/options), per the model above."""
    closures = globex_full_closure_dates(now_et.year) | extra_closures
    day = now_et.date()
    t = now_et.time()
    if day in closures:
        return False
    wd = now_et.weekday()
    if wd == _SAT:
        return False
    if wd == _SUN:
        return t >= GLOBEX_OPEN
    if wd == 4:  # Friday: open until 17:00, weekend after
        return t < GLOBEX_CLOSE
    # Mon-Thu: closed only during the 17:00-18:00 maintenance break
    return not (GLOBEX_CLOSE <= t < GLOBEX_OPEN)


def globex_session_open_ts(now_et: datetime, extra_closures: frozenset[date] = frozenset()) -> float:
    """Epoch seconds of the most recent Globex session OPEN (an 18:00 ET boundary) at/before
    ``now_et`` — the startup-grace anchor mirroring monitoring._todays_open_ts for RTH."""
    closures = globex_full_closure_dates(now_et.year) | extra_closures
    probe = now_et
    for _ in range(10):  # walk back at most 10 days (covers weekend + closures)
        day = probe.date()
        candidate = datetime.combine(day, GLOBEX_OPEN, tzinfo=now_et.tzinfo)
        opened_today = (
            candidate <= now_et
            and day not in closures
            and day.weekday() not in (4, _SAT)  # no Friday/Saturday evening open
        )
        if opened_today:
            return candidate.timestamp()
        probe = probe - timedelta(days=1)
    return datetime.combine(now_et.date(), GLOBEX_OPEN, tzinfo=now_et.tzinfo).timestamp()
