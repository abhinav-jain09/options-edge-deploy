# Vendored from oe-feed-main/src/options_edge_databento_feed/market_calendar.py
# DO NOT EDIT BY HAND — sync via:
#   cp /path/to/oe-feed-main/src/options_edge_databento_feed/market_calendar.py \
#      scripts/jenkins/market_calendar.py
#   then re-add this sync header.
# Pure stdlib (datetime + zoneinfo). No external deps.

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

    def current_trading_date(self, now: datetime | None = None) -> date:
        """ET trading date: today if a trading day, else the most recent trading day."""
        day = self._now_et(now).date()
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
