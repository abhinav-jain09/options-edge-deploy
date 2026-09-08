# oe-trading-day.sh — the ONE trading-day gate. Sourced, never executed.
#
# Extracted from oe-archive-daily.sh on 2026-08-12 so the archiver and the completeness verifier
# agree on which dates are EXPECTED to hold data. If they disagreed, the verifier would either
# alert every weekend or stay silent on a missed weekday — and a completeness check that is wrong
# about which days count is not a completeness check.
#
# FAIL-SAFE DIRECTION: a weekday with no usable calendar is treated as a TRADING DAY. Archiving on
# a holiday costs an empty run; skipping a real session costs a session. Note this is the opposite
# of the verifier's own failure direction, which alerts when unsure — both err toward "do not lose
# evidence silently".
CALENDAR_DIR="${CALENDAR_DIR:-/home/abhinav/autostart/jenkins}"

# is_trading_day [YYYY-MM-DD]  -> prints "yes" | "no"
# With no argument, uses the current date in New York.
is_trading_day() {
  CALENDAR_DIR="$CALENDAR_DIR" OE_DAY="${1:-}" python3 - <<'PY' 2>/dev/null
import os, sys
from datetime import datetime, date
from zoneinfo import ZoneInfo
sys.path.insert(0, os.environ.get("CALENDAR_DIR", ""))
raw = os.environ.get("OE_DAY", "").strip()
if raw:
    try:
        d = date.fromisoformat(raw)
    except ValueError:
        print("no"); raise SystemExit
else:
    d = datetime.now(ZoneInfo("America/New_York")).date()
if d.weekday() > 4:
    print("no"); raise SystemExit
try:
    from market_calendar import MarketCalendar
    print("yes" if MarketCalendar().is_trading_day(d) else "no")
except Exception:
    print("yes")   # weekday and no calendar -> assume trading day (fail-safe: archive anyway)
PY
}
