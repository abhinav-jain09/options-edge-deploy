#!/usr/bin/env bash
# Picks the latest Databento OPRA Historical expiry that is a US trading day.
#
# Usage:   scripts/jenkins/pick-databento-historical-expiry.sh
# Stdout:  the resolved 8-digit YYYYMMDD value (for $(...) capture).
# Stderr:  trace logging (source, raw metadata end, walk-back candidates, final).
# Exit 0:  success. Exit 1: any error (fail-closed; no silent fallback).
#
# Inputs (env):
#   DATABENTO_API_KEY  — required. Must run inside withCredentials.
#   DATABENTO_EXPIRY   — optional override; YYYYMMDD. If set, validated
#                        (regex + trading-day via MarketCalendar) and echoed back.
#
# Runtime dependencies:
#   - python3 (system; tested on /opt/homebrew/bin/python3 + /usr/bin/python3).
#   - The `databento` PyPI package, bootstrapped into a local venv at
#     scripts/jenkins/.helper-venv on first run. ~5 MB, cached across deploys.
#   - market_calendar.py is VENDORED at scripts/jenkins/market_calendar.py
#     (pure stdlib, no external deps). Synced from
#     oe-feed-main/src/options_edge_databento_feed/market_calendar.py — header at
#     top of the vendored file documents the sync procedure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELPER_VENV="$SCRIPT_DIR/.helper-venv"
VENDORED_CAL_DIR="$SCRIPT_DIR"   # market_calendar.py is here

: "${DATABENTO_API_KEY:?DATABENTO_API_KEY must be set (must run inside withCredentials)}"

# --- Override path: validate regex AND trading-day via vendored MarketCalendar ---
if [ -n "${DATABENTO_EXPIRY:-}" ]; then
  # AUTO override: the feed + gateway self-resolve the current trading date from their embedded
  # MarketCalendar (AUTO mode), so pass it straight through with no Historical lookup.
  if printf '%s' "$DATABENTO_EXPIRY" | grep -Eqi '^AUTO$'; then
    echo "source=override value=AUTO (feed+gateway self-resolve from the market calendar)" >&2
    printf 'AUTO\n'
    exit 0
  fi
  if ! printf '%s\n' "$DATABENTO_EXPIRY" | grep -Eq '^[0-9]{8}$'; then
    echo "ERROR: DATABENTO_EXPIRY override '$DATABENTO_EXPIRY' is not 8 digits YYYYMMDD" >&2
    exit 1
  fi
  if ! VENDORED_CAL_DIR="$VENDORED_CAL_DIR" python3 - "$DATABENTO_EXPIRY" <<'PY' >&2
import os, sys
sys.path.insert(0, os.environ["VENDORED_CAL_DIR"])
from datetime import date
from market_calendar import MarketCalendar
raw = sys.argv[1]
try:
    d = date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))
except Exception as e:
    print(f"ERROR: DATABENTO_EXPIRY override '{raw}' is not a valid calendar date: {e}", file=sys.stderr)
    sys.exit(1)
if not MarketCalendar().is_trading_day(d):
    print(f"ERROR: DATABENTO_EXPIRY override '{raw}' is not a US trading day "
          f"({d.strftime('%a')}, weekend or holiday)", file=sys.stderr)
    sys.exit(1)
print(f"override validated: {d} ({d.strftime('%a')}) is a trading day", file=sys.stderr)
PY
  then
    exit 1
  fi
  echo "source=override value=$DATABENTO_EXPIRY" >&2
  printf '%s\n' "$DATABENTO_EXPIRY"
  exit 0
fi

# --- Default (no override): emit AUTO. The feed AND gateway now self-resolve the current ET trading date
# from their embedded MarketCalendar and daily-roll it (options-edge-databento-feed#35 + feed-gateway#41),
# so apply.sh writes DATABENTO_EXPIRY=AUTO / IB_EXPIRY=AUTO — no lagging Databento Historical lookup and no
# daily redeploy. An explicit DATABENTO_EXPIRY=YYYYMMDD still pins (replay). The Historical-walk code below
# is retained (dead) for reference / quick revert.
echo "source=auto value=AUTO (feed+gateway self-resolve from the market calendar)" >&2
printf 'AUTO\n'
exit 0

# --- Bootstrap helper venv with the `databento` package (cached) ---
if [ ! -x "$HELPER_VENV/bin/python3" ]; then
  echo "bootstrapping helper venv at $HELPER_VENV (one-time, ~5 MB)" >&2
  python3 -m venv "$HELPER_VENV" >&2
fi
if ! "$HELPER_VENV/bin/python3" -c "import databento" 2>/dev/null; then
  echo "installing databento into helper venv" >&2
  "$HELPER_VENV/bin/pip" install --quiet --disable-pip-version-check 'databento>=0.40' >&2
fi

# --- Auto path: Databento metadata + vendored MarketCalendar walk ---
result=$(VENDORED_CAL_DIR="$VENDORED_CAL_DIR" "$HELPER_VENV/bin/python3" <<'PY'
import os, re, sys
sys.path.insert(0, os.environ["VENDORED_CAL_DIR"])
from datetime import datetime, timedelta, timezone
try:
    import databento as db
    from market_calendar import MarketCalendar
except ImportError as e:
    print(f"ERROR: required module missing inside helper venv: {e}", file=sys.stderr)
    sys.exit(1)

try:
    client = db.Historical(os.environ["DATABENTO_API_KEY"])
    info = client.metadata.get_dataset_range("OPRA.PILLAR")
except Exception as e:
    print(f"ERROR: Databento metadata API call failed: {e}", file=sys.stderr)
    sys.exit(1)

raw_end = info.get("end")
print(f"databento OPRA.PILLAR metadata end (exclusive) = {raw_end}", file=sys.stderr)
if not raw_end:
    print("ERROR: Databento metadata returned no 'end' field", file=sys.stderr); sys.exit(1)
try:
    end = datetime.fromisoformat(raw_end.replace("Z", "+00:00"))
except Exception as e:
    print(f"ERROR: cannot parse Databento end timestamp '{raw_end}': {e}", file=sys.stderr); sys.exit(1)

day = (end - timedelta(seconds=1)).date()
print(f"last data day (inclusive)            = {day}", file=sys.stderr)
cal = MarketCalendar()
WALK_LIMIT = 14
walked = 0
while not cal.is_trading_day(day):
    print(f"  skip {day} ({day.strftime('%a')}, holiday/weekend)", file=sys.stderr)
    day -= timedelta(days=1)
    walked += 1
    if walked > WALK_LIMIT:
        print(f"ERROR: walked back {WALK_LIMIT} days without finding a trading day", file=sys.stderr)
        sys.exit(1)

result = day.strftime("%Y%m%d")
if not re.match(r"^\d{8}$", result):
    print(f"ERROR: result '{result}' is not 8 digits YYYYMMDD", file=sys.stderr); sys.exit(1)
print(f"source=auto resolved={result} (trading day {day} {day.strftime('%a')})", file=sys.stderr)
print(result)
PY
)
printf '%s\n' "$result"
