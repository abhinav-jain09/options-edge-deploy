# SPX / SPXW options-session calendar for the tunnel supervisor: a port of app.kafka.SpxSessionCalendar (processing-common),
# reading the SAME versioned table (spx-session-calendar.csv). No session on weekends or exchange holidays. A trading date D is
# open from 20:15 ET on the calendar day before it (when that day is a Sunday or a trading day; otherwise from 09:30 ET on D)
# until its cash close (16:00 ET, 13:00 ET on a half day). The tunnel window is that session plus MKT_LEAD_MIN before the open
# and MKT_LAG_MIN after the close (SPX/SPXW options trade to 16:15 ET). Outside the table's range the answer is "unknown" and the
# tunnel stays up (an idle ssh costs nothing; guessing "closed" could cost data).
# Verified against SpxSessionCalendar.optionsEntryDate by market-test.sh. macOS bash 3.2 compatible (no associative arrays).
#
# THE CALENDAR ARITHMETIC LIVES IN market_cal.py, AND MUST. Every step here used to be `/bin/date -j`, the BSD parsing form.
# The absolute path dodged the hazard that killed the 2026-09-01 catch-up — a GNU `date` earlier on $PATH, see
# scripts/ops/et_session.py — but it only moved it: /bin/date on Linux IS GNU date, where -j is a hard error, so this file
# could not run anywhere but a Mac. tests/test_close_board_session_resolution.py forbids shell date arithmetic for exactly
# that reason, and had been failing on this file since #1440 landed behind an already-red gate.
# One python3 call now answers a whole evaluation, where the shell took six `date` forks per day examined.
CAL_FILE=${CAL_FILE:-"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/spx-session-calendar.csv"}
MKT_LEAD_MIN=${MKT_LEAD_MIN:-5}
MKT_LAG_MIN=${MKT_LAG_MIN:-20}
MKT_CAL_PY=${MKT_CAL_PY:-"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/market_cal.py"}
# Said at source time, as the old top-level `grep ,RANGE,` did: a missing table should be visible before the first evaluation.
# Not fatal — sourcing a library must not kill its caller — and market_eval below fails OPEN if it stays broken.
[ -r "$CAL_FILE" ] || echo "market.sh: the calendar table $CAL_FILE is not readable" >&2
[ -r "$MKT_CAL_PY" ] || echo "market.sh: the calendar helper $MKT_CAL_PY is not readable" >&2

# market_eval <epoch>: sets MKT_STATE (open|closed|unknown), MKT_END (epoch the current window ends, lag included), MKT_NEXT (epoch the next window starts, lead included)
market_eval() {
  local out state end next
  MKT_STATE=closed; MKT_END=0; MKT_NEXT=0
  # stdout only: the helper's diagnostics go to the caller's stderr, where the supervisor log keeps them.
  out=$(CAL_FILE="$CAL_FILE" MKT_LEAD_MIN="$MKT_LEAD_MIN" MKT_LAG_MIN="$MKT_LAG_MIN" python3 "$MKT_CAL_PY" eval "$1") || out=
  read -r state end next <<< "$out"
  case "${state:-}" in open|closed|unknown) ;; *) state= ;; esac
  case "${end:-x}${next:-x}" in *[!0-9]*) state= ;; esac
  if [ -z "$state" ]; then
    # The helper could not answer: no python3, an unreadable or malformed table, a bug. Reporting "closed" would DROP the
    # tunnel on a tooling failure; this file's bias is the other way round, so fail OPEN as "unknown" and say so.
    echo "market.sh: $MKT_CAL_PY could not answer for epoch $1 — keeping the tunnel up (got: ${out:-no output})" >&2
    MKT_STATE=unknown; MKT_END=9999999999; MKT_NEXT=0
    return
  fi
  MKT_STATE=$state; MKT_END=$end; MKT_NEXT=$next
}
et_text() { python3 "$MKT_CAL_PY" ettext "$1"; }
