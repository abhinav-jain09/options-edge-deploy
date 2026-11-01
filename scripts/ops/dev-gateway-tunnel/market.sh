# SPX / SPXW options-session calendar for the tunnel supervisor: a port of app.kafka.SpxSessionCalendar (processing-common),
# reading the SAME versioned table (spx-session-calendar.csv). No session on weekends or exchange holidays. A trading date D is
# open from 20:15 ET on the calendar day before it (when that day is a Sunday or a trading day; otherwise from 09:30 ET on D)
# until its cash close (16:00 ET, 13:00 ET on a half day). The tunnel window is that session plus MKT_LEAD_MIN before the open
# and MKT_LAG_MIN after the close (SPX/SPXW options trade to 16:15 ET). Outside the table's range the answer is "unknown" and the
# tunnel stays up (an idle ssh costs nothing; guessing "closed" could cost data).
# Verified against SpxSessionCalendar.optionsEntryDate by market-test.sh. macOS bash 3.2 compatible (no associative arrays).
CAL_FILE=${CAL_FILE:-"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/spx-session-calendar.csv"}
MKT_LEAD_MIN=${MKT_LEAD_MIN:-5}
MKT_LAG_MIN=${MKT_LAG_MIN:-20}
_rf=$(grep -m1 ',RANGE,' "$CAL_FILE" | cut -d, -f1); _rt=$(grep -m1 ',RANGE,' "$CAL_FILE" | cut -d, -f3)
cal_supported() { [[ "$1" > "$_rf" || "$1" == "$_rf" ]] && [[ "$1" < "$_rt" || "$1" == "$_rt" ]]; }
cal_dow()   { /bin/date -j -f "%Y-%m-%d %H:%M:%S" "$1 12:00:00" +%u; }    # 1=Mon .. 7=Sun (midday: date -j fills a missing time from the clock, which DST could skew)
cal_add()   { /bin/date -j -v"$2" -f "%Y-%m-%d %H:%M:%S" "$1 12:00:00" +%Y-%m-%d; }   # cal_add 2026-11-02 -1d
cal_epoch() { TZ=America/New_York /bin/date -j -f "%Y-%m-%d %H:%M:%S" "$1 $2:00" +%s; }   # seconds pinned to :00 (date -j would otherwise borrow the current seconds)
cal_trading() { [ "$(cal_dow "$1")" -le 5 ] && cal_supported "$1" && ! grep -q "^$1,CLOSED," "$CAL_FILE"; }
cal_close_epoch() { if grep -q "^$1,EARLY_CLOSE," "$CAL_FILE"; then cal_epoch "$1" 13:00; else cal_epoch "$1" 16:00; fi; }
cal_open_epoch() {
  local b; b=$(cal_add "$1" -1d)
  if [ "$(cal_dow "$b")" = 7 ] || { cal_supported "$b" && cal_trading "$b"; }; then cal_epoch "$b" 20:15; else cal_epoch "$1" 09:30; fi
}
# market_eval <epoch>: sets MKT_STATE (open|closed|unknown), MKT_END (epoch the current window ends, lag included), MKT_NEXT (epoch the next window starts, lead included)
market_eval() {
  local now=$1 d i o c; MKT_STATE=closed; MKT_END=0; MKT_NEXT=0
  d=$(TZ=America/New_York /bin/date -j -f %s "$now" +%Y-%m-%d)
  if ! cal_supported "$d"; then MKT_STATE=unknown; MKT_END=9999999999; return; fi
  for i in 0 1 2 3 4 5 6 7 8 9 10; do                                    # a long holiday weekend needs up to 5 days of look-ahead
    if cal_trading "$d"; then
      o=$(( $(cal_open_epoch "$d") - MKT_LEAD_MIN * 60 )); c=$(( $(cal_close_epoch "$d") + MKT_LAG_MIN * 60 ))
      if [ "$now" -ge "$o" ] && [ "$now" -lt "$c" ]; then MKT_STATE=open; MKT_END=$c; return; fi
      if [ "$now" -lt "$o" ]; then MKT_NEXT=$o; return; fi
    fi
    d=$(cal_add "$d" +1d)
  done
}
et_text() { TZ=America/New_York /bin/date -j -f %s "$1" "+%a %Y-%m-%d %H:%M ET"; }
