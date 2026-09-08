#!/usr/bin/env bash
# calibration-progress-watch.sh — A5.7's independent watchdog.
#
# "The reporter alerts when no progress record exists" cannot detect a reporter that never ran. This
# runs on a DIFFERENT HOST and a DIFFERENT SCHEDULE (the dev Mac, 07:00 ET, against the reporter's
# 21:00 ET on .252) and asserts, per expected trading day, that a progress record exists and that its
# corpus advanced. A silent reporter and a healthy empty corpus look identical from the outside; this
# is the only thing that tells them apart.
set -uo pipefail
ENV_NAME="${ENV:-prod}"
ARCHIVE_DIR="${ARCHIVE_DIR:-/Volumes/database/optionsedge}"
OUT_ROOT="$ARCHIVE_DIR/calibration-runs/$ENV_NAME"
CAL="${CALENDAR_DIR:-$HOME/development/workspace/options-edge-deploy/scripts/jenkins}"
log() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"; }
# oe-alert.sh is SOURCE-ONLY and mode 0644, so testing -x silently disables every alert this
# watchdog exists to raise. Source it, and say so in the log when it is not there.
# oe-alert.sh is SOURCE-ONLY and mode 0644 — testing -x on it silently disables every alert this
# watchdog exists to raise. It defines alert() itself, so source it and let it own the name; if it is
# not on this host, fall back to a log line and say plainly that the line is the only notice.
if [ -r "$(dirname "$0")/oe-alert.sh" ]; then
  # shellcheck source=/dev/null
  . "$(dirname "$0")/oe-alert.sh"
else
  alert() { log "ALERT (no alert helper on this host — this line is the only notice): $*"; }
fi

# The day to check is the PREVIOUS trading day: the reporter runs at 21:00 ET, this runs at 07:00 ET.
# market_calendar exposes a CLASS. Probing the module for a bare is_trading_day always missed, so this
# ran on the weekday fallback and picked Labor Day as "the previous trading day" (r8 #5). There is no
# fallback now: a watchdog that guesses the calendar alerts on the wrong day, which is worse than one
# that says plainly that it cannot run.
DAY="${CHECK_DATE:-$(python3 - "$CAL" <<'PY'
import sys, datetime
sys.path.insert(0, sys.argv[1])
try:
    from market_calendar import MarketCalendar
except Exception as e:
    print("NO_CALENDAR: %s" % e.__class__.__name__, file=sys.stderr)
    sys.exit(3)
cal = MarketCalendar()
d = datetime.date.today()
for _ in range(10):
    d -= datetime.timedelta(days=1)
    if cal.is_trading_day(d):
        print(d.isoformat())
        break
PY
)}"
[ -n "$DAY" ] || { alert "calibration watchdog: could not determine the previous trading day — the market calendar is missing or unreadable at $CAL"; exit 1; }

found=$(find "$OUT_ROOT" -name "dt=$DAY.json" 2>/dev/null | head -1)
if [ -z "$found" ]; then
  # This is the failure the watchdog exists for: not "the corpus is empty" but "nobody reported".
  alert "calibration progress MISSING for $DAY (env=$ENV_NAME) — the reporter did not run, or the NAS is unreachable. An empty corpus and a dead reporter look the same without this check."
  exit 1
fi
log "progress record present for $DAY: $found"
PREV="$(find "$OUT_ROOT" -name 'dt=*.json' 2>/dev/null | sort | tail -2 | head -1)"
export PREV
python3 - "$found" "$DAY" "${PREV:-}" <<'PY'
import json, os, sys
d = json.load(open(sys.argv[1])); day = sys.argv[2]
prev_path = sys.argv[3] if len(sys.argv) > 3 else ""
# The report keys sessions "date|hash|lineage" — a bare date lookup found nothing and called a
# perfectly healthy session NOT_IN_CORPUS every single day (r8 #6). Match on the date COMPONENT, and
# take the best status among that date's rows: one lineage landing COMPLETE is the day landing.
ORDER = ["COMPLETE", "PENDING_SEAL", "DISCONTINUITY", "INCOMPLETE", "CORRUPT", "MISSING"]
rows = [v for k, v in d.get("sessions", {}).items()
        if (v.get("sessionDate") or str(k).split("|")[0]) == day]
st = "NOT_IN_CORPUS"
if rows:
    st = sorted((r.get("archiveStatus", "MISSING") for r in rows),
                key=lambda x: ORDER.index(x) if x in ORDER else len(ORDER))[0]
line = d.get("cohorts", [{}])[0]
print("  %s: archiveStatus=%s conflicts=%s" % (day, st, d.get("conflicts")))
cv = d.get("corpusVersion")
print("  corpusVersion=%s" % (cv or "<absent>"))
if not cv:
    print("  WARN: the report carries no corpusVersion — it cannot be shown to have read anything")
    sys.exit(2)
# A report that is byte-identical to the previous day's read the SAME corpus: either nothing was
# archived, or the reporter re-published a stale read. Both are worth saying out loud.
if prev_path and os.path.exists(prev_path) and prev_path != sys.argv[1]:
    try:
        pv = json.load(open(prev_path)).get("corpusVersion")
        if pv == cv:
            print("  WARN: corpusVersion did NOT advance since %s — the corpus gained nothing" % os.path.basename(prev_path))
            sys.exit(2)
    except Exception:
        pass
if st not in ("COMPLETE", "NOT_EXPECTED"):
    print("  WARN: %s did not land COMPLETE — it counts toward nothing until it does" % day)
    sys.exit(2)
PY
rc=$?
if [ $rc -ne 0 ]; then
  alert "calibration progress for $DAY is not COMPLETE — that session counts toward nothing"
  # A watchdog that alerts and then exits 0 is invisible to launchd, to a cron mail rule, and to
  # anything watching THIS job: it reports failure only to whoever happens to read the log (r8 #6).
  exit 1
fi
exit 0
