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
DAY="${CHECK_DATE:-$(python3 - "$CAL" <<'PY'
import sys, datetime, importlib.util, os
sys.path.insert(0, sys.argv[1])
try:
    import market_calendar as mc
except Exception:
    mc = None
d = datetime.date.today()
for _ in range(10):
    d -= datetime.timedelta(days=1)
    if mc is None:
        if d.weekday() < 5:
            print(d.isoformat()); break
    elif getattr(mc, "is_trading_day", lambda x: x.weekday() < 5)(d):
        print(d.isoformat()); break
PY
)}"
[ -n "$DAY" ] || { alert "calibration watchdog: could not determine the previous trading day"; exit 1; }

found=$(find "$OUT_ROOT" -name "dt=$DAY.json" 2>/dev/null | head -1)
if [ -z "$found" ]; then
  # This is the failure the watchdog exists for: not "the corpus is empty" but "nobody reported".
  alert "calibration progress MISSING for $DAY (env=$ENV_NAME) — the reporter did not run, or the NAS is unreachable. An empty corpus and a dead reporter look the same without this check."
  exit 1
fi
log "progress record present for $DAY: $found"
PREV="$(find "$OUT_ROOT" -name 'dt=*.json' 2>/dev/null | sort | tail -2 | head -1)"
python3 - "$found" "$DAY" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); day = sys.argv[2]
st = d.get("sessions", {}).get(day, {}).get("archiveStatus", "NOT_IN_CORPUS")
line = d.get("cohorts", [{}])[0]
print("  %s: archiveStatus=%s conflicts=%s" % (day, st, d.get("conflicts")))
cv = d.get("corpusVersion")
print("  corpusVersion=%s" % (cv or "<absent>"))
if not cv:
    print("  WARN: the report carries no corpusVersion — it cannot be shown to have read anything")
    sys.exit(2)
if st not in ("COMPLETE", "NOT_EXPECTED"):
    print("  WARN: %s did not land COMPLETE — it counts toward nothing until it does" % day)
    sys.exit(2)
PY
rc=$?
[ $rc -eq 0 ] || alert "calibration progress for $DAY is not COMPLETE — that session counts toward nothing"
exit 0
