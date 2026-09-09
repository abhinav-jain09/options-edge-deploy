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
# The archive root is RESOLVED, not guessed. The old default was /Volumes/database/optionsedge, which
# is not where the NAS is mounted on the dev Mac this watchdog is scheduled on (it is
# //192.168.100.100/database on $HOME/nas-db), so installing the launchd agent would have produced a
# MISSING alert every trading day forever. A watchdog that cries wolf daily is read by nobody, which
# is the same outcome as never installing it (BZ 360 #1).
#
# An explicit ARCHIVE_DIR still wins outright, which is what every test passes and what the .252
# crontab would pass; the candidates below are only consulted when the caller said nothing.
ARCHIVE_ROOT_CANDIDATES="${ARCHIVE_ROOT_CANDIDATES:-$HOME/nas-db/optionsedge /Volumes/database/optionsedge /mnt/nas/optionsedge}"
if [ -n "${ARCHIVE_DIR:-}" ]; then
  ARCHIVE_ROOT_SOURCE="ARCHIVE_DIR was set explicitly"
else
  for _c in $ARCHIVE_ROOT_CANDIDATES; do
    [ -d "$_c" ] || continue
    ARCHIVE_DIR="$_c"; ARCHIVE_ROOT_SOURCE="resolved from ARCHIVE_ROOT_CANDIDATES"; break
  done
fi
OUT_ROOT="${ARCHIVE_DIR:-}/calibration-runs/$ENV_NAME"
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
[ -n "$DAY" ] || { alert "calibration watchdog cannot run: could not determine the previous trading day — the market calendar is missing or unreadable at $CAL"; exit 1; }

# TWO FAILURES, TWO ALERTS. This watchdog exists to tell a silent reporter apart from a healthy empty
# corpus; it cannot do that while an unreachable archive root produces the same sentence as a dead
# reporter. One is a mount to fix HERE, the other is a job to fix on .252, and reading the alert must
# be enough to know which (BZ 360 #2). So the root is checked first, and says so in its own words.
if [ -z "${ARCHIVE_DIR:-}" ] || [ ! -d "$ARCHIVE_DIR" ]; then
  alert "calibration watchdog cannot run: no archive root on this host (tried: $ARCHIVE_ROOT_CANDIDATES). This is a MOUNT problem here, NOT evidence about the reporter on .252 — it says nothing about whether $DAY was archived."
  exit 1
fi
log "archive root $ARCHIVE_DIR (${ARCHIVE_ROOT_SOURCE:-resolved})"
if [ ! -d "$OUT_ROOT" ]; then
  alert "calibration watchdog cannot run: the archive root $ARCHIVE_DIR is mounted but has no calibration-runs/$ENV_NAME. Either the env name is wrong here, or the reporter has never once written for env=$ENV_NAME — check .252 before treating this as a mount problem."
  exit 1
fi

# THE COHORT IS DECLARED, NOT DISCOVERED (r19 #1). This used to take the first dt=<day>.json anywhere
# under the env, so a healthy record belonging to an OLD cohort — a previous parameter hash, or a
# previous TRACK_FROM_PUSH — satisfied the watchdog while the cohort we actually declare had nothing.
# That is the exact failure this watchdog exists to catch, passing because it looked at the wrong
# corpus. The declaration lives beside this script, with no way to point it elsewhere, for the same
# reason the reporter and the evaluator refuse a caller-supplied one: a watchdog that can be handed
# another declaration can be made to watch another cohort.
TARGETS="$(dirname "$0")/calibration-targets.env"
if [ ! -f "$TARGETS" ]; then
  alert "calibration watchdog cannot run: no calibration-targets.env beside $0. It would otherwise fall back to whichever cohort happens to be on disk, which is how a stale cohort passes for a live one."
  exit 1
fi
# shellcheck source=/dev/null
. "$TARGETS"
eval "DECLARED_HASH=\"\${OE_CAL_PARAMETER_SET_HASH_${ENV_NAME}:-}\""
eval "DECLARED_TRACK=\"\${OE_CAL_TRACK_FROM_PUSH_${ENV_NAME}:-}\""
# WHERE the cohort's records live is decided in ONE place — oe_corpus_reader.cohort_progress_dir —
# and both the reporter and this watchdog ask it. They used to each compute the path, and their
# fallbacks disagreed (the reporter used "unfrozen" for an absent trackFromPush, this used the empty
# string). Both environments declare 2099-01-01 today, so they agreed by luck rather than by design.
# A HALF-FROZEN DECLARATION HAS NO KNOWABLE COHORT PATH. While the hash is UNFROZEN the reporter does
# not filter to a declared cohort, so once calls qualify it writes under each DISCOVERED hash — and
# this script would be looking under "UNFROZEN". With both literals unfrozen (what ships) nothing
# qualifies and the two agree, so the disagreement needs a hash still UNFROZEN while TRACK_FROM_PUSH
# has already passed. The design forbids that: the literals are frozen together, in one commit,
# before anyone looks at the data. Depending on the design being obeyed is how a property held in two
# places goes wrong, so refuse it here and say which half is missing.
if [ "${DECLARED_HASH:-}" = "UNFROZEN" ] && [ -n "${DECLARED_TRACK:-}" ] \
   && [ "${DECLARED_TRACK:0:10}" != "UNFROZEN" ] \
   && [ "${DECLARED_TRACK:0:10}" \< "$(TZ=America/New_York date '+%Y-%m-%d')" ]; then
  alert "calibration watchdog cannot run: the declaration is half-frozen — OE_CAL_PARAMETER_SET_HASH_${ENV_NAME} is still UNFROZEN while OE_CAL_TRACK_FROM_PUSH_${ENV_NAME}=${DECLARED_TRACK} has already passed. The reporter writes under each cohort it DISCOVERS in that state, so there is no cohort path to check. Freeze the hash with the rest of the literals, in one commit."
  exit 1
fi
if [ ! -f "$(dirname "$0")/oe_corpus_reader.py" ]; then
  alert "calibration watchdog cannot run: oe_corpus_reader.py is not beside $0, so the cohort path would have to be recomputed here — which is exactly the duplication that let the reporter and this watchdog disagree."
  exit 1
fi
COHORT_DIR="$(SD="$(dirname "$0")" OR="$OUT_ROOT" H="${DECLARED_HASH:-}" T="${DECLARED_TRACK:-}" python3 -c '
import os, sys
sys.path.insert(0, os.environ["SD"])
import oe_corpus_reader as R
print(R.cohort_progress_dir(os.environ["OR"], os.environ["H"] or None, os.environ["T"] or None))')"
[ -n "$COHORT_DIR" ] || { alert "calibration watchdog cannot run: could not resolve the cohort directory from the declaration"; exit 1; }
found=""
[ -f "$COHORT_DIR/dt=$DAY.json" ] && found="$COHORT_DIR/dt=$DAY.json"
if [ -z "$found" ]; then
  # Distinguish "nobody reported" from "somebody reported, for a cohort we no longer declare". The
  # second is not a quiet reporter — it is a declaration that moved and a watchdog pointed at history.
  foreign="$(find "$OUT_ROOT" -name "dt=$DAY.json" 2>/dev/null | head -1)"
  if [ -n "$foreign" ]; then
    alert "calibration progress for $DAY exists ONLY for a cohort we do not declare (found $foreign; the declared cohort is hash=${DECLARED_HASH:-<none>} trackFromPush=${DECLARED_TRACK:-<none>}, whose records belong under $COHORT_DIR). The reporter ran, but not for the cohort calibration-targets.env names — nothing under the current declaration counts."
    exit 1
  fi
fi
if [ -z "$found" ]; then
  # This is the failure the watchdog exists for: not "the corpus is empty" but "nobody reported".
  # The archive root is known-present by here, so this sentence no longer has to hedge about the NAS.
  alert "calibration progress MISSING for $DAY (env=$ENV_NAME) — the archive root $ARCHIVE_DIR IS reachable and holds calibration-runs/$ENV_NAME, so this is the reporter on .252 not running. An empty corpus and a dead reporter look the same without this check."
  exit 1
fi
log "progress record present for $DAY: $found"
PREV="$(find "$COHORT_DIR" -name 'dt=*.json' 2>/dev/null | sort | tail -2 | head -1)"
export PREV
DECLARED_HASH="$DECLARED_HASH" DECLARED_TRACK="$DECLARED_TRACK" \
  python3 - "$found" "$DAY" "${PREV:-}" <<'PY'
import json, os, sys
d = json.load(open(sys.argv[1])); day = sys.argv[2]
# Defence in depth for r19 #1: the PATH says which cohort this record claims to be, and the record
# says it too. A hand-made or half-migrated directory can satisfy one without the other, so require
# both to agree with the declaration before believing anything else in the file.
_want_h, _want_t = os.environ.get("DECLARED_HASH",""), os.environ.get("DECLARED_TRACK","")
_coh = (d.get("cohorts") or [{}])[0]
_got_h, _got_t = _coh.get("parameterSetHash") or "", _coh.get("trackFromPush") or ""
# NO WAIVER FOR "UNFROZEN". This used to skip the hash comparison whenever the declared hash was the
# literal UNFROZEN — which is what ships today, so the one configuration in production was the one
# with the check turned off. The reporter always writes the DECLARED hash into cohorts[0], so an
# honest record under an UNFROZEN declaration carries "UNFROZEN" and equality holds; the only thing
# the waiver could ever admit was a record that disagrees, which is precisely what this check is for.
# ABSENCE IS A MISMATCH, NOT A PASS. The predicate also read "_got_h and ..." — so a record that
# carried NO cohort identity at all satisfied it. The check exists to catch a hand-made or
# half-migrated record sitting in the right directory, and a hand-made record is exactly the kind that
# omits a field rather than getting it wrong. If the declaration names a hash, the record has to carry
# the same one; nothing is not the same one (r20).
if (_want_h and _got_h != _want_h) or (_want_t and _got_t != _want_t):
    print("  WARN: this record is for cohort hash=%s trackFromPush=%s, but we declare hash=%s trackFromPush=%s"
          % (_got_h or "<absent>", _got_t or "<absent>", _want_h, _want_t))
    sys.exit(2)
prev_path = sys.argv[3] if len(sys.argv) > 3 else ""
# The report keys sessions "date|hash|lineage" — a bare date lookup found nothing and called a
# perfectly healthy session NOT_IN_CORPUS every single day (r8 #6). Match on the date COMPONENT, and
# take the best status among that date's rows: one lineage landing COMPLETE is the day landing.
# The WORST status on the date, not the best. "One lineage landing COMPLETE is the day landing" was my
# reasoning and it was wrong: A5 says a lost day contributes to NOTHING, and a date with one COMPLETE
# lineage and one CORRUPT one is a date whose population is not knowable. Taking the best let exactly
# that report COMPLETE and exit 0 (r18 #1).
ORDER = ["MISSING", "CORRUPT", "INCOMPLETE", "DISCONTINUITY", "PENDING_SEAL", "COMPLETE"]
rows = [v for k, v in d.get("sessions", {}).items()
        if (v.get("sessionDate") or str(k).split("|")[0]) == day]
st = "NOT_IN_CORPUS"
if rows:
    st = sorted((r.get("archiveStatus", "MISSING") for r in rows),
                key=lambda x: ORDER.index(x) if x in ORDER else -1)[0]
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
