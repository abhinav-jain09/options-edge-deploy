#!/usr/bin/env bash
# oe-calibration-progress.sh — A5.7. Answers ONE question, every trading day: how far is the Candle
# Direction calibration, and did today's session actually land in the corpus?
#
# It reads the NAS corpus, never the live service. The service is per-session by construction and
# knows only today; the corpus is the only thing that can answer "how far".
#
# The report leads with WHICH CLOCK IS RUNNING, because there are two and only one of them counts:
#   CALIBRATION  TRACK_FROM_PUSH is in the future, so no call counts toward the cohort yet. Calls are
#                being collected so the literals can be chosen. This is where the work is today.
#   VALIDATION   the literals are frozen and the cohort is filling against A2's four thresholds.
#
# "Done" is deliberately FOUR facts, not one. thresholdsMet says the counts are reached; corpusComplete
# says every session in the window sealed and reconciled; readyForEvaluation is both; and
# evaluationDecision is the only one that is a claim about the instrument being any good — and it is
# NOT_RUN until a PushValidationArtifact says otherwise. Nothing here sets validationStatus, and
# nothing here is actionable.
set -uo pipefail

ENV_NAME="${ENV:-prod}"
ARCHIVE_DIR="${ARCHIVE_DIR:-/mnt/nas/optionsedge}"
# Declared, never supplied. A caller who can name the topic can point the whole corpus at one they
# prepared — the same move every other preregistered value already refuses (r14 #1). The declaration is
# sourced below; this only records that the name is NOT the caller's to give.
if [ -n "${LEDGER_TOPIC:-}" ]; then
  echo "FATAL: LEDGER_TOPIC is declared in calibration-targets.env and cannot be supplied by the caller" >&2
  exit 1
fi
# ROOT is set AFTER the declaration is sourced, because the topic comes from it.
OUT_ROOT="$ARCHIVE_DIR/calibration-runs/$ENV_NAME"
STAMP="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
TODAY="${REPORT_DATE:-$(TZ=America/New_York date '+%Y-%m-%d')}"
log() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"; }

# The targets, the required cell universe and the corpus start date all come from the ONE declaration
# below — calibration-targets.env. They used to be defaults in this file, which meant the reporter and
# the evaluator each carried their own copy of what "done" means and either could be changed without
# the other noticing.

command -v python3 >/dev/null || { log "FATAL: python3 missing"; exit 1; }

# THE reader lives in oe_corpus_reader.py, next to this script and shared with the A5.8 evaluator.
# Two copies of "is this session COMPLETE" would drift, and nothing would notice which one was wrong.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/oe_corpus_reader.py" ] || { log "FATAL: oe_corpus_reader.py missing beside $0"; exit 1; }

# A5.7: the targets come from a declaration neither the reporter nor the evaluator can lose, and which
# is derived neither from oe-topics.env nor from what happens to be in the archive — those are the two
# things that go quiet together. Without it a dropped ledger topic would report nothing at all instead of
# reporting a target whose corpus is MISSING.
# Beside this script, with no way to point it elsewhere — same reasoning as the evaluator (r7 #1): a
# reporter that can be handed another declaration can be made to report against another cohort.
TARGETS="$SCRIPT_DIR/calibration-targets.env"
[ -f "$TARGETS" ] || { log "FATAL: no calibration target declaration at $TARGETS"; exit 1; }
# shellcheck source=/dev/null
. "$TARGETS"
LEDGER_TOPIC="${OE_CAL_TOPIC:?calibration-targets.env declares no OE_CAL_TOPIC}"
ROOT="$ARCHIVE_DIR/kafka/$ENV_NAME/$LEDGER_TOPIC"
[ -d "$ROOT" ] || { log "FATAL: no corpus at $ROOT — the ledger has never been archived for env=$ENV_NAME"; exit 1; }
eval "CORPUS_START_DATE=\"\${OE_CAL_CORPUS_START_DATE_${ENV_NAME}:-}\""
eval "DECLARED_HASH=\"\${OE_CAL_PARAMETER_SET_HASH_${ENV_NAME}:-}\""
eval "DECLARED_TRACK_FROM=\"\${OE_CAL_TRACK_FROM_PUSH_${ENV_NAME}:-}\""
eval "DECLARED_STAMP=\"\${OE_CAL_SEMANTIC_STAMP_${ENV_NAME}:-}\""
export CORPUS_START_DATE DECLARED_HASH DECLARED_TRACK_FROM DECLARED_STAMP

# A5.6: the manifest is built under a SNAPSHOT LOCK that excludes a concurrent archive run, or a
# version can straddle a half-written date — the reporter is scheduled at 20:30 and the seal archive
# pass at 20:45, so the two genuinely can meet (r8 #7). This is the archiver's own TOPIC lock, taken on
# the same key it uses, so the exclusion is real rather than a different lock with a similar name.
# The key must be the archiver's own, character for character, or this "lock" excludes nothing at all
# and only looks like it does. oe-archive-kafka.sh builds it as
#   /tmp/oe-archive-kafka.$ENV.$_dir_key.t-<topic with anything outside [A-Za-z0-9._-] as _>.lock
_dir_key="$(printf '%s' "$ARCHIVE_DIR" | cksum | cut -d' ' -f1)"
_topic_key="$(printf '%s' "$LEDGER_TOPIC" | tr -c 'A-Za-z0-9._-' '_')"
SNAPSHOT_LOCK="/tmp/oe-archive-kafka.$ENV_NAME.$_dir_key.t-$_topic_key.lock"
if command -v flock >/dev/null 2>&1; then
  exec 9>"$SNAPSHOT_LOCK" || { log "FATAL: cannot open the snapshot lock $SNAPSHOT_LOCK"; exit 1; }
  # Wait rather than skip: a progress run that quietly does not run is the failure the watchdog exists
  # to catch, and 300 s comfortably outlasts a ledger archive pass.
  if ! flock -w 300 9; then
    log "FATAL: an archive run held $SNAPSHOT_LOCK for 300s — refusing to mint a version that could straddle a half-written date"
    exit 1
  fi
else
  log "WARNING: no flock on this host — the manifest is being minted WITHOUT the archive snapshot lock"
fi

T_SESSIONS="${OE_CAL_T_SESSIONS:-30}"; T_COHORT="${OE_CAL_T_COHORT:-200}"
T_CLASS="${OE_CAL_T_CLASS:-50}";       T_CELL="${OE_CAL_T_CELL:-50}"
REQUIRED_CLASSES="${OE_CAL_REQUIRED_CLASSES:-}"; REQUIRED_CELLS="${OE_CAL_REQUIRED_CELLS:-}"
[ -n "$REQUIRED_CELLS" ] || { log "FATAL: $TARGETS declares no required cell universe — a per-cell count over an empty universe is not a measurement"; exit 1; }
[ -n "$CORPUS_START_DATE" ] || { log "FATAL: $TARGETS declares no corpusStartDate for env=$ENV_NAME"; exit 1; }

python3 - "$SCRIPT_DIR" "$ROOT" "$OUT_ROOT" "$TODAY" "$STAMP" "$ENV_NAME" "$T_SESSIONS" "$T_COHORT" "$T_CLASS" "$T_CELL" "$REQUIRED_CLASSES" "$REQUIRED_CELLS" "$LEDGER_TOPIC" <<'PY'
import json, os, sys, hashlib, tempfile

(script_dir, root, out_root, today, stamp, env, t_sessions, t_cohort, t_class, t_cell,
 req_classes, req_cells, ledger_topic) = sys.argv[1:14]
sys.path.insert(0, script_dir)
import oe_corpus_reader as R

t_sessions, t_cohort, t_class, t_cell = int(t_sessions), int(t_cohort), int(t_class), int(t_cell)
REQUIRED_CLASSES = req_classes.split()
REQUIRED_CELLS = req_cells.split()

read_errors = []
declared_hash = os.environ.get("DECLARED_HASH") or ""
declared_track = os.environ.get("DECLARED_TRACK_FROM") or ""
read = R.read_logical(root)
sidecar = R.read_sidecar(root, read_errors)
sessions, seals, calls, outcomes = R.classify_sessions(read, sidecar, today)
# The reader's per-file errors were never merged here, so an unreadable archived file outside an
# otherwise complete session left corpusComplete true while the evaluator rejected (r11 #3).
read_errors += read.get("readErrors", [])
logical, files = read["logical"], read["files"]
conflicts_by_session = read["conflictsBySession"]

# ---- the counters, per (hash, trackFrom). Only VALIDATION members count -----------------------
by_cohort = {}
for c in calls:
    sd, ph = c.get("sessionDate"), c.get("parameterSetHash")
    k = "%s|%s|%s" % (sd, ph, c.get("sessionLineageId"))
    if sessions.get(k, {}).get("archiveStatus") != "COMPLETE":
        continue                                   # a lost or unfinished day contributes to NOTHING
    if c.get("phaseAtCall") != "VALIDATION":
        continue                                   # the validation clock has not started for it
    # A5.7: the exact SEMANTIC STAMP as well as the hash. The hash deliberately excludes the stamp, so
    # two parameter sets whose literals differ would otherwise pool into one cohort.
    declared_stamp = os.environ.get("DECLARED_STAMP") or ""
    if declared_stamp and c.get("semanticStamp") != declared_stamp:
        continue
    tf = c.get("trackFromPush")
    k = (ph, tf)
    b = by_cohort.setdefault(k, {"sessions": set(), "calls": 0, "classes": {}, "cells": {}})
    b["sessions"].add(sd)
    b["calls"] += 1
    cls = str(c.get("predictedSign"))
    b["classes"][cls] = b["classes"].get(cls, 0) + 1
    cell = R.cell_key(c)
    b["cells"][cell] = b["cells"].get(cell, 0) + 1

# A5.6: the version is a PUBLISHED, IMMUTABLE MANIFEST — published by atomic rename to
# corpus/<corpusVersion>/manifest.json and never mutated. A bare hash of the live archive is not a pin:
# it matches again after a deletion, so a calibration could not be re-run against the inputs it used.
declared = os.environ.get("CORPUS_START_DATE")
# The most recently published manifest, so entries it already names keep the generation it recorded
# rather than being relabelled with today's (r10 #2).
_prev = None
try:
    import glob as _pg
    _cands = sorted(_pg.glob(os.path.join(out_root, "corpus", "*", "manifest.json")), key=os.path.getmtime)
    if _cands:
        _prev = json.load(open(_cands[-1]))
except Exception:
    read_errors.append("the previous manifest could not be read — generations cannot be carried forward")
manifest = R.build_manifest(read, ledger_topic, env, corpus_start=declared,
                            cal=R.load_calendar(), previous=_prev)
corpus_version, manifest_path, minted = R.publish_manifest(out_root, manifest)
_cal = R.load_calendar()
if _cal is None:
    print("FATAL: no market calendar — owed trading days cannot be enumerated and a holiday would be "
          "reported as a permanent MISSING", file=sys.stderr)
    sys.exit(1)

# A5.5: DECLARED, not inferred. min(sessions) lets deletion of the earliest weeks redefine the
# expected window as "whatever survived", which is the failure the owed-session check exists to catch.
# A5.5/A5.7: DECLARED, never inferred. Falling back to min(sessions) lets a deletion of the earliest
# weeks redefine the expected window as "whatever survived" — the exact failure the owed-session check
# exists to catch, and a warning in readErrors was not enough to stop it (r6 #9). The declaration is
# calibration-targets.env, installed beside this script, and its absence is fatal rather than a default.
if not declared:
    print("FATAL: corpusStartDate is not declared for env=%s in calibration-targets.env — a start date "
          "inferred from the archive turns a deletion into a narrower window" % env, file=sys.stderr)
    sys.exit(1)
corpus_start = declared
# Scoped to the DECLARED cohort, exactly as the evaluator scopes it: a zero-call COMPLETE seal under a
# foreign hash used to satisfy an owed day here too, so a cohort holding one real session reported
# corpusComplete (r8 #4). A session belongs to a cohort or it belongs to nothing.
def cohort_days(ph, tf):
    # The SEMANTIC STAMP as well (r16 #1): a session sealed under other literals is not this cohort's,
    # and letting it satisfy an owed day is the same hole the cohort filter already closes.
    stamp = os.environ.get("DECLARED_STAMP") or ""
    return {v["sessionDate"] for v in sessions.values()
            if v["archiveStatus"] == "COMPLETE"
            and (ph in (None, "", "UNFROZEN") or v.get("parameterSetHash") == ph)
            and (tf in (None, "", "UNFROZEN") or v.get("trackFromPush") == tf)
            and (not stamp or v.get("semanticStamp") == stamp)}

have_days = cohort_days(declared_hash, declared_track)
for day in R.owed(corpus_start, today, _cal):
    if day not in have_days:
        sessions[day] = {"sessionDate": day, "sessionLineageId": None,
                         "parameterSetHash": None, "archiveStatus": "MISSING",
                         "reason": "a trading day the corpus owes and does not have",
                         "calls": 0, "outcomes": 0, "phase": None, "trackFromPush": None,
                         "attritionRows": 0}

complete = [s for s in sessions.values() if s["archiveStatus"] == "COMPLETE"]
calib_calls = sum(s["calls"] for s in complete)
today_rows = [v for v in sessions.values() if v["sessionDate"] == today]
today_status = today_rows[0]["archiveStatus"] if today_rows else "NOT_EXPECTED"

# The cohort reported on is the DECLARED one. Reporting whatever cohorts happen to be in the archive
# means a new parameter set publishes the old one's numbers, and the day the hash changes is exactly
# the day the report must say so (r7 #8).
if declared_hash and declared_hash != "UNFROZEN":
    by_cohort = {k: v for k, v in by_cohort.items()
                 if k[0] == declared_hash and (not declared_track or k[1] == declared_track)}

# A5.7: evaluationDecision is the ONE of the four facts that is a claim about the instrument being any
# good, and it comes from a PushValidationArtifact — reading the latest one rather than hardcoding
# NOT_RUN, which made an artifact's verdict invisible here forever (r7 #8).
def latest_decision(ph, tf):
    import glob as _g
    best, decision = None, "NOT_RUN"
    key = ph if ph else "*"
    pat = os.path.join(out_root, key, str(tf or "*").replace(":", "").replace("/", ""), "artifacts", "*.json")
    for f in _g.glob(pat):
        try:
            a = json.load(open(f))
        except Exception:
            read_errors.append("artifact unreadable: %s" % os.path.basename(f))
            continue
        if a.get("corpusVersion") != corpus_version:
            continue          # an artifact about ANOTHER corpus says nothing about this one
        at = a.get("generatedAt") or ""
        if best is None or at > best:
            best, decision = at, a.get("decision", "NOT_RUN")
    return decision

# A5.7: WHICH CLOCK IS RUNNING is read from the preregistration, not inferred from whether calls
# happened to arrive. Inferring it meant a frozen TRACK_FROM_PUSH with 32 complete but QUIET sessions
# reported phase=CALIBRATION and validationClockStarted=false — hiding a validation cohort that is not
# filling, which is exactly the thing this report exists to say out loud (r12 #7).
clock_started = False
if declared_track and declared_track != "UNFROZEN":
    try:
        clock_started = str(declared_track)[:10] <= today
    except Exception:
        clock_started = False

reports = []
if not by_cohort:
    reports.append({
        "phase": "VALIDATION" if clock_started else "CALIBRATION",
        "validationClockStarted": clock_started,
        "parameterSetHash": declared_hash or None, "trackFromPush": declared_track or None,
        "note": ("the validation clock HAS started (TRACK_FROM_PUSH=%s) and this cohort has NO qualifying "
                 "calls — it is not filling" % declared_track) if clock_started else
                "the validation clock has NOT started: TRACK_FROM_PUSH is in the future, so no call counts toward the cohort. Calls are being collected so the literals can be chosen.",
        "callsCollected": calib_calls,
        "sessionsComplete": len(complete),
        "thresholdsMet": False, "corpusComplete": False, "readyForEvaluation": False,
        "evaluationDecision": latest_decision(declared_hash, declared_track),
    })
else:
    for (ph, tf), b in sorted(by_cohort.items()):
        # over the REQUIRED universe, not the observed one: a cell that never appeared is zero, not absent
        min_class = min(b["classes"].get(c, 0) for c in REQUIRED_CLASSES) if REQUIRED_CLASSES else 0
        min_cell = min(b["cells"].get(c, 0) for c in REQUIRED_CELLS) if REQUIRED_CELLS else 0
        empty_cells = [c for c in REQUIRED_CELLS if b["cells"].get(c, 0) == 0]
        thresholds = (len(b["sessions"]) >= t_sessions and b["calls"] >= t_cohort
                      and min_class >= t_class and min_cell >= t_cell)
        # Scoped to THIS cohort in BOTH directions: sessions before TRACK_FROM are not its business, and
        # neither are sessions belonging to another parameter set (r8 #4). Every owed day in the window
        # must be present AS THIS COHORT'S, or the corpus is not complete for it.
        owed_here = [d for d in R.owed(max(corpus_start, str(tf)[:10]) if tf else corpus_start, today, _cal)]
        mine = cohort_days(ph, tf)
        # A conflict is counted in the record but was not allowed to affect corpusComplete, so the
        # reporter could call a corpus complete on the same day the evaluator called it NOT_EVALUABLE.
        # The two must not be able to disagree about COMPLETE — that is the whole reason they share a
        # reader.
        # ONE predicate, in the shared reader, for both halves (r11 #3). Every round of review found
        # another input this side had and the other did not — conflicts, then read errors, then a
        # session that graded nothing — because there were two predicates over one reader.
        defects = R.corpus_defects(read, sessions, seals, cohort_days=mine | set(owed_here))
        corpus_ok = (bool(owed_here) and all(d in mine for d in owed_here)
                     and not defects and not read_errors)
        reports.append({
            "phase": "VALIDATION", "validationClockStarted": True,
            "parameterSetHash": ph, "trackFromPush": tf,
            "sessions": {"have": len(b["sessions"]), "target": t_sessions},
            "cohort": {"have": b["calls"], "target": t_cohort},
            "perClass": {"have": min_class, "target": t_class},
            "perCell": {"have": min_cell, "target": t_cell, "requiredCells": len(REQUIRED_CELLS),
                        "cellsWithNoCalls": empty_cells},
            "thresholdsMet": thresholds, "corpusComplete": corpus_ok,
            "readyForEvaluation": thresholds and corpus_ok,
            "evaluationDecision": latest_decision(ph, tf),
        })

# A5.7 requires both, and their ABSENCE is what would make the counts a lie: hashChangedOn because a
# semantic change restarts the cohort and the report must say so on the day it happens, and attrition
# because a cohort that fills while the instrument refuses most ticks is not the cohort anyone thinks
# it is (r12 #8).
hash_changed_on = {}
for c in calls:
    ph_, sd_ = c.get("parameterSetHash"), c.get("sessionDate")
    if ph_ and sd_ and (ph_ not in hash_changed_on or sd_ < hash_changed_on[ph_]):
        hash_changed_on[ph_] = sd_

attrition_report = {}
for (sd_, ph_), seal in sorted(seals.items()):
    rows = seal.get("attrition") or []
    rate = R.session_refusal_rate(seal)
    attrition_report["%s|%s" % (sd_, ph_)] = {
        "refusalRate": None if rate is None else round(rate, 6),
        "gradedTotal": sum(r.get("graded", 0) for r in rows if isinstance(r.get("graded"), (int, float))),
        "byEtHour": {str(r.get("etHour")): {k: v for k, v in r.items()
                                            if k not in ("sessionDate", "etHour")} for r in rows},
        "shapeValid": not R.attrition_violations(seal),
    }

report = {
    "generatedAt": stamp, "env": env, "reportDate": today,
    "hashChangedOn": hash_changed_on, "attrition": attrition_report,
    "archiveStatusToday": today_status,
    "archivedToday": today_status == "COMPLETE",
    "conflicts": sum(conflicts_by_session.values()), "filesRead": files, "readErrors": read_errors[:20],
    "corpusVersion": corpus_version, "corpusStartDate": corpus_start,
    "calendar": "market_calendar" if _cal is not None else "WEEKDAY_FALLBACK",
    "sessions": {k: v for k, v in sorted(sessions.items())},
    "sessionsMissing": sorted(v["sessionDate"] for v in sessions.values() if v["archiveStatus"] == "MISSING"),
    "cohorts": reports,
    # All FOUR A4 shadow labels, on the record that is SERVED as well as the one on disk: two of them
    # were missing, and a consumer that checks only what it is given would have seen an unlabelled
    # record (r7 #8).
    "actionable": False, "slice": "COMMISSIONING_SHADOW",
    "evidenceBasis": "NONE_SHADOW", "validationStatus": "FORWARD_UNMEASURED",
    "manifestPath": manifest_path, "manifestMinted": minted,
}

for r in reports:
    key = r.get("parameterSetHash", "calibration")
    tf = (r.get("trackFromPush") or "unfrozen").replace(":", "").replace("/", "")
    d = os.path.join(out_root, key, tf, "progress")
    os.makedirs(d, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile("w", dir=d, delete=False, suffix=".tmp")
    json.dump(report, tmp, indent=1, sort_keys=True)
    tmp.close()
    os.replace(tmp.name, os.path.join(d, "dt=%s.json" % today))     # atomic publication (A5.6)

# one human line, which is the part anyone actually reads
r = reports[0]
if r["validationClockStarted"] and "sessions" not in r:
    # the clock is running and NOTHING qualifies — the case the phase fix exists to surface, and the
    # one the counts-based line cannot print because there are no counts
    print("VALIDATION %s — the clock has started (TRACK_FROM_PUSH=%s) and this cohort has NO qualifying "
          "calls. today=%s" % ((r.get("parameterSetHash") or "?")[:12], r.get("trackFromPush"), today_status))
elif not r["validationClockStarted"]:
    print("CALIBRATION — validation clock NOT started. %d calls over %d complete sessions. today=%s"
          % (r["callsCollected"], r["sessionsComplete"], today_status))
else:
    print("VALIDATION %s — sessions %d/%d, cohort %d/%d, per-class %d/%d, per-cell %d/%d. "
          "thresholdsMet=%s corpusComplete=%s readyForEvaluation=%s evaluation=%s today=%s"
          % (r["parameterSetHash"][:12], r["sessions"]["have"], r["sessions"]["target"],
             r["cohort"]["have"], r["cohort"]["target"], r["perClass"]["have"], r["perClass"]["target"],
             r["perCell"]["have"], r["perCell"]["target"], r["thresholdsMet"], r["corpusComplete"],
             r["readyForEvaluation"], r["evaluationDecision"], today_status))
PY
rc=$?
[ $rc -eq 0 ] || { log "FATAL: progress report failed (rc=$rc)"; exit $rc; }
log "calibration progress written under $OUT_ROOT"

# A5.7: the SAME record is published to context-tape.direction.progress, because the panel has no other
# way to see it — the gateway today knows only push, alert and scorecard. The NAS copy is the durable
# one and is written first; this is the delivery, and its failure is loud rather than silent, since a
# reporter that quietly stops publishing looks exactly like a corpus with nothing to say.
#
# BOOTSTRAP unset means "NAS only", which is the correct posture off the broker host — but it is SAID
# rather than assumed, so nobody reads the silence as a successful publish.
PROGRESS_TOPIC="${PROGRESS_TOPIC:-context-tape.direction.progress}"
if [ -z "${BOOTSTRAP:-}" ]; then
  log "no BOOTSTRAP: the progress record is on the NAS only, not published to $PROGRESS_TOPIC"
  exit 0
fi
latest="$(find "$OUT_ROOT" -name "dt=$TODAY.json" -print 2>/dev/null | sort | tail -1)"
[ -n "$latest" ] || { log "FATAL: no progress record for $TODAY to publish — the reader claimed success and wrote nothing"; exit 1; }
# the same KAFKA_BIN the archiver uses on this host — one location, not a PATH lookup that differs
# between an interactive shell and cron
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/current/bin}"
producer="$KAFKA_BIN/kafka-console-producer.sh"
[ -x "$producer" ] || producer="$(command -v kafka-console-producer.sh || command -v kafka-console-producer || true)"
[ -n "$producer" ] && [ -x "$producer" ] || { log "FATAL: BOOTSTRAP is set but no kafka-console-producer under $KAFKA_BIN or on PATH — refusing to report success without publishing"; exit 1; }
# one line, keyed by env so the panel reads the latest record per environment
if ! python3 -c "
import json, sys
d = json.load(open(sys.argv[1]))
sys.stdout.write(sys.argv[2] + '\t' + json.dumps(d, separators=(',', ':'), sort_keys=True) + '\n')" "$latest" "$ENV_NAME" \
     | "$producer" --bootstrap-server "$BOOTSTRAP" --topic "$PROGRESS_TOPIC" \
                   --property "parse.key=true" --property "key.separator=	" >/dev/null 2>&1; then
  log "FATAL: publishing the progress record to $PROGRESS_TOPIC failed"
  exit 1
fi
log "progress record published to $PROGRESS_TOPIC"
