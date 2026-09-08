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
LEDGER_TOPIC="${LEDGER_TOPIC:-context-tape.direction.ledger}"
ROOT="$ARCHIVE_DIR/kafka/$ENV_NAME/$LEDGER_TOPIC"
OUT_ROOT="$ARCHIVE_DIR/calibration-runs/$ENV_NAME"
STAMP="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
TODAY="${REPORT_DATE:-$(TZ=America/New_York date '+%Y-%m-%d')}"
log() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"; }

# The targets are A2's, adopted by A5 as its own. They are not a claim that A4 feeds A2.
T_SESSIONS="${T_SESSIONS:-30}"; T_COHORT="${T_COHORT:-200}"; T_CLASS="${T_CLASS:-50}"; T_CELL="${T_CELL:-50}"

# A5.8: the REQUIRED cell universe, frozen BEFORE validation and declared here rather than inferred.
# Taking a minimum over the cells that happen to APPEAR lets a cell with no calls at all satisfy the
# threshold by being absent — choosing the answer after seeing the data. A cell listed here and never
# observed counts as ZERO, and the classes are the two directions the instrument can call.
REQUIRED_CLASSES="${REQUIRED_CLASSES:-1 -1}"
REQUIRED_CELLS="${REQUIRED_CELLS:-EXHAUSTED|CALL_WALL|POS_GAMMA EXHAUSTED|PUT_WALL|POS_GAMMA EXHAUSTED|CALL_WALL|NEG_GAMMA EXHAUSTED|PUT_WALL|NEG_GAMMA STRONG|CALL_WALL|NEG_GAMMA STRONG|PUT_WALL|NEG_GAMMA EXHAUSTED|NONE|POS_GAMMA EXHAUSTED|NONE|NEG_GAMMA STRONG|NONE|NEG_GAMMA STRONG|NONE|POS_GAMMA}"

[ -d "$ROOT" ] || { log "FATAL: no corpus at $ROOT — the ledger has never been archived for env=$ENV_NAME"; exit 1; }
command -v python3 >/dev/null || { log "FATAL: python3 missing"; exit 1; }

# THE reader lives in oe_corpus_reader.py, next to this script and shared with the A5.8 evaluator.
# Two copies of "is this session COMPLETE" would drift, and nothing would notice which one was wrong.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/oe_corpus_reader.py" ] || { log "FATAL: oe_corpus_reader.py missing beside $0"; exit 1; }

python3 - "$SCRIPT_DIR" "$ROOT" "$OUT_ROOT" "$TODAY" "$STAMP" "$ENV_NAME" "$T_SESSIONS" "$T_COHORT" "$T_CLASS" "$T_CELL" "$REQUIRED_CLASSES" "$REQUIRED_CELLS" <<'PY'
import json, os, sys, hashlib, tempfile

script_dir, root, out_root, today, stamp, env, t_sessions, t_cohort, t_class, t_cell, req_classes, req_cells = sys.argv[1:13]
sys.path.insert(0, script_dir)
import oe_corpus_reader as R

t_sessions, t_cohort, t_class, t_cell = int(t_sessions), int(t_cohort), int(t_class), int(t_cell)
REQUIRED_CLASSES = req_classes.split()
REQUIRED_CELLS = req_cells.split()

read_errors = []
read = R.read_logical(root)
sidecar = R.read_sidecar(root, read_errors)
sessions, seals, calls, outcomes = R.classify_sessions(read, sidecar, today)
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
    tf = c.get("trackFromPush")
    k = (ph, tf)
    b = by_cohort.setdefault(k, {"sessions": set(), "calls": 0, "classes": {}, "cells": {}})
    b["sessions"].add(sd)
    b["calls"] += 1
    cls = str(c.get("predictedSign"))
    b["classes"][cls] = b["classes"].get(cls, 0) + 1
    cell = R.cell_key(c)
    b["cells"][cell] = b["cells"].get(cell, 0) + 1

corpus_version = R.corpus_version(logical)
_cal = R.load_calendar()

# A5.5: DECLARED, not inferred. min(sessions) lets deletion of the earliest weeks redefine the
# expected window as "whatever survived", which is the failure the owed-session check exists to catch.
declared = os.environ.get("CORPUS_START_DATE")
observed = min((v["sessionDate"] for v in sessions.values()), default=today)
corpus_start = declared or observed
if not declared:
    read_errors.append("corpusStartDate is INFERRED from the archive (%s); declare CORPUS_START_DATE "
                       "or a deletion of the earliest sessions silently narrows the expected window" % observed)
have_days = {v["sessionDate"] for v in sessions.values()}
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

reports = []
if not by_cohort:
    reports.append({
        "phase": "CALIBRATION",
        "validationClockStarted": False,
        "note": "the validation clock has NOT started: TRACK_FROM_PUSH is in the future, so no call counts toward the cohort. Calls are being collected so the literals can be chosen.",
        "callsCollected": calib_calls,
        "sessionsComplete": len(complete),
        "thresholdsMet": False, "corpusComplete": False, "readyForEvaluation": False,
        "evaluationDecision": "NOT_RUN",
    })
else:
    for (ph, tf), b in sorted(by_cohort.items()):
        # over the REQUIRED universe, not the observed one: a cell that never appeared is zero, not absent
        min_class = min(b["classes"].get(c, 0) for c in REQUIRED_CLASSES) if REQUIRED_CLASSES else 0
        min_cell = min(b["cells"].get(c, 0) for c in REQUIRED_CELLS) if REQUIRED_CELLS else 0
        empty_cells = [c for c in REQUIRED_CELLS if b["cells"].get(c, 0) == 0]
        thresholds = (len(b["sessions"]) >= t_sessions and b["calls"] >= t_cohort
                      and min_class >= t_class and min_cell >= t_cell)
        # scoped to THIS cohort's window: sessions before TRACK_FROM are not its business
        window = [k for k, v in sessions.items() if tf is None or v["sessionDate"] >= str(tf)[:10]]
        corpus_ok = bool(window) and all(sessions[k]["archiveStatus"] == "COMPLETE" for k in window)
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
            "evaluationDecision": "NOT_RUN",
        })

report = {
    "generatedAt": stamp, "env": env, "reportDate": today,
    "archiveStatusToday": today_status,
    "archivedToday": today_status == "COMPLETE",
    "conflicts": sum(conflicts_by_session.values()), "filesRead": files, "readErrors": read_errors[:20],
    "corpusVersion": corpus_version, "corpusStartDate": corpus_start,
    "calendar": "market_calendar" if _cal is not None else "WEEKDAY_FALLBACK",
    "sessions": {k: v for k, v in sorted(sessions.items())},
    "sessionsMissing": sorted(v["sessionDate"] for v in sessions.values() if v["archiveStatus"] == "MISSING"),
    "cohorts": reports,
    "actionable": False, "slice": "COMMISSIONING_SHADOW",
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
if not r["validationClockStarted"]:
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
