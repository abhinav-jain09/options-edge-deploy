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

# The targets, the required cell universe and the corpus start date all come from the ONE declaration
# below — calibration-targets.env. They used to be defaults in this file, which meant the reporter and
# the evaluator each carried their own copy of what "done" means and either could be changed without
# the other noticing.

[ -d "$ROOT" ] || { log "FATAL: no corpus at $ROOT — the ledger has never been archived for env=$ENV_NAME"; exit 1; }
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
eval "CORPUS_START_DATE=\"\${OE_CAL_CORPUS_START_DATE_${ENV_NAME}:-}\""
eval "DECLARED_HASH=\"\${OE_CAL_PARAMETER_SET_HASH_${ENV_NAME}:-}\""
eval "DECLARED_TRACK_FROM=\"\${OE_CAL_TRACK_FROM_PUSH_${ENV_NAME}:-}\""
export CORPUS_START_DATE DECLARED_HASH DECLARED_TRACK_FROM
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

# A5.6: the version is a PUBLISHED, IMMUTABLE MANIFEST — published by atomic rename to
# corpus/<corpusVersion>/manifest.json and never mutated. A bare hash of the live archive is not a pin:
# it matches again after a deletion, so a calibration could not be re-run against the inputs it used.
manifest = R.build_manifest(read, ledger_topic, env)
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
declared = os.environ.get("CORPUS_START_DATE")
if not declared:
    print("FATAL: corpusStartDate is not declared for env=%s in calibration-targets.env — a start date "
          "inferred from the archive turns a deletion into a narrower window" % env, file=sys.stderr)
    sys.exit(1)
corpus_start = declared
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

# The cohort reported on is the DECLARED one. Reporting whatever cohorts happen to be in the archive
# means a new parameter set publishes the old one's numbers, and the day the hash changes is exactly
# the day the report must say so (r7 #8).
declared_hash = os.environ.get("DECLARED_HASH") or ""
declared_track = os.environ.get("DECLARED_TRACK_FROM") or ""
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

reports = []
if not by_cohort:
    reports.append({
        "phase": "CALIBRATION",
        "validationClockStarted": False,
        "note": "the validation clock has NOT started: TRACK_FROM_PUSH is in the future, so no call counts toward the cohort. Calls are being collected so the literals can be chosen.",
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
            "evaluationDecision": latest_decision(ph, tf),
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
