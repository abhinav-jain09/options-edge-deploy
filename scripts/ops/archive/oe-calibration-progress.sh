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

[ -d "$ROOT" ] || { log "FATAL: no corpus at $ROOT — the ledger has never been archived for env=$ENV_NAME"; exit 1; }
command -v python3 >/dev/null || { log "FATAL: python3 missing"; exit 1; }

python3 - "$ROOT" "$OUT_ROOT" "$TODAY" "$STAMP" "$ENV_NAME" "$T_SESSIONS" "$T_COHORT" "$T_CLASS" "$T_CELL" <<'PY'
import gzip, json, os, sys, glob, hashlib, tempfile

root, out_root, today, stamp, env, t_sessions, t_cohort, t_class, t_cell = sys.argv[1:10]
t_sessions, t_cohort, t_class, t_cell = int(t_sessions), int(t_cohort), int(t_class), int(t_cell)

# ---- read every archived ledger record, collapsing replays by (key, semantic digest) -------------
# A5.3: equal key + equal digest is ONE logical record; equal key + a different digest is a CONFLICT
# and poisons its session. The reader RECOMPUTES the digest from the payload — a digest carried in a
# record is diagnostic only, or a corrupted payload would verify itself.
def canonical(o):
    if isinstance(o, dict):
        return "{" + ",".join('%s:%s' % (json.dumps(k), canonical(v)) for k, v in sorted(o.items())) + "}"
    if isinstance(o, list):
        return "[" + ",".join(canonical(v) for v in o) + "]"
    if o is None:
        return "null"
    if isinstance(o, bool):
        return "true" if o else "false"
    if isinstance(o, str):
        return json.dumps(o)
    return json.dumps(str(o))

logical, conflicts, files = {}, 0, 0
for f in sorted(glob.glob(os.path.join(root, "dt=*", "*.jsonl.gz"))):
    files += 1
    try:
        with gzip.open(f, "rt", errors="replace") as fh:
            for line in fh:
                i = line.find("{")
                if i < 0:
                    continue
                try:
                    rec = json.loads(line[i:])
                except Exception:
                    continue
                if "kind" not in rec:
                    continue
                body = {k: v for k, v in rec.items() if k not in ("ts", "publishedAtMs", "runId", "semanticDigest")}
                dig = hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()
                key = (rec.get("parameterSetHash"), rec.get("sessionLineageId"), rec.get("kind"),
                       rec.get("callId"), rec.get("horizon"), rec.get("sessionDate"))
                prev = logical.get(key)
                if prev is None:
                    logical[key] = (dig, rec)
                elif prev[0] != dig:
                    conflicts += 1
    except Exception:
        pass

seals, calls, outcomes = {}, [], []
for (_h, _l, kind, _c, _hz, _sd), (_d, rec) in logical.items():
    if kind == "seal":
        seals[(rec.get("sessionDate"), rec.get("parameterSetHash"))] = rec
    elif kind == "call":
        calls.append(rec)
    elif kind == "outcome":
        outcomes.append(rec)

# ---- per-session status. Only a COMPLETE session enters any counter (A5.7) ------------------------
sessions = {}
for (sd, ph), seal in seals.items():
    got_calls = sum(1 for c in calls if c.get("sessionDate") == sd and c.get("parameterSetHash") == ph)
    got_out = sum(1 for o in outcomes if o.get("sessionDate") == sd and o.get("parameterSetHash") == ph)
    want_calls = int(seal.get("logicalCallCount", -1))
    want_out = int(seal.get("logicalOutcomeCount", -1))
    if seal.get("conflicts", 0) or conflicts:
        status, why = "CORRUPT", "conflicting records for one key"
    elif seal.get("discontinuities"):
        status, why = "DISCONTINUITY", ";".join(seal.get("discontinuities"))
    elif got_calls != want_calls or got_out != want_out:
        status, why = "INCOMPLETE", "archived %d/%d calls, %d/%d outcomes" % (got_calls, want_calls, got_out, want_out)
    elif want_out != 3 * want_calls:
        status, why = "INCOMPLETE", "the seal itself is not 3 horizons per call"
    else:
        status, why = "COMPLETE", ""
    sessions[sd] = {"parameterSetHash": ph, "archiveStatus": status, "reason": why,
                    "calls": got_calls, "outcomes": got_out,
                    "phase": (["CALIBRATION", "VALIDATION"][0]), "trackFromPush": seal.get("trackFromPush"),
                    "attritionRows": len(seal.get("attrition", []) or [])}

# a day with records but NO seal has not finished; a day with neither is not in the corpus at all
for c in calls:
    sd = c.get("sessionDate")
    if sd not in sessions:
        sessions[sd] = {"parameterSetHash": c.get("parameterSetHash"), "archiveStatus": "PENDING_SEAL",
                        "reason": "records archived, no seal yet", "calls": 0, "outcomes": 0,
                        "phase": "CALIBRATION", "trackFromPush": None, "attritionRows": 0}

# ---- the counters, per (hash, trackFrom). Only VALIDATION members count -----------------------
by_cohort = {}
for c in calls:
    sd, ph = c.get("sessionDate"), c.get("parameterSetHash")
    if sessions.get(sd, {}).get("archiveStatus") != "COMPLETE":
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
    cell = "%s|%s|%s" % (c.get("enteredState"), (c.get("node") or {}).get("roles") or "NONE", c.get("regime"))
    b["cells"][cell] = b["cells"].get(cell, 0) + 1

complete = [s for s in sessions.values() if s["archiveStatus"] == "COMPLETE"]
calib_calls = sum(s["calls"] for s in complete)
today_status = sessions.get(today, {}).get("archiveStatus", "NOT_EXPECTED")

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
        min_class = min(b["classes"].values()) if b["classes"] else 0
        min_cell = min(b["cells"].values()) if b["cells"] else 0
        thresholds = (len(b["sessions"]) >= t_sessions and b["calls"] >= t_cohort
                      and min_class >= t_class and min_cell >= t_cell)
        corpus_ok = all(s["archiveStatus"] == "COMPLETE" for s in sessions.values())
        reports.append({
            "phase": "VALIDATION", "validationClockStarted": True,
            "parameterSetHash": ph, "trackFromPush": tf,
            "sessions": {"have": len(b["sessions"]), "target": t_sessions},
            "cohort": {"have": b["calls"], "target": t_cohort},
            "perClass": {"have": min_class, "target": t_class},
            "perCell": {"have": min_cell, "target": t_cell},
            "thresholdsMet": thresholds, "corpusComplete": corpus_ok,
            "readyForEvaluation": thresholds and corpus_ok,
            "evaluationDecision": "NOT_RUN",
        })

report = {
    "generatedAt": stamp, "env": env, "reportDate": today,
    "archiveStatusToday": today_status,
    "archivedToday": today_status == "COMPLETE",
    "conflicts": conflicts, "filesRead": files,
    "sessions": {k: v for k, v in sorted(sessions.items())},
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
rc=$?
[ $rc -eq 0 ] || { log "FATAL: progress report failed (rc=$rc)"; exit $rc; }
log "calibration progress written under $OUT_ROOT"
