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

python3 - "$ROOT" "$OUT_ROOT" "$TODAY" "$STAMP" "$ENV_NAME" "$T_SESSIONS" "$T_COHORT" "$T_CLASS" "$T_CELL" "$REQUIRED_CLASSES" "$REQUIRED_CELLS" <<'PY'
import gzip, json, os, sys, glob, hashlib, tempfile, re, datetime

root, out_root, today, stamp, env, t_sessions, t_cohort, t_class, t_cell, req_classes, req_cells = sys.argv[1:12]
t_sessions, t_cohort, t_class, t_cell = int(t_sessions), int(t_cohort), int(t_class), int(t_cell)
REQUIRED_CLASSES = req_classes.split()
REQUIRED_CELLS = req_cells.split()

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

# A read error is NOT a quiet day. Swallowing it would let a truncated or unreadable archive report a
# smaller population as complete — the exact failure this reader exists to catch.
logical, files = {}, 0
conflicts_by_session, read_errors_by_session, bad_keys = {}, {}, {}
read_errors = []                                    # file-level: attributed to the dates in the file
for f in sorted(glob.glob(os.path.join(root, "dt=*", "*.jsonl.gz"))):
    files += 1
    try:
        with gzip.open(f, "rt") as fh:
            for lineno, line in enumerate(fh, 1):
                i = line.find("{")
                if i < 0:
                    continue
                try:
                    rec = json.loads(line[i:])
                except Exception:
                    dt = re.search(r"dt=(\d{4}-\d{2}-\d{2})", f)
                    read_errors_by_session.setdefault(dt.group(1) if dt else "?", []).append(
                        "%s:%d unparseable" % (os.path.basename(f), lineno))
                    continue
                if "kind" not in rec:
                    continue
                # The reader RECOMPUTES the digest from the payload. A digest carried in the record is
                # diagnostic only — a corrupted payload with its own matching digest would verify itself.
                body = {k: v for k, v in rec.items() if k not in ("ts", "publishedAtMs", "runId", "semanticDigest")}
                dig = hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()
                # the PHYSICAL key, as the producer wrote it — not a tuple the reader invents
                if rec.get("kind") == "seal":
                    pkey = "%s|%s|%s" % (rec.get("sessionDate"), rec.get("parameterSetHash"), rec.get("sessionLineageId"))
                else:
                    lid = rec.get("callId") if rec.get("kind") == "call" else "%s|%s" % (rec.get("callId"), rec.get("horizon"))
                    pkey = "%s|%s|%s" % (rec.get("parameterSetHash"), rec.get("sessionLineageId"), lid)
                prefix = line[:i]
                m = re.search(r"Offset:(\d+)", prefix)
                off = int(m.group(1)) if m else None
                # the ACTUAL Kafka key, not one derived from the value: a record written under a
                # different key than its payload implies is exactly the substitution to catch
                km = re.search(r"Offset:\d+\s+(\S+)\s*$", prefix) or re.search(r"\s(\S+)\s*$", prefix)
                actual_key = km.group(1) if km else None
                sd_of = rec.get("sessionDate")
                if actual_key is not None and actual_key != pkey:
                    bad_keys.setdefault(sd_of, []).append(actual_key)
                prev = logical.get(pkey)
                if prev is None:
                    logical[pkey] = (dig, rec, off)
                elif prev[0] != dig:
                    # same key, different content: this session is poisoned, and only this one
                    conflicts_by_session[sd_of] = conflicts_by_session.get(sd_of, 0) + 1
                elif off is not None and (prev[2] is None or off < prev[2]):
                    logical[pkey] = (dig, rec, off)      # A5.4: keep the LOWEST physical offset
    except Exception as e:
        dt = re.search(r"dt=(\d{4}-\d{2}-\d{2})", f)
        read_errors_by_session.setdefault(dt.group(1) if dt else "?", []).append(
            "%s unreadable: %s" % (os.path.basename(f), e.__class__.__name__))

seals, calls, outcomes = {}, [], []
for pkey, (_d, rec, _off) in logical.items():
    kind = rec.get("kind")
    if kind == "seal":
        seals[(rec.get("sessionDate"), rec.get("parameterSetHash"))] = rec
    elif kind == "call":
        calls.append(rec)
    elif kind == "outcome":
        outcomes.append(rec)

# ---- per-session status. Only a COMPLETE session enters any counter (A5.7) ------------------------
# The archiver's own discontinuity sidecar: a log reset it saw poisons every session it spans.
sidecar = []
for f in glob.glob(os.path.join(os.path.dirname(root.rstrip("/")), "_manifest", "*.discontinuities.jsonl")) + \
         glob.glob(os.path.join(root, "..", "_manifest", "*.discontinuities.jsonl")):
    try:
        for line in open(f):
            sidecar.append(json.loads(line))
    except Exception:
        read_errors.append("discontinuity sidecar unreadable")

def chain(domain, recs):
    """A5.4's chain, recomputed from the archive in lowest-offset order. This is the PROOF: counts
    alone accept a missing record paired with a substituted one, and a wrong seal would pass."""
    d = b"\x00" * 32
    for _off, pkey, dig in sorted(recs, key=lambda r: (r[0] if r[0] is not None else 0, r[1])):
        k = pkey.encode("utf-8")
        raw = bytes.fromhex(dig)
        d = hashlib.sha256(d + bytes([domain]) + len(k).to_bytes(4, "big") + k
                           + len(raw).to_bytes(4, "big") + raw).digest()
    return d.hex()

sessions = {}
for (sd, ph), seal in seals.items():
    lin = seal.get("sessionLineageId")
    # LINEAGE-SCOPED: records of another lineage of the same session are not this seal's population
    mine_c = [(o, k, dg) for k, (dg, r, o) in logical.items()
              if r.get("kind") == "call" and r.get("sessionDate") == sd
              and r.get("parameterSetHash") == ph and r.get("sessionLineageId") == lin]
    mine_o = [(o, k, dg) for k, (dg, r, o) in logical.items()
              if r.get("kind") == "outcome" and r.get("sessionDate") == sd
              and r.get("parameterSetHash") == ph and r.get("sessionLineageId") == lin]
    got_calls, got_out = len(mine_c), len(mine_o)
    want_calls = int(seal.get("logicalCallCount", -1))
    want_out = int(seal.get("logicalOutcomeCount", -1))
    call_ids = {r.get("callId") for _dg, r, _o in logical.values()
                if r.get("kind") == "call" and r.get("sessionDate") == sd and r.get("sessionLineageId") == lin}
    orphans = [r.get("callId") for _dg, r, _o in logical.values()
               if r.get("kind") == "outcome" and r.get("sessionDate") == sd
               and r.get("sessionLineageId") == lin and r.get("callId") not in call_ids]
    horizons = {}
    for _dg, r, _o in logical.values():
        if r.get("kind") == "outcome" and r.get("sessionDate") == sd and r.get("sessionLineageId") == lin:
            horizons.setdefault(r.get("callId"), set()).add(r.get("horizon"))
    bad_horizons = [c for c in call_ids if horizons.get(c, set()) != {"H3", "H5", "H15"}]
    # ONLY a discontinuity dated for THIS session poisons it — `or d.get("topic")` matched every entry
    spans = [d for d in sidecar if d.get("dt") == sd]
    errs = read_errors_by_session.get(sd, [])
    offs = sorted(o for o, _k, _d in (mine_c + mine_o) if o is not None)
    want_first, want_last = seal.get("firstOffset"), seal.get("lastOffset")
    required = ("parameterSetHash", "sessionLineageId", "sessionDate", "phaseAtCall")
    missing_fields = []
    for _dg, r, _o in logical.values():
        if r.get("sessionDate") != sd or r.get("sessionLineageId") != lin or r.get("kind") == "seal":
            continue
        need = required + (("delivery",) if r.get("kind") == "outcome" else ())
        missing_fields += ["%s:%s" % (r.get("kind"), f) for f in need if r.get(f) in (None, "")]
    # attrition must satisfy A4.12's shape and arithmetic before any number from it is quoted
    att_bad = []
    for row in (seal.get("attrition") or []):
        graded = row.get("graded")
        reasons = [v for k, v in row.items() if k not in ("sessionDate", "etHour", "graded")
                   and isinstance(v, (int, float))]
        if not isinstance(graded, (int, float)) or graded < 0 or any(v < 0 for v in reasons) \
                or sum(reasons) > graded:
            att_bad.append(row.get("etHour"))
    if errs:
        status, why = "CORRUPT", "; ".join(errs[:3])
    elif bad_keys.get(sd):
        status, why = "CORRUPT", "%d record(s) written under a key their payload does not imply" % len(bad_keys[sd])
    elif seal.get("conflicts", 0) or conflicts_by_session.get(sd):
        status, why = "CORRUPT", "conflicting records for one key"
    elif att_bad:
        status, why = "CORRUPT", "attrition rows violate A4.12 shape/arithmetic at hour(s) %s" % att_bad
    elif offs and want_first is not None and (offs[0] != want_first or offs[-1] != want_last):
        status, why = "CORRUPT", "archived offsets [%s,%s] are not the seal's [%s,%s]" % (offs[0], offs[-1], want_first, want_last)
    elif offs and len(offs) != len(set(offs)):
        status, why = "CORRUPT", "duplicate offsets in the archive"
    elif missing_fields:
        status, why = "INCOMPLETE", "records missing pinned fields: %s" % sorted(set(missing_fields))[:4]
    elif seal.get("discontinuities") or spans:
        status, why = "DISCONTINUITY", ";".join(seal.get("discontinuities") or []) or "archiver recorded a log reset"
    elif orphans:
        status, why = "CORRUPT", "%d outcome(s) belong to no call in this lineage" % len(orphans)
    elif bad_horizons:
        status, why = "INCOMPLETE", "%d call(s) do not have exactly H3/H5/H15" % len(bad_horizons)
    elif got_calls != want_calls or got_out != want_out:
        status, why = "INCOMPLETE", "archived %d/%d calls, %d/%d outcomes" % (got_calls, want_calls, got_out, want_out)
    elif want_out != 3 * want_calls:
        status, why = "INCOMPLETE", "the seal itself is not 3 horizons per call"
    elif chain(0x01, mine_c) != seal.get("callsDigest") or chain(0x02, mine_o) != seal.get("outcomesDigest"):
        # The strongest check, and the only one a substitution cannot pass: the seal's chains must be
        # REPRODUCIBLE from the archive. Equal counts with a swapped record fail here.
        status, why = "CORRUPT", "the seal's chains are not reproducible from the archive"
    else:
        status, why = "COMPLETE", ""
    sessions["%s|%s|%s" % (sd, ph, lin)] = {"sessionDate": sd, "sessionLineageId": lin,
                    "parameterSetHash": ph, "archiveStatus": status, "reason": why,
                    "calls": got_calls, "outcomes": got_out,
                    "phase": (["CALIBRATION", "VALIDATION"][0]), "trackFromPush": seal.get("trackFromPush"),
                    "attritionRows": len(seal.get("attrition", []) or [])}

# a day with records but NO seal has not finished; a day with neither is not in the corpus at all
# A day with records but no seal has not finished. It AGES: unsealed by the following close it is
# INCOMPLETE, not perpetually "pending" — a status that never changes is a status nobody acts on.
for c in calls:
    k = "%s|%s|%s" % (c.get("sessionDate"), c.get("parameterSetHash"), c.get("sessionLineageId"))
    if k not in sessions:
        sd = c.get("sessionDate")
        aged = sd < today
        sessions[k] = {"sessionDate": sd, "sessionLineageId": c.get("sessionLineageId"),
                       "parameterSetHash": c.get("parameterSetHash"),
                       "archiveStatus": "INCOMPLETE" if aged else "PENDING_SEAL",
                       "reason": "records archived, no seal by the following close" if aged
                                 else "records archived, seal not yet written",
                       "calls": 0, "outcomes": 0, "phase": "CALIBRATION",
                       "trackFromPush": None, "attritionRows": 0}

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
    cell = "%s|%s|%s" % (c.get("enteredState"), (c.get("node") or {}).get("roles") or "NONE", c.get("regime"))
    b["cells"][cell] = b["cells"].get(cell, 0) + 1

# A5.6: the version is a content address of exactly what was read, so a progress run is reproducible
# and a watchdog can tell "the corpus advanced" from "nobody ran".
manifest = sorted("%s|%s|%s" % (k, v[2] if v[2] is not None else -1, v[0]) for k, v in logical.items())
corpus_version = hashlib.sha256("\n".join(manifest).encode("utf-8")).hexdigest()

# A5.5: the sessions that were OWED come from the trading calendar, not from what happens to be on
# disk — otherwise a wipe of the first weeks silently redefines "expected" as "whatever survived".
# The REAL calendar, not "weekday". Labor Day is a weekday and the market is shut; owing a session on
# it would report a permanent MISSING that no run can ever satisfy, and an alarm that can never clear
# is an alarm that gets ignored.
_cal = None
for _p in (os.environ.get("CALENDAR_DIR"), "/home/abhinav/oe-ops",
           os.path.expanduser("~/development/workspace/options-edge-deploy/scripts/jenkins")):
    if _p and os.path.isdir(_p):
        sys.path.insert(0, _p)
try:
    import market_calendar as _cal
except Exception:
    _cal = None

def owed(start, end):
    days, d = [], datetime.date.fromisoformat(start)
    last = datetime.date.fromisoformat(end)
    while d <= last:
        if _cal is not None and hasattr(_cal, "is_trading_day"):
            ok = _cal.is_trading_day(d)
        else:
            ok = d.weekday() < 5
        if ok:
            days.append(d.isoformat())
        d += datetime.timedelta(days=1)
    return days

# A5.5: DECLARED, not inferred. min(sessions) lets deletion of the earliest weeks redefine the
# expected window as "whatever survived", which is the failure the owed-session check exists to catch.
declared = os.environ.get("CORPUS_START_DATE")
observed = min((v["sessionDate"] for v in sessions.values()), default=today)
corpus_start = declared or observed
if not declared:
    read_errors.append("corpusStartDate is INFERRED from the archive (%s); declare CORPUS_START_DATE "
                       "or a deletion of the earliest sessions silently narrows the expected window" % observed)
have_days = {v["sessionDate"] for v in sessions.values()}
for day in owed(corpus_start, today):
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
rc=$?
[ $rc -eq 0 ] || { log "FATAL: progress report failed (rc=$rc)"; exit $rc; }
log "calibration progress written under $OUT_ROOT"
