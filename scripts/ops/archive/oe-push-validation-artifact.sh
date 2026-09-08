#!/usr/bin/env bash
# oe-push-validation-artifact.sh — A5.8. The OFFLINE evaluator: one pinned corpusVersion in, one
# PushValidationArtifact out.
#
# What it is NOT. It is not A2. A4 has no admission, no plan, no contract and no execution, so its
# records cannot produce the A2 ValidationArtifactV1 pair. `decision = ACCEPT` means only that the push
# instrument's own forward evidence met its own preregistered bar: it sets no field on any served
# record, it cannot move validationStatus, and it is not a step on the A2 path.
#
# The stopping boundary is PREREGISTERED — frozen when TRACK_FROM_PUSH is set, recorded in the build
# ledger with its commit, and READ here rather than chosen. Choosing it after seeing the data, or
# taking any subset of the window, is what would make every number below meaningless.
set -uo pipefail

ENV_NAME="${ENV:-prod}"
ARCHIVE_DIR="${ARCHIVE_DIR:-/mnt/nas/optionsedge}"
LEDGER_TOPIC="${LEDGER_TOPIC:-context-tape.direction.ledger}"
ROOT="$ARCHIVE_DIR/kafka/$ENV_NAME/$LEDGER_TOPIC"
OUT_ROOT="$ARCHIVE_DIR/calibration-runs/$ENV_NAME"
STAMP="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
TODAY="${REPORT_DATE:-$(TZ=America/New_York date '+%Y-%m-%d')}"
log() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"; }

# Scope: ONE artifact per (env, parameterSetHash, TRACK_FROM_PUSH). Environments are never pooled —
# dev and prod evidence stay split, exactly as the NAS layout keeps them.
: "${PARAMETER_SET_HASH:?PARAMETER_SET_HASH is required — an artifact without a cohort identity is meaningless}"
: "${TRACK_FROM_PUSH:?TRACK_FROM_PUSH is required — it is half the scope of this artifact}"
: "${STOPPING_BOUNDARY_MS:?STOPPING_BOUNDARY_MS is required and must be PREREGISTERED, frozen with TRACK_FROM_PUSH}"
: "${CORPUS_VERSION:?CORPUS_VERSION is required — the artifact evaluates ONE pinned corpus, and a run that reads whatever is on disk today is not reproducible}"

# The four size targets, adopted from A2. Not a claim that A4 feeds A2.
T_SESSIONS="${T_SESSIONS:-30}"; T_COHORT="${T_COHORT:-200}"; T_CLASS="${T_CLASS:-50}"; T_CELL="${T_CELL:-50}"

# The REQUIRED cell universe, frozen BEFORE validation begins and declared rather than inferred. The
# artifact enumerates every one of them, zero-count cells included: reporting only the cells that
# happen to appear would let an evaluator quietly omit the sparse or failing ones, which is choosing
# the answer after seeing the data.
REQUIRED_CLASSES="${REQUIRED_CLASSES:-1 -1}"
REQUIRED_CELLS="${REQUIRED_CELLS:-EXHAUSTED|CALL_WALL|POS_GAMMA EXHAUSTED|PUT_WALL|POS_GAMMA EXHAUSTED|CALL_WALL|NEG_GAMMA EXHAUSTED|PUT_WALL|NEG_GAMMA STRONG|CALL_WALL|NEG_GAMMA STRONG|PUT_WALL|NEG_GAMMA EXHAUSTED|NONE|POS_GAMMA EXHAUSTED|NONE|NEG_GAMMA STRONG|NONE|NEG_GAMMA STRONG|NONE|POS_GAMMA}"

# A2's estimator VERBATIM, not a new one: resample whole SESSIONS with replacement to the original
# session count, B replicates, the statistic pooled over the resampled sessions, type-7 quantiles,
# frozen seed. Naming an estimator without its resampling unit, replicate count, quantile convention
# and seed leaves the pass/fail free to move; A2 learned that at its own r9.
BOOTSTRAP_SEED="${BOOTSTRAP_SEED:-20260906}"
BOOTSTRAP_B="${BOOTSTRAP_B:-10000}"

# The acceptance SHAPE is frozen in the design; the NUMBERS are PROVISIONAL_PENDING_MEASUREMENT and are
# chosen at the same moment the engine's literals are frozen, exactly as A4.11 does. The defaults here
# are deliberately impossible to pass, so an unconfigured run REJECTS rather than quietly accepting —
# a default that accepts is how an unmeasured instrument gets a certificate.
RESULT_LCB_FLOOR="${RESULT_LCB_FLOOR:-999999}"
HIT_RATE_LCB_FLOOR="${HIT_RATE_LCB_FLOOR:-1.01}"
MEDIAN_MAE_CEIL="${MEDIAN_MAE_CEIL:--1}"
P90_MAE_CEIL="${P90_MAE_CEIL:--1}"
COVERAGE_FLOOR="${COVERAGE_FLOOR:-1.01}"
ATTRITION_CEIL="${ATTRITION_CEIL:--1}"
THRESHOLDS_PROVISIONAL="${THRESHOLDS_PROVISIONAL:-true}"

[ -d "$ROOT" ] || { log "FATAL: no corpus at $ROOT — the ledger has never been archived for env=$ENV_NAME"; exit 1; }
command -v python3 >/dev/null || { log "FATAL: python3 missing"; exit 1; }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/oe_corpus_reader.py" ] || { log "FATAL: oe_corpus_reader.py missing beside $0"; exit 1; }

python3 - "$SCRIPT_DIR" "$ROOT" "$OUT_ROOT" "$TODAY" "$STAMP" "$ENV_NAME" "$PARAMETER_SET_HASH" \
         "$TRACK_FROM_PUSH" "$STOPPING_BOUNDARY_MS" "$CORPUS_VERSION" "$T_SESSIONS" "$T_COHORT" \
         "$T_CLASS" "$T_CELL" "$REQUIRED_CLASSES" "$REQUIRED_CELLS" "$BOOTSTRAP_SEED" "$BOOTSTRAP_B" \
         "$RESULT_LCB_FLOOR" "$HIT_RATE_LCB_FLOOR" "$MEDIAN_MAE_CEIL" "$P90_MAE_CEIL" \
         "$COVERAGE_FLOOR" "$ATTRITION_CEIL" "$THRESHOLDS_PROVISIONAL" <<'PY'
import json, os, sys, hashlib, random, tempfile

(script_dir, root, out_root, today, stamp, env, phash, track_from, stopping, pinned_version,
 t_sessions, t_cohort, t_class, t_cell, req_classes, req_cells, seed, B,
 f_result, f_hit, c_median_mae, c_p90_mae, f_cov, c_attr, provisional) = sys.argv[1:26]
sys.path.insert(0, script_dir)
import oe_corpus_reader as R

stopping = int(stopping); seed = int(seed); B = int(B)
t_sessions, t_cohort, t_class, t_cell = int(t_sessions), int(t_cohort), int(t_class), int(t_cell)
f_result, f_hit, f_cov = float(f_result), float(f_hit), float(f_cov)
c_median_mae, c_p90_mae, c_attr = float(c_median_mae), float(c_p90_mae), float(c_attr)
REQUIRED_CLASSES = req_classes.split()
REQUIRED_CELLS = req_cells.split()
PRIMARY = "H5"

read_errors = []
read = R.read_logical(root)
sidecar = R.read_sidecar(root, read_errors)
sessions, seals, calls, outcomes = R.classify_sessions(read, sidecar, today)
logical = read["logical"]

# ---- the corpus this artifact claims to have evaluated must be the corpus it read ----------------
# Without this the artifact is an assertion about a moving target: a rerun a day later would carry the
# same corpusVersion field and a different population.
actual_version = R.corpus_version(logical)
version_ok = (actual_version == pinned_version)

clauses = []
def clause(name, state, passed, value=None, note=""):
    clauses.append({"clause": name, "state": state, "passed": bool(passed),
                    "value": None if value is None else round(float(value), 6), "note": note})

# ---- the cohort: EVERY call in the preregistered window, no subset ------------------------------
# refT, not sessionDate: the boundary is a UTC instant and the window is closed at both ends.
tf_ms = None
try:
    import datetime
    tf_ms = int(datetime.datetime.fromisoformat(
        (track_from if "T" in track_from else track_from + "T00:00:00") + ("" if track_from.endswith("Z") or "+" in track_from else "+00:00")
    ).timestamp() * 1000)
except Exception:
    read_errors.append("TRACK_FROM_PUSH is not an instant this evaluator can parse: %s" % track_from)

def in_window(rec):
    t = rec.get("refT")
    return isinstance(t, (int, float)) and tf_ms is not None and tf_ms <= t <= stopping

def session_key(rec):
    return "%s|%s|%s" % (rec.get("sessionDate"), rec.get("parameterSetHash"), rec.get("sessionLineageId"))

cohort = [c for c in calls
          if c.get("parameterSetHash") == phash and c.get("trackFromPush") == track_from
          and c.get("phaseAtCall") == "VALIDATION" and in_window(c)]
cohort_ids = {c.get("callId") for c in cohort}
by_call = {c.get("callId"): c for c in cohort}

# ---- COMPLETENESS, which is the verdict here and not a warning ----------------------------------
# The window's sessions are the ones this artifact is answerable for. A NOT_EVALUABLE session inside it
# does not make a smaller cohort — it makes one whose population is unknown, and a statistic over an
# unknown population is worse than no statistic.
window_sessions = sorted({session_key(c) for c in cohort})
not_evaluable = []
for k in window_sessions:
    s = sessions.get(k)
    if s is None or s["archiveStatus"] != "COMPLETE":
        not_evaluable.append({"session": k, "why": (s or {}).get("archiveStatus", "ABSENT"),
                              "reason": (s or {}).get("reason", "no session row for a call in the window")})

# A session whose attrition cannot be validated is NOT_EVALUABLE too — a number nobody can trust is
# not a smaller number, it is a different failure.
refusal_by_session, attrition_unusable = {}, []
for k in window_sessions:
    s = sessions.get(k) or {}
    seal = seals.get((s.get("sessionDate"), s.get("parameterSetHash")))
    if seal is None:
        attrition_unusable.append({"session": k, "why": "no seal"})
        continue
    if R.attrition_violations(seal):
        attrition_unusable.append({"session": k, "why": "attrition rows violate A4.12 shape/arithmetic"})
        continue
    rate = R.session_refusal_rate(seal)
    if rate is None:
        # Sum graded == 0: the session graded nothing, so refusal says nothing about it. Never 0%.
        attrition_unusable.append({"session": k, "why": "no graded ticks — refusal rate is undefined, not zero"})
        continue
    refusal_by_session[k] = rate

# Bijection, scoped to the window and nothing outside it. Records outside are legitimate and are
# neither orphans nor members.
window_outcomes = [o for o in outcomes
                   if o.get("parameterSetHash") == phash and o.get("callId") in cohort_ids]
outside = [o for o in outcomes
           if o.get("parameterSetHash") == phash and o.get("callId") not in cohort_ids
           and by_call.get(o.get("callId")) is not None]
horizons = {}
for o in window_outcomes:
    horizons.setdefault(o.get("callId"), set()).add(o.get("horizon"))
missing_horizon = sorted(c for c in cohort_ids if horizons.get(c, set()) != set(R.HORIZONS))
orphans = sorted({o.get("callId") for o in outcomes
                  if o.get("parameterSetHash") == phash and o.get("callId") not in cohort_ids
                  and in_window({"refT": o.get("refT")}) and o.get("phaseAtCall") == "VALIDATION"
                  and o.get("trackFromPush") == track_from and o.get("callId") not in by_call})

# ---- the statistics: A4.9's reducers over the cohort at the primary horizon ----------------------
prim = [o for o in window_outcomes if o.get("horizon") == PRIMARY]
obs = [o for o in prim if o.get("resultState") == "OBSERVED" and isinstance(o.get("resultTicks"), (int, float))]
mae = [o for o in prim if o.get("pathState") == "OBSERVED" and isinstance(o.get("maeTicks"), (int, float))]
coverage = (float(len(obs)) / float(len(prim))) if prim else None

def q_type7(xs, p):
    """Type-7 (linear interpolation), stated because a different convention moves the pass/fail."""
    if not xs:
        return None
    s = sorted(xs)
    if len(s) == 1:
        return float(s[0])
    h = (len(s) - 1) * p
    lo = int(h)
    hi = min(lo + 1, len(s) - 1)
    return float(s[lo]) + (h - lo) * (float(s[hi]) - float(s[lo]))

def pooled(sess_ids, picker):
    out = []
    for sid in sess_ids:
        out.extend(picker.get(sid, []))
    return out

results_by_session, hits_by_session, obs_by_session, mae_by_session = {}, {}, {}, {}
for o in obs:
    sid = session_key(by_call[o["callId"]])
    results_by_session.setdefault(sid, []).append(float(o["resultTicks"]))
    hits_by_session.setdefault(sid, []).append(1.0 if float(o["resultTicks"]) > 0 else 0.0)
for o in mae:
    sid = session_key(by_call[o["callId"]])
    mae_by_session.setdefault(sid, []).append(float(o["maeTicks"]))

def bootstrap(picker, statistic, tail):
    """Resample WHOLE SESSIONS with replacement to the original session count, B replicates, the
    statistic pooled over the resampled sessions. An LCB95 is the 5th percentile of the replicates and
    a UCB95 the 95th — the tail is named per clause, because naming only "95%" makes an acceptance test
    optimistic."""
    ids = sorted(picker.keys())
    if not ids:
        return None
    rnd = random.Random(seed)
    reps = []
    for _ in range(B):
        draw = [ids[rnd.randrange(len(ids))] for _ in range(len(ids))]
        v = statistic(pooled(draw, picker))
        if v is not None:
            reps.append(v)
    if not reps:
        return None
    return q_type7(reps, 0.05 if tail == "lower" else 0.95)

def mean(xs):
    return (sum(xs) / len(xs)) if xs else None

result_lcb = bootstrap(results_by_session, mean, "lower")
hit_lcb = bootstrap(hits_by_session, mean, "lower")
median_mae_ucb = bootstrap(mae_by_session, lambda xs: q_type7(xs, 0.50), "upper")
p90_mae_ucb = bootstrap(mae_by_session, lambda xs: q_type7(xs, 0.90), "upper")

# ---- the clauses, one row each, none omitted ----------------------------------------------------
cohort_sessions = {session_key(c) for c in cohort}
class_counts = {c: 0 for c in REQUIRED_CLASSES}
cell_counts = {c: 0 for c in REQUIRED_CELLS}
for c in cohort:
    cls = str(c.get("predictedSign"))
    if cls in class_counts:
        class_counts[cls] += 1
    ck = R.cell_key(c)
    if ck in cell_counts:
        cell_counts[ck] += 1
min_class = min(class_counts.values()) if class_counts else 0
min_cell = min(cell_counts.values()) if cell_counts else 0
size_ok = (len(cohort_sessions) >= t_sessions and len(cohort) >= t_cohort
           and min_class >= t_class and min_cell >= t_cell)
clause("COHORT_SIZE", "PASS" if size_ok else "FAIL", size_ok, len(cohort),
       "sessions %d/%d cohort %d/%d per-class %d/%d per-cell %d/%d over %d required cells"
       % (len(cohort_sessions), t_sessions, len(cohort), t_cohort, min_class, t_class,
          min_cell, t_cell, len(REQUIRED_CELLS)))

complete_ok = (version_ok and not not_evaluable and not attrition_unusable
               and not missing_horizon and not orphans and bool(cohort))
why = []
if not version_ok:
    why.append("corpusVersion read (%s) is not the pinned one (%s)" % (actual_version[:12], pinned_version[:12]))
if not cohort:
    why.append("the window contains no cohort call at all")
if not_evaluable:
    why.append("%d session(s) in the window are NOT_EVALUABLE" % len(not_evaluable))
if attrition_unusable:
    why.append("%d session(s) have attrition that cannot be used" % len(attrition_unusable))
if missing_horizon:
    why.append("%d call(s) do not have exactly H3/H5/H15" % len(missing_horizon))
if orphans:
    why.append("%d outcome(s) in the window belong to no cohort call" % len(orphans))
clause("COMPLETENESS", "PASS" if complete_ok else "FAIL", complete_ok, len(window_sessions), "; ".join(why))

def bound_clause(name, value, cmp_floor=None, cmp_ceil=None, note=""):
    if value is None:
        clause(name, "NOT_EVALUABLE", False, None, note or "no observation to estimate from")
        return
    ok = (value > cmp_floor) if cmp_floor is not None else (value <= cmp_ceil)
    clause(name, "PASS" if ok else "FAIL", ok, value, note)

bound_clause("RESULT_LCB", result_lcb, cmp_floor=f_result,
             note="LCB95 (5th pct of %d replicates) of mean result ticks at %s vs floor %s" % (B, PRIMARY, f_result))
bound_clause("HIT_RATE_LCB", hit_lcb, cmp_floor=f_hit,
             note="LCB95 of hit rate at %s (hit = resultTicks > 0) vs floor %s" % (PRIMARY, f_hit))
if median_mae_ucb is None or p90_mae_ucb is None:
    clause("MAE_CEILING", "NOT_EVALUABLE", False, None, "no OBSERVED path to measure MAE on")
else:
    ok = median_mae_ucb <= c_median_mae and p90_mae_ucb <= c_p90_mae
    clause("MAE_CEILING", "PASS" if ok else "FAIL", ok, median_mae_ucb,
           "UCB95 median %.4f vs %s and UCB95 p90 %.4f vs %s" % (median_mae_ucb, c_median_mae, p90_mae_ucb, c_p90_mae))
bound_clause("COVERAGE", coverage, cmp_floor=f_cov,
             note="observed/calls at %s — an instrument whose outcomes mostly go UNOBSERVED has not been measured" % PRIMARY)

# ATTRITION_CEILING takes NO tail: it is a data-quality gate on what was observed, not an estimate of a
# population, and it is PER SESSION — a pooled rate would let a blind week hide behind a busy month.
if not refusal_by_session:
    clause("ATTRITION_CEILING", "NOT_EVALUABLE", False, None,
           "no session in the window has a usable refusal rate")
else:
    worst_id = max(refusal_by_session, key=lambda k: refusal_by_session[k])
    worst = refusal_by_session[worst_id]
    ok = worst <= c_attr and not attrition_unusable
    clause("ATTRITION_CEILING", "PASS" if ok else "FAIL", ok, worst,
           "raw MAXIMUM session refusal rate (no tail), worst = %s vs ceiling %s" % (worst_id, c_attr))

decision = "ACCEPT" if all(x["passed"] for x in clauses) else "REJECT"

artifact = {
    "eventType": "PUSH_VALIDATION_ARTIFACT", "artifactVersion": 1,
    "env": env, "generatedAt": stamp,
    "parameterSetHash": phash, "trackFromPush": track_from, "stoppingBoundaryMs": stopping,
    "primaryHorizon": PRIMARY,
    "corpusVersion": pinned_version, "corpusVersionRead": actual_version,
    "estimator": {"unit": "SESSION", "replicates": B, "seed": seed, "quantile": "TYPE_7",
                  "lcb95": "5th percentile of replicates", "ucb95": "95th percentile of replicates"},
    "thresholds": {"resultLcbFloor": f_result, "hitRateLcbFloor": f_hit,
                   "medianMaeCeiling": c_median_mae, "p90MaeCeiling": c_p90_mae,
                   "coverageFloor": f_cov, "attritionCeiling": c_attr,
                   "state": "PROVISIONAL_PENDING_MEASUREMENT" if provisional == "true" else "FROZEN"},
    "cohort": {"calls": len(cohort), "sessions": len(cohort_sessions),
               "classCounts": class_counts, "cellCounts": cell_counts,
               "requiredCells": REQUIRED_CELLS, "requiredClasses": REQUIRED_CLASSES,
               "outcomesAtPrimary": len(prim), "observedAtPrimary": len(obs)},
    "notEvaluableSessions": not_evaluable, "attritionUnusableSessions": attrition_unusable,
    "callsMissingAHorizon": missing_horizon[:20], "orphanOutcomes": orphans[:20],
    "sessionRefusalRates": {k: round(v, 6) for k, v in sorted(refusal_by_session.items())},
    "readErrors": read_errors[:20],
    "clauseResults": clauses,
    "decision": decision,
    # NON-AUTHORIZING, stated in the record itself so no consumer has to infer it.
    "authorizing": False, "actionable": False, "slice": "COMMISSIONING_SHADOW",
    "note": "A4 evidence only. This artifact cannot satisfy A2, cannot set validationStatus, and is not a step on the A2 path.",
}
# identity = SHA-256 over the canonical JSON with artifactId excluded — the same canonical form A1 uses
artifact["artifactId"] = hashlib.sha256(R.canonical(artifact).encode("utf-8")).hexdigest()

d = os.path.join(out_root, phash, str(track_from).replace(":", "").replace("/", ""), "artifacts")
os.makedirs(d, exist_ok=True)
tmp = tempfile.NamedTemporaryFile("w", dir=d, delete=False, suffix=".tmp")
json.dump(artifact, tmp, indent=1, sort_keys=True); tmp.close()
path = os.path.join(d, "%s.json" % artifact["artifactId"][:16])
os.replace(tmp.name, path)                                  # atomic publication

print("PushValidationArtifact %s decision=%s — %s" % (
    artifact["artifactId"][:12], decision,
    " ".join("%s=%s" % (x["clause"], x["state"]) for x in clauses)))
print("written to %s" % path)
sys.exit(0 if decision == "ACCEPT" else 3)
PY
rc=$?
if [ $rc -eq 0 ]; then log "decision=ACCEPT"; elif [ $rc -eq 3 ]; then log "decision=REJECT (an artifact was still written — a REJECT is evidence, not an error)"; else log "FATAL: evaluator failed (rc=$rc)"; fi
exit $rc
