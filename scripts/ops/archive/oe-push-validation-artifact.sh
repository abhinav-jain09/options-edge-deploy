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

# ---- the preregistration, READ rather than accepted from the caller (r6 #1) ---------------------
# Every one of these was an environment variable, which meant an evaluator could pick a smaller cell
# universe, move the stopping boundary, lower a size target or tune a threshold AFTER seeing the data
# and still get an artifact that looked exactly like a real one. They now come from the repo-managed
# calibration-targets.env installed beside this script, and an attempt to override one is refused
# rather than silently honoured.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGETS="${OE_CAL_TARGETS_FILE:-$SCRIPT_DIR/calibration-targets.env}"
[ -f "$TARGETS" ] || { log "FATAL: no preregistration at $TARGETS — an artifact without one is a number chosen after the fact"; exit 1; }
[ -f "$SCRIPT_DIR/oe_corpus_reader.py" ] || { log "FATAL: oe_corpus_reader.py missing beside $0"; exit 1; }

for v in PARAMETER_SET_HASH TRACK_FROM_PUSH STOPPING_BOUNDARY_MS CORPUS_START_DATE T_SESSIONS T_COHORT \
         T_CLASS T_CELL REQUIRED_CLASSES REQUIRED_CELLS BOOTSTRAP_SEED BOOTSTRAP_B RESULT_LCB_FLOOR \
         HIT_RATE_LCB_FLOOR MEDIAN_MAE_CEIL P90_MAE_CEIL COVERAGE_FLOOR ATTRITION_CEIL THRESHOLDS_STATE; do
  if [ -n "${!v:-}" ]; then
    log "FATAL: $v is preregistered in $TARGETS and cannot be supplied by the caller — that is choosing the answer after seeing the data"
    exit 1
  fi
done

# shellcheck source=/dev/null
. "$TARGETS"
env_pick() { eval "printf '%s' \"\${OE_CAL_${1}_${ENV_NAME}:-}\""; }
PARAMETER_SET_HASH="$(env_pick PARAMETER_SET_HASH)"
TRACK_FROM_PUSH="$(env_pick TRACK_FROM_PUSH)"
STOPPING_BOUNDARY_MS="$(env_pick STOPPING_BOUNDARY_MS)"
CORPUS_START_DATE="$(env_pick CORPUS_START_DATE)"
T_SESSIONS="$OE_CAL_T_SESSIONS"; T_COHORT="$OE_CAL_T_COHORT"
T_CLASS="$OE_CAL_T_CLASS";       T_CELL="$OE_CAL_T_CELL"
REQUIRED_CLASSES="$OE_CAL_REQUIRED_CLASSES"; REQUIRED_CELLS="$OE_CAL_REQUIRED_CELLS"
BOOTSTRAP_SEED="$OE_CAL_BOOTSTRAP_SEED";     BOOTSTRAP_B="$OE_CAL_BOOTSTRAP_B"
THRESHOLDS_STATE="$OE_CAL_THRESHOLDS_STATE"
RESULT_LCB_FLOOR="$OE_CAL_RESULT_LCB_FLOOR"; HIT_RATE_LCB_FLOOR="$OE_CAL_HIT_RATE_LCB_FLOOR"
MEDIAN_MAE_CEIL="$OE_CAL_MEDIAN_MAE_CEIL";   P90_MAE_CEIL="$OE_CAL_P90_MAE_CEIL"
COVERAGE_FLOOR="$OE_CAL_COVERAGE_FLOOR";     ATTRITION_CEIL="$OE_CAL_ATTRITION_CEIL"

for pair in "PARAMETER_SET_HASH:$PARAMETER_SET_HASH" "TRACK_FROM_PUSH:$TRACK_FROM_PUSH" \
            "STOPPING_BOUNDARY_MS:$STOPPING_BOUNDARY_MS" "CORPUS_START_DATE:$CORPUS_START_DATE"; do
  name="${pair%%:*}"; val="${pair#*:}"
  [ -n "$val" ] || { log "FATAL: $name is not declared for env=$ENV_NAME in $TARGETS"; exit 1; }
  [ "$val" != "UNFROZEN" ] || { log "REFUSING: $name is still UNFROZEN for env=$ENV_NAME. The boundary and the parameter set are frozen together, in one commit, before any of this data is looked at."; exit 2; }
done

# The corpus this artifact claims must be one a PROGRESS RUN ALREADY PUBLISHED (r6 #5). A version
# recomputed from the archive as it stands today would match again after records or whole days
# disappeared, so a pin that is merely "what is on disk" pins nothing.
: "${CORPUS_VERSION:?CORPUS_VERSION is required — name the published corpus this artifact evaluates}"

[ -d "$ROOT" ] || { log "FATAL: no corpus at $ROOT — the ledger has never been archived for env=$ENV_NAME"; exit 1; }
command -v python3 >/dev/null || { log "FATAL: python3 missing"; exit 1; }
python3 - "$SCRIPT_DIR" "$ROOT" "$OUT_ROOT" "$TODAY" "$STAMP" "$ENV_NAME" "$PARAMETER_SET_HASH" \
         "$TRACK_FROM_PUSH" "$STOPPING_BOUNDARY_MS" "$CORPUS_VERSION" "$T_SESSIONS" "$T_COHORT" \
         "$T_CLASS" "$T_CELL" "$REQUIRED_CLASSES" "$REQUIRED_CELLS" "$BOOTSTRAP_SEED" "$BOOTSTRAP_B" \
         "$RESULT_LCB_FLOOR" "$HIT_RATE_LCB_FLOOR" "$MEDIAN_MAE_CEIL" "$P90_MAE_CEIL" \
         "$COVERAGE_FLOOR" "$ATTRITION_CEIL" "$THRESHOLDS_STATE" "$CORPUS_START_DATE" <<'PY'
import json, os, sys, hashlib, random, tempfile

(script_dir, root, out_root, today, stamp, env, phash, track_from, stopping, pinned_version,
 t_sessions, t_cohort, t_class, t_cell, req_classes, req_cells, seed, B,
 f_result, f_hit, c_median_mae, c_p90_mae, f_cov, c_attr, thresholds_state,
 corpus_start) = sys.argv[1:27]
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
# A version recomputed from the archive as it stands today matches again after records or whole days
# have disappeared, so "the hash of what I just read" pins nothing on its own (r6 #5). The pin must ALSO
# name a version some progress run already PUBLISHED, and the artifact records which run and when.
published = {}
try:
    import glob as _glob
    for f in _glob.glob(os.path.join(out_root, "*", "*", "progress", "dt=*.json")):
        try:
            rec = json.load(open(f))
        except Exception:
            read_errors.append("progress record unreadable: %s" % os.path.basename(f))
            continue
        v = rec.get("corpusVersion")
        if v:
            published.setdefault(v, []).append(rec.get("reportDate"))
except Exception as e:
    read_errors.append("could not read published progress records: %s" % e.__class__.__name__)
published_on = sorted(published.get(pinned_version, []))
version_ok = (actual_version == pinned_version) and bool(published_on)

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

def call_identity(rec):
    """A callId alone is not an identity. A5 scopes every key by hash and lineage precisely because a
    callId can repeat across lineages of the same session, and joining outcomes on the bare id admits
    another lineage's outcomes into this cohort (r6 #3)."""
    return (rec.get("parameterSetHash"), rec.get("sessionLineageId"), rec.get("callId"))

cohort_ids = {call_identity(c) for c in cohort}
by_call = {call_identity(c): c for c in cohort}

# ---- COMPLETENESS, which is the verdict here and not a warning ----------------------------------
# The window's sessions are the ones this artifact is answerable for. A NOT_EVALUABLE session inside it
# does not make a smaller cohort — it makes one whose population is unknown, and a statistic over an
# unknown population is worse than no statistic.
window_sessions = sorted({session_key(c) for c in cohort})
not_evaluable = []
for k in window_sessions:
    srow = sessions.get(k)
    if srow is None or srow["archiveStatus"] != "COMPLETE":
        not_evaluable.append({"session": k, "why": (srow or {}).get("archiveStatus", "ABSENT"),
                              "reason": (srow or {}).get("reason", "no session row for a call in the window")})

# A day the corpus OWES and does not have is the failure this clause exists to catch, and it cannot be
# seen by looking at the sessions that survived: those are, by construction, the ones still there.
# Removing a whole trading day used to leave a smaller cohort that passed COMPLETENESS (r6 #2). The owed
# days come from the trading calendar between the preregistered start and the stopping boundary.
_cal = R.load_calendar()
boundary_date = None
try:
    import datetime as _dt
    boundary_date = _dt.datetime.fromtimestamp(stopping / 1000.0, _dt.timezone.utc).date().isoformat()
except Exception:
    read_errors.append("STOPPING_BOUNDARY_MS is not an instant this evaluator can turn into a date")
have_days = {v["sessionDate"] for v in sessions.values() if v["archiveStatus"] == "COMPLETE"}
missing_days = []
if boundary_date:
    window_start = max(str(corpus_start)[:10], str(track_from)[:10])
    for day in R.owed(window_start, boundary_date, _cal):
        if day not in have_days:
            missing_days.append(day)
            not_evaluable.append({"session": day, "why": "MISSING",
                                  "reason": "a trading day inside the preregistered window that the corpus owes and does not have"})

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
window_outcomes = [o for o in outcomes if call_identity(o) in cohort_ids]

# Outcomes carry no refT — the record is (callId, horizon, targetT, ...) — so membership is decided
# THROUGH the call, never by re-testing the window on a field that does not exist (r6 #3). An outcome
# whose call lies outside the window is legitimately neither a member nor an orphan.
all_call_ids = {call_identity(c) for c in calls}
orphans = sorted({"|".join(str(x) for x in call_identity(o)) for o in outcomes
                  if o.get("parameterSetHash") == phash and o.get("trackFromPush") == track_from
                  and o.get("phaseAtCall") == "VALIDATION"
                  and session_key(o) in window_sessions
                  and call_identity(o) not in all_call_ids})

# Counting DISTINCT horizons hides a duplicate representation of the same horizon, which is exactly the
# conflict the ledger refuses to chain — so count them and compare both (r6 #3).
horizons, horizon_rows = {}, {}
for o in window_outcomes:
    horizons.setdefault(call_identity(o), set()).add(o.get("horizon"))
    horizon_rows[call_identity(o)] = horizon_rows.get(call_identity(o), 0) + 1
missing_horizon = sorted("|".join(str(x) for x in c) for c in cohort_ids
                         if horizons.get(c, set()) != set(R.HORIZONS)
                         or horizon_rows.get(c, 0) != len(R.HORIZONS))

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

def nearest_rank(xs, pct):
    """A4.9's reducer, x[ceil(p*n)] on ascending values — the SAMPLE statistic. The replicate quantile
    below is type-7, which is A2's convention; using one convention for both would silently be a
    different estimator than either amendment specifies (r6 #4)."""
    if not xs:
        return None
    a = sorted(xs)
    rank = -(-pct * len(a) // 100)
    return float(a[max(0, min(len(a) - 1, rank - 1))])

def pooled(sess_ids, picker):
    out = []
    for sid in sess_ids:
        out.extend(picker.get(sid, []))
    return out

results_by_session, hits_by_session, obs_by_session, mae_by_session = {}, {}, {}, {}
for o in obs:
    sid = session_key(by_call[call_identity(o)])
    results_by_session.setdefault(sid, []).append(float(o["resultTicks"]))
    hits_by_session.setdefault(sid, []).append(1.0 if float(o["resultTicks"]) > 0 else 0.0)
for o in mae:
    sid = session_key(by_call[call_identity(o)])
    mae_by_session.setdefault(sid, []).append(float(o["maeTicks"]))

def bootstrap(picker, statistic, tail, ids):
    """Resample WHOLE SESSIONS with replacement to the original session count, B replicates, the
    statistic pooled over the resampled sessions. An LCB95 is the 5th percentile of the replicates and
    a UCB95 the 95th — the tail is named per clause, because naming only "95%" makes an acceptance test
    optimistic.

    `ids` is EVERY cohort session, not only the ones that produced a usable observation (r6 #4).
    Resampling the survivors would quietly condition the estimate on having observed something, which
    is the same bias the coverage clause exists to measure."""
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

cohort_session_ids = sorted({session_key(c) for c in cohort})
result_lcb = bootstrap(results_by_session, mean, "lower", cohort_session_ids)
hit_lcb = bootstrap(hits_by_session, mean, "lower", cohort_session_ids)
median_mae_ucb = bootstrap(mae_by_session, lambda xs: nearest_rank(xs, 50), "upper", cohort_session_ids)
p90_mae_ucb = bootstrap(mae_by_session, lambda xs: nearest_rank(xs, 90), "upper", cohort_session_ids)

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
               and not missing_horizon and not orphans and bool(cohort) and not read_errors)
why = []
if actual_version != pinned_version:
    why.append("corpusVersion read (%s) is not the pinned one (%s)" % (actual_version[:12], pinned_version[:12]))
elif not published_on:
    why.append("the pinned corpusVersion was never published by a progress run — a version recomputed "
               "from today's archive would match again after a deletion")
if missing_days:
    why.append("%d owed trading day(s) are absent from the window: %s"
               % (len(missing_days), ", ".join(missing_days[:5])))
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
if read_errors:
    # An unreadable discontinuity sidecar or progress record used to sit quietly beside an ACCEPT
    # (r6 #6). Something the reader could not read is not something the evaluator may pass over.
    why.append("%d read error(s): %s" % (len(read_errors), read_errors[0]))
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

# While the numbers are PROVISIONAL_PENDING_MEASUREMENT nothing may ACCEPT, however good the data looks
# (r6 #1). An acceptance measured against thresholds nobody has committed to is not an acceptance, and
# an artifact that said ACCEPT under provisional numbers would be quoted as if it were one.
frozen = (thresholds_state == "FROZEN")
clause("THRESHOLDS_FROZEN", "PASS" if frozen else "FAIL", frozen, None,
       "acceptance numbers are %s in the preregistration" % thresholds_state)

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
                   "state": thresholds_state},
    "cohort": {"calls": len(cohort), "sessions": len(cohort_sessions),
               "classCounts": class_counts, "cellCounts": cell_counts,
               "requiredCells": REQUIRED_CELLS, "requiredClasses": REQUIRED_CLASSES,
               "outcomesAtPrimary": len(prim), "observedAtPrimary": len(obs)},
    "corpusVersionPublishedOn": published_on,
    "corpusStartDate": corpus_start, "owedDaysMissing": missing_days,
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
