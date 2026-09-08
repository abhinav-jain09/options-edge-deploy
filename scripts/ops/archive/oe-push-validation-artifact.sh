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
# The declaration is the file INSTALLED BESIDE THIS SCRIPT, and there is deliberately no way to point
# it somewhere else (r7 #1). An OE_CAL_TARGETS_FILE hatch defeats every refusal below in one move: the
# caller simply writes their own boundary, cell universe, targets, seed and thresholds into a file of
# their own. A test that needs a different declaration stages the whole unit into a directory and runs
# the script from there, which is what the deployed unit is anyway.
TARGETS="$SCRIPT_DIR/calibration-targets.env"
[ -f "$TARGETS" ] || { log "FATAL: no preregistration at $TARGETS — an artifact without one is a number chosen after the fact"; exit 1; }
[ -f "$SCRIPT_DIR/oe_corpus_reader.py" ] || { log "FATAL: oe_corpus_reader.py missing beside $0"; exit 1; }

for v in OE_CAL_TARGETS_FILE PARAMETER_SET_HASH TRACK_FROM_PUSH STOPPING_BOUNDARY_MS CORPUS_START_DATE T_SESSIONS T_COHORT \
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
import json, os, sys, hashlib, tempfile

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
# A5.6: the pin is a PUBLISHED, IMMUTABLE MANIFEST at corpus/<version>/manifest.json, and this reads it
# rather than recomputing a hash of the live archive (r7 #6). A hash of what happens to be on disk today
# matches again after a deletion; a manifest names, for every record, the (topic, generation, partition,
# offset) coordinate it occupied and the high-water mark it covers.
manifest = R.load_manifest(out_root, pinned_version)
manifest_entries = {} if manifest is None else {e["key"]: e for e in manifest.get("entries", [])}
if manifest is None:
    read_errors.append("no published manifest for corpusVersion %s — a version that was never published "
                       "is not a pin" % pinned_version[:12])
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

# The archive must still BE the corpus that manifest describes: every entry present, at the same
# coordinate, with the same digest, and nothing extra below the high-water mark.
missing_from_archive, moved, extra = [], [], []
if manifest is not None:
    coords = read["coords"]
    for k, e in manifest_entries.items():
        if k not in logical:
            missing_from_archive.append(k)
            continue
        part, off = coords.get(k, (None, None))
        # The manifest is CANONICAL JSON, in which every scalar is a string — that is what makes two
        # builders agree byte for byte. So the comparison is on canonical form, not on Python types.
        if str(logical[k][0]) != str(e["digest"]) \
                or (off is not None and str(off) != str(e["offset"])) \
                or (part is not None and str(part) != str(e["partition"])):
            moved.append(k)
    # ANY record the manifest does not name is extra, at any offset. The old test asked whether it sat
    # below the high-water mark, which made appending above the mark invisible (r8 #1).
    for k in logical:
        if k not in manifest_entries:
            extra.append(k)
version_ok = (manifest is not None and bool(published_on)
              and not missing_from_archive and not moved and not extra)

# A CONFLICT anywhere in the corpus is a defect of the corpus, and the evaluator never looked at one:
# it relied on the session walk, which sees a conflict only for a session that has a seal and whose
# conflicting record carries the same sessionDate. A5.3 is not conditional — two different contents
# under one key means the population is not knowable, whoever wrote them (r9).
conflicts_by_session = read.get("conflictsBySession", {})
conflicts_total = read.get("conflictsTotal", 0)

# A read error the SESSION walk cannot see: a file that will not open may be the only evidence a session
# existed at all, so the flat list joins the evaluator's own (r7 #3).
read_errors += read.get("readErrors", [])

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

# THE CORPUS IS THE MANIFEST. Filtering only "extra records at or below the high-water mark" left a
# gap you could drive a session through: append a favourable day ABOVE the mark, do not republish, and
# the old pin still verified while the new records joined the cohort (r8 #1). A record the pinned
# manifest does not name is not in the corpus, wherever it sits.
def manifested(rec):
    return R.physical_key(rec) in manifest_entries

calls = [c for c in calls if manifested(c)]
outcomes = [o for o in outcomes if manifested(o)]
unmanifested = sum(1 for k in logical if k not in manifest_entries)

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
if _cal is None:
    read_errors.append("no market calendar available — owed trading days cannot be enumerated")
elif not R.is_rth_close(stopping, _cal):
    # A5.8 requires it, so the boundary can never cut through an A4.12 hour row and no slicing rule is
    # needed. An arbitrary instant silently reintroduces one (r7 #1).
    read_errors.append("STOPPING_BOUNDARY_MS is not an RTH close instant on a trading day")
boundary_date = None
try:
    import datetime as _dt
    boundary_date = _dt.datetime.fromtimestamp(stopping / 1000.0, _dt.timezone.utc).date().isoformat()
except Exception:
    read_errors.append("STOPPING_BOUNDARY_MS is not an instant this evaluator can turn into a date")
# Scoped to the TARGET hash and trackFromPush: a zero-call seal under ANOTHER parameter set used to
# satisfy an owed day, so the population hole moved rather than closed (r7 #2). And the window starts at
# the DECLARED corpusStartDate — max(corpusStart, trackFrom) let a declared start before the archive's
# first day quietly shrink to the archive.
have_days = {v["sessionDate"] for v in sessions.values()
             if v["archiveStatus"] == "COMPLETE" and v.get("parameterSetHash") == phash
             and v.get("trackFromPush") == track_from}
missing_days = []
if boundary_date and _cal is not None:
    window_start = str(corpus_start)[:10]
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

# Presence is not agreement. An outcome that keeps (hash, lineage, callId) but carries another
# session's phase, clock or semantic stamp is a record from a different experiment wearing this one's
# identity, and every clause below would have counted it (r8 #2).
BOUND = ("sessionDate", "phaseAtCall", "trackFromPush", "semanticStamp")
mismatched = []
for o in window_outcomes:
    c = by_call[call_identity(o)]
    bad = [f for f in BOUND if o.get(f) != c.get(f)]
    # delivery is copied from the call row by the producer, but a call row evicted before its outcome
    # finalised legitimately yields UNKNOWN — so it is bound only when the outcome claims a value.
    if o.get("delivery") not in (None, "", "UNKNOWN") and o.get("delivery") != c.get("delivery"):
        bad.append("delivery")
    if bad:
        mismatched.append("%s|%s:%s" % (o.get("callId"), o.get("horizon"), ",".join(bad)))

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
    sid = by_call[call_identity(o)].get("sessionDate")
    results_by_session.setdefault(sid, []).append(float(o["resultTicks"]))
    hits_by_session.setdefault(sid, []).append(1.0 if float(o["resultTicks"]) > 0 else 0.0)
for o in mae:
    sid = by_call[call_identity(o)].get("sessionDate")
    mae_by_session.setdefault(sid, []).append(float(o["maeTicks"]))

# ONE common resample matrix, generated ONCE per artifact run and reused across every clause — the
# generator is never reset or advanced in clause-dependent order (A2 r10 #4). The PRNG is
# java.util.SplittableRandom seeded with BOOTSTRAP_SEED, reproduced exactly; the session list is sorted
# ascending by sessionDate so the index-to-session mapping is stable. A per-clause Python RNG was a
# different estimator that merely happened to be a bootstrap (r7 #5).
matrix_ids, MATRIX = R.resample_matrix(sorted({c.get("sessionDate") for c in cohort}), B, seed)

def bootstrap(picker, statistic, tail):
    """`picker` is keyed by sessionDate over EVERY cohort session, not only the ones that produced a
    usable observation: resampling the survivors conditions the estimate on having observed something.

    A replicate whose denominator is ZERO is EXCLUDED AND COUNTED, and if more than 1% of replicates are
    excluded the clause is NOT_EVALUABLE — silently dropping them while still claiming B replicates
    reports an interval narrower than the data supports (r7 #5)."""
    if not matrix_ids:
        return None, 0, True
    reps, excluded = [], 0
    for row in MATRIX:
        v = statistic(pooled([matrix_ids[i] for i in row], picker))
        if v is None:
            excluded += 1
        else:
            reps.append(v)
    if not reps:
        return None, excluded, True
    if excluded > 0.01 * len(MATRIX):
        return None, excluded, True
    return q_type7(reps, 0.05 if tail == "lower" else 0.95), excluded, False

def mean(xs):
    return (sum(xs) / len(xs)) if xs else None

result_lcb, result_excluded, result_ne = bootstrap(results_by_session, mean, "lower")
hit_lcb, hit_excluded, hit_ne = bootstrap(hits_by_session, mean, "lower")
median_mae_ucb, mae_excluded, mae_ne = bootstrap(mae_by_session, lambda xs: nearest_rank(xs, 50), "upper")
p90_mae_ucb, p90_excluded, p90_ne = bootstrap(mae_by_session, lambda xs: nearest_rank(xs, 90), "upper")

# A clause whose WHOLE-COHORT denominator is zero is NOT_EVALUABLE, which is a REJECT: coverage that
# cannot be demonstrated is never assumed.
if not results_by_session:
    result_ne = True
if not mae_by_session:
    mae_ne = p90_ne = True

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
               and not missing_horizon and not orphans and bool(cohort) and not read_errors
               and not mismatched and not unmanifested and not conflicts_total)
why = []
if manifest is None:
    why.append("no published manifest for corpusVersion %s" % pinned_version[:12])
if missing_from_archive:
    why.append("%d record(s) the manifest names are not in the archive" % len(missing_from_archive))
if moved:
    why.append("%d record(s) changed coordinate or digest since the manifest was published" % len(moved))
if extra:
    why.append("%d record(s) below the high-water mark are not in the manifest" % len(extra))
if manifest is not None and not published_on:
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
if mismatched:
    why.append("%d outcome(s) disagree with their call on a pinned field: %s"
               % (len(mismatched), "; ".join(mismatched[:3])))
if unmanifested:
    why.append("%d archived record(s) are not named by the pinned manifest" % unmanifested)
if conflicts_total:
    why.append("%d conflicting record(s) in the corpus (sessions: %s)"
               % (conflicts_total, ", ".join(str(k) for k in sorted(conflicts_by_session)[:4])))
if read_errors:
    # An unreadable discontinuity sidecar or progress record used to sit quietly beside an ACCEPT
    # (r6 #6). Something the reader could not read is not something the evaluator may pass over.
    why.append("%d read error(s): %s" % (len(read_errors), read_errors[0]))
clause("COMPLETENESS", "PASS" if complete_ok else "FAIL", complete_ok, len(window_sessions), "; ".join(why))

def bound_clause(name, value, cmp_floor=None, cmp_ceil=None, note="", not_evaluable=False):
    if not_evaluable or value is None:
        clause(name, "NOT_EVALUABLE", False, None, note or "no observation to estimate from")
        return
    ok = (value > cmp_floor) if cmp_floor is not None else (value <= cmp_ceil)
    clause(name, "PASS" if ok else "FAIL", ok, value, note)

bound_clause("RESULT_LCB", result_lcb, cmp_floor=f_result, not_evaluable=result_ne,
             note="LCB95 (5th pct of %d replicates, %d excluded) of mean result ticks at %s vs floor %s"
                  % (B, result_excluded, PRIMARY, f_result))
bound_clause("HIT_RATE_LCB", hit_lcb, cmp_floor=f_hit, not_evaluable=hit_ne,
             note="LCB95 of hit rate at %s (hit = resultTicks > 0, %d replicates excluded) vs floor %s"
                  % (PRIMARY, hit_excluded, f_hit))
if mae_ne or p90_ne or median_mae_ucb is None or p90_mae_ucb is None:
    clause("MAE_CEILING", "NOT_EVALUABLE", False, None,
           "no OBSERVED path to measure MAE on, or too many replicates excluded (%d/%d)" % (mae_excluded, B))
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
    "corpusVersion": pinned_version,
    "estimator": {"unit": "SESSION", "replicates": B, "seed": seed, "quantile": "TYPE_7",
                  "prng": "SplittableRandom", "matrix": "ONE_PER_RUN",
                  "replicatesExcluded": {"result": result_excluded, "hitRate": hit_excluded,
                                         "medianMae": mae_excluded, "p90Mae": p90_excluded},
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
    "manifestHighWaterMark": None if manifest is None else manifest.get("highWaterMark"),
    "manifestRecordCount": None if manifest is None else manifest.get("recordCount"),
    "archiveVsManifest": {"missing": missing_from_archive[:10], "moved": moved[:10], "extra": extra[:10]},
    "corpusStartDate": corpus_start, "owedDaysMissing": missing_days,
    "notEvaluableSessions": not_evaluable, "attritionUnusableSessions": attrition_unusable,
    "callsMissingAHorizon": missing_horizon[:20], "orphanOutcomes": orphans[:20],
    "outcomesDisagreeingWithTheirCall": mismatched[:20], "recordsNotInManifest": unmanifested,
    "conflicts": conflicts_total,
    "sessionRefusalRates": {k: round(v, 6) for k, v in sorted(refusal_by_session.items())},
    "readErrors": read_errors[:20],
    "clauseResults": clauses,
    "decision": decision,
    # NON-AUTHORIZING, stated in the record itself so no consumer has to infer it.
    "authorizing": False, "actionable": False, "slice": "COMMISSIONING_SHADOW",
    "note": "A4 evidence only. This artifact cannot satisfy A2, cannot set validationStatus, and is not a step on the A2 path.",
}
# Identity = SHA-256 over the canonical JSON with artifactId excluded, the same canonical form A1 uses —
# and with generatedAt excluded too, for the same reason the ledger's semantic digest excludes ts and
# runId: WHEN the artifact was computed is not part of what it says. Including it would give the same
# evidence, the same corpus and the same thresholds a different identity on every run, which is the
# opposite of a content address.
identity = {k: v for k, v in artifact.items() if k != "generatedAt"}
artifact["artifactId"] = hashlib.sha256(R.canonical(identity).encode("utf-8")).hexdigest()

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
