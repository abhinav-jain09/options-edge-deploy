#!/usr/bin/env bash
# push-validation-artifact-test.sh — proves the A5.8 evaluator and the shared corpus reader on a
# corpus this script builds itself: real chains, real seals, no NAS and no Kafka.
#
# Every case below is a way the evaluator could be wrong in the direction that MATTERS — accepting
# something it should not. A test that only checks the happy path would pass on an evaluator that
# returns ACCEPT unconditionally.
set -uo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$(mktemp -d)"
# The scripts under test are STAGED into a directory of their own, declaration and all, and run from
# there. There is no way to point the evaluator at another declaration — that hatch would defeat every
# refusal this suite checks — so a case that needs different targets rewrites the staged copy, which is
# exactly the file the deployed unit reads.
HERE="$WORK/unit"
mkdir -p "$HERE"
cp "$SRC/oe-push-validation-artifact.sh" "$SRC/oe-calibration-progress.sh" \
   "$SRC/oe_corpus_reader.py" "$SRC/calibration-targets.env" "$HERE/"
chmod +x "$HERE/oe-push-validation-artifact.sh" "$HERE/oe-calibration-progress.sh"
trap 'rm -rf "$WORK"' EXIT
ROOT="$WORK/kafka/prod/context-tape.direction.ledger"
PH="a1b2c3d4e5f60718"
TF="2026-07-01"
SB="$(python3 -c "import datetime;print(int(datetime.datetime.fromisoformat('2026-08-13T20:00:00+00:00').timestamp()*1000))")"
fails=0
# the REAL trading calendar, which the reader now requires rather than falling back to weekdays
CAL_DIR="$(cd "$SRC/../../jenkins" 2>/dev/null && pwd || true)"
[ -f "$CAL_DIR/market_calendar.py" ] || CAL_DIR="$HOME/development/workspace/options-edge-deploy/scripts/jenkins"
[ -f "$CAL_DIR/market_calendar.py" ] || { echo "FATAL: no market_calendar.py for the suite to use"; exit 1; }
export CAL_DIR
ok()   { printf '  ok   %s\n' "$1"; }
bad()  { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

build() {   # build <sessions> <calls-per-session> [mutation]
  SESSIONS="$1" PER="$2" MUTATE="${3:-none}" ROOT="$ROOT" PH="$PH" TF="$TF" python3 - <<'PY'
import gzip, json, os, hashlib, random, shutil, datetime
root, ph, tf = os.environ["ROOT"], os.environ["PH"], os.environ["TF"]
n_sessions, per, mutate = int(os.environ["SESSIONS"]), int(os.environ["PER"]), os.environ["MUTATE"]
HOR = ["H3", "H5", "H15"]
CELLS = ["EXHAUSTED|CALL_WALL|POS_GAMMA", "EXHAUSTED|PUT_WALL|POS_GAMMA", "EXHAUSTED|CALL_WALL|NEG_GAMMA",
         "EXHAUSTED|PUT_WALL|NEG_GAMMA", "STRONG|CALL_WALL|NEG_GAMMA", "STRONG|PUT_WALL|NEG_GAMMA",
         "EXHAUSTED|NONE|POS_GAMMA", "EXHAUSTED|NONE|NEG_GAMMA", "STRONG|NONE|NEG_GAMMA",
         "STRONG|NONE|POS_GAMMA"]

def canonical(o):
    if isinstance(o, dict):
        return "{" + ",".join('%s:%s' % (json.dumps(k), canonical(v)) for k, v in sorted(o.items())) + "}"
    if isinstance(o, list): return "[" + ",".join(canonical(v) for v in o) + "]"
    if o is None: return "null"
    if isinstance(o, bool): return "true" if o else "false"
    if isinstance(o, str): return json.dumps(o)
    return json.dumps(str(o))

def dig(rec):
    return hashlib.sha256(canonical({k: v for k, v in rec.items()
        if k not in ("ts", "publishedAtMs", "runId", "semanticDigest")}).encode()).hexdigest()

def pkey(rec):
    if rec["kind"] == "seal":
        return "%s|%s|%s" % (rec["sessionDate"], rec["parameterSetHash"], rec["sessionLineageId"])
    lid = rec["callId"] if rec["kind"] == "call" else "%s|%s" % (rec["callId"], rec["horizon"])
    return "%s|%s|%s" % (rec["parameterSetHash"], rec["sessionLineageId"], lid)

def chain(domain, recs):
    d = b"\x00" * 32
    for off, k, dg in sorted(recs, key=lambda r: (r[0], r[1])):
        kb, raw = k.encode(), bytes.fromhex(dg)
        d = hashlib.sha256(d + bytes([domain]) + len(kb).to_bytes(4, "big") + kb
                           + len(raw).to_bytes(4, "big") + raw).digest()
    return d.hex()

shutil.rmtree(root, ignore_errors=True)
rnd = random.Random(7)
d0 = datetime.date.fromisoformat(tf)
offset, made = 0, 0
while made < n_sessions:
    if d0.weekday() >= 5:
        d0 += datetime.timedelta(days=1); continue
    sd = d0.isoformat(); made += 1
    lin = "lin-%s" % sd
    os.makedirs(os.path.join(root, "dt=%s" % sd), exist_ok=True)
    lines, crecs, orecs = [], [], []
    first = offset
    base_t = int(datetime.datetime.fromisoformat(sd + "T14:00:00+00:00").timestamp() * 1000)
    for i in range(per):
        cell = CELLS[i % len(CELLS)]
        st, roles, regime = cell.split("|")
        cid = "%s-c%02d" % (sd, i)
        c = {"kind": "call", "callId": cid, "parameterSetHash": ph, "sessionLineageId": lin,
             "sessionDate": sd, "phaseAtCall": "VALIDATION", "trackFromPush": tf, "delivery": "LIVE",
             "predictedSign": 1 if i % 2 == 0 else -1, "enteredState": st, "regime": regime,
             "node": {"roles": [] if roles == "NONE" else [roles]},
             "semanticStamp": "2026-09-08T18:00:00Z",
             "refT": base_t + i * 60000, "ts": base_t, "runId": "r1"}
        k, dg = pkey(c), dig(c)
        lines.append((offset, k, json.dumps(c))); crecs.append((offset, k, dg)); offset += 1
        for h in HOR:
            res = rnd.choice([3, 5, -2, 7, -1, 4])
            o = {"kind": "outcome", "callId": cid, "horizon": h, "parameterSetHash": ph,
                 "sessionLineageId": lin, "sessionDate": sd, "phaseAtCall": "VALIDATION",
                 "trackFromPush": tf, "delivery": "LIVE", "resultState": "OBSERVED",
                 "resultTicks": res, "pathState": "OBSERVED", "maeTicks": abs(res) + 2,
                 "mfeTicks": abs(res) + 5, "cellKey": "%s|%s" % (cell, h),
                 "semanticStamp": "2026-09-08T18:00:00Z",
                 "refT": base_t + i * 60000, "ts": base_t, "runId": "r1"}
            k, dg = pkey(o), dig(o)
            lines.append((offset, k, json.dumps(o))); orecs.append((offset, k, dg)); offset += 1
    att = [{"sessionDate": sd, "etHour": 10, "graded": 100, "NO_TICKS": 3},
           {"sessionDate": sd, "etHour": 11, "graded": 100, "NO_TICKS": 2}]
    if mutate == "blind_day" and made == 3:
        # ONE blind session out of thirty. A pooled rate would drown it; the per-session gate must not.
        att = [{"sessionDate": sd, "etHour": 10, "graded": 100, "NO_TICKS": 95},
               {"sessionDate": sd, "etHour": 11, "graded": 100, "NO_TICKS": 90}]
    if mutate == "ungraded_day" and made == 3:
        att = [{"sessionDate": sd, "etHour": 10, "graded": 0}]
    seal = {"kind": "seal", "sessionDate": sd, "parameterSetHash": ph, "sessionLineageId": lin,
            "phaseAtCall": "VALIDATION", "trackFromPush": tf, "delivery": "SEAL",
            "ledgerTopic": "context-tape.direction.ledger",
            "generation": "3f2a1c04-5b6d-4e7f-8a9b-0c1d2e3f4a5b",
            "semanticStamp": "2026-09-08T18:00:00Z",
            "logicalCallCount": len(crecs), "logicalOutcomeCount": len(orecs),
            "callsDigest": chain(0x01, crecs), "outcomesDigest": chain(0x02, orecs),
            "firstOffset": first, "lastOffset": offset - 1, "conflicts": 0,
            "attrition": att, "ts": base_t, "runId": "r1"}
    if mutate == "broken_chain" and made == 3:
        seal["callsDigest"] = "0" * 64        # a seal whose chain the archive cannot reproduce
    lines.append((offset, pkey(seal), json.dumps(seal))); offset += 1
    with gzip.open(os.path.join(root, "dt=%s" % sd, "part-000.jsonl.gz"), "wt") as fh:
        for off, k, payload in lines:
            fh.write("Offset:%d %s\t%s\n" % (off, k, payload))
    d0 += datetime.timedelta(days=1)
PY
}

version_of() {   # the corpusVersion the reader computes for what is on disk right now
  ROOT="$ROOT" HERE="$HERE" python3 -c "
import os, sys
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
print(R.corpus_version(R.read_logical(os.environ['ROOT'])['logical']))"
}

# The preregistration under test. Everything the evaluator is not allowed to take from the caller lives
# here, so a case that wants a different boundary or a different threshold must DECLARE it — which is
# itself the property being tested.
targets() {   # targets <thresholds-state> <boundary-ms> [B]
  cat > "$HERE/calibration-targets.env" <<TEOF
OE_CAL_TOPIC=context-tape.direction.ledger
OE_CAL_PARAMETER_SET_HASH_prod=$PH
OE_CAL_TRACK_FROM_PUSH_prod=$TF
OE_CAL_STOPPING_BOUNDARY_MS_prod=$2
OE_CAL_CORPUS_START_DATE_prod=$TF
OE_CAL_T_SESSIONS=30
OE_CAL_T_COHORT=200
OE_CAL_T_CLASS=50
OE_CAL_T_CELL=50
OE_CAL_REQUIRED_CLASSES="1 -1"
OE_CAL_REQUIRED_CELLS="EXHAUSTED|CALL_WALL|POS_GAMMA EXHAUSTED|PUT_WALL|POS_GAMMA EXHAUSTED|CALL_WALL|NEG_GAMMA EXHAUSTED|PUT_WALL|NEG_GAMMA STRONG|CALL_WALL|NEG_GAMMA STRONG|PUT_WALL|NEG_GAMMA EXHAUSTED|NONE|POS_GAMMA EXHAUSTED|NONE|NEG_GAMMA STRONG|NONE|NEG_GAMMA STRONG|NONE|POS_GAMMA"
OE_CAL_BOOTSTRAP_SEED=20260906
OE_CAL_BOOTSTRAP_B=${3:-300}
OE_CAL_THRESHOLDS_STATE=$1
OE_CAL_RESULT_LCB_FLOOR=0
OE_CAL_HIT_RATE_LCB_FLOOR=0.4
OE_CAL_MEDIAN_MAE_CEIL=20
OE_CAL_P90_MAE_CEIL=20
OE_CAL_COVERAGE_FLOOR=0.9
OE_CAL_ATTRITION_CEIL=0.10
TEOF
}

# The pin must name a version a PROGRESS RUN published, so every case publishes one first — which is
# also the only way the reporter and the evaluator are shown to be talking about the same corpus.
publish() {
  env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 \
      CALENDAR_DIR="$CAL_DIR" bash "$HERE/oe-calibration-progress.sh" > "$WORK/publish.log" 2>&1 \
      || { echo "  FAIL the progress run did not publish:"; sed -n '1,6p' "$WORK/publish.log"; fails=$((fails+1)); }
}

# The pinned version is the one the LAST progress run published, read back from its own record — never
# recomputed here, which is precisely the shortcut the evaluator refuses.
published_version() {
  WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/progress/dt=*.json'), key=os.path.getmtime)
print(json.load(open(f[-1]))['corpusVersion'] if f else 'NONE')"
}

evaluate() {   # evaluate [corpusVersion override]
  env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 CALENDAR_DIR="$CAL_DIR" \
      CORPUS_VERSION="${1:-$(published_version)}" bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | head -1
}

echo "1. a complete corpus with frozen, reachable thresholds ACCEPTS"
build 32 20
targets FROZEN "$SB"; publish
OUT="$(evaluate)"
case "$OUT" in
  *"decision=ACCEPT"*) ok "ACCEPT on a complete corpus";;
  *) bad "expected ACCEPT, got: $OUT"
     # a failing positive fixture is useless without the reason, and the reason is in the artifact
     WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/artifacts/*.json'), key=os.path.getmtime)
d = json.load(open(f[-1]))
print('       why:', [c for c in d['clauseResults'] if c['clause'] == 'COMPLETENESS'][0]['note'][:400])
print('       archiveVsManifest:', json.dumps(d.get('archiveVsManifest'))[:300])
print('       readErrors:', d.get('readErrors', [])[:3])";;
esac
for c in COHORT_SIZE COMPLETENESS RESULT_LCB HIT_RATE_LCB MAE_CEILING COVERAGE ATTRITION_CEILING THRESHOLDS_FROZEN; do
  case "$OUT" in *"$c=PASS"*) :;; *) bad "$c did not pass on the positive fixture: $OUT";; esac
done
ok "every clause is present and named in the output"

echo "2. PROVISIONAL thresholds can never ACCEPT, however good the data is"
targets PROVISIONAL_PENDING_MEASUREMENT "$SB"; publish
OUT="$(evaluate)"
case "$OUT" in *"THRESHOLDS_FROZEN=FAIL"*) ok "numbers nobody has committed to cannot certify anything";; *) bad "provisional thresholds accepted: $OUT";; esac
case "$OUT" in *"decision=REJECT"*) ok "and the decision follows";; *) bad "expected REJECT: $OUT";; esac

echo "3. the caller cannot supply anything that is preregistered"
targets FROZEN "$SB"
OUT="$(env ENV=prod ARCHIVE_DIR="$WORK" CORPUS_VERSION=x \
       COVERAGE_FLOOR=0.1 bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | tail -1)"
case "$OUT" in *"cannot be supplied by the caller"*) ok "a threshold passed in is refused, not honoured";; *) bad "override accepted: $OUT";; esac
OUT="$(env ENV=prod ARCHIVE_DIR="$WORK" CORPUS_VERSION=x \
       STOPPING_BOUNDARY_MS=1 bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | tail -1)"
case "$OUT" in *"cannot be supplied by the caller"*) ok "the stopping boundary cannot be moved from outside";; *) bad "boundary override accepted: $OUT";; esac

echo "4. an UNFROZEN boundary refuses to evaluate at all"
targets FROZEN UNFROZEN
OUT="$(env ENV=prod ARCHIVE_DIR="$WORK" CORPUS_VERSION=x \
       bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | tail -1)"
case "$OUT" in *"still UNFROZEN"*) ok "no artifact exists before the boundary is frozen";; *) bad "ran without a boundary: $OUT";; esac

echo "5. a corpusVersion that is not the one on disk fails COMPLETENESS"
targets FROZEN "$SB"; publish
OUT="$(evaluate "$(printf '0%.0s' $(seq 64))")"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "an unpinned corpus is not silently evaluated";; *) bad "pin not enforced: $OUT";; esac

echo "6. a version nobody PUBLISHED fails, even when it matches what is on disk"
rm -rf "$WORK/calibration-runs"
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "a self-computed pin is not a pin";; *) bad "unpublished version accepted: $OUT";; esac

echo "7. REMOVING a whole trading day is caught, even after the pin is recomputed and republished"
build 32 20
targets FROZEN "$SB"; publish
victim="$(ls -d "$ROOT"/dt=* | sed -n '5p')"
rm -rf "$victim"
publish
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "an owed trading day that is gone is the verdict: $(basename "$victim")";; *) bad "a deleted day produced a smaller accepted cohort: $OUT";; esac
case "$OUT" in *"decision=REJECT"*) ok "decision follows COMPLETENESS";; *) bad "expected REJECT: $OUT";; esac

echo "8. ONE unreproducible seal inside the window makes the artifact REJECT, not a smaller cohort"
build 32 20 broken_chain
targets FROZEN "$SB"; publish
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "a NOT_EVALUABLE session is the verdict, not a warning";; *) bad "broken chain tolerated: $OUT";; esac

echo "9. ONE blind session out of thirty fails ATTRITION_CEILING (a pooled rate would hide it)"
build 32 20 blind_day
targets FROZEN "$SB"; publish
OUT="$(evaluate)"
case "$OUT" in *"ATTRITION_CEILING=FAIL"*) ok "the gate is per session, not pooled";; *) bad "blind day hidden: $OUT";; esac

echo "10. a session that graded nothing is NOT_EVALUABLE, never a refusal rate of zero"
build 32 20 ungraded_day
targets FROZEN "$SB"; publish
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "an ungraded session cannot contribute a 0% refusal";; *) bad "ungraded day counted: $OUT";; esac

echo "11. the stopping boundary is honoured — an earlier declared boundary yields a smaller cohort"
build 32 20
EARLY="$(python3 -c "import datetime;print(int(datetime.datetime.fromisoformat('2026-07-10T20:00:00+00:00').timestamp()*1000))")"
targets FROZEN "$EARLY"; publish
OUT="$(evaluate)"
case "$OUT" in *"COHORT_SIZE=FAIL"*) ok "calls after the preregistered boundary are excluded";; *) bad "boundary ignored: $OUT";; esac

echo "12. the artifactId is a content address: same inputs same id, changed threshold different id"
build 32 20
targets FROZEN "$SB"; publish
A="$(evaluate | sed -n 's/^PushValidationArtifact \([0-9a-f]*\).*/\1/p')"
B2="$(evaluate | sed -n 's/^PushValidationArtifact \([0-9a-f]*\).*/\1/p')"
sed -i'' -e 's/OE_CAL_COVERAGE_FLOOR=0.9/OE_CAL_COVERAGE_FLOOR=0.5/' "$HERE/calibration-targets.env"
C="$(evaluate | sed -n 's/^PushValidationArtifact \([0-9a-f]*\).*/\1/p')"
{ [ -n "$A" ] && [ "$A" = "$B2" ]; } && ok "identical inputs reproduce the artifactId ($A)" || bad "artifactId not reproducible: $A vs $B2"
[ "$A" != "$C" ] && ok "a changed threshold changes the identity" || bad "artifactId ignores the thresholds"

echo "13. the estimator is deterministic, and it is the one the artifact declares"
targets FROZEN "$SB"; publish; evaluate >/dev/null
V1="$(WORKDIR="$WORK" PHASH="$PH" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/' + os.environ['PHASH'] + '/*/artifacts/*.json'), key=os.path.getmtime)
d = json.load(open(f[-1]))
print([c['value'] for c in d['clauseResults'] if c['clause'] == 'RESULT_LCB'][0], d['estimator']['unit'], d['estimator']['quantile'])")"
case "$V1" in *"SESSION TYPE_7") ok "RESULT_LCB reproduces as $V1";; *) bad "estimator not as declared: $V1";; esac

echo "14. the artifact says, in the record itself, that it authorizes nothing"
if WORKDIR="$WORK" python3 -c "
import json, glob, os, sys
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/artifacts/*.json'), key=os.path.getmtime)[-1]
d = json.load(open(f))
missing = [k for k, v in (('authorizing', False), ('actionable', False), ('slice', 'COMMISSIONING_SHADOW')) if d.get(k) != v]
sys.exit(1 if missing else 0)"; then
  ok "authorizing=false actionable=false slice=COMMISSIONING_SHADOW"
else
  bad "the artifact does not carry its non-authorizing labels"
fi

echo "15. the SHIPPED preregistration is the conservative one"
grep -q '^OE_CAL_THRESHOLDS_STATE=PROVISIONAL_PENDING_MEASUREMENT' "$SRC/calibration-targets.env" \
  && ok "the shipped thresholds are PROVISIONAL, so no artifact can ACCEPT yet" \
  || bad "the shipped declaration claims FROZEN thresholds"
grep -q '^OE_CAL_BOOTSTRAP_B=10000' "$SRC/calibration-targets.env" \
  && ok "the shipped replicate count is A2's 10000 (the cases above use fewer, on purpose)" \
  || bad "the shipped B is not 10000"
grep -q '^OE_CAL_STOPPING_BOUNDARY_MS_prod=UNFROZEN' "$SRC/calibration-targets.env" \
  && ok "the shipped stopping boundary is UNFROZEN, as it must be before the literals are chosen" \
  || bad "the shipped stopping boundary is already set"


echo "16. a market HOLIDAY is not an owed trading day"
build 32 20
targets FROZEN "$SB"; publish
OUT="$(evaluate)"
case "$OUT" in *"decision=ACCEPT"*) ok "2026-07-03 (Independence Day observed) is not owed";; *) bad "a holiday was owed: $OUT";; esac
CAL_DIR=/nonexistent-calendar bash -c '
  env ENV=prod ARCHIVE_DIR="'"$WORK"'" REPORT_DATE=2026-08-13 CALENDAR_DIR=/nonexistent \
      bash "'"$HERE"'/oe-calibration-progress.sh"' >/dev/null 2>&1 \
  && bad "the reporter ran without a calendar" \
  || ok "no calendar is FATAL, never a silent weekday fallback"

echo "17. a zero-call seal under ANOTHER hash does not satisfy an owed day"
build 32 20
targets FROZEN "$SB"; publish
victim="$(ls -d "$ROOT"/dt=* | sed -n '5p')"
vd="$(basename "$victim" | sed 's/dt=//')"
rm -rf "$victim"
VD="$vd" ROOT="$ROOT" python3 - <<'PYCASE'
import gzip, json, os, hashlib
root, sd = os.environ["ROOT"], os.environ["VD"]
def canonical(o):
    if isinstance(o, dict):
        return "{" + ",".join('%s:%s' % (json.dumps(k), canonical(v)) for k, v in sorted(o.items())) + "}"
    if isinstance(o, list): return "[" + ",".join(canonical(v) for v in o) + "]"
    if o is None: return "null"
    if isinstance(o, bool): return "true" if o else "false"
    if isinstance(o, str): return json.dumps(o)
    return json.dumps(str(o))
def chain(domain, recs):
    d = b"\x00" * 32
    for off, k, dg in sorted(recs, key=lambda r: (r[0], r[1])):
        kb, raw = k.encode(), bytes.fromhex(dg)
        d = hashlib.sha256(d + bytes([domain]) + len(kb).to_bytes(4, "big") + kb
                           + len(raw).to_bytes(4, "big") + raw).digest()
    return d.hex()
other = "ffffffffffffffff"
lin = "lin-other-%s" % sd
seal = {"kind": "seal", "sessionDate": sd, "parameterSetHash": other, "sessionLineageId": lin,
        "phaseAtCall": "VALIDATION", "trackFromPush": "2026-07-01", "delivery": "SEAL",
        "ledgerTopic": "context-tape.direction.ledger",
        "generation": "3f2a1c04-5b6d-4e7f-8a9b-0c1d2e3f4a5b",
        "semanticStamp": "2026-09-08T18:00:00Z",
        "logicalCallCount": 0, "logicalOutcomeCount": 0,
        "callsDigest": chain(0x01, []), "outcomesDigest": chain(0x02, []),
        "firstOffset": None, "lastOffset": None, "conflicts": 0,
        "attrition": [{"sessionDate": sd, "etHour": 10, "graded": 10, "NO_TICKS": 1}],
        "ts": 0, "runId": "r1"}
key = "%s|%s|%s" % (sd, other, lin)
os.makedirs(os.path.join(root, "dt=%s" % sd), exist_ok=True)
with gzip.open(os.path.join(root, "dt=%s" % sd, "part-000.jsonl.gz"), "wt") as fh:
    fh.write("Offset:999999 %s\t%s\n" % (key, json.dumps(seal)))
PYCASE
publish
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "another parameter set's seal does not fill this cohort's hole";; *) bad "a foreign seal satisfied an owed day: $OUT";; esac

echo "18. a declared corpusStartDate BEFORE the archive begins owes those days"
build 32 20
targets FROZEN "$SB"; publish
sed -i'' -e 's/^OE_CAL_CORPUS_START_DATE_prod=.*/OE_CAL_CORPUS_START_DATE_prod=2026-06-22/' "$HERE/calibration-targets.env"
publish
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "the window is the DECLARED one, not whatever the archive starts at";; *) bad "a declared earlier start was ignored: $OUT";; esac

echo "19. an unreadable archive file cannot sit beside an ACCEPT"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
printf 'this is not gzip' > "$ROOT/dt=2026-07-09/part-999.jsonl.gz"
OUT="$(evaluate "$PIN")"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "a file the reader could not open is part of the verdict";; *) bad "an unreadable file was passed over: $OUT";; esac

echo "20. a record that MOVED since the manifest was published fails"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, sys, os
f = sorted(glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")))[3]
lines = gzip.open(f, "rt").read().splitlines()
lines[0] = lines[0].replace("Offset:", "Offset:1", 1)      # same record, another coordinate
with gzip.open(f, "wt") as fh:
    fh.write("\n".join(lines) + "\n")
PYCASE
OUT="$(evaluate "$PIN")"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "the archive must still BE the corpus the manifest names";; *) bad "a moved record was accepted: $OUT";; esac

echo "21. the PRNG is java.util.SplittableRandom, not a lookalike"
HERE="$HERE" python3 -c "
import sys, os
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
r = R.SplittableRandom(20260906)
got = [r.next_int(32) for _ in range(8)]
# these are java.util.SplittableRandom(20260906L).nextInt(32) x8, captured from a real JVM
want = [3, 8, 11, 23, 1, 28, 29, 27]
sys.exit(0 if got == want else 1)" \
  && ok "nextInt reproduces the JVM's own sequence" \
  || bad "the PRNG does not match java.util.SplittableRandom"

echo
if [ $fails -eq 0 ]; then echo "PASS — the A5.8 evaluator holds on every case"; exit 0; fi
echo "FAIL — $fails assertion(s)"; exit 1
