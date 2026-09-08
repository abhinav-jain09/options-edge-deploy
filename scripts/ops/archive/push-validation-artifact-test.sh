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
# "quiet" means the records are CALIBRATION from the start, so the seal's chains are computed over those
# bodies and the sessions are genuinely whole. Rewriting the phase afterwards only corrupts them, which
# tests the corruption path and not the quiet one.
PHASE = "CALIBRATION" if mutate == "quiet" else "VALIDATION"
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
# The archiver records the log's TopicId beside the data; without it a manifest has no generation and
# is not a coordinate, so a realistic fixture has one. Kafka's CLI spelling (base64url), deliberately —
# the reader is what normalises it to the producer's canonical hex UUID.
man = os.path.join(os.path.dirname(root.rstrip("/")), "_manifest")
os.makedirs(man, exist_ok=True)
with open(os.path.join(man, "context-tape.direction.ledger.identity"), "w") as fh:
    fh.write("topic_id=PyocBFttTn-KmwwdLj9KWw\nobserved=2026-09-08T00:00:00Z\n")
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
             "sessionDate": sd, "phaseAtCall": PHASE, "trackFromPush": tf, "delivery": "LIVE",
             "predictedSign": 1 if i % 2 == 0 else -1, "enteredState": st, "regime": regime,
             "node": {"roles": [] if roles == "NONE" else [roles]},
             "semanticStamp": "2026-09-08T18:00:00Z",
             "refT": base_t + i * 60000, "ts": base_t, "runId": "r1"}
        k, dg = pkey(c), dig(c)
        lines.append((offset, k, json.dumps(c))); crecs.append((offset, k, dg)); offset += 1
        for h in HOR:
            res = rnd.choice([3, 5, -2, 7, -1, 4])
            o = {"kind": "outcome", "callId": cid, "horizon": h, "parameterSetHash": ph,
                 "sessionLineageId": lin, "sessionDate": sd, "phaseAtCall": PHASE,
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
            "phaseAtCall": PHASE, "trackFromPush": tf, "delivery": "SEAL",
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
            # the archiver's consumer runs with print.partition=true AND print.offset=true, so a real
            # archive line carries both; a fixture without Partition: was letting a manifest of -1s
            # verify against a corpus with no coordinates (r12 #3)
            fh.write("Partition:0 Offset:%d %s\t%s\n" % (off, k, payload))
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
OE_CAL_SEMANTIC_STAMP_prod=2026-09-08T18:00:00Z
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

# A REJECT can be had by breaking almost anything, so a case that wants a SPECIFIC protection must read
# the reason out of the artifact. Case 33 was found passing for the wrong reason; this is that check,
# made reusable so the next case does not have to re-invent it.
why_says() {   # why_says <substring> <ok message> <fail message>
  if [ -z "$LAST_ARTIFACT" ]; then bad "$3 (the run named no artifact)"; return; fi
  if ARTIFACT="$LAST_ARTIFACT" NEEDLE="$1" python3 -c "
import json, os, sys
d = json.load(open(os.environ['ARTIFACT']))
note = [c for c in d['clauseResults'] if c['clause'] == 'COMPLETENESS'][0]['note']
sys.exit(0 if os.environ['NEEDLE'] in note else 1)"; then
    ok "$2"
  else
    bad "$3"
  fi
}

# The evaluator NEVER rewrites a published artifact, so "the newest file by mtime" is not the artifact
# this run produced — it is whichever run last created one. Every evaluate records the path its own run
# named, and why_says reads THAT.
LAST_ARTIFACT=""
evaluate() {   # evaluate [corpusVersion override]
  local out
  out="$(env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 CALENDAR_DIR="$CAL_DIR" \
         CORPUS_VERSION="${1:-$(published_version)}" bash "$HERE/oe-push-validation-artifact.sh" 2>&1)"
  LAST_ARTIFACT="$(printf '%s\n' "$out" | sed -n 's/^written to //p' | tail -1)"
  printf '%s\n' "$out" | head -1
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
    fh.write("Partition:0 Offset:999999 %s\t%s\n" % (key, json.dumps(seal)))
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
evaluate "$PIN" >/dev/null
# The reason, not the verdict: this case broke the seal envelope as well as the coordinate, so removing
# the `moved` protection left it green on a completely different failure (r14 #5).
why_says "changed coordinate or digest" \
  "the archive must still BE the corpus the manifest names" \
  "it rejected, but not because a record moved"

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


echo "22. a record appended ABOVE the manifest's high-water mark is not in the corpus"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, json, os, sys, hashlib
# a whole extra call+outcomes, at offsets far above anything the manifest saw
root = sys.argv[1]
d = sorted(glob.glob(os.path.join(root, "dt=*")))[-1]
sd = os.path.basename(d).replace("dt=", "")
lin = "lin-%s" % sd
rows = []
cid = "%s-cZZ" % sd
base = {"parameterSetHash": "a1b2c3d4e5f60718", "sessionLineageId": lin, "sessionDate": sd,
        "phaseAtCall": "VALIDATION", "trackFromPush": "2026-07-01", "delivery": "LIVE",
        "semanticStamp": "2026-09-08T18:00:00Z", "ts": 0, "runId": "r1"}
call = dict(base, kind="call", callId=cid, predictedSign=1, enteredState="EXHAUSTED",
            regime="POS_GAMMA", node={"roles": ["CALL_WALL"]},
            refT=int(__import__("datetime").datetime.fromisoformat(sd + "T14:30:00+00:00").timestamp() * 1000))
rows.append(("%s|%s|%s" % (base["parameterSetHash"], lin, cid), call))
for h in ("H3", "H5", "H15"):
    o = dict(base, kind="outcome", callId=cid, horizon=h, resultState="OBSERVED", resultTicks=9,
             pathState="OBSERVED", maeTicks=1, mfeTicks=9, cellKey="EXHAUSTED|CALL_WALL|POS_GAMMA|%s" % h)
    rows.append(("%s|%s|%s|%s" % (base["parameterSetHash"], lin, cid, h), o))
with gzip.open(os.path.join(d, "part-900.jsonl.gz"), "wt") as fh:
    for i, (k, r) in enumerate(rows):
        fh.write("Partition:0 Offset:%d %s\t%s\n" % (900000 + i, k, json.dumps(r)))
PYCASE
OUT="$(evaluate "$PIN")"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "a record above the high-water mark is still not in the pinned corpus";; *) bad "records appended above the mark joined the cohort: $OUT";; esac

echo "23. an outcome that keeps the identity but changes a pinned VALUE is caught"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, json, os, sys
f = sorted(glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")))[6]
out = []
for line in gzip.open(f, "rt"):
    i = line.find("{")
    rec = json.loads(line[i:])
    if rec.get("kind") == "outcome":
        # same (hash, lineage, callId) — another experiment's clock and stamp
        rec["trackFromPush"] = "2020-01-01"
        rec["semanticStamp"] = "1999-01-01T00:00:00Z"
    out.append(line[:i] + json.dumps(rec) + "\n")
with gzip.open(f, "wt") as fh:
    fh.write("".join(out))
PYCASE
publish
evaluate >/dev/null
# Same lesson: rewriting the outcomes also broke the seal chain, so the `mismatched` protection could be
# removed with this case still green (r14 #5).
why_says "disagree with their call on a pinned field" \
  "presence is not agreement — a foreign clock on a familiar id is caught" \
  "it rejected, but not because the outcome disagreed with its call"

echo "24. the manifest carries the generation, the declared start and the calendar it used"
build 32 20
targets FROZEN "$SB"
publish
WORKDIR="$WORK" python3 -c "
import json, glob, os, sys
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/corpus/*/manifest.json'), key=os.path.getmtime)[-1]
m = json.load(open(f))
gen = m.get('generation')
cal = m.get('calendar') or {}
ok = (gen == '3f2a1c04-5b6d-4e7f-8a9b-0c1d2e3f4a5b'
      and m.get('corpusStartDate') == '2026-07-01'
      and isinstance(cal.get('module'), str) and len(cal.get('module','')) == 64)
print('       generation=%s corpusStartDate=%s calendar=%s' % (gen, m.get('corpusStartDate'), bool(cal)))
sys.exit(0 if ok else 1)" \
  && ok "the coordinate is a coordinate: Kafka's TopicId normalised to the producer's spelling" \
  || bad "the manifest omits its generation, its declared start or its calendar"

echo "25. the watchdog reads the report's composite session key and exits nonzero on a bad day"
build 3 20
targets FROZEN "$SB"; publish
# The watchdog checks the report FOR a day: it looks up dt=<day>.json and then asks whether that day's
# session inside it is COMPLETE. So the report has to BE for that day — which is exactly the shape the
# 07:00 ET run sees, checking the previous trading day against the 21:00 ET report.
DAY="$(ls -d "$ROOT"/dt=* | sed -n '2p' | sed 's|.*dt=||')"
env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE="$DAY" CALENDAR_DIR="$CAL_DIR" \
    bash "$HERE/oe-calibration-progress.sh" >/dev/null 2>&1
if CHECK_DATE="$DAY" ENV=prod ARCHIVE_DIR="$WORK" CALENDAR_DIR="$CAL_DIR" bash "$SRC/calibration-progress-watch.sh" >"$WORK/watch.log" 2>&1; then
  grep -q "archiveStatus=COMPLETE" "$WORK/watch.log" \
    && ok "a healthy session under a composite key reads COMPLETE, not NOT_IN_CORPUS" \
    || bad "the watchdog passed without recognising the session: $(head -3 "$WORK/watch.log")"
else
  bad "the watchdog failed on a healthy day: $(head -4 "$WORK/watch.log")"
fi
if CHECK_DATE=2026-01-02 ENV=prod ARCHIVE_DIR="$WORK" CALENDAR_DIR="$CAL_DIR" bash "$SRC/calibration-progress-watch.sh" >/dev/null 2>&1; then
  bad "the watchdog exited 0 on a day it alerted about"
else
  ok "an alert exits nonzero, so launchd and cron can see it"
fi


echo "26. the manifest is minted under the ARCHIVER'S OWN lock, not a lookalike"
build 3 20
targets FROZEN "$SB"
# the exact key oe-archive-kafka.sh builds for this (env, ARCHIVE_DIR, topic)
_dk="$(printf '%s' "$WORK" | cksum | cut -d' ' -f1)"
_tk="$(printf '%s' "context-tape.direction.ledger" | tr -c 'A-Za-z0-9._-' '_')"
ARCHIVER_LOCK="/tmp/oe-archive-kafka.prod.$_dk.t-$_tk.lock"
if command -v flock >/dev/null 2>&1; then
  ( exec 7>"$ARCHIVER_LOCK"; flock 7; sleep 6 ) &
  holder=$!
  sleep 1
  start=$(date +%s)
  env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 CALENDAR_DIR="$CAL_DIR" \
      bash "$HERE/oe-calibration-progress.sh" >/dev/null 2>&1
  waited=$(( $(date +%s) - start ))
  wait $holder 2>/dev/null || true
  [ "$waited" -ge 4 ] \
    && ok "the reporter waited ${waited}s for the archive lock instead of snapshotting through it" \
    || bad "the reporter did not wait (${waited}s) — its lock key does not match the archiver's"
else
  ok "no flock on this host; the reporter says so out loud rather than pretending it locked"
fi
# The key derivation is checkable WITHOUT flock, and it is the half that actually goes wrong: the first
# version of this used a plausible-looking key of its own and therefore excluded nothing. Both sides are
# read out of the two scripts and compared.
ARCH_LINE="$(grep -m1 '^  topic_lock=' "$SRC/oe-archive-kafka.sh")"
REP_LINE="$(grep -m1 '^SNAPSHOT_LOCK=' "$SRC/oe-calibration-progress.sh")"
a="$(ENV_NAME=prod _dir_key=K topic=context-tape.direction.ledger bash -c "${ARCH_LINE#  }"'; printf "%s" "$topic_lock"')"
b="$(ENV_NAME=prod _dir_key=K LEDGER_TOPIC=context-tape.direction.ledger bash -c '_topic_key="$(printf "%s" "$LEDGER_TOPIC" | tr -c "A-Za-z0-9._-" "_")"; '"${REP_LINE}"'; printf "%s" "$SNAPSHOT_LOCK"')"
[ -n "$a" ] && [ "$a" = "$b" ]   && ok "the reporter's lock path IS the archiver's: $a"   || bad "lock paths differ — archiver=[$a] reporter=[$b]"


echo "27. a conflicting record anywhere in the corpus is the verdict"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, json, os, sys
# the SAME key with different content: A5.3's conflict. Written into a session that is otherwise whole.
f = sorted(glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")))[8]
first = None
for line in gzip.open(f, "rt"):
    i = line.find("{")
    rec = json.loads(line[i:])
    if rec.get("kind") == "call":
        first = (line[:i], rec)
        break
prefix, rec = first
rec["predictedSign"] = -rec.get("predictedSign", 1)          # same key, different content
with gzip.open(f.replace(".jsonl.gz", ".dup.jsonl.gz"), "wt") as fh:
    fh.write(prefix + json.dumps(rec) + "\n")
PYCASE
publish
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "two contents under one key means the population is not knowable";; *) bad "a conflict produced an ACCEPT: $OUT";; esac

echo "28. the generation comes from THIS topic's identity file, not whichever sorts first"
HERE="$HERE" python3 -c "
import sys, os, tempfile, pathlib
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
td = tempfile.mkdtemp()
root = pathlib.Path(td) / 'kafka/prod/context-tape.direction.ledger'
root.mkdir(parents=True)
m = root.parent / '_manifest'; m.mkdir()
(m / 'aaa.some-other-topic.identity').write_text('topic_id=AAAAAAAAAAAAAAAAAAAAAA\n')
(m / 'context-tape.direction.ledger.identity').write_text('topic_id=PyocBFttTn-KmwwdLj9KWw\n')
got = R.topic_generation(str(root))
sys.exit(0 if got == '3f2a1c04-5b6d-4e7f-8a9b-0c1d2e3f4a5b' else 1)" \
  && ok "a neighbouring topic's generation cannot be stamped onto this corpus" \
  || bad "topic_generation picked the wrong identity file"


echo "29. a corpus that never recorded a generation cannot be pinned"
build 32 20
rm -f "$WORK/kafka/prod/_manifest/context-tape.direction.ledger.identity"
# and no earlier manifest to carry generations forward from: once a published manifest has RECORDED a
# key's generation that recording stands, so the case being tested is a corpus that never had one
rm -rf "$WORK/calibration-runs"
targets FROZEN "$SB"; publish
OUT="$(evaluate)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "no generation means the coordinate cannot say which log it enumerated";; *) bad "a generation-less manifest was accepted as a pin: $OUT";; esac

echo "30. a published artifact is never rewritten at its own identity"
build 32 20
targets FROZEN "$SB"; publish
FIRST="$(evaluate)"
id="$(printf '%s' "$FIRST" | sed -n 's/^PushValidationArtifact \([0-9a-f]*\).*/\1/p')"
f="$(find "$WORK/calibration-runs" -name "${id}*.json" | head -1)"
before="$(shasum "$f" | cut -d' ' -f1)"
SECOND="$(evaluate)"
after="$(shasum "$f" | cut -d' ' -f1)"
case "$SECOND" in *"already published, left untouched"*) ok "the second run recognises its own artifact";; *) bad "a republish was not recognised: $SECOND";; esac
[ "$before" = "$after" ] && ok "the file on disk is byte-identical after the second run" || bad "a published verdict was rewritten"

echo "31. the archive verifier catches a data file no manifest line names"
VDIR="$WORK/varchive/kafka/prod/underlying.spx.price/dt=2026-09-08"
mkdir -p "$VDIR"
printf 'named\n'   | gzip > "$VDIR/named.jsonl.gz"
printf 'residue\n' | gzip > "$VDIR/orphan.jsonl.gz"
VDIR="$VDIR" python3 -c "
import json, hashlib, os
d = os.environ['VDIR']
p = os.path.join(d, 'named.jsonl.gz')
ent = {'file': 'named.jsonl.gz', 'records': 1, 'partition': 0, 'offset_from': 0, 'offset_to': 1,
       'sha256': hashlib.sha256(open(p,'rb').read()).hexdigest(), 'archived_at': '2026-09-08T20:00:00Z'}
open(os.path.join(d, '_manifest.jsonl'), 'w').write(json.dumps(ent) + '\n')"
VOUT="$(env ARCHIVE_DIR="$WORK/varchive" ENV=prod FORCE=true VERIFY_CHECKSUMS=none \
        bash "$SRC/oe-archive-verify.sh" 2026-09-08 2>&1)"
case "$VOUT" in *"no manifest line names"*) ok "crash residue between rename and manifest append is visible";; *) bad "an unnamed data file was ignored";; esac

echo "32. every script the crontab invokes is in the repo and in UNIT"
# The deploy job mounts ONLY scripts/ops/archive into its container, so ../../ci resolves outside the
# mount and this case would have failed there while passing locally — a suite that is green on the
# developer's machine and red in the job that gates the deploy (r11 #5). It runs where it can, says so
# where it cannot, and the deploy job runs the validator as its own step regardless.
GUARD="$SRC/../../ci/validate-archive-unit-completeness.sh"
if [ ! -f "$GUARD" ]; then
  ok "unit-completeness guard not reachable from here (the deploy job runs it as its own step)"
elif bash "$GUARD" >/dev/null 2>&1; then
  ok "no unit member is missing or unmanaged"
else
  bad "the archive unit is incomplete: $(bash "$GUARD" 2>&1 | head -2)"
fi


echo "33. a seal that names one generation cannot be relabelled with another"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
# Relabel the manifest as a topic recreation would — a valid but DIFFERENT uuid — and publish it under
# its own name, exactly as a fresh run would. The archived seals still name the original.
NEWPIN="$(HERE="$HERE" WORKDIR="$WORK" PIN="$PIN" python3 -c "
import json, os, sys
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
out_root = os.path.join(os.environ['WORKDIR'], 'calibration-runs', 'prod')
m = json.load(open(os.path.join(out_root, 'corpus', os.environ['PIN'], 'manifest.json')))
for e in m['entries']:
    e['generation'] = '99999999-8888-7777-6666-555555555555'
v, path, minted = R.publish_manifest(out_root, m)
# and make it a version a progress run published, so the ONLY thing wrong is the relabelling
p = sorted(__import__('glob').glob(out_root + '/*/*/progress/dt=*.json'), key=os.path.getmtime)[-1]
d = json.load(open(p)); d['corpusVersion'] = v; json.dump(d, open(p, 'w'))
print(v)")"
OUT="$(evaluate "$NEWPIN")"
case "$OUT" in *"COMPLETENESS=FAIL"*) : ;; *) bad "historical records were relabelled and accepted: $OUT";; esac
# and for the RIGHT reason: a REJECT can be had by breaking almost anything, so name the cause
WORKDIR="$WORK" python3 -c "
import json, glob, os, sys
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/artifacts/*.json'), key=os.path.getmtime)[-1]
d = json.load(open(f))
note = [c for c in d['clauseResults'] if c['clause'] == 'COMPLETENESS'][0]['note']
sys.exit(0 if 'different generation' in note and d.get('sealsRelabelled') else 1)"   && ok "a relabelled generation is caught by the seals themselves"   || bad "it rejected, but not because of the relabelling"

echo "34. a calendar edited after publication invalidates the pin"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
CALTMP="$WORK/cal"; mkdir -p "$CALTMP"
cp "$CAL_DIR/market_calendar.py" "$CALTMP/market_calendar.py"
printf '\n# a change after publication\n' >> "$CALTMP/market_calendar.py"
OUT="$(env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 CALENDAR_DIR="$CALTMP" \
       CORPUS_VERSION="$PIN" bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | head -1)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "recording the calendar means nothing unless it is checked";; *) bad "a corpus was re-judged under a changed calendar: $OUT";; esac

echo "35. reporter and evaluator cannot disagree about COMPLETE"
build 32 20
targets FROZEN "$SB"
# a session that graded NOTHING: COMPLETE to a naive reader, NOT_EVALUABLE to the evaluator
build 32 20 ungraded_day
publish
R_READY="$(WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/progress/dt=*.json'), key=os.path.getmtime)[-1]
d = json.load(open(f))
print(d['cohorts'][0].get('corpusComplete'))")"
OUT="$(evaluate)"
case "$OUT" in
  *"COMPLETENESS=FAIL"*)
    [ "$R_READY" = "False" ] \
      && ok "both halves say incomplete on the same corpus in the same minute" \
      || bad "the evaluator rejected while the reporter reported corpusComplete=$R_READY" ;;
  *) bad "the evaluator did not reject an ungraded session: $OUT" ;;
esac

# These two used to grep the scripts for the strings their protections contain, which is not a test of
# anything: dead code greps the same as live code. The gate proved it by inserting `exit 0` above the
# protections and leaving the searched text in place — both cases still passed. They RUN the scripts
# now, against a stub psql, and read the exit status and the side effects.
echo "36. the Postgres archiver refuses to checkpoint past rows retention already deleted"
PGSTUB="$WORK/pgstub"; mkdir -p "$PGSTUB"
cat > "$PGSTUB/psql" <<'STUB'
#!/usr/bin/env bash
# a Postgres whose retention has already deleted everything below id=200 while our checkpoint says 100
q="${@: -1}"
case "$q" in
  *"max(id)"*)   echo 400 ;;
  *"min(id)"*)   echo 200 ;;
  *"count(*)"*)  echo 201 ;;
  *)             echo "" ;;
esac
STUB
chmod +x "$PGSTUB/psql"
cat > "$PGSTUB/kubectl" <<'STUB'
#!/usr/bin/env bash
printf 'cGFzcw=='            # base64 "pass" — the archiver only needs a non-empty secret
STUB
chmod +x "$PGSTUB/kubectl"
PGROOT="$WORK/pgarchive"; mkdir -p "$PGROOT/postgres/prod/_manifest"
printf 'last_id=100 rows=1 dt=2026-09-08 archived=x
' > "$PGROOT/postgres/prod/_manifest/signal_fired.state"
gapout="$(PATH="$PGSTUB:$PATH" env ARCHIVE_DIR="$PGROOT" ENV=prod TABLES=signal_fired           ALLOW_NON_NAS=true bash "$SRC/oe-archive-postgres.sh" 2>&1)"; grc=$?
gapfile="$PGROOT/postgres/prod/_manifest/signal_fired.gaps"
after="$(awk '{split($1,a,"="); if (a[1]=="last_id") print a[2]}' "$PGROOT/postgres/prod/_manifest/signal_fired.state" | tail -1)"
[ "$grc" -ne 0 ] && ok "a GAP makes the run FAIL (rc=$grc), not log-and-continue" || bad "the run exited 0 over a gap: $gapout"
[ -s "$gapfile" ] && ok "the loss is recorded in a sidecar: $(head -1 "$gapfile")" || bad "no .gaps sidecar was written"
[ "$after" = "100" ] && ok "the checkpoint did NOT advance past the hole" || bad "the checkpoint moved to $after"

echo "37. the retention job fails loudly when its DELETE fails"
RSTUB="$WORK/rstub"; mkdir -p "$RSTUB"
cat > "$RSTUB/kubectl" <<'STUB'
#!/usr/bin/env bash
printf 'cGFzcw=='
STUB
cat > "$RSTUB/psql" <<'STUB'
#!/usr/bin/env bash
echo "ERROR:  relation "databento_option_raw_snapshot" does not exist" >&2
exit 1
STUB
chmod +x "$RSTUB/psql" "$RSTUB/kubectl"
rlog="$WORK/retention.log"
PATH="$RSTUB:$PATH" env LOG="$rlog" bash "$SRC/ibkr-raw-retention.sh" >/dev/null 2>&1; rrc=$?
[ "$rrc" -ne 0 ] && ok "a failing DELETE exits nonzero (rc=$rrc) instead of echoing success" || bad "the retention job reported success on a failed DELETE"
grep -q "retention DELETE failed" "$rlog" && ok "and the log says so" || bad "the log does not name the failure: $(tail -2 "$rlog")"
# the same run must not have written a credential anywhere
! grep -rqi "pgpassword=" "$rlog" && ok "no credential reached the log" || bad "a credential was logged"


echo "38. the evaluator uses the SAME completeness predicate as the reporter"
build 32 20
targets FROZEN "$SB"
# a foreign lineage, incomplete, on an owed target date — the reporter saw this, the evaluator did not
victim="$(ls -d "$ROOT"/dt=* | sed -n '7p')"; vd="$(basename "$victim" | sed 's/dt=//')"
VD="$vd" ROOT="$ROOT" python3 - <<'PYCASE'
import gzip, json, os
root, sd = os.environ["ROOT"], os.environ["VD"]
lin = "lin-foreign-%s" % sd
rec = {"kind": "call", "callId": "%s-foreign" % sd, "parameterSetHash": "a1b2c3d4e5f60718",
       "sessionLineageId": lin, "sessionDate": sd, "phaseAtCall": "VALIDATION",
       "trackFromPush": "2026-07-01", "delivery": "LIVE", "semanticStamp": "2026-09-08T18:00:00Z",
       "predictedSign": 1, "enteredState": "EXHAUSTED", "regime": "POS_GAMMA",
       "node": {"roles": ["CALL_WALL"]}, "refT": 0, "ts": 0, "runId": "r1"}
k = "%s|%s|%s" % (rec["parameterSetHash"], lin, rec["callId"])
with gzip.open(os.path.join(root, "dt=%s" % sd, "part-800.jsonl.gz"), "wt") as fh:
    fh.write("Partition:0 Offset:800000 %s\t%s\n" % (k, json.dumps(rec)))
PYCASE
publish
R_OK="$(WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/progress/dt=*.json'), key=os.path.getmtime)[-1]
print(json.load(open(f))['cohorts'][0].get('corpusComplete'))")"
OUT="$(evaluate)"
case "$OUT" in
  *"COMPLETENESS=FAIL"*) [ "$R_OK" = "False" ] && ok "both halves reject the same corpus" || bad "evaluator rejected, reporter said corpusComplete=$R_OK" ;;
  *) bad "the evaluator accepted what the reporter called incomplete: $OUT" ;;
esac

echo "39. a pin is bound to the corpusStartDate it was published under"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
sed -i'' -e 's/^OE_CAL_CORPUS_START_DATE_prod=.*/OE_CAL_CORPUS_START_DATE_prod=2026-07-06/' "$HERE/calibration-targets.env"
evaluate "$PIN" >/dev/null
why_says "published under corpusStartDate" \
  "the window cannot be re-cut after publication" \
  "it rejected, but not because the start date moved"

echo "40. an entry with no real (partition, offset) is not a coordinate"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
WORKDIR="$WORK" PIN="$PIN" HERE="$HERE" python3 -c "
import json, os, sys
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
root = os.path.join(os.environ['WORKDIR'], 'calibration-runs', 'prod')
m = json.load(open(os.path.join(root, 'corpus', os.environ['PIN'], 'manifest.json')))
for e in m['entries']:
    e['partition'] = '-1'
v, p, minted = R.publish_manifest(root, m)
import glob
f = sorted(glob.glob(root + '/*/*/progress/dt=*.json'), key=os.path.getmtime)[-1]
d = json.load(open(f)); d['corpusVersion'] = v; json.dump(d, open(f, 'w'))
print(v)" > "$WORK/nopart.txt"
evaluate "$(cat "$WORK/nopart.txt")" >/dev/null
why_says "no real (partition, offset)" \
  "-1 is not a partition" \
  "it rejected, but not because the coordinates were missing"

echo "41. the dev ledger is in the set the dev archive actually passes"
grep -q 'DEALER_LEDGER_EVIDENCE=.*context-tape\.direction\.ledger' "$SRC/oe-topics.env" \
  && grep -q 'TOPICS="\$DEALER_LEDGER_EVIDENCE"' "$SRC/oe-archive-daily.sh" \
  && grep -q 'ENV=dev.*oe-calibration-progress.sh' "$SRC/oe-archive.crontab" \
  && ok "dev archives the ledger and has a progress run behind its declaration" \
  || bad "the dev corpus is still never captured or never reported"

echo "42. the progress record carries hashChangedOn and attrition"
build 32 20
targets FROZEN "$SB"; publish
WORKDIR="$WORK" python3 -c "
import json, glob, os, sys
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/progress/dt=*.json'), key=os.path.getmtime)[-1]
d = json.load(open(f))
att = d.get('attrition') or {}
one = next(iter(att.values()), {})
sys.exit(0 if d.get('hashChangedOn') and att and 'refusalRate' in one and 'byEtHour' in one else 1)" \
  && ok "a cohort that fills while the instrument refuses most ticks is visible" \
  || bad "the report omits hashChangedOn or attrition"

echo "43. a quiet VALIDATION cohort is not reported as CALIBRATION"
build 32 20
targets FROZEN "$SB"
# frozen clock, complete sessions, but no qualifying calls: phase must follow the DECLARATION
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, json, os, sys
for f in glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")):
    out = []
    for line in gzip.open(f, "rt"):
        i = line.find("{")
        rec = json.loads(line[i:])
        if rec.get("kind") in ("call", "outcome"):
            rec["phaseAtCall"] = "CALIBRATION"      # nothing qualifies for the VALIDATION cohort
        out.append(line[:i] + json.dumps(rec) + "\n")
    with gzip.open(f, "wt") as fh:
        fh.write("".join(out))
PYCASE
publish
PH_OUT="$(WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/progress/dt=*.json'), key=os.path.getmtime)[-1]
c = json.load(open(f))['cohorts'][0]
print('%s %s' % (c.get('phase'), c.get('validationClockStarted')))")"
case "$PH_OUT" in "VALIDATION True") ok "an empty validation cohort says so instead of hiding as CALIBRATION";; *) bad "phase inferred from the data, not the clock: $PH_OUT";; esac


echo "44. the shared predicate is load-bearing, not decorative"
# A call can sit there doing nothing if a duplicate check beside it reaches the same verdict, and no
# test notices — which is exactly what happened: with corpus_defects() called AND the old per-session
# checks still standing, deleting the call changed no verdict at all. This removes the call from a COPY
# and requires the corpus to be accepted, proving the call is the only thing refusing it.
MUT="$WORK/mutant"; mkdir -p "$MUT"
cp "$HERE"/*.sh "$HERE"/*.py "$HERE"/calibration-targets.env "$MUT/"
python3 - "$MUT/oe-push-validation-artifact.sh" <<'PYCASE'
import re, sys
p = sys.argv[1]
s = open(p).read()
s2 = re.sub(r'shared_defects = R\.corpus_defects\(read, sessions, seals,\n[^\n]*\n[^\n]*\n',
            'shared_defects = []\n', s)
sys.exit(0 if s2 != s and open(p, "w").write(s2) is not None else 1)
PYCASE
# Earlier cases deliberately corrupt manifests and rewrite published versions in this shared tree, and
# published_version() takes the newest record — so this case starts from a clean calibration-runs or it
# would be measuring their damage instead of its own.
rm -rf "$WORK/calibration-runs"
build 32 20 ungraded_day
targets FROZEN "$SB"; publish
cp "$HERE/calibration-targets.env" "$MUT/calibration-targets.env"
# Compare the REASONS, not the verdicts: a REJECT can be reached by several routes, so the proof that
# this call is load-bearing is that the corpus-defect reason exists with it and vanishes without it.
evaluate >/dev/null
real_note="$(WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/artifacts/*.json'), key=os.path.getmtime)[-1]
d = json.load(open(f))
print([c for c in d['clauseResults'] if c['clause'] == 'COMPLETENESS'][0]['note'])")"
env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 CALENDAR_DIR="$CAL_DIR" \
    CORPUS_VERSION="$(published_version)" bash "$MUT/oe-push-validation-artifact.sh" >/dev/null 2>&1
mut_note="$(WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/artifacts/*.json'), key=os.path.getmtime)[-1]
d = json.load(open(f))
print([c for c in d['clauseResults'] if c['clause'] == 'COMPLETENESS'][0]['note'])")"
case "$real_note" in *"corpus defect"*) : ;; *) bad "the real evaluator gave no corpus-defect reason: $real_note";; esac
case "$mut_note"  in *"corpus defect"*) bad "the reason survives without the call — it comes from somewhere else";; *) ok "the corpus-defect reason exists only while the shared call does";; esac


echo "45. a cohort admits only the DECLARED semantic stamp"
build 32 20
targets FROZEN "$SB"; publish
# the literals changed but the hash did not — which is exactly the case the hash cannot catch, because
# A4.11 deliberately excludes the stamp from it
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, json, os, sys
for f in glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")):
    out = []
    for line in gzip.open(f, "rt"):
        i = line.find("{")
        rec = json.loads(line[i:])
        if rec.get("kind") == "call":
            rec["semanticStamp"] = "2099-01-01T00:00:00Z"
        out.append(line[:i] + json.dumps(rec) + "\n")
    with gzip.open(f, "wt") as fh:
        fh.write("".join(out))
PYCASE
publish
OUT="$(evaluate)"
case "$OUT" in *"COHORT_SIZE=FAIL"*) ok "calls under another semantic stamp do not join this cohort";; *) bad "a foreign semantic stamp was pooled into the cohort: $OUT";; esac


echo "46. the ledger topic is declared, not supplied"
OUT="$(env ENV=prod ARCHIVE_DIR="$WORK" LEDGER_TOPIC=chosen.after.data CORPUS_VERSION=x \
       bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | tail -1)"
case "$OUT" in *"cannot be supplied by the caller"*) ok "a corpus cannot be pointed at a topic the caller prepared";; *) bad "LEDGER_TOPIC override accepted: $OUT";; esac
OUT="$(env ENV=prod ARCHIVE_DIR="$WORK" LEDGER_TOPIC=chosen.after.data \
       bash "$HERE/oe-calibration-progress.sh" 2>&1 | tail -1)"
case "$OUT" in *"cannot be supplied by the caller"*) ok "and the reporter refuses it too";; *) bad "the reporter honoured an override: $OUT";; esac

echo "47. a live record with no coordinate is a mismatch, not an exemption"
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, os, sys
for f in glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")):
    body = gzip.open(f, "rt").read().replace("Partition:0 ", "")   # offsets kept, partitions gone
    with gzip.open(f, "wt") as fh:
        fh.write(body)
PYCASE
evaluate "$PIN" >/dev/null
why_says "changed coordinate or digest" \
  "an archive that lost its coordinates does not verify against a manifest that has them" \
  "it rejected, but not because the live coordinates were missing"

echo "48. an outcome-only lineage cannot hide"
build 32 20
targets FROZEN "$SB"
victim="$(ls -d "$ROOT"/dt=* | sed -n '9p')"; vd="$(basename "$victim" | sed 's/dt=//')"
VD="$vd" ROOT="$ROOT" python3 - <<'PYCASE'
import gzip, json, os
root, sd = os.environ["ROOT"], os.environ["VD"]
lin = "lin-orphan-%s" % sd
base = {"parameterSetHash": "a1b2c3d4e5f60718", "sessionLineageId": lin, "sessionDate": sd,
        "phaseAtCall": "VALIDATION", "trackFromPush": "2026-07-01", "delivery": "LIVE",
        "semanticStamp": "2026-09-08T18:00:00Z", "ts": 0, "runId": "r1"}
rows = []
for h in ("H3", "H5", "H15"):
    o = dict(base, kind="outcome", callId="%s-ghost" % sd, horizon=h, resultState="OBSERVED",
             resultTicks=9, pathState="OBSERVED", maeTicks=1, mfeTicks=9,
             cellKey="EXHAUSTED|CALL_WALL|POS_GAMMA|%s" % h)
    rows.append(("%s|%s|%s|%s" % (base["parameterSetHash"], lin, o["callId"], h), o))
with gzip.open(os.path.join(root, "dt=%s" % sd, "part-700.jsonl.gz"), "wt") as fh:
    for i, (k, r) in enumerate(rows):
        fh.write("Partition:0 Offset:%d %s\t%s\n" % (700000 + i, k, json.dumps(r)))
PYCASE
publish
evaluate >/dev/null
why_says "corpus defect" \
  "outcomes belonging to no call and no seal are a session with a status, not silence" \
  "an outcome-only lineage passed unnoticed"

echo "49. a seal whose semantic stamp differs from its records is CORRUPT"
build 32 20
targets FROZEN "$SB"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, json, os, sys
f = sorted(glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")))[11]
out = []
for line in gzip.open(f, "rt"):
    i = line.find("{")
    rec = json.loads(line[i:])
    if rec.get("kind") == "seal":
        rec["semanticStamp"] = "1999-01-01T00:00:00Z"   # sealed under other literals
    out.append(line[:i] + json.dumps(rec) + "\n")
with gzip.open(f, "wt") as fh:
    fh.write("".join(out))
PYCASE
publish
evaluate >/dev/null
why_says "corpus defect" \
  "a seal cannot describe a population built under literals it does not name" \
  "a seal with a foreign semantic stamp was accepted"


echo "50. an owed day is satisfied only by a session of THIS cohort's semantic stamp"
build 32 20
targets FROZEN "$SB"; publish
# one session moved coherently to another stamp: its own calls, outcomes and seal all agree, so every
# per-session check passes — the only thing wrong is that it is not this cohort's session
victim="$(ls -d "$ROOT"/dt=* | sed -n '6p')"
python3 - "$victim" <<'PYCASE'
import gzip, glob, json, os, sys
for f in glob.glob(os.path.join(sys.argv[1], "*.jsonl.gz")):
    out = []
    for line in gzip.open(f, "rt"):
        i = line.find("{")
        rec = json.loads(line[i:])
        rec["semanticStamp"] = "2099-12-31T00:00:00Z"
        out.append(line[:i] + json.dumps(rec) + "\n")
    with gzip.open(f, "wt") as fh:
        fh.write("".join(out))
PYCASE
publish
evaluate >/dev/null
why_says "owed trading day" \
  "a session sealed under other literals does not fill a day this cohort owes" \
  "it rejected, but not because the owed day was unfilled"


echo "51. every archive script decides trading days from the calendar deployed WITH the unit"
# oe-trading-day.sh prefers market_calendar.py beside it — but a caller that sets CALENDAR_DIR before
# sourcing silently defeats that, and oe-archive-daily.sh did exactly that with a hardcoded default.
# The archiver would then decide trading days from one calendar while the pin records another.
CALPROBE="$WORK/calprobe"; mkdir -p "$CALPROBE"
cp "$SRC/oe-trading-day.sh" "$SRC/oe-alert.sh" "$CALPROBE/"
cp "$SRC/../../jenkins/market_calendar.py" "$CALPROBE/" 2>/dev/null || cp "$CAL_DIR/market_calendar.py" "$CALPROBE/"
cat > "$CALPROBE/probe.sh" <<'PROBE'
#!/usr/bin/env bash
set -uo pipefail
CALENDAR_DIR="${CALENDAR_DIR:-}"; [ -n "$CALENDAR_DIR" ] || unset CALENDAR_DIR
. "$(dirname "$0")/oe-trading-day.sh"
printf '%s %s' "$(is_trading_day 2026-09-07)" "$(is_trading_day 2026-09-08)"
PROBE
chmod +x "$CALPROBE/probe.sh"
got="$(bash "$CALPROBE/probe.sh")"
[ "$got" = "no yes" ] \
  && ok "the colocated calendar decides: Labor Day no, Tuesday yes" \
  || bad "the colocated calendar was not used (got: $got)"
grep -q 'CALENDAR_DIR="${CALENDAR_DIR:-/home' "$SRC/oe-archive-daily.sh" \
  && bad "oe-archive-daily.sh still hardcodes a calendar directory, defeating the unit's own" \
  || ok "no archive script overrides the unit's calendar with a hardcoded path"


echo "52. the collapse keeps the LOWEST coordinate, whichever record was seen first"
build 3 20
targets FROZEN "$SB"
manifest_version_now() {
  HERE="$HERE" ROOT="$ROOT" python3 -c "
import os, sys
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
r = R.read_logical(os.environ['ROOT'])
print(R.manifest_version(R.build_manifest(r, 'context-tape.direction.ledger', 'prod')))"
}
offset_of() {   # offset_of <physical key>
  HERE="$HERE" ROOT="$ROOT" KEY="$1" python3 -c "
import os, sys
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
r = R.read_logical(os.environ['ROOT'])
m = R.build_manifest(r, 'context-tape.direction.ledger', 'prod')
print([e['offset'] for e in m['entries'] if e['key'] == os.environ['KEY']][0])"
}
BEFORE="$(manifest_version_now)"

# (a) a replay ABOVE the original changes nothing: the lowest is still the original
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, os, re, sys
d = sorted(glob.glob(os.path.join(sys.argv[1], "dt=*")))[0]
f = os.path.join(d, "part-000.jsonl.gz")
lines = gzip.open(f, "rt").read().splitlines()
mid = lines[len(lines) // 2] + "\n"
with gzip.open(os.path.join(d, "part-mmm.jsonl.gz"), "wt") as fh:
    fh.write(re.sub(r"Offset:(\d+)", lambda m: "Offset:%d" % (int(m.group(1)) + 900000), mid))
PYCASE
[ "$(manifest_version_now)" = "$BEFORE" ] \
  && ok "a replay further down the log leaves the corpus version alone" \
  || bad "a replay above the original moved the coordinate"

# (b) a replay BELOW it, in a later-sorted file, must WIN — that is what "lowest" means, and it is the
# only arrangement where "keep the lowest" and "keep the first seen" differ
KEYLINE="$(python3 - "$ROOT" <<'PYCASE'
import gzip, glob, os, re, sys
d = sorted(glob.glob(os.path.join(sys.argv[1], "dt=*")))[0]
f = os.path.join(d, "part-000.jsonl.gz")
lines = gzip.open(f, "rt").read().splitlines()
mid = lines[len(lines) // 2]
off = int(re.search(r"Offset:(\d+)", mid).group(1))
key = re.search(r"Offset:\d+ (\S+)\t", mid).group(1)
lower = re.sub(r"Offset:\d+", "Offset:%d" % (off - 1), mid) + "\n"
with gzip.open(os.path.join(d, "part-zzz.jsonl.gz"), "wt") as fh:
    fh.write(lower)
print("%s %s" % (key, off - 1))
PYCASE
)"
K="${KEYLINE%% *}"; WANT="${KEYLINE##* }"
GOT="$(offset_of "$K")"
[ "$GOT" = "$WANT" ] \
  && ok "a replay at a lower offset wins: the entry is $GOT, not the one seen first" \
  || bad "the collapse kept the first record seen ($GOT) instead of the lowest ($WANT)"

echo "53. a quiet corpus can be COMPLETE without being full"
# Every session sealed and reconciled, and NOTHING qualifying for the cohort. corpusComplete is about
# the ARCHIVE; the counts are about the cohort. Hardcoding completeness false whenever the cohort was
# empty answered a different question than the one it was asked (r17 #3).
build 32 20 quiet
targets FROZEN "$SB"; publish
OUT="$(WORKDIR="$WORK" python3 -c "
import json, glob, os
f = sorted(glob.glob(os.environ['WORKDIR'] + '/calibration-runs/prod/*/*/progress/dt=*.json'), key=os.path.getmtime)[-1]
c = json.load(open(f))['cohorts'][0]
print('%s %s %s' % (c.get('phase'), c.get('thresholdsMet'), c.get('corpusComplete')))")"
case "$OUT" in
  "VALIDATION False True") ok "not full, and complete — two different facts, both stated" ;;
  *) bad "completeness was collapsed into the counts: $OUT" ;;
esac


echo "54. a session whose files could not be read is NOT COMPLETE"
# The mutation audit found this: making the reader's CORRUPT branch return COMPLETE left the suite
# green. Case 19 covers an unreadable file through the flat readErrors list, so the SESSION-STATUS path
# for the same failure was bound by nothing — and that path is what decides whether the session's calls
# enter a counter.
build 32 20
targets FROZEN "$SB"; publish
printf 'this is not gzip' > "$ROOT/dt=2026-07-14/part-998.jsonl.gz"
STATUS="$(HERE="$HERE" ROOT="$ROOT" python3 -c "
import os, sys
sys.path.insert(0, os.environ['HERE'])
import oe_corpus_reader as R
read = R.read_logical(os.environ['ROOT'])
sessions, seals, calls, outcomes = R.classify_sessions(read, R.read_sidecar(os.environ['ROOT'], []), '2026-08-13')
rows = [v for v in sessions.values() if v['sessionDate'] == '2026-07-14']
print(rows[0]['archiveStatus'] if rows else 'NO_ROW')")"
[ "$STATUS" = "CORRUPT" ] \
  && ok "the session itself is CORRUPT, not merely noted in a list elsewhere" \
  || bad "a session with an unreadable file was classified $STATUS"

echo "55. an explicit -1 coordinate is refused"
# Also from the audit. Case 47 strips Partition: entirely, which the digest/offset comparison catches
# on its own — so the guard that exists for an explicit -1 (a coordinate that IS present and IS
# meaningless) was redundant for every fixture and bound by nothing.
build 32 20
targets FROZEN "$SB"; publish
PIN="$(published_version)"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, os, re, sys
for f in glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")):
    body = gzip.open(f, "rt").read()
    with gzip.open(f, "wt") as fh:
        fh.write(re.sub(r"Partition:0", "Partition:-1", body))
PYCASE
evaluate "$PIN" >/dev/null
why_says "changed coordinate or digest" \
  "-1 is refused as a live coordinate, not compared as if it meant something" \
  "an explicit -1 coordinate was accepted"


echo "56. the reader RECOMPUTES the digest, so a payload cannot verify itself"
# Third survivor from the audit. A record carries a semanticDigest as a diagnostic; if the reader
# trusted it, a corrupted payload with its original digest still attached would verify against itself
# and the seal's chain would agree. Nothing bound that, so the recomputation was correct and untested.
build 32 20
targets FROZEN "$SB"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, hashlib, json, os, sys
def canon(o):
    if isinstance(o, dict):
        return "{" + ",".join('%s:%s' % (json.dumps(k), canon(v)) for k, v in sorted(o.items())) + "}"
    if isinstance(o, list): return "[" + ",".join(canon(v) for v in o) + "]"
    if o is None: return "null"
    if isinstance(o, bool): return "true" if o else "false"
    if isinstance(o, str): return json.dumps(o)
    return json.dumps(str(o))
f = sorted(glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")))[4]
out, done = [], False
for line in gzip.open(f, "rt"):
    i = line.find("{")
    rec = json.loads(line[i:])
    # change what the record SAYS while leaving the digest it carries alone
    if not done and rec.get("kind") == "outcome":
        # The digest the record carries must be the one its ORIGINAL body hashes to — that is the whole
        # point. An all-zero digest breaks the chain by itself, so a reader that trusted it would be
        # caught anyway and the case proved nothing (r18 #5).
        body = {k: v for k, v in rec.items() if k not in ("ts", "publishedAtMs", "runId", "semanticDigest")}
        rec["semanticDigest"] = hashlib.sha256(canon(body).encode("utf-8")).hexdigest()
        rec["resultTicks"] = (rec.get("resultTicks") or 0) + 1000     # ... and now the body disagrees
        done = True
    out.append(line[:i] + json.dumps(rec) + "\n")
with gzip.open(f, "wt") as fh:
    fh.write("".join(out))
PYCASE
publish
evaluate >/dev/null
why_says "corpus defect" \
  "an altered payload is caught by recomputation, not excused by the digest it carries" \
  "a payload verified itself against its own carried digest"


echo "57. an arbitrary stopping instant is refused"
# The audit found this unbound: A5.8 requires the boundary to be an RTH close so it can never cut
# through an A4.12 hour row, and disabling that check left the whole suite green.
build 32 20
NOON="$(python3 -c "import datetime;print(int(datetime.datetime.fromisoformat('2026-08-13T16:30:00+00:00').timestamp()*1000))")"
targets FROZEN "$NOON"; publish
evaluate >/dev/null
why_says "not an RTH close instant" \
  "a boundary that is not an RTH close is refused, so it can never cut through an hour row" \
  "an arbitrary stopping instant was accepted"

echo "58. COVERAGE is a clause with teeth"
# Also unbound: forcing coverage to 1.0 left all cases green, so the clause that says "an instrument
# whose outcomes mostly go UNOBSERVED has not been measured" was decoration.
build 32 20
targets FROZEN "$SB"
python3 - "$ROOT" <<'PYCASE'
import gzip, glob, json, os, sys
# most outcomes UNOBSERVED at the primary horizon: the calls happened, the results did not
for f in glob.glob(os.path.join(sys.argv[1], "dt=*", "*.jsonl.gz")):
    out = []
    for line in gzip.open(f, "rt"):
        i = line.find("{")
        rec = json.loads(line[i:])
        if rec.get("kind") == "outcome" and rec.get("horizon") == "H5":
            rec["resultState"] = "UNOBSERVED"; rec["resultReason"] = "NO_TERMINAL_TICK"
            rec["resultTicks"] = None
        out.append(line[:i] + json.dumps(rec) + "\n")
    with gzip.open(f, "wt") as fh:
        fh.write("".join(out))
PYCASE
publish
OUT="$(evaluate)"
case "$OUT" in
  *"COVERAGE=FAIL"*|*"COVERAGE=NOT_EVALUABLE"*) ok "an instrument whose outcomes go UNOBSERVED has not been measured";; 
  *) bad "coverage passed on a cohort with no observed results: $OUT";;
esac

echo "59. a date with one CORRUPT lineage is not a date that landed"
# The watchdog took the BEST status among a date's rows — my reasoning, and wrong: A5 says a lost day
# contributes to nothing, so a date with one COMPLETE lineage and one CORRUPT one is a date whose
# population is not knowable. It reported COMPLETE and exited 0.
build 3 20
targets FROZEN "$SB"
DAY="$(ls -d "$ROOT"/dt=* | sed -n '2p' | sed 's|.*dt=||')"
DAY="$DAY" ROOT="$ROOT" python3 - <<'PYCASE'
import gzip, json, os
root, sd = os.environ["ROOT"], os.environ["DAY"]
# a SECOND lineage for the same date, with records and a seal whose chains do not reproduce
lin = "lin-broken-%s" % sd
seal = {"kind": "seal", "sessionDate": sd, "parameterSetHash": "a1b2c3d4e5f60718", "sessionLineageId": lin,
        "phaseAtCall": "VALIDATION", "trackFromPush": "2026-07-01", "delivery": "SEAL",
        "ledgerTopic": "context-tape.direction.ledger",
        "generation": "3f2a1c04-5b6d-4e7f-8a9b-0c1d2e3f4a5b", "semanticStamp": "2026-09-08T18:00:00Z",
        "logicalCallCount": 1, "logicalOutcomeCount": 3, "callsDigest": "0" * 64, "outcomesDigest": "0" * 64,
        "firstOffset": 0, "lastOffset": 1, "conflicts": 0,
        "attrition": [{"sessionDate": sd, "etHour": 10, "graded": 10, "NO_TICKS": 1}], "ts": 0, "runId": "r1"}
k = "%s|%s|%s" % (sd, seal["parameterSetHash"], lin)
with gzip.open(os.path.join(root, "dt=%s" % sd, "part-broken.jsonl.gz"), "wt") as fh:
    fh.write("Partition:0 Offset:950000 %s\t%s\n" % (k, json.dumps(seal)))
PYCASE
env ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE="$DAY" CALENDAR_DIR="$CAL_DIR" \
    bash "$HERE/oe-calibration-progress.sh" >/dev/null 2>&1
if CHECK_DATE="$DAY" ENV=prod ARCHIVE_DIR="$WORK" CALENDAR_DIR="$CAL_DIR" \
   bash "$SRC/calibration-progress-watch.sh" >"$WORK/watch2.log" 2>&1; then
  bad "the watchdog called a date COMPLETE while one of its lineages is CORRUPT"
else
  ok "the worst lineage decides the date, not the best"
fi

echo
if [ $fails -eq 0 ]; then echo "PASS — the A5.8 evaluator holds on every case"; exit 0; fi
echo "FAIL — $fails assertion(s)"; exit 1
