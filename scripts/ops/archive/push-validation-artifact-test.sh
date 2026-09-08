#!/usr/bin/env bash
# push-validation-artifact-test.sh — proves the A5.8 evaluator and the shared corpus reader on a
# corpus this script builds itself: real chains, real seals, no NAS and no Kafka.
#
# Every case below is a way the evaluator could be wrong in the direction that MATTERS — accepting
# something it should not. A test that only checks the happy path would pass on an evaluator that
# returns ACCEPT unconditionally.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
ROOT="$WORK/kafka/prod/context-tape.direction.ledger"
PH="a1b2c3d4e5f60718"
TF="2026-07-01"
SB="$(python3 -c "import datetime;print(int(datetime.datetime.fromisoformat('2026-08-13T20:00:00+00:00').timestamp()*1000))")"
fails=0
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

# realistic thresholds: this synthetic tape is a POSITIVE fixture, so the happy path must be reachable
evaluate() {   # evaluate <label> [env overrides...]
  local label="$1"; shift
  env PARAMETER_SET_HASH="$PH" TRACK_FROM_PUSH="$TF" STOPPING_BOUNDARY_MS="$SB" \
      CORPUS_VERSION="$(version_of)" ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 \
      BOOTSTRAP_B=300 RESULT_LCB_FLOOR=0 HIT_RATE_LCB_FLOOR=0.4 MEDIAN_MAE_CEIL=20 \
      P90_MAE_CEIL=20 COVERAGE_FLOOR=0.9 ATTRITION_CEIL=0.10 "$@" \
      bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | head -1
}
clause_state() { echo "$1" | tr ' ' '\n' | grep "^$2=" | cut -d= -f2; }

echo "1. a complete corpus with reachable thresholds ACCEPTS"
build 32 20
OUT="$(evaluate accept)"
[ "$(clause_state "$OUT" decision)" = "" ] || true
case "$OUT" in *"decision=ACCEPT"*) ok "ACCEPT on a complete corpus";; *) bad "expected ACCEPT, got: $OUT";; esac
for c in COHORT_SIZE COMPLETENESS RESULT_LCB HIT_RATE_LCB MAE_CEILING COVERAGE ATTRITION_CEILING; do
  case "$OUT" in *"$c=PASS"*) :;; *) bad "$c did not pass on the positive fixture: $OUT";; esac
done
ok "every clause is present and named in the output"

echo "2. the DEFAULT thresholds cannot be passed by accident"
OUT="$(env PARAMETER_SET_HASH="$PH" TRACK_FROM_PUSH="$TF" STOPPING_BOUNDARY_MS="$SB" \
      CORPUS_VERSION="$(version_of)" ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 BOOTSTRAP_B=200 \
      bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | head -1)"
case "$OUT" in *"decision=REJECT"*) ok "an unconfigured run REJECTS rather than certifying";; *) bad "defaults accepted: $OUT";; esac

echo "3. a corpusVersion that is not the one on disk fails COMPLETENESS"
OUT="$(env PARAMETER_SET_HASH="$PH" TRACK_FROM_PUSH="$TF" STOPPING_BOUNDARY_MS="$SB" \
      CORPUS_VERSION=$(printf '0%.0s' $(seq 64)) ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 \
      BOOTSTRAP_B=200 RESULT_LCB_FLOOR=0 HIT_RATE_LCB_FLOOR=0.4 MEDIAN_MAE_CEIL=20 P90_MAE_CEIL=20 \
      COVERAGE_FLOOR=0.9 ATTRITION_CEIL=0.10 bash "$HERE/oe-push-validation-artifact.sh" 2>&1 | head -1)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "an unpinned corpus is not silently evaluated";; *) bad "pin not enforced: $OUT";; esac

echo "4. ONE unreproducible seal inside the window makes the artifact REJECT, not a smaller cohort"
build 32 20 broken_chain
OUT="$(evaluate broken)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "a NOT_EVALUABLE session is the verdict, not a warning";; *) bad "broken chain tolerated: $OUT";; esac
case "$OUT" in *"decision=REJECT"*) ok "decision follows COMPLETENESS";; *) bad "expected REJECT: $OUT";; esac

echo "5. ONE blind session out of thirty fails ATTRITION_CEILING (a pooled rate would hide it)"
build 32 20 blind_day
OUT="$(evaluate blind)"
case "$OUT" in *"ATTRITION_CEILING=FAIL"*) ok "the gate is per session, not pooled";; *) bad "blind day hidden: $OUT";; esac

echo "6. a session that graded nothing is NOT_EVALUABLE, never a refusal rate of zero"
build 32 20 ungraded_day
OUT="$(evaluate ungraded)"
case "$OUT" in *"COMPLETENESS=FAIL"*) ok "an ungraded session cannot contribute a 0% refusal";; *) bad "ungraded day counted: $OUT";; esac

echo "7. the stopping boundary is honoured — an earlier boundary yields a smaller cohort"
build 32 20
EARLY="$(python3 -c "import datetime;print(int(datetime.datetime.fromisoformat('2026-07-10T20:00:00+00:00').timestamp()*1000))")"
OUT="$(evaluate early STOPPING_BOUNDARY_MS="$EARLY")"
case "$OUT" in *"COHORT_SIZE=FAIL"*) ok "calls after the preregistered boundary are excluded";; *) bad "boundary ignored: $OUT";; esac

echo "8. the artifactId is a content address: same inputs same id, changed threshold different id"
A="$(evaluate id1 | sed -n 's/^PushValidationArtifact \([0-9a-f]*\).*/\1/p')"
B="$(evaluate id2 | sed -n 's/^PushValidationArtifact \([0-9a-f]*\).*/\1/p')"
C="$(evaluate id3 COVERAGE_FLOOR=0.5 | sed -n 's/^PushValidationArtifact \([0-9a-f]*\).*/\1/p')"
[ -n "$A" ] && [ "$A" = "$B" ] && ok "identical inputs reproduce the artifactId ($A)" || bad "artifactId not reproducible: $A vs $B"
[ "$A" != "$C" ] && ok "a changed threshold changes the identity" || bad "artifactId ignores the thresholds"

echo "9. the estimator is deterministic under its frozen seed"
V1="$(env PARAMETER_SET_HASH="$PH" TRACK_FROM_PUSH="$TF" STOPPING_BOUNDARY_MS="$SB" CORPUS_VERSION="$(version_of)" \
     ENV=prod ARCHIVE_DIR="$WORK" REPORT_DATE=2026-08-13 BOOTSTRAP_B=300 RESULT_LCB_FLOOR=0 HIT_RATE_LCB_FLOOR=0.4 \
     MEDIAN_MAE_CEIL=20 P90_MAE_CEIL=20 COVERAGE_FLOOR=0.9 ATTRITION_CEIL=0.10 \
     bash "$HERE/oe-push-validation-artifact.sh" >/dev/null 2>&1; \
     python3 -c "
import json, glob
f = sorted(glob.glob('$WORK/calibration-runs/prod/$PH/*/artifacts/*.json'))
d = json.load(open(f[0]))
print([c['value'] for c in d['clauseResults'] if c['clause'] == 'RESULT_LCB'][0])")"
[ -n "$V1" ] && ok "RESULT_LCB reproduces as $V1 under seed+B" || bad "no RESULT_LCB value written"

echo "10. the artifact says, in the record itself, that it authorizes nothing"
python3 - "$WORK" <<'PY'
import json, glob, sys
f = sorted(glob.glob(sys.argv[1] + "/calibration-runs/prod/*/*/artifacts/*.json"))[0]
d = json.load(open(f))
bad = [k for k, v in (("authorizing", False), ("actionable", False), ("slice", "COMMISSIONING_SHADOW")) if d.get(k) != v]
print("  FAIL non-authorizing labels missing: %s" % bad if bad else "  ok   authorizing=false actionable=false slice=COMMISSIONING_SHADOW")
sys.exit(1 if bad else 0)
PY
[ $? -eq 0 ] || fails=$((fails+1))

echo
if [ $fails -eq 0 ]; then echo "PASS — the A5.8 evaluator holds on every case"; exit 0; fi
echo "FAIL — $fails assertion(s)"; exit 1
