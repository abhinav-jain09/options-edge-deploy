#!/usr/bin/env bash
# Negative controls for schema-code-verify.sh with a stub `mvn` and stub processing checkouts (docs/schema-governance R5b, R10, A-criteria):
# a requirements file that disagrees with the manifest, a failing module test, no requirements file at all, and a service the manifest does not bind must each fail.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"
cat > "$T/bin/mvn" <<'M'
#!/usr/bin/env bash
[ "${STUB_MVN:-ok}" = ok ] || { echo "stub mvn: tests FAILED" >&2; exit 1; }
exit 0
M
chmod +x "$T/bin/mvn"
# The fixtures are DERIVED from the manifest's BINDINGS for this service - never a pinned version - and the python
# block WRITES THEM AS FILES, so no manifest text ever reaches the shell as code. The GOOD file used to name v2
# (id 2811) by hand and silently became a NEGATIVE control the day the EAV ledger moved to v3: v2 is still in the
# manifest but RETAINED with no producer, so R5e refused it, the positive control failed, and that is what held
# deploy-validation - and the ~88 steps behind it - red. R5e is an EXACT TWO-WAY match (Planner.compareLabel), so
# the derivation emits EVERY binding, produces and consumes, not just the subject this test reasons about.
SVC=early-auction-verdict
FX="$T/fx"
drc=0
python3 - "$SVC" "$FX" <<'PY' || drc=$?
import json, os, re, sys
# Build this test's fixtures from the manifest, mirroring Planner.compareLabel exactly (R5e is an EXACT TWO-WAY match,
# so the label must carry every binding, not just the one this test cares about). Nothing here is pinned to a version.
svc, out = sys.argv[1], sys.argv[2]
m = json.load(open("schema-manifest/manifest.json"))
MIRRORED = [e for e in m["entries"] if e.get("kind") == "MIRRORED"]   # compareLabel reads m.mirrored() only
SUBJ, HASH = re.compile(r"^[A-Za-z0-9._-]+$"), re.compile(r"^[0-9a-f]{64}$")

def bindings(env):
    """compareLabel's `want` sets for this service in this env: produces keyed by (subject, e.id, e.schemaHash)
    (the entry's own id, NOT the per-environment one), consumes as subject -> accepts ids."""
    produces, consumes = {}, {}
    for e in MIRRORED:
        s = e["subject"]
        for p in (e.get("producers") or []):
            if p.get("service") == svc and env in (p.get("environments") or []):
                produces[(s, e["id"], e["schemaHash"])] = e["version"]
        for c in (e.get("consumers") or []):
            if c.get("service") == svc and env in (c.get("environments") or []):
                consumes.setdefault(s, set()).update(c.get("accepts") or [])
    return produces, consumes

ENVS = ["dev", "prod", "es4"]                                   # the environments schema-code-verify.sh walks
sets = {env: bindings(env) for env in ENVS}
bound = [env for env in ENVS if sets[env][0] or sets[env][1]]
assert bound, "the manifest binds %s in NO environment, so there is no passing fixture to derive" % svc
# ONE requirements file is compared against EVERY bound environment, so the bindings must agree across them.
def shape(env):
    p, c = sets[env]
    return (sorted(p), sorted((k, sorted(v)) for k, v in c.items()))
for env in bound[1:]:
    assert shape(env) == shape(bound[0]), \
        "%s's bindings differ between %s and %s; one requirements file cannot satisfy both" % (svc, bound[0], env)

produces, consumes = sets[bound[0]]
assert produces, "%s produces nothing in the manifest; these fixtures need a produced subject" % svc
for (s, i, h) in produces:
    assert SUBJ.match(s), "produced subject is not a plain name: %r" % (s,)
    assert isinstance(i, int) and i > 0, "produced id is not a positive int: %r" % (i,)
    assert HASH.match(h), "produced schemaHash is not 64 hex chars: %r" % (h,)
for s, ids in consumes.items():
    assert SUBJ.match(s), "consumed subject is not a plain name: %r" % (s,)
    assert ids and all(isinstance(i, int) and i > 0 for i in ids), "consumed accepts is not positive ints: %r" % (ids,)

# The refusal fixture targets ONE produced tuple - the highest bound version. A service may legitimately be bound to
# several versions of the SAME subject (PlannerTest.an_image_label_matching_the_manifest_passes_R5e has one service
# producing eav-value at both v1 and v2 in prod), so there is deliberately NO per-subject uniqueness rule here; the
# fixtures mutate the selected tuple only and leave every other binding intact.
target = max(produces, key=lambda k: produces[k])
cands = [o for o in MIRRORED if o["subject"] == target[0] and o["id"] != target[1]
         and o.get("retained") is True and not ((o.get("producers") or []) + (o.get("consumers") or []))]
assert cands, "no RETAINED, unbound version of %s to use as the refusal case" % target[0]
u = max(cands, key=lambda o: o["version"])
assert HASH.match(u["schemaHash"]), "the retained version's schemaHash is not 64 hex chars"
assert u["schemaHash"] != target[2] and u["id"] != target[1], \
    "the retained version is indistinguishable from the bound one (same id or hash)"
assert (target[0], u["id"], u["schemaHash"]) not in produces, \
    "the retained version is itself one of this service's bindings, so swapping it in would not be a refusal"

ZERO = "0" * 64
def label(swap=None, zero=False):
    ps = []
    for key in sorted(produces):                 # the SELECTED tuple only; every other binding is passed through
        s, i, h = key
        if key == target and swap is not None:
            i, h = swap["id"], swap["schemaHash"]
        elif key == target and zero:
            h = ZERO
        ps.append({"subject": s, "id": i, "schemaHash": h})
    cs = [{"subject": s, "accepts": sorted(ids)} for s, ids in sorted(consumes.items())]
    return {"produces": ps, "consumes": cs}

os.makedirs(out, exist_ok=True)
for name, obj in (("good", label()), ("badhash", label(zero=True)), ("unbound", label(swap=u))):
    with open(os.path.join(out, name + ".json"), "w") as fh:
        json.dump(obj, fh)
assert label() != label(zero=True) and label() != label(swap=u), "a defect fixture is identical to the good one"
with open(os.path.join(out, "meta.txt"), "w") as fh:
    fh.write("subject %s\n" % target[0])
    fh.write("bound_v %d\n" % produces[target])
    fh.write("bound_id %d\n" % target[1])
    fh.write("unbound_v %d\n" % u["version"])
    fh.write("unbound_id %d\n" % u["id"])
    fh.write("envs %s\n" % ",".join(bound))
    fh.write("produces %d\n" % len(produces))
    fh.write("consumes %d\n" % len(consumes))
PY
[ "$drc" = 0 ] || { echo "cannot derive the fixtures from the manifest (reason above)" >&2; exit 1; }
for f in good.json badhash.json unbound.json meta.txt; do
  [ -s "$FX/$f" ] || { echo "the manifest derivation did not write $f" >&2; exit 1; }
done
SUBJECT=; BOUND_V=; BOUND_ID=; UNBOUND_V=; UNBOUND_ID=; ENVS=
while read -r k v; do          # READ, never eval: nothing from the manifest is evaluated by the shell
  case "$k" in
    subject)   SUBJECT="$v" ;;
    bound_v)   BOUND_V="$v" ;;
    bound_id)  BOUND_ID="$v" ;;
    unbound_v) UNBOUND_V="$v" ;;
    unbound_id) UNBOUND_ID="$v" ;;
    envs)      ENVS="$v" ;;
  esac
done < "$FX/meta.txt"
for v in "$SUBJECT" "$BOUND_V" "$BOUND_ID" "$UNBOUND_V" "$UNBOUND_ID" "$ENVS"; do
  [ -n "$v" ] || { echo "the manifest derivation left a meta field empty" >&2; exit 1; }
done
case "$BOUND_V$BOUND_ID$UNBOUND_V$UNBOUND_ID" in *[!0-9]*) echo "the derived versions/ids are not numeric" >&2; exit 1 ;; esac
echo "derived from the manifest: $SUBJECT bound at v$BOUND_V id=$BOUND_ID ($ENVS), retained+unbound v$UNBOUND_V id=$UNBOUND_ID"
mk() { rm -rf "$T/p"; mkdir -p "$T/p/$1/src/main/resources"; cp "$2" "$T/p/$1/src/main/resources/schema-requirements.json"; }
fail=0
: > "$T/empty-backlog"   # the stub checkouts are not the real tree; the real ratchet has its own tests (passed explicitly below)
expect() { local name="$1" want="$2" out rc=0; shift 2; out=$("$@" 2>&1) || rc=$?
  if [ "$want" = ok ]; then [ "$rc" = 0 ] && echo "ok     $name" || { echo "NOT-OK $name: must pass"; echo "$out" | tail -4; fail=1; }
  else [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "$want" && echo "ok     $name" || { echo "NOT-OK $name: expected failure '$want' (rc=$rc)"; echo "$out" | tail -4; fail=1; }; fi; }
run() { PATH="$T/bin:$PATH" bash scripts/ci/schema-code-verify.sh "$T/p" "$T/empty-backlog"; }
mk "$SVC-service" "$FX/good.json"; expect "requirements equal to the manifest's bindings and passing module tests" ok run
mk "$SVC-service" "$FX/badhash.json"; expect "a requirements hash that differs from the manifest" "requirements disagree with the manifest" run
# The RETAINED predecessor with its REAL id and REAL hash: in the manifest, bound to no service, so R5e must refuse
# it both ways (it lacks the bound pair and declares an unbound one). A fabricated hash - what this case used to
# carry - would have been refused for the wrong reason. This is the exact shape that broke the positive control.
mk "$SVC-service" "$FX/unbound.json"; expect "a retained version the manifest binds to no service (v$UNBOUND_V, real id and hash)" "requirements disagree" run
mk "$SVC-service" "$FX/good.json"; expect "module tests failing against the manifest" "tests failed against the manifest" env STUB_MVN=fail PATH="$T/bin:$PATH" bash scripts/ci/schema-code-verify.sh "$T/p" "$T/empty-backlog"
rm -rf "$T/p"; mkdir -p "$T/p/some-service/src/main/resources"; expect "no requirements file anywhere is an error, not 'verified'" "no schema-requirements.json found" run
mk ghost-service "$FX/good.json"; expect "a service the manifest binds to no environment" "is bound to no environment" run
exit $fail
