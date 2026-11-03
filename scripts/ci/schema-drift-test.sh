#!/usr/bin/env bash
# Negative controls for schema-drift.sh against a mock registry (docs/schema-governance R10, R14, R15): every failing state must exit non-zero.
set -euo pipefail
cd "$(dirname "$0")/../.."
JAR="${SCHEMA_GOV_JAR:-tools/schema-governance/schema-governance-tool.jar}"
T="$(mktemp -d)"; trap 'kill $SRV 2>/dev/null || true; rm -rf "$T"' EXIT
mkdir -p "$T/repo/scripts/ci" "$T/repo/tools/schema-governance"
cp scripts/ci/schema-drift.sh "$T/repo/scripts/ci/"; cp tools/schema-governance/PIN "$JAR" "$T/repo/tools/schema-governance/"
cp -R schema-manifest "$T/repo/schema-manifest"
PORT=$((20000 + RANDOM % 20000)); SRV=
# a registry that serves exactly early-auction v1,v2 (prod) as the manifest says, plus switchable defects
cat > "$T/mock.py" <<'PY'
import http.server, json, os, sys, urllib.parse
M = json.load(open(os.environ["MANIFEST"])); MODE = os.environ["MODE"]
ents = [e for e in M["entries"] if e["kind"] == "MIRRORED" and "prod" in e["environments"]]
snap = os.environ["SNAP"]
def text(e): return open(os.path.join(snap, e["schemaFile"])).read()
subjects = {}
for e in ents: subjects.setdefault(e["subject"], {})[e["version"]] = (e.get("idByEnvironment", {}).get("prod", e["id"]), text(e))
for e in M["entries"]:
    if e["kind"] == "ENV_LOCAL" and "prod" in e["environments"]: subjects.setdefault(e["subject"], {1: (1, '{"type":"record","name":"X","fields":[]}')})
if MODE == "extra": subjects["early-auction.verdicts.shadow-value"][3] = (3059, "{}")
if MODE == "missing": subjects.pop("early-auction.verdicts.shadow-value", None)
if MODE == "diffid": i, tx = subjects["early-auction.verdicts.shadow-value"][2]; subjects["early-auction.verdicts.shadow-value"][2] = (i + 1, tx)
if MODE == "difftext": i, tx = subjects["early-auction.verdicts.shadow-value"][2]; subjects["early-auction.verdicts.shadow-value"][2] = (i, tx.replace('"name":"EarlyAuctionVerdictLedger"', '"name":"EarlyAuctionVerdictLedgerX"', 1))
if MODE == "absentlocal":
    for e in M["entries"]:
        if e["kind"] == "ENV_LOCAL" and "prod" in e["environments"]: subjects.pop(e["subject"], None)
if MODE == "unmanifested": subjects["stranger"] = {1: (9, '{"type":"record","name":"S","fields":[]}')}
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def j(self, c, o): b = json.dumps(o).encode(); self.send_response(c); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        if MODE == "down": return self.j(500, {"error_code": 500})
        p = urllib.parse.unquote(self.path)
        if p == "/subjects": return self.j(200, sorted(subjects))
        parts = p.strip("/").split("/")
        if parts[0] == "subjects" and parts[2] == "versions":
            s = subjects.get(parts[1])
            if s is None: return self.j(404, {"error_code": 40401})
            if len(parts) == 3: return self.j(200, sorted(s))
            v = int(parts[3]); i, t = s[v]; return self.j(200, {"subject": parts[1], "version": v, "id": i, "schema": t})
        return self.j(404, {})
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
run() { kill $SRV 2>/dev/null || true; MODE="$1" MANIFEST="$T/repo/schema-manifest/manifest.json" SNAP="$T/repo/schema-manifest/schemas" python3 "$T/mock.py" "$PORT" & SRV=$!; sleep 1
  python3 - "$T/repo/schema-manifest/registries.json" "$PORT" <<'PY'
import json,sys; p=sys.argv[1]; d=json.load(open(p)); d["prod"]="http://127.0.0.1:"+sys.argv[2]; json.dump(d,open(p,"w"))
PY
}
fail=0
check() { local name="$1" want="$2" out rc=0; out=$(cd "$T/repo" && SCHEMA_DRIFT_ANYTIME=true bash scripts/ci/schema-drift.sh prod 2>&1) || rc=$?
  if [ "$want" = ok ]; then [ "$rc" = 0 ] && echo "ok     $name" || { echo "NOT-OK $name: must pass"; echo "$out" | tail -5; fail=1; }
  else [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "$want" && echo "ok     $name" || { echo "NOT-OK $name: expected failure matching '$want' (rc=$rc)"; echo "$out" | tail -5; fail=1; }; fi; }
run clean;        check "a registry equal to the manifest is clean" ok
run extra;        check "a dev-only version found on prod (EXTRA_VERSION)" "EXTRA_VERSION"
run missing;      check "the early-auction subject missing (MISSING)" "MISSING"
run diffid;       check "same version, wrong registry id (DIFFERENT)" "DIFFERENT"
run difftext;     check "same id, different schema text (DIFFERENT)" "DIFFERENT"
run absentlocal;  check "an absent ENV_LOCAL subject is the one tolerated absence (ABSENT_OK)" ok
run unmanifested; check "a subject the manifest never named (UNMANIFESTED)" "UNMANIFESTED"
run down;         check "an unreachable registry is a failure, not zero drift" "DRIFT"
# a flaky registry must not turn a failing drift into OK: ONE drift query decides (the earlier two-query form could contradict itself)
run clean; python3 - "$T/repo/scripts/ci/schema-drift.sh" <<'PY'
import sys; t=open(sys.argv[1]).read(); assert t.count(' drift --manifest') == 1, "schema-drift.sh must query drift exactly once per environment"
PY
echo "ok     schema-drift.sh queries drift exactly once per environment"
(cd "$T/repo" && SCHEMA_DRIFT_ANYTIME=true bash scripts/ci/schema-drift.sh 'prod;touch /tmp/pwned' >/dev/null 2>&1) && { echo "NOT-OK an injected environment word was accepted"; fail=1; } || echo "ok     an environment word that is not dev, prod or es4 is refused"
[ ! -e /tmp/pwned ] && echo "ok     nothing in the environment word was executed" || { echo "NOT-OK injected command ran"; fail=1; }
exit $fail
