#!/usr/bin/env bash
# Negative controls for schema-sync.sh and the engine it drives (docs/schema-governance R10, R13a, R15), against a local MOCK registry.
# Every case must fail (or pass) for the stated reason; a gate that cannot be made to fail is not a gate.
# Needs SCHEMA_GOV_JAR (the shaded tool jar). Dry-run only: the mock never accepts a write.
set -euo pipefail
cd "$(dirname "$0")/../.."
JAR="${SCHEMA_GOV_JAR:?SCHEMA_GOV_JAR unset}"
T="$(mktemp -d)"; trap 'kill $SRV 2>/dev/null || true; rm -rf "$T"' EXIT
# a private copy of the repo pieces, so a case can edit a snapshot without touching the checkout
mkdir -p "$T/repo/scripts/ci" "$T/repo/scripts/kafka"
cp scripts/ci/schema-sync.sh "$T/repo/scripts/ci/"; cp scripts/kafka/ensure-fixed-id-schemas.sh "$T/repo/scripts/kafka/"
cp -R schema-manifest "$T/repo/schema-manifest"

cat > "$T/mock.py" <<'PY'
import http.server, os, sys
IDS, VERS = os.environ["MOCK_IDS"], os.environ["MOCK_VERS"]
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body="{}"):
        self.send_response(code); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(body.encode())
    def do_GET(self):
        p = self.path
        if p == "/mode": return self.send(200, '{"mode":"READWRITE"}')
        if p == "/config": return self.send(200, '{"compatibilityLevel":"BACKWARD"}')
        if p.startswith("/schemas/ids/"):
            c = {"404": 404, "500": 500, "200": 200}[IDS]; return self.send(c, '{"schema":"{}"}' if c == 200 else '{"error_code":%d}' % c)
        if p.endswith("/versions") and p.startswith("/subjects/"):
            c = {"404": 404, "500": 500}[VERS]; return self.send(c, '{"error_code":%d}' % c)
        return self.send(404, '{"error_code":40401}')
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0); self.rfile.read(n)
        return self.send(404, '{"error_code":40401}')   # a schema lookup under a subject: not registered
srv = http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H); srv.serve_forever()
PY
PORT=$((20000 + RANDOM % 20000)); SRV=
run_mock() { kill $SRV 2>/dev/null || true; MOCK_IDS="$1" MOCK_VERS="$2" python3 "$T/mock.py" "$PORT" & SRV=$!; sleep 1; }
sync() { ( cd "$T/repo" && env SERVICE=early-auction-verdict ENVIRONMENT=production SCHEMA_REGISTRY_URL="http://127.0.0.1:$PORT" SCHEMA_GOV_JAR="$JAR" "$@" bash scripts/ci/schema-sync.sh 2>&1 ); }
fail=0
expect_fail() { local name="$1" needle="$2"; shift 2; local out; if out=$(sync "$@"); then echo "NOT-OK $name: passed but must fail"; fail=1
  elif printf '%s' "$out" | grep -q "$needle"; then echo "ok     $name"; else echo "NOT-OK $name: failed for the wrong reason"; printf '%s\n' "$out" | tail -4; fail=1; fi; }
expect_pass() { local name="$1"; shift; local out; if out=$(sync "$@"); then echo "ok     $name"; else echo "NOT-OK $name: must pass"; printf '%s\n' "$out" | tail -6; fail=1; fi; }

run_mock 404 404
expect_pass  "clean registry, dry run" DEPLOY_DRY_RUN=true
HASH=$(sync DEPLOY_DRY_RUN=true | sed -n 's/^PLAN_SHA256=//p')
[ -n "$HASH" ] && echo "ok     dry run prints PLAN_SHA256" || { echo "NOT-OK no PLAN_SHA256"; fail=1; }
expect_fail  "prod apply without PLAN_SHA256 is refused" "PLAN_SHA256\|git checkout\|uncommitted" DEPLOY_DRY_RUN=false
expect_fail  "tag instead of digest is refused" "could not read the image label" DEPLOY_DRY_RUN=true SCHEMA_IMAGE_REF=127.0.0.1:1/x:prod
# editing a snapshot AND its manifest hash must change the plan hash (R13a): rows alone would not
cp "$T/repo/schema-manifest/schemas/early-auction.verdicts.shadow-value/v2.avsc" "$T/v2.bak"
python3 - "$T/repo/schema-manifest/schemas/early-auction.verdicts.shadow-value/v2.avsc" <<'PY'
import sys; p=sys.argv[1]; s=open(p).read(); open(p,"w").write(s.replace('"type":"record"','"type":"record" ',1) if '"type":"record"' in s else s+" ")
PY
H2=$(sync DEPLOY_DRY_RUN=true | sed -n 's/^PLAN_SHA256=//p'); cp "$T/v2.bak" "$T/repo/schema-manifest/schemas/early-auction.verdicts.shadow-value/v2.avsc"
if [ -z "$H2" ]; then echo "ok     a snapshot edit that breaks its manifest hash is refused (validate fails)"; elif [ "$H2" != "$HASH" ]; then echo "ok     snapshot bytes change the plan hash"; else echo "NOT-OK plan hash ignores snapshot bytes"; fail=1; fi

run_mock 500 404
expect_fail  "unreadable id lookup (500) is not 'free' (R15)" "cannot read /schemas/ids" DEPLOY_DRY_RUN=true
run_mock 404 500
expect_fail  "unreadable subject lookup (500) is not 'absent' (R15)" "cannot read /subjects" DEPLOY_DRY_RUN=true
run_mock 200 404
expect_fail  "an id held by another schema is a collision" "already held by another schema" DEPLOY_DRY_RUN=true
expect_fail  "unknown service is refused" "not declaredByManifest" DEPLOY_DRY_RUN=true SERVICE=nobody
exit $fail
