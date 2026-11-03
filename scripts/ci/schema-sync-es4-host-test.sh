#!/usr/bin/env bash
# Negative controls for schema-sync-es4-host.sh (docs/schema-governance R11b, A13): a bad bundle must be refused BEFORE the wipe (verify) and a good one
# must register through the engine. Mock registry, dry-run apply only.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'kill $SRV 2>/dev/null || true; rm -rf "$T"' EXIT
mkdir -p "$T/repo/scripts/ci" "$T/repo/scripts/kafka"
cp scripts/ci/schema-sync-es4-host.sh "$T/repo/scripts/ci/"; cp scripts/kafka/ensure-fixed-id-schemas.sh "$T/repo/scripts/kafka/"
H="$T/repo/scripts/ci/schema-sync-es4-host.sh"
S='{"type":"record","name":"A","fields":[{"name":"a","type":"int","default":1}]}'
mkbundle() { rm -rf "$T/b"; mkdir -p "$T/b/schemas/s-value"; printf '%s' "$S" > "$T/b/schemas/s-value/v1.avsc"
  printf 's-value|2100|s-value/v1.avsc|1\n' > "$T/b/plan.map"; echo c0ffee > "$T/b/manifest.commit"
  ( cd "$T/b" && { shasum -a 256 plan.map manifest.commit; find schemas -type f | sort | xargs shasum -a 256; } > SHA256SUMS ); }
cat > "$T/mock.py" <<'PY'
import http.server, sys
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def s(self, c, b='{}'): self.send_response(c); self.send_header("Content-Type","application/json"); self.end_headers(); self.wfile.write(b.encode())
    def do_GET(self):
        if self.path == "/mode": return self.s(200, '{"mode":"READWRITE"}')
        if self.path == "/config": return self.s(200, '{"compatibilityLevel":"BACKWARD"}')
        return self.s(404, '{"error_code":40401}')
    def do_POST(self): self.rfile.read(int(self.headers.get("Content-Length") or 0)); return self.s(404, '{"error_code":40401}')
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
PORT=$((20000 + RANDOM % 20000)); python3 "$T/mock.py" "$PORT" & SRV=$!; sleep 1
fail=0
expect() { local name="$1" want="$2"; shift 2; local out rc=0; out=$("$@" 2>&1) || rc=$?
  if [ "$want" = ok ]; then [ "$rc" = 0 ] && echo "ok     $name" || { echo "NOT-OK $name: must pass"; echo "$out" | tail -4; fail=1; }
  else [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "$want" && echo "ok     $name" || { echo "NOT-OK $name: expected failure '$want' (rc=$rc)"; echo "$out" | tail -4; fail=1; }; fi; }
mkbundle; expect "a good bundle verifies against its own commit" ok bash "$H" verify "$T/b" c0ffee
mkbundle; expect "a bundle from a different commit is stale" "stale bundle" bash "$H" verify "$T/b" deadbeef
mkbundle; printf 'x' >> "$T/b/schemas/s-value/v1.avsc"; expect "an altered snapshot" "does not match its SHA256SUMS" bash "$H" verify "$T/b" c0ffee
mkbundle; rm "$T/b/plan.map"; expect "a bundle with no plan.map" "missing or incomplete" bash "$H" verify "$T/b" c0ffee
mkbundle; : > "$T/b/plan.map"; ( cd "$T/b" && { shasum -a 256 plan.map manifest.commit; find schemas -type f | sort | xargs shasum -a 256; } > SHA256SUMS ); expect "an EMPTY plan is not 'nothing to register'" "empty plan.map" bash "$H" verify "$T/b" c0ffee
mkbundle; printf 's-value|2100|s-value/ghost.avsc|1\n' > "$T/b/plan.map"; ( cd "$T/b" && { shasum -a 256 plan.map manifest.commit; find schemas -type f | sort | xargs shasum -a 256; } > SHA256SUMS ); expect "a row naming a snapshot the bundle lacks" "does not contain" bash "$H" verify "$T/b" c0ffee
expect "no bundle directory at all" "missing or incomplete" bash "$H" verify "$T/nope" c0ffee
mkbundle; expect "apply (dry run) registers the plan through the engine" ok env SCHEMA_REGISTRY_URL="http://127.0.0.1:$PORT" DEPLOY_DRY_RUN=true bash "$H" apply "$T/b" c0ffee
mkbundle; printf 'x' >> "$T/b/plan.map"; expect "apply refuses a bundle that fails verification" "SHA256SUMS" env SCHEMA_REGISTRY_URL="http://127.0.0.1:$PORT" DEPLOY_DRY_RUN=true bash "$H" apply "$T/b" c0ffee
exit $fail
