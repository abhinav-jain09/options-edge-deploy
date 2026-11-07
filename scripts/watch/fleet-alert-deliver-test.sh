#!/usr/bin/env bash
# Controlled delivery test (R7d): fleet-alert-deliver.sh driven against a LOCAL endpoint, so the whole
# path - run the check, compose, source oe-alert.sh, POST - is exercised without a real webhook.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
DELIVER="$HERE/fleet-alert-deliver.sh"
W="$(mktemp -d)"; trap 'kill ${SRV:-0} 2>/dev/null || true; rm -rf "$W"' EXIT
pass=0; fail=0

ok()  { printf '  ok   %-60s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %-60s %s\n' "$1" "$2"; fail=$((fail+1)); }

# A webhook that records what it is sent and can be told to answer non-2xx.
cat > "$W/hook.py" <<'PY'
import http.server, json, os, sys
CODE = int(os.environ.get("HTTP_CODE", "204"))
OUT = os.environ["RECORD"]
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n).decode("utf-8", "replace")
        with open(OUT, "a") as fh:
            fh.write(body + "\n")
        self.send_response(CODE); self.end_headers(); self.wfile.write(b"{}")
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY

PORT=$((21000 + RANDOM % 9000))
RECORD="$W/posted.txt"; : > "$RECORD"
HTTP_CODE=204 RECORD="$RECORD" python3 "$W/hook.py" "$PORT" & SRV=$!
sleep 1
HOOK="http://127.0.0.1:$PORT/webhook"

# a stub check whose exit status and output the test controls
cat > "$W/check.sh" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "${STUB_OUT:-}"
exit "${STUB_RC:-0}"
SH
chmod +x "$W/check.sh"

deliver() { # <mode> <rc> <out> [webhook]
  local mode="$1" rc="$2" text="$3" hook="${4-$HOOK}" rrc=0
  : > "$RECORD"
  DISCORD_WEBHOOK_URL="$hook" FLEET_ENV=dev FLEET_CHECK=stub FLEET_MODE="$mode" \
    STUB_RC="$rc" STUB_OUT="$text" OE_OPS_DIR="$W" \
    bash "$DELIVER" "$W/check.sh" > "$W/log.txt" 2>&1 || rrc=$?
  echo "$rrc"
}

# --- the check's status is the script's status, always
rc=$(deliver paging 0 "stub: all clear")
[ "$rc" = 0 ] && ok "exit 0 from the check is exit 0 from the deliverer" || bad "exit 0 passes through" "got $rc"
rc=$(deliver paging 1 "CRASHLOOP pod-a: container app is CrashLoopBackOff")
[ "$rc" = 1 ] && ok "exit 1 from the check is RE-RAISED after delivery" || bad "exit 1 re-raised" "got $rc"
rc=$(deliver paging 2 "ERROR: cannot read kubectl output")
[ "$rc" = 2 ] && ok "exit 2 from the check is re-raised" || bad "exit 2 re-raised" "got $rc"

# --- what actually reached the endpoint
deliver paging 1 "CRASHLOOP pod-a: container app is CrashLoopBackOff" >/dev/null
if grep -q "CRASHLOOP" "$RECORD" && grep -q '"content"' "$RECORD"; then
  ok "a finding is POSTed as a Discord content payload"
else
  bad "a finding is POSTed" "record: $(head -c 200 "$RECORD")"
fi
if grep -q "dev" "$RECORD"; then ok "the payload names the environment"; else bad "payload names the env" ""; fi

deliver paging 0 "stub: all clear" >/dev/null
[ ! -s "$RECORD" ] && ok "a clean check POSTs nothing" || bad "clean check is silent" "$(head -c 120 "$RECORD")"

# --- R1e/§1b: observation-only is silent on a FINDING but not on exit 2
deliver observation-only 1 "SKEW t [svc]: busiest partition took 100%" >/dev/null
[ ! -s "$RECORD" ] && ok "observation-only POSTs nothing for a finding" || bad "observation-only silent on finding" "$(head -c 120 "$RECORD")"
deliver observation-only 2 "ERROR: broker unreachable" >/dev/null
if grep -q "could not check" "$RECORD"; then
  ok "observation-only STILL POSTs 'could not check'"
else
  bad "observation-only posts exit 2" "record: $(head -c 200 "$RECORD")"
fi

# --- R1d: a delivery failure must not change the verdict, and must not read as delivered
kill $SRV 2>/dev/null; sleep 0.3
HTTP_CODE=500 RECORD="$RECORD" python3 "$W/hook.py" "$PORT" & SRV=$!
sleep 1
rc=$(deliver paging 1 "CRASHLOOP pod-a: container app is CrashLoopBackOff")
if [ "$rc" = 1 ] && grep -qF "ALERT DELIVERY FAILED" "$W/log.txt"; then
  ok "a 500 is logged as FAILED and the check's verdict survives"
else
  bad "a 500 is logged as a failure" "rc=$rc log: $(grep -i deliver "$W/log.txt" | head -1)"
fi
if grep -qF "(alert delivered to Discord" "$W/log.txt"; then
  bad "a 500 must NOT read as delivered" "log claims delivery"
else
  ok "a 500 does not read as 'delivered'"
fi

kill $SRV 2>/dev/null; sleep 0.3
rc=$(deliver paging 1 "CRASHLOOP pod-a: x" "http://127.0.0.1:$PORT/gone")
[ "$rc" = 1 ] && ok "an unreachable webhook does not change the verdict" || bad "unreachable webhook" "got $rc"
if grep -qF "ALERT: " "$W/log.txt"; then
  ok "the condition is LOGGED before delivery is attempted"
else
  bad "the condition is logged first" "no ALERT: line"
fi

# --- a bad mode is refused, not silently treated as paging
rrc=0
FLEET_ENV=dev FLEET_CHECK=stub FLEET_MODE=whatever bash "$DELIVER" "$W/check.sh" >/dev/null 2>&1 || rrc=$?
[ "$rrc" = 2 ] && ok "an unknown FLEET_MODE is refused" || bad "unknown mode refused" "got $rrc"
rrc=0
FLEET_ENV=dev FLEET_CHECK=stub bash "$DELIVER" >/dev/null 2>&1 || rrc=$?
[ "$rrc" = 2 ] && ok "no check command is refused" || bad "no command refused" "got $rrc"

# --- MAJOR-1 from the implementation review: a state directory that cannot be made must still REACH
# Discord as "could not check". Before stateful-check-run.sh the Jenkinsfile exited before the deliverer.
PORT=$((21000 + RANDOM % 9000)); HOOK="http://127.0.0.1:$PORT/webhook"
HTTP_CODE=204 RECORD="$RECORD" python3 "$W/hook.py" "$PORT" & SRV=$!
sleep 1
RO="$W/readonly"; mkdir -p "$RO"; chmod 500 "$RO"
: > "$RECORD"
rrc=0
DISCORD_WEBHOOK_URL="$HOOK" FLEET_ENV=dev FLEET_CHECK=kafka-lag FLEET_MODE=paging \
  FLEET_STATE_DIR="$RO/sub" OE_OPS_DIR="$W" \
  bash "$HERE/fleet-alert-deliver.sh" bash "$HERE/stateful-check-run.sh" kafka-lag.py \
  > "$W/log.txt" 2>&1 || rrc=$?
chmod 755 "$RO"
if [ "$rrc" = 2 ] && grep -q "could not check" "$RECORD"; then
  ok "an unmakeable state directory is DELIVERED as 'could not check'"
else
  bad "unmakeable state dir reaches Discord" "rc=$rrc record=$(head -c 160 "$RECORD")"
fi

# --- the wrapper refuses to guess where the baseline lives
rrc=0
( unset WORKSPACE; FLEET_ENV=dev bash "$HERE/stateful-check-run.sh" kafka-lag.py >"$W/o.txt" 2>&1 ) || rrc=$?
if [ "$rrc" = 2 ] && grep -q "neither FLEET_STATE_DIR nor WORKSPACE" "$W/o.txt"; then
  ok "no state location is an ERROR, not a guess"
else
  bad "no state location is refused" "rc=$rrc $(head -c 120 "$W/o.txt")"
fi
rrc=0
FLEET_ENV=dev FLEET_STATE_DIR="$W/st" bash "$HERE/stateful-check-run.sh" no-such-check.py >"$W/o.txt" 2>&1 || rrc=$?
[ "$rrc" = 2 ] && ok "an unknown check script is refused" || bad "unknown check refused" "got $rrc"

printf 'fleet-alert-deliver: %d ok, %d FAILED\n' "$pass" "$fail"
[ "$fail" = 0 ]
