#!/usr/bin/env bash
# verify-public-surface.sh — the Gate 5 deployed proofs (PGL-040 … PGL-044), as one runnable check.
#
# WHY THIS EXISTS AS A SCRIPT
# ---------------------------
# Every other gate's claims are asserted by tests that run on every build. Gate 5's cannot be: they
# are properties of a RUNNING deployment — which routes are actually reachable from the internet,
# whether a token from one realm is really refused by the other, what the approval lifecycle does in
# practice. Those get verified once, by hand, on the day of the deploy, and are then never checked
# again — which is exactly the shape of a control that quietly stops being true.
#
# So they are written down as commands rather than as prose in a runbook. This can be re-run after
# any change to the tunnel, the realm, the NetworkPolicies or the image, and it answers in seconds.
#
# ⚠️ NOTHING HERE HAS BEEN RUN YET. At the time of writing the public deployment sits at zero replicas
# with no DNS record and no tunnel route (PGL-072), so every check below would fail to connect. That
# is the correct state — this script is the harness the proofs will run in, not evidence that they
# passed. Do not cite it as proof of anything until it has actually executed green against the
# deployed surface, and record that run.
#
# USAGE
#   verify-public-surface.sh --token-approved <jwt> --token-pending <jwt> --token-internal <jwt>
#
#   Every token argument is optional; checks needing one are SKIPPED (loudly) when it is absent, so a
#   partial run is possible without a skipped check ever being mistaken for a passed one.
set -uo pipefail

PUBLIC_HOST="${PUBLIC_HOST:-https://bleedingoptions.com}"
PUBLIC_AUTH_HOST="${PUBLIC_AUTH_HOST:-https://auth.bleedingoptions.com}"
INTERNAL_HOST="${INTERNAL_HOST:-https://bleadingoptions.com}"
INTERNAL_AUTH_HOST="${INTERNAL_AUTH_HOST:-https://auth.bleadingoptions.com}"
SYMBOL="${SYMBOL:-SPX}"

TOKEN_APPROVED="" ; TOKEN_PENDING="" ; TOKEN_INTERNAL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --token-approved) TOKEN_APPROVED="$2"; shift 2 ;;
    --token-pending)  TOKEN_PENDING="$2";  shift 2 ;;
    --token-internal) TOKEN_INTERNAL="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

PASS=0 ; FAIL=0 ; SKIP=0
pass() { PASS=$((PASS+1)); printf '  PASS  %s\n' "$*"; }
fail() { FAIL=$((FAIL+1)); printf '  FAIL  %s\n' "$*" >&2; }
skip() { SKIP=$((SKIP+1)); printf '  SKIP  %s\n' "$*" >&2; }

# Status of a GET. Never follows redirects: a 302 to a login page is a DIFFERENT answer from a 200,
# and following it would turn "denied" into "served" in the output.
status() {
  local url="$1"; shift
  curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$@" "$url" 2>/dev/null || echo "000"
}

expect() {
  local what="$1" want="$2" got="$3"
  if [[ "$got" == "$want" ]]; then pass "$what (=$got)"; else fail "$what: expected $want, got $got"; fi
}

# A denial is 401, 403 or 404 — NOT a 200 and not a redirect to content. Written as a set because
# which of the three a given route produces is a Spring Security detail, while "not served" is the
# property under test.
expect_denied() {
  local what="$1" got="$2"
  case "$got" in
    401|403|404) pass "$what (denied, $got)" ;;
    *)           fail "$what: expected denial, got $got" ;;
  esac
}

echo "=== PGL-040: the public host serves the Gamma Lab and NOTHING else ==="
expect "gamma-lab shell is served" 200 "$(status "$PUBLIC_HOST/gamma-lab")"
expect "apex redirects to the lab"  302 "$(status "$PUBLIC_HOST/")"
for path in /option-chain /greeks /system-status /paper-trades /stock-gex /context-tape /zones \
            /indicators /delta-flow /reversal /mission-control; do
  expect_denied "internal board $path" "$(status "$PUBLIC_HOST$path")"
done
for path in /api/greeks /api/orders/spread /api/paper-trades /api/config /api/connect \
            /api/system-status /api/hunt/state /api/stock-gex/stream; do
  expect_denied "internal API $path" "$(status "$PUBLIC_HOST$path")"
done
expect "board without a token is 401" 401 "$(status "$PUBLIC_HOST/api/stock-gex/board?symbol=$SYMBOL")"
# PGL-058: health must NOT be reachable from the internet.
expect_denied "actuator is not public" "$(status "$PUBLIC_HOST/actuator/health")"
expect_denied "actuator/env is not public" "$(status "$PUBLIC_HOST/actuator/env")"
# PGL-032: the admin console and master realm are edge-404.
expect "public /admin is 404 at the edge" 404 "$(status "$PUBLIC_AUTH_HOST/admin")"
expect "public /realms/master is 404 at the edge" 404 "$(status "$PUBLIC_AUTH_HOST/realms/master")"

echo
echo "=== PGL-036A: the two near-identical auth hosts are NOT the same thing ==="
# One letter apart, different trust. A typo anywhere in DNS, the tunnel or a manifest must show up
# here rather than as public traffic arriving at the internal identity provider.
pub_realm="$(curl -s --max-time 15 "$PUBLIC_AUTH_HOST/realms/bleedingoptions/.well-known/openid-configuration" \
  | jq -r '.issuer // empty' 2>/dev/null)"
int_realm="$(curl -s --max-time 15 "$INTERNAL_AUTH_HOST/realms/optionsedge/.well-known/openid-configuration" \
  | jq -r '.issuer // empty' 2>/dev/null)"
if [[ "$pub_realm" == "$PUBLIC_AUTH_HOST/realms/bleedingoptions" ]]; then
  pass "public auth host serves the bleedingoptions realm"
else
  fail "public auth host issuer is '$pub_realm', expected $PUBLIC_AUTH_HOST/realms/bleedingoptions"
fi
if [[ "$int_realm" == "$INTERNAL_AUTH_HOST/realms/optionsedge" ]]; then
  pass "internal auth host serves the optionsedge realm"
else
  fail "internal auth host issuer is '$int_realm', expected $INTERNAL_AUTH_HOST/realms/optionsedge"
fi
# The decisive one: the public host must NOT answer for the internal realm, or the two have been crossed.
expect_denied "public auth host does not serve the INTERNAL realm" \
  "$(status "$PUBLIC_AUTH_HOST/realms/optionsedge/.well-known/openid-configuration")"

echo
echo "=== PGL-041: cross-realm rejection, BOTH directions ==="
if [[ -n "$TOKEN_INTERNAL" ]]; then
  expect_denied "an optionsedge token is refused by the PUBLIC pod" \
    "$(status "$PUBLIC_HOST/api/stock-gex/board?symbol=$SYMBOL" -H "Authorization: Bearer $TOKEN_INTERNAL")"
else
  skip "PGL-041 (internal->public): pass --token-internal"
fi
if [[ -n "$TOKEN_APPROVED" ]]; then
  expect_denied "a bleedingoptions token is refused by the INTERNAL web tier" \
    "$(status "$INTERNAL_HOST/api/stock-gex/board?symbol=$SYMBOL" -H "Authorization: Bearer $TOKEN_APPROVED")"
else
  skip "PGL-041 (public->internal): pass --token-approved"
fi

echo
echo "=== PGL-042: the approval lifecycle ==="
if [[ -n "$TOKEN_PENDING" ]]; then
  code="$(status "$PUBLIC_HOST/api/stock-gex/board?symbol=$SYMBOL" -H "Authorization: Bearer $TOKEN_PENDING")"
  expect "an unapproved account is 403" 403 "$code"
  body="$(curl -s --max-time 15 -H "Authorization: Bearer $TOKEN_PENDING" \
    "$PUBLIC_HOST/api/stock-gex/board?symbol=$SYMBOL")"
  if grep -q 'NOT_APPROVED' <<<"$body"; then
    pass "the refusal carries the NOT_APPROVED code the page keys on"
  else
    fail "the 403 body does not carry NOT_APPROVED; the page will show a generic error: $body"
  fi
else
  skip "PGL-042 (pending): register an account, do NOT approve it, pass --token-pending"
fi
if [[ -n "$TOKEN_APPROVED" ]]; then
  expect "an approved account is served" 200 \
    "$(status "$PUBLIC_HOST/api/stock-gex/board?symbol=$SYMBOL" -H "Authorization: Bearer $TOKEN_APPROVED")"
else
  skip "PGL-042 (approved): pass --token-approved"
fi
echo "  NOTE  revocation-within-SLA is a TIMED check: remove the user from gamma-lab-approved, end"
echo "        their sessions, then re-run the approved check and confirm it turns 403 within 300 s"
echo "        plus measured clock offset (PGL-026, PGL-026B). It is not automated here because it"
echo "        needs a wall-clock wait and an admin action between two observations."

echo
echo "=== PGL-043: the INTERNAL deployment is untouched ==="
# The regression that would matter most: this whole project must not have changed the desk's site.
for path in /option-chain /greeks /system-status /gamma-lab /stock-gex; do
  code="$(status "$INTERNAL_HOST$path")"
  if [[ "$code" == "200" ]]; then
    pass "internal $path still served"
  else
    fail "internal $path returned $code — the internal deployment has regressed"
  fi
done

echo
echo "=== PGL-044: capacity — concurrent viewers collapse to one upstream request ==="
if [[ -n "$TOKEN_APPROVED" ]]; then
  # Twenty simultaneous reads of ONE board. With the cache and single-flight working, the upstream
  # sees one request; the observable proxy for that here is that every caller gets a 200 and the
  # responses are identical. The authoritative count is the upstream's own metric — read it there.
  tmp="$(mktemp -d)"
  for i in $(seq 1 20); do
    ( curl -s --max-time 20 -H "Authorization: Bearer $TOKEN_APPROVED" \
        "$PUBLIC_HOST/api/stock-gex/board?symbol=$SYMBOL&byExpiry=true" > "$tmp/$i.json" ) &
  done
  wait
  distinct="$(md5 -q "$tmp"/*.json 2>/dev/null | sort -u | wc -l | tr -d ' ')"
  if [[ "$distinct" == "1" ]]; then
    pass "20 concurrent viewers received ONE identical cached board"
  else
    fail "20 concurrent viewers received $distinct distinct bodies — single-flight/caching is not holding"
  fi
  rm -rf "$tmp"
  echo "  NOTE  confirm against the upstream's own request counter that it saw ONE call, not twenty."
else
  skip "PGL-044: pass --token-approved"
fi

echo
echo "=============================================="
printf 'PASS %d   FAIL %d   SKIP %d\n' "$PASS" "$FAIL" "$SKIP"
if [[ "$SKIP" -gt 0 ]]; then
  echo "SKIPPED checks are NOT passes. Supply the missing tokens before treating this as complete." >&2
fi
[[ "$FAIL" -eq 0 ]] || exit 1
[[ "$SKIP" -eq 0 ]] || exit 2
echo "All Gate 5 reachable proofs passed."
