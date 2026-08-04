#!/usr/bin/env bash
# Assert the portal's application state matches the REQ-5d contract. EXITS NON-ZERO on mismatch.
#
# A postcondition that pretty-prints for a human to squint at is not a postcondition. Every check
# here compares against an expected value and fails the run if it differs, so "state applied" is a
# machine-verified claim rather than an assertion of good intent.
#
# Read-only: it reads Bugzilla's REST API through the loopback ADMIN listener. It never writes, and
# it never touches the public listener.
set -uo pipefail

PROD_HOST=${PROD_HOST:-192.168.100.252}
PROD_SSH=${PROD_SSH:-abhinav@${PROD_HOST}}
ADMIN_URL=${ADMIN_URL:-http://127.0.0.1:8095}
DB_CONTAINER=${DB_CONTAINER:-options-edge-bugzilla-req-db}
WEB_CONTAINER=${WEB_CONTAINER:-options-edge-bugzilla-req-web}
DB_NAME=${DB_NAME:-bugzilla_req}

FAILED=0
pass() { echo "  PASS  $*"; }
fail() { echo "  FAIL  $*"; FAILED=1; }

rget() { ssh -o BatchMode=yes -o ConnectTimeout=20 "$PROD_SSH" "curl -s -m 15 '${ADMIN_URL}$1'"; }

# Run one SQL statement against the portal database WITHOUT putting the password in argv.
# `mysql -p"$PW"` would expose it through /proc/*/cmdline to every process on the host; the
# credential is written to a 0600 defaults-file inside the container and removed immediately.
dbq() {
  local sql=$1
  ssh -o BatchMode=yes -o ConnectTimeout=20 "$PROD_SSH" \
    "docker exec ${DB_CONTAINER} sh -c '
       set -eu; umask 077
       D=\$(mktemp)
       printf \"[client]\\nuser=root\\npassword=%s\\n\" \"\$MARIADB_ROOT_PASSWORD\" > \"\$D\"
       trap \"rm -f \\\"\$D\\\"\" EXIT
       mysql --defaults-file=\"\$D\" -BN -e \"$sql\" ${DB_NAME}
     '" 2>/dev/null | tr -d '\r'
}

echo "== REQ-5d postconditions against ${ADMIN_URL} (admin listener, loopback) =="

# --- parameters -------------------------------------------------------------------------------
# /rest/parameters exposes only the public subset when unauthenticated. That subset happens to carry
# the ones that matter most here (urlbase, attachment limits, requirelogin behaviour); anything not
# exposed is asserted from the rendered answers file inside the container instead, which is the same
# file checksetup consumed.
PARAMS_JSON=$(rget /rest/parameters)
echo "$PARAMS_JSON" | python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    print("  FAIL  /rest/parameters did not return JSON"); sys.exit(1)
p = d.get("parameters", d)
expected = {"maxattachmentsize": 10240, "urlbase": "https://req.fullfunding.nl/"}
rc = 0
for k, want in expected.items():
    got = p.get(k)
    if got is None:
        print(f"  SKIP  {k} not exposed unauthenticated (asserted from the container template)")
        continue
    if str(got) != str(want):
        print(f"  FAIL  param {k}: expected {want!r}, got {got!r}"); rc = 1
    else:
        print(f"  PASS  param {k} = {want!r}")
sys.exit(rc)
' || FAILED=1

# The parameters the REST API will not show anonymously are asserted directly against the live
# params file in the running container — the same state checksetup wrote.
LIVE=$(ssh -o BatchMode=yes -o ConnectTimeout=20 "$PROD_SSH" \
  "docker exec ${WEB_CONTAINER} cat /var/www/html/data/params.json 2>/dev/null")
echo "$LIVE" | python3 -c '
import json, sys
raw = sys.stdin.read().strip()
if not raw:
    print("  FAIL  could not read live params.json from the container"); sys.exit(1)
p = json.loads(raw)
expected = {
    "user_info_class": "Env,CGI",
    "auth_env_id": "OIDC_CLAIM_sub",
    "auth_env_email": "OIDC_CLAIM_email",
    "auth_env_realname": "OIDC_CLAIM_name",
    "requirelogin": 1,
    "createemailregexp": "",
    "maxlocalattachment": 0,
    "usevisibilitygroups": 1,
    "insidergroup": "",
    "letsubmitterchoosepriority": 0,
    "useqacontact": 0,
    "usetargetmilestone": 0,
    "urlbase": "https://req.fullfunding.nl/",
    "maxattachmentsize": 10240,
}
rc = 0
for k, want in expected.items():
    got = p.get(k)
    if str(got) != str(want):
        print(f"  FAIL  param {k}: expected {want!r}, got {got!r}"); rc = 1
    else:
        print(f"  PASS  param {k} = {want!r}")
sys.exit(rc)
' || FAILED=1

# --- products / components --------------------------------------------------------------------
# Asserted against the database rather than the API: an unauthenticated caller cannot enumerate
# products, and `requirelogin` is on precisely so it cannot.
PRODUCTS=$(dbq "SELECT name FROM products ORDER BY name")
if [ "$PRODUCTS" = "Requirements" ]; then
  pass "exactly one product, named Requirements"
else
  fail "products should be exactly 'Requirements', got: $(echo "$PRODUCTS" | tr '\n' ',')"
fi

COMPONENTS=$(dbq "SELECT name FROM components")
if [ "$COMPONENTS" = "General" ]; then
  pass "exactly one component, named General"
else
  fail "components should be exactly 'General', got: $(echo "$COMPONENTS" | tr '\n' ',')"
fi

# --- flag types (must be none) ------------------------------------------------------------------
# Bugzilla lets any user who can see a bug set a flag (Bug.pm:4567-4570), so the matrix's flag DENY
# row is only true while zero flag types exist. This is the check that keeps that row honest.
FLAGS=$(dbq "SELECT COUNT(*) FROM flagtypes")
[ "$FLAGS" = "0" ] && pass "no flag types defined" || fail "expected 0 flag types, got '${FLAGS}'"

# --- privileged group membership ----------------------------------------------------------------
# Every membership of a privileged group must belong to the single admin account. An external user
# appearing here would silently invalidate most DENY cells in the matrix.
PRIV=$(dbq "SELECT p.login_name, g.name FROM user_group_map m JOIN profiles p ON p.userid=m.user_id JOIN groups g ON g.id=m.group_id WHERE g.name IN ('admin','editbugs','canconfirm','creategroups','editcomponents','editclassifications','bz_sudoers')")
ADMIN_EMAIL=${BZ_ADMIN_EMAIL:-abhinav@fullfunding.nl}
UNEXPECTED=$(echo "$PRIV" | awk -v a="$ADMIN_EMAIL" 'NF && $1 != a {print}')
# An EMPTY result must not pass: that would mean the admin account is missing or holds no admin
# group, which is a different broken state, not a clean one. Assert positively first.
ADMIN_HAS_ADMIN=$(echo "$PRIV" | awk -v a="$ADMIN_EMAIL" 'NF && $1 == a && $2 == "admin" {c++} END{print c+0}')
if [ "$ADMIN_HAS_ADMIN" -lt 1 ]; then
  fail "${ADMIN_EMAIL} does not hold the 'admin' group — expected exactly one administrator"
elif [ -n "$UNEXPECTED" ]; then
  fail "non-admin accounts hold privileged groups:"; echo "$UNEXPECTED"
else
  pass "only ${ADMIN_EMAIL} holds privileged groups, and it does hold 'admin'"
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "REQ-5d postconditions: ALL PASS"
else
  echo "REQ-5d postconditions: FAILURES ABOVE — the portal's state does not match its contract" >&2
fi
exit "$FAILED"
