#!/usr/bin/env bash
# WHO IS WAITING FOR APPROVAL — and approve them, without hunting through the admin console.
#
#   scripts/ops/bleedingoptions-pending.sh                     # list the queue
#   scripts/ops/bleedingoptions-pending.sh --approve <email>    # approve one person
#   scripts/ops/bleedingoptions-pending.sh --revoke  <email>    # take it away again
#
# WHY THIS EXISTS. Keycloak's user list shows an "email verified" badge and NO group column, so it
# cannot tell you who is waiting — and the two are unrelated: verification is about a clicked link,
# approval is membership of /gamma-lab-approved. Reading the queue means Groups -> pending-approval
# -> Members, four clicks deep, and approving is four more. This is that, as one command.
#
# It reads the queue from the GROUP, not from a user list, because the group IS the queue: every
# registration lands in /pending-approval via the realm's defaultGroups, and approving removes them.
set -euo pipefail

KC="${KC_URL:-http://192.168.100.252:8189}"
REALM="${KC_REALM:-bleedingoptions}"
KC_ADMIN_USER="${KC_ADMIN_USER:-admin}"

die() { echo "FAIL: $*" >&2; exit 1; }

# The admin password is PROMPTED, not taken from a file or an argument: it must not land in shell
# history or in the process list, and the bootstrap value in the k8s Secret goes stale the moment
# anyone rotates it or retires that account.
if [ -z "${KC_ADMIN_PASSWORD:-}" ]; then
  printf 'Keycloak admin password for %s (not echoed): ' "$KC_ADMIN_USER"
  read -rs KC_ADMIN_PASSWORD; printf '\n'
fi

TOKEN="$(printf '%s' "$KC_ADMIN_PASSWORD" | curl -s -X POST \
  "$KC/realms/master/protocol/openid-connect/token" \
  -d client_id=admin-cli -d grant_type=password -d "username=$KC_ADMIN_USER" \
  --data-urlencode password@- \
  | python3 -c 'import json,sys
try:
    d = json.load(sys.stdin); print(d.get("access_token") or "")
except Exception: print("")')"
[ -n "$TOKEN" ] || die "could not authenticate as $KC_ADMIN_USER (wrong password, or the account is disabled)"

api() { curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"; }
B="$KC/admin/realms/$REALM"

group_id() {
  api "$B/groups" | OE_WANT="$1" python3 -c '
import json,os,sys
want = os.environ["OE_WANT"]
for g in json.load(sys.stdin):
    if g["name"] == want: print(g["id"]); break'
}
PENDING="$(group_id pending-approval)"
APPROVED="$(group_id gamma-lab-approved)"
[ -n "$PENDING" ] && [ -n "$APPROVED" ] || die "the approval groups are missing from realm $REALM"

user_id() {
  api "$B/users?email=$1&exact=true" | python3 -c '
import json,sys
u = json.load(sys.stdin); print(u[0]["id"] if u else "")'
}

case "${1:-list}" in
  list)
    echo
    echo "WAITING FOR APPROVAL — realm $REALM"
    echo
    api "$B/groups/$PENDING/members?max=500" | python3 -c '
import json,sys,datetime
m = json.load(sys.stdin)
if not m:
    print("  (nobody is waiting)")
else:
    print("  %-38s %-22s %s" % ("EMAIL", "NAME", "REGISTERED"))
    for u in sorted(m, key=lambda x: x.get("createdTimestamp") or 0):
        ts = u.get("createdTimestamp")
        when = datetime.datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M") if ts else "-"
        name = ((u.get("firstName") or "") + " " + (u.get("lastName") or "")).strip() or "-"
        print("  %-38s %-22s %s" % (u.get("email") or u.get("username"), name[:21], when))
    print()
    print("  %d waiting.  Approve with: %s --approve <email>" % (len(m), sys.argv[0] if len(sys.argv)>1 else "this script"))
'
    echo
    ;;
  --approve|--revoke)
    EMAIL="${2:?an email address is required}"
    UID_="$(user_id "$EMAIL")"
    [ -n "$UID_" ] || die "no user with email $EMAIL in realm $REALM"
    if [ "$1" = "--approve" ]; then
      api -X PUT "$B/users/$UID_/groups/$APPROVED" >/dev/null
      api -X DELETE "$B/users/$UID_/groups/$PENDING" >/dev/null
      echo "APPROVED $EMAIL — the board appears for them within one token lifetime (60 s)."
    else
      api -X DELETE "$B/users/$UID_/groups/$APPROVED" >/dev/null
      api -X PUT "$B/users/$UID_/groups/$PENDING" >/dev/null
      # Group removal stops FUTURE tokens; the one in their browser stays valid for its full life.
      api -X POST "$B/users/$UID_/logout" >/dev/null
      echo "REVOKED $EMAIL — sessions ended, so it takes effect immediately rather than in 60 s."
    fi
    ;;
  *) die "usage: $0 [list | --approve <email> | --revoke <email>]" ;;
esac
