#!/usr/bin/env bash
# WHO CAN SEE GAMMA LAB — the one answer, from Keycloak itself.
#
# Why this exists: approval is recorded by membership of /gamma-lab-approved, but the Keycloak
# console gives no at-a-glance answer to "is this person approved?" — and two things make the
# group lists misleading on their own:
#
#   1. /pending-approval is the DEFAULT group for every new registration and is NOT removed when
#      someone is approved. Four of the five live accounts sit in both groups, so "members of
#      /pending-approval" is not a queue of people waiting — it is nearly everyone.
#   2. Keycloak lets an admin assign the `gamma-lab` realm role DIRECTLY to a user, bypassing the
#      group. The realm reconciler says plainly that it cannot see this (PGL-021A: it is blind to
#      users on purpose, so it can never touch approval state) and that an admin must audit it.
#      Nothing did, until this script.
#
# So the report is built from BOTH sides and cross-checks them:
#   - group membership  (/gamma-lab-approved, /pending-approval)
#   - EFFECTIVE role    (GET users/{id}/role-mappings/realm/composite — includes group-derived)
#   - DIRECT role       (GET roles/gamma-lab/users — returns ONLY directly-assigned users, never
#                        the ones who hold it through a group. Reading that endpoint as "everyone
#                        with access" reports every correctly-approved user as broken; it is the
#                        violation check, not the access check.)
# A user holding the role WITHOUT the group is a policy violation and is called out as such: it is
# access that the approval record does not explain, and revoking the group would not remove it.
#
# READ-ONLY. It never writes. Approval and revocation stay manual, exactly as designed.
#
# Usage:
#   KC_ADMIN_PASSWORD=... scripts/ops/gamma-lab-access-report.sh
#   KC_URL=http://192.168.100.252:8189 KC_REALM=bleedingoptions KC_ADMIN_USER=admin \
#     KC_ADMIN_PASSWORD=... scripts/ops/gamma-lab-access-report.sh
set -euo pipefail

KC_URL="${KC_URL:-http://192.168.100.252:8189}"
KC_REALM="${KC_REALM:-bleedingoptions}"
KC_ADMIN_USER="${KC_ADMIN_USER:-admin}"
KC_ADMIN_PASSWORD="${KC_ADMIN_PASSWORD:-}"
APPROVED_GROUP="${APPROVED_GROUP:-gamma-lab-approved}"
PENDING_GROUP="${PENDING_GROUP:-pending-approval}"
ACCESS_ROLE="${ACCESS_ROLE:-gamma-lab}"

if [ -z "$KC_ADMIN_PASSWORD" ]; then
  echo "KC_ADMIN_PASSWORD is required (a REAL admin — the realm reconciler's service account is" >&2
  echo "deliberately blind to users and cannot produce this report)." >&2
  exit 2
fi

TOKEN="$(curl -sS --max-time 20 -X POST "$KC_URL/realms/master/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=admin-cli \
  --data-urlencode "username=$KC_ADMIN_USER" --data-urlencode "password=$KC_ADMIN_PASSWORD" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token",""))')"
[ -n "$TOKEN" ] || { echo "could not obtain an admin token for $KC_URL" >&2; exit 1; }

api() { curl -sS --max-time 30 -H "Authorization: Bearer $TOKEN" "$KC_URL/admin/realms/$KC_REALM/$1"; }

GROUPS_JSON="$(api groups)"
gid_of() {
  printf '%s' "$GROUPS_JSON" | python3 -c "
import json,sys
name=sys.argv[1]
for g in json.load(sys.stdin):
    if g['name']==name: print(g['id']); break
" "$1"
}
APPROVED_GID="$(gid_of "$APPROVED_GROUP")"
PENDING_GID="$(gid_of "$PENDING_GROUP")"
[ -n "$APPROVED_GID" ] || { echo "group '$APPROVED_GROUP' not found in realm $KC_REALM" >&2; exit 1; }

USERS="$(api 'users?briefRepresentation=true&max=1000')"
APPROVED_MEMBERS="$(api "groups/$APPROVED_GID/members?max=1000")"
PENDING_MEMBERS="$([ -n "$PENDING_GID" ] && api "groups/$PENDING_GID/members?max=1000" || echo '[]')"
DIRECT_HOLDERS="$(api "roles/$ACCESS_ROLE/users?max=1000" || echo '[]')"

# Effective role per user: the composite realm mapping is the only view that includes roles
# granted THROUGH a group, which is how every legitimate approval is granted here.
EFFECTIVE_IDS=""
for uid in $(printf '%s' "$USERS" | python3 -c 'import json,sys; [print(u["id"]) for u in json.load(sys.stdin)]'); do
  if api "users/$uid/role-mappings/realm/composite" \
      | python3 -c "import json,sys; sys.exit(0 if any(r.get('name')=='$ACCESS_ROLE' for r in json.load(sys.stdin)) else 1)"; then
    EFFECTIVE_IDS="$EFFECTIVE_IDS $uid"
  fi
done
EFFECTIVE_JSON="$(printf '%s' "$EFFECTIVE_IDS" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().split()))')"

python3 - "$APPROVED_GROUP" "$PENDING_GROUP" "$ACCESS_ROLE" <<PY
import json, sys
approved_group, pending_group, role = sys.argv[1], sys.argv[2], sys.argv[3]
users    = json.loads('''$USERS''')
approved = {u['id'] for u in json.loads('''$APPROVED_MEMBERS''')}
pending  = {u['id'] for u in json.loads('''$PENDING_MEMBERS''')}
holders  = set(json.loads('''$EFFECTIVE_JSON'''))          # effective: group-derived OR direct
direct   = {u['id'] for u in json.loads('''$DIRECT_HOLDERS''')}  # direct assignment = violation

def label(u):
    i = u['id']
    if i in approved and i in holders:  return 'APPROVED', ''
    if i in holders and i not in approved:
        how = 'directly assigned' if i in direct else 'via some other group'
        return 'ACCESS (NO GROUP)', 'holds %s %s — approval record does not explain it' % (role, how)
    if i in approved and i not in holders:
        return 'GROUP, NO ROLE', 'in %s but %s is NOT effective — group role mapping is broken' % (approved_group, role)
    if i in pending:                    return 'waiting', ''
    return 'no access', ''

rows = []
for u in sorted(users, key=lambda x: (x.get('username') or '')):
    st, note = label(u)
    rows.append((u.get('username') or u['id'], st, note,
                 'yes' if u['id'] in pending else '-', u.get('enabled', True)))

w = max([len(r[0]) for r in rows] + [8])
print()
print('GAMMA LAB ACCESS — realm %s' % '$KC_REALM')
print()
print('%-*s  %-18s %-9s %s' % (w, 'USER', 'STATUS', 'IN QUEUE', 'NOTE'))
print('-' * (w + 42))
for name, st, note, inq, enabled in rows:
    print('%-*s  %-18s %-9s %s' % (w, name, st + ('' if enabled else ' (disabled)'), inq, note))

n_app  = sum(1 for r in rows if r[1] == 'APPROVED')
n_bad  = sum(1 for r in rows if r[1] == 'ACCESS (NO GROUP)')
n_direct = len(direct)
n_gnr  = sum(1 for r in rows if r[1] == 'GROUP, NO ROLE')
n_wait = sum(1 for r in rows if r[1] == 'waiting')
n_none = sum(1 for r in rows if r[1] == 'no access')
stale  = sum(1 for r in rows if r[1] == 'APPROVED' and r[3] == 'yes')
print()
print('%d approved, %d waiting, %d with no access, %d total' % (n_app, n_wait, n_none, len(rows)))
if direct:
    print()
    print('!! %d user(s) have %s assigned DIRECTLY (not through a group). The realm states the role' % (n_direct, role))
    print('   is held only via /%s; a direct grant is invisible to the reconciler.' % approved_group)
if n_bad:
    print()
    print('!! %d user(s) hold %s WITHOUT %s. That is access the approval record does not' % (n_bad, role, approved_group))
    print('   explain, and removing the group would NOT revoke it. Remove the direct role assignment.')
if n_gnr:
    print()
    print('!! %d user(s) are in %s but the role is not effective — the group role mapping is broken.' % (n_gnr, approved_group))
if stale:
    print()
    print('note: %d approved user(s) are still in /%s. Approval does not remove the default' % (stale, pending_group))
    print('      group, so "members of /%s" is not a queue of people waiting.' % pending_group)
    print('      Removing it on approval would make both lists mean what they say.')
print()
PY
