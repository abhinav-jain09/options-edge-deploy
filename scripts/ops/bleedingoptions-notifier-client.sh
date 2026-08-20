#!/usr/bin/env bash
# CREATE THE `approval-notifier` KEYCLOAK CLIENT — the one the email service authenticates as.
#
#   scripts/ops/bleedingoptions-notifier-client.sh                 # create (or verify) it
#   scripts/ops/bleedingoptions-notifier-client.sh --print-secret  # ...and print the secret once
#
# WHY THIS IS A SCRIPT AND NOT THE REALM RECONCILER. The reconciler manages exactly ONE client —
# it reads `.clients[0]` from the manifest and says so ("must be created by hand once") — so adding
# a second client to that file would be silently ignored, which is worse than not adding it. This
# is the "by hand" step, written down so it is reproducible and reviewable rather than a memory of
# which boxes were ticked in the console.
#
# WHY view-users AND NOTHING ELSE. The email service asks one question: who is in
# /gamma-lab-approved. It never writes a user, never reads a credential, never impersonates. The
# realm reconciler is deliberately denied every user-reading role for the same kind of reason, so
# reusing ITS credential here would have quietly granted what that decision withholds — this is a
# separate identity precisely so the two grants stay separate.
set -euo pipefail

KC="${KC_URL:-http://192.168.100.252:8189}"
REALM="${KC_REALM:-bleedingoptions}"
KC_ADMIN_USER="${KC_ADMIN_USER:-admin}"
CLIENT_ID="approval-notifier"
PRINT_SECRET=0
[[ "${1:-}" == "--print-secret" ]] && PRINT_SECRET=1

die() { echo "FAIL: $*" >&2; exit 1; }
note() { echo "==> $*"; }

command -v jq >/dev/null || die "jq is required"

# PROMPTED, never an argument or a file: it must not land in shell history or in `ps`.
if [ -z "${KC_ADMIN_PASSWORD:-}" ]; then
  printf 'Keycloak admin password for %s (not echoed): ' "$KC_ADMIN_USER"
  read -rs KC_ADMIN_PASSWORD; printf '\n'
fi

TOKEN="$(printf '%s' "$KC_ADMIN_PASSWORD" | curl -s -X POST \
  "$KC/realms/master/protocol/openid-connect/token" \
  -d client_id=admin-cli -d grant_type=password -d "username=$KC_ADMIN_USER" \
  --data-urlencode password@- | jq -r '.access_token // empty')"
[[ -n "$TOKEN" ]] || die "could not authenticate as $KC_ADMIN_USER"

api() { # METHOD PATH [BODY]
  local m="$1" p="$2"; shift 2
  curl -sS -X "$m" "$KC/admin/realms/$p" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"
}

# --- the client -----------------------------------------------------------------------------
EXISTING="$(api GET "$REALM/clients?clientId=$CLIENT_ID" | jq -r '.[0].id // empty')"
if [[ -n "$EXISTING" ]]; then
  note "client $CLIENT_ID already exists ($EXISTING) — verifying, not recreating"
  UID_="$EXISTING"
else
  note "creating client $CLIENT_ID"
  # standardFlowEnabled=false: nobody logs IN through this client, it only presents its own
  # credential. directAccessGrantsEnabled=false: it must never be able to exchange a username and
  # password for a token. Both off is what makes a leaked secret useful for exactly one thing.
  api POST "$REALM/clients" -d "$(jq -nc --arg c "$CLIENT_ID" '{
      clientId: $c,
      name: "Approval notifier (email service)",
      description: "Reads members of gamma-lab-approved so approved users can be told. view-users only.",
      enabled: true,
      publicClient: false,
      serviceAccountsEnabled: true,
      standardFlowEnabled: false,
      directAccessGrantsEnabled: false,
      implicitFlowEnabled: false
    }')" >/dev/null || die "client creation failed"
  UID_="$(api GET "$REALM/clients?clientId=$CLIENT_ID" | jq -r '.[0].id // empty')"
  [[ -n "$UID_" ]] || die "client was created but cannot be read back"
fi

# --- exactly one role -----------------------------------------------------------------------
SA_ID="$(api GET "$REALM/clients/$UID_/service-account-user" | jq -r '.id // empty')"
[[ -n "$SA_ID" ]] || die "no service-account user — serviceAccountsEnabled did not take"

RM_ID="$(api GET "$REALM/clients?clientId=realm-management" | jq -r '.[0].id // empty')"
[[ -n "$RM_ID" ]] || die "realm-management client not found"

ROLE="$(api GET "$REALM/clients/$RM_ID/roles/view-users")"
jq -e '.id' <<<"$ROLE" >/dev/null || die "view-users role not found on realm-management"

note "granting view-users"
api POST "$REALM/users/$SA_ID/role-mappings/clients/$RM_ID" \
  -d "$(jq -c '[{id: .id, name: .name}]' <<<"$ROLE")" >/dev/null || die "role grant failed"

# --- VERIFY THE NEGATIVE --------------------------------------------------------------------
# The grant succeeding proves view-users is present. It does NOT prove nothing else is, and an
# over-privileged service account is exactly the failure this whole design is arranged to avoid —
# so the check is on what is ABSENT, not on what was just added.
GRANTED="$(api GET "$REALM/users/$SA_ID/role-mappings/clients/$RM_ID" | jq -r '.[].name' | sort)"
UNEXPECTED="$(comm -23 <(printf '%s\n' "$GRANTED") <(echo view-users))"
if [[ -n "$UNEXPECTED" ]]; then
  echo "FAIL: $CLIENT_ID holds roles beyond view-users:" >&2
  printf '  %s\n' $UNEXPECTED >&2
  die "remove them before using this client — least privilege is the point of it existing"
fi
note "roles on $CLIENT_ID: $GRANTED  (nothing else — verified)"

# --- the secret -----------------------------------------------------------------------------
if [[ "$PRINT_SECRET" == 1 ]]; then
  SECRET="$(api GET "$REALM/clients/$UID_/client-secret" | jq -r '.value // empty')"
  [[ -n "$SECRET" ]] || die "could not read the client secret"
  echo
  echo "client secret (paste into the Jenkins credential 'bo-email-notifier-client-secret'):"
  echo "  $SECRET"
  echo
  echo "It is now in this terminal's scrollback. Clear it when you are done."
else
  echo
  echo "Secret NOT printed. Re-run with --print-secret, or read it from"
  echo "  $KC  ->  Clients  ->  $CLIENT_ID  ->  Credentials"
fi

cat <<'EOF_NEXT'

NEXT
  1. Jenkins credential store, two Secret text entries:
       bo-email-notifier-client-secret  = the value above
       bo-email-send-api-token          = openssl rand -base64 30
  2. Run bleedingoptions-secrets-deploy
       ENVIRONMENT=production, DEPLOY_DRY_RUN=false  ->  writes bo-email-secrets
  3. Deploy the service: oe-email-service-deploy, then service-deploy
  4. Watch the FIRST log line. It must say the ledger was SEEDED and no mail was sent.
     If it does not, stop -- see oe-email-service/README.md.
EOF_NEXT
