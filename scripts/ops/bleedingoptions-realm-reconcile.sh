#!/usr/bin/env bash
# bleedingoptions-realm-reconcile.sh — make the LIVE `bleedingoptions` realm match the manifest.
#
# WHY THIS EXISTS
# ---------------
# `--import-realm` creates a realm that does not exist and SKIPS one that does. So after the very
# first boot, k8s/bleedingoptions/keycloak-realm-configmap.yaml applies to nothing: editing it, and
# even rolling the pod, changes the live realm not at all. Without this script the realm's real
# configuration is whatever anyone last clicked in the admin console, and the file in git is a
# description of the past.
#
# AUTHORITATIVE, NOT ADD-ONLY (PGL-031A)
# --------------------------------------
# An add-only reconciler cannot remove an accidentally granted default role, an over-broad redirect
# URI, or a composite that leaks the approval role — and those are precisely the drifts that must be
# removed rather than reported. So for the fields it owns, this script SETS them to the manifest
# value and removes what is extra.
#
#   AUTHORITATIVE (this script owns; drift is corrected):
#     realm login/registration settings, brute-force settings, token and session lifespans,
#     OTP policy, required actions, the browser flow binding, realm roles, group->role mappings,
#     default groups, and the bleedingoptions-web client's redirect URIs, web origins, flags and
#     protocol mappers.
#
#   NEVER TOUCHED (operator data, not manifest data):
#     users, their group memberships, their credentials, and their sessions. Approval state is
#     recorded by group membership; a reconciler that "corrected" memberships would un-approve
#     everyone Abhinav had approved. Its service account can READ users (view-users — Keycloak 26
#     hides groups entirely from a service account without it, verified live 2026-08-22: GET
#     /groups returned [] under the original six-role set, which made this script half-apply and
#     die on a 409), but it holds no role that can WRITE them: no manage-users, no impersonation,
#     no realm-admin (PGL-031A1). The preflight below proves that from the token itself before a
#     single write.
#
# SERIALISATION: none here, deliberately. Concurrency is prevented by the Jenkins job's existing
# disableConcurrentBuilds() (Jenkinsfile.common-infra:28). An earlier design took a Kubernetes Lease,
# which was unsatisfiable under its own controls — the namespace grants no API-server egress and
# service-account tokens are not mounted. This script talks to Keycloak and nothing else.
#
# FAILS CLOSED: any step that cannot be completed aborts with a non-zero exit, so a deploy stops
# rather than proceeding with a half-reconciled identity provider.
#
# USAGE
#   bleedingoptions-realm-reconcile.sh --apply     # reconcile
#   bleedingoptions-realm-reconcile.sh --check     # report drift, change nothing, non-zero if any
#
# ENVIRONMENT
#   KC_URL              admin base URL             (default http://192.168.100.252:8189)
#   KC_REALM            realm to reconcile          (default bleedingoptions)
#   KC_ADMIN_CLIENT     reconciler client id        (default realm-reconciler)
#   KC_ADMIN_SECRET     reconciler client secret    (REQUIRED; from bo-keycloak-secrets)
#   KC_SMTP_PASSWORD    SMTP app password           (REQUIRED to set mail; from bo-keycloak-secrets)
#   REALM_FILE          manifest realm JSON         (default: extracted from the ConfigMap in this repo)
set -euo pipefail

KC_URL="${KC_URL:-http://192.168.100.252:8189}"
KC_REALM="${KC_REALM:-bleedingoptions}"
KC_ADMIN_CLIENT="${KC_ADMIN_CLIENT:-realm-reconciler}"
MODE=""

usage() { sed -n '1,50p' "$0"; exit 2; }

case "${1:-}" in
  --apply) MODE=apply ;;
  --check) MODE=check ;;
  *) usage ;;
esac

die() { echo "FAIL: $*" >&2; exit 1; }
note() { echo "  $*"; }

command -v jq  >/dev/null || die "jq is required"
command -v curl >/dev/null || die "curl is required"

: "${KC_ADMIN_SECRET:?KC_ADMIN_SECRET is required (reconciler client secret from bo-keycloak-secrets)}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIGMAP="$REPO_ROOT/k8s/bleedingoptions/keycloak-realm-configmap.yaml"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# The manifest is the realm JSON embedded in the ConfigMap. Extracting it here rather than keeping a
# second copy on disk means there is exactly one description of desired state — a second file would
# be one more thing to drift.
if [[ -n "${REALM_FILE:-}" ]]; then
  cp "$REALM_FILE" "$WORK/desired.json"
else
  [[ -f "$CONFIGMAP" ]] || die "cannot find $CONFIGMAP"
  # Strip the YAML block-scalar indentation from the `bleedingoptions-realm.json: |` key.
  # BLANK lines are part of the block and must be kept: an earlier version exited on the first one,
  # which silently truncated the realm to its first six lines and produced invalid JSON. Only a
  # non-blank, non-indented line ends the block.
  awk '/^  bleedingoptions-realm\.json: \|/{flag=1;next}
       flag && /^[[:space:]]*$/{print ""; next}
       flag && /^    /{sub(/^    /,""); print; next}
       flag{exit}' \
    "$CONFIGMAP" > "$WORK/desired.json"
fi
jq empty "$WORK/desired.json" 2>/dev/null || die "the extracted realm JSON is not valid JSON"

echo "==> Authenticating to $KC_URL as client '$KC_ADMIN_CLIENT'"
TOKEN="$(curl -fsS -X POST "$KC_URL/realms/$KC_REALM/protocol/openid-connect/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=$KC_ADMIN_CLIENT" \
  -d "client_secret=$KC_ADMIN_SECRET" 2>/dev/null | jq -r '.access_token // empty')" \
  || die "token request failed"
[[ -n "$TOKEN" ]] || die "no access token returned — check KC_ADMIN_CLIENT/KC_ADMIN_SECRET and that the reconciler client exists"

api() {
  local method="$1" path="$2"; shift 2
  curl -fsS -X "$method" "$KC_URL/admin/realms/$path" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" "$@"
}

DRIFT=0
drift() {
  DRIFT=1
  echo "DRIFT: $*"
}

# --- preflight: prove authorization and readability BEFORE any write -----------------------------
# Why this exists: the first real run (2026-08-22) authenticated fine, applied the realm settings,
# and then died on `POST /groups` -> 409 — because the service account could not SEE the groups it
# was told to create. A permission gap that surfaces mid-apply leaves a half-reconciled identity
# provider, and Keycloak has no transaction to roll that back. So every authorization fact and every
# managed resource is proven readable here, and the first write happens only after all of them pass.
#
# The role check reads the access token itself rather than calling an endpoint: the token's
# `resource_access.realm-management.roles` claim IS the authorization, so asserting it needs no
# extra permission and cannot mutate anything. It is asserted EXACTLY — not "contains what I need",
# and not "lacks three names I thought of" — because both weaker forms let a drifted grant ride
# along silently (PGL-031A1).
#
# The claim carries the EFFECTIVE set, with composites expanded: view-clients brings query-clients,
# view-users brings query-users (both verified against Keycloak 26.0.8, 2026-08-22). So the list
# below is the nine documented DIRECT roles plus those two children — eleven names. If a Keycloak
# upgrade changes the expansion, this check fails loudly and the list gets re-verified rather than
# the difference passing unnoticed: an unexpected name in an ADMIN token is never a detail.
echo "==> Preflight"
b64url_dec() {
  local p="${1//-/+}"; p="${p//_//}"
  while (( ${#p} % 4 )); do p+="="; done
  printf '%s' "$p" | base64 -d 2>/dev/null
}
CLAIMS="$(b64url_dec "$(cut -d. -f2 <<<"$TOKEN")")" \
  && jq -e 'type == "object"' <<<"$CLAIMS" >/dev/null 2>&1 \
  || die "could not decode the access token's claims"

WANT_ROLES="$(printf '%s\n' manage-authorization manage-clients manage-events manage-realm \
  query-clients query-groups query-users view-clients view-events view-realm view-users | sort)"
HELD_ROLES="$(jq -r '.resource_access["realm-management"].roles[]?' <<<"$CLAIMS" | sort)"
if [[ "$HELD_ROLES" != "$WANT_ROLES" ]]; then
  echo "token's effective realm-management roles: ${HELD_ROLES//$'\n'/ }" >&2
  echo "expected effective set:                   ${WANT_ROLES//$'\n'/ }" >&2
  for forbidden in manage-users impersonation realm-admin; do
    grep -qxF "$forbidden" <<<"$HELD_ROLES" \
      && die "the reconciler holds '$forbidden', which PGL-031A1 forbids — it must never be able to write users or approval memberships. Remove it (scripts/ops/bleedingoptions-bootstrap.sh re-asserts the exact set) before this script will run."
  done
  die "the reconciler's effective roles are not EXACTLY the documented set (docs/bleedingoptions-keycloak.md 3). With less than this, Keycloak 26 hides groups or the event config and an apply would fail half-way; with more, the least-privilege contract is broken. Fix the direct grants, then re-run."
fi
note "roles: exactly the documented set, none forbidden"

# Every resource a later section reads or writes, read NOW. Each `die` here happens before any
# write anywhere, so a failure in this block leaves the realm untouched.
api GET "$KC_REALM" >/dev/null                                  || die "preflight: cannot read realm $KC_REALM"
PREFLIGHT_GROUPS="$(api GET "$KC_REALM/groups")"                || die "preflight: cannot list groups"
jq -e 'type == "array"' <<<"$PREFLIGHT_GROUPS" >/dev/null 2>&1  || die "preflight: group list is not an array"
# view-users is what makes the group list REAL in Keycloak 26 — without it the same call returns []
# with HTTP 200 and every group looks MISSING. The role assertion above already proved view-users is
# held, so from here on an absent group can be trusted to actually be absent.
api GET "$KC_REALM/events/config" | jq -e 'type == "object"' >/dev/null 2>&1 \
                                                                || die "preflight: cannot read the event configuration (needs view-events)"
api GET "$KC_REALM/authentication/flows" >/dev/null             || die "preflight: cannot read authentication flows"
api GET "$KC_REALM/authentication/required-actions" >/dev/null  || die "preflight: cannot read required actions"
api GET "$KC_REALM/default-groups" >/dev/null                   || die "preflight: cannot read default groups"
# The direct-assignment audit's reads happen HERE too, cached for the audit section below — its
# section runs after the first writes, and a read that fails there is the half-apply path again.
# A role the manifest names but the realm lacks yet has NO direct holders by construction (it will
# be created by this very run, and a just-created role cannot have been hand-assigned), so a
# PROVEN 404 caches as an empty list rather than failing a fresh realm. Only a 404: a 403, a 500
# or a timeout is not absence, and guessing "absent" on those would send the realm-roles section
# into a POST that 409s mid-apply — the exact path this preflight exists to close.
api_code() {  # method path -> prints the HTTP status; the body is discarded
  local method="$1" path="$2"
  curl -sS -o /dev/null -w '%{http_code}' -X "$method" "$KC_URL/admin/realms/$path" \
    -H "Authorization: Bearer $TOKEN"
}
mkdir -p "$WORK/direct-holders" "$WORK/roles-present"
while read -r role; do
  [[ -z "$role" ]] && continue
  code="$(api_code GET "$KC_REALM/roles/$role")" \
    || die "preflight: could not query realm role '$role'"
  case "$code" in
    200)
      touch "$WORK/roles-present/$role"
      api GET "$KC_REALM/roles/$role/users" > "$WORK/direct-holders/$role.json" 2>/dev/null \
        || die "preflight: cannot list direct holders of realm role '$role'"
      jq -e 'type == "array"' "$WORK/direct-holders/$role.json" >/dev/null 2>&1 \
        || die "preflight: direct-holder list for '$role' is not an array"
      ;;
    404)
      printf '[]' > "$WORK/direct-holders/$role.json"
      ;;
    *)
      die "preflight: reading realm role '$role' returned HTTP $code — that is not 'absent', so refusing to proceed and guess"
      ;;
  esac
done < <(jq -r '.roles.realm[]?.name' "$WORK/desired.json")
note "all managed resources readable — no write can now fail on a permission it should have had"

# --- realm-level settings ------------------------------------------------------------------------
# Only the keys this script owns are sent. A whole-realm PUT would also overwrite fields the manifest
# does not mention, which is how a reconciler quietly reverts something nobody meant it to manage.
REALM_KEYS=(registrationAllowed registrationEmailAsUsername loginWithEmailAllowed duplicateEmailsAllowed
            resetPasswordAllowed verifyEmail rememberMe bruteForceProtected permanentLockout failureFactor
            waitIncrementSeconds maxFailureWaitSeconds maxDeltaTimeSeconds accessTokenLifespan
            ssoSessionIdleTimeout ssoSessionMaxLifespan revokeRefreshToken refreshTokenMaxReuse
            otpPolicyType otpPolicyAlgorithm otpPolicyDigits otpPolicyPeriod otpPolicyLookAheadWindow
            sslRequired)
# NOTE the event settings (PGL-062) are NOT in this list: Keycloak exposes them through a separate
# endpoint (/admin/realms/<realm>/events/config), not as realm attributes, so a realm PUT silently
# ignores them. They are reconciled explicitly below — an omission here would have left auditing off
# while every other setting reported clean.
# NOTE browserFlow is deliberately NOT in this list. It is not in the import manifest either (see the
# realm ConfigMap header), so a manifest-vs-live comparison would read "absent" and skip it. The flow
# section further down owns it explicitly, against a constant.

echo "==> Realm settings"
LIVE_REALM="$(api GET "$KC_REALM")" || die "cannot read realm $KC_REALM"
DESIRED_REALM="$(jq -c --argjson keys "$(printf '%s\n' "${REALM_KEYS[@]}" | jq -R . | jq -s .)" \
  'with_entries(select(.key as $k | $keys | index($k)))' "$WORK/desired.json")"

# `has($k)`, not `// "ABSENT"`: jq's // treats false (and null) as empty, so with // every
# false-valued manifest key read as ABSENT and was silently skipped — a manifest that says
# `duplicateEmailsAllowed: false` was never enforced, and a live `false` displayed as ABSENT.
for key in "${REALM_KEYS[@]}"; do
  want="$(jq -r --arg k "$key" 'if has($k) then (.[$k]|tostring) else "ABSENT" end' <<<"$DESIRED_REALM")"
  have="$(jq -r --arg k "$key" 'if has($k) then (.[$k]|tostring) else "ABSENT" end' <<<"$LIVE_REALM")"
  [[ "$want" == "ABSENT" ]] && continue
  if [[ "$want" != "$have" ]]; then
    drift "realm.$key: live='$have' manifest='$want'"
  fi
done

if [[ "$MODE" == "apply" && "$DRIFT" == "1" ]]; then
  note "applying realm settings"
  api PUT "$KC_REALM" -d "$DESIRED_REALM" >/dev/null || die "realm update failed"
fi

# --- SMTP password -------------------------------------------------------------------------------
# The manifest deliberately carries every SMTP field EXCEPT the password: a ConfigMap in git is not a
# place for a credential. The password is injected here from the Secret, so the live realm ends up
# complete while the repo never holds it.
if [[ "$MODE" == "apply" ]]; then
  if [[ -n "${KC_SMTP_PASSWORD:-}" ]]; then
    echo "==> SMTP"
    SMTP="$(jq -c --arg pw "$KC_SMTP_PASSWORD" '.smtpServer + {password: $pw}' "$WORK/desired.json")"
    api PUT "$KC_REALM" -d "{\"smtpServer\": $SMTP}" >/dev/null || die "smtp update failed"
    note "smtp configured for $(jq -r '.smtpServer.from' "$WORK/desired.json") (password from Secret, not from git)"
  else
    # Not fatal on a --check run, and not silently skipped either: mail underpins signup verification
    # and password reset, so an operator must know it was left unset.
    echo "WARN: KC_SMTP_PASSWORD unset — SMTP left as-is. Verification and password-reset mail will not send." >&2
  fi
fi

# --- realm roles ---------------------------------------------------------------------------------
echo "==> Realm roles"
# Existence comes from the preflight's PROVEN 200/404 verdict, not from a fresh probe: a probe here
# that failed with a 500 would read as "missing" and drive a POST that 409s mid-apply. The realm has
# no concurrent writer (the Jenkins job disables concurrent builds), so the verdict cannot go stale.
while read -r role; do
  [[ -z "$role" ]] && continue
  if [[ ! -e "$WORK/roles-present/$role" ]]; then
    drift "realm role '$role' is MISSING"
    if [[ "$MODE" == "apply" ]]; then
      note "creating role $role"
      desc="$(jq -r --arg r "$role" '.roles.realm[] | select(.name==$r) | .description // ""' "$WORK/desired.json")"
      api POST "$KC_REALM/roles" -d "$(jq -nc --arg n "$role" --arg d "$desc" '{name:$n,description:$d}')" >/dev/null \
        || die "could not create role $role"
    fi
  fi
done < <(jq -r '.roles.realm[]?.name' "$WORK/desired.json")

# --- groups and their role mappings --------------------------------------------------------------
# This is the approval mechanism itself: /gamma-lab-approved maps `gamma-lab` and /pending-approval
# maps nothing. An extra role on either group is a silent grant to every member, so extras are
# REMOVED rather than reported.
echo "==> Groups"
LIVE_GROUPS="$(api GET "$KC_REALM/groups")" || die "cannot list groups"

while read -r gname; do
  [[ -z "$gname" ]] && continue
  gid="$(jq -r --arg n "$gname" '.[] | select(.name==$n) | .id // empty' <<<"$LIVE_GROUPS")"
  if [[ -z "$gid" ]]; then
    drift "group '$gname' is MISSING"
    if [[ "$MODE" == "apply" ]]; then
      note "creating group $gname"
      api POST "$KC_REALM/groups" -d "$(jq -nc --arg n "$gname" '{name:$n}')" >/dev/null \
        || die "could not create group $gname"
      LIVE_GROUPS="$(api GET "$KC_REALM/groups")"
      gid="$(jq -r --arg n "$gname" '.[] | select(.name==$n) | .id' <<<"$LIVE_GROUPS")"
    else
      continue
    fi
  fi

  want_roles="$(jq -r --arg n "$gname" '.groups[] | select(.name==$n) | .realmRoles[]?' "$WORK/desired.json" | sort)"
  have_roles="$(api GET "$KC_REALM/groups/$gid/role-mappings/realm" | jq -r '.[].name' | sort)"

  # Missing -> add.
  while read -r r; do
    [[ -z "$r" ]] && continue
    if ! grep -qxF "$r" <<<"$have_roles"; then
      drift "group '$gname' is MISSING role '$r'"
      if [[ "$MODE" == "apply" ]]; then
        note "granting $r to $gname"
        rep="$(api GET "$KC_REALM/roles/$r")"
        api POST "$KC_REALM/groups/$gid/role-mappings/realm" -d "[$rep]" >/dev/null \
          || die "could not map $r to $gname"
      fi
    fi
  done <<<"$want_roles"

  # Extra -> REMOVE. This is the add-only trap the design called out: an accidental role here grants
  # data access to every member of the group, and reporting it without removing it leaves the grant live.
  while read -r r; do
    [[ -z "$r" ]] && continue
    if ! grep -qxF "$r" <<<"$want_roles"; then
      drift "group '$gname' has UNEXPECTED role '$r' (security-relaxing)"
      if [[ "$MODE" == "apply" ]]; then
        note "removing $r from $gname"
        rep="$(api GET "$KC_REALM/roles/$r")"
        api DELETE "$KC_REALM/groups/$gid/role-mappings/realm" -d "[$rep]" >/dev/null \
          || die "could not unmap $r from $gname"
      fi
    fi
  done <<<"$have_roles"
done < <(jq -r '.groups[]?.name' "$WORK/desired.json")

# --- default-role composites: THE approval bypass ------------------------------------------------
# Keycloak grants every user the realm's `default-roles-<realm>` composite automatically. If
# `gamma-lab` is ever added to that composite — by a mis-click, or deliberately — then EVERY
# self-registrant is approved the moment they register, and every check elsewhere in this script still
# passes: the groups are right, the default group is right, the role exists. Group membership would
# have stopped being the record of who is approved, and nothing would say so.
#
# This is checked explicitly, and any approval-bearing role found there is REMOVED, not reported.
echo "==> Default-role composites"
#
# FAILS CLOSED. An earlier version fell back to an empty composite list when the API call failed,
# which meant an unreachable or forbidden endpoint produced a GREEN result on the one check standing
# between "approved by an operator" and "approved by registering" — the precise false-green this
# script exists to prevent. A composite set that cannot be READ is not a composite set that is EMPTY.
DEFAULT_ROLE="default-roles-$KC_REALM"
DR_ROLE_JSON="$(api GET "$KC_REALM/roles/$DEFAULT_ROLE" 2>/dev/null || true)"
if [[ -n "$DR_ROLE_JSON" ]] && jq -e '.id' <<<"$DR_ROLE_JSON" >/dev/null 2>&1; then
  DR_ID="$(jq -r '.id' <<<"$DR_ROLE_JSON")"
  DR_COMPOSITES="$(api GET "$KC_REALM/roles-by-id/$DR_ID/composites" 2>/dev/null || true)"
  # An unreadable list is a FAILURE, never an empty one.
  jq -e 'type == "array"' <<<"${DR_COMPOSITES:-}" >/dev/null 2>&1 \
    || die "could not read the composites of $DEFAULT_ROLE. This is the check that proves no role grants approval to every registered user, so an unreadable answer is treated as a failure, not as 'no composites'. Confirm the reconciler client holds view-realm."
  while read -r guarded; do
    [[ -z "$guarded" ]] && continue
    if jq -e --arg r "$guarded" '.[] | select(.name==$r)' <<<"$DR_COMPOSITES" >/dev/null 2>&1; then
      drift "SECURITY: role '$guarded' is a composite of $DEFAULT_ROLE — EVERY registered user is approved"
      if [[ "$MODE" == "apply" ]]; then
        note "removing $guarded from $DEFAULT_ROLE"
        rep="$(api GET "$KC_REALM/roles/$guarded")"
        api DELETE "$KC_REALM/roles-by-id/$DR_ID/composites" -d "[$rep]" >/dev/null \
          || die "could not remove $guarded from $DEFAULT_ROLE"
      fi
    fi
  done < <(jq -r '.roles.realm[]?.name' "$WORK/desired.json")
else
  # Also fails closed. A realm could in principle name its default role differently, but "the role I
  # expected is not there" and "I could not check the bypass" are indistinguishable from here, and only
  # one of them is safe to assume.
  die "could not read $DEFAULT_ROLE. The default-role composite is how every registered user could be granted approval at once; leaving it unverified is not an option. If this realm names its default role differently, set it explicitly and re-run."
fi

# --- direct role assignment audit (PGL-021A) -----------------------------------------------------
# The realm documents that `gamma-lab` is held only via /gamma-lab-approved, but Keycloak lets an
# administrator assign a realm role straight to a user. This used to be "NOT CHECKED HERE" because
# the six-role contract could not read users; with view-users (which Keycloak 26 requires for group
# visibility anyway) the audit is possible, so it runs. `roles/<r>/users` returns DIRECT
# assignments only — group-derived holders do not appear — which is exactly the drift in question.
#
# Report-only, deliberately: correcting it would edit a user's role mappings, and this reconciler
# holds no role that can write users — by contract, not by accident. The drift still FAILS the
# build (PGL-031B), so it cannot be ignored; an operator removes the assignment in the console.
echo "==> Direct role assignment"
while read -r role; do
  [[ -z "$role" ]] && continue
  # Read from the preflight cache, not live: preflight proved these readable BEFORE any write, so
  # this section can no longer be the read that dies half-way through an apply. The snapshot is
  # seconds old and this script performs no user writes in between, so it cannot be self-stale.
  [[ -s "$WORK/direct-holders/$role.json" ]] \
    || die "no preflight snapshot of direct holders for '$role'. This audit is what backs the claim that group membership is the only record of approval, so a missing answer is a failure."
  n="$(jq 'length' "$WORK/direct-holders/$role.json")"
  if [[ "$n" != "0" ]]; then
    drift "SECURITY: realm role '$role' is assigned DIRECTLY to $n user(s) — approval must only be granted via group membership. Remove in the console; this reconciler cannot write users and will not."
  fi
done < <(jq -r '.roles.realm[]?.name' "$WORK/desired.json")

# --- default groups ------------------------------------------------------------------------------
# New registrations must land in /pending-approval. If this is wrong, either signups get no group at
# all (harmless but unreviewable) or — the dangerous case — they land somewhere that maps a role.
echo "==> Default groups"
want_default="$(jq -r '.defaultGroups[]?' "$WORK/desired.json" | sort)"
have_default="$(api GET "$KC_REALM/default-groups" | jq -r '.[].path' | sort)"
if [[ "$want_default" != "$have_default" ]]; then
  drift "default groups: live='${have_default//$'\n'/,}' manifest='${want_default//$'\n'/,}'"
  if [[ "$MODE" == "apply" ]]; then
    while read -r p; do
      [[ -z "$p" ]] && continue
      gid="$(jq -r --arg n "${p#/}" '.[] | select(.name==$n) | .id // empty' <<<"$(api GET "$KC_REALM/groups")")"
      [[ -n "$gid" ]] || die "cannot resolve default group $p"
      note "setting default group $p"
      api PUT "$KC_REALM/default-groups/$gid" >/dev/null || die "could not set default group $p"
    done <<<"$want_default"
    # Remove any default group the manifest does not name.
    while read -r p; do
      [[ -z "$p" ]] && continue
      grep -qxF "$p" <<<"$want_default" && continue
      gid="$(jq -r --arg n "${p#/}" '.[] | select(.name==$n) | .id // empty' <<<"$(api GET "$KC_REALM/groups")")"
      [[ -n "$gid" ]] || continue
      drift "removing unexpected default group $p"
      api DELETE "$KC_REALM/default-groups/$gid" >/dev/null || die "could not remove default group $p"
    done <<<"$have_default"
  fi
fi

# --- the REQUIRED-OTP browser flow (PGL-028) -----------------------------------------------------
# Keycloak's stock browser flow makes OTP CONDITIONAL on the user having configured it, which means an
# account with no authenticator signs in with a password alone. CONFIGURE_TOTP as a default required
# action closes that for new users, but not for an account whose OTP credential is later deleted — so
# the requirement is enforced in the flow, where it cannot be sidestepped.
#
# This lives here rather than in the realm import for a specific reason: Keycloak creates the BUILT-IN
# flows (registration, reset credentials, direct grant) only when the imported realm declares no
# authenticationFlows of its own. A custom flow in the import would therefore produce a realm with a
# hardened browser flow and NO REGISTRATION FLOW — on a realm whose whole purpose is self-registration.
#
# ⚠️ UNVERIFIED AGAINST A LIVE KEYCLOAK. The rest of this script uses admin endpoints whose shapes are
# stable and obvious; the subflow-creation call below is the one exception. Keycloak's
# `executions/flow` endpoint takes a `provider` field whose accepted value for a plain basic-flow
# subflow is a long-standing oddity in the admin API, and it cannot be confirmed without a running
# server. On the FIRST --apply, watch for a failure here and fall back to building the flow once by
# hand in the admin console (docs/bleedingoptions-keycloak.md §3a) — the binding check below is
# independent and will still tell you whether OTP is actually being enforced either way. Do not
# assume this worked because the script exited zero on a run where the flow already existed.
FLOW="bleedingoptions-browser"
SUBFLOW="bleedingoptions-browser-forms"

echo "==> Browser flow ($FLOW, OTP REQUIRED)"

# Does the flow actually ENFORCE what it claims? Presence of the alias is not the question — a run
# that created the top-level flow and then failed partway leaves an alias behind with no OTP step in
# it, and binding that produces a PASSWORD-ONLY login while every "flow exists" check passes.
#
# WHAT THIS PROVES, AND WHAT IT DOES NOT. It asserts that the flow contains a REQUIRED password form
# and a REQUIRED OTP form. That is enough to catch every partial-build case, which is what it is for.
# It is NOT a proof that the authentication graph as a whole demands both: nesting, ordering and
# ALTERNATIVE siblings could in principle still admit a path around them. Verifying the graph properly
# means attempting a real login and observing that a password alone is refused — a deployed proof
# (Gate 5), not something a config read can establish. Do not read a pass here as "MFA is proven".
flow_enforces_otp() {
  local execs
  execs="$(api GET "$KC_REALM/authentication/flows/$FLOW/executions" 2>/dev/null || echo '[]')"
  jq -e '
    (any(.[]; .providerId=="auth-username-password-form" and .requirement=="REQUIRED"))
    and
    (any(.[]; .providerId=="auth-otp-form" and .requirement=="REQUIRED"))
  ' <<<"$execs" >/dev/null 2>&1
}

FLOW_EXISTS=0
api GET "$KC_REALM/authentication/flows" | jq -e --arg f "$FLOW" '.[] | select(.alias==$f)' >/dev/null 2>&1 && FLOW_EXISTS=1

if [[ "$FLOW_EXISTS" == "1" ]] && ! flow_enforces_otp; then
  # The dangerous middle state: something is there, but it does not do the job.
  drift "SECURITY: flow '$FLOW' exists but does NOT contain a REQUIRED password form AND a REQUIRED OTP form — binding it would give password-only logins"
  if [[ "$MODE" == "apply" ]]; then
    die "refusing to repair a partially-built flow automatically: delete '$FLOW' in the admin console and re-run, or build it by hand (docs/bleedingoptions-keycloak.md 3a). Half-repairing an auth flow is how a login ends up neither working nor safe."
  fi
fi

if [[ "$FLOW_EXISTS" == "0" ]]; then
  drift "browser flow '$FLOW' is MISSING (login would fall back to the stock CONDITIONAL-OTP flow)"
  if [[ "$MODE" == "apply" ]]; then
    note "creating $FLOW"
    api POST "$KC_REALM/authentication/flows" \
      -d "$(jq -nc --arg a "$FLOW" '{alias:$a, providerId:"basic-flow", topLevel:true, builtIn:false,
            description:"Username/password THEN a TOTP code, both REQUIRED (PGL-028)."}')" >/dev/null \
      || die "could not create flow $FLOW"

    # A cookie execution first, so an existing SSO session is honoured rather than re-prompting.
    api POST "$KC_REALM/authentication/flows/$FLOW/executions/execution" \
      -d '{"provider":"auth-cookie"}' >/dev/null || die "could not add auth-cookie"

    api POST "$KC_REALM/authentication/flows/$FLOW/executions/flow" \
      -d "$(jq -nc --arg a "$SUBFLOW" '{alias:$a, type:"basic-flow", provider:"registration-page-form",
            description:"Password then TOTP."}')" >/dev/null || die "could not add subflow $SUBFLOW"

    api POST "$KC_REALM/authentication/flows/$SUBFLOW/executions/execution" \
      -d '{"provider":"auth-username-password-form"}' >/dev/null || die "could not add password form"
    api POST "$KC_REALM/authentication/flows/$SUBFLOW/executions/execution" \
      -d '{"provider":"auth-otp-form"}' >/dev/null || die "could not add otp form"

    # Requirements are set by PUTting the execution back with the requirement changed. auth-cookie is
    # ALTERNATIVE (an existing session short-circuits); the forms subflow is ALTERNATIVE at top level;
    # both executions INSIDE it are REQUIRED — which is the whole point.
    set_requirement() {
      local flow="$1" display="$2" req="$3"
      local exec_json
      exec_json="$(api GET "$KC_REALM/authentication/flows/$flow/executions" \
        | jq -c --arg d "$display" '.[] | select(.displayName==$d or .providerId==$d)')"
      [[ -n "$exec_json" ]] || die "cannot find execution '$display' in flow '$flow'"
      api PUT "$KC_REALM/authentication/flows/$flow/executions" \
        -d "$(jq -c --arg r "$req" '. + {requirement:$r}' <<<"$exec_json")" >/dev/null \
        || die "could not set '$display' to $req"
      note "$flow / $display -> $req"
    }
    set_requirement "$FLOW"    "auth-cookie"                 "ALTERNATIVE"
    set_requirement "$FLOW"    "$SUBFLOW"                    "ALTERNATIVE"
    set_requirement "$SUBFLOW" "auth-username-password-form" "REQUIRED"
    set_requirement "$SUBFLOW" "auth-otp-form"               "REQUIRED"

    # Prove what was just built before anything binds it. A flow assembled by a sequence of calls that
    # each returned 200 can still be wrong; this asks the server what it actually has.
    flow_enforces_otp \
      || die "built '$FLOW' but it does not enforce a REQUIRED password + REQUIRED OTP — refusing to bind it. Build it by hand (docs/bleedingoptions-keycloak.md 3a)."
    note "verified: $FLOW enforces REQUIRED password + REQUIRED OTP"
  fi
fi

# Bind it — but ONLY once it has been proven to enforce OTP. Binding is the step that makes the flow
# real for every login, so a flow that has not been verified must never reach it: that is how a
# password-only login ships while the deploy reports success.
LIVE_BROWSER_FLOW="$(jq -r '.browserFlow // ""' <<<"$LIVE_REALM")"
if [[ "$LIVE_BROWSER_FLOW" != "$FLOW" ]]; then
  drift "realm.browserFlow: live='$LIVE_BROWSER_FLOW' expected='$FLOW' (OTP is NOT being enforced)"
  if [[ "$MODE" == "apply" ]]; then
    flow_enforces_otp \
      || die "refusing to bind '$FLOW': it does not enforce a REQUIRED password + REQUIRED OTP"
    note "binding browserFlow=$FLOW"
    api PUT "$KC_REALM" -d "$(jq -nc --arg f "$FLOW" '{browserFlow:$f}')" >/dev/null \
      || die "could not bind browser flow"
  fi
elif ! flow_enforces_otp; then
  # Bound AND broken is the worst combination: every login is going through a flow that does not ask
  # for a second factor, and the binding check alone would have called that healthy.
  drift "SECURITY: '$FLOW' is BOUND but does not enforce OTP — logins are password-only right now"
fi

# --- required actions ----------------------------------------------------------------------------
# CONFIGURE_TOTP as a DEFAULT action is what makes every new account set up an authenticator. If it is
# disabled or stops being default, new users register with no second factor at all — and with the flow
# above requiring an OTP form they would be unable to log in, so this breaks signup rather than
# silently weakening it. Either way it must not drift.
echo "==> Required actions"
LIVE_ACTIONS="$(api GET "$KC_REALM/authentication/required-actions" 2>/dev/null || echo '[]')"
while read -r ra; do
  [[ -z "$ra" ]] && continue
  alias="$(jq -r '.alias' <<<"$ra")"
  want_enabled="$(jq -r '.enabled' <<<"$ra")"
  want_default="$(jq -r '.defaultAction' <<<"$ra")"
  live="$(jq -c --arg a "$alias" '.[] | select(.alias==$a)' <<<"$LIVE_ACTIONS")"
  if [[ -z "$live" ]]; then
    drift "required action '$alias' is MISSING"
    continue
  fi
  have_enabled="$(jq -r '.enabled' <<<"$live")"
  have_default="$(jq -r '.defaultAction' <<<"$live")"
  if [[ "$want_enabled" != "$have_enabled" || "$want_default" != "$have_default" ]]; then
    drift "required action '$alias': live enabled=$have_enabled default=$have_default; manifest enabled=$want_enabled default=$want_default"
    if [[ "$MODE" == "apply" ]]; then
      note "correcting required action $alias"
      api PUT "$KC_REALM/authentication/required-actions/$alias" \
        -d "$(jq -c --argjson e "$want_enabled" --argjson d "$want_default" '. + {enabled:$e, defaultAction:$d}' <<<"$live")" \
        >/dev/null || die "could not update required action $alias"
    fi
  fi
done < <(jq -c '.requiredActions[]?' "$WORK/desired.json")

# --- event auditing (PGL-062) --------------------------------------------------------------------
# Who registered, who was approved, who was de-approved, who changed the realm. On the component
# where those are the questions an incident actually asks, "we do not log that" is not an answer.
echo "==> Event auditing"
WANT_EVENTS="$(jq -c '{eventsEnabled, eventsExpiration, adminEventsEnabled, adminEventsDetailsEnabled}' \
  "$WORK/desired.json")"
LIVE_EVENTS="$(api GET "$KC_REALM/events/config" 2>/dev/null || true)"
if ! jq -e 'type == "object"' <<<"${LIVE_EVENTS:-}" >/dev/null 2>&1; then
  die "could not read the realm's event configuration. Auditing is a control, so an unreadable answer is a failure rather than an assumption that it is on."
fi
# Same has()-not-// rule as the realm settings above: a live `false` must compare as "false",
# not vanish into "ABSENT" and drift forever against a manifest `false`.
for key in eventsEnabled eventsExpiration adminEventsEnabled adminEventsDetailsEnabled; do
  want="$(jq -r --arg k "$key" '.[$k]' <<<"$WANT_EVENTS")"
  have="$(jq -r --arg k "$key" 'if has($k) then (.[$k]|tostring) else "ABSENT" end' <<<"$LIVE_EVENTS")"
  if [[ "$want" != "$have" ]]; then
    drift "events.$key: live='$have' manifest='$want'"
    if [[ "$MODE" == "apply" ]]; then
      note "correcting events.$key"
      api PUT "$KC_REALM/events/config" \
        -d "$(jq -c --argjson want "$WANT_EVENTS" '. * $want' <<<"$LIVE_EVENTS")" >/dev/null \
        || die "could not update the realm event configuration"
      LIVE_EVENTS="$(api GET "$KC_REALM/events/config")"
    fi
  fi
done

# --- master realm frontend URL (PGL-036B): NOT CHECKED HERE --------------------------------------
# The master realm's frontendUrl must stay pinned to the LAN address, but this reconciler cannot be
# the one to check it: its roles live in the BLEEDINGOPTIONS realm's realm-management client, which
# scopes its admin rights to that realm alone — GET /admin/realms/master returns 403 (verified live
# 2026-08-22). An earlier version of this script read and PUT master here, which could never have
# worked with this credential; granting the reconciler master-realm rights to make it work would
# hand the realm-scoped service account the superuser realm, which is exactly backwards.
# The check-and-set lives in scripts/ops/bleedingoptions-bootstrap.sh, which already authenticates
# as a master administrator and re-asserts the reconciler's roles on every rerun.
echo "==> Master realm frontendUrl"
note "NOT CHECKED HERE: this reconciler is scoped to the $KC_REALM realm and cannot read master."
note "bleedingoptions-bootstrap.sh asserts it (PGL-036B); manually:"
note "  GET /admin/realms/master -> .attributes.frontendUrl == http://192.168.100.252:8189  (as a master admin)"

# --- the public client ---------------------------------------------------------------------------
# Redirect URIs and web origins are the client's real security boundary: an extra entry is an open
# redirect for the authorization code, so extras are overwritten, not merged.
echo "==> Client"
CLIENT_ID="$(jq -r '.clients[0].clientId' "$WORK/desired.json")"
LIVE_CLIENT="$(api GET "$KC_REALM/clients?clientId=$CLIENT_ID" | jq -r '.[0] // empty')"
[[ -n "$LIVE_CLIENT" ]] || die "client $CLIENT_ID does not exist in the live realm — it is created by realm import on a rebuild, or must be created by hand once"

CUID="$(jq -r '.id' <<<"$LIVE_CLIENT")"
for field in redirectUris webOrigins publicClient standardFlowEnabled directAccessGrantsEnabled implicitFlowEnabled; do
  want="$(jq -c --arg f "$field" '.clients[0][$f]' "$WORK/desired.json")"
  have="$(jq -c --arg f "$field" '.[$f]' <<<"$LIVE_CLIENT")"
  if [[ "$want" != "$have" ]]; then
    drift "client.$field: live=$have manifest=$want"
  fi
done

if [[ "$MODE" == "apply" && "$DRIFT" == "1" ]]; then
  note "applying client settings"
  PATCH="$(jq -c '{redirectUris, webOrigins, publicClient, standardFlowEnabled, directAccessGrantsEnabled, implicitFlowEnabled, attributes} | .' <<<"$(jq -c '.clients[0]' "$WORK/desired.json")")"
  api PUT "$KC_REALM/clients/$CUID" -d "$PATCH" >/dev/null || die "client update failed"
fi

echo
if [[ "$DRIFT" == "0" ]]; then
  echo "OK: live realm matches the manifest."
  exit 0
fi

if [[ "$MODE" == "check" ]]; then
  # PGL-031B: a drift check that only reports is a check nobody acts on. Non-zero fails the pipeline.
  echo "FAIL: the live realm has drifted from the manifest (see DRIFT lines above)." >&2
  exit 1
fi

echo "APPLIED: drift corrected. Re-run with --check to confirm."
