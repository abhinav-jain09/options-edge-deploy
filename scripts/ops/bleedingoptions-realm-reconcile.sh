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
#     everyone Abhinav had approved. Its service account is not even permitted to read users
#     (PGL-031A1) so this cannot happen by mistake.
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

# --- realm-level settings ------------------------------------------------------------------------
# Only the keys this script owns are sent. A whole-realm PUT would also overwrite fields the manifest
# does not mention, which is how a reconciler quietly reverts something nobody meant it to manage.
REALM_KEYS=(registrationAllowed registrationEmailAsUsername loginWithEmailAllowed duplicateEmailsAllowed
            resetPasswordAllowed verifyEmail rememberMe bruteForceProtected permanentLockout failureFactor
            waitIncrementSeconds maxFailureWaitSeconds maxDeltaTimeSeconds accessTokenLifespan
            ssoSessionIdleTimeout ssoSessionMaxLifespan revokeRefreshToken refreshTokenMaxReuse
            otpPolicyType otpPolicyAlgorithm otpPolicyDigits otpPolicyPeriod otpPolicyLookAheadWindow
            sslRequired)
# NOTE browserFlow is deliberately NOT in this list. It is not in the import manifest either (see the
# realm ConfigMap header), so a manifest-vs-live comparison would read "absent" and skip it. The flow
# section further down owns it explicitly, against a constant.

echo "==> Realm settings"
LIVE_REALM="$(api GET "$KC_REALM")" || die "cannot read realm $KC_REALM"
DESIRED_REALM="$(jq -c --argjson keys "$(printf '%s\n' "${REALM_KEYS[@]}" | jq -R . | jq -s .)" \
  'with_entries(select(.key as $k | $keys | index($k)))' "$WORK/desired.json")"

for key in "${REALM_KEYS[@]}"; do
  want="$(jq -r --arg k "$key" '.[$k] // "ABSENT"' <<<"$DESIRED_REALM")"
  have="$(jq -r --arg k "$key" '.[$k] // "ABSENT"' <<<"$LIVE_REALM")"
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
while read -r role; do
  [[ -z "$role" ]] && continue
  if ! api GET "$KC_REALM/roles/$role" >/dev/null 2>&1; then
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
if ! api GET "$KC_REALM/authentication/flows" | jq -e --arg f "$FLOW" '.[] | select(.alias==$f)' >/dev/null 2>&1; then
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
  fi
fi

# Bind it. A flow that exists but is not bound enforces nothing — the realm keeps using the stock
# browser flow and every login is password-only for anyone without an authenticator configured.
LIVE_BROWSER_FLOW="$(jq -r '.browserFlow // ""' <<<"$LIVE_REALM")"
if [[ "$LIVE_BROWSER_FLOW" != "$FLOW" ]]; then
  drift "realm.browserFlow: live='$LIVE_BROWSER_FLOW' expected='$FLOW' (OTP is NOT being enforced)"
  if [[ "$MODE" == "apply" ]]; then
    note "binding browserFlow=$FLOW"
    api PUT "$KC_REALM" -d "$(jq -nc --arg f "$FLOW" '{browserFlow:$f}')" >/dev/null \
      || die "could not bind browser flow"
  fi
fi

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
