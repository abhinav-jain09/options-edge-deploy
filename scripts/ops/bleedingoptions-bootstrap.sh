#!/usr/bin/env bash
# ONE COMMAND to bring the public tenant up from nothing.
#
#   scripts/ops/bleedingoptions-bootstrap.sh
#
# It generates the two passwords that do not exist yet, stores all three credentials in Jenkins,
# and then runs the Jenkins jobs in the order the runbook describes — including creating the realm
# reconciler client over Keycloak's admin API, which was previously a manual console step.
#
# WHAT IT DOES NOT DO
# -------------------
# It never applies anything to the cluster itself. Every cluster write goes through a Jenkins job,
# because that is the rule. This script only decides WHICH jobs run and in what order.
#
# THE ONE VALUE IT CANNOT INVENT is the Gmail app password for info@bleedingoptions.com, which
# belongs to a real Google account. Supply it once, either way:
#   KC_SMTP_PASSWORD='xxxx xxxx xxxx xxxx' scripts/ops/bleedingoptions-bootstrap.sh
# or let it prompt (input is not echoed and does not reach your shell history).
#
# The two generated passwords are never printed. They go straight into the Jenkins credential store,
# which is where every job reads them from. If you ever need them, read them from Jenkins.
set -euo pipefail

NS=bleedingoptions
# TWO clusters are in play and they are not the same one. Jenkins runs in docker-desktop on this Mac;
# the public tenant runs on the production k3s node at 192.168.100.252. Reading the Jenkins admin
# password from the wrong one, or waiting for bo-keycloak on the wrong one, both fail confusingly —
# the first attempt at this applied the tenant's RBAC to docker-desktop and the deploy stayed
# Forbidden, because the grant was on a cluster the job never touches.
KCTX_JENKINS="${KCTX_JENKINS:-docker-desktop}"          # where Jenkins itself runs
KPROD="${KPROD:-$HOME/.kube/prod-k3s.yaml}"             # where bleedingoptions runs
kprod() { kubectl --kubeconfig="$KPROD" "$@"; }
JENKINS_URL="${JENKINS_URL:-http://localhost:8085}"
JUSER="${JENKINS_USER:-admin}"
UNATTENDED="${UNATTENDED:-true}"   # auto-approve the secret job's input gate; set false to click it yourself

say()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFATAL: %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- preflight
say "Preflight"
command -v kubectl >/dev/null || fail "kubectl not on PATH"
command -v curl    >/dev/null || fail "curl not on PATH"

JPASS="${JENKINS_PASS:-$(kubectl --context="$KCTX_JENKINS" get secret -n jenkins jenkins -o jsonpath='{.data.jenkins-admin-password}' 2>/dev/null | base64 -d)}"
[ -n "$JPASS" ] || fail "no Jenkins password (set JENKINS_PASS, or ensure kubectl can read secret/jenkins in ns jenkins)"

# One trapped dir for the cookie jar and the netrc. Jenkins basic-auth via --netrc-file keeps the
# admin password out of curl's argv, where `ps` would show it on every one of the dozens of calls.
umask 077
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
COOKIES="$WORK/cookies"
NETRC="$WORK/netrc"
printf 'machine %s login %s password %s\n' "$(printf '%s' "$JENKINS_URL" | sed -E 's#https?://##; s#:.*##')" "$JUSER" "$JPASS" > "$NETRC"
J=(curl --netrc-file "$NETRC" -b "$COOKIES")
CRUMB="$("${J[@]}" -fsS -c "$COOKIES" \
  "$JENKINS_URL/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,\":\",//crumb)" 2>/dev/null)" \
  || fail "cannot authenticate to Jenkins at $JENKINS_URL"
echo "  Jenkins: reachable, authenticated"

# `jq`-free JSON probing keeps this script dependency-light; Jenkins' api/json is predictable enough.
jenkins_job_exists() { "${J[@]}" -fsS "$JENKINS_URL/job/$1/api/json" >/dev/null 2>&1; }
for job in common-infra-deploy bleedingoptions-secrets-deploy; do
  jenkins_job_exists "$job" || fail "Jenkins job '$job' does not exist. Register it before running this."
  echo "  job: $job"
done

[ -r "$KPROD" ] || fail "production kubeconfig not readable at $KPROD (set KPROD)"
kprod version --request-timeout=5s >/dev/null 2>&1 || fail "cannot reach the production cluster via $KPROD"
echo "  cluster: reachable ($(kprod config view --minify -o jsonpath='{.clusters[0].cluster.server}'))"

# The tenant bootstrap RBAC is a cluster-admin step that no Jenkins job can perform: the deployer
# cannot grant itself rights in a namespace it cannot see. Fail here with the fix rather than let a
# build fail with a wall of Forbidden.
kprod auth can-i create deployments -n "$NS" \
  --as=system:serviceaccount:options-edge:jenkins-deployer >/dev/null 2>&1 \
  && [ "$(kprod auth can-i create resourcequotas -n "$NS" --as=system:serviceaccount:options-edge:jenkins-deployer 2>/dev/null)" = yes ] \
  || fail "the Jenkins deployer has no rights in $NS. Apply the tenant bootstrap first, as cluster-admin:
    kubectl --kubeconfig=$KPROD create namespace $NS
    kubectl --kubeconfig=$KPROD apply -f k8s/tenants/bleedingoptions/bootstrap/
  See k8s/tenants/bleedingoptions/bootstrap/README.md for why this cannot be a Jenkins job."
echo "  deployer RBAC: present"

# ---------------------------------------------------------------- credentials
say "Credentials"

# The SMTP password is the only one this script cannot invent.
if [ -z "${KC_SMTP_PASSWORD:-}" ]; then
  printf 'Gmail app password for info@bleedingoptions.com (not echoed): '
  read -rs KC_SMTP_PASSWORD; printf '\n'
fi
[ -n "$KC_SMTP_PASSWORD" ] || fail "the SMTP password is empty"
case $KC_SMTP_PASSWORD in *"
"*) fail "the SMTP password contains a newline" ;; esac

# Generated here, seen by nobody. -base64 30 gives ~40 chars from 240 bits; tr strips the characters
# that make shell quoting and JDBC URLs unpleasant rather than reducing entropy meaningfully.
gen() { openssl rand -base64 30 | tr -d '\n/+=' ; }

cred_exists() { "${J[@]}" -fsS "$JENKINS_URL/credentials/store/system/domain/_/credential/$1/api/json" >/dev/null 2>&1; }

# NEVER regenerate POSTGRES_PASSWORD on a rerun. `POSTGRES_PASSWORD` initialises the database role
# only on an EMPTY data directory: on an existing volume Postgres keeps the password it already has,
# so a fresh value changes only what Keycloak PRESENTS, and Keycloak then fails authentication and
# CrashLoops. A script advertised as rerunnable that silently breaks the database on its second run
# is worse than one that refuses to run twice. Rotation is a deliberate, documented procedure
# (docs/bleedingoptions-keycloak.md 1a), not a side effect of re-running bootstrap.
REUSED=0
if cred_exists bo-keycloak-postgres-password; then
  echo "  bo-keycloak-postgres-password: already exists — KEEPING it (regenerating would break the existing database)"
  REUSED=1
else
  PG_PASSWORD="$(gen)"
fi
if cred_exists bo-keycloak-admin-password; then
  echo "  bo-keycloak-admin-password: already exists — keeping it"
else
  KC_ADMIN_PASSWORD="$(gen)"
fi

# Credential writes. Three properties this must have, each learned the hard way:
#   * the value never reaches argv — `python3 -c prog "$SECRET"` publishes it to the process list;
#   * no delete-then-create — a failure between the two leaves NO credential, and the jobs that bind
#     it then fail on a missing binding rather than a stale value;
#   * `$url` is chosen before use, not referenced into existence (an earlier edit left it unset, and
#     under `set -u` every write aborted AFTER the delete had already happened).
put_credential() {
  local id="$1" secret="$2" desc="$3"

  if cred_exists "$id"; then
    # UPDATE via config.xml. `updateSubmit` expects a full Stapler form and returns 500 for the same
    # JSON that createCredentials accepts (verified against this Jenkins), and delete-then-create
    # would leave no credential at all if anything failed in between.
    OE_ID="$id" OE_SECRET="$secret" OE_DESC="$desc" python3 -c '
import os
from xml.sax.saxutils import escape
print("<org.jenkinsci.plugins.plaincredentials.impl.StringCredentialsImpl>"
      "<scope>GLOBAL</scope><id>%s</id><description>%s</description><secret>%s</secret>"
      "</org.jenkinsci.plugins.plaincredentials.impl.StringCredentialsImpl>"
      % (escape(os.environ["OE_ID"]), escape(os.environ["OE_DESC"]), escape(os.environ["OE_SECRET"])))
' \
      | "${J[@]}" -fsS -H "$CRUMB" -H 'Content-Type: application/xml' -X POST \
          --data-binary @- \
          "$JENKINS_URL/credentials/store/system/domain/_/credential/$id/config.xml" >/dev/null \
      || fail "could not update credential '$id'"
    echo "  updated: $id"
  else
    OE_ID="$id" OE_SECRET="$secret" OE_DESC="$desc" python3 -c '
import json, os
print(json.dumps({"": "0", "credentials": {
  "scope": "GLOBAL", "id": os.environ["OE_ID"], "secret": os.environ["OE_SECRET"],
  "description": os.environ["OE_DESC"],
  "$class": "org.jenkinsci.plugins.plaincredentials.impl.StringCredentialsImpl"}}))
' \
      | "${J[@]}" -fsS -H "$CRUMB" -X POST \
          --data-urlencode json@- \
          "$JENKINS_URL/credentials/store/system/domain/_/createCredentials" >/dev/null \
      || fail "could not create credential '$id'"
    echo "  stored: $id"
  fi
}

[ -n "${PG_PASSWORD:-}" ] \
  && put_credential bo-keycloak-postgres-password "$PG_PASSWORD" "bleedingoptions: Postgres + Keycloak JDBC (generated)"
[ -n "${KC_ADMIN_PASSWORD:-}" ] \
  && put_credential bo-keycloak-admin-password "$KC_ADMIN_PASSWORD" "bleedingoptions: Keycloak bootstrap admin (generated)"
put_credential bo-keycloak-smtp-password     "$KC_SMTP_PASSWORD"  "bleedingoptions: Gmail app password for info@bleedingoptions.com"

# ---------------------------------------------------------------- job running
# Jenkins' queue API is the only reliable way to follow a parameterised build: the POST returns a
# queue item, which later gains an executable with the real build number.
run_job() {
  local job="$1"; shift
  local args=() p
  for p in "$@"; do args+=(--data-urlencode "$p"); done

  say "Jenkins: $job ($*)"
  local queue
  queue="$("${J[@]}" -fsS -H "$CRUMB" -X POST \
            "${args[@]}" -D - -o /dev/null \
            "$JENKINS_URL/job/$job/buildWithParameters" \
          | tr -d '\r' | awk 'tolower($1)=="location:"{print $2}')"
  [ -n "$queue" ] || fail "$job did not queue"

  local build="" i=0
  while [ -z "$build" ] && [ $i -lt 60 ]; do
    build="$("${J[@]}" -fsS "${queue}api/json" 2>/dev/null \
             | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("executable") or {}).get("number",""))' 2>/dev/null || true)"
    [ -n "$build" ] || { sleep 2; i=$((i+1)); }
  done
  [ -n "$build" ] || fail "$job never started (still queued after 2 minutes — is an agent online?)"
  echo "  build #$build: $JENKINS_URL/job/$job/$build/console"

  local result="" j=0
  while [ -z "$result" ] && [ $j -lt 900 ]; do
    result="$("${J[@]}" -fsS "$JENKINS_URL/job/$job/$build/api/json" 2>/dev/null \
              | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result") or "")' 2>/dev/null || true)"
    if [ -z "$result" ]; then
      # An input step parks the build without finishing it, and BOTH jobs have one. The input id is
      # a content hash, not a stable name — an earlier version guessed `Proceed` and silently never
      # approved anything, leaving the build parked until the 30-minute timeout. Ask Jenkins for the
      # pending action and post to the URL it hands back.
      if [ "$UNATTENDED" = true ]; then
        local purl
        purl="$("${J[@]}" -fsS \
                 "$JENKINS_URL/job/$job/$build/wfapi/nextPendingInputAction" 2>/dev/null \
               | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("id",""))
except Exception: print("")' 2>/dev/null || true)"
        if [ -n "$purl" ]; then
          # proceedEmpty, NOT the wfapi proceedUrl the same response advertises: posting to
          # inputSubmit without its expected form body returns 200 and does nothing, so the build
          # stays parked while this loop cheerfully "approves" it every two seconds.
          echo "  approving input $purl"
          "${J[@]}" -fsS -H "$CRUMB" -X POST \
            "$JENKINS_URL/job/$job/$build/input/$purl/proceedEmpty" >/dev/null 2>&1 || true
        fi
      fi
      sleep 2; j=$((j+1))
    fi
  done
  [ -n "$result" ] || fail "$job build #$build did not finish within 30 minutes"
  case $result in
    SUCCESS)  echo "  result: SUCCESS" ;;
    UNSTABLE)
      # Exactly ONE unstable outcome is expected and safe: common-infra marks itself unstable when
      # RECONCILE_REALM=false, to say out loud that the realm was not reconciled. Anything else
      # unstable is a real signal and must not be walked past on the way to further mutations.
      if [ "$job" = common-infra-deploy ] && printf '%s\n' "$@" | grep -qx 'RECONCILE_REALM=false'; then
        echo "  result: UNSTABLE — expected here (RECONCILE_REALM=false says the realm was skipped)"
      else
        fail "$job build #$build finished UNSTABLE, which is not an expected outcome for this step — see $JENKINS_URL/job/$job/$build/console"
      fi
      ;;
    *)        fail "$job build #$build finished $result — see $JENKINS_URL/job/$job/$build/console" ;;
  esac
}

# ---------------------------------------------------------------- the sequence
# 0. Prime the parameter definitions. Jenkins caches a job's parameters from the LAST build, not from
#    the Jenkinsfile on disk, so a job whose Jenkinsfile gained a parameter still advertises the old
#    set until something builds it. common-infra-deploy currently does not know RECONCILE_REALM
#    exists — and Jenkins DROPS parameters a job does not declare. Submitting RECONCILE_REALM=false
#    now would silently fall back to the Jenkinsfile default of true, which fails on first boot
#    because the reconciler client does not exist yet. A dry-run build costs a minute and makes the
#    real run's parameters actually take effect.
say "Priming Jenkins parameter definitions (dry run — writes nothing)"
run_job common-infra-deploy ENVIRONMENT=production DEPLOY_DRY_RUN=true

have_param() {
  "${J[@]}" -fsS -g \
    "$JENKINS_URL/job/$1/api/json?tree=property%5BparameterDefinitions%5Bname%5D%5D" \
  | python3 -c 'import json,sys; print(any(d["name"]==sys.argv[1] for p in json.load(sys.stdin).get("property",[]) for d in p.get("parameterDefinitions",[])))' "$2"
}
[ "$(have_param common-infra-deploy RECONCILE_REALM)" = True ] \
  || fail "common-infra-deploy still does not advertise RECONCILE_REALM after a build. Submitting it would be silently dropped and the realm would be reconciled on first boot, which cannot work. Check that the job builds main and Jenkinsfile.common-infra."
echo "  RECONCILE_REALM is now a real parameter"

# 1. Infra first: it owns the namespace, and the Secret job refuses to create one.
#    RECONCILE_REALM=false because the reconciler client cannot exist before Keycloak's first boot.
run_job common-infra-deploy ENVIRONMENT=production DEPLOY_DRY_RUN=false RECONCILE_REALM=false

# 2. The Secret. Keycloak and Postgres cannot start without it, so it comes before waiting for them.
run_job bleedingoptions-secrets-deploy ENVIRONMENT=production DEPLOY_DRY_RUN=false

say "Waiting for Keycloak to become ready"
kprod -n "$NS" rollout status deploy/bo-keycloak --timeout=600s \
  || fail "bo-keycloak did not become ready — check: kubectl --kubeconfig=$KPROD -n $NS get pods"

# 3. The reconciler client, over the admin API. This was a manual console step; there is no reason
#    for it to be one, and a hand-clicked client is a client nobody can reproduce.
say "Creating the realm-reconciler client"
KC=http://127.0.0.1:18189
kprod -n "$NS" port-forward svc/bo-keycloak 18189:8080 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null; rm -rf "$WORK"' EXIT   # WORK, not just the cookie jar: it holds the netrc
for i in $(seq 30); do curl -fsS "$KC/realms/master" >/dev/null 2>&1 && break; sleep 1; done

# Admin token. --data-urlencode keeps the password out of argv.
# After the runbook's first-boot step the `admin` account is DISABLED, so a rerun cannot use it.
# Let the operator name the real administrator they created, and skip this whole section when the
# reconciler client already exists — which it will on every run after the first.
KC_ADMIN_USER="${KC_ADMIN_USER:-admin}"
if [ -n "${KC_ADMIN_PASSWORD:-}" ]; then :; else
  KC_ADMIN_PASSWORD="${KC_ADMIN_PASSWORD_OVERRIDE:-}"
fi

# On a rerun the credential already exists, so there is nothing to store — but the client's ROLES
# are still worth re-asserting, and skipping the whole section meant a drifted client (someone adding
# manage-users by hand) would never be caught. Only the credential write is conditional.
RECON_EXISTS=0
cred_exists bo-keycloak-reconciler-secret && RECON_EXISTS=1
if [ "$RECON_EXISTS" = 1 ] && [ -z "${KC_ADMIN_PASSWORD:-}${KC_ADMIN_PASSWORD_OVERRIDE:-}" ]; then
  echo "  realm-reconciler already provisioned; no admin credential this run, so roles were not re-checked"
else
[ -n "${KC_ADMIN_PASSWORD:-}" ] || fail "the bootstrap admin password is not available in this run (it is only generated on the FIRST run). If the reconciler client still needs creating, pass the administrator you created at first boot: KC_ADMIN_USER=<you> KC_ADMIN_PASSWORD_OVERRIDE=<password>"

TOKEN="$(printf '%s' "$KC_ADMIN_PASSWORD" \
  | curl -fsS -X POST "$KC/realms/master/protocol/openid-connect/token" \
      -d client_id=admin-cli -d grant_type=password -d username="$KC_ADMIN_USER" \
      --data-urlencode password@- \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')" \
  || fail "could not get a Keycloak admin token"

# KC and TOKEN go through the ENVIRONMENT, not argv — a bearer token in the process list is as good
# as the admin password for as long as it lives.
RECON_SECRET="$(OE_KC="$KC" OE_TOKEN="$TOKEN" python3 - <<'PY'
import json,os,sys,urllib.request
kc, tok = os.environ["OE_KC"], os.environ["OE_TOKEN"]
def req(method, path, body=None):
    r = urllib.request.Request(f"{kc}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        if e.code == 409: return None      # already exists — fine, we look it up below
        raise

req("POST", "/admin/realms/bleedingoptions/clients", {
    "clientId": "realm-reconciler", "protocol": "openid-connect",
    "publicClient": False, "serviceAccountsEnabled": True,
    "standardFlowEnabled": False, "directAccessGrantsEnabled": False,
    "description": "Authoritative realm reconciler (PGL-031A). Created by bleedingoptions-bootstrap.sh.",
})
found = [c for c in req("GET", "/admin/realms/bleedingoptions/clients?clientId=realm-reconciler")]
if not found: sys.exit("realm-reconciler client was not created")
cid = found[0]["id"]

# Least privilege (PGL-031A1). EXACTLY the six roles docs/bleedingoptions-keycloak.md section 3
# specifies — not `realm-admin`, which is the blanket role that doc explicitly forbids because it
# carries manage-users, view-users and impersonation. The reconciler's whole contract is that it
# never touches users: approval state lives in group membership, and a reconciler able to edit
# memberships could un-approve everyone.
sa   = req("GET", f"/admin/realms/bleedingoptions/clients/{cid}/service-account-user")
mgmt = req("GET", "/admin/realms/bleedingoptions/clients?clientId=realm-management")[0]
want = {"view-realm", "manage-realm", "view-clients", "manage-clients",
        "query-groups", "manage-authorization"}
forbidden = {"realm-admin", "manage-users", "view-users", "impersonation"}
available = req("GET", f"/admin/realms/bleedingoptions/clients/{mgmt['id']}/roles")
roles = [r for r in available if r["name"] in want]
missing = want - {r["name"] for r in roles}
if missing:
    sys.exit(f"realm-management is missing expected roles: {sorted(missing)}")
if roles:
    req("POST", f"/admin/realms/bleedingoptions/users/{sa['id']}/role-mappings/clients/{mgmt['id']}", roles)

# Assert the held set is EXACTLY the six, not merely "contains them and none of four named bad
# ones". A prior run, a hand edit, or a future default could leave some other realm-management role
# mapped, and an allowlist that only rejects names it thought of is not an allowlist. Extras are
# removed rather than reported, so the end state is the documented one either way.
held_roles = req("GET", f"/admin/realms/bleedingoptions/users/{sa['id']}/role-mappings/clients/{mgmt['id']}") or []
extra = [r for r in held_roles if r["name"] not in want]
if extra:
    req("DELETE", f"/admin/realms/bleedingoptions/users/{sa['id']}/role-mappings/clients/{mgmt['id']}",
        [{"id": r["id"], "name": r["name"]} for r in extra])
    held_roles = req("GET", f"/admin/realms/bleedingoptions/users/{sa['id']}/role-mappings/clients/{mgmt['id']}") or []

held = {r["name"] for r in held_roles}
if held != want:
    sys.exit(f"realm-reconciler roles are {sorted(held)}, expected exactly {sorted(want)}")

print(req("GET", f"/admin/realms/bleedingoptions/clients/{cid}/client-secret")["value"])
PY
)" || fail "could not create the realm-reconciler client"

if [ "$RECON_EXISTS" = 1 ]; then
  echo "  realm-reconciler already provisioned — roles re-asserted, credential left as is"
else
  put_credential bo-keycloak-reconciler-secret "$RECON_SECRET" "bleedingoptions: realm-reconciler client secret"
fi
fi
kill $PF 2>/dev/null || true

# 4. Now the reconciler exists, so the realm can be reconciled into its desired state.
run_job common-infra-deploy ENVIRONMENT=production DEPLOY_DRY_RUN=false RECONCILE_REALM=true

say "Done"
cat <<EOF
The public tenant is up. Keycloak answers on the LAN at http://192.168.100.252:8189.

Still yours to do, deliberately not automated:
  1. Retire the bootstrap admin (§2 of docs/bleedingoptions-keycloak.md). The generated admin
     password is in Jenkins as bo-keycloak-admin-password if you need it.
  2. Revoke the app password that is sitting in the git-tracked url.md and reissue a fresh one —
     this script stored whatever you gave it, but that file is still one 'git add -A' from GitHub.
  3. PGL-072 still gates the public web tier: it stays at replicas 0 until PGL-050/051/052 pass on
     the exact digest. Nothing here changes that, and nothing here exposed the page to the internet.
EOF
