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

COOKIES="$(mktemp)"; trap 'rm -f "$COOKIES"' EXIT
CRUMB="$(curl -fsS -u "$JUSER:$JPASS" -c "$COOKIES" \
  "$JENKINS_URL/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,\":\",//crumb)" 2>/dev/null)" \
  || fail "cannot authenticate to Jenkins at $JENKINS_URL"
echo "  Jenkins: reachable, authenticated"

# `jq`-free JSON probing keeps this script dependency-light; Jenkins' api/json is predictable enough.
jenkins_job_exists() { curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" "$JENKINS_URL/job/$1/api/json" >/dev/null 2>&1; }
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
PG_PASSWORD="$(gen)"
KC_ADMIN_PASSWORD="$(gen)"

# Values go over stdin via curl's @- form, never in argv. `ps` on this machine shows only the URL.
put_credential() {
  local id="$1" secret="$2" desc="$3"
  # Delete first so this is idempotent: Jenkins has no upsert for credentials, and a second run
  # should replace the value rather than fail with "already exists".
  curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" -H "$CRUMB" -X POST \
    "$JENKINS_URL/credentials/store/system/domain/_/credential/$id/doDelete" >/dev/null 2>&1 || true

  python3 -c '
import json,sys
print(json.dumps({"": "0", "credentials": {
  "scope": "GLOBAL", "id": sys.argv[1], "secret": sys.argv[2], "description": sys.argv[3],
  "$class": "org.jenkinsci.plugins.plaincredentials.impl.StringCredentialsImpl"}}))
' "$id" "$secret" "$desc" \
    | curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" -H "$CRUMB" -X POST \
        --data-urlencode json@- "$JENKINS_URL/credentials/store/system/domain/_/createCredentials" >/dev/null \
    || fail "could not store credential '$id'"
  echo "  stored: $id"
}

put_credential bo-keycloak-postgres-password "$PG_PASSWORD"       "bleedingoptions: Postgres + Keycloak JDBC (generated)"
put_credential bo-keycloak-admin-password    "$KC_ADMIN_PASSWORD" "bleedingoptions: Keycloak bootstrap admin (generated)"
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
  queue="$(curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" -H "$CRUMB" -X POST \
            "${args[@]}" -D - -o /dev/null \
            "$JENKINS_URL/job/$job/buildWithParameters" \
          | tr -d '\r' | awk 'tolower($1)=="location:"{print $2}')"
  [ -n "$queue" ] || fail "$job did not queue"

  local build="" i=0
  while [ -z "$build" ] && [ $i -lt 60 ]; do
    build="$(curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" "${queue}api/json" 2>/dev/null \
             | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("executable") or {}).get("number",""))' 2>/dev/null || true)"
    [ -n "$build" ] || { sleep 2; i=$((i+1)); }
  done
  [ -n "$build" ] || fail "$job never started (still queued after 2 minutes — is an agent online?)"
  echo "  build #$build: $JENKINS_URL/job/$job/$build/console"

  local result="" j=0
  while [ -z "$result" ] && [ $j -lt 900 ]; do
    result="$(curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" "$JENKINS_URL/job/$job/$build/api/json" 2>/dev/null \
              | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result") or "")' 2>/dev/null || true)"
    if [ -z "$result" ]; then
      # An input step parks the build without finishing it. Approve it when unattended.
      if [ "$UNATTENDED" = true ]; then
        curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" -H "$CRUMB" -X POST \
          "$JENKINS_URL/job/$job/$build/input/Proceed/proceedEmpty" >/dev/null 2>&1 || true
      fi
      sleep 2; j=$((j+1))
    fi
  done
  [ -n "$result" ] || fail "$job build #$build did not finish within 30 minutes"
  case $result in
    SUCCESS)  echo "  result: SUCCESS" ;;
    UNSTABLE) echo "  result: UNSTABLE (continuing — check the console above)" ;;
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
  curl -fsS -u "$JUSER:$JPASS" -b "$COOKIES" -g \
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
PF=$!; trap 'kill $PF 2>/dev/null; rm -f "$COOKIES"' EXIT
for i in $(seq 30); do curl -fsS "$KC/realms/master" >/dev/null 2>&1 && break; sleep 1; done

# Admin token. --data-urlencode keeps the password out of argv.
TOKEN="$(printf '%s' "$KC_ADMIN_PASSWORD" \
  | curl -fsS -X POST "$KC/realms/master/protocol/openid-connect/token" \
      -d client_id=admin-cli -d grant_type=password -d username=admin \
      --data-urlencode password@- \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')" \
  || fail "could not get a Keycloak admin token"

RECON_SECRET="$(python3 - "$KC" "$TOKEN" <<'PY'
import json,sys,urllib.request
kc, tok = sys.argv[1], sys.argv[2]
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

# Least privilege (PGL-031A1): only the realm-management roles the reconciler actually uses.
sa   = req("GET", f"/admin/realms/bleedingoptions/clients/{cid}/service-account-user")
mgmt = req("GET", "/admin/realms/bleedingoptions/clients?clientId=realm-management")[0]
want = {"realm-admin"}
roles = [r for r in req("GET", f"/admin/realms/bleedingoptions/clients/{mgmt['id']}/roles") if r["name"] in want]
if roles:
    req("POST", f"/admin/realms/bleedingoptions/users/{sa['id']}/role-mappings/clients/{mgmt['id']}", roles)

print(req("GET", f"/admin/realms/bleedingoptions/clients/{cid}/client-secret")["value"])
PY
)" || fail "could not create the realm-reconciler client"

put_credential bo-keycloak-reconciler-secret "$RECON_SECRET" "bleedingoptions: realm-reconciler client secret"
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
