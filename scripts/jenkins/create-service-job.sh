#!/usr/bin/env bash
# Create a Jenkins build job for a NEW OptionsEdge service by cloning an existing
# job's config (so it inherits the right git credentials + SCM setup), then
# pointing it at the new repo + Jenkinsfile. Poll trigger is stripped (manual job).
#
# Jenkins runs in docker-desktop on the Mac at http://localhost:8085.
#
# Usage:
#   scripts/jenkins/create-service-job.sh <new-job-name> <git-ssh-url> [scriptPath] [template-job]
# Example:
#   scripts/jenkins/create-service-job.sh options-edge-foo \
#     git@github.com:abhinav-jain09/options-edge-foo.git Jenkinsfile
#
# Env overrides: JENKINS_URL, JENKINS_USER, JENKINS_PASS, TEMPLATE_JOB
set -euo pipefail

JOB="${1:?new job name required}"
REPO="${2:?git ssh url required}"
SCRIPT_PATH="${3:-Jenkinsfile}"
TEMPLATE_JOB="${4:-${TEMPLATE_JOB:-options-edge-ibkr-feed}}"   # a working build job to clone SCM+creds from
JENKINS_URL="${JENKINS_URL:-http://localhost:8085}"
JUSER="${JENKINS_USER:-admin}"
# Default password: read from the docker-desktop jenkins secret.
JPASS="${JENKINS_PASS:-$(kubectl get secret -n jenkins jenkins -o jsonpath='{.data.jenkins-admin-password}' 2>/dev/null | base64 -d)}"
[ -n "$JPASS" ] || { echo "No Jenkins password (set JENKINS_PASS or have kubectl access to the jenkins secret)" >&2; exit 1; }

tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT

echo "Cloning config from template job '$TEMPLATE_JOB' ..."
curl -fsS -u "$JUSER:$JPASS" "$JENKINS_URL/job/$TEMPLATE_JOB/config.xml" -o "$tmp"

python3 - "$tmp" "$REPO" "$SCRIPT_PATH" <<'PY'
import sys, re
f, repo, script = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(f).read()
s = re.sub(r'<url>[^<]+</url>', f'<url>{repo}</url>', s, count=1)              # point at new repo
s = re.sub(r'<scriptPath>[^<]+</scriptPath>', f'<scriptPath>{script}</scriptPath>', s)
s = re.sub(r'<name>\*/[^<]+</name>', '<name>*/main</name>', s)                # build main
s = re.sub(r'<triggers>.*?</triggers>', '<triggers/>', s, flags=re.S)        # no SCM poll (manual)
open(f, 'w').write(s)
PY

echo "Creating job '$JOB' ..."
crumb="$(curl -fsS -u "$JUSER:$JPASS" "$JENKINS_URL/crumbIssuer/api/json" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["crumbRequestField"]+":"+d["crumb"])')"
code="$(curl -s -o /dev/null -w '%{http_code}' -u "$JUSER:$JPASS" -H "$crumb" \
  -H 'Content-Type: application/xml' --data-binary @"$tmp" "$JENKINS_URL/createItem?name=$JOB")"
[ "$code" = "200" ] || { echo "createItem failed (HTTP $code) — does the job already exist?" >&2; exit 1; }

echo "Created: $JENKINS_URL/job/$JOB/"
echo "Next: open it -> Build with Parameters (set SERVICE_NAME; dev defaults, or prod overrides)."
