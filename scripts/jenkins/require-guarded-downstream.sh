#!/usr/bin/env bash
# require-guarded-downstream.sh <job-name> — fail-closed compatibility check before `build job:`.
#
# Deployment Permission Rule: a downstream image build or rollout is a deployment of its own, and the
# caller forwards the permitted SHA for it. Forwarding proves nothing by itself: if the downstream job's
# LIVE definition does not declare PERMITTED_SHA, Jenkins filters the unknown parameter out of the
# child's environment (at most a controller log line) and the child runs its old, unguarded pipeline.
# So the caller asks the controller — anonymously, read-only, over the same JENKINS_URL every build has —
# whether the downstream job's registered parameter definitions include PERMITTED_SHA, and refuses to
# schedule it otherwise. Declarative registers a job's parameters when its pipeline runs, so a child
# whose guarded Jenkinsfile has merged but never run is refused too, until Abhinav runs it once (its own
# guard refuses safely on the empty value, and the parameter registers). This turns "suggested merge
# order" into an enforced activation dependency.
#
# Exit 0 = the downstream declares PERMITTED_SHA (its guard will judge the forwarded value).
# Any other exit = REFUSED; the Jenkinsfile must error() before `build job:`.
set -euo pipefail
job="${1:?usage: require-guarded-downstream.sh <job-name>}"

refuse() {
  echo "require-guarded-downstream: REFUSED — $*" >&2
  echo "require-guarded-downstream: verdict=REFUSED (the downstream job was not triggered)" >&2
  exit 1
}

[ -n "${JENKINS_URL:-}" ] || refuse "JENKINS_URL is not set; cannot confirm that '$job' is guarded"
# -g: the tree filter carries brackets, which curl would otherwise expand as a glob.
url="${JENKINS_URL%/}/job/${job}/api/json?tree=property[parameterDefinitions[name]]"
json="$(curl -sfg --max-time 20 "$url")" \
  || refuse "could not read the definition of '$job' from $url — not scheduling a deployment whose guard cannot be confirmed"

if printf '%s' "$json" | python3 -c '
import json, sys
d = json.load(sys.stdin)
names = [p.get("name") for pr in d.get("property", []) for p in (pr.get("parameterDefinitions") or [])]
sys.exit(0 if "PERMITTED_SHA" in names else 3)
'; then
  echo "require-guarded-downstream: '$job' declares PERMITTED_SHA — its own guard judges the forwarded value"
else
  refuse "'$job' does not declare a PERMITTED_SHA parameter: its live definition is unguarded (or has never run since gaining the guard), so a forwarded value would be dropped. Not triggering it. Merge and run that job's guard first."
fi
