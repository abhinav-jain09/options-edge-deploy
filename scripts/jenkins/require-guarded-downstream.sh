#!/usr/bin/env bash
# require-guarded-downstream.sh <child-job> [extra-required-parameter ...]
#   — fail-closed compatibility check before `build job: '<child-job>'`.
#
# Deployment Permission Rule: a downstream image build or rollout is a deployment of its own, and the
# caller forwards the permitted SHA for it. Forwarding proves nothing by itself, so the caller asks the
# controller — anonymously, read-only, over the JENKINS_URL every build has — for the child's LIVE
# definition and refuses to schedule it unless the child ENFORCES this guard:
#   * PERMITTED_SHA is a registered string parameter;
#   * PERMITTED_SHA_GUARD_VERSION is a registered string parameter whose DEFAULT equals the sha256 of
#     the caller's own scripts/jenkins/permitted-sha-guard.sh. That default is written only by a run
#     of a job definition that declares it, and that definition's guard refuses to run unless the
#     script it executes hashes to the same value (see the guard, step 0). So the registered default
#     is evidence that the child last executed THIS guard — not merely that someone declared a
#     parameter with the right name. An un-activated child (merged but never run), an older child,
#     or a child whose parameter was hand-made without the guard, is refused;
#   * every extra parameter named on the command line (e.g. CONTRACTS_PERMITTED_SHA for the image
#     job) is registered too — the caller's forwarded value would otherwise be dropped unenforced.
# The child is addressed the way Jenkins resolves the simple name in `build job:` — relative to the
# CALLER's folder (JOB_NAME) — so the definition inspected is the definition scheduled.
#
# Exit 0 = the child enforces the guard. Any other exit = REFUSED; the Jenkinsfile must error()
# before `build job:`.
set -euo pipefail
child="${1:?usage: require-guarded-downstream.sh <child-job> [extra-required-parameter ...]}"
shift
extras=("$@")

refuse() {
  echo "require-guarded-downstream: REFUSED — $*" >&2
  echo "require-guarded-downstream: verdict=REFUSED (the downstream job was not triggered)" >&2
  exit 1
}

[ -n "${JENKINS_URL:-}" ] || refuse "JENKINS_URL is not set; cannot confirm that '$child' is guarded"
case "$child" in
  ''|*/*|*' '*) refuse "child job '$child' must be a simple job name (it is resolved relative to this job's folder, as build job: does)" ;;
esac
# Resolve the child the way `build job:` does: relative to the caller's folder.
folder=""
case "${JOB_NAME:-}" in
  */*) folder="${JOB_NAME%/*}" ;;
esac
full="${folder:+$folder/}$child"
path=""
IFS='/' read -r -a segs <<< "$full"
for s in "${segs[@]}"; do path="$path/job/$s"; done

own_version="$(bash "$(dirname "${BASH_SOURCE[0]}")/permitted-sha-guard-version.sh")"
# -g: the tree filter carries brackets, which curl would otherwise expand as a glob.
url="${JENKINS_URL%/}${path}/api/json?tree=property[parameterDefinitions[name,type,defaultParameterValue[value]]]"
json="$(curl -sfg --max-time 20 "$url")" \
  || refuse "could not read the definition of '$full' from $url — not scheduling a deployment whose guard cannot be confirmed"

verdict="$(printf '%s' "$json" | OWN_VERSION="$own_version" python3 -c '
import json, os, sys
d = json.load(sys.stdin)
defs = {}
for pr in d.get("property", []):
    for p in (pr.get("parameterDefinitions") or []):
        dv = p.get("defaultParameterValue") or {}
        defs[p.get("name")] = (p.get("type", ""), str(dv.get("value", "")) if dv.get("value") is not None else "")
def string_param(name):
    t = defs.get(name)
    return t is not None and t[0] == "StringParameterDefinition"
problems = []
if not string_param("PERMITTED_SHA"):
    problems.append("PERMITTED_SHA is not a registered string parameter")
if not string_param("PERMITTED_SHA_GUARD_VERSION"):
    problems.append("PERMITTED_SHA_GUARD_VERSION is not a registered string parameter (the child does not declare which guard it enforces)")
else:
    got = defs["PERMITTED_SHA_GUARD_VERSION"][1]
    if got != os.environ["OWN_VERSION"]:
        problems.append("PERMITTED_SHA_GUARD_VERSION default is %r, this caller enforces %s — the child last ran a different (older, newer or absent) guard" % (got, os.environ["OWN_VERSION"]))
for extra in sys.argv[1:]:
    if not string_param(extra):
        problems.append("%s is not a registered string parameter — a forwarded value would be dropped unenforced" % extra)
print("\n".join(problems))
' "${extras[@]}")" || refuse "could not parse the definition of '$full' (not JSON — a login page?)"

if [ -n "$verdict" ]; then
  refuse "'$full' does not enforce this guard: $(printf '%s' "$verdict" | tr '\n' ';') Not triggering it. Merge its guard, run it once (its own guard refuses safely and registers the version), then retry."
fi
echo "require-guarded-downstream: '$full' enforces guard $own_version${extras:+ and declares ${extras[*]}} — its own guard judges the forwarded value"
