#!/usr/bin/env bash
# require-guarded-downstream.sh <child-job> <forwarded-sha> [extra-required-parameter ...]
#   — fail-closed check, run immediately before `build job: '<child-job>'`, that the child will EXECUTE
#     the permitted-commit guard on the commit the caller forwards.
#
# Deployment Permission Rule: a downstream image build or rollout is a deployment of its own, and the
# caller forwards the permitted SHA for it. Forwarding binds nothing unless the child's pipeline runs
# the guard. A registered PERMITTED_SHA_GUARD_VERSION default proves only that some definition DECLARED
# it, so it is a fast pre-check here, not the proof. The proof is the definition itself:
#
#   0. the child is one this helper knows (table below): its GitHub repository, the Jenkinsfile path
#      its job must load, and where that repository's guard manifest lives;
#   1. pre-check, anonymous read of the child's registered parameters: PERMITTED_SHA and
#      PERMITTED_SHA_GUARD_VERSION are string parameters, the version default equals the sha256 of the
#      caller's own guard, and every extra parameter named on the command line is registered;
#   2. the child's JOB CONFIGURATION (config.xml, anonymous read): Pipeline script from SCM
#      (CpsScmFlowDefinition) with GitSCM, exactly ONE remote that is the expected GitHub repository,
#      exactly ONE branch spec, `*/main`, the expected scriptPath, and no SCM extensions (a pre-build
#      merge or similar would change what is built). An inline pipeline script, another repository,
#      another branch, a parameterised branch, another Jenkinsfile: refused;
#   3. the forwarded SHA is a full commit id AND is the tip of the child repository's main right now
#      (git ls-remote). The child checks out `*/main`; a SHA that is not the tip would be refused by
#      the child's guard anyway — refusing here keeps the definition judged in step 4 the definition
#      Jenkins will load;
#   4. that exact commit is fetched (git, into a private temporary directory; `gh api` as the fallback)
#      and, AT THAT COMMIT: the scriptPath Jenkinsfile, scripts/jenkins/permitted-sha-guard.sh and the
#      guard manifest are read; the CALLER'S OWN scripts/jenkins/validate-jenkinsfile-guard.py judges
#      that Jenkinsfile (--only) — guard stage, executable gates, post gating, nested bindings, its own
#      downstream checks, everything — and the fetched guard's sha256 must equal the caller's guard.
#
# Then, and only then, exit 0: Jenkins loads the child's Jenkinsfile from SCM at main, whose tip is the
# forwarded commit, and that definition was just shown to run this guard before any effect. ASSUMPTIONS
# that remain (stated, not proven): Jenkins honours the job's SCM definition (nobody Replays the build
# with an edited script, and the job is not reconfigured between this read and the trigger); main does
# not move in the seconds between step 3 and the child loading its definition (if it does, the child
# checks out the newer commit and ITS definition decides — its guard, if present, refuses it).
#
# Exit 0 = triggering is allowed. Any other exit = REFUSED; the Jenkinsfile must error() first.
set -euo pipefail
child="${1:?usage: require-guarded-downstream.sh <child-job> <forwarded-sha> [extra-required-parameter ...]}"
sha="${2:-}"
shift 2 2>/dev/null || shift $#
extras=("$@")
extras_txt=""
[ "$#" -eq 0 ] || extras_txt="$*"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

refuse() {
  echo "require-guarded-downstream: REFUSED — $*" >&2
  echo "require-guarded-downstream: verdict=REFUSED (the downstream job was not triggered)" >&2
  exit 1
}

# 0. known children: <job> <owner/repo> <scriptPath> <guard manifest>
case "$child" in
  service-deploy)          repo="abhinav-jain09/options-edge-deploy";     script_path="Jenkinsfile.service-deploy"; manifest="scripts/ci/jenkins-permitted-sha-scope.txt" ;;
  options-edge-processing) repo="abhinav-jain09/options-edge-processing"; script_path="Jenkinsfile";                manifest="scripts/jenkins/jenkins-permitted-sha-scope.txt" ;;
  options-edge-web-deploy) repo="abhinav-jain09/options-edge";            script_path="Jenkinsfile";                manifest="scripts/jenkins/jenkins-permitted-sha-scope.txt" ;;
  *) refuse "'$child' is not a downstream job this helper knows (repository, Jenkinsfile, guard manifest) — add it to the table after review; an unknown child is never triggered" ;;
esac
[ -n "${JENKINS_URL:-}" ] || refuse "JENKINS_URL is not set; cannot confirm that '$child' is guarded"
case "$sha" in
  *[!0-9a-f]*|'') refuse "forwarded SHA '$sha' is not a lowercase commit id" ;;
esac
[ "${#sha}" -eq 40 ] || refuse "forwarded SHA '$sha' is not a full 40-character commit id"

ident="$(bash "$here/jenkins-job-path.sh" "$child")" || refuse "cannot resolve '$child' relative to JOB_NAME='${JOB_NAME:-}'"
full="$(printf '%s\n' "$ident" | sed -n 's/^full=//p')"
path="$(printf '%s\n' "$ident" | sed -n 's/^path=//p')"
own_version="$(bash "$here/permitted-sha-guard-version.sh")"
base="${JENKINS_URL%/}${path}"

# 1. fast pre-check: the registered interface and guard version.
# -g: the tree filter carries brackets, which curl would otherwise expand as a glob.
json="$(curl -sfg --max-time 20 "$base/api/json?tree=property[parameterDefinitions[name,type,defaultParameterValue[value]]]")" \
  || refuse "could not read the registered parameters of '$full' — not scheduling a deployment whose guard cannot be confirmed"
verdict="$(printf '%s' "$json" | OWN_VERSION="$own_version" python3 -c '
import json, os, sys
d = json.load(sys.stdin)
defs = {}
for pr in d.get("property", []):
    for p in (pr.get("parameterDefinitions") or []):
        dv = p.get("defaultParameterValue") or {}
        defs[p.get("name")] = (p.get("type", ""), str(dv.get("value")) if dv.get("value") is not None else "")
def string_param(name):
    t = defs.get(name)
    return t is not None and t[0] == "StringParameterDefinition"
problems = []
if not string_param("PERMITTED_SHA"):
    problems.append("PERMITTED_SHA is not a registered string parameter")
if not string_param("PERMITTED_SHA_GUARD_VERSION"):
    problems.append("PERMITTED_SHA_GUARD_VERSION is not a registered string parameter")
elif defs["PERMITTED_SHA_GUARD_VERSION"][1] != os.environ["OWN_VERSION"]:
    problems.append("PERMITTED_SHA_GUARD_VERSION default is %r, this caller runs %s (the child has not registered this guard version)" % (defs["PERMITTED_SHA_GUARD_VERSION"][1], os.environ["OWN_VERSION"]))
for extra in sys.argv[1:]:
    if not string_param(extra):
        problems.append("%s is not a registered string parameter — a forwarded value would be dropped" % extra)
print("; ".join(problems))
' $extras_txt)" || refuse "could not parse the registered parameters of '$full' (not JSON — a login page?)"
[ -z "$verdict" ] || refuse "'$full' pre-check failed: $verdict"

# 2. the job configuration: which definition Jenkins will load.
config="$(curl -sf --max-time 20 "$base/config.xml")" \
  || refuse "could not read the job configuration of '$full' ($base/config.xml) — its definition cannot be confirmed"
verdict="$(printf '%s' "$config" | EXPECT_REPO="$repo" EXPECT_SCRIPT="$script_path" python3 -c '
import os, re, sys
import xml.etree.ElementTree as ET
problems = []
try:
    root = ET.fromstring(sys.stdin.read())
except ET.ParseError as e:
    print("config.xml does not parse: %s" % e); sys.exit(0)
if root.tag != "flow-definition":
    problems.append("not a Pipeline job (root element %r)" % root.tag)
defs = root.findall("definition")
d = defs[0] if len(defs) == 1 else None
if d is None or d.get("class") != "org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition":
    problems.append("definition is not Pipeline script from SCM (%s)" % (d.get("class") if d is not None else "%d definitions" % len(defs)))
else:
    scms = d.findall("scm")
    scm = scms[0] if len(scms) == 1 else None
    if scm is None or scm.get("class") != "hudson.plugins.git.GitSCM":
        problems.append("SCM is not a single GitSCM")
    else:
        urls = [u.text or "" for u in scm.findall("userRemoteConfigs/hudson.plugins.git.UserRemoteConfig/url")]
        want = os.environ["EXPECT_REPO"]
        ok_urls = {"git@github.com:%s.git" % want, "git@github.com:%s" % want, "https://github.com/%s.git" % want,
                   "https://github.com/%s" % want, "ssh://git@github.com/%s.git" % want, "ssh://git@github.com/%s" % want}
        if len(urls) != 1 or urls[0].strip() not in ok_urls:
            problems.append("remote(s) %r are not exactly github.com/%s" % (urls, want))
        branches = [b.text or "" for b in scm.findall("branches/hudson.plugins.git.BranchSpec/name")]
        if branches != ["*/main"]:
            problems.append("branch spec(s) %r are not exactly [\"*/main\"]" % branches)
        ext = scm.find("extensions")
        if ext is not None and len(list(ext)):
            problems.append("SCM extensions %r can change what is checked out" % [e.tag for e in ext])
        sub = scm.find("doGenerateSubmoduleConfigurations")
        if sub is not None and (sub.text or "").strip() not in ("", "false"):
            problems.append("submodule configuration generation is enabled")
    sp = d.find("scriptPath")
    if sp is None or (sp.text or "").strip() != os.environ["EXPECT_SCRIPT"]:
        problems.append("scriptPath is %r, not %r" % (sp.text if sp is not None else None, os.environ["EXPECT_SCRIPT"]))
print("; ".join(problems))
')" || refuse "could not judge the job configuration of '$full'"
[ -z "$verdict" ] || refuse "'$full' does not load its Jenkinsfile from the expected SCM definition: $verdict"

# 3. the forwarded SHA is main's tip. (git over ssh first — the agents' own key, as their checkouts use —
#    then https; bounded, never prompting.)
export GIT_SSH_COMMAND="${GIT_SSH_COMMAND:-ssh -o BatchMode=yes -o ConnectTimeout=15}"
ssh_url="git@github.com:$repo.git"
https_url="https://github.com/$repo.git"
tip=""
for u in "$ssh_url" "$https_url"; do
  tip="$(GIT_TERMINAL_PROMPT=0 git ls-remote "$u" refs/heads/main 2>/dev/null | awk '$2=="refs/heads/main"{print $1; exit}')" || tip=""
  [ -z "$tip" ] || break
done
if [ -z "$tip" ] && command -v gh >/dev/null 2>&1; then
  tip="$(gh api "repos/$repo/commits/main" --jq .sha 2>/dev/null)" || tip=""
fi
[ -n "$tip" ] || refuse "could not read the tip of $repo main — cannot confirm which definition '$full' will load"
[ "$tip" = "$sha" ] || refuse "forwarded SHA $sha is not the tip of $repo main ($tip): '$full' checks out main and would load a definition that was not judged (and its guard would refuse the forwarded SHA)"

# 4. the definition at that commit.
work="$(mktemp -d "${TMPDIR:-/tmp}/guarded-downstream.XXXXXX")"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/git" "$work/root/scripts/jenkins" "$work/root/$(dirname "$manifest")"
fetched=false
for u in "$ssh_url" "$https_url"; do
  if GIT_TERMINAL_PROMPT=0 git -C "$work/git" init -q 2>/dev/null \
     && GIT_TERMINAL_PROMPT=0 git -C "$work/git" fetch -q --depth 1 "$u" "$sha" 2>/dev/null \
     && [ "$(git -C "$work/git" rev-parse "FETCH_HEAD^{commit}" 2>/dev/null)" = "$sha" ]; then
    git -C "$work/git" show "$sha:$script_path" > "$work/root/$script_path" \
      && git -C "$work/git" show "$sha:scripts/jenkins/permitted-sha-guard.sh" > "$work/root/scripts/jenkins/permitted-sha-guard.sh" \
      && git -C "$work/git" show "$sha:$manifest" > "$work/root/$manifest" \
      && fetched=true
    break
  fi
done
if [ "$fetched" != true ] && command -v gh >/dev/null 2>&1; then
  gh_get() { gh api "repos/$repo/contents/$1?ref=$sha" --jq .content 2>/dev/null | python3 -c 'import base64,sys; sys.stdout.buffer.write(base64.b64decode(sys.stdin.read()))'; }
  gh_get "$script_path" > "$work/root/$script_path" \
    && gh_get "scripts/jenkins/permitted-sha-guard.sh" > "$work/root/scripts/jenkins/permitted-sha-guard.sh" \
    && gh_get "$manifest" > "$work/root/$manifest" \
    && [ -s "$work/root/$script_path" ] && [ -s "$work/root/scripts/jenkins/permitted-sha-guard.sh" ] \
    && fetched=true
fi
[ "$fetched" = true ] || refuse "could not fetch $script_path, the guard and the guard manifest of $repo at $sha"

child_guard="$(bash "$here/permitted-sha-guard-version.sh" "$work/root/scripts/jenkins/permitted-sha-guard.sh")"
[ "$child_guard" = "$own_version" ] \
  || refuse "$repo at $sha carries guard $child_guard, this caller runs $own_version — '$full' would execute a different guard than the one judged"
if ! out="$(python3 "$here/validate-jenkinsfile-guard.py" --root "$work/root" --manifest "$work/root/$manifest" --only "$script_path" 2>&1)"; then
  printf '%s\n' "$out" | sed 's/^/require-guarded-downstream:   /' >&2
  refuse "$script_path of $repo at $sha does not pass the permitted-commit validator — '$full' would not execute the guard before its effects"
fi

echo "require-guarded-downstream: '$full' loads $script_path from $repo */main, whose tip is the forwarded $sha; that definition passes the validator and runs guard $own_version${extras_txt:+; declares $extras_txt} — triggering is allowed (assumes Jenkins honours the SCM definition: no Replay, no reconfiguration in between)"
