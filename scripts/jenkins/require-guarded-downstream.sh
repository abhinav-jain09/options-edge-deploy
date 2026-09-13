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
#   2. the child's JOB CONFIGURATION (config.xml, anonymous read) — the COMPLETE mapping from the
#      repository to the loaded file, every element judged against an allowlist (anything unknown refuses):
#        * Pipeline script from SCM (CpsScmFlowDefinition) with exactly scm, scriptPath, lightweight;
#          scriptPath the expected Jenkinsfile; lightweight=true (the file is read from the branch head
#          named below, not from a full checkout whose fetch could be remapped);
#        * GitSCM with exactly ONE UserRemoteConfig: url = the expected GitHub repository, name absent or
#          `origin`, refspec absent/empty or EXACTLY `+refs/heads/*:refs/remotes/origin/*` or
#          `+refs/heads/main:refs/remotes/origin/main` (a refspec such as
#          `+refs/tags/x:refs/remotes/origin/main` or `+refs/heads/feature:refs/remotes/origin/main`
#          would make `*/main` select another commit), credentialsId allowed;
#        * exactly ONE branch spec, `*/main`; no SCM extensions; no submodule generation; no other
#          scm element (gitTool only as `Default`, an empty browser element is not accepted either).
#      An inline pipeline script, another repository, another branch, a parameterised branch, another
#      Jenkinsfile, a heavyweight checkout, a remapping refspec, another remote name: refused;
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
# Then, and only then, exit 0: Jenkins loads the child's Jenkinsfile from SCM at main, whose tip was the
# forwarded commit, and that definition was just shown to run this guard before any effect.
#
# THE PROOF WINDOW, stated honestly. Step 3 is a snapshot, not a pin. The child reads its definition when
# it LEAVES THE QUEUE — after the rest of this check, the `build job:` scheduling, any queue wait behind
# disableConcurrentBuilds or busy executors (minutes, possibly longer), and the definition load itself.
# ASSUMPTIONS that remain (stated, not proven): for that whole window (a) main is not moved to a commit
# whose Jenkinsfile lacks the guard (if main moves to a guarded commit, that child's guard refuses the
# older forwarded SHA), (b) nobody reconfigures the job, and (c) nobody Replays the build with an edited
# script. Network operations here are each bounded (GUARDED_DOWNSTREAM_NET_DEADLINE, default 120 s) and
# a stalled one ends in a named refusal.
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
deadline_s="${GUARDED_DOWNSTREAM_NET_DEADLINE:-120}"
case "$deadline_s" in ''|*[!0-9]*) deadline_s=120 ;; esac

# with_deadline <seconds> <command...> — run a network command with an OVERALL wall-clock limit (a TCP
# connect timeout does not bound a stalled transfer). macOS agents have no coreutils `timeout`; python3
# (already required here) starts the command in its own session and, at the deadline, TERMs then KILLs
# the WHOLE process group — git's ssh/https helpers included — so no orphan keeps a pipe open and a
# command substitution around this returns. Exit 124 on the deadline, the command's status otherwise.
with_deadline() {
  python3 -c '
import os, signal, subprocess, sys
p = subprocess.Popen(sys.argv[2:], start_new_session=True)
try:
    sys.exit(p.wait(timeout=float(sys.argv[1])))
except subprocess.TimeoutExpired:
    for sig, grace in ((signal.SIGTERM, 5), (signal.SIGKILL, 5)):
        try:
            os.killpg(p.pid, sig)
        except ProcessLookupError:
            break
        try:
            p.wait(timeout=grace)
            break
        except subprocess.TimeoutExpired:
            pass
    print("require-guarded-downstream: %s did not finish within %ss" % (sys.argv[2], sys.argv[1]), file=sys.stderr)
    sys.exit(124)
' "$@"
}

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
    extra_def = sorted({c.tag for c in d} - {"scm", "scriptPath", "lightweight"})
    if extra_def:
        problems.append("definition carries unexpected element(s) %r" % extra_def)
    lw = d.find("lightweight")
    if lw is None or (lw.text or "").strip() != "true":
        problems.append("lightweight checkout is %r, not true — a full checkout loads the file from a fetch this check does not reproduce" % (lw.text if lw is not None else None))
    scms = d.findall("scm")
    scm = scms[0] if len(scms) == 1 else None
    if scm is None or scm.get("class") != "hudson.plugins.git.GitSCM":
        problems.append("SCM is not a single GitSCM")
    else:
        allowed_scm = {"configVersion", "userRemoteConfigs", "branches", "doGenerateSubmoduleConfigurations", "submoduleCfg", "extensions", "gitTool"}
        extra_scm = sorted({c.tag for c in scm} - allowed_scm)
        if extra_scm:
            problems.append("SCM carries unexpected element(s) %r" % extra_scm)
        gt = scm.find("gitTool")
        if gt is not None and (gt.text or "").strip() not in ("Default",):
            problems.append("gitTool is %r" % gt.text)
        remotes = scm.findall("userRemoteConfigs/hudson.plugins.git.UserRemoteConfig")
        urc_children = [c.tag for c in scm.findall("userRemoteConfigs/*")]
        want = os.environ["EXPECT_REPO"]
        ok_urls = {"git@github.com:%s.git" % want, "git@github.com:%s" % want, "https://github.com/%s.git" % want,
                   "https://github.com/%s" % want, "ssh://git@github.com/%s.git" % want, "ssh://git@github.com/%s" % want}
        if len(remotes) != 1 or urc_children != ["hudson.plugins.git.UserRemoteConfig"]:
            problems.append("exactly one remote is required, found %d (%r)" % (len(remotes), urc_children))
        else:
            r = remotes[0]
            extra_r = sorted({c.tag for c in r} - {"url", "name", "refspec", "credentialsId"})
            if extra_r:
                problems.append("remote carries unexpected element(s) %r" % extra_r)
            urls = [u.text or "" for u in r.findall("url")]
            if len(urls) != 1 or urls[0].strip() not in ok_urls:
                problems.append("remote(s) %r are not exactly github.com/%s" % (urls, want))
            names = [(n.text or "").strip() for n in r.findall("name")]
            if len(names) > 1 or (names and names[0] not in ("", "origin")):
                problems.append("remote name %r is not origin — */main would be resolved against another remote" % names)
            refspecs = [(x.text or "").strip() for x in r.findall("refspec")]
            ok_refspecs = {"", "+refs/heads/*:refs/remotes/origin/*", "+refs/heads/main:refs/remotes/origin/main"}
            if len(refspecs) > 1 or (refspecs and refspecs[0] not in ok_refspecs):
                problems.append("fetch refspec %r can populate refs/remotes/origin/main from something other than refs/heads/main" % refspecs)
        branches = [b.text or "" for b in scm.findall("branches/hudson.plugins.git.BranchSpec/name")]
        if branches != ["*/main"] or len(list(scm.findall("branches/*"))) != 1:
            problems.append("branch spec(s) %r are not exactly [\"*/main\"]" % branches)
        ext = scm.find("extensions")
        if ext is not None and len(list(ext)):
            problems.append("SCM extensions %r can change what is checked out" % [e.tag for e in ext])
        sub = scm.find("doGenerateSubmoduleConfigurations")
        if sub is not None and (sub.text or "").strip() not in ("", "false"):
            problems.append("submodule configuration generation is enabled")
        sc = scm.find("submoduleCfg")
        if sc is not None and len(list(sc)):
            problems.append("submodule configuration is not empty")
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
  tip="$(GIT_TERMINAL_PROMPT=0 with_deadline "$deadline_s" git ls-remote "$u" refs/heads/main 2>/dev/null | awk '$2=="refs/heads/main"{print $1; exit}')" || tip=""
  [ -z "$tip" ] || break
done
if [ -z "$tip" ] && command -v gh >/dev/null 2>&1; then
  tip="$(with_deadline "$deadline_s" gh api "repos/$repo/commits/main" --jq .sha 2>/dev/null)" || tip=""
fi
[ -n "$tip" ] || refuse "could not read the tip of $repo main within ${deadline_s}s per transport (ssh, https, gh) — cannot confirm which definition '$full' will load"
[ "$tip" = "$sha" ] || refuse "forwarded SHA $sha is not the tip of $repo main ($tip): '$full' checks out main and would load a definition that was not judged (and its guard would refuse the forwarded SHA)"

# 4. the definition at that commit.
work="$(mktemp -d "${TMPDIR:-/tmp}/guarded-downstream.XXXXXX")"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/git" "$work/root/scripts/jenkins" "$work/root/$(dirname "$manifest")"
fetched=false
for u in "$ssh_url" "$https_url"; do
  if GIT_TERMINAL_PROMPT=0 git -C "$work/git" init -q 2>/dev/null \
     && GIT_TERMINAL_PROMPT=0 with_deadline "$deadline_s" git -C "$work/git" fetch -q --depth 1 "$u" "$sha" 2>/dev/null \
     && [ "$(git -C "$work/git" rev-parse "FETCH_HEAD^{commit}" 2>/dev/null)" = "$sha" ]; then
    git -C "$work/git" show "$sha:$script_path" > "$work/root/$script_path" \
      && git -C "$work/git" show "$sha:scripts/jenkins/permitted-sha-guard.sh" > "$work/root/scripts/jenkins/permitted-sha-guard.sh" \
      && git -C "$work/git" show "$sha:$manifest" > "$work/root/$manifest" \
      && fetched=true
    break
  fi
done
if [ "$fetched" != true ] && command -v gh >/dev/null 2>&1; then
  gh_get() { with_deadline "$deadline_s" gh api "repos/$repo/contents/$1?ref=$sha" --jq .content 2>/dev/null | python3 -c 'import base64,sys; sys.stdout.buffer.write(base64.b64decode(sys.stdin.read()))'; }
  gh_get "$script_path" > "$work/root/$script_path" \
    && gh_get "scripts/jenkins/permitted-sha-guard.sh" > "$work/root/scripts/jenkins/permitted-sha-guard.sh" \
    && gh_get "$manifest" > "$work/root/$manifest" \
    && [ -s "$work/root/$script_path" ] && [ -s "$work/root/scripts/jenkins/permitted-sha-guard.sh" ] \
    && fetched=true
fi
[ "$fetched" = true ] || refuse "could not fetch $script_path, the guard and the guard manifest of $repo at $sha (each transport bounded to ${deadline_s}s)"

child_guard="$(bash "$here/permitted-sha-guard-version.sh" "$work/root/scripts/jenkins/permitted-sha-guard.sh")"
[ "$child_guard" = "$own_version" ] \
  || refuse "$repo at $sha carries guard $child_guard, this caller runs $own_version — '$full' would execute a different guard than the one judged"
if ! out="$(python3 "$here/validate-jenkinsfile-guard.py" --root "$work/root" --manifest "$work/root/$manifest" --only "$script_path" 2>&1)"; then
  printf '%s\n' "$out" | sed 's/^/require-guarded-downstream:   /' >&2
  refuse "$script_path of $repo at $sha does not pass the permitted-commit validator — '$full' would not execute the guard before its effects"
fi

echo "require-guarded-downstream: '$full' loads $script_path from $repo */main, whose tip is the forwarded $sha; that definition passes the validator and runs guard $own_version${extras_txt:+; declares $extras_txt} — triggering is allowed (a snapshot: assumes main keeps a guarded Jenkinsfile, the job is not reconfigured and the build is not Replayed until the child has left the queue and loaded its definition)"
