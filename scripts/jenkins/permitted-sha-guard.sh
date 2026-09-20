#!/usr/bin/env bash
# permitted-sha-guard.sh — the Jenkins-side "permitted commit" guard.
#
# Deployment Permission Rule (options-edge rule.md): "Jenkins enforces the permitted commit, not the
# assistant." Every deployment job takes a PERMITTED_SHA parameter and, before any stage that builds,
# publishes, applies, rolls, restarts or alters anything, fails closed unless the checked-out commit
# equals PERMITTED_SHA exactly — while still refusing any ref other than the environment's branch
# (Tiered Environment-Branch Deployment Rule; with dev and experiment frozen that is `main` for every
# environment). The motivating failure: a build queued for commit A checks out a newer commit B once
# the branch moves, and publishes or rolls B before anyone can react. This runs INSIDE the build, in
# the workspace the effect will come from, so B cannot proceed under A's permission.
#
# Exit 0 = PERMITTED. Any other exit = REFUSED, and the Jenkinsfile must error() the build. Order:
#   0. this script IS the version the job declares: PERMITTED_SHA_GUARD_VERSION (a parameter whose
#      default is the sha256 of this file) must equal the sha256 of the running script. This protects
#      a run that EXECUTES the guard from running a different guard than it declares. It proves
#      nothing about a job that never calls the guard: a registered default is a declaration only.
#      That a downstream job will execute the guard is established separately, by the caller, from
#      the job's SCM definition and its Jenkinsfile at the forwarded commit
#      (scripts/jenkins/require-guarded-downstream.sh).
#   1. the directory is a git WORKING checkout (`rev-parse --is-inside-work-tree` prints `true` — a
#      bare repository or a .git metadata directory prints `false` and is refused) and HEAD resolves
#      to a full commit id
#   2. environment-branch restriction, INDEPENDENT of the SHA, three parts, all of which must hold:
#      a. the source ref the pipeline explicitly SELECTED for this checkout, when the caller names it
#         (--ref: PROCESSING_BRANCH, CONTRACTS_BRANCH, the `branch:` of a git step), must be the
#         allowed branch name LITERALLY — no alias (`origin/main`, `refs/heads/main`, `*/main`) is a
#         selection of main: the checkout machinery interprets those differently (a branch literally
#         named `origin/main` exists as a bypass), so a selected ref is judged as the exact name;
#      b. for the job's own workspace (no --dir): EVERY branch variable Jenkins set — BRANCH_NAME and
#         GIT_BRANCH, each judged on its own — must name the allowed branch (these are SCM metadata,
#         where Jenkins's own spellings origin/<b> and */<b> are legitimate). One variable never masks
#         the other: BRANCH_NAME=main with GIT_BRANCH=origin/feature is a contradiction and is refused;
#      c. HEAD must BE origin/<branch> as fetched right now: the branch is fetched, HEAD must be
#         contained in it AND equal to its tip (a failed fetch refuses). Containment alone is not
#         enough — an ANCESTOR of the tip is a commit the branch has already moved past, while the
#         rule says the branch HEAD must BE the permitted commit. The two conditions stay apart so
#         a refusal names the real fault: "not on origin/<branch>" and "behind origin/<branch>"
#         are different faults.
#   3. PERMITTED_SHA is present and well-formed: exactly 40 lowercase hex characters. Unset, empty,
#      whitespace, a short SHA, uppercase, a branch or tag name — all refused. Nothing is EVER
#      substituted for a missing value: not HEAD, not GIT_COMMIT, not the branch tip, not the
#      previous build's value. A manual click needs the SHA too.
#   4. HEAD == PERMITTED_SHA, byte for byte. A prefix, an ancestry test or "same branch" is not
#      equality.
# Once HEAD resolves (step 1), both values are printed before either is judged, so a refusal's log
# still shows what was checked out and what was permitted. A usage error, a version mismatch or a
# non-checkout refuses before that and prints only its reason.
#
# WHAT THIS STILL CANNOT PROVE — two limits, stated as limits.
#
# 1. THE SNAPSHOT. Every check here is made at ONE moment: the moment this script fetched. The branch
#    is a ref anyone may move, so the tip can advance right afterwards. What is established is that AT
#    THE MOMENT OF THE CHECK the checkout was the permitted commit and was the branch tip. Nothing
#    here makes a RUNNING PIPELINE immune to a later merge: a job that invokes the guard again in a
#    second workspace re-asks the same question against a NEWER tip, so an unchanged, already-admitted
#    checkout can be refused at that later invocation — service-deploy re-guards after its long
#    validation stage, and a merge during that stage is enough. That is the rule working (the branch
#    HEAD must BE the permitted commit, and it no longer is), not a defect, but it is a real
#    operational consequence: take the SHA and start the build together, and expect a long pipeline to
#    lose the race occasionally.
#
# 2. WHAT THE FILES ARE. This script judges a COMMIT ID, never file contents. What the FILES of the
#    checkout are when work actually runs is a different question, answered by
#    scripts/jenkins/verify-permitted-tree.sh — and answered only where a pipeline places it. It is
#    NOT true that every effect in every job is preceded by that verification. What
#    validate-jenkinsfile-guard.py REQUIRES to be preceded by it is the set of source-consuming
#    effects its grammar names: commands that turn this checkout into an artifact that is packaged,
#    installed, published or shipped (mvn at the package phase or later, docker build/buildx build,
#    rsync, scp, helm install/upgrade, ansible-playbook, and repository scripts that run one of them).
#    Applying a rendered manifest — kubectl, a deploy helper that ends in kubectl — is NOT in that set,
#    so the validator does not demand a verification in front of it. Where a job wants one there, it
#    places it itself and its repository's tests assert the adjacency (Jenkinsfile.nifty-gex-service's
#    'Deploy (service-scoped)' stage does exactly that). Read the claim as "every source-consuming
#    effect the validator recognises", never as "every effect".
#
# Usage: permitted-sha-guard.sh [--dir <checkout>] [--branch <name>] [--ref <selected-ref>]
#   --dir     guard a NESTED application checkout (e.g. nifty-gex-src, .deps/options-edge-contracts)
#             instead of the workspace root. BRANCH_NAME/GIT_BRANCH describe the job's own SCM, not
#             the nested clone, so step 2b is skipped for it — pass --ref with the branch the clone
#             selected instead. The branch-tip condition and exact equality still apply. The
#             caller supplies that source's own permitted SHA as PERMITTED_SHA.
#   --ref     the source ref this checkout was explicitly selected from (step 2a; literal name).
#   --branch  the allowed branch (default: $PERMITTED_BRANCH, default main).
# Reads PERMITTED_SHA and PERMITTED_SHA_GUARD_VERSION from the environment: Jenkins exposes the
# build parameters as environment variables.
set -euo pipefail

dir="."
branch="${PERMITTED_BRANCH:-main}"
ref=""
nested=false
while [ $# -gt 0 ]; do
  case "$1" in
    --dir)    dir="${2:?--dir needs a path}";     nested=true; shift 2 ;;
    --branch) branch="${2:?--branch needs a name}";             shift 2 ;;
    --ref)    ref="${2:?--ref needs a ref name}";               shift 2 ;;
    *) echo "permitted-sha-guard: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

refuse() {
  echo "permitted-sha-guard: REFUSED — $*" >&2
  echo "permitted-sha-guard: verdict=REFUSED (no deployment effect may follow)" >&2
  exit 1
}

# 0. The running guard is the declared version.
self="${BASH_SOURCE[0]}"
if command -v sha256sum >/dev/null 2>&1; then
  own_version="$(sha256sum "$self" | cut -d' ' -f1)"
else
  own_version="$(shasum -a 256 "$self" | cut -d' ' -f1)"
fi
declared="${PERMITTED_SHA_GUARD_VERSION:-}"
[ -n "$declared" ] || refuse "PERMITTED_SHA_GUARD_VERSION is not set: this job must declare the guard version it enforces (a string parameter whose default is the sha256 of scripts/jenkins/permitted-sha-guard.sh)"
[ "$declared" = "$own_version" ] || refuse "PERMITTED_SHA_GUARD_VERSION is '$declared' but the running guard is $own_version — the job's declared guard version and the guard it executes disagree; nothing may deploy under a guard the declaration does not describe"
echo "permitted-sha-guard: version           = $own_version (declared and running agree)"

# SCM-metadata spellings of the allowed branch (step 2b). Literal alternatives, not globs.
names_allowed_branch() {
  case "$1" in
    "$branch"|"origin/$branch"|"*/$branch"|"refs/heads/$branch"|"refs/remotes/origin/$branch") return 0 ;;
  esac
  return 1
}

# 1. The checkout. Refusing here, not defaulting: a workspace that is not a git WORKING checkout
#    cannot prove what it holds. `--is-inside-work-tree` exits 0 and prints `false` inside a bare
#    repository or a .git directory — the printed answer is what is judged, never the exit status.
inside="$(git -C "$dir" rev-parse --is-inside-work-tree 2>/dev/null)" || inside=""
[ "$inside" = "true" ] || refuse "'$dir' is not a git checkout (is-inside-work-tree: '${inside:-error}')"
head_sha="$(git -C "$dir" rev-parse HEAD 2>/dev/null)" || refuse "cannot resolve HEAD in '$dir'"
case "$head_sha" in
  ''|*[!0-9a-f]*) refuse "HEAD in '$dir' did not resolve to a commit id (got '$head_sha')" ;;
esac
[ "${#head_sha}" -eq 40 ] || refuse "HEAD in '$dir' is not a full commit id (got '$head_sha')"

# Print both sides before judging either, so a refusal's log still carries the facts.
echo "permitted-sha-guard: checked-out HEAD  = $head_sha (dir: $dir)"
if [ -z "${PERMITTED_SHA+set}" ]; then
  echo "permitted-sha-guard: PERMITTED_SHA     = <not set>"
else
  echo "permitted-sha-guard: PERMITTED_SHA     = '${PERMITTED_SHA}'"
fi

# 2. Environment-branch restriction — its own condition, judged FIRST and never satisfied by the SHA.
# 2a. the explicitly selected source ref: the literal branch name, nothing else.
if [ -n "$ref" ]; then
  [ "$ref" = "$branch" ] \
    || refuse "selected source ref is '$ref'; this job deploys only from '$branch', selected by that exact name (Tiered Environment-Branch Deployment Rule) — an alias, a remote-tracking spelling or a ref that merely points at a merged commit is not a selection of '$branch'"
  echo "permitted-sha-guard: selected ref      = $ref (allowed)"
fi
# 2b. the job's own SCM metadata: every variable that is set must agree; a contradiction is refused.
if [ "$nested" = false ]; then
  for var in BRANCH_NAME GIT_BRANCH; do
    val="${!var:-}"
    [ -n "$val" ] || continue
    names_allowed_branch "$val" \
      || refuse "$var is '$val'; this job deploys only from '$branch' (Tiered Environment-Branch Deployment Rule)"
    echo "permitted-sha-guard: $var = $val (allowed)"
  done
fi
# 2c. the branch, fetched NOW, never a stale remote-tracking ref. A fetch that fails is a refusal: the
#     branch cannot be confirmed. THREE points, in this order:
#     * the ref is FULLY QUALIFIED — `refs/heads/$branch`, not `$branch`. `git fetch origin main`
#       resolves through git's ref-disambiguation rules, so a TAG named `main` satisfies it when the
#       branch does not exist at all, and the guard would then report the tag's commit as "the tip of
#       origin/main". Branch and tag namespaces are separate; a tag is not the environment's branch,
#       and tip equality cannot repair a ref that named the wrong thing to begin with.
#     * the fetched tip is captured ONCE, into $tip, and BOTH predicates below are judged against that
#       one commit. FETCH_HEAD is a mutable file: reading it again for the second predicate lets a
#       concurrent fetch in the same checkout answer the two questions about two different commits,
#       which is how a correct build gets refused as off-branch while the log prints its own commit as
#       the tip. One capture, one snapshot, one story in the log.
#     * TWO conditions, judged and reported separately: HEAD is ON the branch (merged work), and HEAD
#       IS the branch tip (current work).
git -C "$dir" fetch --quiet origin "refs/heads/$branch" || refuse "could not fetch refs/heads/$branch from origin to confirm the branch — refusing rather than guessing (a tag or any other ref of that name is not the branch)"
tip="$(git -C "$dir" rev-parse FETCH_HEAD^{commit} 2>/dev/null)" || tip=""
case "$tip" in
  ''|*[!0-9a-f]*) refuse "the fetched refs/heads/$branch did not resolve to a commit id (got '${tip:-<none>}')" ;;
esac
[ "${#tip}" -eq 40 ] || refuse "the fetched refs/heads/$branch did not resolve to a full commit id (got '$tip')"
git -C "$dir" merge-base --is-ancestor "$head_sha" "$tip" \
  || refuse "commit $head_sha is not on origin/$branch (tip is $tip); this job deploys only merged work"
[ "$head_sha" = "$tip" ] \
  || refuse "commit $head_sha is on origin/$branch but is NOT its tip ($tip) — origin/$branch has moved past it. This job deploys the BRANCH HEAD: an ancestor is work the branch has already left behind, and being merged is not being current. Take the permission for $tip and run again."
echo "permitted-sha-guard: branch ok          — $head_sha is the tip of origin/$branch"

# 3. PERMITTED_SHA: present and well-formed. Every refusal names what was wrong; none substitutes.
if [ -z "${PERMITTED_SHA+set}" ]; then
  refuse "PERMITTED_SHA is not set. Every run of this job needs Abhinav's permission for ONE exact commit: pass its full 40-character SHA as PERMITTED_SHA (a manual click needs it too)."
fi
p="$PERMITTED_SHA"
[ -n "$p" ] || refuse "PERMITTED_SHA is empty. Every run of this job needs Abhinav's permission for ONE exact commit: pass its full 40-character SHA as PERMITTED_SHA (a manual click needs it too)."
case "$p" in
  *[[:space:]]*) refuse "PERMITTED_SHA contains whitespace ('$p'); pass exactly the 40-character commit id" ;;
esac
case "$p" in
  *[!0-9a-fA-F]*) refuse "PERMITTED_SHA is not a commit id ('$p'); pass the full 40-character SHA, never a branch, tag or ref" ;;
  *[A-F]*)        refuse "PERMITTED_SHA must be lowercase hex, exactly as git prints it ('$p')" ;;
esac
if [ "${#p}" -ne 40 ]; then
  refuse "PERMITTED_SHA '$p' has ${#p} characters — a short SHA is refused; pass the full 40-character commit id"
fi

# 4. Exact equality with the checkout the effect will come from.
[ "$head_sha" = "$p" ] \
  || refuse "checked-out HEAD $head_sha is not the permitted commit $p — the branch moved past the permitted commit, or the permission is for a different one; nothing may deploy under it"

echo "permitted-sha-guard: verdict=PERMITTED — checked-out $head_sha == permitted $p == tip of origin/$branch, guard $own_version"
