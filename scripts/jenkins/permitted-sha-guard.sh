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
#      default is the sha256 of this file, registered by the job's own definition) must equal the
#      sha256 of the running script. A caller's compatibility check reads that registered default
#      from the child's live definition and refuses unless it equals the caller's own — so "declares
#      the parameter" and "enforces this guard" become the same statement: only a run of a definition
#      whose guard hashes to X can register X, and that guard refuses to run under any other X.
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
#      c. HEAD must be contained in origin/<branch> as fetched right now (a failed fetch refuses).
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
# Usage: permitted-sha-guard.sh [--dir <checkout>] [--branch <name>] [--ref <selected-ref>]
#   --dir     guard a NESTED application checkout (e.g. nifty-gex-src, .deps/options-edge-contracts)
#             instead of the workspace root. BRANCH_NAME/GIT_BRANCH describe the job's own SCM, not
#             the nested clone, so step 2b is skipped for it — pass --ref with the branch the clone
#             selected instead. Containment and exact equality still apply. The caller supplies that
#             source's own permitted SHA as PERMITTED_SHA.
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
# 2c. containment, tested against the branch tip fetched NOW (FETCH_HEAD), never against a stale
#     remote-tracking ref. A fetch that fails is a refusal: the branch cannot be confirmed.
git -C "$dir" fetch --quiet origin "$branch" || refuse "could not fetch origin/$branch to confirm the branch — refusing rather than guessing"
tip="$(git -C "$dir" rev-parse FETCH_HEAD)"
git -C "$dir" merge-base --is-ancestor "$head_sha" FETCH_HEAD \
  || refuse "commit $head_sha is not on origin/$branch (tip is $tip); this job deploys only merged work"
echo "permitted-sha-guard: branch ok          — $head_sha is on origin/$branch (tip $tip)"

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

echo "permitted-sha-guard: verdict=PERMITTED — checked-out $head_sha == permitted $p, on origin/$branch, guard $own_version"
