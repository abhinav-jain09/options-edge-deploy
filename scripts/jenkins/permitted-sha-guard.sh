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
#   1. the directory is a git checkout and HEAD resolves to a full commit id
#   2. environment-branch restriction, INDEPENDENT of the SHA: the ref Jenkins reports for the job's
#      own SCM (BRANCH_NAME / GIT_BRANCH, when set) must name the allowed branch, AND HEAD must be
#      contained in origin/<branch> as fetched right now. A matching SHA on any other branch is
#      refused here, before the SHA is even looked at.
#   3. PERMITTED_SHA is present and well-formed: exactly 40 lowercase hex characters. Unset, empty,
#      whitespace, a short SHA, uppercase, a branch or tag name — all refused. Nothing is EVER
#      substituted for a missing value: not HEAD, not GIT_COMMIT, not the branch tip, not the
#      previous build's value. A manual click needs the SHA too.
#   4. HEAD == PERMITTED_SHA, byte for byte. A prefix, an ancestry test or "same branch" is not
#      equality.
# Both values are printed on every run, so the build log shows what was checked out and what was
# permitted whether the verdict is PERMITTED or REFUSED.
#
# Usage: permitted-sha-guard.sh [--dir <checkout>] [--branch <name>]
#   --dir     guard a NESTED application checkout (e.g. nifty-gex-src) instead of the workspace root.
#             The BRANCH_NAME/GIT_BRANCH name check is skipped for it — those variables describe the
#             job's own SCM, not the nested clone — but containment in origin/<branch> and exact
#             equality still apply. The caller supplies that source's own permitted SHA as
#             PERMITTED_SHA (e.g. PERMITTED_SHA="$NIFTY_PERMITTED_SHA" ... --dir nifty-gex-src).
#   --branch  the allowed branch (default: $PERMITTED_BRANCH, default main).
# Reads PERMITTED_SHA from the environment: Jenkins exposes the build parameter as one.
set -euo pipefail

dir="."
branch="${PERMITTED_BRANCH:-main}"
nested=false
while [ $# -gt 0 ]; do
  case "$1" in
    --dir)    dir="${2:?--dir needs a path}";     nested=true; shift 2 ;;
    --branch) branch="${2:?--branch needs a name}";             shift 2 ;;
    *) echo "permitted-sha-guard: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

refuse() {
  echo "permitted-sha-guard: REFUSED — $*" >&2
  echo "permitted-sha-guard: verdict=REFUSED (no deployment effect may follow)" >&2
  exit 1
}

# 1. The checkout. Refusing here, not defaulting: a workspace that is not a git checkout cannot
#    prove what it holds.
git -C "$dir" rev-parse --is-inside-work-tree >/dev/null 2>&1 || refuse "'$dir' is not a git checkout"
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
if [ "$nested" = false ]; then
  reported="${BRANCH_NAME:-${GIT_BRANCH:-}}"
  if [ -n "$reported" ]; then
    case "$reported" in
      "$branch"|"origin/$branch"|"*/$branch"|"refs/heads/$branch"|"refs/remotes/origin/$branch") ;;
      *) refuse "checked-out ref is '$reported'; this job deploys only from '$branch' (Tiered Environment-Branch Deployment Rule)" ;;
    esac
  fi
fi
# Containment is tested against the branch tip fetched NOW (FETCH_HEAD), never against a stale
# remote-tracking ref. A fetch that fails is a refusal: the branch cannot be confirmed.
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

echo "permitted-sha-guard: verdict=PERMITTED — checked-out $head_sha == permitted $p, on origin/$branch"
