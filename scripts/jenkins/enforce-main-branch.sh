#!/usr/bin/env bash
set -euo pipefail

allowed_branch="${JENKINS_ALLOWED_DEPLOY_BRANCH:-main}"
reported_branch="${BRANCH_NAME:-${GIT_BRANCH:-}}"

if [[ -n "$reported_branch" ]]; then
  case "$reported_branch" in
    "$allowed_branch"|"origin/$allowed_branch"|"*/$allowed_branch")
      ;;
    *)
      echo "Jenkins deployment blocked: checked-out branch is '$reported_branch', but only '$allowed_branch' is allowed." >&2
      exit 1
      ;;
  esac
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Jenkins deployment blocked: current workspace is not a Git worktree." >&2
  exit 1
fi

git fetch --quiet origin "$allowed_branch"

head_sha="$(git rev-parse HEAD)"
if ! git branch -r --contains "$head_sha" | sed 's/^[[:space:]]*//' | grep -Fx "origin/$allowed_branch" >/dev/null; then
  echo "Jenkins deployment blocked: commit $head_sha is not contained in origin/$allowed_branch." >&2
  exit 1
fi

echo "Jenkins main-branch deployment guard passed: commit $head_sha is contained in origin/$allowed_branch."
