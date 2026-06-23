#!/usr/bin/env bash
set -euo pipefail

job_name="${JOB_NAME:-}"
replay_job_name="${JENKINS_REPLAY_JOB_NAME:-hpsf-historical-replay}"

if [[ -z "${JENKINS_URL:-}" || -z "${BUILD_NUMBER:-}" || -z "$job_name" ]]; then
  echo "Jenkins deployment blocked: Kubernetes deploys must run from Jenkins. Manual kubectl/script deploys are banned." >&2
  exit 1
fi

target_env="${ENVIRONMENT:-dev}"
if [[ "$job_name" == "$replay_job_name" ]]; then
  # The replay job intentionally tests un-merged changes against a per-run topic
  # namespace; it is the ONLY job permitted to override the allowed branch.
  allowed_branch="${JENKINS_ALLOWED_DEPLOY_BRANCH:-main}"
else
  # Every other deploy is LOCKED TO MAIN — dev AND prod. Only reviewed-and-merged
  # code reaches a cluster. The earlier dev-only branch escape ("dev may deploy
  # any branch") was removed by user policy: feature branches must be merged
  # before any cluster deploy.
  requested_branch="${DEPLOY_BRANCH:-main}"
  if [[ "$requested_branch" != "main" ]]; then
    echo "Jenkins deployment blocked: '$target_env' deploys must use branch 'main' (got '$requested_branch'). All deploys are main-only (dev and prod). Merge first." >&2
    exit 1
  fi
  if [[ "${JENKINS_ALLOWED_DEPLOY_BRANCH:-main}" != "main" ]]; then
    echo "Jenkins deployment blocked: only the replay job may override JENKINS_ALLOWED_DEPLOY_BRANCH." >&2
    exit 1
  fi
  allowed_branch="main"
fi

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

echo "Jenkins deployment guard passed: job '$job_name' is allowed to deploy commit $head_sha from origin/$allowed_branch."
