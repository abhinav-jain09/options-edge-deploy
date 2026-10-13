#!/usr/bin/env bash
# Is the source tree about to run against PRODUCTION the reviewed one?
#
#   require_reviewed_source <src-dir> <file> [<file> ...]
#     returns 0  every named file is identical to origin/main's version of it
#     returns 1  it is not, or the question could not be answered — and then nothing may run
#
# WHY. On 2026-10-13 at 05:47 a production clean slate ran from
# /Users/abhinav/development/workspace/options-edge-deploy, which was sitting on another session's
# branch with 14 uncommitted files -- among them scripts/kafka/apply-topics.sh, 89 lines behind main and
# without the skip/failure split that had been merged the day before (#1173). So the reset wiped
# production and then took the OLD path: "apply-topics.sh exited 1 / recreate FAILED / mirrors stay
# PAUSED", leaving 53 declared topics absent and 15 mirrors down. The merged fix was never in play.
#
# The lesson is not about that branch. scripts/ops/prod-clean-slate.sh ships and runs whatever is in its
# DEPLOY_SRC checkout, so "the fix is in main" protects production exactly never. What protects it is
# refusing to wipe production with code nobody reviewed.
#
# CONTENT IDENTITY, NOT BRANCH NAME. The check is `git diff origin/main -- <file>` per file, working
# tree against origin/main:
#   * it catches the real 05:47 cause, which was UNCOMMITTED edits -- a branch-name check would have
#     passed if those edits had been made on main itself;
#   * it allows a worktree on any branch whose relevant files happen to equal main's, which is how the
#     deploy worktrees in this repo are normally used;
#   * it says nothing about files this run does not use, so unrelated work in the tree is not punished.
#
# origin/main IS FETCHED FIRST, and a fetch that fails is a refusal: a stale remote ref would let a tree
# that matches YESTERDAY's main pass as reviewed.
#
# NO ENVIRONMENT OVERRIDE, deliberately. An escape hatch is how a production lock stops being one (the
# Ansible mirror rollout learnt that when -e oe_prod_permitted=true defeated its own gate). The seam for
# trying unreviewed code is the MODE the caller already has: `prod-clean-slate.sh dry` changes nothing
# and is not guarded, `wipe` is.

_rsg_say() { if declare -F say >/dev/null 2>&1; then say "$*"; else printf '%s\n' "$*"; fi; }

require_reviewed_source() {
  local src="${1-}"; shift || true
  [ -n "$src" ] && [ "$#" -gt 0 ] || { _rsg_say "require_reviewed_source: need <src-dir> and at least one file"; return 1; }
  [ -d "$src" ] || { _rsg_say "REFUSING: $src is not a directory"; return 1; }

  local head_sha main_sha f bad=0 why
  if ! git -C "$src" rev-parse --git-dir >/dev/null 2>&1; then
    _rsg_say "REFUSING: $src is not a git checkout, so what is about to run cannot be identified"
    return 1
  fi
  if ! git -C "$src" fetch -q origin main 2>/dev/null; then
    _rsg_say "REFUSING: could not fetch origin/main in $src — a stale remote ref would let yesterday's"
    _rsg_say "          main pass as reviewed. Fix the network or the remote, then rerun."
    return 1
  fi
  main_sha="$(git -C "$src" rev-parse --short origin/main 2>/dev/null)"
  head_sha="$(git -C "$src" rev-parse --short HEAD 2>/dev/null)"
  [ -n "$main_sha" ] || { _rsg_say "REFUSING: cannot resolve origin/main in $src"; return 1; }

  for f in "$@"; do
    if [ ! -r "$src/$f" ]; then
      _rsg_say "   NOT REVIEWED: $f is missing from $src"; bad=$((bad+1)); continue
    fi
    if ! git -C "$src" ls-files --error-unmatch -- "$f" >/dev/null 2>&1; then
      _rsg_say "   NOT REVIEWED: $f is untracked in $src (it is in nobody's history)"; bad=$((bad+1)); continue
    fi
    if ! git -C "$src" diff --quiet origin/main -- "$f" 2>/dev/null; then
      # Say WHICH kind of difference, because the two have different fixes: commit and merge it, or
      # stash/checkout the edit.
      if git -C "$src" diff --quiet HEAD -- "$f" 2>/dev/null; then why="committed, but not in origin/main"
      else why="UNCOMMITTED edits in the working tree"; fi
      _rsg_say "   NOT REVIEWED: $f differs from origin/main ($why)"; bad=$((bad+1))
    fi
  done

  if [ "$bad" -ne 0 ]; then
    _rsg_say "REFUSING: $bad of $# file(s) this run would use are not origin/main's ($src HEAD=$head_sha, origin/main=$main_sha)."
    _rsg_say "          A production reset runs the files above, not the ones in main. Merge them, or point"
    _rsg_say "          DEPLOY_SRC at a checkout of origin/main. \`dry\` mode is not guarded."
    return 1
  fi
  _rsg_say "source verified: all $# file(s) identical to origin/main ($main_sha); $src HEAD=$head_sha"
}
