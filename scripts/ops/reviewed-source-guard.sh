#!/usr/bin/env bash
# A production reset must run origin/main's code — so it RUNS FROM origin/main, rather than checking a
# working tree and then trusting it.
#
#   exec_from_reviewed_tree <src-dir> <this-script> [<arg> ...]
#     On the first call it resolves origin/main's commit from the REMOTE, extracts that commit into a
#     private temp directory and re-execs <this-script> from there with OE_REVIEWED_TREE set. It does
#     not return.
#     On the re-exec'd run (OE_REVIEWED_TREE set) it PROVES the tree it is running from is that commit
#     — still the remote's main, every blob equal, nothing extra — and then returns 0.
#     Any failure to establish or extract the reviewed tree is a refusal: it returns 1 and the caller
#     must stop.
#   reviewed_tree_dir   the extracted tree, exported for the caller to use as its source
#
# WHY NOT A CONTENT CHECK, which is what this file did first. On 2026-10-13 05:47 a production clean
# slate ran from a checkout on another session's branch with 14 uncommitted files, including
# scripts/kafka/apply-topics.sh 89 lines behind main and lacking the skip/failure split merged the day
# before (#1173); it wiped production and took the old failure path. The obvious fix — diff every file
# the run uses against origin/main and refuse otherwise — was reviewed and had three holes that cannot
# be patched (deploy Codex round 1 of this change):
#   * the WRAPPER, this guard, and the sibling-list generator all execute before or at the check, so a
#     dirty copy of them is already running: a stubbed guard passes itself;
#   * time of check is not time of use — files are diffed, then shipped and executed later;
#   * `dry` is not harmless either: it still copies files to the production host and root-executes the
#     host-side script there, so exempting it exempted exactly the code that can ignore DRY_RUN.
# Extracting the commit removes all three: nothing from the working tree runs after the re-exec, there
# is no window between check and use because there is no check, and both modes are covered.
#
# THE REMOTE IS THE AUTHORITY, not refs/remotes/origin/main. `git fetch origin main` is documented to
# update FETCH_HEAD; whether it also moves the remote-tracking ref depends on the refspec configuration,
# so a "successful fetch" could still leave yesterday's main in that ref (round 1 BLOCKER). The commit
# is read with `git ls-remote`, fetched by explicit refspec into a ref this file owns, and the two are
# compared before anything is extracted.
#
# NO ENVIRONMENT OVERRIDE for the decision. OE_REVIEWED_TREE only claims "you are already inside the
# extracted tree", and the claim is verified against the commit before it is honoured (see
# verify_reviewed_tree), so neither a stale value from an earlier run nor one aimed at a dirty checkout
# becomes permission.

_rsg_say() { if declare -F say >/dev/null 2>&1; then say "$*"; else printf '%s\n' "$*"; fi; }

# The ref this file owns. Never origin/main, so nothing here depends on how the checkout is configured.
_RSG_REF=refs/remotes/origin/oe-reviewed-main

reviewed_tree_sha() { # <src-dir> -> the commit on the REMOTE's main, verified present locally
  local src="${1-}" remote_sha local_sha
  git -C "$src" rev-parse --git-dir >/dev/null 2>&1 || {
    _rsg_say "REFUSING: $src is not a git checkout, so what would run cannot be identified"; return 1; }
  remote_sha="$(git -C "$src" ls-remote origin refs/heads/main 2>/dev/null | awk 'NR==1{print $1}')"
  [ -n "$remote_sha" ] || {
    _rsg_say "REFUSING: could not read refs/heads/main from the remote of $src (offline, or no origin)"; return 1; }
  git -C "$src" fetch -q origin "+refs/heads/main:$_RSG_REF" 2>/dev/null || {
    _rsg_say "REFUSING: could not fetch main into $_RSG_REF in $src"; return 1; }
  local_sha="$(git -C "$src" rev-parse "$_RSG_REF" 2>/dev/null)"
  [ "$local_sha" = "$remote_sha" ] || {
    _rsg_say "REFUSING: $_RSG_REF is $local_sha but the remote says $remote_sha — the fetch did not take"; return 1; }
  printf '%s\n' "$remote_sha"
}

materialise_reviewed_tree() { # <src-dir> <sha> <dest-dir> <repo-relative-sanity-path>
  local src="$1" sha="$2" dest="$3" rel="$4"
  mkdir -p "$dest" || { _rsg_say "REFUSING: cannot create $dest"; return 1; }
  # archive|tar, with BOTH statuses checked: a pipeline's last stage succeeding says nothing about the
  # first, and a half-extracted tree is the worst outcome of all.
  local st
  git -C "$src" archive --format=tar "$sha" > "$dest/.tree.tar" || {
    _rsg_say "REFUSING: git archive of $sha failed in $src"; return 1; }
  tar -x -f "$dest/.tree.tar" -C "$dest"; st=$?
  rm -f "$dest/.tree.tar"
  [ "$st" -eq 0 ] || { _rsg_say "REFUSING: could not extract $sha into $dest"; return 1; }
  [ -r "$dest/$rel" ] || {
    _rsg_say "REFUSING: the extracted tree has no $rel — wrong commit, or a partial extract"; return 1; }
}

# The marker is a HINT, never the authority. A re-exec'd run proves, from the source checkout, that the
# tree it is running from IS that commit: the commit is still the remote's main, and the tree differs
# from it in no file at all. So a stale OE_REVIEWED_TREE left in an operator's shell — a previously
# extracted tree, or the dirty checkout itself — is refused rather than believed.
#
# The comparison is git's own, against a throwaway index, because git is what defines "this tree is that
# commit": read-tree loads the commit into that index, `update-index --refresh` re-hashes every file
# whose stat data does not match (all of them, in a fresh index — so nothing is believed on a
# timestamp), `diff-index` then reports any content, mode or missing-file difference, and
# `ls-files --others` — deliberately WITHOUT
# --exclude-standard, so an extra file cannot hide behind .gitignore — reports anything present that
# the commit does not contain. Any output from either is a refusal.
#
# `git status` was the first attempt and was wrong: it also diffs index against HEAD, so a clean
# extraction of origin/main was reported as four differences whenever the checkout sat on another
# branch. The gate is about the FILES that will run, not which branch the operator has checked out.
#
# It is NOT written as a `python3 - <<'PY'` heredoc, which is what it was first: /bin/bash 3.2 misparses
# a heredoc inside a function when the file is sourced inside a subshell that is part of a pipeline
# (`( . guard.sh ) | tail` fails, `bash -c '. guard.sh'` succeeds), and a guard may not depend on how
# its caller is sourced.
#
# A third party able to write the temp tree between this check and a later read is not defended against;
# it is a 0700 mktemp directory owned by the operator.
verify_reviewed_tree() { # <src-dir> <tree-dir> <sha>
  local src="${1-}" dir="${2-}" sha="${3-}" fresh gd idx out st
  [ -n "$src" ] && [ -n "$dir" ] && [ -n "$sha" ] || {
    _rsg_say "REFUSING: OE_REVIEWED_TREE is set but names no tree/commit, or OE_REVIEWED_FROM no checkout"; return 1; }
  fresh="$(reviewed_tree_sha "$src")" || return 1
  [ "$fresh" = "$sha" ] || {
    _rsg_say "REFUSING: this run was extracted from $sha but the remote's main is now $fresh — stale"; return 1; }
  gd="$(git -C "$src" rev-parse --absolute-git-dir 2>/dev/null)" || gd=""
  [ -n "$gd" ] || { _rsg_say "REFUSING: cannot locate the git directory of $src"; return 1; }
  idx="$(mktemp -t oe-reviewed-index)" || { _rsg_say "REFUSING: cannot create a temp index"; return 1; }
  rm -f "$idx"
  GIT_INDEX_FILE="$idx" git --git-dir="$gd" --work-tree="$dir" read-tree "$sha" 2>/dev/null || {
    rm -f "$idx"; _rsg_say "REFUSING: cannot read commit $sha out of $src"; return 1; }
  # update-index --refresh is what makes the comparison a CONTENT comparison: diff-index on its own
  # believes the index's stat data, and a just-read-tree'd index has none, so every file in the tree
  # came back "M". The refresh re-hashes anything whose stat does not match and is expected to exit
  # non-zero when files really do differ, so its status is deliberately not a verdict — diff-index is.
  GIT_INDEX_FILE="$idx" git --git-dir="$gd" --work-tree="$dir" update-index -q --refresh >/dev/null 2>&1
  out="$(GIT_INDEX_FILE="$idx" git --git-dir="$gd" --work-tree="$dir" \
           diff-index --name-status "$sha" -- 2>&1)"; st=$?
  if [ "$st" -ne 0 ]; then
    rm -f "$idx"; _rsg_say "REFUSING: could not compare $dir against $sha ($out)"; return 1
  fi
  local extra
  extra="$(cd "$dir" && GIT_INDEX_FILE="$idx" git --git-dir="$gd" --work-tree="$dir" \
             ls-files --others 2>&1)"; st=$?
  rm -f "$idx"
  if [ "$st" -ne 0 ]; then
    _rsg_say "REFUSING: could not list untracked files in $dir ($extra)"; return 1
  fi
  [ -z "$extra" ] || out="$(printf '%s\n%s' "$out" "$(printf '%s\n' "$extra" | sed 's/^/not in the commit: /')")"
  out="$(printf '%s' "$out" | sed '/^$/d')"
  if [ -n "$out" ]; then
    _rsg_say "REFUSING: $dir is not commit ${sha:0:12} — $(printf '%s\n' "$out" | wc -l | tr -d ' ') difference(s):"
    printf '%s\n' "$out" | head -8 | while IFS= read -r _l; do _rsg_say "  $_l"; done
    return 1
  fi
  _rsg_say "verified: $dir matches commit ${sha:0:12} in every file"
}

exec_from_reviewed_tree() { # <src-dir> <path-relative-to-repo-root> [<arg>...]
  local src="${1-}" rel="${2-}"; shift 2 2>/dev/null || true
  [ -n "$src" ] && [ -n "$rel" ] || { _rsg_say "exec_from_reviewed_tree: need <src-dir> <repo-relative-script>"; return 1; }
  case "$rel" in /*) _rsg_say "exec_from_reviewed_tree: <repo-relative-script> must be relative, got $rel"; return 1 ;; esac

  if [ -n "${OE_REVIEWED_TREE:-}" ]; then
    # Claimed to be inside the extracted tree. PROVE it before returning permission to continue.
    local dir="${OE_REVIEWED_TREE%%:*}" sha="${OE_REVIEWED_TREE##*:}" out
    [ -d "$dir" ] && [ -r "$dir/$rel" ] || {
      _rsg_say "REFUSING: OE_REVIEWED_TREE names $dir, which has no $rel"; return 1; }
    out="$(verify_reviewed_tree "${OE_REVIEWED_FROM:-}" "$dir" "$sha" 2>&1)" || {
      _rsg_say "$out"; return 1; }
    _rsg_say "$out"
    reviewed_tree_dir="$dir"
    _rsg_say "running from the REVIEWED tree: $dir (origin/main $sha)"
    return 0
  fi

  local sha dest
  sha="$(reviewed_tree_sha "$src")" || return 1
  dest="$(mktemp -d -t oe-reviewed-tree)" || { _rsg_say "REFUSING: cannot create a temp directory"; return 1; }
  materialise_reviewed_tree "$src" "$sha" "$dest" "$rel" || { rm -rf "$dest"; return 1; }
  _rsg_say "extracted origin/main $sha from $src into $dest — re-executing $rel from it"
  # The tree is left in place: bash reads a script incrementally, so the running copy cannot delete the
  # file it is executing. It is a mktemp directory under the system temp area; the path is printed above
  # and in the log, so a stale one is identifiable.
  OE_REVIEWED_TREE="$dest:$sha" OE_REVIEWED_FROM="$src" exec bash "$dest/$rel" "$@"
}
