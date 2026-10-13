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

# The marker is a HINT, never the authority. A re-exec'd run proves three things before continuing:
# the commit is still the remote's main, the tree it is running from IS that commit file for file, and
# THE PROCESS IS RUNNING THE SCRIPT INSIDE THAT TREE. The third is not redundant: without it a marker
# pointing at a faithful extraction lets a dirty copy of the script keep running, having verified
# somebody else's files (deploy Codex round 2 BLOCKER).
#
# WHY NOT `git status`, OR AN INDEX AT ALL, both of which this file used first:
#   * `status` also diffs the index against HEAD, so a faithful extraction of origin/main looked like
#     four differences whenever the operator's checkout sat on another branch;
#   * `diff-index` believes the index's stat data, and a just-read-tree'd index has none, so every file
#     came back modified until update-index --refresh was added;
#   * and the fatal one: every index path applies the checkout's CLEAN FILTERS, end-of-line conversion
#     and ident expansion. A physically different file that a configured filter normalises to the
#     committed blob passes that comparison and then executes as it physically is. The filter driver
#     lives in the source checkout's config, which is not part of the commit, so the commit cannot
#     constrain it (round 2 BLOCKER).
# So the comparison is now direct and conversion-free: `ls-tree -r -z` says what the commit contains,
# `hash-object --no-filters` says what is on disk, and `find` says what else is there. No index, no
# attributes, no ignore rules, nothing inherited from the checkout's configuration.
#
# Entry kinds are enumerated rather than assumed. A regular blob is hashed and its executable bit
# checked; a symlink (mode 120000, of which main has seven under scripts/jenkins/effect-shim) is
# compared as the BLOB OF ITS TARGET STRING, because hash-object follows a symlink and would otherwise
# hash whatever it points at; any other mode — a gitlink above all, which `diff-index` can be
# configured to ignore outright — is refused, so adding a submodule to this repository fails this guard
# loudly instead of silently leaving a hole.
#
# Nothing here deletes recursively. A failed extraction or a failed verification leaves its temp
# directory in place and says where it is; the operator can look at it, and no code path in this file
# can remove a tree.
verify_reviewed_tree() { # <src-dir> <tree-dir> <sha> <repo-relative-script>
  local src="${1-}" dir="${2-}" sha="${3-}" rel="${4-}" fresh gd treep selfp
  [ -n "$src" ] && [ -n "$dir" ] && [ -n "$sha" ] && [ -n "$rel" ] || {
    _rsg_say "REFUSING: OE_REVIEWED_TREE is set but names no tree/commit, or OE_REVIEWED_FROM no checkout"; return 1; }
  fresh="$(reviewed_tree_sha "$src")" || return 1
  [ "$fresh" = "$sha" ] || {
    _rsg_say "REFUSING: this run was extracted from $sha but the remote's main is now $fresh — stale"; return 1; }
  gd="$(git -C "$src" rev-parse --absolute-git-dir 2>/dev/null)" || gd=""
  [ -n "$gd" ] || { _rsg_say "REFUSING: cannot locate the git directory of $src"; return 1; }
  treep="$(cd "$dir" 2>/dev/null && pwd -P)" || treep=""
  [ -n "$treep" ] || { _rsg_say "REFUSING: $dir is not a directory"; return 1; }

  # Is THIS process running the script in that tree? Both sides resolved, because a mktemp path on
  # macOS is /var/folders/... which is itself a symlink to /private/var/folders/...
  selfp="$(cd "$(dirname "$0")" 2>/dev/null && pwd -P)/$(basename "$0")" || selfp=""
  [ "$selfp" = "$treep/$rel" ] || {
    _rsg_say "REFUSING: this process is running $selfp, not $treep/$rel — a verified tree does not make"
    _rsg_say "          an unverified process acceptable"; return 1; }

  local paths want got n=0 line diffs=0
  paths="$(mktemp -t oe-rev-paths)" && want="$(mktemp -t oe-rev-want)" && got="$(mktemp -t oe-rev-got)" || {
    _rsg_say "REFUSING: cannot create temp files for the comparison"; return 1; }
  _rsg_cleanup_verify() { rm -f "$paths" "$want" "$got"; }

  local ent meta mode type hash path full target
  while IFS= read -r -d '' ent; do
    meta="${ent%%	*}"; path="${ent#*	}"
    mode="${meta%% *}"; hash="${meta##* }"; type="${meta#* }"; type="${type%% *}"
    full="$treep/$path"
    n=$((n+1))
    printf '%s\n' "$full" >> "$paths"            # every entry, so the extras set below is exact
    if [ "$type" != blob ]; then
      _rsg_say "REFUSING: $path is a $type ($mode) in $sha — this guard only verifies blobs and symlinks"
      _rsg_cleanup_verify; return 1
    fi
    case "$mode" in
      100644|100755)
        if [ ! -f "$full" ] || [ -L "$full" ]; then
          _rsg_say "  missing or not a regular file: $path"; diffs=$((diffs+1)); continue
        fi
        if [ "$mode" = 100755 ] && [ ! -x "$full" ]; then
          _rsg_say "  not executable, but the commit says it is: $path"; diffs=$((diffs+1))
        elif [ "$mode" = 100644 ] && [ -x "$full" ]; then
          _rsg_say "  executable, but the commit says it is not: $path"; diffs=$((diffs+1))
        fi
        printf '%s %s\n' "$hash" "$full" >> "$want" ;;
      120000)
        if [ ! -L "$full" ]; then
          _rsg_say "  not a symlink, but the commit says it is: $path"; diffs=$((diffs+1)); continue
        fi
        target="$(readlink "$full")"
        if [ "$(printf '%s' "$target" | git --git-dir="$gd" hash-object --stdin 2>/dev/null)" != "$hash" ]; then
          _rsg_say "  symlink points somewhere else: $path -> $target"; diffs=$((diffs+1))
        fi ;;
      *)
        _rsg_say "REFUSING: unexpected mode $mode for $path in $sha"; _rsg_cleanup_verify; return 1 ;;
    esac
  done < <(git --git-dir="$gd" ls-tree -r -z "$sha")
  [ "$n" -gt 0 ] || { _rsg_say "REFUSING: $sha lists no files at all"; _rsg_cleanup_verify; return 1; }

  # Every regular file, hashed with NO conversion, in one process.
  cut -d' ' -f2- < "$want" > "$paths.reg"
  if ! git --git-dir="$gd" hash-object --no-filters --stdin-paths < "$paths.reg" > "$got" 2>/dev/null; then
    rm -f "$paths.reg"; _rsg_say "REFUSING: could not hash the files in $treep"; _rsg_cleanup_verify; return 1
  fi
  rm -f "$paths.reg"
  # One join pass, not a lookup per file: `sed -n "${i}p"` per line made this quadratic (1,677 files in
  # main today). hash-object emits one line per input path, in order, so paste lines them up: the
  # wanted hash is field 1, the computed hash the last field, and the path everything between — which
  # keeps paths containing spaces intact.
  local nwant nhash
  nwant="$(wc -l < "$want" | tr -d ' ')"; nhash="$(wc -l < "$got" | tr -d ' ')"
  if [ "$nwant" != "$nhash" ]; then
    _rsg_say "  hashed $nhash file(s) but the commit lists $nwant"; diffs=$((diffs+1))
  else
    local mism
    mism="$(paste -d' ' "$want" "$got" | awk -v root="$treep/" '
      { want=$1; got=$NF; path=""
        for (i=2; i<NF; i++) path = path (i>2 ? " " : "") $i
        if (want != got) { sub("^" root, "", path); print "  content differs: " path } }')"
    if [ -n "$mism" ]; then
      printf '%s\n' "$mism" | head -8 | while IFS= read -r _l; do _rsg_say "$_l"; done
      diffs=$((diffs + $(printf '%s\n' "$mism" | wc -l | tr -d ' ')))
    fi
  fi

  # ...and anything present that the commit does not contain, including an empty directory.
  local extra
  extra="$(find "$treep" -mindepth 1 \( -type f -o -type l \) -print 2>/dev/null \
            | LC_ALL=C sort | LC_ALL=C comm -23 - <(LC_ALL=C sort "$paths"))"
  if [ -n "$extra" ]; then
    printf '%s\n' "$extra" | head -8 | while IFS= read -r _e; do _rsg_say "  not in the commit: ${_e#$treep/}"; done
    diffs=$((diffs + $(printf '%s\n' "$extra" | wc -l | tr -d ' ')))
  fi
  local emptydirs
  emptydirs="$(find "$treep" -mindepth 1 -type d -empty -print 2>/dev/null | head -4)"
  if [ -n "$emptydirs" ]; then
    printf '%s\n' "$emptydirs" | while IFS= read -r _e; do _rsg_say "  empty directory, not in the commit: ${_e#$treep/}"; done
    diffs=$((diffs+1))
  fi
  _rsg_cleanup_verify

  [ "$diffs" -eq 0 ] || { _rsg_say "REFUSING: $treep is not commit ${sha:0:12} — $diffs difference(s) above"; return 1; }
  _rsg_say "verified: $n entr(ies) in $treep match commit ${sha:0:12}, and this process is running $rel from it"
}

exec_from_reviewed_tree() { # <src-dir> <path-relative-to-repo-root> [<arg>...]
  local src="${1-}" rel="${2-}"; shift 2 2>/dev/null || true
  [ -n "$src" ] && [ -n "$rel" ] || { _rsg_say "exec_from_reviewed_tree: need <src-dir> <repo-relative-script>"; return 1; }
  case "$rel" in /*) _rsg_say "exec_from_reviewed_tree: <repo-relative-script> must be relative, got $rel"; return 1 ;; esac

  if [ -n "${OE_REVIEWED_TREE:-}" ]; then
    # Claimed to be inside the extracted tree. PROVE it before returning permission to continue.
    # Split on the LAST colon, so a temp path containing one still parses.
    local dir="${OE_REVIEWED_TREE%:*}" sha="${OE_REVIEWED_TREE##*:}" out
    [ -d "$dir" ] && [ -r "$dir/$rel" ] || {
      _rsg_say "REFUSING: OE_REVIEWED_TREE names $dir, which has no $rel"; return 1; }
    out="$(verify_reviewed_tree "${OE_REVIEWED_FROM:-}" "$dir" "$sha" "$rel" 2>&1)" || {
      printf '%s\n' "$out" | while IFS= read -r _l; do _rsg_say "$_l"; done; return 1; }
    printf '%s\n' "$out" | while IFS= read -r _l; do _rsg_say "$_l"; done
    reviewed_tree_dir="$(cd "$dir" && pwd -P)"
    _rsg_say "running from the REVIEWED tree: $dir (origin/main $sha)"
    return 0
  fi

  local sha dest
  sha="$(reviewed_tree_sha "$src")" || return 1
  dest="$(mktemp -d -t oe-reviewed-tree)" || { _rsg_say "REFUSING: cannot create a temp directory"; return 1; }
  # No recursive delete on failure: the extraction is left where it is, named in the message above, so
  # it can be looked at. This file never removes a directory tree.
  materialise_reviewed_tree "$src" "$sha" "$dest" "$rel" || return 1
  _rsg_say "extracted origin/main $sha from $src into $dest — re-executing $rel from it"
  # The tree is left in place: bash reads a script incrementally, so the running copy cannot delete the
  # file it is executing. It is a mktemp directory under the system temp area; the path is printed above
  # and in the log, so a stale one is identifiable.
  OE_REVIEWED_TREE="$dest:$sha" OE_REVIEWED_FROM="$src" exec bash "$dest/$rel" "$@"
}
