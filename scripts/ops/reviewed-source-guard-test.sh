#!/usr/bin/env bash
# The guard's one claim: a production reset runs origin/main's code, whatever is in the operator's
# checkout. Driven over real throwaway git repositories — a bare "remote" plus a clone — because the
# claim is about git state (unpushed commits, a stale remote-tracking ref, upstream moving) and a fake
# cannot be wrong about that in the way a real repository can.
#
# Each case runs a committed wrapper that prints the VERSION STRING BAKED INTO THE FILE IT RAN FROM.
# "reviewed-v1" in the output means the committed copy ran; "dirty" means the working tree's copy did.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="$HERE/reviewed-source-guard.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -r "$GUARD" ] || { echo "FAIL: $GUARD missing" >&2; exit 1; }
command -v git >/dev/null 2>&1 || { echo "SKIP: git is not on PATH" >&2; exit 0; }

write_wrapper() { # <path> <version>
  cat > "$1" <<WRAP
#!/usr/bin/env bash
set -uo pipefail
HERE="\$(cd "\$(dirname "\$0")/../.." && pwd)"
. "\$HERE/scripts/ops/reviewed-source-guard.sh" || exit 1
exec_from_reviewed_tree "\${OE_REVIEWED_FROM:-\$HERE}" scripts/ops/wrapper.sh "\$@" || exit 1
printf 'RAN version=%s tree=%s args=%s\n' '$2' "\$reviewed_tree_dir" "\$*"
WRAP
  chmod +x "$1"
}

# A bare remote plus a clone, both holding the guard and a wrapper committed as reviewed-v1.
build_repo() { # <root> -> sets BARE and CO
  BARE="$1/remote.git"; CO="$1/checkout"
  mkdir -p "$1"
  git init -q --bare "$BARE"
  git -C "$BARE" symbolic-ref HEAD refs/heads/main   # or a --depth clone lands on a nonexistent master
  git init -q "$CO"
  git -C "$CO" symbolic-ref HEAD refs/heads/main
  git -C "$CO" config user.email test@example.com
  git -C "$CO" config user.name "Guard Test"
  git -C "$CO" config commit.gpgsign false
  mkdir -p "$CO/scripts/ops"
  cp "$GUARD" "$CO/scripts/ops/reviewed-source-guard.sh"
  write_wrapper "$CO/scripts/ops/wrapper.sh" reviewed-v1
  printf 'echo helper\n' > "$CO/scripts/ops/helper.sh"; chmod +x "$CO/scripts/ops/helper.sh"
  ln -sf helper.sh "$CO/scripts/ops/helper-link.sh"      # main really does carry symlinks (7 of them)
  git -C "$CO" add scripts >/dev/null
  git -C "$CO" commit -qm "first" >/dev/null
  git -C "$CO" remote add origin "$BARE"
  git -C "$CO" push -q origin main
}

# Push a new wrapper version to the remote from a second clone, so the operator's checkout has no idea.
push_upstream_version() { # <root> <version>
  local root="$1" ver="$2" tmp="$1/pusher.$2"
  git clone -q "$root/remote.git" "$tmp"
  git -C "$tmp" config user.email test@example.com
  git -C "$tmp" config user.name "Guard Test"
  git -C "$tmp" config commit.gpgsign false
  write_wrapper "$tmp/scripts/ops/wrapper.sh" "$ver"
  git -C "$tmp" add scripts/ops/wrapper.sh >/dev/null
  git -C "$tmp" commit -qm "$ver" >/dev/null
  git -C "$tmp" push -q origin HEAD:main
}

run_wrapper() { # <checkout> [args...] -> OUT, RC; markers cleared, as a fresh operator shell has them
  local co="$1"; shift
  OUT="$(cd "$co" && env -u OE_REVIEWED_TREE -u OE_REVIEWED_FROM bash scripts/ops/wrapper.sh "$@" 2>&1)"
  RC=$?
}

# <from> empty means OE_REVIEWED_FROM is UNSET, which is a case of its own. It is spelled with env
# rather than `${from:+OE_REVIEWED_FROM=...}`: a word that comes from an expansion is not an assignment,
# it is the command name, so the prefix form silently ran "OE_REVIEWED_FROM=/path: not found" and every
# case using it reported a refusal that had nothing to do with the guard.
run_with_marker() { # <checkout> <marker> <from> -> OUT, RC
  local co="$1" marker="$2" from="$3"
  if [ -n "$from" ]; then
    OUT="$(cd "$co" && env OE_REVIEWED_TREE="$marker" OE_REVIEWED_FROM="$from" bash scripts/ops/wrapper.sh wipe 2>&1)"
  else
    OUT="$(cd "$co" && env -u OE_REVIEWED_FROM OE_REVIEWED_TREE="$marker" bash scripts/ops/wrapper.sh wipe 2>&1)"
  fi
  RC=$?
}

# `bash -c <script> <name> <args...>` sets $0 to <name>, so this drives verify_reviewed_tree with the
# self-path check satisfied while the GUARD IT SOURCES comes from <guard-dir> — which a mutation case
# can edit. Without this a mutation of any other rule would be masked by the self-path refusal, and
# neutering two rules at once could not say which one the case binds to.
drive_verify() { # <guard-dir> <tree> <sha> <src> -> OUT, RC
  OUT="$(bash -c '. "$1/scripts/ops/reviewed-source-guard.sh" || exit 1
                  verify_reviewed_tree "$2" "$3" "$4" scripts/ops/wrapper.sh' \
           "$2/scripts/ops/wrapper.sh" "$1" "$4" "$2" "$3" 2>&1)"
  RC=$?
}

# ...and the end-to-end form: run the wrapper that lives INSIDE the tree, which is how a re-exec'd run
# actually looks.
run_from_tree() { # <tree> <sha> <src> -> OUT, RC
  OUT="$(cd "$1" && env OE_REVIEWED_TREE="$1:$2" OE_REVIEWED_FROM="$3" bash "$1/scripts/ops/wrapper.sh" wipe 2>&1)"
  RC=$?
}

echo "reviewed-source-guard:"

# ---- 1. the whole point: a dirty working tree does not get to run ----
# The tamper is a VERSION STRING, never a command: the wrapper sources the working tree's guard before
# re-exec, so anything written into these files is executed by the test itself. (An earlier draft of
# this file appended `rm -rf /` here as a "marker" and the test ran it.)
build_repo "$WORK/c1"
write_wrapper "$CO/scripts/ops/wrapper.sh" dirty-DO-NOT-RUN
printf '%s\n' '_rsg_tampered_working_copy() { printf "DIRTY-GUARD\n"; }' >> "$CO/scripts/ops/reviewed-source-guard.sh"
run_wrapper "$CO" wipe after=down
case "$OUT" in
  *"RAN version=reviewed-v1"*) ok "a dirty wrapper and an appended-to guard: the committed version ran" ;;
  *) bad "dirty tree ran its own code (rc=$RC): $OUT" ;;
esac
case "$OUT" in *dirty-DO-NOT-RUN*) bad "the working tree's wrapper ran" ;; *) ok "the working tree's version did not run" ;; esac
case "$OUT" in *"args=wipe after=down"*) ok "arguments reach the re-exec'd run" ;; *) bad "arguments lost: $OUT" ;; esac
n="$(printf '%s\n' "$OUT" | grep -c 'extracted origin/main')"
[ "$n" = 1 ] && ok "extraction happens exactly once — no re-exec loop" || bad "extracted $n times"

# ---- 1b. THE STATED LIMIT, asserted rather than left to the comments: the bootstrap is unreviewed, so
# a guard edited to stub itself DOES run the working tree's code. This case exists so the limit is a
# measured fact. Closing it needs something outside this process (a signed wrapper, or an operator path
# that cannot reach the checkout at all), not another check inside it.
build_repo "$WORK/c1b"
write_wrapper "$CO/scripts/ops/wrapper.sh" dirty-via-stubbed-guard
printf '%s\n' 'exec_from_reviewed_tree() { reviewed_tree_dir="$PWD"; return 0; }' \
  >> "$CO/scripts/ops/reviewed-source-guard.sh"
run_wrapper "$CO" wipe
case "$OUT" in
  *dirty-via-stubbed-guard*) ok "LIMIT confirmed: a stubbed guard runs the dirty tree (bootstrap is unreviewed)" ;;
  *) bad "the limit changed — a stubbed guard no longer runs the dirty tree; update the comments: $OUT" ;;
esac

# ---- 2. committed locally but never pushed is still unreviewed ----
build_repo "$WORK/c2"
write_wrapper "$CO/scripts/ops/wrapper.sh" local-commit-not-pushed
git -C "$CO" add scripts/ops/wrapper.sh >/dev/null
git -C "$CO" commit -qm "local only" >/dev/null
run_wrapper "$CO" wipe
case "$OUT" in
  *"RAN version=reviewed-v1"*) ok "a local commit that was never pushed does not run" ;;
  *) bad "an unpushed commit ran (rc=$RC): $OUT" ;;
esac

# ---- 3. upstream advancing AFTER the clone is picked up (the freshness claim) ----
build_repo "$WORK/c3"
push_upstream_version "$WORK/c3" reviewed-v2
run_wrapper "$CO" wipe
case "$OUT" in
  *"RAN version=reviewed-v2"*) ok "main that moved after the clone is what runs" ;;
  *) bad "ran a stale main (rc=$RC): $OUT" ;;
esac

# ---- 4. a stale ref the guard owns is corrected, not trusted ----
build_repo "$WORK/c4"
old="$(git -C "$CO" rev-parse HEAD)"
git -C "$CO" update-ref refs/remotes/origin/oe-reviewed-main "$old"
push_upstream_version "$WORK/c4" reviewed-v3
run_wrapper "$CO" wipe
case "$OUT" in
  *"RAN version=reviewed-v3"*) ok "a stale remote-tracking ref is re-fetched, not believed" ;;
  *) bad "trusted the stale ref (rc=$RC): $OUT" ;;
esac
[ "$(git -C "$CO" rev-parse refs/remotes/origin/oe-reviewed-main)" != "$old" ] \
  && ok "the ref the guard owns moved to the remote's commit" || bad "the ref did not move"

# ---- 5. cannot reach the remote: refuse, run nothing ----
build_repo "$WORK/c5"
git -C "$CO" remote remove origin
run_wrapper "$CO" wipe
[ "$RC" -ne 0 ] && ok "no remote: refused (rc=$RC)" || bad "no remote: proceeded"
case "$OUT" in *RAN*) bad "no remote: something still ran" ;; *) ok "no remote: nothing ran" ;; esac

# ---- 6. the remote answers but the fetch cannot land: refuse ----
build_repo "$WORK/c6"
git -C "$CO" update-ref refs/remotes/origin/oe-reviewed-main/blocked "$(git -C "$CO" rev-parse HEAD)"
run_wrapper "$CO" wipe
[ "$RC" -ne 0 ] && ok "fetch blocked by a ref conflict: refused" || bad "fetch failure: proceeded"
case "$OUT" in *RAN*) bad "fetch failure: something still ran" ;; *) ok "fetch failure: nothing ran" ;; esac

# ---- 7. not a git checkout at all ----
build_repo "$WORK/c7"
mkdir -p "$WORK/c7/plain"; cp -R "$CO/scripts" "$WORK/c7/plain/"
run_wrapper "$WORK/c7/plain" wipe
[ "$RC" -ne 0 ] && ok "a non-git directory: refused" || bad "a non-git directory: proceeded"
case "$OUT" in *RAN*) bad "non-git: something still ran" ;; *) ok "non-git: nothing ran" ;; esac

# ---- 8. the marker is not permission: aimed at the dirty checkout ----
build_repo "$WORK/c8"
write_wrapper "$CO/scripts/ops/wrapper.sh" dirty-via-marker
sha="$(git -C "$CO" rev-parse HEAD)"
run_with_marker "$CO" "$CO:$sha" "$CO"
[ "$RC" -ne 0 ] && ok "a marker aimed at the dirty checkout: refused" || bad "marker forgery accepted"
case "$OUT" in *dirty-via-marker*) bad "the dirty wrapper ran under a forged marker" ;; *) ok "the dirty wrapper did not run" ;; esac

# ---- 9. a correctly extracted tree, but of a commit that is no longer main ----
build_repo "$WORK/c9"
sha="$(git -C "$CO" rev-parse HEAD)"
tree="$WORK/c9/extracted"; mkdir -p "$tree"
git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
push_upstream_version "$WORK/c9" reviewed-v4
run_with_marker "$CO" "$tree:$sha" "$CO"
[ "$RC" -ne 0 ] && ok "a tree extracted from a superseded commit: refused as stale" || bad "stale extracted tree accepted"

# ---- 10. an extra file in the marker's tree ----
build_repo "$WORK/c10"
sha="$(git -C "$CO" rev-parse HEAD)"
tree="$WORK/c10/extracted"; mkdir -p "$tree"
git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
printf 'x\n' > "$tree/EXTRA-UNREVIEWED.sh"
run_with_marker "$CO" "$tree:$sha" "$CO"
[ "$RC" -ne 0 ] && ok "an extra file in the extracted tree: refused" || bad "an extra file was accepted"
# and the same tree without it is accepted, so the case above is not refusing for another reason
rm -f "$tree/EXTRA-UNREVIEWED.sh"
run_from_tree "$tree" "$sha" "$CO"
case "$OUT" in *"RAN version=reviewed-v1"*) ok "the same tree, extra removed: accepted" ;; *) bad "a faithful tree was refused: $OUT" ;; esac

# ---- 11. a marker with no source checkout to verify against ----
build_repo "$WORK/c11"
sha="$(git -C "$CO" rev-parse HEAD)"
tree="$WORK/c11/extracted"; mkdir -p "$tree"
git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
run_with_marker "$CO" "$tree:$sha" ""
[ "$RC" -ne 0 ] && ok "a marker with no source checkout: refused" || bad "unverifiable marker accepted"

# ---- 12. the function's own arguments ----
build_repo "$WORK/c12"
. "$GUARD"
( exec_from_reviewed_tree "$CO" /absolute/path.sh ) >/dev/null 2>&1 \
  && bad "an absolute script path was accepted" || ok "an absolute script path is refused"
( exec_from_reviewed_tree "$CO" ) >/dev/null 2>&1 \
  && bad "a missing script path was accepted" || ok "a missing script path is refused"
( exec_from_reviewed_tree "$CO" scripts/ops/not-in-the-commit.sh ) >/dev/null 2>&1 \
  && bad "a path absent from the commit was accepted" || ok "a path absent from the commit is refused"

# ---- 12b. A SHALLOW checkout, which is not hypothetical: on 2026-10-13 the deploy clone on this Mac
# was shallow (one boundary commit in .git/shallow), so every open branch had NO merge base with
# origin/main and a "Merge pull request" commit read as a parentless root. The guard needs one commit's
# tree and nothing of the history, so it must work there — and must still follow main when it moves,
# which in a shallow clone means the fetch has to bring the new commit in.
build_repo "$WORK/c12b"
shallow="$WORK/c12b/shallow"
git clone -q --depth 1 --branch main "file://$BARE" "$shallow" 2>/dev/null
if [ -r "$shallow/scripts/ops/wrapper.sh" ] && [ "$(git -C "$shallow" rev-parse --is-shallow-repository)" = true ]; then
  write_wrapper "$shallow/scripts/ops/wrapper.sh" dirty-in-a-shallow-clone
  run_wrapper "$shallow" wipe
  case "$OUT" in
    *"RAN version=reviewed-v1"*) ok "a shallow checkout: the committed version still ran" ;;
    *) bad "shallow checkout (rc=$RC): $OUT" ;;
  esac
  push_upstream_version "$WORK/c12b" reviewed-v5
  run_wrapper "$shallow" wipe
  case "$OUT" in
    *"RAN version=reviewed-v5"*) ok "a shallow checkout follows main when it moves" ;;
    *) bad "shallow checkout did not follow main (rc=$RC): $OUT" ;;
  esac
else
  bad "could not build a shallow clone to test with"
fi

# ---- 12c. A VERIFIED TREE DOES NOT EXCUSE AN UNVERIFIED PROCESS (round 2 BLOCKER). The marker points
# at a faithful extraction, and the script that is actually executing is the dirty one in the checkout.
# Verification of the marker's tree succeeds; the run must still refuse, because what continues past
# the guard is this process, not the tree.
build_repo "$WORK/c12c"
sha="$(git -C "$CO" rev-parse HEAD)"
tree="$WORK/c12c/faithful"; mkdir -p "$tree"
git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
write_wrapper "$CO/scripts/ops/wrapper.sh" dirty-with-a-good-marker
run_with_marker "$CO" "$tree:$sha" "$CO"
[ "$RC" -ne 0 ] && ok "a faithful tree plus a dirty running script: refused" || bad "the self-path check did not bind"
case "$OUT" in *dirty-with-a-good-marker*) bad "the dirty script ran behind a faithful tree" ;; *) ok "the dirty script did not run" ;; esac

# ---- 12d. A CLEAN FILTER MUST NOT LAUNDER A DIFFERENT FILE (round 2 BLOCKER). The commit assigns a
# filter to the wrapper and the operator's checkout configures the driver; the extracted tree's wrapper
# is physically different but cleans to the committed blob. Any comparison that goes through git's
# index (status, diff-index) calls that equal and then executes the physical file.
build_repo "$WORK/c12d"
printf 'scripts/ops/wrapper.sh filter=stripdirt\n' > "$CO/.gitattributes"
git -C "$CO" add .gitattributes >/dev/null
git -C "$CO" commit -qm "attributes" >/dev/null
git -C "$CO" push -q origin main
git -C "$CO" config filter.stripdirt.clean "grep -v '^#DIRT'"
sha="$(git -C "$CO" rev-parse HEAD)"
tree="$WORK/c12d/laundered"; mkdir -p "$tree"
git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
printf '#DIRT this line is stripped by the configured clean filter\n' >> "$tree/scripts/ops/wrapper.sh"
OUT="$(cd "$tree" && env OE_REVIEWED_TREE="$tree:$sha" OE_REVIEWED_FROM="$CO" bash "$tree/scripts/ops/wrapper.sh" wipe 2>&1)"
RC=$?
[ "$RC" -ne 0 ] && ok "a file laundered by a clean filter: refused" || bad "a clean filter laundered an unreviewed file"
case "$OUT" in
  *"content differs: scripts/ops/wrapper.sh"*) ok "and the physical difference is what was reported" ;;
  *) bad "refused for another reason, so the filter case is not what bound: $OUT" ;;
esac

# ---- 12e. the kinds of entry this guard is willing to verify ----
build_repo "$WORK/c12e"
sha="$(git -C "$CO" rev-parse HEAD)"
tree="$WORK/c12e/t"; mkdir -p "$tree"
git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
ln -sf /bin/sh "$tree/scripts/ops/helper-link.sh"
OUT="$(cd "$tree" && env OE_REVIEWED_TREE="$tree:$sha" OE_REVIEWED_FROM="$CO" bash "$tree/scripts/ops/wrapper.sh" wipe 2>&1)"; RC=$?
case "$OUT" in *"symlink points somewhere else"*) ok "a retargeted symlink: refused, and named" ;; *) bad "symlink retarget not caught: $OUT" ;; esac
rm -f "$tree/scripts/ops/helper-link.sh"; ln -sf helper.sh "$tree/scripts/ops/helper-link.sh"
chmod -x "$tree/scripts/ops/helper.sh"
OUT="$(cd "$tree" && env OE_REVIEWED_TREE="$tree:$sha" OE_REVIEWED_FROM="$CO" bash "$tree/scripts/ops/wrapper.sh" wipe 2>&1)"; RC=$?
case "$OUT" in *"not executable, but the commit says it is"*) ok "a cleared executable bit: refused, and named" ;; *) bad "exec bit not caught: $OUT" ;; esac
chmod +x "$tree/scripts/ops/helper.sh"
mkdir -p "$tree/scripts/ops/empty-dir"
OUT="$(cd "$tree" && env OE_REVIEWED_TREE="$tree:$sha" OE_REVIEWED_FROM="$CO" bash "$tree/scripts/ops/wrapper.sh" wipe 2>&1)"; RC=$?
case "$OUT" in *"empty directory, not in the commit"*) ok "an added empty directory: refused" ;; *) bad "empty directory not caught: $OUT" ;; esac
rmdir "$tree/scripts/ops/empty-dir"
OUT="$(cd "$tree" && env OE_REVIEWED_TREE="$tree:$sha" OE_REVIEWED_FROM="$CO" bash "$tree/scripts/ops/wrapper.sh" wipe 2>&1)"; RC=$?
case "$OUT" in *"RAN version=reviewed-v1"*) ok "and the same tree, restored, is accepted" ;; *) bad "a faithful tree was refused: $OUT" ;; esac

# ---- 12f. a gitlink: `diff-index` can be configured to ignore submodules outright, so this guard
# refuses the entry kind instead of trying to verify it. Adding a submodule here must fail loudly.
build_repo "$WORK/c12f"
git -C "$CO" update-index --add --cacheinfo "160000,$(git -C "$CO" rev-parse HEAD),vendor/sub" 2>/dev/null
git -C "$CO" commit -qm "a gitlink" >/dev/null 2>&1
git -C "$CO" push -q origin main 2>/dev/null
sha="$(git -C "$CO" rev-parse HEAD)"
tree="$WORK/c12f/t"; mkdir -p "$tree"
git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
OUT="$(cd "$tree" && env OE_REVIEWED_TREE="$tree:$sha" OE_REVIEWED_FROM="$CO" bash "$tree/scripts/ops/wrapper.sh" wipe 2>&1)"; RC=$?
if git -C "$CO" ls-tree -r "$sha" | grep -q '^160000'; then
  [ "$RC" -ne 0 ] && ok "a gitlink in the commit: refused rather than skipped" || bad "a gitlink was verified away"
  case "$OUT" in *"only verifies blobs and symlinks"*) ok "and the refusal says why" ;; *) bad "wrong reason: $OUT" ;; esac
else
  bad "could not build a commit with a gitlink to test with"
fi

# ---- 13. mutations: each case above must FAIL to refuse when its own rule is removed ----
# One mutation at a time, each aimed at exactly one assertion.
mutate_and_run_stale() { # removes the freshness comparison; case 9 must then pass
  build_repo "$WORK/m1"
  sha="$(git -C "$CO" rev-parse HEAD)"
  tree="$WORK/m1/extracted"; mkdir -p "$tree"
  git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
  push_upstream_version "$WORK/m1" mutant-v2
  # The mutation must make the comparison always SUCCEED (so the guard proceeds), not always fail:
  # an earlier version replaced the expected value and so turned the check into a permanent refusal,
  # which "passes" the stale case for the opposite reason and proves nothing.
  sed -i.bak 's/^  \[ "\$fresh" = "\$sha" \] ||/  [ -n "$fresh" ] ||/' "$CO/scripts/ops/reviewed-source-guard.sh"
  grep -q '^  \[ -n "\$fresh" \] ||' "$CO/scripts/ops/reviewed-source-guard.sh" || { bad "mutation 1 did not apply"; return; }
  drive_verify "$CO" "$tree" "$sha" "$CO"
  [ "$RC" -eq 0 ] && ok "mutation 1 (freshness check removed) is caught by case 9" \
                  || bad "mutation 1 still refused, so case 9 proves nothing (rc=$RC): $OUT"
}
mutate_and_run_stale

mutate_and_run_content() { # removes the content/extras verdict; case 10 must then pass
  build_repo "$WORK/m2"
  sha="$(git -C "$CO" rev-parse HEAD)"
  tree="$WORK/m2/extracted"; mkdir -p "$tree"
  git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
  printf 'x\n' > "$tree/EXTRA-UNREVIEWED.sh"
  sed -i.bak 's/^  \[ "\$diffs" -eq 0 \] ||/  [ 0 -eq 0 ] ||/' "$CO/scripts/ops/reviewed-source-guard.sh"
  grep -q '^  \[ 0 -eq 0 \] ||' "$CO/scripts/ops/reviewed-source-guard.sh" || { bad "mutation 2 did not apply"; return; }
  drive_verify "$CO" "$tree" "$sha" "$CO"
  [ "$RC" -eq 0 ] && ok "mutation 2 (the difference verdict removed) is caught by case 10" \
                  || bad "mutation 2 still refused, so case 10 proves nothing (rc=$RC): $OUT"
}
mutate_and_run_content

mutate_and_run_selfpath() { # removes the self-path check; case 12c must then run the dirty script
  build_repo "$WORK/m3"
  sha="$(git -C "$CO" rev-parse HEAD)"
  tree="$WORK/m3/faithful"; mkdir -p "$tree"
  git -C "$CO" archive --format=tar "$sha" | tar -x -C "$tree"
  write_wrapper "$CO/scripts/ops/wrapper.sh" dirty-with-a-good-marker
  # '#' as the delimiter: the line being replaced ends in '||', so '|' cannot delimit it.
  sed -i.bak 's#^  \[ "\$selfp" = "\$treep/\$rel" \] ||#  [ -n "$selfp" ] ||#' "$CO/scripts/ops/reviewed-source-guard.sh"
  grep -q '^  \[ -n "\$selfp" \] ||' "$CO/scripts/ops/reviewed-source-guard.sh" || { bad "mutation 3 did not apply"; return; }
  run_with_marker "$CO" "$tree:$sha" "$CO"
  case "$OUT" in
    *dirty-with-a-good-marker*) ok "mutation 3 (self-path check removed) is caught by case 12c" ;;
    *) bad "mutation 3 still refused, so case 12c proves nothing (rc=$RC): $OUT" ;;
  esac
}
mutate_and_run_selfpath

if [ "$fails" -eq 0 ]; then echo "reviewed-source-guard: all checks passed"; else
  echo "reviewed-source-guard: $fails check(s) FAILED"; fi
exit $(( fails > 0 ? 1 : 0 ))
