#!/usr/bin/env bash
# verify-permitted-tree.sh — Jenkins-side provenance verification for a source checkout.
#
# Deployment Permission Rule (options-edge rule.md): "Jenkins enforces the permitted commit, not the
# assistant." The permitted-commit guard (permitted-sha-guard.sh) proves, at one moment, that a
# checkout's HEAD is the permitted commit on the environment's branch. But an effect that runs later
# consumes the FILES in that directory, and between the guard and the effect those files can be
# replaced while HEAD stays the permitted commit — a later `git pull`/`checkout`/`reset`, a copy or an
# archive extracted over the tree, an edited file. This script re-verifies, immediately before each
# effect and in the same workspace, that the tree the effect is about to consume is still exactly the
# permitted commit's tree: nothing has been changed after checkout.
#
# It answers the question the guard cannot: not "was HEAD the permitted commit" but "are the files
# right now identical to that commit, with nothing added, modified, staged, deleted or extracted over
# them". A replacement that leaves HEAD unchanged still shows up as a working-tree difference or an
# untracked/ignored path, so it is caught here regardless of how the replacement happened.
#
# Exit 0 = the tree is the permitted commit's tree, unchanged. Any other exit = REFUSED, and the
# Jenkinsfile must error() the build before the effect runs.
#
# Checks (all must hold):
#   1. --dir is a git WORKING checkout and HEAD resolves to a full commit id (as the guard requires).
#   2. HEAD == PERMITTED_SHA exactly (the same permitted commit the guard was given; the caller passes
#      the same source's permission variable as PERMITTED_SHA, e.g.
#      PERMITTED_SHA="${CONTRACTS_PERMITTED_SHA:-}").
#   3. The working tree is clean against HEAD: `git status --porcelain=v1 --untracked-files=all` prints
#      nothing (no modified, staged, deleted, renamed or untracked path) and `git diff --quiet HEAD`
#      passes. A file replaced or added after checkout is a difference here.
#   4. No ignored path is present under the tree UNLESS its top path component is a directory named on
#      an --allow-ignored list (e.g. build output such as `target`). An archive or copy dropped over
#      the source, even into a path the repo happens to ignore, is refused unless explicitly allowed.
#
# Usage: verify-permitted-tree.sh [--dir <checkout>] [--allow-ignored <dir>]...
#   --dir            the checkout to verify (default: the workspace root, ".").
#   --allow-ignored  a directory (workspace-relative to --dir, one literal path component or deeper)
#                    whose ignored build output is permitted to exist. Repeatable. Everything else
#                    that is ignored-but-present under the tree is refused.
# Reads PERMITTED_SHA from the environment (Jenkins exposes build parameters as environment variables),
# exactly as permitted-sha-guard.sh does.
set -euo pipefail

dir="."
allow_ignored=()
while [ $# -gt 0 ]; do
  case "$1" in
    --dir)           dir="${2:?--dir needs a path}";            shift 2 ;;
    --allow-ignored) allow_ignored+=("${2:?--allow-ignored needs a path}"); shift 2 ;;
    *) echo "verify-permitted-tree: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

refuse() {
  echo "verify-permitted-tree: REFUSED — $*" >&2
  echo "verify-permitted-tree: verdict=REFUSED (source changed after checkout; no deployment effect may follow)" >&2
  exit 1
}

# 1. A git working checkout with a resolvable full HEAD (same requirement as the guard).
inside="$(git -C "$dir" rev-parse --is-inside-work-tree 2>/dev/null)" || inside=""
[ "$inside" = "true" ] || refuse "'$dir' is not a git checkout (is-inside-work-tree: '${inside:-error}') — a directory that is not a working checkout cannot prove what it holds"
head_sha="$(git -C "$dir" rev-parse HEAD 2>/dev/null)" || refuse "cannot resolve HEAD in '$dir'"
case "$head_sha" in
  ''|*[!0-9a-f]*) refuse "HEAD in '$dir' did not resolve to a commit id (got '$head_sha')" ;;
esac
[ "${#head_sha}" -eq 40 ] || refuse "HEAD in '$dir' is not a full commit id (got '$head_sha')"

echo "verify-permitted-tree: checked-out HEAD = $head_sha (dir: $dir)"
if [ -z "${PERMITTED_SHA+set}" ]; then
  echo "verify-permitted-tree: PERMITTED_SHA    = <not set>"
else
  echo "verify-permitted-tree: PERMITTED_SHA    = '${PERMITTED_SHA}'"
fi

# 2. HEAD is the permitted commit. Present, well-formed, exactly equal — never substituted.
if [ -z "${PERMITTED_SHA+set}" ]; then
  refuse "PERMITTED_SHA is not set — the permitted commit for '$dir' must be provided (the same value the guard was given for this source); nothing is substituted"
fi
p="$PERMITTED_SHA"
[ -n "$p" ] || refuse "PERMITTED_SHA is empty for '$dir'; pass the full 40-character permitted commit id"
case "$p" in
  *[[:space:]]*) refuse "PERMITTED_SHA contains whitespace ('$p')" ;;
  *[!0-9a-fA-F]*) refuse "PERMITTED_SHA is not a commit id ('$p')" ;;
  *[A-F]*) refuse "PERMITTED_SHA must be lowercase hex ('$p')" ;;
esac
[ "${#p}" -eq 40 ] || refuse "PERMITTED_SHA '$p' has ${#p} characters — pass the full 40-character commit id"
[ "$head_sha" = "$p" ] \
  || refuse "checked-out HEAD $head_sha in '$dir' is not the permitted commit $p — the source was moved to another commit after checkout; nothing may deploy from it"

# FAIL-CLOSED: every git query below must SUCCEED. An inventory that cannot be produced (a git error, a killed
# process, partial output followed by failure) is never treated as "clean" or "empty" — it is a refusal. The exit
# status of each command is checked explicitly rather than defaulted away.

# 3. The working tree is identical to HEAD: no modified, staged, deleted, renamed or untracked path.
if ! status="$(git -C "$dir" status --porcelain=v1 --untracked-files=all 2>/dev/null)"; then
  refuse "cannot read the working-tree status of '$dir' (git status failed) — an inventory that cannot be produced is not an empty one"
fi
if [ -n "$status" ]; then
  first="$(printf '%s\n' "$status" | head -3 | tr '\n' '|')"
  refuse "the working tree of '$dir' differs from the permitted commit $p — files were changed, added or removed after checkout (git status: ${first%|}); the tree the effect would consume is not the permitted commit's tree"
fi
# `git diff --quiet HEAD` exits 0 = identical, 1 = differences, >1 = error. Only 0 is acceptable; both a difference
# and an error refuse.
diff_rc=0
git -C "$dir" diff --quiet HEAD >/dev/null 2>&1 || diff_rc=$?
[ "$diff_rc" -eq 0 ] \
  || refuse "tracked files in '$dir' differ from the permitted commit $p, or the comparison could not be made (git diff HEAD rc=$diff_rc) — the source was modified after checkout, or its state could not be confirmed"

# 4. Ignored-but-present paths are refused unless their top component is on the --allow-ignored list. An archive or
#    copy laid over the source is caught by check 3 as untracked; this closes the case where the dropped path happens
#    to be ignored by the repository (e.g. under a build-output dir). The enumeration itself must succeed: a failed
#    --ignored scan REFUSES, never falls through to "no ignored paths".
if ! ignored_raw="$(git -C "$dir" status --porcelain=v1 --untracked-files=all --ignored 2>/dev/null)"; then
  refuse "cannot enumerate the ignored paths under '$dir' (git status --ignored failed) — an inventory that cannot be produced is not an empty one; refusing rather than treating it as clean"
fi
while IFS= read -r line; do
  case "$line" in
    "!! "*) path="${line#!! }" ;;
    *) continue ;;
  esac
  [ -n "$path" ] || continue
  # git may quote paths containing special characters; a quoted path never matches a plain literal allow entry, so it
  # is refused, which is the safe direction. An allow entry matches when it is the whole path, a leading directory of
  # it, OR appears as a full path-component run anywhere in it — so `--allow-ignored target` covers a module's
  # `<module>/target/…` and `--allow-ignored .m2` covers `.m2/repository/…`, without listing every module by hand.
  allowed=false
  for a in ${allow_ignored[@]+"${allow_ignored[@]}"}; do
    a="${a%/}"
    case "/$path/" in
      "/$a/"|"/$a"/*|*"/$a/"*) allowed=true; break ;;
    esac
  done
  if [ "$allowed" = false ]; then
    refuse "ignored path '$path' is present under '$dir' but is not declared build output (--allow-ignored) — something was written into the source tree after checkout; declare it explicitly if it is expected build output"
  fi
done <<EOF
$ignored_raw
EOF

echo "verify-permitted-tree: verdict=PERMITTED — '$dir' is exactly the permitted commit $head_sha, working tree clean${allow_ignored+ (allowed ignored: ${allow_ignored[*]-})}"
