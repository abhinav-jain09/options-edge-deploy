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
#   4. No ignored path is present under the tree UNLESS it lies at or under one of the PATHS declared
#      with --allow-ignored (e.g. build output such as `target`). An archive or copy dropped over the
#      source, even into a path the repo happens to ignore, is refused unless explicitly allowed.
#
#      A declaration is a PATH, ANCHORED at --dir — not a name matched wherever it occurs. That
#      distinction is the point: a repository can contain a SOURCE directory whose name happens to be
#      an output name, and a name matched anywhere would exempt it. `src/main/resources/target/x`, for
#      instance, is a Maven RESOURCE that is packaged into the artifact, and `.gitignore`'s `target/`
#      ignores it — so `--allow-ignored target`, read as a name, would let a file written there after
#      checkout into the build. Read as a path it does not: `target` declares the top-level output
#      directory and nothing else. A module's output is declared as what it is, `*/target`.
#
# Usage: verify-permitted-tree.sh [--dir <checkout>] [--allow-ignored <path>]...
#   --dir            the checkout to verify (default: the workspace root, "."). It must be the ROOT of
#                    that checkout, not a subdirectory of it: the ignored-path inventory below comes
#                    from `git status`, which prints paths relative to the REPOSITORY root, so a
#                    subdirectory would have every declaration measured from the wrong anchor and would
#                    refuse its own declared build output. A subdirectory is a usage error (exit 2),
#                    never a silent mis-anchoring.
#   --allow-ignored  a PATH, relative to --dir, whose ignored build output is permitted to exist:
#                    the path itself and everything under it. Repeatable.
#
#                    THE DECLARATION GRAMMAR, which scripts/jenkins/validate-jenkinsfile-guard.py
#                    enforces character for character on the canonical verify step, so that what CI
#                    accepts and what this script accepts are the same language (a declaration CI
#                    blesses but the verifier refuses is a build that always fails at deploy time; a
#                    declaration the verifier accepts but CI refuses cannot be written at all):
#                      declaration := component ( "/" component )*
#                      component   := "*" | name
#                      name        := one or more of [A-Za-z0-9._-], and not "." or ".."
#                    "*" is exactly ONE whole path component — `*/target` covers `<module>/target/...`
#                    for every module directory and nothing deeper. There is no `**`: a declaration
#                    that would match at any depth is exactly what this refuses. Anything else — an
#                    empty declaration, a leading or trailing "/", an empty component, "~", "." or
#                    "..", "*" glued into a longer name, a space or any other character outside the
#                    name set — is a USAGE ERROR (exit 2), never a permissive default. Everything that
#                    is ignored-but-present under the tree and not covered is refused (exit 1).
# Reads PERMITTED_SHA from the environment (Jenkins exposes build parameters as environment variables),
# exactly as permitted-sha-guard.sh does.
set -euo pipefail

usage() {   # every usage error leaves with 2, including a missing or empty operand
  echo "verify-permitted-tree: $*" >&2
  exit 2
}

dir="."
dir_given=false
allow_ignored=()
while [ $# -gt 0 ]; do
  case "$1" in
    --dir)           [ $# -ge 2 ] || usage "--dir needs a path"
                     dir="$2"; dir_given=true;          shift 2 ;;
    --allow-ignored) [ $# -ge 2 ] || usage "--allow-ignored needs a path"
                     allow_ignored+=("$2");             shift 2 ;;
    *) usage "unknown argument '$1'" ;;
  esac
done
[ "$dir_given" = false ] || [ -n "$dir" ] || usage "--dir needs a path"

refuse() {
  echo "verify-permitted-tree: REFUSED — $*" >&2
  echo "verify-permitted-tree: verdict=REFUSED (source changed after checkout; no deployment effect may follow)" >&2
  exit 1
}

# An --allow-ignored declaration is a path ANCHORED at --dir, in the grammar spelled out in the usage
# block above. This function IS that grammar; validate-jenkinsfile-guard.py's ALLOW_ARG expresses the
# same one as a regular expression, and validate-jenkinsfile-guard-test.py runs a shared corpus of
# declarations through both so the two cannot drift apart. Anything that could be read as "this name,
# anywhere" — `**`, `*` glued into a longer component — and anything outside the name set is a usage
# error (exit 2), never a permissive default: a declaration nobody can read is not a permission.
declaration_is_wellformed() {
  local e="$1" rest comp
  case "$e" in
    ''|/*|*/) return 1 ;;                       # empty, absolute, or a trailing "/"
  esac
  rest="$e"
  while :; do
    comp="${rest%%/*}"
    case "$comp" in
      '*') : ;;                                 # exactly one whole component
      ''|*'*'*) return 1 ;;                     # empty component, `**`, or `*` glued into a name
      *[!A-Za-z0-9._-]*) return 1 ;;            # only the name set; "~", spaces and the rest are out
      *[!.]*) : ;;                              # a name has at least one non-dot: "." and ".." are out
      *) return 1 ;;
    esac
    if [ "$comp" = "$rest" ]; then break; fi
    rest="${rest#*/}"
  done
  return 0
}

# True when the ignored path $1 is the declared path $2 or lies under it. Both are checkout-relative and
# compared COMPONENT BY COMPONENT from the root, so a declared name never matches deeper in the path.
path_is_under_declared() {
  local p="$1" e="$2" pc ec
  while [ -n "$e" ]; do
    [ -n "$p" ] || return 1
    ec="${e%%/*}"
    if [ "$ec" = "$e" ]; then e=""; else e="${e#*/}"; fi
    pc="${p%%/*}"
    if [ "$pc" = "$p" ]; then p=""; else p="${p#*/}"; fi
    if [ "$ec" != '*' ] && [ "$ec" != "$pc" ]; then return 1; fi
  done
  return 0
}

for a in ${allow_ignored[@]+"${allow_ignored[@]}"}; do
  if ! declaration_is_wellformed "$a"; then
    usage "unusable --allow-ignored declaration '$a' — a declaration is <component>[/<component>...] where a component is '*' (exactly one whole component) or a name of [A-Za-z0-9._-] that is not '.' or '..'; an empty declaration, a leading or trailing '/', an empty component, '~', '**', '*' glued into a longer name, a space or any other character are refused"
  fi
done

# 1. A git working checkout with a resolvable full HEAD (same requirement as the guard).
inside="$(git -C "$dir" rev-parse --is-inside-work-tree 2>/dev/null)" || inside=""
[ "$inside" = "true" ] || refuse "'$dir' is not a git checkout (is-inside-work-tree: '${inside:-error}') — a directory that is not a working checkout cannot prove what it holds"
# The ignored-path inventory below is printed by git RELATIVE TO THE REPOSITORY ROOT. If --dir were a
# subdirectory, every --allow-ignored declaration would be measured from a different anchor than the one
# the usage block promises, and the script would refuse a workspace's own declared build output. Refuse
# the ambiguity instead of quietly measuring from the wrong place.
prefix="$(git -C "$dir" rev-parse --show-prefix 2>/dev/null)" || prefix="?"
[ -z "$prefix" ] || usage "'$dir' is inside a checkout but is not its root (it sits at '$prefix' within it) — --allow-ignored declarations are anchored at the checkout root, so pass the root itself"
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
  # is refused, which is the safe direction. An allow entry matches when the declared path, read from the ROOT of the
  # checkout, is the whole path or a leading directory of it — never when the same name merely occurs somewhere along
  # it. `--allow-ignored .m2` covers `.m2/repository/…`; `--allow-ignored "*/target"` covers `<module>/target/…` for
  # any one module directory; neither covers `src/main/resources/target/…`, which is source the build packages.
  allowed=false
  for a in ${allow_ignored[@]+"${allow_ignored[@]}"}; do
    if path_is_under_declared "${path%/}" "$a"; then allowed=true; break; fi
  done
  if [ "$allowed" = false ]; then
    refuse "ignored path '$path' is present under '$dir' but is not at or under a declared build-output path (--allow-ignored) — something was written into the source tree after checkout; declare the exact path (anchored at the checkout root, '*' matching one whole component) if it is expected build output"
  fi
done <<EOF
$ignored_raw
EOF

echo "verify-permitted-tree: verdict=PERMITTED — '$dir' is exactly the permitted commit $head_sha, working tree clean${allow_ignored+ (declared ignored paths: ${allow_ignored[*]-})}"
