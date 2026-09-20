#!/bin/bash
# THE SHIM'S INTEGRITY, CHECKED FROM OUTSIDE ITSELF.
#
# A wrapper cannot detect its own absence, and mutable code cannot establish its own integrity by
# checking itself -- THE JOB OWNS ITS OWN PROCESS, and a step in the workspace can delete or edit a file
# in the workspace. That is not fixable here (see effect-shim/_shim.sh for the alternatives considered
# and why each fails). What IS possible is to look at the shim directory from outside, twice:
#
#   AT THE START of a guarded stage this is PREVENTION: a stage that begins with a tampered shim stops
#   before its steps run, and nothing unverified happens.
#
#   AT THE END it is DETECTION, and the difference matters at 06:30 in front of a red build: THE BUILD
#   FAILS AND NAMES THE TAMPERING, AND WHATEVER THE UNVERIFIED STEP DID HAS ALREADY HAPPENED. If that
#   step was a `kubectl apply`, something in the cluster changed. This turns "silently unprotected" into
#   "loudly wrong". It does not make the effect safe and must never be described as if it does.
#
# What it checks: every expected wrapper name is present and is the symlink it should be, _shim.sh
# matches the digest recorded in this file's sibling manifest, and the directory holds nothing else.
#
# Usage: effect-shim-integrity.sh --dir <checkout> --when start|end
set -u

dir=""; when=""
usage() { echo "effect-shim-integrity: $*" >&2; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --dir)  [ $# -ge 2 ] || usage "--dir needs a path";  dir="$2";  shift 2 ;;
    --when) [ $# -ge 2 ] || usage "--when needs start|end"; when="$2"; shift 2 ;;
    *) usage "unknown argument '$1'" ;;
  esac
done
[ -n "$dir" ]  || usage "--dir is required"
case "$when" in start|end) ;; *) usage "--when must be start or end" ;; esac

self="${BASH_SOURCE[0]}"
case "$self" in */*) here="$(cd "${self%/*}" && pwd -P)" ;; *) here="$(pwd -P)" ;; esac
shim="$dir/scripts/jenkins/effect-shim"
TOOLS="mvn docker rsync scp helm ansible-playbook kubectl"

refuse() {
  echo "effect-shim-integrity($when): REFUSED — $*" >&2
  if [ "$when" = end ]; then
    echo "effect-shim-integrity(end): the stage's effects ALREADY RAN. This build is failing to tell you" >&2
    echo "effect-shim-integrity(end): they were not covered — not to undo them. If one was a deployment," >&2
    echo "effect-shim-integrity(end): the cluster changed. Check what this build applied before re-running." >&2
  fi
  echo "effect-shim-integrity($when): verdict=REFUSED" >&2
  exit 3
}

[ -d "$shim" ] || refuse "the shim directory is missing at $shim — nothing was intercepting anything"

for t in $TOOLS; do
  [ -e "$shim/$t" ] || refuse "the wrapper '$t' is missing: a wrapper cannot detect its own absence, so every invocation of $t in this stage resolved to the real binary unverified"
  link="$(readlink "$shim/$t" 2>/dev/null || true)"
  [ "$link" = "_shim.sh" ] || refuse "the wrapper '$t' is not the expected symlink to _shim.sh (found: '${link:-a regular file}')"
  # EXECUTABILITY IS PART OF EXISTING. `chmod 644 _shim.sh` leaves the bytes, the symlinks and the digest
  # untouched and every other check in this file green, while execvp() skips the wrapper and runs the
  # real binary unverified. A file the kernel will not execute is not a wrapper.
  [ -x "$shim/$t" ] || refuse "the wrapper '$t' is not executable: execvp() skips it and resolves $t to the real binary, unverified"
done
# (There is no separate `-x` check for _shim.sh: each wrapper IS a symlink to it, and `[ -x ]` follows the
# link, so the loop above already tests that inode seven times. A second check of the same thing cannot
# be made to fail on its own, which is how the sweep found it.)

# NOTHING ELSE MAY LIVE HERE: an extra executable on this PATH runs ahead of the real tools. Hidden
# entries are included -- `.kubectl` would not be found by a PATH lookup, but a dotfile is exactly where
# someone hides the thing this check is meant to notice, and listing only `*` skipped them.
shopt -s nullglob dotglob
for f in "$shim"/*; do
  b="${f##*/}"
  [ "$b" = "." ] && continue
  [ "$b" = ".." ] && continue
  [ "$b" = "_shim.sh" ] && continue
  case " $TOOLS " in *" $b "*) continue ;; esac
  refuse "unexpected entry '$b' in the shim directory — this directory is first on PATH for the whole stage"
done
shopt -u nullglob dotglob

if command -v sha256sum >/dev/null 2>&1; then
  got="$(sha256sum "$shim/_shim.sh" | cut -d' ' -f1)"
else
  got="$(shasum -a 256 "$shim/_shim.sh" | cut -d' ' -f1)"
fi
want_file="$here/effect-shim-digest.txt"
[ -f "$want_file" ] || refuse "the expected digest file is missing at $want_file"
want="$(tr -d ' \n' < "$want_file")"
[ "$got" = "$want" ] || refuse "_shim.sh is $got but this repository declares $want — the wrapper that ran was not the reviewed one"

echo "effect-shim-integrity($when): ok — 7 wrappers present, _shim.sh $got as declared"
