#!/bin/bash
# THE SHIM'S INTEGRITY, CHECKED FROM OUTSIDE ITSELF.
#
# WHAT THIS CATCHES IS AN ACCIDENT, AND ONLY AN ACCIDENT. A wrapper deleted, a permission dropped, an
# entry added, a file edited by something that did not also edit the digest -- those it names. IT DOES
# NOT WITHSTAND A JOB THAT EDITS THE CHECKER OR THE DIGEST, AND IT NEVER CAN WHILE BOTH LIVE IN THE
# WORKSPACE THE JOB OWNS. THE SHIM'S OWN INTEGRITY IS A LIMIT, NOT SOMETHING THIS PREVENTS OR DETECTS.
#
# THIS SCRIPT, effect-shim-digest.txt AND _shim.sh ARE ALL IN THE CHECKOUT. A step that edits _shim.sh
# and re-records the digest, or that edits this file, passes both inspections with an unverified wrapper
# in place -- an edited wrapper can run a `kubectl apply` against a tree nobody proved while both
# inspections print ok. Two cases in effect-shim-test.sh do exactly that and assert the green result, so
# the limit is pinned by a test rather than by this paragraph. A word like PREVENTION or DETECTION
# attached to the shim's own integrity was here and has been removed, because at 06:30 in front of a
# red build a responder reads a green integrity line as evidence the wrapper was intact, and it is not
# that evidence. Mutable code cannot establish its own integrity by checking itself (see
# effect-shim/_shim.sh for the alternatives considered and why each fails).
#
# WHAT THE TWO INSPECTIONS DO DIFFER IN is WHEN they run, and that is about ordering, not about strength:
# the one at the START of a guarded stage refuses before the stage's steps run, so an accident caught
# there costs nothing; the one at the END refuses after them, and says so -- THE BUILD FAILS AND NAMES
# WHAT IT FOUND, AND THE STAGE'S EFFECTS HAVE ALREADY HAPPENED. What it does NOT say is that those
# effects were unverified: an effect that passed through a wrapper was verified AT THE MOMENT IT RAN,
# and drift found afterwards does not reach back. What the end check cannot do is attest that EVERY
# effect in the stage was covered. If one was a `kubectl apply`, something in the cluster changed
# either way. Neither ordering makes an effect safe, and neither un-verifies one.
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
    echo "effect-shim-integrity(end): it cannot attest COVERAGE FOR ALL of them — not that any one was" >&2
    echo "effect-shim-integrity(end): unverified, and not to undo them. An effect that went through a" >&2
    echo "effect-shim-integrity(end): wrapper was verified when it ran. If one was a deployment, the" >&2
    echo "effect-shim-integrity(end): cluster changed. Check what this build applied before re-running." >&2
  fi
  echo "effect-shim-integrity($when): verdict=REFUSED" >&2
  exit 3
}

[ -d "$shim" ] || refuse "the shim directory is missing at $shim — nothing was intercepting anything"

for t in $TOOLS; do
  [ -e "$shim/$t" ] || refuse "the wrapper '$t' is missing, so coverage BY THIS CHECKOUT'S SHIM cannot be attested for $t in this stage. This does not say an invocation was unverified: a wrapper cannot detect its own absence, and PATH may have held another shim copy ahead of this directory"
  link="$(readlink "$shim/$t" 2>/dev/null || true)"
  [ "$link" = "_shim.sh" ] || refuse "the wrapper '$t' is not the expected symlink to _shim.sh (found: '${link:-a regular file}')"
  # EXECUTABILITY IS PART OF EXISTING. `chmod 644 _shim.sh` leaves the bytes, the symlinks and the digest
  # untouched and every other check in this file green, while execvp() skips the wrapper and runs the
  # real binary unverified. A file the kernel will not execute is not a wrapper.
  [ -x "$shim/$t" ] || refuse "the wrapper '$t' is not executable, so execvp() skips it and coverage BY THIS CHECKOUT'S SHIM cannot be attested for $t. A file the kernel will not execute is not a wrapper; what a given invocation actually resolved to is not observable from here"
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

# The success line says what it checked, not that the shim was intact: this script and the digest it
# compares against are both editable by the job, so "ok" means nothing here disagreed -- not that
# nothing was tampered with.
echo "effect-shim-integrity($when): ok — 7 wrappers present, _shim.sh $got as declared (this checks for an ACCIDENT; it cannot outrank a step that edits this script or the digest)"
