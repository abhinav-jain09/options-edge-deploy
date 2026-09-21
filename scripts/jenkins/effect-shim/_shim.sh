#!/bin/bash
# An ABSOLUTE interpreter path, deliberately: this script runs with a PATH the job controls, and
# `#!/usr/bin/env bash` needs `env` AND `bash` to be findable on it. A shim that cannot start records
# nothing and refuses nothing -- the failure mode a sibling suite hit in review, one layer in.
#
# ADJACENCY AS A RUNTIME PROPERTY. One implementation; the six names beside this file are symlinks to it,
# and each reads the tool it stands for from its own invoked name.
#
# WHY THIS EXISTS. The guard proves the WORKSPACE is the permitted commit. Proving that a verification
# runs immediately BEFORE an effect was attempted as a static reader of Jenkinsfile text, and that reader
# was beaten on a new axis in each of six review rounds by valid input: a trailing `; true`, a
# double-quoted command, `-f ./pom.xml` naming the same project another way, a closure parameter hiding a
# brace-less `if`, a locally defined `timeout` swallowing the verification's failure. None of those was
# the defect. The defect is that deciding from TEXT what a command DOES is not decidable, so each fix
# narrowed one spelling while the space stayed infinite.
#
# So the proof stops being about text. A stage prepends this directory to PATH; every effect binary
# resolved BY NAME lands here first; this script verifies the checkout it is about to act on and then
# EXECs the real binary, or refuses. Spelling, quoting, escapes, variables, stage boundaries, `; true`,
# `-f .` and shared-library calls all stop mattering, because whatever reaches `mvn` reaches this.
#
# WHAT THIS IS NOT: A SECURITY BOUNDARY AGAINST THE JOB DEFINITION.
#
# THE JOB OWNS ITS OWN PROCESS. That one sentence is why every candidate boundary fails, and it fails for
# the same reason each time rather than for four different reasons. A wrapper that lives in the workspace
# can be deleted by a step in the workspace; a wrapper cannot detect its own absence; and mutable code
# cannot establish its own integrity by voluntarily checking itself. What this mechanism closes is the
# class that actually defeated four rounds of review -- SPELLING -- not a job definition that sets out to
# get around it.
#
# ALTERNATIVES CONSIDERED AND WHY EACH FAILS (a list of what does not work is more use to the next person
# than a list of what does):
#   * ROOT-OWNED WRAPPERS on the agent, prepended by Jenkins node configuration: survives `rm` from a
#     step, but the job sets its own PATH -- `withEnv` is how it already does that -- and an absolute
#     path reaches the real binary regardless.
#   * RENAMING THE REAL BINARIES so the only `mvn` on the box is the wrapper: the first thing that
#     actually closes name-based bypass at the agent level, but it loses to a job that brings its own
#     toolchain (this gateway already resolves its own JDK, and a Maven wrapper in the repository is one
#     commit away), and it changes behaviour for every job on that agent, including ones nobody in this
#     review owns.
#   * A READ-ONLY CHECKOUT: incompatible with building in it.
#   * RUNNING EFFECTS IN A NAMESPACE THE JOB CANNOT INFLUENCE: that is owning the agent image, which is a
#     different project with a different owner.
#
# IF WE EVER OWN THE AGENT IMAGE, the combination worth building is: renamed real binaries, plus
# root-owned wrappers at the canonical paths, plus a policy that jobs do not supply their own toolchain.
# That is the shape of the real fix and what it would cost -- not an argument that this one is equivalent.
#
# WHAT IT DOES DELIVER, exactly: AN INVOCATION BY NAME, THROUGH AN INTACT SHIM, IN A STAGE THAT INSTALLED
# IT, IS VERIFIED AT THE MOMENT IT ACTS. That covers `mvn -B test`, `"mvn -B test"`, `mvn -f ./pom.xml
# test`, a trailing `; true`, a second stage, a variable holding the command and a shared-library call,
# because none of them changes what execvp() looks up. Descendants are covered too: the real binary
# inherits the shim on PATH, so a Maven plugin that runs `kubectl` is verified again (removing the shim
# from the CHILD's PATH was a bug, not a trade -- stripping is only needed to RESOLVE the real binary).
#
# WHAT IT DOES NOT COVER. Each of these is pinned by a case in effect-shim-test.sh asserting the
# DOCUMENTED outcome, so it is a limit rather than a sentence someone will delete:
#   * A DELETED WRAPPER. Remove effect-shim/kubectl after the verification and `kubectl` resolves to the
#     real binary: status 0, unverified. A wrapper cannot detect its own absence.
#   * AN EDITED _shim.sh. Change this file to skip its verification and it prints `verified` and runs the
#     tool. Mutable code cannot establish its own integrity by checking itself.
#   * A DIFFERENT WRAPPER VERSION reached on a PATH outside the verified checkout: no version comparison
#     is performed, and NOTHING DETECTS IT EITHER. The integrity checks inspect the shim directory INSIDE
#     the checkout; a copy somewhere else is not looked at, and a PATH pointing at it leaves both checks
#     green. Said plainly because the previous wording claimed detection covered this and it does not.
#
#   NONE OF THESE THREE IS DETECTED, AND THE WORD THAT USED TO BE HERE CLAIMED OTHERWISE. The guarded
#   stage runs effect-shim-integrity.sh over THIS directory at its start and again at its end, and what
#   that catches is an ACCIDENT: a wrapper deleted, a permission dropped, an entry added, a file edited
#   by something that did not also edit the digest. IT DOES NOT WITHSTAND A JOB THAT EDITS THE CHECKER
#   OR THE DIGEST, AND IT NEVER CAN WHILE BOTH LIVE IN THE WORKSPACE THE JOB OWNS -- edit this file and
#   re-record effect-shim-digest.txt, or edit effect-shim-integrity.sh itself, and an unverified
#   `kubectl apply` runs while both inspections print ok. THE SHIM'S OWN INTEGRITY IS A LIMIT. Where the
#   two inspections differ is WHEN they run, which is about ordering and not about strength: the end one
#   runs unconditionally, including when the effect itself failed, and when it does refuse the build
#   fails and names what it found while the stage's effects have already happened. It does NOT mean an
#   effect ran unverified -- one that reached a wrapper was verified at that moment -- only that
#   coverage cannot be attested for every effect in the stage.
#   * AN ABSOLUTE PATH performs no PATH lookup, so nothing reaches this script. validate-jenkinsfile-guard
#     rule 14 refuses a LITERAL `/<tool>` in a guarded definition; an absolute path ASSEMBLED FROM QUOTED
#     FRAGMENTS (`sh '"/usr/bin/"kubectl version'`) contains no such literal and is outside both. Chasing
#     that through Groovy quoting is the same undecidable game one layer over, and it is not attempted.
#   * INSIDE A CONTAINER. `docker run`/`docker buildx build` are verified HERE, at launch: the tree the
#     build context and any mount are taken from is proven at that moment. A process started inside the
#     container does not inherit this PATH and is not shimmed, so a READ-WRITE mount of the checkout can
#     change it afterwards. Each subsequent `docker` invocation re-proves the tree.
#   * A BINARY INVOKED WITHOUT A PATH LOOKUP -- a wrapper script re-execing an absolute path, or a
#     language runtime's own process API given one.
#   * WHAT THE TOOL DOES AFTER IT STARTS. This proves the tree at the moment of invocation, nothing about
#     the seconds that follow. A long build is not re-verified mid-flight.
#
# CONFIGURATION, and every one of these is REQUIRED (an unset variable is a refusal, never a default):
#   OE_SHIM_DIR      the checkout to verify — the one this invocation acts on
#   OE_SHIM_SHA      the permission the checkout must equal (the job's PERMITTED_SHA for that source)
#   OE_SHIM_ALLOW    the --allow-ignored declarations for that checkout, space-separated; may be empty
#                    but must be SET, so that "this job owns no output" is a statement someone made
#                    rather than a variable nobody exported
set -u

tool="${0##*/}"
say() { echo "effect-shim($tool): $*" >&2; }

refuse() {
  say "REFUSED — $*"
  say "verdict=REFUSED (the command was NOT run)"
  exit 3
}

# A signal is a refusal, for the same reason it is in the guard: an interrupted run must not look like a
# verification that passed. These are installed before anything else can be interrupted by them.
on_signal() {
  say "REFUSED — interrupted by SIG$1 before the checkout was proven; the command was NOT run"
  say "verdict=REFUSED (the command was NOT run)"
  exit 3
}
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM
trap 'on_signal HUP' HUP
trap 'on_signal QUIT' QUIT

# Its own directory, computed with SHELL EXPANSION rather than `dirname`: this script runs under a PATH
# the job controls, and a shim that needs an external command to work out where it is fails under a
# minimal PATH -- which the suite's own control demonstrated before this line was written this way.
self="${BASH_SOURCE[0]}"
case "$self" in
  */*) here="$(cd "${self%/*}" && pwd -P)" ;;
  *)   here="$(pwd -P)" ;;
esac

[ -n "${OE_SHIM_DIR+set}" ]   || refuse "OE_SHIM_DIR is not set: the stage installed this shim without saying which checkout to verify"
[ -n "${OE_SHIM_SHA+set}" ]   || refuse "OE_SHIM_SHA is not set: there is no permission to verify the checkout against"
[ -n "${OE_SHIM_ALLOW+set}" ] || refuse "OE_SHIM_ALLOW is not set: declare the job's own outputs explicitly, or set it empty to declare none"
[ -n "${OE_SHIM_DIR}" ]       || refuse "OE_SHIM_DIR is empty"
[ -n "${OE_SHIM_SHA}" ]       || refuse "OE_SHIM_SHA is empty: this job has no permission for that checkout"

verifier="$here/../verify-permitted-tree.sh"
[ -x "$verifier" ] || [ -f "$verifier" ] || refuse "the verifier is missing at $verifier"

# THE VERIFICATION, on every invocation. `git status --porcelain` over the largest of these repositories
# (3018 tracked files) costs 0.19 s cold and 0.02-0.04 s warm — nothing beside a Maven build or an image
# push, and it is the entire point that it happens EVERY time rather than once per stage: a per-stage
# cache would reopen exactly the window this replaces.
# THE DECLARATIONS ARE SPLIT ON WHITESPACE WITH PATHNAME EXPANSION OFF. `*/target` is a VALID
# declaration -- the verifier's own grammar allows `*` as a path component -- and an unquoted expansion
# turned it into the directories that happen to exist right now: `--allow-ignored a/target b/target`,
# which the verifier then refused as an unknown argument. A clean, correctly-declared tree was refused.
# `set -f` is the whole fix; `eval` is not used here under any circumstances.
# (`allow=()` first, and the `${allow[@]+…}` form below: under `set -u` an empty array expansion is an
# unbound-variable error in bash 3.2, which is what the agents run.)
allow=()
set -f
if [ -n "$OE_SHIM_ALLOW" ]; then
  # shellcheck disable=SC2206
  allow=($OE_SHIM_ALLOW)
fi
set +f
if ! PERMITTED_SHA="$OE_SHIM_SHA" bash "$verifier" --dir "$OE_SHIM_DIR" ${allow[@]+"${allow[@]}"} >/dev/null 2>&1; then
  # run it again with output so the log says WHY, then refuse
  PERMITTED_SHA="$OE_SHIM_SHA" bash "$verifier" --dir "$OE_SHIM_DIR" ${allow[@]+"${allow[@]}"} >&2 || true
  refuse "the checkout at '$OE_SHIM_DIR' is not the permitted tree $OE_SHIM_SHA; $tool was not run"
fi

# THE REAL BINARY: this directory removed from PATH so the lookup cannot find this script again. Removed
# by exact element, not by pattern, and a PATH that still resolves $tool to this file is a refusal rather
# than an exec loop.
# Elements are compared by their CANONICAL path, not as strings: on macOS a PATH element under /var and
# this script's own $(pwd -P) under /private/var are the same directory spelled two ways, and a string
# comparison leaves the shim on PATH -- which resolves $tool back to this file.
stripped=""
IFS=':' read -r -a parts <<< "${PATH:-}"
for p in "${parts[@]}"; do
  [ -n "$p" ] || continue
  canon="$(cd "$p" 2>/dev/null && pwd -P || printf '%s' "$p")"
  [ "$canon" = "$here" ] && continue
  case "$canon" in "$here"/*) continue ;; esac
  stripped="${stripped:+$stripped:}$p"
done
real="$(PATH="$stripped" command -v "$tool" 2>/dev/null || true)"
[ -n "$real" ] || refuse "$tool is not on PATH outside this shim directory"
# There was a second check here -- "the resolution found the shim again" -- and the sweep could not reach
# it from any input, because the stripping above removes every element whose CANONICAL path is $here or
# below it, and a lookup among the remaining elements cannot land in a directory that is not among them.
# A branch no input can reach is not a protection; it is untested code that reads like one.

# THE ENTRY THIS SHIM WAS FOUND THROUGH IS MADE ABSOLUTE BEFORE THE CHILD INHERITS IT. A relative
# entry -- `PATH=scripts/jenkins/effect-shim:...`, which is how a Jenkinsfile writes it without
# thinking -- resolves against the CURRENT DIRECTORY, so a verified child that does `cd /tmp` and
# then runs `kubectl` looks up `/tmp/scripts/jenkins/effect-shim/kubectl`, does not find it, and
# reaches the real binary unverified. Review reproduced exactly that. Rewriting the element to the
# absolute directory this script is in makes the interception survive any cwd the child chooses.
abs_path=""
_oldifs="${IFS-}"
IFS=:
for _e in $PATH; do
    case "$_e" in
        /*|"") _keep="$_e" ;;
        *)     _c="$(cd "$_e" 2>/dev/null && pwd -P || true)"
               if [ -n "$_c" ] && [ "$_c" = "$here" ]; then _keep="$here"; else _keep="$_e"; fi ;;
    esac
    abs_path="${abs_path:+$abs_path:}$_keep"
done
IFS="$_oldifs"
[ -n "$abs_path" ] && PATH="$abs_path" && export PATH

# THE CHILD INHERITS THE ORIGINAL PATH, shim first. Stripping is needed only to RESOLVE the real binary;
# handing the stripped PATH to the child removed interception from everything the tool then starts, and
# that is how a Maven plugin reached `kubectl` unverified in review. It is not a trade, it was a bug:
# a descendant that looks up one of these names must be verified like any other invocation. There is no
# loop, because what is exec'd here is always the REAL binary, never this script.
say "verified $OE_SHIM_DIR == $OE_SHIM_SHA; running $real"
exec "$real" "$@"
