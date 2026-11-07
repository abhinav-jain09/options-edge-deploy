#!/usr/bin/env bash
# THE entry point for ansible/es-mirrors.yml, and the only invocation that can tell you a unit was
# installed.
#
# WHY A WRAPPER EXISTS AT ALL. ansible-playbook exits 0 for a play that matched no host and for one
# whose every task was selected away, and no task inside a playbook can defend against a flag that
# stops that task from running. Four such paths were reproduced on #1166:
#   --limit excluding the play            -> rc=0, empty recap          (Codex r3)
#   --tags <anything unused>              -> rc=0, nothing ran          (Codex r4)
#   --skip-tags all / --skip-tags always  -> rc=0, nothing ran          (Codex r4)
#   --list-hosts / --check                -> rc=0, nothing installed    (Codex r4)
# A typo'd run host is then indistinguishable from a completed install, which is the worst failure
# for a tool whose whole job is making mirror state visible. The playbook refuses --limit and tags
# its own refusals `always`, but `--skip-tags always` defeats even that. So the contract lives here:
#   1. the task-selection flags are REFUSED in argv and in the environment, before ansible runs;
#   2. a RECEIPT is required afterwards. The playbook writes it in its last task; if the play never
#      ran, or every task was skipped, there is no receipt and this script fails whatever
#      ansible-playbook's own exit code was.
# Rule 2 is the load-bearing one: it does not enumerate the ways a run can do nothing, it requires
# positive evidence that the run did something.
set -uo pipefail

PLAY=ansible/es-mirrors.yml
INV_DEFAULT=ansible/inventory/oe-mirror-hosts.yml
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$HERE" || { echo "FAIL: cannot cd to the repository root"; exit 1; }
[ -f "$PLAY" ] || { echo "FAIL: $PLAY not found from $HERE"; exit 1; }

usage() {
  cat <<'USAGE'
usage: scripts/ops/install-es-mirrors.sh [-- <extra ansible-playbook args>]
  Every argument is passed through EXCEPT the task-selection flags, which are refused.
  Common invocations:
    # plan (reads brokers, writes no unit)
    scripts/ops/install-es-mirrors.sh
    # dev install
    scripts/ops/install-es-mirrors.sh -e confirm_mirror_install=true
    # prod install (needs the permitted commit)
    scripts/ops/install-es-mirrors.sh -e mirror_target_ip=192.168.100.252 -e mirror_target_port=9092 \
        -e confirm_mirror_install=true -e permitted_sha=$(git rev-parse origin/main)
USAGE
}
[ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] && { usage; exit 0; }
[ "${1:-}" = "--" ] && shift

# ---- 1. refuse the flags that can make a run do nothing while exiting 0 ----
# Checked as whole words and as --flag=value, in argv. Short -t/-l are included: they are the short
# spellings of --tags/--limit, and nothing here needs them.
for a in "$@"; do
  case "$a" in
    --tags|--tags=*|-t|--skip-tags|--skip-tags=*|--limit|--limit=*|-l|\
    --list-hosts|--list-tasks|--list-tags|--check|-C|--start-at-task|--start-at-task=*|--step)
      echo "FAIL: $a is refused by this wrapper."
      echo "      It can make ansible-playbook exit 0 having installed nothing, which is"
      echo "      indistinguishable from a completed run. The run host is chosen with"
      echo "      -e mirror_run_host=<host>; for a dry run use the default (no"
      echo "      confirm_mirror_install), which reads brokers and writes no unit."
      exit 2;;
  esac
done
# the same selection can arrive through the environment, where argv never sees it.
# ⚠ ANSIBLE_RUN_TAGS is the one that broke the first version of this wrapper: it is the env
# spelling of --tags and my list only had ANSIBLE_TAGS, so
# `ANSIBLE_RUN_TAGS=always ... -e confirm_mirror_install=true` exited 0 with a receipt reading
# confirm=True and every count zero (Codex r5, reproduced). ANSIBLE_CONFIG is refused for the same
# reason one level up: a chosen cfg can set tags/skip_tags/limit that nothing here would see.
for v in ANSIBLE_TAGS ANSIBLE_RUN_TAGS ANSIBLE_SKIP_TAGS ANSIBLE_LIMIT ANSIBLE_CHECK_MODE \
         ANSIBLE_START_AT_TASK ANSIBLE_CONFIG ANSIBLE_PLAYBOOK_DIR ANSIBLE_STDOUT_CALLBACK; do
  if [ -n "${!v:-}" ]; then
    echo "FAIL: $v=${!v} is set in the environment; unset it. $(
      )It can select tasks or hosts away, or hide the output this wrapper reads, and the run would"
    echo "      exit 0 having installed nothing."
    exit 2
  fi
done
# and from an ansible.cfg in the directory ansible-playbook will read it from
if [ -f ansible.cfg ] && grep -qE '^[[:space:]]*(tags|skip_tags|limit|stdout_callback)[[:space:]]*=' ansible.cfg; then
  echo "FAIL: ansible.cfg sets tags/skip_tags/limit/stdout_callback. The first three can select the"
  echo "      install away silently; a non-default stdout_callback changes the output this script"
  echo "      reads back — the env refusal alone did not cover the cfg (Codex r6)."
  exit 2
fi

# ---- 2. an inventory must be in play, because the table declares a non-default run_host ----
# -i on the command line, ANSIBLE_INVENTORY, or an ansible.cfg inventory all satisfy this; only the
# "no inventory anywhere" case is refused, and it is refused here rather than discovered as a
# confusing per-row deferral.
have_inv=no
for a in "$@"; do case "$a" in -i|--inventory|--inventory-file|-i*|--inventory=*) have_inv=yes;; esac; done
[ -n "${ANSIBLE_INVENTORY:-}" ] && have_inv=yes
if [ "$have_inv" = no ] && [ -f ansible.cfg ] && grep -qE '^[[:space:]]*inventory[[:space:]]*=' ansible.cfg; then have_inv=yes; fi
INV_ARGS=()
[ "$have_inv" = no ] && INV_ARGS=(-i "$INV_DEFAULT")

# ---- 3. the receipt: positive evidence that the play ran ----
RECEIPT=$(mktemp -t oe-mirror-receipt) || { echo "FAIL: cannot create the receipt file"; exit 1; }
OUT=$(mktemp -t oe-mirror-out) || { echo "FAIL: cannot create the output file"; exit 1; }
trap 'rm -f "$RECEIPT" "$OUT"' EXIT
: > "$RECEIPT"

# ⚠ THE RECEIPT PATH GOES IN THE ENVIRONMENT, NOT -e. An extra var outranks everything, and the
# wrapper's own `-e install_receipt=` sat BEFORE the caller's arguments, so a later
# `-e install_receipt=<elsewhere>` (or `-e @file`, or `-e oe_receipt=`) won — a real install could
# mutate units, send the receipt somewhere else, and hand this script an empty file and exit 3:
# "it failed, run it again", against production (Codex r5). The playbook reads
# OE_MIRROR_RECEIPT with an inline lookup that is never bound to a variable name, so there is no
# name for an extra var to outrank.
export OE_MIRROR_RECEIPT="$RECEIPT"
# A fresh nonce per invocation, and be precise about what it buys — my first wording here was an
# overclaim. The line above sets OE_MIRROR_RECEIPT unconditionally, so a value exported in the
# caller's shell is DISCARDED for wrapper runs and the nonce has nothing to do with that case.
# What the nonce actually establishes: the file at the path this run chose was written BY this run,
# not left behind by an earlier one at a colliding path, and not pre-created by the caller. The
# shell-export workaround Codex r6 predicted is a problem for the SCRIPT-level gate (which only
# tests the variable is non-empty and cannot verify a nonce without a secret), not for this check —
# which is one more reason that gate is documented as an accident-catcher and not a boundary.
NONCE=$( (openssl rand -hex 16 2>/dev/null || od -An -tx1 -N16 /dev/urandom | tr -d ' \n') )
[ -n "$NONCE" ] || { echo "FAIL: cannot generate a run nonce"; exit 1; }
export OE_MIRROR_NONCE="$NONCE"
set +e
ansible-playbook "${INV_ARGS[@]}" "$PLAY" "$@" 2>&1 | tee "$OUT"
# ⚠ Snapshot the WHOLE array in one command. `rc=${PIPESTATUS[0]}; teerc=${PIPESTATUS[1]}` is two
# commands, and PIPESTATUS is reset by the first one — so the second read hit `set -u` and killed
# the script AFTER the playbook had already run, which is precisely the false-failure-after-a-real-
# run this check was added to avoid. Caught by my own sweep, not by reasoning.
st=("${PIPESTATUS[@]}"); rc=${st[0]:-0}; teerc=${st[1]:-0}
set -e
# tee's own failure matters: an incomplete capture makes anything parsed from $OUT unreliable, and
# reporting a parse-based failure AFTER a real mutation is the outcome worth avoiding most (r6).
if [ "$teerc" != 0 ]; then
  echo
  echo "WARNING: the output capture (tee) failed with $teerc, so the saved output may be incomplete."
  echo "         ansible-playbook itself exited $rc; trust that, not any parse of the output."
fi

# ---- 4. WHAT THIS CAN AND CANNOT PROVE -------------------------------------------------------
# ⚠ Read this before trusting anything below. THREE successive designs here claimed to prove the
# run did its work, and Codex broke each one:
#   r4: a receipt task, which `tags: always` made unskippable — so it ran when nothing else did.
#   r5: the task untagged, which fixed that, but the counts come from set_fact and `-e oe_failed=[]`
#       outranks a set_fact, so the numbers are a report and never proof.
#   r6: a grep for Ansible's own TASK banners and PLAY RECAP, on the theory that Ansible's output
#       cannot be forged from inside the play. It can: task NAMES are playbook-controlled and all
#       task output flows through the same pipe. A 12-line play with two debug/copy tasks forges
#       both banners, a recap and a plausible receipt — measured. This script's own CI stub forges
#       exactly those lines, which should have told me the theory was wrong a round earlier.
#
# So, stated plainly and not claimed away: EVERY input a check here can read — task names, extra
# vars, the environment, the stdout callback, the receipt's contents — is controlled by whoever runs
# the command. A tool cannot prove its own completion to someone able to forge its inputs. What this
# script actually does, and all it claims:
#   * it catches ACCIDENTAL no-ops — a typo'd run host, a stray --tags/--limit/--check, an exported
#     ANSIBLE_* variable, an ansible.cfg selection — which is every way this has actually gone wrong;
#   * for an INSTALL it verifies against REALITY rather than against output text, by re-planning and
#     requiring no pending unit changes (see below). That reads the files on disk and the live
#     processes, which is the only thing that was ever the point;
#   * it is NOT a security boundary and nothing here should be read as one. The permitted-commit
#     guard in ansible/templates/mirror-stop.sh.j2 and its two siblings is the boundary, it is
#     enforced inside the rendered scripts, and none of this strengthens or weakens it.
#
# The nonce below is the one thing that does get stronger: it distinguishes "the playbook's receipt
# task ran in THIS invocation" from "some OE_MIRROR_RECEIPT was already exported in the shell" —
# the workaround r6 correctly predicted someone would reach for once dev installs needed the
# wrapper. A stale export cannot carry a nonce generated seconds ago.
if ! grep -qF "nonce=$NONCE" "$RECEIPT" 2>/dev/null; then
  echo
  if [ "$rc" != 0 ]; then
    echo "no receipt for this run: ansible-playbook exited $rc — see above."
    exit "$rc"
  fi
  echo "FAIL: ansible-playbook exited 0 but wrote no receipt carrying THIS run's nonce, so this"
  echo "      invocation installed nothing. A play that matches no host, or whose tasks were all"
  echo "      selected away, exits 0 with an empty recap. If OE_MIRROR_RECEIPT is exported in your"
  echo "      shell, unset it — this script sets its own, and a stale one proves nothing."
  exit 3
fi

# ---- 5. for an INSTALL, verify against the state on disk, not against the output ---------------
# A second run with no confirm: it reads the live unit files and processes and reports, per file,
# whether installing WOULD change anything. After a successful install the answer must be "no".
# This is the only check here that cannot be satisfied by text — it is satisfied by the units
# actually being in the state the table asks for.
is_install=no
for a in "$@"; do case "$a" in confirm_mirror_install=true|confirm_mirror_install=yes) is_install=yes;; esac; done
grep -qE 'confirm_mirror_install=(true|yes)' <<<"$*" && is_install=yes
if [ "$is_install" = yes ] && [ "$rc" = 0 ]; then
  VOUT=$(mktemp -t oe-mirror-verify) || { echo "FAIL: cannot create the verify output file"; exit 1; }
  # shellcheck disable=SC2086
  set +e
  OE_MIRROR_RECEIPT="$RECEIPT" ansible-playbook "${INV_ARGS[@]}" "$PLAY" \
    $(printf '%s\n' "$@" | grep -v 'confirm_mirror_install=' | tr '\n' ' ') > "$VOUT" 2>&1
  vrc=$?
  set -e
  pending=$(grep -c 'would install' "$VOUT" || true)
  if [ "$vrc" != 0 ] || [ "${pending:-0}" -gt 0 ]; then
    echo
    echo "FAIL: the install reported success, but a verifying re-plan still finds work to do"
    echo "      (re-plan rc=$vrc, $pending unit(s) would still change). The units on disk are NOT"
    echo "      in the state the table asks for. Output: $VOUT"
    rm -f "$VOUT"
    exit 6
  fi
  rm -f "$VOUT"
  echo "verified: a re-plan finds no pending unit changes"
fi

echo
echo "receipt:"
sed 's/^/  /' "$RECEIPT"
# Ansible's own recap, for the reader — NOT as evidence. The check that used to parse this was
# removed because it proved nothing a play could not print itself (Codex r6).
echo "ansible recap: $(grep -E '^[A-Za-z0-9_.-]+[[:space:]]+:[[:space:]]+ok=' "$OUT" | tail -1)"
[ "$rc" != 0 ] && echo "ansible-playbook exited $rc — see the Summary above for the unit that refused"
exit "$rc"
