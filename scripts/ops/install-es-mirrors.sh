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
# the same flags can arrive through the environment, where argv never sees them
for v in ANSIBLE_TAGS ANSIBLE_SKIP_TAGS ANSIBLE_LIMIT ANSIBLE_CHECK_MODE ANSIBLE_START_AT_TASK; do
  if [ -n "${!v:-}" ]; then
    echo "FAIL: $v=${!v} is set in the environment; unset it. $(
      )It selects tasks or hosts away and the run would exit 0 having installed nothing."
    exit 2
  fi
done

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
trap 'rm -f "$RECEIPT"' EXIT
: > "$RECEIPT"

set +e
ansible-playbook "${INV_ARGS[@]}" "$PLAY" -e "install_receipt=$RECEIPT" "$@"
rc=$?
set -e

if [ ! -s "$RECEIPT" ]; then
  echo
  if [ "$rc" != 0 ]; then
    # The playbook refused, loudly, and said why above. Its exit code is the useful one; the missing
    # receipt is just the consequence, so do not overwrite a diagnosed failure with exit 3.
    echo "no receipt: ansible-playbook exited $rc and refused before its last task — see above."
    exit "$rc"
  fi
  # THE CASE THIS WRAPPER EXISTS FOR: a SUCCESSFUL exit with no evidence of work.
  echo "FAIL: ansible-playbook exited 0 but produced NO RECEIPT, so this run installed nothing."
  echo "      A play that matches no host, or whose tasks were all selected away, exits 0 with an"
  echo "      empty recap — indistinguishable from a completed install without this check."
  echo "      Check the run host: -e mirror_run_host=<host> must name a host in the inventory."
  exit 3
fi

echo
echo "receipt:"
sed 's/^/  /' "$RECEIPT"
[ "$rc" != 0 ] && echo "ansible-playbook exited $rc — see the Summary above for the unit that refused"
exit "$rc"
