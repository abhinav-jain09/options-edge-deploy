#!/usr/bin/env bash
# The Deployment Permission Rule (options-edge rule.md) is enforced per Jenkins job. ansible/es-mirrors.yml
# can reload a PRODUCTION unit from any checkout, so it has to enforce the same rule itself — and
# "it has a task that runs the guard" is not the same as "the rule holds". Three things are asserted:
#
#   1. the guard version the playbook DECLARES is the one every mirror Jenkinsfile declares. It must be
#      a literal: computing it from the checkout would let a locally edited guard self-approve, which
#      is exactly what the guard's version check exists to stop.
#   2. the task runs only for the production target and only for an install, and its failure is fatal.
#   3. every task that stops, writes or starts a unit ALSO re-checks that the guard passed. One fatal
#      assertion is not enough: while testing this gate I made the guard task non-fatal as a mutation
#      and the run installed and STARTED three production units. The fact is the second lock.
#   4. behaviourally: a production install from THIS checkout is REFUSED, and refused BY THE GUARD.
#      On a PR branch (CI) the refusal is "not on origin/main"; on main with a wrong permitted_sha it
#      is the SHA mismatch. The run is driven in DUMP mode, which ends the play before any unit task,
#      so this gate cannot itself touch a unit even if every other assertion here were wrong.
set -euo pipefail
cd "$(dirname "$0")/../.."
fail=0
PB=ansible/es-mirrors.yml

# ---- 1. the declared guard version ----
declared=$(grep -oE 'oe_guard_version: "[0-9a-f]{64}"' "$PB" | head -1 | grep -oE '[0-9a-f]{64}' || true)
[ -n "$declared" ] || { echo "FAIL: $PB does not declare oe_guard_version as a 64-hex literal"; fail=1; }
for jf in Jenkinsfile.es-cvd-mirror Jenkinsfile.es-indicator-mirror Jenkinsfile.es-auction-mirror \
          Jenkinsfile.es-futures-flow-mirror Jenkinsfile.es-strike-intel-mirror Jenkinsfile.es-tape-zones-mirror; do
  j=$(grep -A1 "name: 'PERMITTED_SHA_GUARD_VERSION'" "$jf" | grep -oE "defaultValue: '[0-9a-f]{64}'" | grep -oE '[0-9a-f]{64}' | head -1 || true)
  [ -n "$j" ] || { echo "FAIL: $jf does not declare a PERMITTED_SHA_GUARD_VERSION default"; fail=1; continue; }
  [ "$j" = "$declared" ] || { echo "FAIL: $jf declares guard version $j but $PB declares $declared"; fail=1; }
done
grep -q 'PERMITTED_SHA_GUARD_VERSION="{{ oe_guard_version }}"' "$PB" \
  || { echo "FAIL: $PB must pass the DECLARED version to the guard — a computed one lets an edited guard self-approve"; fail=1; }
grep -qE 'PERMITTED_SHA_GUARD_VERSION="\$\(' "$PB" \
  && { echo "FAIL: $PB computes the guard version from the checkout"; fail=1; }

# ---- 2. the task's own conditions ----
block=$(awk '/name: Enforce the permitted commit before any PRODUCTION effect/{f=1} f{print} f&&/failed_when: oe_guard.rc != 0/{exit}' "$PB")
[ -n "$block" ] || { echo "FAIL: $PB has no permitted-commit task ending in a fatal failed_when"; fail=1; }
printf '%s\n' "$block" | grep -qF "when: oe_confirm and (oe_target == '192.168.100.252:9092')" \
  || { echo "FAIL: the permitted-commit task must run for a production INSTALL, decided against the literal broker"; fail=1; }
printf '%s\n' "$block" | grep -q 'bash scripts/jenkins/permitted-sha-guard.sh' \
  || { echo "FAIL: the permitted-commit task must run the repository's own guard script"; fail=1; }

# ---- 2b. the production decision is taken against LITERALS ----
# An Ansible extra var outranks every var, fact and register in a play, so a decision taken through a
# NAME can be redefined on the command line: with `-e oe_prod_target=127.0.0.1:19092` the real
# production broker would pass the allow-list, skip the guard and set the permission true. The three
# decisions are therefore written against the literal broker, and that is asserted here — and then
# behaviourally, by running with exactly that override.
while IFS= read -r pat; do
  [ -n "$pat" ] || continue
  grep -qF -- "$pat" "$PB" || { echo "FAIL: $PB must take the production decision against the literal broker, not a redefinable name (missing: $pat)"; fail=1; }
done <<PATS
oe_target in ['127.0.0.1:19092', '192.168.100.252:9092']
when: oe_confirm and (oe_target == '192.168.100.252:9092')
oe_prod_permitted: "{{ (oe_target != '192.168.100.252:9092')
PATS

# ---- 3. every mutating task re-checks the guard's verdict ----
grep -qF "oe_prod_permitted: \"{{ (oe_target != '192.168.100.252:9092') or ((oe_guard.rc | default(1)) == 0) }}\"" "$PB" \
  || { echo "FAIL: $PB must publish oe_prod_permitted from the guard's verdict, against the literal broker"; fail=1; }
for t in "STOP the unit before anything is written" "install the rendered files" "START the unit on the installed files"; do
  blk=$(awk -v n="$t" 'index($0, n){f=1} f{print} f&&/ansible\.builtin\.(command|shell|copy|template)/{exit}' ansible/tasks/es-mirror-unit.yml)
  printf '%s\n' "$blk" | grep -q 'oe_prod_permitted' \
    || { echo "FAIL: the task '$t' does not re-check oe_prod_permitted — a non-fatal guard would let it run on production"; fail=1; }
done
# and the stop must come BEFORE anything is written: a copy ahead of it would replace files under a
# live process, which is the whole reason the install is split into two phases
# Each name's own line, not grep's file order: a combined `grep -n A\|B\|C | cut` returns matches in
# FILE order, so "three increasing numbers" is true however the tasks are arranged — it passed with
# the install moved ahead of the stop, which is the one arrangement this check exists to forbid.
lineof() { grep -n -F -- "$1" ansible/tasks/es-mirror-unit.yml | head -1 | cut -d: -f1; }
l_stop=$(lineof 'STOP the unit before anything is written')
l_inst=$(lineof 'install the rendered files')
l_start=$(lineof 'START the unit on the installed files')
if [ -z "$l_stop" ] || [ -z "$l_inst" ] || [ -z "$l_start" ]; then
  echo "FAIL: cannot find the stop/install/start tasks in ansible/tasks/es-mirror-unit.yml"; fail=1
elif [ "$l_stop" -ge "$l_inst" ] || [ "$l_inst" -ge "$l_start" ]; then
  echo "FAIL: the unit tasks must run stop ($l_stop) then install ($l_inst) then start ($l_start) — a copy ahead of the stop replaces files under a LIVE process, which is why the install is two phases"
  fail=1
fi

# ---- 4. the behaviour: a production install from this checkout is refused BY THE GUARD ----
if command -v ansible-playbook >/dev/null; then
  log=$(mktemp); OUT_DUMP=$(mktemp); set +e
  # dump_rows ends the play before any unit task, so this gate cannot install anything
  ansible-playbook "$PB" -e mirror_target_ip=192.168.100.252 -e mirror_target_port=9092 \
    -e confirm_mirror_install=true -e permitted_sha=0000000000000000000000000000000000000000 \
    -e "dump_rows=$OUT_DUMP" >"$log" 2>&1
  rc=$?; set -e
  [ "$rc" != 0 ] || { echo "FAIL: a production install with a bogus permitted_sha exited 0"; fail=1; }
  grep -q 'permitted-sha-guard: REFUSED' "$log" \
    || { echo "FAIL: the refusal did not come from the guard — see $log"; sed -n '1,25p' "$log"; fail=1; }
  grep -qE 'STOP the unit|install the rendered files|START the unit' "$log" \
    && { echo "FAIL: a refused production run still reached a unit task"; fail=1; }
  # the same run with the classification RENAMED: the literals must make this change nothing
  log2=$(mktemp); set +e
  ansible-playbook "$PB" -e mirror_target_ip=192.168.100.252 -e mirror_target_port=9092 \
    -e confirm_mirror_install=true -e permitted_sha=0000000000000000000000000000000000000000 \
    -e oe_prod_target=127.0.0.1:19092 -e oe_dev_target=192.168.100.252:9092 \
    -e "dump_rows=$OUT_DUMP" >"$log2" 2>&1
  rc2=$?; set -e
  [ "$rc2" != 0 ] || { echo "FAIL: a production install survived -e oe_prod_target/-e oe_dev_target — the decision is being taken through a redefinable name"; fail=1; }
  grep -q 'permitted-sha-guard: REFUSED' "$log2" \
    || { echo "FAIL: with the classification renamed, the refusal no longer comes from the guard — see $log2"; fail=1; }
  rm -f "$log" "$log2" "$OUT_DUMP"
else
  echo "FAIL: ansible-playbook is required to test the production permission path"; fail=1
fi

[ "$fail" = 0 ] || { echo "=== validate-ansible-prod-permission: FAILED ==="; exit 1; }
echo "=== validate-ansible-prod-permission: OK === the declared guard version matches all six mirror pipelines; the decision is taken against the literal production broker and survives -e renaming it; the task is production+install only and fatal; every mutating task re-checks the verdict; the order is stop, install, start; and a production install from this checkout is refused by the guard before any unit task"
