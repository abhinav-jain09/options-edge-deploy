#!/usr/bin/env bash
# The Deployment Permission Rule (options-edge rule.md) is enforced per Jenkins job. ansible/es-mirrors.yml
# can reload a PRODUCTION unit from any checkout, so it has to enforce the same rule itself — and
# "it has a task that runs the guard" is not the same as "the rule holds". What is asserted, in the
# order the checks appear below (the numbering is the code's, not a count of claims):
#
#   1. the guard version is a LITERAL at every invocation — in the playbook's task and in the stop
#      script — and the same one every mirror Jenkinsfile declares. As a variable, `-e
#      oe_guard_version=<hash of an edited guard>` would self-approve a modified guard.
#   2. the task runs only for the production target and only for an install, and its failure is fatal.
#   3. every task that stops, writes or starts a unit ALSO re-checks that the guard passed. One fatal
#      assertion is not enough: while testing this gate I made the guard task non-fatal as a mutation
#      and the run installed and STARTED three production units. The fact is the second lock.
#   4. the BINDING lock is inside ansible/templates/mirror-stop.sh.j2 — the first thing that touches
#      a unit — gating on the literal production broker and running the guard BEFORE it unloads
#      anything. The play-level checks are advisory: an extra var outranks every var, fact and
#      register, so `-e oe_prod_permitted=true` defeated them.
#   5. the unit tasks run in the order stop, install, start: a copy ahead of the stop would replace
#      files under a LIVE process.
#   6. behaviourally: a production install from THIS checkout is REFUSED, and refused BY THE GUARD.
#      On a PR branch (CI) the refusal is "not on origin/main"; on main with a wrong permitted_sha it
#      is the SHA mismatch. The run is driven in DUMP mode, which ends the play before any unit task,
#      so this gate cannot itself touch a unit even if every other assertion here were wrong.
set -euo pipefail
cd "$(dirname "$0")/../.."
fail=0
PB=ansible/es-mirrors.yml
# -i is REQUIRED: the table declares run_host mac74, which the playbook checks against the inventory,
# so an inventory-less run is refused by that row check BEFORE the permitted-commit guard — which is
# exactly what this gate asserts the refusal comes from. Without -i the gate failed while the lock it
# tests was intact (Codex r3 on #1166). Nothing here connects to mac74.
INV=ansible/inventory/oe-mirror-hosts.yml
# A production install now has to come through scripts/ops/install-es-mirrors.sh, and that refusal
# sits IN FRONT of the permitted-commit guard — which is what this gate asserts the refusal comes
# from. So the gate presents itself as a wrapper run. It is not weakening anything: the guard is
# still the thing under test and still refuses, and the wrapper requirement has its own gate in
# scripts/ci/mirror-install-wrapper-test.sh.
export OE_MIRROR_RECEIPT=/tmp/oe-prod-permission-gate-receipt

# ---- 1. the declared guard version ----
# the hash must be a LITERAL at every invocation, in the playbook and in the binding script: as a
# variable, `-e oe_guard_version=<hash of an edited guard>` would self-approve a modified guard
STOP_TPL=ansible/templates/mirror-stop.sh.j2
declared=""
for f in "$PB" "$STOP_TPL"; do
  h=$(grep -oE 'PERMITTED_SHA_GUARD_VERSION="[0-9a-f]{64}"' "$f" | head -1 | grep -oE '[0-9a-f]{64}' || true)
  [ -n "$h" ] || { echo "FAIL: $f must pass PERMITTED_SHA_GUARD_VERSION as a 64-hex LITERAL, not a variable"; fail=1; continue; }
  grep -qE 'PERMITTED_SHA_GUARD_VERSION="\{\{|PERMITTED_SHA_GUARD_VERSION="\$' "$f" \
    && { echo "FAIL: $f still takes the guard version from a variable or a command"; fail=1; }
  case "$declared" in "") declared="$h" ;; *) [ "$h" = "$declared" ] || { echo "FAIL: $f declares guard version $h, $PB declares $declared"; fail=1; } ;; esac
done
for jf in Jenkinsfile.es-cvd-mirror Jenkinsfile.es-indicator-mirror Jenkinsfile.es-auction-mirror \
          Jenkinsfile.es-futures-flow-mirror Jenkinsfile.es-strike-intel-mirror Jenkinsfile.es-tape-zones-mirror; do
  j=$(grep -A1 "name: 'PERMITTED_SHA_GUARD_VERSION'" "$jf" | grep -oE "defaultValue: '[0-9a-f]{64}'" | grep -oE '[0-9a-f]{64}' | head -1 || true)
  [ -n "$j" ] || { echo "FAIL: $jf does not declare a PERMITTED_SHA_GUARD_VERSION default"; fail=1; continue; }
  [ "$j" = "$declared" ] || { echo "FAIL: $jf declares guard version $j but $PB declares $declared"; fail=1; }
done
grep -qE 'PERMITTED_SHA_GUARD_VERSION="\$\(' "$PB" \
  && { echo "FAIL: $PB computes the guard version from the checkout"; fail=1; }

# ---- 2. the task's own conditions ----
block=$(awk '/name: Enforce the permitted commit before any PRODUCTION effect/{f=1} f{print} f&&/failed_when: oe_guard.rc != 0/{exit}' "$PB")
[ -n "$block" ] || { echo "FAIL: $PB has no permitted-commit task ending in a fatal failed_when"; fail=1; }
printf '%s\n' "$block" | grep -qF "when: oe_confirm and (oe_target_ip == '192.168.100.252') and (oe_target_port == '9092')" \
  || { echo "FAIL: the permitted-commit task must run for a production INSTALL, decided against the literal broker"; fail=1; }
printf '%s\n' "$block" | grep -q 'bash scripts/jenkins/permitted-sha-guard.sh' \
  || { echo "FAIL: the permitted-commit task must run the repository's own guard script"; fail=1; }

# ---- 2b. the production decision is taken against LITERALS ----
# An Ansible extra var outranks every var, fact and register in a play, so a decision taken through a
# NAME can be redefined on the command line: with `-e oe_prod_target=127.0.0.1:19092` the real
# production broker would pass the allow-list, skip the guard and set the permission true. The three
# decisions are therefore written against the literal broker, and that is asserted here — and then
# behaviourally, by running with exactly that override.
# and the decisions must read the INPUT variables the unit paths and group ids are built from, not a
# derived one: `-e oe_target=127.0.0.1:19092` would otherwise skip the guard while every path and
# group stayed production. Overriding oe_target_ip/oe_target_port changes the target consistently.
while IFS= read -r pat; do
  [ -n "$pat" ] || continue
  grep -qF -- "$pat" "$PB" || { echo "FAIL: $PB must take the production decision against the literal broker, not a redefinable name (missing: $pat)"; fail=1; }
done <<PATS
(oe_target_ip ~ ':' ~ oe_target_port) in ['127.0.0.1:19092', '192.168.100.252:9092']
when: oe_confirm and (oe_target_ip == '192.168.100.252') and (oe_target_port == '9092')
oe_prod_permitted: "{{ ((oe_target_ip ~ ':' ~ oe_target_port) != '192.168.100.252:9092')
target: "{{ oe_target_ip }}:{{ oe_target_port }}"
PATS

# ---- 2c. the BINDING lock is in EVERY path that touches a unit, not in a variable ----
# An extra var outranks every var, fact and register, so the play-level conditions are advisory:
# `-e oe_prod_permitted=true` defeated them. The lock that cannot be waved through is the one inside
# the first script that touches a unit.
# stop, WRITE and start: an extra var outranks every var, fact and register, so spoofing oe_stop.rc
# made the write path's conditions true however the stop ended — each path re-runs the guard itself
for f in "$STOP_TPL" ansible/templates/mirror-start.sh.j2 ansible/tasks/es-mirror-unit.yml; do
  grep -qF 'if [ "$TGT" = "192.168.100.252:9092" ]; then' "$f" \
    || { echo "FAIL: $f must gate on the literal production target"; fail=1; }
  grep -qF 'bash scripts/jenkins/permitted-sha-guard.sh' "$f" \
    || { echo "FAIL: $f must re-run the repository's own permitted-commit guard for a production unit"; fail=1; }
  grep -qE 'PERMITTED_SHA_GUARD_VERSION="[0-9a-f]{64}"' "$f" \
    || { echo "FAIL: $f must pass the guard version as a literal"; fail=1; }
  grep -qF 'PERMITTED_SHA="${PERMITTED_SHA:-}"' "$f" \
    || { echo "FAIL: $f must take PERMITTED_SHA from the ENVIRONMENT, never interpolated into the command text"; fail=1; }
done
awk '/THE PRODUCTION LOCK LIVES HERE/{f=1} f&&/launchctl unload/{print "LATE"; exit} f&&/permitted-sha-guard.sh/{print "EARLY"; exit}' "$STOP_TPL" | grep -q EARLY \
  || { echo "FAIL: $STOP_TPL must run the guard BEFORE it unloads anything"; fail=1; }

# ---- 2d. every value rendered into a script is shell-QUOTED ----
# ops_dir, launch_agents_dir, kafka_bin and the target are operator-supplied extra vars, and they are
# interpolated into shell BEFORE the guard runs: a double-quoted Jinja interpolation let a crafted
# value define a `bash() { return 0; }` function and make every guard invocation succeed. Each of them
# must go through the quote filter.
for f in ansible/templates/mirror-stop.sh.j2 ansible/templates/mirror-start.sh.j2 \
         ansible/templates/mirror-diff.sh.j2 ansible/templates/mirror-shape-check.sh.j2 \
         ansible/tasks/es-mirror-unit.yml; do
  # anywhere in the line, not only at its start: these scripts put several assignments on one line,
  # and a start-anchored pattern missed `PL=...; MDIR="{{ item.mdir }}"`
  bad=$(grep -nE '[A-Z_]+="\{\{' "$f" || true)
  [ -z "$bad" ] || { echo "FAIL: $f assigns a shell variable from an UNQUOTED interpolation — use the quote filter:"; printf '%s\n' "$bad" | sed 's/^/    /'; fail=1; }
done

# ---- 2e. a hostile PATH is refused, not neutralised ----
# run-mirror.sh — the script launchd EXECUTES — cannot have its paths quoted without breaking the
# byte-identity with the pipelines' own runner that the parity gate asserts, so the operator-supplied
# paths are REFUSED unless they are plain. Asserted here, and then behaviourally.
grep -qF "oe_ops is match('^[A-Za-z0-9._/-]+$')" "$PB" \
  || { echo "FAIL: $PB must refuse a non-plain ops_dir — run-mirror.sh cannot quote it"; fail=1; }
for v in oe_agents oe_kbin oe_rendered_dir; do
  grep -qF "$v is match('^[A-Za-z0-9._/-]+$')" "$PB" \
    || { echo "FAIL: $PB must refuse a non-plain $v"; fail=1; }
done
if command -v ansible-playbook >/dev/null; then
  hp=$(mktemp); set +e
  ansible-playbook -i "$INV" "$PB" -e mirror_target_ip=192.168.100.252 -e mirror_target_port=9092 \
    -e only_topics=es.futures.cvd -e '{"ops_dir": "/tmp/x\"; touch /tmp/OE_PATH_GATE_INJECTED; #"}' >"$hp" 2>&1
  rc=$?; set -e
  [ "$rc" != 0 ] || { echo "FAIL: a hostile ops_dir did not stop the run"; fail=1; }
  grep -q 'must be plain paths' "$hp" || { echo "FAIL: the refusal did not come from the path assertion — see $hp"; fail=1; }
  [ ! -e /tmp/OE_PATH_GATE_INJECTED ] || { echo "FAIL: the hostile ops_dir EXECUTED"; rm -f /tmp/OE_PATH_GATE_INJECTED; fail=1; }
  rm -f "$hp"
fi

# ---- 3. every mutating task re-checks the guard's verdict ----
grep -qF "oe_prod_permitted: \"{{ ((oe_target_ip ~ ':' ~ oe_target_port) != '192.168.100.252:9092') or ((oe_guard.rc | default(1)) == 0) }}\"" "$PB" \
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
  ansible-playbook -i "$INV" "$PB" -e mirror_target_ip=192.168.100.252 -e mirror_target_port=9092 \
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
  ansible-playbook -i "$INV" "$PB" -e mirror_target_ip=192.168.100.252 -e mirror_target_port=9092 \
    -e confirm_mirror_install=true -e permitted_sha=0000000000000000000000000000000000000000 \
    -e oe_prod_target=127.0.0.1:19092 -e oe_dev_target=192.168.100.252:9092 \
    -e oe_target=127.0.0.1:19092 \
    -e "dump_rows=$OUT_DUMP" >"$log2" 2>&1
  rc2=$?; set -e
  [ "$rc2" != 0 ] || { echo "FAIL: a production install survived -e oe_prod_target/-e oe_dev_target/-e oe_target — a decision is being taken through a derived name"; fail=1; }
  grep -q 'permitted-sha-guard: REFUSED' "$log2" \
    || { echo "FAIL: with the classification renamed, the refusal no longer comes from the guard — see $log2"; fail=1; }
  rm -f "$log" "$log2" "$OUT_DUMP"
else
  echo "FAIL: ansible-playbook is required to test the production permission path"; fail=1
fi

[ "$fail" = 0 ] || { echo "=== validate-ansible-prod-permission: FAILED ==="; exit 1; }
echo "=== validate-ansible-prod-permission: OK === the declared guard version matches all six mirror pipelines; the decision is taken against the literal production broker and survives -e renaming it; the task is production+install only and fatal; every mutating task re-checks the verdict; the order is stop, install, start; and a production install from this checkout is refused by the guard before any unit task"
