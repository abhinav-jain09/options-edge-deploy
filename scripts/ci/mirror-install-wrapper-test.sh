#!/usr/bin/env bash
# scripts/ops/install-es-mirrors.sh is the only invocation that can tell you a mirror unit was
# installed, so its two guarantees are tested here rather than trusted:
#   1. it REFUSES every flag and environment variable that can make ansible-playbook exit 0 having
#      done nothing;
#   2. it FAILS when a run exits 0 without a receipt, and PASSES when a receipt is written.
# Guarantee 2 is tested with a stub ansible-playbook on PATH, not a real run: the point is the
# wrapper's reaction to an exit-0-with-no-work, and no real invocation should be able to produce one.
set -uo pipefail
cd "$(dirname "$0")/.." && cd .. || exit 1
W=scripts/ops/install-es-mirrors.sh
fail=0
[ -x "$W" ] || { echo "FAIL: $W is not executable"; exit 1; }
bash -n "$W" || { echo "FAIL: $W does not parse"; exit 1; }

# ---- 1. the refusals, in argv ----
for f in "--tags zzz" "--tags=zzz" "-t zzz" "--skip-tags all" "--skip-tags always" \
         "--skip-tags=always" "--limit mac74" "--limit=mac74" "-l mac74" "--check" "-C" \
         "--list-hosts" "--list-tasks" "--list-tags" "--step" "--start-at-task x"; do
  out=$(bash "$W" $f 2>&1); rc=$?
  if [ "$rc" != 2 ] || ! grep -q 'refused by this wrapper' <<<"$out"; then
    echo "FAIL: $W did not refuse '$f' (rc=$rc)"; fail=1
  fi
done

# ---- 1b. the refusals, in the environment, where argv never sees them ----
# ANSIBLE_RUN_TAGS is first because it is the one that got through: it is the env spelling of
# --tags and the first wrapper only refused ANSIBLE_TAGS, so `ANSIBLE_RUN_TAGS=always` produced a
# rc=0 run with a receipt reading confirm=True and every count zero (Codex r5, BLOCKER).
for v in ANSIBLE_RUN_TAGS=always ANSIBLE_TAGS=zzz ANSIBLE_SKIP_TAGS=always ANSIBLE_LIMIT=mac74 \
         ANSIBLE_CHECK_MODE=1 ANSIBLE_START_AT_TASK=x ANSIBLE_CONFIG=/dev/null \
         ANSIBLE_PLAYBOOK_DIR=/tmp ANSIBLE_STDOUT_CALLBACK=null; do
  out=$(env "$v" bash "$W" 2>&1); rc=$?
  if [ "$rc" != 2 ] || ! grep -q 'is set in the environment' <<<"$out"; then
    echo "FAIL: $W did not refuse $v (rc=$rc)"; fail=1
  fi
done

# ---- 2. the receipt contract, against a stub ansible-playbook ----
STUB=$(mktemp -d) || exit 1
trap 'rm -f "$STUB/ansible-playbook" "$STUB/mode" scripts/ops/.install-es-mirrors.backstop-test.sh; rmdir "$STUB" 2>/dev/null' EXIT
cat > "$STUB/ansible-playbook" <<'STUBEOF'
#!/usr/bin/env bash
# Reads $STUB_MODE: "silent" exits 0 writing nothing; "receipt" writes the receipt and exits 0;
# "fail" exits 2 writing nothing. The receipt path is the value after -e install_receipt=.
banners() {
  echo 'TASK [Flatten the unit table into one row per launchd unit] ***'
  echo 'TASK [Account for the run, and write the receipt last whatever the outcome] ***'
  echo 'localhost                  : ok=12   changed=0    unreachable=0    failed=0'
}
# $r is empty unless the wrapper exported the path: the stub reads OE_MIRROR_RECEIPT, exactly as
# the playbook does, so a wrapper that stopped exporting it would fail these sections.
r="${OE_MIRROR_RECEIPT:-}"
case "${STUB_MODE:-}" in
  receipt)  banners; [ -n "$r" ] && printf 'host=localhost\nunits_in_table=18\n' > "$r"; exit 0;;
  fail)     banners; exit 2;;
  nobanner) exit 0;;
  *)        banners; exit 0;;
esac
STUBEOF
chmod +x "$STUB/ansible-playbook"

# 2a. exit 0 with no receipt MUST fail — this is the whole reason the wrapper exists
out=$(PATH="$STUB:$PATH" STUB_MODE=silent bash "$W" 2>&1); rc=$?
if [ "$rc" != 3 ] || ! grep -q 'exited 0 but produced NO RECEIPT' <<<"$out"; then
  echo "FAIL: a run that exited 0 having written no receipt was not caught (rc=$rc)"; echo "$out" | sed 's/^/    /' | tail -5; fail=1
fi
# 2b. a receipt means success, and the receipt is shown
out=$(PATH="$STUB:$PATH" STUB_MODE=receipt bash "$W" 2>&1); rc=$?
if [ "$rc" != 0 ] || ! grep -q 'units_in_table=18' <<<"$out"; then
  echo "FAIL: a run that wrote a receipt did not pass cleanly (rc=$rc)"; echo "$out" | sed 's/^/    /' | tail -5; fail=1
fi
# 2c. a DIAGNOSED failure keeps its own exit code rather than being relabelled 3
out=$(PATH="$STUB:$PATH" STUB_MODE=fail bash "$W" 2>&1); rc=$?
if [ "$rc" != 2 ] || ! grep -q 'no receipt: ansible-playbook exited 2' <<<"$out"; then
  echo "FAIL: a playbook failure was not reported with its own exit code (rc=$rc)"; echo "$out" | sed 's/^/    /' | tail -5; fail=1
fi

# ---- 2d. the receipt path is taken from the ENVIRONMENT, so no -e can redirect it -------------
# The wrapper used to pass `-e install_receipt=` BEFORE the caller's arguments, so a later
# `-e install_receipt=<elsewhere>` won and a real install could mutate units while the wrapper saw
# an empty receipt and reported exit 3 — "it failed, run it again", against production (Codex r5).
grep -q 'export OE_MIRROR_RECEIPT=' "$W" \
  || { echo "FAIL: $W no longer passes the receipt path through the environment"; fail=1; }
grep -qE '^[^#]*-e[[:space:]]+"?install_receipt=' "$W" \
  && { echo "FAIL: $W passes the receipt as an extra var, which a later -e outranks"; fail=1; }
# and the playbook must read it with an inline lookup, never through a variable name
grep -q "lookup('env', 'OE_MIRROR_RECEIPT')" ansible/es-mirrors.yml \
  || { echo "FAIL: the playbook no longer reads the receipt path from the environment inline"; fail=1; }
grep -qE "^\s+oe_receipt:" ansible/es-mirrors.yml \
  && { echo "FAIL: the receipt path is bound to a variable name again, which -e outranks"; fail=1; }

# ---- 2e. the receipt task must NOT be tagged `always` -----------------------------------------
# This is the inversion that made the first receipt worthless: `tags: always` guarantees the
# receipt runs under a narrowed tag selection, which is the opposite of evidence that the work ran.
# A refusal should survive selection; evidence must not.
python3 - <<'PYEOF' || fail=1
import sys, yaml
plays = yaml.safe_load(open('ansible/es-mirrors.yml'))
bad = []
def scan(tasks, inside_receipt_block=False):
    for t in tasks or []:
        if not isinstance(t, dict):
            continue
        name = t.get('name', '')
        for key in ('block', 'always', 'rescue'):
            if key in t:
                scan(t[key])
        # the receipt WRITE task only, by exact name — not every task mentioning a receipt
        if name == 'Write the run receipt' and 'always' in str(t.get('tags', '')):
            bad.append(name)
for p in plays:
    scan(p.get('tasks'))
if bad:
    print("FAIL: the receipt task is tagged `always`, so a narrowed run still writes it: %s" % bad)
    sys.exit(1)
# positive control: an exact-name match passes trivially if the name ever changes, so require the
# task to EXIST. Without this the check is satisfied by the task being absent or renamed.
seen = []
def find(tasks):
    for t in tasks or []:
        if not isinstance(t, dict):
            continue
        for key in ('block', 'always', 'rescue'):
            if key in t:
                find(t[key])
        if t.get('name') == 'Write the run receipt':
            seen.append(t)
for p in plays:
    find(p.get('tasks'))
if not seen:
    print("FAIL: no task named 'Write the run receipt' — the always-tag check matched nothing, so it proves nothing")
    sys.exit(1)
PYEOF

# ---- 2f. THE BACKSTOP, tested with the enumeration DEFEATED ------------------------------------
# The flag list is an enumeration, which is the shape that failed in r3, r4 and r5. So this proves
# the non-enumerative layer stands on its own: with ANSIBLE_RUN_TAGS removed from the refusal list,
# the run must STILL fail — on the mandatory task banners missing from Ansible's own output.
MUT=scripts/ops/.install-es-mirrors.backstop-test.sh
python3 - "$W" "$MUT" <<'PYEOF'
import sys
src, dst = sys.argv[1], sys.argv[2]
s = open(src).read()
# drop ANSIBLE_RUN_TAGS from the refusal loop only
s2 = s.replace("for v in ANSIBLE_TAGS ANSIBLE_RUN_TAGS ANSIBLE_SKIP_TAGS",
               "for v in ANSIBLE_TAGS ANSIBLE_SKIP_TAGS", 1)
assert s2 != s, "could not find the env refusal loop to mutate"
open(dst, 'w').write(s2)
PYEOF
if [ -s "$MUT" ]; then
  out=$(ANSIBLE_RUN_TAGS=always bash "$MUT" 2>&1); rc=$?
  if [ "$rc" = 0 ]; then
    echo "FAIL: with the env enumeration defeated, a zero-work run exited 0 — the backstop does not stand alone"
    fail=1
  elif ! grep -q 'did not run the tasks that do the work' <<<"$out"; then
    echo "FAIL: the zero-work run failed (rc=$rc) but not on the mandatory-task check, so the backstop is not what caught it"
    echo "$out" | sed 's/^/    /' | tail -6
    fail=1
  fi
fi
rm -f "$MUT"

# ---- 3. the stub test must be able to FAIL ---------------------------------------------------
# 2a passes trivially if the wrapper never runs ansible-playbook at all, so prove the stub is
# actually reached: in receipt mode the receipt content must come from the stub.
out=$(PATH="$STUB:$PATH" STUB_MODE=receipt bash "$W" 2>&1)
grep -q 'units_in_table=18' <<<"$out" || { echo "FAIL: the stub ansible-playbook was never invoked, so section 2 proves nothing"; fail=1; }

[ "$fail" = 0 ] && echo "=== mirror-install-wrapper-test: PASS ===" || echo "=== mirror-install-wrapper-test: FAILED ==="
exit "$fail"
