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
for v in ANSIBLE_TAGS=zzz ANSIBLE_SKIP_TAGS=always ANSIBLE_LIMIT=mac74 ANSIBLE_CHECK_MODE=1 \
         ANSIBLE_START_AT_TASK=x; do
  out=$(env "$v" bash "$W" 2>&1); rc=$?
  if [ "$rc" != 2 ] || ! grep -q 'is set in the environment' <<<"$out"; then
    echo "FAIL: $W did not refuse $v (rc=$rc)"; fail=1
  fi
done

# ---- 2. the receipt contract, against a stub ansible-playbook ----
STUB=$(mktemp -d) || exit 1
trap 'rm -f "$STUB/ansible-playbook" "$STUB/mode"; rmdir "$STUB" 2>/dev/null' EXIT
cat > "$STUB/ansible-playbook" <<'STUBEOF'
#!/usr/bin/env bash
# Reads $STUB_MODE: "silent" exits 0 writing nothing; "receipt" writes the receipt and exits 0;
# "fail" exits 2 writing nothing. The receipt path is the value after -e install_receipt=.
r=""
for a in "$@"; do case "$a" in install_receipt=*) r="${a#install_receipt=}";; esac; done
case "${STUB_MODE:-}" in
  receipt) [ -n "$r" ] && printf 'host=localhost\nunits_considered=1\n' > "$r"; exit 0;;
  fail)    exit 2;;
  *)       exit 0;;
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
if [ "$rc" != 0 ] || ! grep -q 'units_considered=1' <<<"$out"; then
  echo "FAIL: a run that wrote a receipt did not pass cleanly (rc=$rc)"; echo "$out" | sed 's/^/    /' | tail -5; fail=1
fi
# 2c. a DIAGNOSED failure keeps its own exit code rather than being relabelled 3
out=$(PATH="$STUB:$PATH" STUB_MODE=fail bash "$W" 2>&1); rc=$?
if [ "$rc" != 2 ] || ! grep -q 'no receipt: ansible-playbook exited 2' <<<"$out"; then
  echo "FAIL: a playbook failure was not reported with its own exit code (rc=$rc)"; echo "$out" | sed 's/^/    /' | tail -5; fail=1
fi

# ---- 3. the stub test must be able to FAIL ---------------------------------------------------
# 2a passes trivially if the wrapper never runs ansible-playbook at all, so prove the stub is
# actually reached: in receipt mode the receipt content must come from the stub.
out=$(PATH="$STUB:$PATH" STUB_MODE=receipt bash "$W" 2>&1)
grep -q 'host=localhost' <<<"$out" || { echo "FAIL: the stub ansible-playbook was never invoked, so section 2 proves nothing"; fail=1; }

[ "$fail" = 0 ] && echo "=== mirror-install-wrapper-test: PASS ===" || echo "=== mirror-install-wrapper-test: FAILED ==="
exit "$fail"
