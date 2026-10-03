#!/usr/bin/env bash
# The deployment / migration exclusion barrier (scripts/deploy/zerodte-migrate-barrier.sh): the exact function service-deploy.sh calls before
# `kubectl apply`, driven against a fake kubectl through every state of the lock — absent (free), held (refused, the holder named), unreadable
# (refused: never treated as free) — and for a service the barrier does not cover (a no-op). Also proves service-deploy.sh calls it BEFORE its apply.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"
cat > "$T/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
case "$*" in
  *"get configmap/zerodte-research-migrate-lock -o jsonpath={.metadata.name}")
    case "${FAKE_LOCK:-absent}" in
      absent) echo 'Error from server (NotFound): configmaps "zerodte-research-migrate-lock" not found' >&2; exit 1 ;;
      held) printf 'zerodte-research-migrate-lock' ;;
      unreadable) echo 'Unable to connect to the server: dial tcp: i/o timeout' >&2; exit 1 ;;
    esac ;;
  *"get configmap/zerodte-research-migrate-lock -o jsonpath="*holder*) printf 'build-12-20261004T120000Z' ;;
  *) echo "fake kubectl: unexpected call: $*" >&2; exit 99 ;;
esac
FAKE
chmod +x "$T/bin/kubectl"
pass=0; fail=0
run() { # run <name> <want_rc> <want substring> <FAKE_LOCK> <service>
  local out rc
  out="$(env PATH="$T/bin:$PATH" FAKE_LOCK="$4" bash scripts/deploy/zerodte-migrate-barrier.sh options-edge "$5" 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$2" ] && printf '%s' "$out" | grep -qF -- "$3"; then pass=$((pass+1)); echo "  ok   $1 (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $1: rc=$rc want $2; want [$3]"; printf '%s\n' "$out" | sed 's/^/       | /'; fi
}
run "the lock is absent: the deploy proceeds"            0 "is absent — no migration is running; vix-option-inteligence may roll" absent vix-option-inteligence
run "the lock is held: refused, the holder named"        1 "held by build-12-20261004T120000Z"                  held       vix-option-inteligence
run "the lock state is unreadable: refused, never free"  1 "could not be read"                                  unreadable vix-option-inteligence
run "another service: the barrier does not apply"        0 "not applicable to databento-gex"                   held       databento-gex
# service-deploy.sh sources the barrier and calls it BEFORE its apply
line_barrier="$(grep -n '^zerodte_migrate_barrier "\$NAMESPACE" "\$SERVICE" || exit 1$' scripts/deploy/service-deploy.sh | cut -d: -f1 | head -1)"
line_apply="$(grep -n '^if ! kubectl apply -f "\$RENDER"' scripts/deploy/service-deploy.sh | cut -d: -f1 | head -1)"
if [ -n "$line_barrier" ] && [ -n "$line_apply" ] && [ "$line_barrier" -lt "$line_apply" ]; then pass=$((pass+1)); echo "  ok   service-deploy.sh calls the barrier (line $line_barrier) before its apply (line $line_apply)"; else fail=$((fail+1)); echo "  FAIL service-deploy.sh does not call the barrier before its apply (barrier=$line_barrier apply=$line_apply)"; fi
grep -q '^\. "\$(dirname "\$0")/zerodte-migrate-barrier.sh"$' scripts/deploy/service-deploy.sh && { pass=$((pass+1)); echo "  ok   service-deploy.sh sources the barrier"; } || { fail=$((fail+1)); echo "  FAIL service-deploy.sh does not source the barrier"; }
echo "zerodte migrate barrier: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-migrate-barrier-test: OK ==="; exit 0; }
echo "=== zerodte-migrate-barrier-test: FAILED ==="; exit 1
