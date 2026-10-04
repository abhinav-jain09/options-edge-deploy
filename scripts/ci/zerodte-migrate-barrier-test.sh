#!/usr/bin/env bash
# The deployment / migration MUTUAL EXCLUSION (scripts/deploy/zerodte-migrate-barrier.sh): the exact functions service-deploy.sh calls — acquire
# before `kubectl apply`, release on EXIT — driven against a STATEFUL fake kubectl (the lock is a file: an atomic create fails when it exists,
# a delete removes it) through every state: free (acquired, the holder recorded), held by a migration (refused, the holder named), held by an
# earlier deploy (refused), an API that cannot create (refused), release of what this process holds, release of nothing, a service the
# exclusion does not cover; the interleaving a second deploy / a migration sees while a deploy holds it; and that service-deploy.sh sources the
# barrier, ACQUIRES before its apply and traps the release. (The migration wrapper's side of the same lock — refused while a deploy holds it,
# a deploy refused while a migration holds it through its Job — is in zerodte-research-migrate-receipt-test.sh.)
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/k8s"
cat > "$T/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
K="${FAKE_K8S:?}"
case "$*" in
  *"create -f -")
    body="$(cat)"
    [ "${FAKE_API_DOWN:-0}" = 1 ] && { echo "Unable to connect to the server: dial tcp: i/o timeout" >&2; exit 1; }
    if printf '%s' "$body" | grep -q "name: zerodte-research-migrate-lock"; then
      [ -f "$K/lock" ] && { echo 'Error from server (AlreadyExists): configmaps "zerodte-research-migrate-lock" already exists' >&2; exit 1; }
      printf '%s' "$body" | sed -n 's/^ *options-edge.io\/holder: "\(.*\)"$/\1/p' | head -1 > "$K/lock"
    fi ;;
  *"get configmap/zerodte-research-migrate-lock -o jsonpath="*holder*) [ -f "$K/lock" ] && cat "$K/lock" || { echo 'Error from server (NotFound)' >&2; exit 1; } ;;
  *"delete configmap/zerodte-research-migrate-lock"*) [ "${FAKE_DELETE_FAILS:-0}" = 1 ] && { echo "error: timed out" >&2; exit 1; }; rm -f "$K/lock" ;;
  *) echo "fake kubectl: unexpected call: $*" >&2; exit 99 ;;
esac
FAKE
chmod +x "$T/bin/kubectl"
pass=0; fail=0
B="scripts/deploy/zerodte-migrate-barrier.sh"
run() { # run <name> <want_rc> <want substring> <args...>   (env FAKE_* as exported by the caller)
  local name="$1" want_rc="$2" want="$3"; shift 3
  local out rc
  out="$(env PATH="$T/bin:$PATH" FAKE_K8S="$T/k8s" bash "$B" "$@" 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then pass=$((pass+1)); echo "  ok   $name (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | sed 's/^/       | /'; fi
}
check() { local name="$1"; shift; if "$@"; then pass=$((pass+1)); echo "  ok   $name"; else fail=$((fail+1)); echo "  FAIL $name"; fi; }
rm -f "$T/k8s/lock"
run "acquire when free: the deploy holds the lock"   0 "ACQUIRED by service-deploy-vix-dev-build-12" acquire options-edge vix-option-inteligence service-deploy-vix-dev-build-12
check "the lock exists with the deploy as holder" bash -c "[ \"\$(cat '$T/k8s/lock')\" = service-deploy-vix-dev-build-12 ]"
run "a second deploy while the first holds it: refused" 1 "is held by service-deploy-vix-dev-build-12" acquire options-edge vix-option-inteligence service-deploy-vix-dev-build-13
check "the holder is unchanged" bash -c "[ \"\$(cat '$T/k8s/lock')\" = service-deploy-vix-dev-build-12 ]"
run "release of what the deploy holds"               0 "released" release options-edge
check "the lock is gone" bash -c "! [ -f '$T/k8s/lock' ]"
printf '%s' "research-migrate-build-7-20261004T120000Z" > "$T/k8s/lock"
run "held by a migration: refused, the holder named" 1 "is held by research-migrate-build-7-20261004T120000Z" acquire options-edge vix-option-inteligence service-deploy-vix-dev-build-14
check "the migration's lock is untouched" bash -c "[ \"\$(cat '$T/k8s/lock')\" = research-migrate-build-7-20261004T120000Z ]"
rm -f "$T/k8s/lock"
FAKE_API_DOWN=1 run "the API cannot create: refused (never assumed free)" 1 "could not create zerodte-research-migrate-lock" acquire options-edge vix-option-inteligence service-deploy-vix-dev-build-15
check "nothing was created" bash -c "! [ -f '$T/k8s/lock' ]"
run "another service: the exclusion does not apply"  0 "not applicable to databento-gex" acquire options-edge databento-gex x
check "no lock for another service" bash -c "! [ -f '$T/k8s/lock' ]"
run "a holder id is required"                        1 "a holder id is required" acquire options-edge vix-option-inteligence ""
printf '%s' "service-deploy-vix-dev-build-16" > "$T/k8s/lock"
FAKE_DELETE_FAILS=1 run "a release that fails warns and leaves the lock for the operator" 0 "could not release zerodte-research-migrate-lock" release options-edge
check "the lock remains" bash -c "[ -f '$T/k8s/lock' ]"
rm -f "$T/k8s/lock"
# service-deploy.sh: sources the barrier, ACQUIRES before its apply, traps the release
line_src="$(grep -n '^\. "\$(dirname "\$0")/zerodte-migrate-barrier.sh"$' scripts/deploy/service-deploy.sh | cut -d: -f1 | head -1)"
line_acq="$(grep -n '^zerodte_migrate_barrier_acquire "\$NAMESPACE" "\$SERVICE" ' scripts/deploy/service-deploy.sh | cut -d: -f1 | head -1)"
line_trap="$(grep -n "^trap 'zerodte_migrate_barrier_release \"\$NAMESPACE\"' EXIT$" scripts/deploy/service-deploy.sh | cut -d: -f1 | head -1)"
line_apply="$(grep -n '^if ! kubectl apply -f "\$RENDER"' scripts/deploy/service-deploy.sh | cut -d: -f1 | head -1)"
line_gate="$(grep -n '^echo "=== health gate ==="' scripts/deploy/service-deploy.sh | cut -d: -f1 | head -1)"
check "service-deploy.sh sources the barrier" test -n "$line_src"
check "service-deploy.sh acquires (line $line_acq) before its apply (line $line_apply) and traps the release (line $line_trap) right after acquiring" bash -c "[ -n '$line_acq' ] && [ -n '$line_trap' ] && [ -n '$line_apply' ] && [ '$line_acq' -lt '$line_apply' ] && [ '$line_trap' -eq \$(( $line_acq + 1 )) ]"
check "the release is on EXIT, i.e. after the rollout and the health gate (line $line_gate), not before them" bash -c "[ -n '$line_gate' ] && [ '$line_acq' -lt '$line_gate' ] && ! sed -n '$line_acq,\$p' scripts/deploy/service-deploy.sh | grep -q '^zerodte_migrate_barrier_release'"
check "the acquire's failure exits the deploy (|| exit 1)" bash -c "sed -n '${line_acq}p' scripts/deploy/service-deploy.sh | grep -q '|| exit 1$'"
echo "zerodte migrate barrier (mutual exclusion): $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-migrate-barrier-test: OK ==="; exit 0; }
echo "=== zerodte-migrate-barrier-test: FAILED ==="; exit 1
