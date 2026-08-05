#!/usr/bin/env bash
# Focused tests for scripts/deploy/dev-capacity-preflight.sh, with a MOCKED kubectl.
#
# The preflight is a safety control on a real failure mode (a rollout that can never schedule its
# surge pod, applied anyway, then timing out half-rolled), so its own decisions are tested rather
# than assumed: it must size surge from the CANDIDATE manifest using Kubernetes semantics
# (replicas x maxSurge, percentage rounding, Recreate and maxSurge:0 exempt), refuse when free CPU
# is short, and FAIL OPEN when the cluster cannot be queried.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
PREFLIGHT="$PWD/scripts/deploy/dev-capacity-preflight.sh"
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"
fail=0
: > "$TMP/render.yaml"

# The mock answers the three calls the preflight makes: the local candidate conversion, the node,
# and the pod list. The candidate JSON is whatever the current case wrote.
write_mock() { # $1 allocatable cpu, $2 used cpu (single pod), $3 broken?
  if [ "${3:-}" = "broken" ]; then
    printf '#!/usr/bin/env bash\nexit 1\n' > "$TMP/bin/kubectl"
  else
    cat > "$TMP/bin/kubectl" <<EOF
#!/usr/bin/env bash
case "\$*" in
  *"create"*)   cat "$TMP/candidate.json" ;;
  *"get node"*) echo '{"items":[{"status":{"allocatable":{"cpu":"$1"}}}]}' ;;
  *"get pods"*) echo '{"items":[{"status":{"phase":"Running"},"spec":{"containers":[{"resources":{"requests":{"cpu":"$2"}}}]}}]}' ;;
  *) echo '{}' ;;
esac
EOF
  fi
  chmod +x "$TMP/bin/kubectl"
}

# One Deployment, as `kubectl create --dry-run=client -o json` would emit it.
candidate() { # $1 cpu, $2 replicas, $3 strategy-json
  cat > "$TMP/candidate.json" <<EOF
{"kind":"Deployment","spec":{"replicas":$2,$3"template":{"spec":{"containers":[
  {"resources":{"requests":{"cpu":"$1","memory":"1Gi"}}}]}}}}
EOF
}

expect() { # name, want_rc, [want_substring]
  local name="$1" want="$2" want_out="${3:-}"
  out=$(PATH="$TMP/bin:$PATH" "$PREFLIGHT" "$TMP/render.yaml" test 2>&1); rc=$?
  if [ "$rc" != "$want" ]; then
    echo "FAIL: $name — rc=$rc want=$want"; printf '%s\n' "$out" | sed 's/^/      /'; fail=1; return
  fi
  if [ -n "$want_out" ] && ! printf '%s' "$out" | grep -qF "$want_out"; then
    echo "FAIL: $name — output missing '$want_out'"; printf '%s\n' "$out" | sed 's/^/      /'; fail=1; return
  fi
  echo "ok: $name"
}

# 500m candidate, 10000m allocatable, 9800m used => 200m free: refuse. Proves it sizes from the
# CANDIDATE — the live Deployment in this scenario is the 250m one the server would have reported.
candidate 500m 1 ''; write_mock "10" "9800m"
expect "refuses when the CANDIDATE surge does not fit" 1 "needs 500m"

write_mock "10" "5000m"
expect "proceeds when there is room" 0 "proceeding"

# Recreate replaces in place — no surge pod is ever created.
candidate 500m 1 '"strategy":{"type":"Recreate"},'; write_mock "10" "9800m"
expect "ignores Recreate deployments" 0

# maxSurge: 0 is how raw-postgres-writer already rolls without extra capacity.
candidate 500m 1 '"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxSurge":0}},'; write_mock "10" "9800m"
expect "respects maxSurge: 0" 0

# replicas: 0 does not roll at all.
candidate 500m 0 ''; write_mock "10" "9800m"
expect "ignores replicas: 0" 0

# maxSurge: 2 needs TWO extra pods — 1000m, not 500m.
candidate 500m 1 '"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxSurge":2}},'; write_mock "10" "9200m"
expect "counts maxSurge: 2 as two pods" 1 "needs 1000m"

# 25% of 3 replicas rounds UP to 1 pod, the Kubernetes rule.
candidate 500m 3 ''; write_mock "10" "9800m"
expect "rounds a percentage maxSurge up" 1 "needs 500m"

# THE INCIDENT: the capacity-recovery deploy must not refuse itself. The monolith path runs this
# AFTER `kubectl apply -k`, so the scale-downs are already live and the node has room again.
candidate 250m 1 ''; write_mock "10" "8250m"
expect "does not block the capacity-recovery deploy once the scale-downs are applied" 0 "proceeding"

# A cluster it cannot query must not wedge the deploy.
write_mock "10" "9800m" broken
expect "fails OPEN when kubectl errors" 0

[ $fail -eq 0 ] && echo "=== dev-capacity-preflight-test: OK ===" || { echo "=== dev-capacity-preflight-test: FAILED ==="; exit 1; }
