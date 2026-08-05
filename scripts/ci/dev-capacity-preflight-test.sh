#!/usr/bin/env bash
# Focused tests for scripts/deploy/dev-capacity-preflight.sh, with a MOCKED kubectl.
#
# The preflight is a safety control on a real failure mode (a rollout that can never schedule its
# surge pod, applied anyway, then timing out half-rolled), so its own decisions are tested rather
# than assumed: it must read the CANDIDATE manifest (not the live Deployment), respect Recreate,
# refuse when free CPU is short, and FAIL OPEN when the cluster cannot be queried.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
PREFLIGHT="$PWD/scripts/deploy/dev-capacity-preflight.sh"
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
fail=0

mock_kubectl() { # $1 = allocatable cpu, $2 = used cpu (one pod)
  mkdir -p "$TMP/bin"
  cat > "$TMP/bin/kubectl" <<EOF
#!/usr/bin/env bash
case "\$*" in
  *"get node"*)  echo '{"items":[{"status":{"allocatable":{"cpu":"$1"}}}]}' ;;
  *"get pods"*)  echo '{"items":[{"status":{"phase":"Running"},"spec":{"containers":[{"resources":{"requests":{"cpu":"$2"}}}]}}]}' ;;
  *) echo '{}' ;;
esac
EOF
  chmod +x "$TMP/bin/kubectl"
}
mock_kubectl_broken() {
  mkdir -p "$TMP/bin"; printf '#!/usr/bin/env bash\nexit 1\n' > "$TMP/bin/kubectl"; chmod +x "$TMP/bin/kubectl"
}
render() { printf '%s\n' "$1" > "$TMP/render.yaml"; }
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

ROLLING='apiVersion: apps/v1
kind: Deployment
metadata:
  name: candidate
spec:
  template:
    spec:
      containers:
        - name: c
          resources:
            requests:
              cpu: 500m
              memory: 1Gi'

# The candidate asks 500m. 10000m allocatable, 9800m used => 200m free: must refuse.
render "$ROLLING"; mock_kubectl "10" "9800m"
expect "refuses when the CANDIDATE surge does not fit" 1 "needs 500m"

# Same manifest, plenty free: must proceed.
mock_kubectl "10" "5000m"
expect "proceeds when there is room" 0 "proceeding"

# Recreate needs no surge pod, so it is never blocked.
render "${ROLLING/  template:/  strategy:
    type: Recreate
  template:}"
mock_kubectl "10" "9800m"
expect "ignores Recreate deployments" 0

# A cluster it cannot query must not wedge the deploy.
render "$ROLLING"; mock_kubectl_broken
expect "fails OPEN when kubectl errors" 0

# Nothing to weigh.
render 'kind: ConfigMap
metadata:
  name: x'
mock_kubectl "10" "9800m"
expect "no Deployment in the render is a no-op" 0

[ $fail -eq 0 ] && echo "=== dev-capacity-preflight-test: OK ===" || { echo "=== dev-capacity-preflight-test: FAILED ==="; exit 1; }
