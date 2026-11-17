# Sourced by the fleet-watch wrappers that call kubectl. NOT executable on its own.
#
# WHY. kubectl against a cluster whose API port does not answer retries its API-group discovery five times and only then gives up: measured
# 2026-11-18 against production (.252, offline), `kubectl --request-timeout=20s get pods` took 100 s. Seven stages x two dead clusters is 23 minutes,
# past the job's 10-minute timeout, so the build was ABORTED (builds 1421, 1422) and no stage ever reached Discord. A 5-second TCP probe of the
# kubeconfig's own server turns "the cluster does not answer" into an immediate, ordinary exit 2 ("could not check", which IS delivered).
#
# oe_kube_reachable  -> 0 reachable (or the server cannot be determined: then kubectl gets to say why), 2 not reachable (ERROR: line printed)
oe_kube_reachable() {
  local server hp host port
  server="$(kubectl config view --minify -o 'jsonpath={.clusters[0].cluster.server}' 2> /dev/null)" || return 0
  [ -n "$server" ] || return 0
  hp="${server#*://}"; hp="${hp%%/*}"
  case "$hp" in
    \[*\]:*) host="${hp%%\]*}"; host="${host#\[}"; port="${hp##*\]:}" ;;
    *:*) host="${hp%:*}"; port="${hp##*:}" ;;
    *) host="$hp"; case "$server" in https://*) port=443 ;; *) port=80 ;; esac ;;
  esac
  # python3, not nc: macOS `nc -z -w 5` does not bound the CONNECT (measured: 75 s against the dead host), a socket timeout does.
  if ! python3 -c 'import socket,sys; socket.create_connection((sys.argv[1], int(sys.argv[2])), float(sys.argv[3])).close()' \
        "$host" "$port" "${OE_KUBE_PROBE_SECONDS:-5}" > /dev/null 2>&1; then
    echo "ERROR: the Kubernetes API at $host:$port does not answer (TCP connect failed within ${OE_KUBE_PROBE_SECONDS:-5}s) - the cluster or its host is down or unreachable from this agent"
    return 2
  fi
  return 0
}
