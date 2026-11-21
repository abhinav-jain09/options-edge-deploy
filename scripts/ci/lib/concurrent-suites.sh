# concurrent-suites.sh — run independent validation suites concurrently, report them in a fixed order.
#
# Sourced by validate-services.sh; exercised by scripts/ci/concurrent-suites-test.sh under whatever bash runs it
# (the service-deploy agents are macOS, where that can be /bin/bash 3.2).
#
#   suites_init <scratch-dir>            output files live here; call once
#   suites_start quiet|loud <label> <script>
#                                        fails closed (exit 1) if <script> is missing or not executable;
#                                        quiet = output shown only on failure, loud = always under "=== <label> ==="
#   suites_collect                       waits for EVERY started suite in start order, prints each result, returns 1
#                                        if any exited non-zero (never stops at the first failure)
#   suites_kill_live                     for an EXIT trap: signals only suites not yet reaped
#
# A suite's PID leaves the live set the moment it is reaped. Keeping reaped PIDs for the trap would let an abort
# signal whatever process the kernel has since given that PID (Codex #1045 r1).

suites_init() {
  _SUITES_DIR="$1"
  _suites_pids=(); _suites_modes=(); _suites_labels=(); _suites_scripts=(); _suites_live=()
}

suites_start() { # quiet|loud label script
  local mode="$1" label="$2" script="$3" i
  case "$mode" in quiet|loud) ;; *) echo "FAIL: suites_start: mode must be quiet or loud, got '$mode'"; exit 1;; esac
  if [ ! -x "$script" ]; then
    echo "FAIL: $script missing or not executable"
    exit 1
  fi
  i="${#_suites_pids[@]}"
  bash "$script" >"$_SUITES_DIR/suite-$i.out" 2>&1 &
  _suites_pids[$i]="$!"; _suites_modes[$i]="$mode"; _suites_labels[$i]="$label"; _suites_scripts[$i]="$script"
  _suites_live[$i]="$!"
}

suites_collect() {
  local i st any=0 n="${#_suites_pids[@]}"
  i=0
  while [ "$i" -lt "$n" ]; do
    st=0; wait "${_suites_pids[$i]}" || st=$?
    unset '_suites_live[i]'
    # Callers test this function with `if !`, which suspends set -e inside it — so an unreadable output file must
    # fail the run explicitly rather than rely on errexit.
    if [ "${_suites_modes[$i]}" = loud ]; then
      echo "=== ${_suites_labels[$i]} ==="
      cat "$_SUITES_DIR/suite-$i.out" || { echo "FAIL: output of ${_suites_scripts[$i]} unreadable"; any=1; }
      [ "$st" -eq 0 ] || echo "FAIL: ${_suites_scripts[$i]} exited $st"
    elif [ "$st" -ne 0 ]; then
      echo "FAIL: ${_suites_scripts[$i]}"
      sed 's/^/      /' "$_SUITES_DIR/suite-$i.out" || echo "      (output unreadable)"
    fi
    [ "$st" -eq 0 ] || any=1
    i=$((i + 1))
  done
  return "$any"
}

suites_kill_live() {
  # ${a[@]+"${a[@]}"}: an empty array is "unbound" to bash 3.2 under set -u.
  kill ${_suites_live[@]+"${_suites_live[@]}"} 2>/dev/null || true
}
