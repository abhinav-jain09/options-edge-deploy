#!/usr/bin/env bash
# The reload guard (ansible/templates/mirror-reload.sh.j2) is the only part of the Ansible mirror path
# that stops and starts a LIVE mirror, and the scenario it exists for is the quiet one: `launchctl
# unload` of a unit that is not loaded is a no-op, and a `launchctl load` that FAILS leaves the OLD
# process running — whose command line still names this unit's producer.properties. A guard that only
# checks "a pid exists and is bound to the right config" reports success there while the mirror keeps
# publishing the pre-change settings.
#
# So drive it against stub launchctl/ps binaries, once per way it can be lied to.
set -euo pipefail
cd "$(dirname "$0")/../.."
TPL=ansible/templates/mirror-reload.sh.j2
[ -r "$TPL" ] || { echo "FAIL: $TPL not readable"; exit 1; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
MDIR="$WORK/unit"; mkdir -p "$MDIR"
LBL=com.optionsedge.test-mirror
PL="$WORK/$LBL.plist"; : > "$PL"

# render the template the way ansible would for one unit (these are its only expressions)
sed -e "s|{{ item.plist }}|$PL|g" -e "s|{{ item.label }}|$LBL|g" -e "s|{{ item.mdir }}|$MDIR|g" "$TPL" > "$WORK/reload.sh"
chmod +x "$WORK/reload.sh"
grep -q '{{' "$WORK/reload.sh" && { echo "FAIL: the rendered guard still has unresolved expressions — this test's substitution list is stale"; exit 1; }

# stub launchctl and ps: behaviour driven by marker files in $WORK
mkdir -p "$WORK/bin"
cat > "$WORK/bin/launchctl" <<'L'
#!/usr/bin/env bash
W="$(dirname "$0")/.."
case "${1:-}" in
  list)   cat "$W/pid.$(cat "$W/phase")" 2>/dev/null; exit 0 ;;
  unload) echo loaded > "$W/phase"; [ -e "$W/unload_fails" ] || echo unloaded > "$W/phase"; exit "$([ -e "$W/unload_fails" ] && echo 1 || echo 0)" ;;
  load)   [ -e "$W/load_fails" ] && exit 1; echo running > "$W/phase"; exit 0 ;;
esac
exit 0
L
chmod +x "$WORK/bin/launchctl"
cat > "$WORK/bin/ps" <<P
#!/usr/bin/env bash
[ -e "$WORK/ps_fails" ] && exit 1
# the two forms the guard uses: 'ps -p <pid> -o command=' and 'ps -axo pid=,command='
W="$WORK"; MDIR="$MDIR"
case "\$*" in
  *-axo*)
    # every process currently publishing SOME unit's config, per phase
    while read -r pid path; do
      [ -n "\$pid" ] || continue
      echo "\$pid java kafka.tools.MirrorMaker --producer.config \$path"
    done < "\$W/procs.\$(cat "\$W/phase")" 2>/dev/null
    ;;
  *)
    if [ -e "\$W/wrong_binding" ]; then echo "java kafka.tools.MirrorMaker --producer.config /some/other/unit/producer.properties"
    else echo "java kafka.tools.MirrorMaker --producer.config \$MDIR/producer.properties"; fi
    ;;
esac
P
chmod +x "$WORK/bin/ps"
export PATH="$WORK/bin:$PATH" OE_RELOAD_SETTLE=0

fail=0
scenario() { # name  expect(pass|fail)  [want=<substring>]  setup...
  local name="$1" expect="$2"; shift 2
  local want=""
  case "${1:-}" in want=*) want="${1#want=}"; shift ;; esac
  rm -f "$WORK"/unload_fails "$WORK"/load_fails "$WORK"/wrong_binding "$WORK"/ps_fails "$WORK"/pid.* "$WORK"/procs.* "$WORK"/phase
  "$@"
  set +e; out=$(bash "$WORK/reload.sh" 2>&1); rc=$?; set -e
  if { [ "$expect" = pass ] && [ "$rc" = 0 ]; } || { [ "$expect" = fail ] && [ "$rc" != 0 ]; }; then
    # the LAST line only: a message printed from a subshell before it exited is still in $out, so
    # matching anywhere let a scenario pass on another check's diagnosis — the refusal that ENDED the
    # script is the last line
    if [ -n "$want" ] && ! printf '%s' "$out" | tail -1 | grep -q -- "$want"; then
      printf '  FAIL %-54s rc=%s refused for the WRONG reason (wanted %s): %s\n' "$name" "$rc" "$want" "$(printf '%s' "$out" | tail -1)"
      fail=1
      return
    fi
    printf '  ok   %-54s rc=%s %s\n' "$name" "$rc" "$(printf '%s' "$out" | tail -1 | cut -c1-70)"
  else
    printf '  FAIL %-54s rc=%s (expected %s) %s\n' "$name" "$rc" "$expect" "$(printf '%s' "$out" | tail -1)"
    fail=1
  fi
}

# the happy path: a unit running as pid 100 is replaced by pid 200
setup_replaced() {
  echo "100 0 $LBL" > "$WORK/pid.loaded"; : > "$WORK/pid.unloaded"; echo "200 0 $LBL" > "$WORK/pid.running"
  echo "100 $MDIR/producer.properties" > "$WORK/procs.loaded"; : > "$WORK/procs.unloaded"
  echo "200 $MDIR/producer.properties" > "$WORK/procs.running"
  echo loaded > "$WORK/phase"
}
scenario "a running unit is replaced by a NEW pid" pass setup_replaced

# the scenario this guard exists for: unload fails, the old process survives, load fails
setup_unload_and_load_fail() { setup_replaced; : > "$WORK/unload_fails"; : > "$WORK/load_fails"; }
scenario "unload fails + old pid remains + load fails" fail setup_unload_and_load_fail

# load alone fails: the unit is down, and that is not a success
setup_load_fails() { setup_replaced; : > "$WORK/load_fails"; }
scenario "load fails after a clean unload" fail setup_load_fails

# the load reports success but the unit is not running
setup_crashed() { setup_replaced; echo "- 0 $LBL" > "$WORK/pid.running"; }
scenario "loaded but no numeric pid (crashed on start)" fail setup_crashed

# the same process as before: nothing actually restarted, so the new config is not in effect
setup_same_pid() { setup_replaced; echo "100 0 $LBL" > "$WORK/pid.running"; }
scenario "the pid is unchanged after the reload" fail want="the same process as before" setup_same_pid

# a pid that belongs to another unit's config
setup_wrong_binding() { setup_replaced; : > "$WORK/wrong_binding"; }
scenario "the new pid reads ANOTHER unit's producer.config" fail setup_wrong_binding

# unload REPORTS success but the old process lingers, and the load then fails: the shape where a
# "load failed" diagnosis and an "unchanged pid" diagnosis are the same refusal
setup_lingering() { setup_replaced; echo "100 0 $LBL" > "$WORK/pid.unloaded"; echo "100 0 $LBL" > "$WORK/pid.running"
  echo "100 $MDIR/producer.properties" > "$WORK/procs.unloaded"; : > "$WORK/load_fails"; }
scenario "unload returns 0, the old pid lingers, load fails" fail setup_lingering

# the launchd REGISTRATION is gone but the old java process is alive: loading now would give the topic
# a SECOND mirror, and a check that only reads `launchctl list` cannot see it
setup_deregistered_but_alive() { setup_replaced; : > "$WORK/pid.unloaded"; echo "100 $MDIR/producer.properties" > "$WORK/procs.unloaded"; }
scenario "registration gone, OLD process still publishing" fail want="would run a SECOND mirror" setup_deregistered_but_alive

# the load succeeded and the new process is bound, but the old one never died: two mirrors, one topic
setup_two_running() { setup_replaced; printf '100 %s\n200 %s\n' "$MDIR/producer.properties" "$MDIR/producer.properties" > "$WORK/procs.running"; }
scenario "TWO processes publishing after the load" fail want="duplicate every record" setup_two_running

# the process table cannot be read: empty output would say "nothing is running", and loading would
# then start a SECOND mirror — an unreadable table is not an empty one
setup_ps_fails() { setup_replaced; : > "$WORK/ps_fails"; }
scenario "ps fails (the process table is unreadable)" fail want="cannot enumerate processes" setup_ps_fails

# a first install: nothing was running, and a new pid is a success
setup_first_install() {
  : > "$WORK/pid.loaded"; : > "$WORK/pid.unloaded"; echo "300 0 $LBL" > "$WORK/pid.running"
  : > "$WORK/procs.loaded"; : > "$WORK/procs.unloaded"; echo "300 $MDIR/producer.properties" > "$WORK/procs.running"
  echo loaded > "$WORK/phase"
}
scenario "a FIRST install (no old process) starts a pid" pass setup_first_install

[ "$fail" = 0 ] || { echo "mirror reload guard: FAILED"; exit 1; }
echo "mirror reload guard: 11 scenarios behave as specified"
