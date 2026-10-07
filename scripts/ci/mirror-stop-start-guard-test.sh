#!/usr/bin/env bash
# The stop/start guards (ansible/templates/mirror-{stop,start}.sh.j2) are the only part of the Ansible mirror path
# that stops and starts a LIVE mirror, and the scenario it exists for is the quiet one: `launchctl
# unload` of a unit that is not loaded is a no-op, and a `launchctl load` that FAILS leaves the OLD
# process running — whose command line still names this unit's producer.properties. A guard that only
# checks "a pid exists and is bound to the right config" reports success there while the mirror keeps
# publishing the pre-change settings.
#
# So drive them against stub launchctl/ps binaries, once per way those two can be lied to: unload
# failing, the registration gone while the process lives, `launchctl list` unreadable, `ps`
# unreadable, load failing, no pid, the SAME pid, a pid that is not the publisher, a publisher that is
# not this unit, and two publishers at once — plus the clean paths (a replacement and a first
# install). What it does NOT cover, because no stub can: launchd's own timing, a unit that dies after
# the settle, and anything the real `ps` reports that this stub's two forms do not.
set -euo pipefail
cd "$(dirname "$0")/../.."
STOP_TPL=ansible/templates/mirror-stop.sh.j2
START_TPL=ansible/templates/mirror-start.sh.j2
for t in "$STOP_TPL" "$START_TPL"; do [ -r "$t" ] || { echo "FAIL: $t not readable"; exit 1; }; done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
MDIR="$WORK/unit"; mkdir -p "$MDIR"
LBL=com.optionsedge.test-mirror
PL="$WORK/$LBL.plist"; : > "$PL"

# render the template the way ansible would for one unit (these are its only expressions)
# The templates shell-QUOTE every value they render (Ansible's quote filter), so the substitutions
# here must match those expressions, not the bare ones — a stale list is caught below by the
# unresolved-expression check rather than silently producing a script that tests nothing.
render_tpl() { # template  target
  sed -E -e "s#\{\{ item\.plist \| quote \}\}#'$PL'#g" \
         -e "s#\{\{ item\.label \| quote \}\}#'$LBL'#g" \
         -e "s#\{\{ item\.mdir \| quote \}\}#'$MDIR'#g" \
         -e "s#\{\{ item\.target \| quote \}\}#'$2'#g" \
         -e "s#\{\{ \(playbook_dir ~ \"/\.\.\"\) \| quote \}\}#'$PWD'#g" \
         -e "s#\{\{ item\.topic[^}]*\}\}#'es\\\\.test\\\\.topic'#g" \
         "$1"
}

# Two renderings per phase: a DEV unit (the production lock must not fire) and a PRODUCTION one (it
# must, and must refuse from any checkout that is not the permitted commit on origin/main).
for pair in "stop:$STOP_TPL" "start:$START_TPL"; do
  n=${pair%%:*}; t=${pair#*:}
  render_tpl "$t" "127.0.0.1:19092" > "$WORK/$n.sh"
  chmod +x "$WORK/$n.sh"
  grep -q '{{' "$WORK/$n.sh" && { echo "FAIL: the rendered $n guard still has unresolved expressions — this test's substitution list is stale:"; grep -n '{{' "$WORK/$n.sh" | head -3; exit 1; }
  render_tpl "$t" "192.168.100.252:9092" > "$WORK/$n-prod.sh"
  chmod +x "$WORK/$n-prod.sh"
  grep -q '{{' "$WORK/$n-prod.sh" && { echo "FAIL: the rendered $n-prod guard still has unresolved expressions:"; grep -n '{{' "$WORK/$n-prod.sh" | head -3; exit 1; }
done

# stub launchctl and ps: behaviour driven by marker files in $WORK
mkdir -p "$WORK/bin"
cat > "$WORK/bin/launchctl" <<'L'
#!/usr/bin/env bash
W="$(dirname "$0")/.."
# an unreadable job table: every subcommand fails, which is what a broken launchctl looks like
[ -e "$W/launchctl_fails" ] && exit 1
case "${1:-}" in
  list)   [ -e "$W/list_fails" ] && exit 1; cat "$W/pid.$(cat "$W/phase")" 2>/dev/null; exit 0 ;;
  unload) echo unload >> "$W/unloads"; echo loaded > "$W/phase"; [ -e "$W/unload_fails" ] || echo unloaded > "$W/phase"; exit "$([ -e "$W/unload_fails" ] && echo 1 || echo 0)" ;;
  load)   [ -e "$W/load_fails" ] && exit 1; echo running > "$W/phase"; exit 0 ;;
esac
exit 0
L
chmod +x "$WORK/bin/launchctl"
cat > "$WORK/bin/ps" <<P
#!/usr/bin/env bash
[ -e "$WORK/ps_fails" ] && exit 1
# the ONE form the guards use: 'ps -axo pid=,command='. They used to ask 'ps -p <pid> -o command='
# as well, which answered a narrower question — it could not see a SECOND mirror on the same topic —
# so any other form reaching this stub is a mistake and is reported rather than answered.
W="$WORK"; MDIR="$MDIR"
case "\$*" in
  *-axo*)
    # every process currently publishing SOME unit's config, per phase. The command line carries the
    # whitelist too, so the start guard can look for another unit mirroring the SAME topic.
    while read -r pid path; do
      [ -n "\$pid" ] || continue
      # UNQUOTED, as a real ps prints argv: the shell strips run-mirror.sh's quotes, and a stub that
      # kept them certified a check that matched nothing on a real machine
      echo "\$pid java kafka.tools.MirrorMaker --producer.config \$path --whitelist es\\.test\\.topic --num.streams 1"
    done < "\$W/procs.\$(cat "\$W/phase")" 2>/dev/null
    ;;
  *)
    # the guards ask only for the full table now; this form stays so an accidental use is visible
    echo "ps: unsupported form in this stub: \$*" >&2; exit 2
    ;;
esac
P
chmod +x "$WORK/bin/ps"
export PATH="$WORK/bin:$PATH" OE_RELOAD_SETTLE=0

fail=0
scenario() { # phase(stop|start|both)  name  expect(pass|fail)  [want=<substring>]  setup...
  local phase="$1" name="$2" expect="$3"; shift 3
  local want=""
  case "${1:-}" in want=*) want="${1#want=}"; shift ;; esac
  rm -f "$WORK"/unload_fails "$WORK"/load_fails "$WORK"/ps_fails "$WORK"/launchctl_fails "$WORK"/list_fails "$WORK"/unloads "$WORK"/pid.* "$WORK"/procs.* "$WORK"/phase
  "$@"
  set +e
  case "$phase" in
    stop)  out=$(bash "$WORK/stop.sh" 2>&1); rc=$? ;;
    stop-prod) out=$(bash "$WORK/stop-prod.sh" 2>&1); rc=$? ;;
    start) out=$(OE_OLD_PID="${OE_OLD_PID_FOR_TEST:-}" bash "$WORK/start.sh" 2>&1); rc=$?
           # one stream, so the order is preserved and the CAUSE stays last; kept on disk as well so
           # a scenario can assert what the undo reported
           printf '%s\n' "$out" > "$WORK/last_err" ;;
    both)  out=$(bash "$WORK/stop.sh" 2>&1); rc=$?
           if [ "$rc" = 0 ]; then
             old=$(printf '%s' "$out" | sed -n 's/.*was pid \([0-9]*\).*/\1/p')
             out2=$(OE_OLD_PID="$old" bash "$WORK/start.sh" 2>&1); rc=$?
             out="$out
$out2"
           fi ;;
  esac
  set -e
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

# ---- the two unit states every scenario starts from ----
# a unit running as pid 100 which, after a stop and a load, comes back as pid 200
setup_replaced() {
  echo "100 0 $LBL" > "$WORK/pid.loaded"; : > "$WORK/pid.unloaded"; echo "200 0 $LBL" > "$WORK/pid.running"
  echo "100 $MDIR/producer.properties" > "$WORK/procs.loaded"; : > "$WORK/procs.unloaded"
  echo "200 $MDIR/producer.properties" > "$WORK/procs.running"
  echo loaded > "$WORK/phase"
}
# a FIRST install: nothing is running, and the load brings up pid 300
setup_first_install() {
  : > "$WORK/pid.loaded"; : > "$WORK/pid.unloaded"; echo "300 0 $LBL" > "$WORK/pid.running"
  : > "$WORK/procs.loaded"; : > "$WORK/procs.unloaded"; echo "300 $MDIR/producer.properties" > "$WORK/procs.running"
  echo loaded > "$WORK/phase"
}

scenario both "stop then start replaces the process" pass want="started: pid 100 -> 200" setup_replaced

# ---- PHASE 1 (stop): nothing may be written unless the unit is really stopped ----
setup_unload_fails() { setup_replaced; : > "$WORK/unload_fails"; }
scenario stop "unload fails and the unit stays loaded" fail want="still LOADED after unload" setup_unload_fails

setup_deregistered_but_alive() { setup_replaced; : > "$WORK/pid.unloaded"; echo "100 $MDIR/producer.properties" > "$WORK/procs.unloaded"; }
scenario stop "registration gone but the OLD process is alive" fail want="still publishing this unit" setup_deregistered_but_alive

setup_ps_fails() { setup_replaced; : > "$WORK/ps_fails"; }
scenario stop "ps fails (the process table is unreadable)" fail want="cannot enumerate processes" setup_ps_fails

# an unreadable JOB table is not an unloaded unit: concluding "absent" there would write files under a
# still-registered KeepAlive job
setup_launchctl_fails() { setup_replaced; : > "$WORK/launchctl_fails"; }
scenario stop "launchctl list fails" fail want="cannot read launchctl list" setup_launchctl_fails

scenario stop "a unit that was not running stops cleanly" pass want="stopped: was pid none" setup_first_install

# ---- the production lock, inside the script that touches the unit first ----
# A DEV unit never consults the guard (the cases above are all dev renderings and they pass). A
# PRODUCTION unit does, and from this checkout — not the permitted commit on origin/main, and with an
# empty PERMITTED_SHA — it must refuse before stopping or writing anything. This is the lock an
# Ansible extra var cannot wave through.
scenario stop-prod "a PRODUCTION unit consults the permitted-commit guard" fail want="REFUSED this PRODUCTION unit" setup_replaced

# ---- PHASE 2 (start): the unit must come up on the new files, alone ----
scenario start "a first install starts a pid" pass want="started: pid none -> 300" setup_first_install

setup_load_fails() { setup_replaced; echo unloaded > "$WORK/phase"; : > "$WORK/load_fails"; }
scenario start "launchctl load fails" fail want="the unit is DOWN" setup_load_fails

setup_crashed() { setup_replaced; echo unloaded > "$WORK/phase"; echo "- 0 $LBL" > "$WORK/pid.running"; }
scenario start "loaded but no numeric pid" fail want="loaded but not running" setup_crashed

setup_same_pid() { setup_replaced; echo unloaded > "$WORK/phase"; echo "100 0 $LBL" > "$WORK/pid.running"; echo "100 $MDIR/producer.properties" > "$WORK/procs.running"; }
OE_OLD_PID_FOR_TEST=100 scenario start "the same pid phase 1 was to have stopped" fail want="the same process PHASE 1" setup_same_pid

setup_nothing_on_this_config() { setup_replaced; echo unloaded > "$WORK/phase"; echo "200 /some/other/unit/producer.properties" > "$WORK/procs.running"; }
scenario start "the started process reads ANOTHER unit config" fail want="one mirror per topic" setup_nothing_on_this_config

setup_pid_mismatch() { setup_replaced; echo unloaded > "$WORK/phase"; echo "999 $MDIR/producer.properties" > "$WORK/procs.running"; }
scenario start "launchd reports a pid that is not the publisher" fail want="but the process publishing this config" setup_pid_mismatch

setup_two_running() { setup_replaced; echo unloaded > "$WORK/phase"; printf '100 %s\n200 %s\n' "$MDIR/producer.properties" "$MDIR/producer.properties" > "$WORK/procs.running"; }
scenario start "TWO processes publishing after the load" fail want="duplicate every record" setup_two_running

setup_start_ps_fails() { setup_replaced; echo unloaded > "$WORK/phase"; : > "$WORK/ps_fails"; }
scenario start "ps fails while proving it is the only one" fail want="cannot enumerate processes" setup_start_ps_fails

# the load succeeds but the job table then cannot be read: without pid_of checking launchctl's
# status this returned empty, and an empty pid is not the same question as an unreadable table
setup_start_list_fails() { setup_replaced; echo unloaded > "$WORK/phase"; : > "$WORK/list_fails"; }
scenario start "launchctl list fails after a successful load" fail want="cannot read launchctl list" setup_start_list_fails

# ---- a post-load refusal must put the load BACK ----
# Every refusal after the load leaves a KeepAlive job running otherwise: a process nobody verified.
setup_two_running_cleanup() { setup_two_running; }
scenario start "a post-load refusal unloads what it loaded" fail want="duplicate every record" setup_two_running_cleanup
if [ -e "$WORK/unloads" ]; then
  printf '  ok   %-54s %s\n' "...and the unload really happened" "$(wc -l < "$WORK/unloads" | tr -d ' ') unload call(s)"
else
  printf '  FAIL %-54s %s\n' "...and the unload really happened" "the guard never called launchctl unload"; fail=1
fi

# the undo itself has to be VERIFIED: a deregistered-but-alive process keeps publishing, and an
# unreadable table afterwards is not a clean undo either
setup_undo_leaves_process() {
  setup_two_running; echo unloaded > "$WORK/phase"
  # after the unload the job is gone from launchctl, but a process is still publishing this config
  printf '100 %s\n' "$MDIR/producer.properties" > "$WORK/procs.unloaded"
  printf '100 %s\n200 %s\n' "$MDIR/producer.properties" "$MDIR/producer.properties" > "$WORK/procs.running"
}
scenario start "the undo leaves a process publishing" fail want="duplicate every record" setup_undo_leaves_process
if grep -q 'STILL there after the unload' "$WORK/last_err" 2>/dev/null; then
  printf '  ok   %-54s %s\n' "...and the undo reports it as unverified" "named in the diagnosis"
else
  printf '  FAIL %-54s %s\n' "...and the undo reports it as unverified" "the undo claimed a clean result"; fail=1
fi

# ---- ANOTHER unit mirroring the same topic to the same target ----
# The count above is per CONFIG PATH, so it cannot see a second unit installed elsewhere that mirrors
# the same topic to the same broker — two such processes duplicate every record. Driven on its own
# fixture: this one needs the whitelist and a second unit's producer config, not the phase machinery
# the scenarios above share.
dup_check() {
  local D; D=$(mktemp -d)
  mkdir -p "$D/bin" "$D/unit" "$D/other"; : > "$D/u.plist"
  printf 'bootstrap.servers=127.0.0.1:19092\n' > "$D/other/producer.properties"
  PL="$D/u.plist" LBL=L MDIR="$D/unit" render_tpl "$START_TPL" "127.0.0.1:19092" > "$D/start.sh"
  if grep -q '{{' "$D/start.sh"; then echo "  FAIL duplicate-mirror check: unresolved expressions in the rendering"; fail=1; rm -rf "$D"; return; fi
  cat > "$D/bin/launchctl" <<L
#!/usr/bin/env bash
[ "\$1" = list ] && echo "200 0 L"
exit 0
L
  cat > "$D/bin/ps" <<P
#!/usr/bin/env bash
echo "200 java kafka.tools.MirrorMaker --producer.config $D/unit/producer.properties --whitelist es\\.test\\.topic --num.streams 1"
[ -e "$D/with_other" ] && echo "300 java kafka.tools.MirrorMaker --producer.config $D/other/producer.properties --whitelist es\\.test\\.topic --num.streams 1"
exit 0
P
  chmod +x "$D/bin"/*
  local out rc
  set +e; out=$(PATH="$D/bin:$PATH" OE_RELOAD_SETTLE=0 bash "$D/start.sh" 2>&1); rc=$?; set -e
  if [ "$rc" = 0 ]; then printf '  ok   %-54s rc=%s\n' "this unit alone is accepted" "$rc"
  else printf '  FAIL %-54s rc=%s %s\n' "this unit alone is accepted" "$rc" "$out"; fail=1; fi
  : > "$D/with_other"
  set +e; out=$(PATH="$D/bin:$PATH" OE_RELOAD_SETTLE=0 bash "$D/start.sh" 2>&1); rc=$?; set -e
  if [ "$rc" != 0 ] && printf '%s' "$out" | tail -1 | grep -q 'from a different unit directory'; then
    printf '  ok   %-54s rc=%s\n' "ANOTHER unit on this topic and target is refused" "$rc"
  else
    printf '  FAIL %-54s rc=%s %s\n' "ANOTHER unit on this topic and target is refused" "$rc" "$out"; fail=1
  fi
  rm -rf "$D"
}
dup_check

[ "$fail" = 0 ] || { echo "mirror stop/start guards: FAILED"; exit 1; }
echo "mirror stop/start guards: 18 scenarios, the post-load cleanup (verified both ways) and the duplicate-mirror check behave as specified"
