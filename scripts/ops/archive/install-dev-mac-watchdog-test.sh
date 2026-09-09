#!/usr/bin/env bash
# The installer's job is to REFUSE anything it cannot vouch for, and to say exactly what it did when
# it cannot finish. That is what this tests: it runs the real installer against a copied archive
# directory with a stubbed launchctl, breaks one thing at a time, and asserts on the host state.
#
# Fourteen defects across four review rounds lived in this install path, and every one of them was in
# code written to fix the previous one. The rollback that caused five of them is gone; what is left
# has to be bound, including the refusals that replaced it.
#
# NOTE ON HOW THE ENVIRONMENT REACHES THE WATCHDOG: it does not come from here. The installer verifies
# under launchd's environment only, so this test puts ARCHIVE_DIR and CHECK_DATE into the staged
# plist's EnvironmentVariables — the same channel the scheduled agent uses. An earlier version passed
# them as caller variables, which is precisely the leak r26 #4 found.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

stage() { # stage <registered-count> [label-to-report]
  TMP="$(mktemp -d)"
  cp -R "$HERE" "$TMP/archive"
  mkdir -p "$TMP/bin" "$TMP/dest" "$TMP/agents" "$TMP/nas/calibration-runs/prod/UNFROZEN/2099-01-01/progress"
  cat > "$TMP/bin/launchctl" <<EOF
#!/usr/bin/env bash
echo "launchctl \$*" >> "$TMP/calls"
if [ "\${1:-}" = "list" ]; then
  i=0; while [ "\$i" -lt "$1" ]; do printf -- "-\t0\t%s\n" "${2:-com.optionsedge.calibration-progress-watch}"; i=\$((i+1)); done
fi
if [ "\${1:-}" = "load" ] && [ -n "\${LOAD_FAILS:-}" ]; then echo "Load failed" >&2; exit 1; fi
exit 0
EOF
  chmod +x "$TMP/bin/launchctl"; : > "$TMP/calls"
  # A plist consistent with THIS destination, carrying the environment the watchdog needs. Before
  # r24 the test overrode OE_OPS_DIR while the plist still named /Users/abhinav/oe-ops, so the
  # healthy case passed while verifying a copy launchd would never run.
  python3 - "$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist" \
            "${TEST_INTERP:-/bin/bash}" "$TMP/dest/calibration-progress-watch.sh" \
            "$TMP/nas" "${TEST_PATH_OVERRIDE:-}" <<'PYEOF'
import plistlib, sys, os
p, interp, script, nas, path_override = sys.argv[1:6]
d = plistlib.load(open(p, 'rb'))
d['ProgramArguments'] = [interp, script]
env = d.setdefault('EnvironmentVariables', {})
env['ENV'] = 'prod'
env['ARCHIVE_DIR'] = nas
env['CHECK_DATE'] = '2026-07-02'
if path_override:
    env['PATH'] = path_override
plistlib.dump(d, open(p, 'wb'))
PYEOF
}
install_run() {
  PATH="$TMP/bin:$PATH" OE_OPS_DIR="$TMP/dest" LAUNCH_AGENTS_DIR="$TMP/agents" \
    OE_WATCHDOG_INSTALL_LOCK="${LOCK_OVERRIDE:-$TMP/lock}" LOAD_FAILS="${LOAD_FAILS:-}" \
    bash "$TMP/archive/install-dev-mac-watchdog.sh" > "$TMP/out" 2>&1
  RC=$?
}
loaded()   { grep -q 'launchctl load' "$TMP/calls" 2>/dev/null; }
dest_empty() { [ -z "$(ls -A "$TMP/dest" 2>/dev/null)" ] && [ -z "$(ls -A "$TMP/agents" 2>/dev/null)" ]; }

echo "1. a healthy install succeeds and registers exactly one agent"
stage 1; install_run
[ "$RC" -eq 0 ] && ok "the installer completed" || bad "healthy install failed (exit $RC): $(tail -3 "$TMP/out")"
loaded && ok "and it loaded the agent" || bad "it finished without loading the agent"

echo "2. a watchdog that dies on startup is REFUSED, and nothing is written"
stage 1
printf '#!/usr/bin/env bash\necho "Traceback (most recent call last)"; exit 3\n' > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "the installer refused (exit $RC)" || bad "a watchdog emitting a traceback was accepted"
dest_empty && ok "and the host was never touched" || bad "it wrote to the host before verifying"
grep -q "did not start" "$TMP/out" && ok "it said the watchdog did not start" || bad "the refusal did not name the cause"

echo "3. a watchdog that produces neither verdict nor alert is REFUSED"
stage 1
printf '#!/usr/bin/env bash\necho "nothing to say"\nexit 1\n' > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "silence is not a verdict" || bad "an empty run was accepted as verification"
dest_empty && ok "and nothing was written" || bad "it wrote to the host anyway"

echo "4. a deliberate refusal by the watchdog blocks the install"
stage 1
printf '#!/usr/bin/env bash\necho "ALERT: calibration watchdog cannot run: no archive root on this host"\nexit 1\n' \
  > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "a self-declared refusal is not installed" || bad "a watchdog that says it cannot run was installed"
dest_empty && ok "and nothing was written" || bad "it wrote to the host anyway"

echo "5. a refusal leaves an EXISTING installation byte-identical"
# Verification happens before any host write, so a refusal cannot disturb what is already there.
stage 1
printf '#!/usr/bin/env bash\necho "I am the previous installation"\n' > "$TMP/dest/calibration-progress-watch.sh"
cp "$TMP/dest/calibration-progress-watch.sh" "$TMP/previous"
printf '#!/usr/bin/env bash\necho "Traceback (most recent call last)"; exit 3\n' > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "the installer refused" || bad "a broken watchdog was installed"
cmp -s "$TMP/dest/calibration-progress-watch.sh" "$TMP/previous" \
  && ok "the previous watchdog is untouched" || bad "a refusal overwrote the working installation"

echo "6. the agent failing to register is an INSTALL FAILURE that says where the old copies are"
stage 0; install_run
[ "$RC" -ne 0 ] && ok "zero registered agents fails the install" || bad "it exited 0 with nothing scheduled"
grep -q "the previous copies are in" "$TMP/out" && ok "and it printed the backup location" || bad "it did not say where the previous copies went"
grep -q "does NOT do that for you on purpose" "$TMP/out" \
  && ok "and said plainly that it will not roll back for you" || bad "it did not say rollback is manual"

echo "7. a LOOKALIKE agent label does not count as the agent"
stage 1 "com.optionsedge.calibration-progress-watch.backup"; install_run
[ "$RC" -ne 0 ] && ok "a similarly named agent does not satisfy the check" || bad "a lookalike label passed as the agent"

echo "8. a launchctl load that FAILS is an install failure, not a warning"
stage 1; LOAD_FAILS=1 install_run; LOAD_FAILS=""
[ "$RC" -ne 0 ] && ok "a failed load fails the install" || bad "a failed load was reported as success"
grep -q "could not load" "$TMP/out" && ok "and named the failure" || bad "it did not say the load failed"

echo "9. the plist and the destination must agree, or nothing is installed"
stage 1
python3 - "$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist" <<'PYEOF'
import plistlib, sys
d = plistlib.load(open(sys.argv[1], 'rb'))
d['ProgramArguments'] = ['/bin/bash', '/somewhere/else/calibration-progress-watch.sh']
plistlib.dump(d, open(sys.argv[1], 'wb'))
PYEOF
install_run
[ "$RC" -ne 0 ] && ok "a plist pointing elsewhere stops the install" || bad "it installed to a directory the plist does not name"
dest_empty && ok "and nothing was written" || bad "it wrote to the host anyway"

echo "10. an interpreter the plist names but the host lacks stops the install"
TEST_INTERP=/nonexistent/bash stage 1; install_run; unset TEST_INTERP
[ "$RC" -ne 0 ] && ok "a missing interpreter is caught before anything is written" || bad "it installed an agent whose interpreter does not exist"

echo "11. a plist PATH that cannot run the watchdog is caught BEFORE installing"
TEST_PATH_OVERRIDE=/nonexistent stage 1; install_run; unset TEST_PATH_OVERRIDE
[ "$RC" -ne 0 ] && ok "a PATH launchd would use but nothing can run is refused" || bad "it installed an agent that could never run under launchd"
dest_empty && ok "and nothing was written" || bad "it wrote to the host anyway"

echo "12. a destination that is not a plain file is REFUSED, not written through"
# r26 #3. A dangling symlink reads as absent to [ -f ]; cp then writes THROUGH it to wherever it
# points, and no notion of the previous state survives that.
stage 1
ln -s "$TMP/elsewhere-target" "$TMP/dest/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "a dangling symlink destination is refused" || bad "it wrote through a symlink"
[ ! -e "$TMP/elsewhere-target" ] && ok "and nothing was written to the symlink's target" || bad "it created the symlink's target"

echo "13. two installers cannot run at once"
# r26 #2. Concurrent runs could back up different intermediate states and leave a mixture that never
# existed. The lock is a directory because mkdir is atomic.
stage 1
mkdir -p "$TMP/held-lock"
LOCK_OVERRIDE="$TMP/held-lock" install_run
[ "$RC" -ne 0 ] && ok "a held lock stops the second installer" || bad "two installers ran concurrently"
grep -q "another installer holds" "$TMP/out" && ok "and it said why" || bad "the refusal did not name the lock"

echo "14. the caller cannot change the environment the verification runs in"
# r26 #4. The installer used to inject the caller's ENV/ARCHIVE_DIR/CHECK_DATE AFTER the plist's own
# keys, so an operator could verify a different root or date than the agent will ever receive — and
# override the plist's ENV. Point the plist at a REAL corpus root and the caller at a fake one: the
# verification must use the plist's.
stage 1
ENV=dev ARCHIVE_DIR=/nonexistent-caller-root CHECK_DATE=1999-01-01 install_run
[ "$RC" -eq 0 ] && ok "the install succeeded using the plist's environment" || bad "caller variables reached the verification (exit $RC): $(tail -3 "$TMP/out")"
grep -q "nonexistent-caller-root" "$TMP/out" && bad "the caller's ARCHIVE_DIR reached the watchdog" || ok "the caller's archive root never appeared"

echo "15. every refusal in the watchdog uses the phrase the installer blocks on"
_ref=$(grep -cE 'alert "calibration watchdog cannot run:' "$HERE/calibration-progress-watch.sh")
_all=$(grep -cE 'alert "calibration watchdog' "$HERE/calibration-progress-watch.sh")
[ "$_ref" -eq "$_all" ] && [ "$_ref" -gt 0 ] \
  && ok "all $_ref watchdog refusals share the phrase the installer blocks on" \
  || bad "$((_all - _ref)) refusal(s) do not say 'cannot run', so the installer would not block on them"

echo
if [ "$fails" -eq 0 ]; then echo "=== install-dev-mac-watchdog-test: OK ==="; exit 0; fi
echo "=== install-dev-mac-watchdog-test: $fails problem(s) ===" >&2; exit 1
