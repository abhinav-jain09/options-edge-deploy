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
    LOAD_FAILS="${LOAD_FAILS:-}" OE_WATCHDOG_INSTALL_LOCK="${OE_WATCHDOG_INSTALL_LOCK:-}" OE_WATCHDOG_BACKUP_DIR="${OE_WATCHDOG_BACKUP_DIR:-}" \
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

echo "6. a failed install hands over a recovery script that actually restores the previous state"
# r27 #1. This case used to check only that recovery TEXT was printed, and the text was WRONG: it
# never removed files that had no predecessor, left a new plist where there had been none, and copied
# the backed-up plist into the ops directory. Checking that advice exists is not checking that it
# works, so this now RUNS it and compares the host to what it was.
# (a) recovery after a failed install ON TOP OF an existing one restores that one byte-for-byte.
stage 0
printf '#!/usr/bin/env bash\necho "I am the previous installation"\n' > "$TMP/dest/calibration-progress-watch.sh"
printf 'previous-alert\n' > "$TMP/dest/oe-alert.sh"
cp "$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist" "$TMP/agents/"
mkdir -p "$TMP/before"; cp -R "$TMP/dest/." "$TMP/before/"; cp "$TMP/agents/com.optionsedge.calibration-progress-watch.plist" "$TMP/before.plist"
install_run
[ "$RC" -ne 0 ] && ok "zero registered agents fails the install" || bad "it exited 0 with nothing scheduled"
RESTORE="$(grep -oE '[^ ]*/restore\.sh' "$TMP/out" | head -1)"
[ -x "$RESTORE" ] && ok "it handed over an executable recovery script" || bad "no runnable recovery script was produced: $(tail -3 "$TMP/out")"
PATH="$TMP/bin:$PATH" bash "$RESTORE" >/dev/null 2>&1
[ "$?" -eq 0 ] && ok "the recovery script reported success" || bad "recovery exited non-zero and the case used to ignore that"
if cmp -s "$TMP/dest/calibration-progress-watch.sh" "$TMP/before/calibration-progress-watch.sh" \
   && cmp -s "$TMP/dest/oe-alert.sh" "$TMP/before/oe-alert.sh" \
   && cmp -s "$TMP/agents/com.optionsedge.calibration-progress-watch.plist" "$TMP/before.plist"; then
  ok "running it restored the previous files and plist byte-for-byte"
else
  bad "the recovery script did not restore the previous state"
fi
[ ! -f "$TMP/dest/com.optionsedge.calibration-progress-watch.plist" ] \
  && ok "and it did not drop the plist into the ops directory" \
  || bad "recovery copied the plist into $TMP/dest"

# (b) recovery after a failed FIRST install restores ABSENCE — the case the printed advice never had.
stage 0
rm -rf "$TMP/dest" "$TMP/agents"; mkdir -p "$TMP/dest" "$TMP/agents"
install_run
RESTORE="$(grep -oE '[^ ]*/restore\.sh' "$TMP/out" | head -1)"
PATH="$TMP/bin:$PATH" bash "$RESTORE" >/dev/null 2>&1
if [ -z "$(ls -A "$TMP/agents" 2>/dev/null)" ] \
   && ! ls "$TMP/dest" 2>/dev/null | grep -qv '^\.watchdog-backup'; then
  ok "there was no watchdog before, and there is none after recovery"
else
  bad "recovery left files behind on a host that had none: $(ls -A "$TMP/dest" "$TMP/agents" 2>/dev/null | tr '\n' ' ')"
fi

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
# The lock path is DERIVED from the destination, not passed in — an env-selectable lock is not a
# lock, because two installers simply pick two paths (r27 #2). So this computes the same path the
# installer will, holds it, and requires the second run to refuse.
stage 1
HELD="${TMPDIR:-/tmp}/oe-install-dev-mac-watchdog.$(printf '%s' "$TMP/dest" | cksum | cut -d' ' -f1).lock"
rm -rf "$HELD"; mkdir -p "$HELD"; printf 'someone-else\n' > "$HELD/owner"
install_run
[ "$RC" -ne 0 ] && ok "a held lock stops the second installer" || bad "two installers ran concurrently"
grep -q "another installer holds" "$TMP/out" && ok "and it said why" || bad "the refusal did not name the lock"
[ -d "$HELD" ] && [ "$(cat "$HELD/owner" 2>/dev/null)" = "someone-else" ] \
  && ok "and it did not release a lock it does not own" || bad "the refusing run removed someone else's lock"
# The lock must not be selectable from outside: an installer that can be handed a different path is
# not locked at all, because a second run simply picks another one.
OE_WATCHDOG_INSTALL_LOCK="$TMP/somewhere-else.lock" install_run
[ "$RC" -ne 0 ] \
  && ok "and it cannot be pointed at a different lock to get past the held one" \
  || bad "OE_WATCHDOG_INSTALL_LOCK let a second installer run while the real lock was held"
rm -rf "$HELD"

echo "14. the caller cannot change the environment the verification runs in"
# r26 #4. The installer used to inject the caller's ENV/ARCHIVE_DIR/CHECK_DATE AFTER the plist's own
# keys, so an operator could verify a different root or date than the agent will ever receive — and
# override the plist's ENV. Point the plist at a REAL corpus root and the caller at a fake one: the
# verification must use the plist's.
stage 1
ENV=dev ARCHIVE_DIR=/nonexistent-caller-root CHECK_DATE=1999-01-01 install_run
[ "$RC" -eq 0 ] && ok "the install succeeded using the plist's environment" || bad "caller variables reached the verification (exit $RC): $(tail -3 "$TMP/out")"
grep -q "nonexistent-caller-root" "$TMP/out" && bad "the caller's ARCHIVE_DIR reached the watchdog" || ok "the caller's archive root never appeared"

echo "15. a plist that parses but is not a usable launchd job is refused BEFORE anything is created"
# r27 #3. plutil -lint checks XML, not the launchd schema, and the Label used to be read only after
# the backups were taken — so a parseable plist with no Label touched the host and then aborted.
stage 1
python3 - "$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist" <<'PYEOF'
import plistlib, sys
d = plistlib.load(open(sys.argv[1], 'rb'))
del d['Label']
plistlib.dump(d, open(sys.argv[1], 'wb'))
PYEOF
install_run
[ "$RC" -ne 0 ] && ok "a plist with no Label is refused" || bad "it installed an agent with no label"
dest_empty && ok "and the host was never touched" || bad "it created directories or backups before failing"

echo "16. a STALE backup refuses to restore, instead of silently undoing a later install"
# Found by probing, not by review. With two backup directories present, the OLDER restore.sh removed
# the watchdog entirely and printed "restored" — an operator who picks the wrong directory uninstalls
# the thing and is told it worked.
stage 1
install_run
[ "$RC" -eq 0 ] || bad "case 16 could not complete the first install: $(tail -2 "$TMP/out")"
OLD_BACKUP="$(ls -d "$TMP/dest"/.watchdog-backup-* 2>/dev/null | head -1)"
sleep 1
install_run
[ "$RC" -eq 0 ] || bad "case 16 could not complete the second install"
[ "$(ls -d "$TMP/dest"/.watchdog-backup-* 2>/dev/null | wc -l | tr -d ' ')" -eq 2 ] \
  && ok "two installs leave two dated backups" || bad "the second install did not make its own backup"
PATH="$TMP/bin:$PATH" bash "$OLD_BACKUP/restore.sh" > "$TMP/stale" 2>&1; _sr=$?
[ "$_sr" -eq 3 ] && ok "the older backup refuses to restore (exit 3, its own code)" || bad "a stale backup did not refuse cleanly (exit $_sr)"
grep -q "MIXED state" "$TMP/stale" && bad "a refusal that touched nothing claimed a mixed state" || ok "and a refusal does not claim the host is mixed"
grep -q "NEWER backup exists" "$TMP/stale" && ok "and it said a newer one exists" || bad "the refusal did not explain why"
if [ -f "$TMP/dest/calibration-progress-watch.sh" ] && [ -f "$TMP/agents/com.optionsedge.calibration-progress-watch.plist" ]; then
  ok "the installed watchdog is still there"
else
  bad "the stale restore removed the installed watchdog anyway"
fi
# FORCE is the deliberate escape hatch, and it must actually work.
PATH="$TMP/bin:$PATH" FORCE=1 bash "$OLD_BACKUP/restore.sh" >/dev/null 2>&1
[ ! -f "$TMP/agents/com.optionsedge.calibration-progress-watch.plist" ] \
  && ok "FORCE=1 still performs the restore it refused" || bad "FORCE=1 did not override the refusal"

echo "17. recovery that cannot finish says so, instead of reporting success"
# r28 #1. The generated script omitted set -e, so a failed cp/rm/load fell through to the success
# message and exited 0 — recovery claiming it restored the previous installation while leaving a mixed
# one. And the old case discarded the exit status, so the failure mode was unbound twice over.
stage 0
printf '#!/usr/bin/env bash\necho "previous"\n' > "$TMP/dest/calibration-progress-watch.sh"
install_run
RESTORE="$(grep -oE '[^ ]*/restore\.sh' "$TMP/out" | head -1)"
[ -x "$RESTORE" ] || bad "case 17 got no recovery script"
# Remove a file the script must copy back: the cp now fails partway through.
BDIR="$(dirname "$RESTORE")"; rm -f "$BDIR/calibration-progress-watch.sh"
PATH="$TMP/bin:$PATH" bash "$RESTORE" > "$TMP/rfail" 2>&1; _rf=$?
[ "$_rf" -ne 0 ] && ok "a recovery that cannot complete exits non-zero" || bad "a broken recovery reported success"
grep -q "MIXED state" "$TMP/rfail" && ok "and it says the host is in a mixed state" || bad "it did not warn that the host is mixed"
grep -q "restored the previous installation" "$TMP/rfail" \
  && bad "it printed the success line anyway" || ok "and it never printed the success line"

echo "18. a backup directory that already exists is REFUSED, not written into"
# r28 #2. The default had one-second resolution and the caller could hand back a path already in use;
# mkdir -p plus overwriting copies then replaced an earlier backup AND its restore.sh — destroying the
# only record of the state that install replaced.
stage 1
mkdir -p "$TMP/reused-backup"; printf 'precious\n' > "$TMP/reused-backup/marker"
OE_WATCHDOG_BACKUP_DIR="$TMP/reused-backup" install_run
[ "$RC" -ne 0 ] && ok "an existing backup directory stops the install" || bad "it wrote into an existing backup directory"
[ -f "$TMP/reused-backup/marker" ] && ok "and the earlier backup's contents are untouched" || bad "it overwrote the earlier backup"

echo "19. every refusal in the watchdog uses the phrase the installer blocks on"
_ref=$(grep -cE 'alert "calibration watchdog cannot run:' "$HERE/calibration-progress-watch.sh")
_all=$(grep -cE 'alert "calibration watchdog' "$HERE/calibration-progress-watch.sh")
[ "$_ref" -eq "$_all" ] && [ "$_ref" -gt 0 ] \
  && ok "all $_ref watchdog refusals share the phrase the installer blocks on" \
  || bad "$((_all - _ref)) refusal(s) do not say 'cannot run', so the installer would not block on them"

echo
if [ "$fails" -eq 0 ]; then echo "=== install-dev-mac-watchdog-test: OK ==="; exit 0; fi
echo "=== install-dev-mac-watchdog-test: $fails problem(s) ===" >&2; exit 1
