#!/usr/bin/env bash
# The installer's job is to REFUSE a broken watchdog, so that is what this tests: it runs the real
# installer against a copied archive directory, breaks one thing at a time, and requires a non-zero
# exit and nothing loaded. The first installer accepted almost every failure (r21 #1) and never
# required the agent to be registered (r21 #2) — a verification that cannot fail is the same defect
# as the silent watchdog it was written to prevent.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

# A stub launchctl that records what it was asked to do and answers `list` from a knob, so "the agent
# did not register" is reachable without touching this machine's real LaunchAgents.
stage() { # stage <registered-count> [label-to-report]
  TMP="$(mktemp -d)"
  cp -R "$HERE" "$TMP/archive"
  mkdir -p "$TMP/bin" "$TMP/dest" "$TMP/agents"
  cat > "$TMP/bin/launchctl" <<EOF
#!/usr/bin/env bash
echo "launchctl \$*" >> "$TMP/calls"
if [ "\${1:-}" = "list" ]; then
  i=0; while [ "\$i" -lt "$1" ]; do printf -- "-\t0\t%s\n" "${2:-com.optionsedge.calibration-progress-watch}"; i=\$((i+1)); done
fi
# A load that FAILS is not hypothetical: a malformed plist, a label already loaded, a locked session.
if [ "\${1:-}" = "load" ] && [ -n "\${LOAD_FAILS:-}" ]; then echo "Load failed" >&2; exit 1; fi
exit 0
EOF
  chmod +x "$TMP/bin/launchctl"; : > "$TMP/calls"
  # A real corpus root and a real calendar, so a HEALTHY run reaches one of the watchdog's own outcomes.
  mkdir -p "$TMP/nas/calibration-runs/prod/UNFROZEN/2099-01-01/progress"
  # The installer now holds the install to the command the PLIST declares, so the staged plist must
  # name THIS destination. Before r24 the test overrode OE_OPS_DIR and the plist still pointed at
  # /Users/abhinav/oe-ops — the healthy case passed while verifying a copy launchd would never run,
  # which is the defect itself sitting inside its own test.
  PLIST_PATH="$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist"
  INTERP="${TEST_INTERP:-/bin/bash}"
  python3 - "$PLIST_PATH" "$INTERP" "$TMP/dest/calibration-progress-watch.sh" <<'PYEOF'
import plistlib, sys
p, interp, script = sys.argv[1], sys.argv[2], sys.argv[3]
d = plistlib.load(open(p, 'rb'))
d['ProgramArguments'] = [interp, script]
plistlib.dump(d, open(p, 'wb'))
PYEOF
}
install_run() {
  PATH="$TMP/bin:$PATH" OE_OPS_DIR="$TMP/dest" LAUNCH_AGENTS_DIR="$TMP/agents" \
    LOAD_FAILS="${LOAD_FAILS:-}" \
    ENV=prod CHECK_DATE=2026-07-02 ARCHIVE_DIR="$TMP/nas" \
    bash "$TMP/archive/install-dev-mac-watchdog.sh" > "$TMP/out" 2>&1
  RC=$?
}
loaded() { grep -q 'launchctl load' "$TMP/calls" 2>/dev/null; }

echo "1. a healthy install succeeds and registers exactly one agent"
stage 1; install_run
[ "$RC" -eq 0 ] && ok "the installer completed" || bad "healthy install failed (exit $RC): $(tail -3 "$TMP/out")"
loaded && ok "and it loaded the agent" || bad "it finished without loading the agent"

echo "2. a watchdog that dies on startup is REFUSED, and nothing is loaded"
stage 1
printf '#!/usr/bin/env bash\npython3 -c "raise SystemExit(__import__(%s).stderr.write(chr(10)))" ; echo "Traceback (most recent call last)"; exit 3\n' "'sys'" \
  > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "the installer refused (exit $RC)" || bad "a watchdog emitting a traceback was accepted"
loaded && bad "it loaded the agent anyway" || ok "and nothing was scheduled"
grep -q "did not start" "$TMP/out" && ok "it said the watchdog did not start" || bad "the refusal did not name the cause: $(tail -2 "$TMP/out")"

echo "3. a watchdog that produces neither verdict nor alert is REFUSED"
stage 1
printf '#!/usr/bin/env bash\necho "nothing to say"\nexit 1\n' > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "silence is not a verdict" || bad "an empty run was accepted as verification"
loaded && bad "it loaded the agent anyway" || ok "and nothing was scheduled"

echo "4. a deliberate refusal by the watchdog blocks the install"
stage 1
printf '#!/usr/bin/env bash\necho "ALERT: calibration watchdog cannot run: no archive root on this host"\nexit 1\n' \
  > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "the installer refused to load a watchdog that says it cannot run" || bad "a self-declared refusal was installed"
loaded && bad "it loaded the agent anyway" || ok "and nothing was scheduled"

echo "5. the agent failing to register is an INSTALL FAILURE, not a printed number"
stage 0; install_run
[ "$RC" -ne 0 ] && ok "launchctl reporting zero agents fails the install" || bad "the installer exited 0 with nothing scheduled"
grep -q "nothing is scheduled" "$TMP/out" && ok "and it said so plainly" || bad "the failure did not say the agent is missing: $(tail -2 "$TMP/out")"

echo "6. a LOOKALIKE agent label does not count as the agent"
# r23 #2. The count was an unanchored substring match, so an agent named
# com.optionsedge.calibration-progress-watch.backup satisfied "exactly one" while the label this
# plist declares was absent — an install that registers nothing, reported as success.
stage 1 "com.optionsedge.calibration-progress-watch.backup"; install_run
[ "$RC" -ne 0 ] && ok "a similarly named agent does not satisfy the check" || bad "a lookalike label passed as the agent"
grep -q "nothing is scheduled" "$TMP/out" && ok "and it said nothing is scheduled" || bad "the failure did not say the agent is missing: $(tail -2 "$TMP/out")"

echo "7. a refusal leaves the PREVIOUS installation untouched"
# r24 #1. The installer used to copy over the live files and only then run its checks, so a refusal
# left a working agent pointing at the rejected copy — it broke the very installation it declined to
# replace. Put a known-good file in place, then offer it a broken watchdog.
stage 1
mkdir -p "$TMP/dest"
printf '#!/usr/bin/env bash\necho "I am the previous installation"\n' > "$TMP/dest/calibration-progress-watch.sh"
cp "$TMP/dest/calibration-progress-watch.sh" "$TMP/previous"
printf '#!/usr/bin/env bash\necho "Traceback (most recent call last)"; exit 3\n' > "$TMP/archive/calibration-progress-watch.sh"
install_run
[ "$RC" -ne 0 ] && ok "the installer refused" || bad "a broken watchdog was installed"
if cmp -s "$TMP/dest/calibration-progress-watch.sh" "$TMP/previous"; then
  ok "and the previously installed watchdog is byte-identical to what it was"
else
  bad "a refusal overwrote the working installation"
fi

echo "8. the plist and the destination must agree, or nothing is installed"
# The plist names an absolute path; installing somewhere else registers an agent pointing at a copy
# nobody updates.
stage 1
python3 - "$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist" <<'PYEOF'
import plistlib, sys
d = plistlib.load(open(sys.argv[1], 'rb'))
d['ProgramArguments'] = ['/bin/bash', '/somewhere/else/calibration-progress-watch.sh']
plistlib.dump(d, open(sys.argv[1], 'wb'))
PYEOF
install_run
[ "$RC" -ne 0 ] && ok "a plist pointing elsewhere stops the install" || bad "it installed to a directory the plist does not name"
loaded && bad "it loaded the agent anyway" || ok "and nothing was scheduled"

echo "9. an interpreter the plist names but the host lacks stops the install"
# launchd runs THAT binary. If it is missing the agent fails silently every morning, which is the
# failure this whole watchdog exists to make impossible.
TEST_INTERP=/nonexistent/bash stage 1
install_run
[ "$RC" -ne 0 ] && ok "a missing interpreter is caught before anything is written" || bad "it installed an agent whose interpreter does not exist"

echo "11. a failure DURING replacement rolls back, and unloads the rejected agent first"
# r25 #1/#2. The EXIT trap used to delete scratch directories and nothing else, so a set -e failure
# in the live phase exited without restoring; and the rollback reloaded without unloading, leaving
# launchd holding the configuration it had just been handed.
stage 1
mkdir -p "$TMP/dest" "$TMP/agents"
printf '#!/usr/bin/env bash\necho "I am the previous installation"\n' > "$TMP/dest/calibration-progress-watch.sh"
cp "$TMP/dest/calibration-progress-watch.sh" "$TMP/previous"
cp "$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist" "$TMP/agents/"
cp "$TMP/agents/com.optionsedge.calibration-progress-watch.plist" "$TMP/previous.plist"
LOAD_FAILS=1 install_run
[ "$RC" -ne 0 ] && ok "a failed load fails the install" || bad "a failed load was reported as success"
cmp -s "$TMP/dest/calibration-progress-watch.sh" "$TMP/previous" \
  && ok "the previous watchdog is back, byte-identical" || bad "the rollback did not restore the previous file"
cmp -s "$TMP/agents/com.optionsedge.calibration-progress-watch.plist" "$TMP/previous.plist" \
  && ok "and so is the previous plist" || bad "the rollback did not restore the previous plist"
# The unload must come BEFORE the restoring load, or launchd keeps the rejected configuration.
awk '/launchctl unload/{u=NR} /launchctl load/{l=NR} END{exit !(u && l && u < l)}' "$TMP/calls" \
  && ok "it unloaded the rejected agent before reloading the previous one" \
  || bad "it reloaded without unloading first: $(tr '\n' '; ' < "$TMP/calls")"

echo "12. a FIRST install that fails leaves nothing behind"
# Absence is a state too. Restoring "what was there" when nothing was there means REMOVING what this
# run wrote — otherwise a failed first install leaves the rejected copy and its plist on the host.
stage 0
rm -rf "$TMP/dest" "$TMP/agents"; mkdir -p "$TMP/dest" "$TMP/agents"
install_run
[ "$RC" -ne 0 ] && ok "the install failed as set up" || bad "it succeeded with zero agents registered"
if [ -z "$(ls -A "$TMP/dest" 2>/dev/null)" ] && [ -z "$(ls -A "$TMP/agents" 2>/dev/null)" ]; then
  ok "the destination and the agents directory are empty again"
else
  bad "a failed first install left files behind: $(ls "$TMP/dest" "$TMP/agents" 2>/dev/null | tr '\n' ' ')"
fi

echo "13. a plist PATH that cannot run the watchdog is caught BEFORE installing"
# r25 #3. Verification used to inherit the operator's PATH, so a plist whose PATH lacks python3
# verified perfectly from a terminal and failed every morning under launchd.
stage 1
python3 - "$TMP/archive/launchd/com.optionsedge.calibration-progress-watch.plist" <<'PYEOF'
import plistlib, sys
d = plistlib.load(open(sys.argv[1], 'rb'))
d['EnvironmentVariables']['PATH'] = '/nonexistent'
plistlib.dump(d, open(sys.argv[1], 'wb'))
PYEOF
install_run
[ "$RC" -ne 0 ] && ok "a PATH launchd would use but nothing can run is refused" || bad "it installed an agent that could never run under launchd"
loaded && bad "it loaded the agent anyway" || ok "and nothing was scheduled"

echo "14. every refusal in the watchdog uses the phrase the installer blocks on"
# One marker covers all refusals only while that is true.
_ref=$(grep -cE 'alert "calibration watchdog cannot run:' "$HERE/calibration-progress-watch.sh")
_all=$(grep -cE 'alert "calibration watchdog' "$HERE/calibration-progress-watch.sh")
[ "$_ref" -eq "$_all" ] && [ "$_ref" -gt 0 ] \
  && ok "all $_ref watchdog refusals share the phrase the installer blocks on" \
  || bad "$((_all - _ref)) refusal(s) do not say 'cannot run', so the installer would not block on them"

echo
if [ "$fails" -eq 0 ]; then echo "=== install-dev-mac-watchdog-test: OK ==="; exit 0; fi
echo "=== install-dev-mac-watchdog-test: $fails problem(s) ===" >&2; exit 1
