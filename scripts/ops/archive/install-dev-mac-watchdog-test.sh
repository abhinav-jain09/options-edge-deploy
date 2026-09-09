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
stage() { # stage <registered-count>
  TMP="$(mktemp -d)"
  cp -R "$HERE" "$TMP/archive"
  mkdir -p "$TMP/bin" "$TMP/dest" "$TMP/agents"
  cat > "$TMP/bin/launchctl" <<EOF
#!/usr/bin/env bash
echo "launchctl \$*" >> "$TMP/calls"
if [ "\${1:-}" = "list" ]; then
  i=0; while [ "\$i" -lt "$1" ]; do echo "-	0	com.optionsedge.calibration-progress-watch"; i=\$((i+1)); done
fi
exit 0
EOF
  chmod +x "$TMP/bin/launchctl"; : > "$TMP/calls"
  # A real corpus root and a real calendar, so a HEALTHY run reaches one of the watchdog's own outcomes.
  mkdir -p "$TMP/nas/calibration-runs/prod/UNFROZEN/2099-01-01/progress"
}
install_run() {
  PATH="$TMP/bin:$PATH" OE_OPS_DIR="$TMP/dest" LAUNCH_AGENTS_DIR="$TMP/agents" \
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

echo "6. every refusal in the watchdog uses the phrase the installer blocks on"
# One marker covers all refusals only while that is true.
_ref=$(grep -cE 'alert "calibration watchdog cannot run:' "$HERE/calibration-progress-watch.sh")
_all=$(grep -cE 'alert "calibration watchdog' "$HERE/calibration-progress-watch.sh")
[ "$_ref" -eq "$_all" ] && [ "$_ref" -gt 0 ] \
  && ok "all $_ref watchdog refusals share the phrase the installer blocks on" \
  || bad "$((_all - _ref)) refusal(s) do not say 'cannot run', so the installer would not block on them"

echo
if [ "$fails" -eq 0 ]; then echo "=== install-dev-mac-watchdog-test: OK ==="; exit 0; fi
echo "=== install-dev-mac-watchdog-test: $fails problem(s) ===" >&2; exit 1
