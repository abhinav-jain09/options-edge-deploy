#!/usr/bin/env bash
# The depth-archive installer's job is to REFUSE anything it cannot vouch for, and to say exactly what it did
# when it cannot finish. This runs the real installer against a copied archive directory with a stubbed
# launchctl, breaks one thing at a time, and asserts on the host state — the same shape as the watchdog
# installer's test beside it, for the same reason: a check that only reads the installer's text binds nothing.
#
# HOW THE ENVIRONMENT REACHES THE WRAPPER: through the staged plist's EnvironmentVariables, the channel the
# scheduled agent uses. The installer verifies under launchd's environment only, so ARCHIVE_DIR goes into the
# plist here, never into the caller's environment.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
LABEL=com.optionsedge.oe-archive-depth

stage() { # stage <registered-count> [label-to-report]
  TMP="$(mktemp -d)"
  cp -R "$HERE" "$TMP/archive"
  mkdir -p "$TMP/bin" "$TMP/dest" "$TMP/agents" "$TMP/nas"
  printf 'oe-nas\n' > "$TMP/nas/.oe_nas_sentinel"          # the wrapper requires the archiver's sentinel
  : > "$TMP/dest/canonical-archive-reader.jar"              # the sidecars Jenkins ships; the installer only checks presence
  : > "$TMP/dest/options-edge-contracts.jar"
  cat > "$TMP/bin/launchctl" <<EOF
#!/usr/bin/env bash
echo "launchctl \$*" >> "$TMP/calls"
if [ "\${1:-}" = "list" ]; then
  i=0; while [ "\$i" -lt "$1" ]; do printf -- "-\t0\t%s\n" "${2:-$LABEL}"; i=\$((i+1)); done
fi
if [ "\${1:-}" = "load" ] && [ -n "\${LOAD_FAILS:-}" ]; then echo "Load failed" >&2; exit 1; fi
exit 0
EOF
  chmod +x "$TMP/bin/launchctl"; : > "$TMP/calls"
  # A plist consistent with THIS destination, carrying the root the wrapper needs.
  python3 - "$TMP/archive/launchd/oe-archive-depth.plist" "${TEST_INTERP:-/bin/bash}" "$TMP/dest/oe-archive-depth.sh" "$TMP/nas" <<'PYEOF'
import plistlib, sys
p, interp, script, nas = sys.argv[1:5]
d = plistlib.load(open(p, 'rb'))
d['ProgramArguments'] = [interp, script]
env = d.setdefault('EnvironmentVariables', {})
env['ARCHIVE_DIR'] = nas
plistlib.dump(d, open(p, 'wb'))
PYEOF
}
install_run() {
  PATH="$TMP/bin:$PATH" OE_OPS_DIR="$TMP/dest" LAUNCH_AGENTS_DIR="$TMP/agents" LOAD_FAILS="${LOAD_FAILS:-}" \
    bash "$TMP/archive/install-dev-mac-depth-archive.sh" > "$TMP/out" 2>&1
  RC=$?
}
loaded() { grep -q 'launchctl load' "$TMP/calls" 2>/dev/null; }
untouched() { # only the two sidecars in dest, nothing in agents
  [ "$(ls -A "$TMP/dest" | sort | tr '\n' ' ')" = "canonical-archive-reader.jar options-edge-contracts.jar " ] \
    && [ -z "$(ls -A "$TMP/agents" 2>/dev/null)" ]
}

echo "1. a healthy install succeeds and registers exactly one agent"
stage 1; install_run
[ "$RC" -eq 0 ] && ok "the installer completed" || bad "healthy install failed (exit $RC): $(tail -4 "$TMP/out")"
loaded && ok "and it loaded the agent" || bad "it finished without loading the agent"
grep -q "derived exactly OE_DEPTH_TOPICS_dev" "$TMP/out" && ok "and it proved the staged wrapper derives the set under launchd's environment" || bad "the verification did not derive the set"
for f in oe-archive-depth.sh oe-archive-kafka.sh oe-topics.env oe-alert.sh StrikeArchiveReader.java CanonicalArchiveDecoder.java; do
  [ -f "$TMP/dest/$f" ] || bad "$f was not installed"
done
[ -f "$TMP/agents/oe-archive-depth.plist" ] && ok "the plist landed in the agents directory" || bad "no plist in the agents directory"

echo "2. a missing canonical helper jar is REFUSED, and nothing is written"
stage 1; rm -f "$TMP/dest/canonical-archive-reader.jar"; install_run
[ "$RC" -ne 0 ] && ok "the installer refused (exit $RC)" || bad "an install without the helper jar was accepted"
[ -z "$(ls -A "$TMP/agents" 2>/dev/null)" ] && [ ! -f "$TMP/dest/oe-archive-depth.sh" ] && ok "and the host was never touched" || bad "it wrote to the host"
grep -q "canonical-archive-reader.jar is missing" "$TMP/out" && ok "it named the missing sidecar" || bad "the refusal did not name the sidecar"

echo "3. a policy file without OE_DEPTH_TOPICS_dev is REFUSED before anything is written"
stage 1; grep -v '^OE_DEPTH_TOPICS_dev=' "$TMP/archive/oe-topics.env" > "$TMP/stripped" && mv "$TMP/stripped" "$TMP/archive/oe-topics.env"
install_run
[ "$RC" -ne 0 ] && ok "no set, no install" || bad "an undefined depth set was installed"
untouched && ok "and nothing was written" || bad "it wrote to the host anyway"

echo "4. a wrapper that cannot reach the NAS sentinel is REFUSED"
stage 1; rm -f "$TMP/nas/.oe_nas_sentinel"; install_run
[ "$RC" -ne 0 ] && ok "an unmounted root is not installed over" || bad "a root without the sentinel was accepted"
untouched && ok "and nothing was written" || bad "it wrote to the host anyway"
grep -q "did not start cleanly\|FATAL" "$TMP/out" && ok "it said why" || bad "the refusal did not say why"

echo "5. a plist pointing at a different destination is REFUSED"
stage 1
python3 - "$TMP/archive/launchd/oe-archive-depth.plist" <<'PYEOF'
import plistlib, sys
p = sys.argv[1]; d = plistlib.load(open(p, 'rb'))
d['ProgramArguments'][1] = '/somewhere/else/oe-archive-depth.sh'
plistlib.dump(d, open(p, 'wb'))
PYEOF
install_run
[ "$RC" -ne 0 ] && ok "a plist for another path is refused" || bad "a plist pointing elsewhere was installed"
untouched && ok "and nothing was written" || bad "it wrote to the host anyway"

echo "6. a LOOKALIKE agent label does not count as the agent"
stage 1 "$LABEL.backup"; install_run
[ "$RC" -ne 0 ] && ok "a similarly named agent does not satisfy the check" || bad "a lookalike label passed as the agent"

echo "7. a launchctl load that FAILS is an install failure, not a warning"
stage 1; LOAD_FAILS=1 install_run; LOAD_FAILS=""
[ "$RC" -ne 0 ] && ok "a failed load fails the install" || bad "a failed load was reported as success"
grep -q "could not load" "$TMP/out" && ok "and named the failure" || bad "it did not say the load failed"

echo "8. the dry run writes nothing and names the sidecars it requires"
stage 1
PATH="$TMP/bin:$PATH" OE_OPS_DIR="$TMP/dest" LAUNCH_AGENTS_DIR="$TMP/agents" bash "$TMP/archive/install-dev-mac-depth-archive.sh" --dry-run > "$TMP/out" 2>&1
[ $? -eq 0 ] && untouched && ok "dry run exits 0 and touches nothing" || bad "the dry run wrote or failed: $(tail -2 "$TMP/out")"
grep -q "require: canonical-archive-reader.jar options-edge-contracts.jar" "$TMP/out" && ok "and it states the sidecar requirement" || bad "the dry run does not name the sidecars"

if [ "$fails" -ne 0 ]; then echo "install-dev-mac-depth-archive-test: $fails FAILURE(S)"; exit 1; fi
echo "install-dev-mac-depth-archive-test: PASS"
