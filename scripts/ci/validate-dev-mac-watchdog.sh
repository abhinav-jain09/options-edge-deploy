#!/usr/bin/env bash
# validate-dev-mac-watchdog.sh — A5.7's watchdog runs on another host, so this repo cannot prove it is
# RUNNING. What it can prove, and what was missing (r19 #2), is that the install path exists, that the
# plist and the installer agree with the script, and that nothing the script reads is left behind.
#
# The old arrangement was a blanket exemption in validate-archive-unit-completeness.sh with a true
# reason ("it runs on the dev Mac") and no consequence: the name was excused and then never checked by
# anything. An exemption from one guard has to be an obligation to another, or it is just a hole with
# a comment on it.
set -uo pipefail
cd "$(dirname "$0")/.." 2>/dev/null || true
[ -d scripts/ops/archive ] || cd ..
D=scripts/ops/archive
INST="$D/install-dev-mac-watchdog.sh"
WATCH="$D/calibration-progress-watch.sh"
PLIST="$D/launchd/com.optionsedge.calibration-progress-watch.plist"
fails=0
note() { echo "FAIL: $*" >&2; fails=$((fails+1)); }

for f in "$INST" "$WATCH" "$PLIST"; do
  [ -f "$f" ] || { note "$f is missing — the watchdog has no controlled install path again"; }
done
[ "$fails" -eq 0 ] || { echo "=== validate-dev-mac-watchdog: $fails problem(s) ===" >&2; exit 1; }
[ -x "$INST" ] || note "$INST is not executable, so the documented install command would not run"

# 1. EVERYTHING THE WATCHDOG READS FROM BESIDE ITSELF MUST BE INSTALLED WITH IT.
# It sources oe-alert.sh (a missing copy downgrades every alert to a log line) and
# calibration-targets.env (a missing copy makes it refuse rather than watch the wrong cohort).
needed="$(grep -oE '\$\(dirname "\$0"\)/[A-Za-z0-9._-]+|\$\{BASH_SOURCE\[0\]\}\)/[A-Za-z0-9._-]+' "$WATCH" \
          | sed 's|.*/||' | sort -u)"
files_line="$(grep -m1 '^FILES=' "$INST" | sed 's/^FILES="//; s/"$//')"
for f in $needed; do
  case " $files_line " in
    *" $f "*) : ;;
    *) note "$WATCH reads '$f' from beside itself, but $INST does not install it (FILES=\"$files_line\")" ;;
  esac
done
# The script itself is what the plist runs, so it must be in FILES too.
case " $files_line " in
  *" calibration-progress-watch.sh "*) : ;;
  *) note "$INST does not install the watchdog script itself" ;;
esac

# 2. THE PLIST MUST RUN THE SCRIPT THIS REPO SHIPS, FROM WHERE THE INSTALLER PUTS IT.
plist_script="$(grep -oE '<string>[^<]*calibration-progress-watch\.sh</string>' "$PLIST" \
                | sed 's|<string>||; s|</string>||' | head -1)"
[ -n "$plist_script" ] || note "$PLIST does not name calibration-progress-watch.sh in ProgramArguments"
[ "$(basename "${plist_script:-}")" = "calibration-progress-watch.sh" ] || \
  note "$PLIST runs '${plist_script:-<none>}', which is not the script this repo ships"
dest_default="$(grep -m1 '^DEST=' "$INST" | sed 's/.*:-//; s/}.*//')"
case "${plist_script:-}" in
  *"$(basename "${dest_default:-oe-ops}")"/*) : ;;
  *) note "$PLIST runs $plist_script but $INST installs into ${dest_default:-?} — the agent would run a copy nobody updates" ;;
esac

# 3. THE PLIST MUST NOT PIN AN ARCHIVE ROOT. Hardcoding one is what made this agent alert MISSING every
# day on the host it targets (BZ 360); the script resolves the root and says which one it used.
if grep -qE '<key>ARCHIVE_DIR</key>' "$PLIST"; then
  note "$PLIST pins ARCHIVE_DIR again — resolve it in the script, or the next host that mounts the NAS elsewhere gets a daily false alarm"
fi
# 4. PATH must be explicit: a launchd job inherits almost nothing, and python3/find go missing silently.
grep -qE '<key>PATH</key>' "$PLIST" || note "$PLIST sets no PATH — launchd jobs inherit almost none, and this one needs python3"

# 5. THE INSTALLER'S OWN VERIFICATION MUST BE ABLE TO FAIL.
# Its first version accepted almost every broken watchdog and never required the agent to register,
# so the "proof that it runs" proved nothing. Asserting that by grepping the installer for phrases
# would be the same class of mistake, so run its executable test instead: it drives the real installer
# against a copied tree with a stubbed launchctl and breaks one thing at a time.
INST_TEST="$D/install-dev-mac-watchdog-test.sh"
if [ ! -x "$INST_TEST" ]; then
  note "$INST_TEST is missing or not executable — nothing proves the installer can refuse a broken watchdog"
else
  _out="$(mktemp)"
  if ! bash "$INST_TEST" > "$_out" 2>&1; then
    note "$INST_TEST — the installer accepts a watchdog that cannot run:"
    sed 's/^/      /' "$_out" >&2
  fi
  rm -f "$_out"
fi

if [ "$fails" -ne 0 ]; then echo "=== validate-dev-mac-watchdog: $fails problem(s) ===" >&2; exit 1; fi
echo "checked the dev-Mac watchdog: install path, plist target, resolved archive root, explicit PATH, and an installer that refuses a broken watchdog"
echo "=== validate-dev-mac-watchdog: OK ==="
