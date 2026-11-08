#!/usr/bin/env bash
# validate-dev-mac-depth-archive.sh — the DEV depth archive runs on the dev Mac under launchd, so this repo
# cannot prove it is RUNNING. What it can prove: that the install path exists, that the plist, the installer
# and the wrapper agree with each other and with the archiver's policy, that the schedule is the one the
# requirement names (02:00, 08:00, 14:00, 20:00 ET, document A section 9 item 6), that the set is the one
# document D section 8 names and is classified the way those documents say, and that nothing is registered
# for prod or es4. It is the obligation that validate-archive-unit-completeness.sh's exemption of these files
# creates: an exemption from one guard has to be an obligation to another, or it is just a hole with a comment.
set -uo pipefail
cd "$(dirname "$0")/.." 2>/dev/null || true
[ -d scripts/ops/archive ] || cd ..
D=scripts/ops/archive
INST="$D/install-dev-mac-depth-archive.sh"
WRAP="$D/oe-archive-depth.sh"
PLIST="$D/launchd/oe-archive-depth.plist"
ARCH="$D/oe-archive-kafka.sh"
TOPICS_ENV="$D/oe-topics.env"
DECODER="$D/CanonicalArchiveDecoder.java"
fails=0
note() { echo "FAIL: $*" >&2; fails=$((fails+1)); }

for f in "$INST" "$WRAP" "$PLIST" "$ARCH" "$TOPICS_ENV" "$DECODER"; do
  [ -f "$f" ] || note "$f is missing — the depth archive has no controlled install path"
done
[ "$fails" -eq 0 ] || { echo "=== validate-dev-mac-depth-archive: $fails problem(s) ===" >&2; exit 1; }
[ -x "$INST" ] || note "$INST is not executable, so the documented install command would not run"
[ -x "$WRAP" ] || note "$WRAP is not executable"

# 1. THE SET: exactly the eight members (the three of document D section 8, es.rithmic.mbo 2026-11-05 and the MBO engine's four book topics 2026-11-07), defined once, in the policy file.
want_set="es.rithmic.depth underlying.es.trades.linearized futures.liquidity-wall.closures es.rithmic.mbo es.mbo.level-flow es.mbo.level-flow-final es.mbo.health es.mbo.closures"
got_set="$(unset OE_DEPTH_TOPICS_dev; . "$TOPICS_ENV" >/dev/null 2>&1; printf '%s' "${OE_DEPTH_TOPICS_dev:-}")"
[ "$got_set" = "$want_set" ] || note "$TOPICS_ENV defines OE_DEPTH_TOPICS_dev=\"${got_set:-<unset>}\", expected \"$want_set\""

# 2. THE ARCHIVER declares and validates it like OE_ES4_TOPICS: unset, sourced, required.
grep -qE '^unset .*\bOE_DEPTH_TOPICS_dev\b' "$ARCH" || note "$ARCH does not unset OE_DEPTH_TOPICS_dev before sourcing the policy file (an inherited value would pass the check)"
grep -qF ': "${OE_DEPTH_TOPICS_dev:?' "$ARCH" || note "$ARCH does not require OE_DEPTH_TOPICS_dev from the policy file"
# 3. THE CLASSIFICATION the documents specify: the three Avro topics canonical, the tape committed-read and strict.
canon="$(grep -m1 '^OE_CANONICAL_ARCHIVE_TOPICS=' "$ARCH" | sed 's/^[^"]*"//; s/"$//')"
for t in es.rithmic.depth futures.liquidity-wall.closures es.rithmic.mbo es.mbo.level-flow es.mbo.level-flow-final es.mbo.health es.mbo.closures; do
  case " $canon " in *" $t "*) : ;; *) note "$t is not in OE_CANONICAL_ARCHIVE_TOPICS ($ARCH) — it would take the binary-unsafe console path" ;; esac
  grep -qF "\"$t\"" "$DECODER" || { [ "$t" = es.rithmic.depth ] && grep -qF 'RithmicSchemas.RAW_TOPIC' "$DECODER"; } \
    || { [ "$t" = es.rithmic.mbo ] && grep -qF 'RithmicMboSchemas.TOPIC,RithmicMboSchemas.EVENT' "$DECODER"; } \
    || note "$DECODER binds nothing for $t"
done
grep -qF 'LiquidityWallSchemas.CLOSURE' "$DECODER" || note "$DECODER does not bind LiquidityWallClosure"
grep -qF 'RithmicSchemas.FRAME' "$DECODER" || note "$DECODER does not bind RithmicDepthFrame"
grep -qE '^OE_COMMITTED_READ_TOPICS=.*underlying\.es\.trades\.linearized' "$ARCH" || note "underlying.es.trades.linearized is not in the OE_COMMITTED_READ_TOPICS default ($ARCH)"
grep -qE '^OE_STRICT_TOPICS=.*underlying\.es\.trades\.linearized' "$ARCH" || note "underlying.es.trades.linearized is not in the OE_STRICT_TOPICS default ($ARCH)"

# 4. THE WRAPPER runs the dedicated invocation: ENV=dev, the set as TOPICS, its own job name, a loud lock.
for needle in 'ENV=dev' 'TOPICS="$OE_DEPTH_TOPICS_dev"' 'ARCHIVE_JOB=depth' 'ON_LOCK_BUSY=fail'; do
  grep -qF -- "$needle" "$WRAP" || note "$WRAP does not pass $needle to the archiver"
done
grep -qE '^unset OE_DEPTH_TOPICS_dev' "$WRAP" || note "$WRAP does not discard an inherited OE_DEPTH_TOPICS_dev before sourcing the policy file"

# 5. EVERYTHING THE WRAPPER READS FROM BESIDE ITSELF MUST BE INSTALLED WITH IT, plus the committed-read and
#    canonical sources the archiver runs for this set.
# Files it SOURCES or RUNS from beside itself (.sh/.env/.java/.py) — not the log it writes there.
needed="$(grep -oE '\$OE_DIR/[A-Za-z0-9._-]+\.(sh|env|java|py)' "$WRAP" | sed 's|.*/||' | sort -u) StrikeArchiveReader.java CanonicalArchiveDecoder.java oe-archive-depth.sh oe-archive-session-chain.py"
files_line="$(grep -m1 '^FILES=' "$INST" | sed 's/^FILES="//; s/"$//')"
for f in $needed; do
  case " $files_line " in
    *" $f "*) : ;;
    *) note "the depth archive reads '$f' beside itself, but $INST does not install it (FILES=\"$files_line\")" ;;
  esac
done
sidecars="$(grep -m1 '^SIDECARS=' "$INST" | sed 's/^SIDECARS="//; s/"$//')"
for j in canonical-archive-reader.jar options-edge-contracts.jar; do
  case " $sidecars " in *" $j "*) : ;; *) note "$INST does not require the sidecar $j (the canonical path cannot run without it)" ;; esac
done

# 6. THE PLIST: runs the wrapper this repo ships from where the installer puts it; explicit PATH; no pinned
#    archive root (BZ 360); names the installer and carries no hand-copy recipe; four entries at the hours the
#    requirement names, minute 0; not RunAtLoad.
plist_script="$(grep -oE '<string>[^<]*oe-archive-depth\.sh</string>' "$PLIST" | sed 's|<string>||; s|</string>||' | head -1)"
[ -n "$plist_script" ] || note "$PLIST does not name oe-archive-depth.sh in ProgramArguments"
dest_default="$(grep -m1 '^DEST=' "$INST" | sed 's/.*:-//; s/}.*//')"
case "${plist_script:-}" in
  *"$(basename "${dest_default:-oe-ops}")"/*) : ;;
  *) note "$PLIST runs ${plist_script:-<none>} but $INST installs into ${dest_default:-?} — the agent would run a copy nobody updates" ;;
esac
grep -qE '<key>ARCHIVE_DIR</key>' "$PLIST" && note "$PLIST pins ARCHIVE_DIR — the wrapper resolves the root and proves the sentinel; a pinned wrong path fails four times a day"
grep -qE '<key>PATH</key>' "$PLIST" || note "$PLIST sets no PATH — launchd jobs inherit almost none"
grep -q 'install-dev-mac-depth-archive.sh' "$PLIST" || note "$PLIST does not name the installer, so an operator has nothing to run"
grep -qE '^\s*cp .*\.(sh|env|py|jar)' "$PLIST" && note "$PLIST contains hand-copy instructions — a second, unmaintained copy of the installer's FILES"
grep -qE '<key>TOPICS</key>' "$PLIST" && note "$PLIST carries a TOPICS override — the policy file is the one definition of the set"
sched="$(python3 - "$PLIST" <<'PY'
import plistlib, sys
d = plistlib.load(open(sys.argv[1], 'rb'))
cal = d.get('StartCalendarInterval')
if not isinstance(cal, list):
    print('not-a-list'); sys.exit(0)
print(' '.join('%s:%s' % (e.get('Hour'), e.get('Minute')) for e in cal))
print('RunAtLoad=%s' % d.get('RunAtLoad'))
print('Label=%s' % d.get('Label'))
PY
)"
[ "$(printf '%s\n' "$sched" | sed -n 1p)" = "2:0 8:0 14:0 20:0" ] || note "$PLIST schedule is '$(printf '%s\n' "$sched" | sed -n 1p)', expected four entries at 02:00, 08:00, 14:00 and 20:00 (minute 0)"
[ "$(printf '%s\n' "$sched" | sed -n 2p)" = "RunAtLoad=False" ] || note "$PLIST must not RunAtLoad — loading the agent is not a capture"
[ "$(printf '%s\n' "$sched" | sed -n 3p)" = "Label=com.optionsedge.oe-archive-depth" ] || note "$PLIST label is not com.optionsedge.oe-archive-depth"
plutil -lint "$PLIST" >/dev/null 2>&1 || python3 -c 'import plistlib,sys; plistlib.load(open(sys.argv[1],"rb"))' "$PLIST" 2>/dev/null || note "$PLIST does not parse"

# 7. DEV ONLY. Nothing scheduled on .252 for it, and none of the four topics in a prod or es4 set.
grep -q 'oe-archive-depth' "$D/oe-archive.crontab" && note "the .252 crontab names the depth archive — it belongs to the dev Mac's launchd, not to .252"
for v in OE_ES4_TOPICS OE_ALL_TOPICS_prod; do
  members="$(unset "$v"; . "$TOPICS_ENV" >/dev/null 2>&1; eval "printf '%s' \"\${$v:-}\"")"
  for t in $want_set; do
    case " $members " in *" $t "*) note "$t is in $v — the depth set is registered for dev only" ;; esac
  done
done

# 8. THE INSTALLER'S OWN VERIFICATION MUST BE ABLE TO FAIL: run its executable test.
INST_TEST="$D/install-dev-mac-depth-archive-test.sh"
if [ ! -x "$INST_TEST" ]; then
  note "$INST_TEST is missing or not executable — nothing proves the installer can refuse a broken install"
else
  _out="$(mktemp)"
  if ! bash "$INST_TEST" > "$_out" 2>&1; then
    note "$INST_TEST — the installer accepts an install it should refuse:"
    sed 's/^/      /' "$_out" >&2
  fi
  rm -f "$_out"
fi

# 9. THE BOOK ARCHIVE'S FILING AND ITS CHAIN CHECKER MUST BE ABLE TO FAIL TOO (A-GAP-05): trade-date filing,
#    capture provenance and the per-session completeness chain are run by their own tests wherever the tools exist.
if python3 -c 'import zoneinfo' >/dev/null 2>&1; then
  _out="$(mktemp)"
  if ! python3 "$D/test_archive_session_chain.py" > "$_out" 2>&1; then
    note "$D/test_archive_session_chain.py failed:"; sed 's/^/      /' "$_out" >&2
  fi
  rm -f "$_out"
  if command -v flock >/dev/null 2>&1 && command -v sha256sum >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1; then
    _out="$(mktemp)"
    if ! bash "$D/test-archive-bucket-filing.sh" > "$_out" 2>&1; then
      note "$D/test-archive-bucket-filing.sh failed:"; sed 's/^/      /' "$_out" >&2
    fi
    rm -f "$_out"
  else
    echo "SKIP: test-archive-bucket-filing.sh (flock, sha256sum or timeout missing on this host)"
  fi
else
  echo "SKIP: the book archive's python tests (python3 zoneinfo missing on this host)"
fi

if [ "$fails" -ne 0 ]; then echo "=== validate-dev-mac-depth-archive: $fails problem(s) ===" >&2; exit 1; fi
echo "checked the dev-Mac depth archive: the set, its classification, the wrapper's invocation, the install path, the four-a-day schedule, dev-only registration, and an installer that refuses a broken install"
echo "=== validate-dev-mac-depth-archive: OK ==="
