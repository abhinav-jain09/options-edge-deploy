#!/usr/bin/env bash
# resume_dev_mirrors must judge a mirror agent by a RUNNING PID, never by registration.
#
# WHY THIS GATE EXISTS. dev-cleanup pauses the es4 -> dev MirrorMaker agents around the topic wipe and
# resumes them once ensure_topics has created their targets. The resume used to accept
# `launchctl list <label>` exiting 0 as proof an agent was back — but that exits 0 for a job that is
# merely REGISTERED, with nothing running (measured 2026-10-12: 34 com.optionsedge.* agents were
# loaded with no pid). So a total failure to resume was indistinguishable from success: the clean
# counted every agent as resumed, deleted the pause list and reported OK, while dev spent a night
# with all 14 mirrors down, every mirrored topic frozen, and every downstream dev service reading
# stale data while looking healthy.
#
# The test drives the real function with a fake launchctl that reproduces exactly that state —
# bootstrap succeeds, `list <label>` succeeds, and the pid column is '-'. A version that judges by
# registration passes the old way and FAILS this gate.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
SCRIPT=${1:-scripts/ops/dev-cleanup.sh}
fail=0
T=$(mktemp -d) || exit 1
trap 'rm -f "$T/bin/launchctl" "$T/paused" "$T/fake.plist" "$T/funcs.sh"; rmdir "$T/bin" "$T" 2>/dev/null' EXIT
mkdir -p "$T/bin"

cat > "$T/bin/launchctl" <<'FAKE'
#!/usr/bin/env bash
case "${1:-}" in
  bootstrap) exit 0 ;;
  list)
    if [ -n "${2:-}" ]; then exit 0; fi                 # list <label>: REGISTERED, rc=0
    printf -- '-\t0\tcom.optionsedge.fake-mirror\n'     # list: pid '-' => loaded, NOT running
    exit 0 ;;
esac
exit 0
FAKE
chmod +x "$T/bin/launchctl"
: > "$T/fake.plist"
printf 'com.optionsedge.fake-mirror %s/fake.plist\n' "$T" > "$T/paused"

# Extract the function from the real script by brace matching, so this gate tests the shipped code
# rather than a copy of it.
python3 - "$SCRIPT" > "$T/funcs.sh" <<'PY'
import sys
s = open(sys.argv[1]).read()
# mirror_agent_pid is OPTIONAL: a script that does not define it is by definition the pre-fix
# version, which must FAIL this gate on the pid check rather than crash the extractor.
for name in ("mirror_agent_pid", "resume_dev_mirrors"):
    if (name + "() {") not in s:
        if name == "resume_dev_mirrors":
            raise SystemExit("resume_dev_mirrors not found")
        continue
    i = s.index(name + "() {")
    depth = 0
    j = i
    while True:
        if s[j] == '{':
            depth += 1
        elif s[j] == '}':
            depth -= 1
            if depth == 0:
                print(s[i:j + 1])
                break
        j += 1
PY
[ -s "$T/funcs.sh" ] || { echo "FAIL: could not extract resume_dev_mirrors from $SCRIPT"; exit 1; }

out=$(LAUNCHCTL="$T/bin/launchctl" DEV_MIRRORS_PAUSED="$T/paused" DEV_MIRROR_WAIT_S=4 DEV_MIRROR_SETTLE_S=1 bash -c "
  LAUNCHCTL='$T/bin/launchctl'; DEV_MIRRORS_PAUSED='$T/paused'
  DEV_MIRROR_WAIT_S=4; DEV_MIRROR_SETTLE_S=1
  source '$T/funcs.sh'
  resume_dev_mirrors; echo \"RC=\$?\"
" 2>&1)
rc=$(sed -n 's/^RC=\([0-9]*\)$/\1/p' <<<"$out" | tail -1)

if [ "${rc:-0}" = 0 ]; then
  echo "FAIL: resume_dev_mirrors returned SUCCESS for an agent whose pid is '-' (registered, not running)"
  echo "$out" | sed 's/^/    /'
  fail=1
fi
# Case-INSENSITIVE and matching the literal both failure branches share. My first version grepped
# 'not RUNNING' while the code says 'NOT RUNNING', so the gate failed the very fix it was written
# for — the assertion was testing my memory of the wording, not the behaviour.
if ! grep -qi 'not running' <<<"$out"; then
  echo "FAIL: the failure was not reported as a not-RUNNING agent — the operator cannot see what broke"
  fail=1
fi
if [ ! -s "$T/paused" ]; then
  echo "FAIL: the pause list was deleted even though no agent came back running — the next run cannot retry"
  fail=1
fi

# Positive control: the harness must be able to FAIL. A fake that reports a real pid has to pass,
# otherwise the three checks above could be passing for some unrelated reason.
cat > "$T/bin/launchctl" <<'FAKE2'
#!/usr/bin/env bash
case "${1:-}" in
  bootstrap) exit 0 ;;
  list)
    if [ -n "${2:-}" ]; then exit 0; fi
    printf -- '4242\t0\tcom.optionsedge.fake-mirror\n'   # a real pid => running
    exit 0 ;;
esac
exit 0
FAKE2
chmod +x "$T/bin/launchctl"
printf 'com.optionsedge.fake-mirror %s/fake.plist\n' "$T" > "$T/paused"
out2=$(LAUNCHCTL="$T/bin/launchctl" DEV_MIRRORS_PAUSED="$T/paused" DEV_MIRROR_WAIT_S=4 DEV_MIRROR_SETTLE_S=1 bash -c "
  LAUNCHCTL='$T/bin/launchctl'; DEV_MIRRORS_PAUSED='$T/paused'
  DEV_MIRROR_WAIT_S=4; DEV_MIRROR_SETTLE_S=1
  source '$T/funcs.sh'
  resume_dev_mirrors; echo \"RC=\$?\"
" 2>&1)
rc2=$(sed -n 's/^RC=\([0-9]*\)$/\1/p' <<<"$out2" | tail -1)
if [ "${rc2:-1}" != 0 ]; then
  echo "FAIL positive control: a RUNNING agent (pid 4242) was not accepted — the gate rejects everything"
  echo "$out2" | sed 's/^/    /'
  fail=1
fi

[ "$fail" = 0 ] && echo "=== dev-mirror-resume-test: PASS — judged by pid, reported, pause list retained, running agent accepted ===" \
                || echo "=== dev-mirror-resume-test: FAILED ==="
exit "$fail"
