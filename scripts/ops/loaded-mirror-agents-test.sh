#!/usr/bin/env bash
# loaded_mirror_agents_for is the gate in front of the production wipe, so the cases that matter are the
# ones where it must REFUSE rather than answer "nothing is loaded": launchctl failing, a job whose
# program launchd will not report, an unreadable producer.properties. Driven with a stubbed launchctl,
# because the real one answers about this Mac.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
H="$HERE/loaded-mirror-agents.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -r "$H" ] || { echo "FAIL: $H missing" >&2; exit 1; }

PROD=192.168.100.252:9092
OTHER=127.0.0.1:19092

# A unit directory: a run script plus a producer.properties pointing at <broker>.
unit() { # <name> <broker|-> -> prints the program path
  local d="$WORK/$1"; mkdir -p "$d"
  printf '#!/usr/bin/env bash\nexec /bin/true\n' > "$d/run-mirror.sh"; chmod +x "$d/run-mirror.sh"
  [ "$2" = - ] || printf 'bootstrap.servers=%s\nacks=all\n' "$2" > "$d/producer.properties"
  printf '%s\n' "$d/run-mirror.sh"
}
# The stub: STUB_TABLE is the `launchctl list` table, STUB_JOB_<n> the per-label dict. LIST_RC and
# JOB_RC force failures.
stub() {
  mkdir -p "$WORK/bin"
  cat > "$WORK/bin/launchctl" <<'EOF'
#!/usr/bin/env bash
if [ "$#" -eq 1 ] && [ "$1" = list ]; then
  [ "${LIST_RC:-0}" = 0 ] || exit "$LIST_RC"
  cat "$STUB_TABLE"; exit 0
fi
if [ "$1" = list ]; then
  [ "${JOB_RC:-0}" = 0 ] || exit "$JOB_RC"
  f="$STUB_JOBS/$2"
  [ -r "$f" ] || exit 113
  cat "$f"; exit 0
fi
exit 0
EOF
  chmod +x "$WORK/bin/launchctl"
}
job() { # <label> <program|-> ; writes the per-label dict
  mkdir -p "$WORK/jobs"
  { printf '{\n\t"Label" = "%s";\n' "$1"
    [ "$2" = - ] || printf '\t"Program" = "%s";\n\t"ProgramArguments" = (\n\t\t"%s";\n\t);\n' "$2" "$2"
    printf '};\n'; } > "$WORK/jobs/$1"
}
table() { mkdir -p "$WORK"; : > "$WORK/table"; local l; for l in "$@"; do printf -- '-\t0\t%s\n' "$l" >> "$WORK/table"; done; }
run() { # -> RC, OUT (stdout only), ERR
  OUT="$(PATH="$WORK/bin:$PATH" STUB_TABLE="$WORK/table" STUB_JOBS="$WORK/jobs" \
    LIST_RC="${LIST_RC:-0}" JOB_RC="${JOB_RC:-0}" \
    bash -c '. "$1"; loaded_mirror_agents_for "$2"' _ "$H" "${1:-$PROD}" 2>"$WORK/err")"
  RC=$?; ERR="$(cat "$WORK/err")"
}
stub

echo "1. the straightforward answer"
P1=$(unit prodmirror "$PROD"); P2=$(unit devmirror "$OTHER"); P3=$(unit notamirror -)
table com.optionsedge.prodmirror com.optionsedge.devmirror com.optionsedge.notamirror
job com.optionsedge.prodmirror "$P1"; job com.optionsedge.devmirror "$P2"; job com.optionsedge.notamirror "$P3"
run
[ "$RC" -eq 0 ] && ok "returns 0" || bad "returned $RC: $ERR"
[ "$OUT" = "com.optionsedge.prodmirror" ] && ok "names exactly the agent producing into the target broker" || bad "named [$OUT]"
run "$OTHER"
[ "$OUT" = "com.optionsedge.devmirror" ] && ok "and the other broker's agent for that broker" || bad "named [$OUT]"

echo "2. nothing loaded is an EMPTY answer, not a failure"
table
run
[ "$RC" -eq 0 ] && [ -z "$OUT" ] && ok "an empty launchctl table answers empty, status 0" || bad "rc=$RC out=[$OUT]"
table com.apple.something com.optionsedge.notamirror
job com.optionsedge.notamirror "$P3"
run
[ "$RC" -eq 0 ] && [ -z "$OUT" ] && ok "a loaded job with no producer.properties is not a mirror" || bad "rc=$RC out=[$OUT]"

echo "3. the cases where it must REFUSE (a wipe must not proceed on these)"
table com.optionsedge.prodmirror; job com.optionsedge.prodmirror "$P1"
LIST_RC=3 run; LIST_RC=0
[ "$RC" -ne 0 ] && ok "\`launchctl list\` failing is a refusal" || bad "a failed list returned 0 with [$OUT]"
printf '%s' "$ERR" | grep -q 'launchctl list' && ok "and says so" || bad "the diagnostic is [$ERR]"
JOB_RC=4 run; JOB_RC=0
[ "$RC" -ne 0 ] && ok "a per-label query failing is a refusal" || bad "a failed per-label query returned 0"
job com.optionsedge.prodmirror -
run
[ "$RC" -ne 0 ] && ok "a loaded job launchd reports no program for is a refusal" || bad "a programless job returned 0"
job com.optionsedge.prodmirror "relative/run-mirror.sh"
run
[ "$RC" -ne 0 ] && ok "a non-absolute program is a refusal" || bad "a relative program returned 0"
job com.optionsedge.prodmirror "$P1"; chmod 000 "$WORK/prodmirror/producer.properties"
run; chmod 600 "$WORK/prodmirror/producer.properties"
[ "$RC" -ne 0 ] && ok "an UNREADABLE producer.properties is a refusal (not 'not a mirror')" || bad "an unreadable producer config returned 0 with [$OUT]"
printf '%s' "$ERR" | grep -q 'unreadable' && ok "and says which job" || bad "the diagnostic is [$ERR]"

echo "4. the program comes from ProgramArguments when there is no Program key"
job com.optionsedge.prodmirror "$P1"
python3 - "$WORK/jobs/com.optionsedge.prodmirror" "$P1" <<'PY'
import sys
out, prog = sys.argv[1], sys.argv[2]
open(out, "w").write('{\n\t"Label" = "com.optionsedge.prodmirror";\n\t"ProgramArguments" = (\n\t\t"%s";\n\t);\n};\n' % prog)
PY
run
[ "$OUT" = "com.optionsedge.prodmirror" ] && ok "ProgramArguments[0] is used when Program is absent" || bad "out=[$OUT] rc=$RC err=$ERR"

echo "5. the broker match is over the LIST, and exact per entry"
job com.optionsedge.prodmirror "$P1"
printf 'bootstrap.servers=192.168.100.252:90921\n' > "$WORK/prodmirror/producer.properties"
run
[ -z "$OUT" ] && ok "a longer host:port that merely starts the same does not match" || bad "matched [$OUT]"
# bootstrap.servers is a LIST: a mirror that names this broker among others targets it just as much, and
# requiring the whole value to equal it hid exactly that (deploy Codex round 9).
printf 'bootstrap.servers=192.168.100.252:9092,other.host:9092\n' > "$WORK/prodmirror/producer.properties"
run
[ "$OUT" = "com.optionsedge.prodmirror" ] && ok "a multi-broker list naming the target FIRST matches" || bad "out=[$OUT]"
printf 'bootstrap.servers=other.host:9092, 192.168.100.252:9092 \n' > "$WORK/prodmirror/producer.properties"
run
[ "$OUT" = "com.optionsedge.prodmirror" ] && ok "...and naming it later, with spaces around the entries" || bad "out=[$OUT]"
printf 'bootstrap.servers=a:1\nbootstrap.servers=192.168.100.252:9092\n' > "$WORK/prodmirror/producer.properties"
run
[ "$OUT" = "com.optionsedge.prodmirror" ] && ok "the LAST assignment wins, as java.util.Properties reads it" || bad "out=[$OUT]"
printf 'bootstrap.servers=192.168.100.252:9092\r\n' > "$WORK/prodmirror/producer.properties"
run
[ "$OUT" = "com.optionsedge.prodmirror" ] && ok "a CRLF line still matches" || bad "out=[$OUT]"
printf 'acks=all\n' > "$WORK/prodmirror/producer.properties"
run
[ "$RC" -ne 0 ] && ok "a producer.properties with NO bootstrap.servers is a refusal" || bad "a config with no bootstrap returned 0"
printf 'bootstrap.servers=192.168.100.252:9092\n' > "$WORK/prodmirror/producer.properties"

echo "5b. a label with a SPACE in it is not lost"
table "com.optionsedge.spaced label" ; job "com.optionsedge.spaced label" "$P1"
run
[ "$OUT" = "com.optionsedge.spaced label" ] && ok "the table parse keeps everything after the second tab" || bad "out=[$OUT]"
table com.optionsedge.prodmirror; job com.optionsedge.prodmirror "$P1"

echo "5c. a unit directory that cannot be searched is a refusal, not 'not a mirror'"
mkdir -p "$WORK/locked/unit"; printf '#!/usr/bin/env bash\nexec /bin/true\n' > "$WORK/locked/unit/run-mirror.sh"
printf 'bootstrap.servers=%s\n' "$PROD" > "$WORK/locked/unit/producer.properties"
table com.optionsedge.lockeddir; job com.optionsedge.lockeddir "$WORK/locked/unit/run-mirror.sh"
chmod 000 "$WORK/locked/unit"; run; chmod 700 "$WORK/locked/unit"
[ "$RC" -ne 0 ] && ok "an unreadable unit directory refuses" || bad "an unreadable unit directory returned 0 with [$OUT]"
table com.optionsedge.gonedir; job com.optionsedge.gonedir "$WORK/no-such-dir/run-mirror.sh"
run
[ "$RC" -ne 0 ] && ok "a program whose directory does not exist refuses" || bad "a vanished directory returned 0"
table com.optionsedge.prodmirror; job com.optionsedge.prodmirror "$P1"

echo "5d. the old exactness case, kept"
job com.optionsedge.prodmirror "$P1"
printf 'bootstrap.servers=192.168.100.252:90921\n' > "$WORK/prodmirror/producer.properties"
run
[ -z "$OUT" ] && ok "a longer host:port that merely starts the same does not match" || bad "matched [$OUT]"
printf 'bootstrap.servers=192.168.100.252:9092\n' > "$WORK/prodmirror/producer.properties"
run
[ "$OUT" = "com.optionsedge.prodmirror" ] && ok "and the exact value does" || bad "out=[$OUT]"

echo "6. MUTATION: each refusal is load-bearing"
mut() { # <OLD%%->%%NEW> <label>
  local dir="$WORK/mut.$RANDOM"; mkdir -p "$dir"; cp "$H" "$dir/h.sh"
  printf '%s' "$1" > "$dir/edit"
  python3 -c '
import sys
path, editfile = sys.argv[1], sys.argv[2]
old, new = open(editfile).read().split("%%->%%", 1)
s = open(path).read()
assert old in s, "the mutation did not apply: %r not found" % old
open(path, "w").write(s.replace(old, new, 1))
' "$dir/h.sh" "$dir/edit" || { bad "$2: the mutation did not apply"; return 1; }
  printf '%s\n' "$dir/h.sh"
}
if m="$(mut '    if p.returncode != 0:%%->%%    if False:' "the launchctl-status refusal")"; then
  MOUT="$(PATH="$WORK/bin:$PATH" STUB_TABLE="$WORK/table" STUB_JOBS="$WORK/jobs" LIST_RC=3 \
    bash -c '. "$1"; loaded_mirror_agents_for "$2"' _ "$m" "$PROD" 2>/dev/null)"; MRC=$?
  [ "$MRC" -eq 0 ] && [ -z "$MOUT" ] \
    && ok "without it, a failed \`launchctl list\` answers 'nothing is loaded' (case 3 is load-bearing)" \
    || bad "the mutant answered rc=$MRC out=[$MOUT]"
fi
# The unreadable-config check is a DIAGNOSTIC rule, not the thing that stands between the probe and a
# wipe: without it the read throws and the probe dies anyway. What it buys is that the operator is told
# WHICH job cannot be classified instead of reading a traceback -- so that, and not a flipped verdict,
# is what the mutation shows. Saying it the other way round would be the "right for the wrong reason"
# claim this repo keeps finding.
if m="$(mut '    if not os.access(props, os.R_OK):%%->%%    if False:' "the unreadable-config refusal")"; then
  chmod 000 "$WORK/prodmirror/producer.properties"
  MERR="$WORK/mut.err"
  MOUT="$(PATH="$WORK/bin:$PATH" STUB_TABLE="$WORK/table" STUB_JOBS="$WORK/jobs" \
    bash -c '. "$1"; loaded_mirror_agents_for "$2"' _ "$m" "$PROD" 2>"$MERR")"; MRC=$?
  chmod 600 "$WORK/prodmirror/producer.properties"
  if [ "$MRC" -ne 0 ] && grep -qE 'Traceback|PermissionError' "$MERR"; then
    ok "without it the probe still refuses, but by CRASHING (traceback, no job named) — the check turns that into a stated reason"
  elif [ "$MRC" -eq 0 ]; then
    bad "the mutant ANSWERED (rc=0, out=[$MOUT]) — then the check is load-bearing in the stronger sense and this case should say so"
  else
    bad "the mutant failed without a traceback: rc=$MRC err=[$(head -2 "$MERR")]"
  fi
fi

echo
if [ "$fails" -eq 0 ]; then echo "=== loaded-mirror-agents: OK ==="; exit 0; fi
echo "=== loaded-mirror-agents: $fails problem(s) ===" >&2; exit 1
