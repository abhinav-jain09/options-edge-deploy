#!/usr/bin/env bash
# The calendar validator's parity port is only worth its vectors if the vectors can FAIL it. Each mutation below
# removes one rule the contract enforces from a COPY of scripts/ci/validate-vol-premium-calendar.sh, runs
# `--vectors` against the real fixtures, and requires it to go RED — and names the vector(s) that must catch it.
# The unmutated copy must stay green (the control). The real file is never touched.
#
#   entry cap 512 -> 1024      n37-513-entries-compact (42 KB: under the wire cap, so ONLY the entry cap refuses it —
#                              deploy PR #1044 round 2, M4: the 70 KB fixture was refused by the wire cap first)
#   NFC normalisation removed  p06-kelvin-sign-eventcode-nfc (hash of the NFC form) / n32 (hash of the raw bytes)
#   duplicate keys collapsed   d03-dup-append-schemaversion-after-all-seen (a dict would keep the last value and accept)
#   Java trim -> Python strip  s04-lead-nbsp-padded (Jackson refuses NBSP padding; str.strip() would accept it)
#   wire cap removed           n34-over-wire-cap
set -uo pipefail
cd "$(dirname "$0")/../.."
SRC="$PWD"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
REPO="$work/repo"; mkdir -p "$REPO/scripts/ci"
cp "$SRC/scripts/ci/validate-vol-premium-calendar.sh" "$REPO/scripts/ci/"
cp -R "$SRC/scripts/ci/fixtures" "$REPO/scripts/ci/"
VAL="$REPO/scripts/ci/validate-vol-premium-calendar.sh"
fail=0

reset_copy() { cp "$SRC/scripts/ci/validate-vol-premium-calendar.sh" "$VAL"; }
mutate() { # <python re.sub pattern> <replacement> — must change the copy, or the case proves nothing
  python3 - "$VAL" "$1" "$2" <<'PY'
import re, sys
p, pat, rep = sys.argv[1:]
s = open(p).read(); n = re.subn(pat, rep, s, count=1)
if n[1] != 1: sys.exit("mutation matched nothing: " + pat)
open(p, "w").write(n[0])
PY
}
expect() { # <label> <want: red|green> <vector that must be named when red>
  local label="$1" want="$2" vec="$3" out rc
  out="$(cd "$REPO" && bash scripts/ci/validate-vol-premium-calendar.sh --vectors 2>&1)"; rc=$?
  if [ "$want" = green ]; then
    [ "$rc" = 0 ] && { echo "  ok   $label: green (control)"; return; }
    echo "  FAIL $label: the unmutated copy is RED"; printf '%s\n' "$out" | grep FAIL | head -3 | sed 's/^/       | /'; fail=1; return
  fi
  if [ "$rc" != 0 ] && printf '%s' "$out" | grep -E '^  FAIL' | grep -qF "$vec"; then
    echo "  ok   $label: RED, caught by $vec"
  else
    echo "  FAIL $label: rc=$rc, $vec did not catch it"; printf '%s\n' "$out" | grep -E '^  FAIL|^===' | head -4 | sed 's/^/       | /'; fail=1
  fi
}

echo "--- control ---"
reset_copy; expect "unmutated copy" green -
echo "--- mutations: each must turn --vectors red, and the named vector must be the one that catches it ---"
reset_copy; mutate 'MAX_CALENDAR_ENTRIES = 512' 'MAX_CALENDAR_ENTRIES = 1024';                   expect "entry cap raised to 1024" red n37-513-entries-compact
reset_copy; mutate 'return unicodedata.normalize\("NFC", s\)' 'return s';                          expect "NFC normalisation removed" red p06-kelvin-sign-eventcode-nfc
reset_copy; mutate 'object_pairs_hook=JObj\)' 'object_pairs_hook=lambda pairs: JObj(dict(pairs).items()))'; expect "duplicate keys collapsed (dict semantics)" red d03-dup-append-schemaversion-after-all-seen
reset_copy; mutate 's = java_trim\(v\)' 's = v.strip()';                                         expect "Java trim replaced by str.strip()" red s04-lead-nbsp-padded
reset_copy; mutate 'if len\(raw\) > MAX_CALENDAR_BYTES:' 'if False:';                              expect "wire cap removed" red n34-over-wire-cap
reset_copy; mutate 'if instantiated:' 'if False:';                                                 expect "post-instantiation duplicates accepted" red d03-dup-append-schemaversion-after-all-seen
reset_copy; mutate 'if s == "" or s == "null":' 'if False:';                                       expect "blank string no longer null" red s01-lead-empty-string

[ "$fail" -eq 0 ] && { echo "=== validate-vol-premium-calendar-mutation-test: OK ==="; exit 0; }
echo "=== validate-vol-premium-calendar-mutation-test: FAILED ==="; exit 1
