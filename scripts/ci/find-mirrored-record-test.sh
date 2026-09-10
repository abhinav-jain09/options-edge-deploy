#!/usr/bin/env bash
# The payload proof is only worth its name if this helper actually refuses the cases it claims to.
# Each case below was a REPORTED defect in the shell read-loop this replaced.
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
FIND="$HERE/kafka/find-mirrored-record.py"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
FAILED=0

check() { # name expected-rc ref-file dump-file processed
  local name="$1" want="$2" ref="$3" dump="$4" proc="$5"
  local out; out=$(python3 "$FIND" "$ref" "$dump" "$proc" "$TMP/out.txt" 2>&1); local rc=$?
  if [ "$rc" = "$want" ]; then
    printf '  ok   %-58s rc=%s\n' "$name" "$rc"
  else
    printf '  FAIL %-58s rc=%s want=%s\n' "$name" "$rc" "$want"; printf '       %s\n' "$out"; FAILED=1
  fi
}

REC='SPX|SPXW|20260910@{"status":"COMPLETE","contractCount":484}'

# 1. the happy path
printf '%s\n' "$REC" > "$TMP/ref"
{ printf 'other|record@{"a":1}\n'; printf '%s\n' "$REC"; } > "$TMP/dump"
check "byte-identical record is found" 0 "$TMP/ref" "$TMP/dump" 2
[ -s "$TMP/out.txt" ] || { echo "  FAIL out file not written"; FAILED=1; }
if ! cmp -s "$TMP/out.txt" "$TMP/ref"; then echo "  FAIL out file is not the reference bytes"; FAILED=1; else echo "  ok   the captured record is byte-identical to the reference"; fi

# 2. a NUL inserted into the target record must NOT be called identical (the shell read-loop dropped it)
printf '%s\n' "$REC" > "$TMP/ref"
printf 'SPX|SPXW|20260910@{"status":"COMPLETE\000","contractCount":484}\n' > "$TMP/dump"
check "a NUL byte in the target record is REFUSED" 1 "$TMP/ref" "$TMP/dump" 1

# 3. a reference with no trailing newline must still match (the shell version could never match it)
printf '%s' "$REC" > "$TMP/ref-nonl"
printf '%s\n' "$REC" > "$TMP/dump"
check "a reference lacking its final LF still matches" 0 "$TMP/ref-nonl" "$TMP/dump" 1

# 4. an embedded newline breaks record boundaries -> REFUSE, never compare fragments
printf '%s\n' "$REC" > "$TMP/ref"
{ printf 'garbage-prefix\n'; printf '%s\n' "$REC"; } > "$TMP/dump"
check "a dump with more lines than records is REFUSED" 1 "$TMP/ref" "$TMP/dump" 1

# 5. an unreadable processed count must refuse, and must SAY it was the count -- the refusal itself
# is already guaranteed by the count comparison (a negative can never equal a line count), so the
# message is the only thing this branch actually adds and the only thing worth asserting.
out5=$(python3 "$FIND" "$TMP/ref" "$TMP/dump" -1 "$TMP/out.txt" 2>&1); rc5=$?
if [ "$rc5" = "1" ] && printf '%s' "$out5" | grep -q "processed-record count was not readable"; then
  printf '  ok   %-58s rc=%s\n' "an unreadable count is refused, and says so" "$rc5"
else
  printf '  FAIL %-58s rc=%s\n' "an unreadable count is refused, and says so" "$rc5"; printf '       %s\n' "$out5"; FAILED=1
fi

# 6. a target that simply does not hold the record
printf 'someone|else@{"b":2}\n' > "$TMP/dump-miss"
check "a target without the record FAILS" 1 "$TMP/ref" "$TMP/dump-miss" 1

# 7. degenerate inputs
: > "$TMP/empty"
check "an empty reference FAILS" 1 "$TMP/empty" "$TMP/dump" 1
check "an empty dump FAILS" 1 "$TMP/ref" "$TMP/empty" 0
printf 'a\nb\n' > "$TMP/ref-2line"
check "a multi-line reference FAILS" 1 "$TMP/ref-2line" "$TMP/dump" 1

# 8. CRLF on either side must not be mistaken for a difference
printf '%s\r\n' "$REC" > "$TMP/ref-crlf"
printf '%s\r\n' "$REC" > "$TMP/dump-crlf"
check "CRLF line endings on both sides match" 0 "$TMP/ref-crlf" "$TMP/dump-crlf" 1

if [ "$FAILED" = "0" ]; then echo "=== find-mirrored-record-test: OK ==="; else echo "=== find-mirrored-record-test: FAILED ==="; exit 1; fi
