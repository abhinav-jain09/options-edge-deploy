#!/usr/bin/env bash
# Every `sh '''...'''` block in the es-auction mirror Jenkinsfile must be valid shell.
#
# WHY: a Groovy pipeline is not shell-checked by anything. Jenkinsfile.es-auction-mirror shipped with
# `fi; { : }` in its preflight — a brace group with no separator before `}`, so `}` became an argument
# to `:` and the group was never closed. Groovy was happy, review was happy, and the job failed at
# RUNTIME on its very first install, after the preflight had already talked to both brokers:
#   script.sh.copy: line 102: syntax error: unexpected end of file
# `bash -n` costs milliseconds.
#
# SCOPE. Deliberately this one file. A repo-wide version needs a real Groovy-aware extractor: several
# other Jenkinsfiles mix `sh """..."""`, nested triple quotes and line continuations that a line-based
# reader mis-slices, and reporting those as syntax errors would be a false alarm, not a gate. Widening
# it is its own change.
set -euo pipefail
cd "$(dirname "$0")/../.."

FILE=Jenkinsfile.es-auction-mirror
[ -f "$FILE" ] || { echo "FAIL: $FILE is missing"; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
awk -v dir="$tmp" "
  /sh[[:space:]]*'''\$/ && !inblock { inblock = 1; n++; next }
  inblock && /^[[:space:]]*'''[[:space:]]*\$/ { inblock = 0; next }
  inblock { print > (dir \"/block-\" n \".sh\") }
" "$FILE"

checked=0
fail=0
for b in "$tmp"/block-*.sh; do
  [ -e "$b" ] || continue
  checked=$((checked + 1))
  if ! err="$(bash -n "$b" 2>&1)"; then
    echo "FAIL: $FILE — sh block $(basename "$b") is not valid shell:"
    echo "$err" | sed 's/^/    /'
    fail=1
  fi
done

if [ "$checked" -lt 5 ]; then
  echo "FAIL: only $checked sh blocks extracted from $FILE — the extractor is broken, not the file"
  exit 1
fi
[ "$fail" -eq 0 ] && echo "OK: $checked shell blocks in $FILE parse"
exit "$fail"
