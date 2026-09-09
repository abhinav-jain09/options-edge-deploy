#!/usr/bin/env bash
# Every `sh` heredoc block in the MM1 mirror Jenkinsfiles must be valid shell.
#
# WHY: a Groovy pipeline is not shell-checked by anything. Jenkinsfile.es-auction-mirror shipped with
# `fi; { : }` in its preflight - a brace group with no separator before `}`, so `}` became an argument
# to `:` and the group was never closed. Groovy was happy, review was happy, and the job failed at
# RUNTIME on its very first install, after the preflight had already talked to both brokers:
#   script.sh.copy: line 102: syntax error: unexpected end of file
# `bash -n` costs milliseconds.
#
# SCOPE: the MM1 mirror jobs. They all descend from one another by copy, so they share the one
# single-quoted form this line-based extractor can slice safely. A repo-wide version needs a real
# Groovy-aware extractor - several other Jenkinsfiles mix `sh """..."""`, nested triple quotes and
# line continuations that a line-based reader mis-slices, and reporting those as syntax errors would
# be a false alarm, not a gate. Widening it beyond the mirrors is its own change.
#
# The GLOB matters. This file used to name ONE Jenkinsfile, so the definition-enumeration mirror -
# written by copying the auction one and therefore carrying every one of its shell habits - would have
# shipped unchecked by the very guard its ancestor's runtime failure created.
set -euo pipefail
cd "$(dirname "$0")/../.."

shopt -s nullglob
FILES=(Jenkinsfile.*-mirror)
shopt -u nullglob
[ "${#FILES[@]}" -gt 0 ] || { echo "FAIL: no Jenkinsfile.*-mirror found - the glob and the repo layout have diverged"; exit 1; }

tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT
total=0
fail=0

for FILE in "${FILES[@]}"; do
[ -f "$FILE" ] || { echo "FAIL: $FILE is missing"; exit 1; }
tmp="$tmp_root/$(basename "$FILE")"
mkdir -p "$tmp"
awk -v dir="$tmp" "
  /sh[[:space:]]*'''\$/ && !inblock { inblock = 1; n++; next }
  inblock && /^[[:space:]]*'''[[:space:]]*\$/ { inblock = 0; next }
  inblock { print > (dir \"/block-\" n \".sh\") }
" "$FILE"
checked=0
for b in "$tmp"/block-*.sh; do
  [ -e "$b" ] || continue
  checked=$((checked + 1))
  if ! err="$(bash -n "$b" 2>&1)"; then
    echo "FAIL: $FILE - sh block $(basename "$b") is not valid shell:"
    echo "$err" | sed 's/^/    /'
    fail=1
  fi
done

if [ "$checked" -lt 5 ]; then
  echo "FAIL: only $checked sh blocks extracted from $FILE - the extractor is broken, not the file"
  exit 1
fi
echo "  $FILE: $checked shell block(s) parse"
total=$((total + checked))
done

[ "$fail" -eq 0 ] && echo "OK: $total shell blocks across ${#FILES[@]} mirror Jenkinsfile(s) parse"
exit "$fail"
