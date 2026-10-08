#!/usr/bin/env bash
# The sibling files apply-topics.sh sources out of its OWN directory, printed one per line.
#
# WHY THIS EXISTS. Three tests copy apply-topics.sh into a temp directory with a mutated topics.env
# beside it and run it there (apply-topics-vol-premium-safety-test.sh, apply-topics-strike-safety-test.sh,
# and anything that follows them). On 2026-10-07 apply-topics.sh gained a SECOND sibling --
# resolve-prod-partition-overrides.sh (#1165) -- and neither test copied it. Under
# ENVIRONMENT=production with an empty TOPIC_SET, which is the branch that sources it, both temp copies
# then died at `source: No such file or directory`:
#
#   * apply-topics-strike-safety-test.sh reported "the mutant did not delete — the test is not sensitive
#     to the declaration". It was sensitive; the mutant never ran.
#   * apply-topics-vol-premium-safety-test.sh reported 5 problems, every one of them a control run that
#     had failed to execute.
#
# So validate-durable-topic-preservation.sh -- the gate that holds the whole reset classification -- was
# RED on main from the moment #1165 merged until this file was added, and the red said nothing about
# topics.env. Each caller hard-coding a list of files to copy is what let one new `source` line break
# two tests silently, so the list is DERIVED from the script instead.
#
# Derived, not declared: this reads the `source "$SCRIPT_DIR/<file>"` lines out of the target script, so
# a sibling added tomorrow is copied by every caller without touching any test.
#
# Usage:  for f in $(bash scripts/kafka/apply-topics-sibling-files.sh); do cp "$HERE/$f" "$dst/"; done
#         bash scripts/kafka/apply-topics-sibling-files.sh [script]   # default: apply-topics.sh beside this
#
# FAILS CLOSED. A parse that finds nothing is a refusal, not an empty list: returning nothing quietly
# would restore exactly the bug this file closes -- the callers would copy nothing and the diagnostic
# would again surface as a wrong verdict about somebody's declaration.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
TARGET="${1:-$HERE/apply-topics.sh}"
[ -r "$TARGET" ] || { echo "apply-topics-sibling-files.sh: cannot read $TARGET" >&2; exit 2; }

# Both spellings of the source builtin, both quoting styles, at any indentation. The file name must be a
# literal: a computed path cannot be resolved here, and silently dropping it would under-report, so it is
# a refusal below (nothing matched) rather than an omission.
files="$(sed -nE 's/^[[:space:]]*(source|\.)[[:space:]]+"?\$\{?SCRIPT_DIR\}?\/([A-Za-z0-9._-]+)"?[[:space:]]*$/\2/p' "$TARGET" \
  | awk '!seen[$0]++')"

if [ -z "$files" ]; then
  echo "apply-topics-sibling-files.sh: found no \$SCRIPT_DIR sibling in $TARGET." >&2
  echo "  Either the script stopped sourcing its siblings (then delete this helper's callers too), or" >&2
  echo "  the source line is written in a form this parser does not read. Do NOT let it return nothing:" >&2
  echo "  every caller copies apply-topics.sh into a temp dir and would run it without its siblings." >&2
  exit 1
fi
printf '%s\n' "$files"
