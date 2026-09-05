#!/usr/bin/env bash
# Behavioural tests for the shared reset-preserved parser.
#
# These EXECUTE the parser production uses, against crafted topics.env files. Earlier drafts tried
# to assert the mechanism by grepping the caller for a function name; Codex defeated that in one
# move by deleting the live call and leaving the comments, and the gate stayed green. Running the
# real code is the only assertion that cannot be satisfied by text.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELPER="$HERE/reset-preserved-topics.sh"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
fail=0

run() { # env-file-contents | ENVIRONMENT -> status; output in $work/out
  printf '%s\n' "$1" > "$work/topics.env"
  local st=0
  RPT_TOPICS_ENV="$work/topics.env" ENVIRONMENT="${2:-}" bash "$HELPER" > "$work/out" 2>&1 || st=$?
  return $st
}

check() { # description | want-status | contents | environment | expected-substring
  local desc="$1" want="$2" got=0
  run "$3" "$4" || got=$?
  if [ "$got" != "$want" ]; then
    printf '  FAIL %-50s exit %s, want %s\n' "$desc" "$got" "$want"; fail=1; return
  fi
  if [ -n "${5:-}" ] && ! grep -qF -e "$5" "$work/out"; then
    printf '  FAIL %-50s never said %q\n' "$desc" "$5"; fail=1; return
  fi
  printf '  ok   %-50s exit %s\n' "$desc" "$got"
}

BOTH='OPTIONS_EDGE_RESET_PRESERVED_TOPICS="a.topic b.topic"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="p.topic"'

check "resolves the base list off production" 0 "$BOTH" ""           "a.topic"
# Absence, not presence: vix is durable on production ONLY, and a dev reset must still sweep it.
run "$BOTH" "" || true
if grep -q "p.topic" "$work/out"; then
  printf '  FAIL %-50s prod-only topic leaked into the dev list\n' "prod-only EXCLUDED off production"; fail=1
else
  printf '  ok   %-50s\n' "prod-only EXCLUDED off production"
fi
check "prod-only INCLUDED on production"      0 "$BOTH" production   "p.topic"

# The partial-loss case: the combined result is still non-empty, so a naive emptiness check passes
# while underlying.vix.price silently becomes purgeable on production.
check "base present, PROD declaration missing" 1 \
  'OPTIONS_EDGE_RESET_PRESERVED_TOPICS="a.topic"' production "expected exactly ONE"
check "prod present, BASE declaration missing" 1 \
  'OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="p.topic"' "" "expected exactly ONE"
check "base declared EMPTY" 1 \
  'OPTIONS_EDGE_RESET_PRESERVED_TOPICS=""
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="p.topic"' "" "is empty"
check "duplicate assignments are refused" 1 \
  'OPTIONS_EDGE_RESET_PRESERVED_TOPICS="a.topic"
OPTIONS_EDGE_RESET_PRESERVED_TOPICS="b.topic"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="p.topic"' "" "found 2"

# Unreadable file must stop the purge, not preserve nothing.
st=0; RPT_TOPICS_ENV="$work/does-not-exist" bash "$HELPER" > "$work/out" 2>&1 || st=$?
if [ "$st" = 1 ] && grep -q "cannot read" "$work/out"; then printf '  ok   %-50s exit 1\n' "missing topics.env fails closed"
else printf '  FAIL %-50s exit %s\n' "missing topics.env fails closed" "$st"; fail=1; fi

# SOURCED callers must ignore RPT_TOPICS_ENV. It is a test seam, and an unconditional one would be
# an ambient production input: the off-hours purge launched with a crafted-but-well-formed env file
# would accept it as authoritative and silently omit real preserved topics.
printf 'OPTIONS_EDGE_RESET_PRESERVED_TOPICS="evil.only"\nOPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="evil.prod"\n' > "$work/evil.env"
sourced=$(cd "$HERE" && RPT_TOPICS_ENV="$work/evil.env" bash -c '. ./reset-preserved-topics.sh; printf "%s" "$RESET_PRESERVED_TOPICS"')
if printf '%s' "$sourced" | grep -q 'evil'; then
  printf '  FAIL %-50s injected path honoured when SOURCED\n' "sourced callers pin the declaration"; fail=1
else
  printf '  ok   %-50s\n' "sourced callers pin the declaration"
fi

[ $fail -eq 0 ] && echo "=== reset-preserved-topics-test: OK ===" || { echo "=== reset-preserved-topics-test: FAILED ==="; exit 1; }
