#!/usr/bin/env bash
# Mutation test for the WHITELIST guard in dev-cleanup-indented-declaration-test.sh (its section 4).
#
# That guard is the only thing standing between these indent-tolerant readers and a declaration they would apply
# in the wrong environment: they PARSE topics.env, they do not interpret it, so an indented declaration is used
# whatever gate encloses it. A guard that can be fooled is worse than none, so every shape it is supposed to
# refuse is committed here and must make the guard FAIL - and the one accepted shape must make it PASS.
#
# The refused shapes are the ones a reviewer named on 2026-11-06 as reachable by a shallow detector: a
# production gate, elif, else, a nested if, `[[`, `test`, a case gate, and a REPLACEMENT assignment (one that
# does not read the variable it sets, so it could SHRINK a list rather than grow it).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T="${DEV_CLEANUP_DECL_TEST:-$HERE/dev-cleanup-indented-declaration-test.sh}"
W=$(mktemp -d)
pass=0; fail=0

mk() { printf '%s\n' "$2" > "$W/$1.env"; }

mk control 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.dev:1"
fi'
mk prodgate 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" = "production" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.prod:1"
fi'
mk elifcase 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  :
elif [ "${ENVIRONMENT:-}" = "production" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.prod:1"
fi'
mk elsecase 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  :
else
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.prod:1"
fi'
mk nested 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  if [ "${SOMETHING:-}" = "x" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.x:1"
  fi
fi'
mk dblbracket 'OPTIONS_EDGE_TOPICS="t.base:1"
if [[ "${ENVIRONMENT:-}" != "production" ]]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.dev:1"
fi'
mk testform 'OPTIONS_EDGE_TOPICS="t.base:1"
if test "${ENVIRONMENT:-}" != "production"; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.dev:1"
fi'
mk replace 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS="t.only:1"
fi'
mk casegate 'OPTIONS_EDGE_TOPICS="t.base:1"
case "${ENVIRONMENT:-}" in
  production)
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.prod:1"
  ;;
esac'

run() { # name -> prints the section-4 verdict line
  TOPICS_ENV_REAL="$W/$1.env" bash "$T" 2>/dev/null | grep -E "whitelisted dev-gated APPEND|not a whitelisted" | head -1
}

check() { # name, want (pass|fail)
  local out; out="$(run "$1")"
  case "$out" in
    "  ok "*) got=pass ;;
    *)        got=fail ;;
  esac
  if [ "$got" = "$2" ]; then printf '  ok   %-11s guard=%s (wanted %s)\n' "$1" "$got" "$2"; pass=$((pass+1))
  else printf '  FAIL %-11s guard=%s (wanted %s): %s\n' "$1" "$got" "$2" "$out"; fail=$((fail+1)); fi
}

check control pass
check prodgate fail
check elifcase fail
check elsecase fail
check nested   fail
check dblbracket fail
check testform fail
check replace  fail
check casegate fail

printf 'guard mutation check: %d ok, %d FAILED\n' "$pass" "$fail"
rm -rf "$W"
[ "$fail" = 0 ]
