#!/usr/bin/env bash
# Mutation test for the WHITELIST guard in dev-cleanup-indented-declaration-test.sh (its section 4).
#
# That guard is the only thing between these indent-tolerant readers and a declaration they would apply in the
# wrong environment: they PARSE topics.env, they do not interpret it, so an indented declaration is used whatever
# gate encloses it. A guard that can be fooled is worse than none, so every shape it must refuse is committed
# here and has to produce the guard's own REFUSED line - and the one accepted shape has to produce the guard's
# own accepted line.
#
# Each case is judged on the guard's OWN output, never on the test's exit code: a fixture that broke an unrelated
# section, or a run with no section-4 verdict at all, is reported as INDETERMINATE and fails this test rather
# than counting as a rejection. (Round 2 of review caught exactly that: `*) got=fail` turned every unrelated
# failure into a pass.)
#
# The refused shapes are the ones two review rounds named as reachable by a shallow detector: a production gate,
# elif, else, a nested if, `[[`, `test`, a case gate, a subshell, a brace group, a function body, a REPLACEMENT
# assignment, a `${VAR%...}` transform that SHRINKS the list, a single-quoted assignment that never expands, and
# a replacement hidden after a valid-looking append on the same line.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T="${DEV_CLEANUP_DECL_TEST:-$HERE/dev-cleanup-indented-declaration-test.sh}"
[ -f "$T" ] || { echo "FAIL: $T missing" >&2; exit 1; }
W="$(mktemp -d)"; trap 'rm -rf "$W"' EXIT
pass=0; fail=0

mk() { printf '%s\n' "$2" > "$W/$1.env"; }

# The accepted shape: a plain append, directly inside one of topics.env's two dev gates.
mk control 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.dev:1"
fi'
mk control_dev_gate 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-dev}" = "dev" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.dev:1"
fi'
mk control_brace 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS="${OPTIONS_EDGE_TOPICS} t.dev:1"
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
mk subshell 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  (
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.x:1"
  )
fi'
mk bracegroup 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  {
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.x:1"
  }
fi'
mk funcbody 'OPTIONS_EDGE_TOPICS="t.base:1"
declare_them() {
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.x:1"
}'
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
mk shrink 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS="${OPTIONS_EDGE_TOPICS% t.base:1}"
fi'
mk singlequoted 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS='"'"'$OPTIONS_EDGE_TOPICS t.dev:1'"'"'
fi'
mk trailing 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.ok:1"; OPTIONS_EDGE_TOPICS="t.replaced:1"
fi'
mk heredoc 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
cat <<EOF
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.inheredoc:1"
EOF
fi'
mk multiline_string 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  SOME_TEXT="first line
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.instring:1"
  last line"
fi'
mk continuation 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  some_command --flag \
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.aftercont:1"
fi'
mk control_comment 'OPTIONS_EDGE_TOPICS="t.base:1"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  # an ordinary comment between the gate and the declaration, which must stay legal
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.dev:1"
fi'
mk casegate 'OPTIONS_EDGE_TOPICS="t.base:1"
case "${ENVIRONMENT:-}" in
  production)
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.prod:1"
  ;;
esac'

ACCEPTED='every indented declaration in topics.env is a whitelisted dev-gated APPEND'
REFUSED='is not a whitelisted dev-gated append'

check() { # <fixture> <accept|refuse>
  local name="$1" want="$2" out verdict
  out="$(TOPICS_ENV_REAL="$W/$name.env" bash "$T" 2>/dev/null)"
  # EXACTLY ONE section-4 verdict, or the fixture proves nothing: a run that emits both phrases, or neither,
  # is INDETERMINATE. Taking the first match would let a fixture that also failed elsewhere count as proof.
  local na nr
  na=$(printf '%s' "$out" | grep -cF "$ACCEPTED")
  nr=$(printf '%s' "$out" | grep -cF "$REFUSED")
  if [ "$na" = 1 ] && [ "$nr" = 0 ]; then verdict=accept
  elif [ "$nr" = 1 ] && [ "$na" = 0 ]; then verdict=refuse
  else verdict=INDETERMINATE
  fi
  if [ "$verdict" = "$want" ]; then
    printf '  ok   %-16s guard=%s\n' "$name" "$verdict"; pass=$((pass+1))
  elif [ "$verdict" = INDETERMINATE ]; then
    printf '  FAIL %-16s the guard produced NO verdict (wanted %s) - this fixture proves nothing:\n%s\n' \
      "$name" "$want" "$(printf '%s' "$out" | grep -E '^  (FAIL|ok)' | head -3)"; fail=$((fail+1))
  else
    printf '  FAIL %-16s guard=%s, wanted %s\n' "$name" "$verdict" "$want"; fail=$((fail+1))
  fi
}

check control          accept
check control_dev_gate accept
check control_brace    accept
check prodgate         refuse
check elifcase         refuse
check elsecase         refuse
check nested           refuse
check subshell         refuse
check bracegroup       refuse
check funcbody         refuse
check dblbracket       refuse
check testform         refuse
check replace          refuse
check shrink           refuse
check singlequoted     refuse
check trailing         refuse
check casegate         refuse
check heredoc          refuse
check multiline_string refuse
check continuation     refuse
check control_comment  accept

printf 'dev-cleanup-indented-declaration-mutation-test: %d ok, %d FAILED\n' "$pass" "$fail"
[ "$fail" = 0 ]
