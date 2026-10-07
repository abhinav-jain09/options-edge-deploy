#!/usr/bin/env bash
# ansible/templates/mirror-group-check.sh.j2 decides what an ABSENT consumer group means, and the
# three outcomes it must keep apart are easy to collapse: an earlier version treated a SUCCESSFUL but
# empty listing as a failure, which is the correct answer on a source with no groups — exactly the
# fresh source the first-install escape hatch exists for — so -e allow_new_consumer_group=true could
# never be used. Driven here against a stubbed kafka-consumer-groups, once per outcome.
set -euo pipefail
cd "$(dirname "$0")/../.."
TPL=ansible/templates/mirror-group-check.sh.j2
[ -r "$TPL" ] || { echo "FAIL: $TPL not readable"; exit 1; }
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/bin"
G=es-cvd-mirror-192.168.100.252-es.futures.cvd

render() { # allow(true|false)
  sed -E -e "s#\{\{ oe_kbin \| quote \}\}#'$WORK/bin'#g" \
         -e "s#\{\{ item\.source \| quote \}\}#'192.168.100.4:9092'#g" \
         -e "s#\{\{ item\.group_id \| quote \}\}#'$G'#g" \
         -e "s#\{\{ \(oe_allow_new_group \| lower\) \| quote \}\}#'$1'#g" \
         -e "s#\{\{ item\.offset_reset \| quote \}\}#'earliest'#g" "$TPL" > "$WORK/check.sh"
  if grep -q '{{' "$WORK/check.sh"; then echo "  FAIL: unresolved expressions — this test's substitution list is stale"; grep -n '{{' "$WORK/check.sh" | head -3; return 1; fi
  chmod +x "$WORK/check.sh"
}
# the stub: $WORK/mode decides what `--list` does
cat > "$WORK/bin/kafka-consumer-groups" <<K
#!/usr/bin/env bash
case "\$(cat "$WORK/mode")" in
  present) echo "some-other-group"; echo "$G"; exit 0 ;;
  absent)  echo "some-other-group"; exit 0 ;;
  empty)   exit 0 ;;
  fails)   echo "Error: Connection to node -1 failed" >&2; exit 1 ;;
esac
exit 0
K
chmod +x "$WORK/bin/kafka-consumer-groups"

fail=0
case_is() { # name  mode  allow  expect(pass|fail)  want
  local name="$1" mode="$2" allow="$3" expect="$4" want="$5"
  echo "$mode" > "$WORK/mode"
  render "$allow" || { fail=1; return; }
  set +e; out=$(bash "$WORK/check.sh" 2>&1); rc=$?; set -e
  local ok=0
  { [ "$expect" = pass ] && [ "$rc" = 0 ]; } && ok=1
  { [ "$expect" = fail ] && [ "$rc" != 0 ]; } && ok=1
  if [ "$ok" = 1 ] && printf '%s' "$out" | grep -q -- "$want"; then
    printf '  ok   %-52s rc=%s %s\n' "$name" "$rc" "$(printf '%s' "$out" | head -1 | cut -c1-48)"
  else
    printf '  FAIL %-52s rc=%s want=%s/%s got=%s\n' "$name" "$rc" "$expect" "$want" "$out"; fail=1
  fi
}

case_is "the group exists"                        present false pass "group exists"
case_is "absent, and not allowed"                 absent  false fail "does not exist"
case_is "absent, and explicitly allowed"          absent  true  pass "FIRST install"
case_is "an EMPTY listing is not a failure"       empty   true  pass "FIRST install"
case_is "an empty listing still needs the flag"   empty   false fail "does not exist"
case_is "a FAILED listing is refused, flag or not" fails  true  fail "cannot list consumer groups"
case_is "a failed listing with no flag"           fails   false fail "cannot list consumer groups"

[ "$fail" = 0 ] || { echo "mirror group check: FAILED"; exit 1; }
echo "mirror group check: 7 cases behave as specified"
