#!/usr/bin/env bash
# apply-topics-sibling-files.sh must name every sibling apply-topics.sh sources, DERIVE that list from
# the script rather than hold a copy of it, refuse rather than return nothing, and actually be used by
# the tests that run apply-topics.sh out of a temp directory.
#
# The last case is the one that matters most: the defect this helper closes was not a wrong list, it was
# two tests each carrying their own list. A test that hard-codes the copy again reopens it.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
H="$HERE/apply-topics-sibling-files.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

[ -x "$H" ] || { echo "FAIL: $H missing or not executable" >&2; exit 1; }

echo "1. today's siblings"
out="$(bash "$H" 2>"$WORK/err")" || { bad "the helper exited non-zero on the real apply-topics.sh: $(cat "$WORK/err")"; out=""; }
for want in topics.env resolve-prod-partition-overrides.sh; do
  printf '%s\n' "$out" | grep -qx "$want" && ok "names $want" || bad "does not name $want"
done
# Every name must be a file that is really there: a typo'd sibling would make each caller's `cp` fail.
while read -r f; do
  [ -z "$f" ] && continue
  [ -e "$HERE/$f" ] && ok "$f exists beside apply-topics.sh" || bad "$f does not exist beside apply-topics.sh"
done <<< "$out"

echo "2. the list is COMPLETE, cross-checked by a different engine"
# Scoped identically but written independently (python, not sed): this catches an under-read, which is
# the failure mode that matters -- a sibling the helper misses is a sibling no caller copies.
indep="$(python3 - "$HERE/apply-topics.sh" <<'PY'
import re, sys
s = open(sys.argv[1]).read()
names = []
for m in re.finditer(r'(?m)^\s*(?:source|\.)\s+"?\$\{?SCRIPT_DIR\}?/([A-Za-z0-9._-]+)"?\s*$', s):
    if m.group(1) not in names:
        names.append(m.group(1))
print("\n".join(names))
PY
)"
if [ "$(printf '%s\n' "$out" | sort)" = "$(printf '%s\n' "$indep" | sort)" ]; then
  ok "the helper and an independent read agree on $(printf '%s\n' "$indep" | grep -c .) sibling(s)"
else
  bad "the helper and an independent read DISAGREE:$(printf '\n    helper: %s\n    indep:  %s' "$(echo $out)" "$(echo $indep)")"
fi

echo "3. DERIVED, not declared: a sibling nobody has written yet is reported"
cat > "$WORK/fake.sh" <<'FAKE'
#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/topics.env"
. "$SCRIPT_DIR/brand-new-sibling.sh"
source "${SCRIPT_DIR}/braced-sibling.sh"
FAKE
got="$(bash "$H" "$WORK/fake.sh" 2>&1)"
for want in topics.env brand-new-sibling.sh braced-sibling.sh; do
  printf '%s\n' "$got" | grep -qx "$want" && ok "reports $want from a script it has never seen" || bad "missed $want: $got"
done

echo "4. FAILS CLOSED: no sibling at all is a refusal, not an empty list"
printf '#!/usr/bin/env bash\necho hi\n' > "$WORK/bare.sh"
rc=0; got="$(bash "$H" "$WORK/bare.sh" 2>&1)" || rc=$?
[ "$rc" -ne 0 ] && ok "refused a script with no siblings (exit $rc)" || bad "returned success on a script with no siblings"
printf '%s' "$got" | grep -q 'found no .*sibling' && ok "and says why" || bad "the refusal does not say why: $got"
rc=0; bash "$H" "$WORK/nope-does-not-exist.sh" >/dev/null 2>&1 || rc=$?
[ "$rc" -ne 0 ] && ok "refused an unreadable target (exit $rc)" || bad "accepted an unreadable target"

echo "5. MUTATION: a helper whose parse returns nothing must refuse, not print an empty list"
mkdir -p "$WORK/mut"; cp "$H" "$WORK/mut/h.sh"
# Neuter the extraction only (the sed program becomes one that matches nothing), leaving the refusal arm
# intact. Verified to have applied before the run is believed.
python3 - "$WORK/mut/h.sh" <<'PY'
import re, sys
p = sys.argv[1]; s = open(p).read()
s2 = s.replace("sed -nE 's/^[[:space:]]*(source|\\.)", "sed -nE 's/^ZZNOMATCHZZ(source|\\.)", 1)
assert s2 != s, "the mutation did not apply -- the extraction line has moved or been reworded"
open(p, "w").write(s2)
PY
rc=0; got="$(bash "$WORK/mut/h.sh" "$HERE/apply-topics.sh" 2>&1)" || rc=$?
[ "$rc" -ne 0 ] && ok "the neutered parse REFUSES (exit $rc)" || bad "the neutered parse exited 0 with: $got"

echo "6. every test that copies apply-topics.sh into a temp dir uses the helper"
# Derived from the tree, so a NEW test that hard-codes its copy list fails here rather than discovering
# the next missing sibling as a wrong verdict about somebody's declaration.
callers="$(grep -rl 'cp "\$HERE/apply-topics\.sh"\|cp "\$HERE/apply-topics\.sh" ' "$HERE/.." --include='*test*.sh' 2>/dev/null | sort -u)"
[ -n "$callers" ] && ok "found $(printf '%s\n' "$callers" | wc -l | tr -d ' ') test(s) that copy apply-topics.sh" \
  || bad "found NO test that copies apply-topics.sh — this check has gone vacuous, re-derive it"
for c in $callers; do
  if grep -q 'apply-topics-sibling-files\.sh' "$c"; then ok "$(basename "$c") copies the siblings via the helper"
  else bad "$(basename "$c") copies apply-topics.sh but not its siblings — it will run a crippled copy"; fi
done

echo
if [ "$fails" -eq 0 ]; then echo "=== apply-topics-sibling-files: OK ==="; exit 0; fi
echo "=== apply-topics-sibling-files: $fails problem(s) ===" >&2; exit 1
