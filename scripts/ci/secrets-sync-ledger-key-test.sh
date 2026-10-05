#!/usr/bin/env bash
# The ZERO_DTE_LEDGER_KEY handling of Jenkinsfile.secrets-sync (Codex 7b r1 D9 + r2): the key is a cryptographic ROOT, and the sync must
# never degrade it. The EXACT shell block of stage('Sync runtime secrets') is extracted (stage-bounded, never a copy) and driven against a
# FAKE kubectl through: an existing key with no credential bound (kept byte for byte), an absent Secret (NotFound: written empty), an
# UNREADABLE Secret (no apply), an undecodable existing key (no apply), a malformed bound credential (no apply), a well-formed credential
# (applied). Each case asserts the exit status, the verdict text, and WHETHER an apply happened and with WHICH key value.
set -euo pipefail
cd "$(dirname "$0")/../.."
JF="Jenkinsfile.secrets-sync"
BLOCK="$(python3 - "$JF" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
start = t.find("stage('Sync runtime secrets')")
if start < 0: sys.exit("FAIL: no stage('Sync runtime secrets')")
nxt = re.search(r"\n\s*stage\('", t[start + 1:])
body = t[start: start + 1 + nxt.start()] if nxt else t[start:]
blocks = re.findall(r"sh '''\n(.*?)\n\s*'''", body, re.S)
if len(blocks) != 1: sys.exit("FAIL: stage('Sync runtime secrets') must hold exactly one sh ''' block, found %d" % len(blocks))
print(blocks[0].replace("\\\\", "\\"))   # Groovy's ''' string: \\ is one backslash in the shell
PY
)" || { echo "$BLOCK"; exit 1; }
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
printf '%s\n' "$BLOCK" > "$T/sync.sh"; bash -n "$T/sync.sh"
mkdir -p "$T/bin"
cat > "$T/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
args="$*"
echo "kubectl $args" >> "${FAKE_JOURNAL:?}"
case "$args" in
  *"get secret options-edge-runtime-secrets -o json")
    case "${FAKE_SECRET:-present}" in
      present)  printf '{"data":{"ZERO_DTE_LEDGER_KEY":"%s","POSTGRES_PASSWORD":"eA=="}}' "$(printf '%s' "${FAKE_EXISTING_KEY:-}" | base64 | tr -d '\n')" ;;
      nokey)    printf '{"data":{"POSTGRES_PASSWORD":"eA=="}}' ;;
      emptykey) printf '{"data":{"ZERO_DTE_LEDGER_KEY":"","POSTGRES_PASSWORD":"eA=="}}' ;;
      nodata)   printf '{"metadata":{"name":"options-edge-runtime-secrets"}}' ;;
      garbage)  printf '{"data":{"ZERO_DTE_LEDGER_KEY":"%%%%not-base64%%%%"}}' ;;
      absent)   echo 'Error from server (NotFound): secrets "options-edge-runtime-secrets" not found' >&2; exit 1 ;;
      error)    echo 'Unable to connect to the server: EOF' >&2; exit 1 ;;
    esac ;;
  *"create secret generic options-edge-runtime-secrets"*"--dry-run=client -o yaml")
    for a in "$@"; do case "$a" in --from-literal=ZERO_DTE_LEDGER_KEY=*) printf 'rendered-key=%s\n' "${a#--from-literal=ZERO_DTE_LEDGER_KEY=}" ;; esac; done ;;
  *"apply"*" -f -") cat > "${FAKE_APPLIED:?}" ;;
  *"get secret options-edge-runtime-secrets -o go-template"*) printf 'POSTGRES_PASSWORD\nZERO_DTE_LEDGER_KEY\n' ;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99 ;;
esac
FAKE
chmod +x "$T/bin/kubectl"
GOOD="$(printf 'a%.0s' $(seq 64))"
pass=0; fail=0
run() { # run <name> <want_rc> <want substring> <want applied: none|<key value>> [VAR=value ...]
  local name="$1" want_rc="$2" want="$3" applied="$4"; shift 4
  local out rc journal="$T/journal.$RANDOM" file="$T/applied.$RANDOM"
  : > "$journal"; rm -f "$file"
  out="$(env -i PATH="$T/bin:$PATH" HOME="$T" FAKE_JOURNAL="$journal" FAKE_APPLIED="$file" NAMESPACE=options-edge ENVIRONMENT=dev OE_SECRETS_DRY_RUN=false POSTGRES_PASSWORD=pw "$@" bash "$T/sync.sh" 2>&1)" && rc=0 || rc=$?
  local ok=1
  [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want" || ok=0
  if [ "$applied" = none ]; then [ -f "$file" ] && ok=0; else { [ -f "$file" ] && grep -qxF "rendered-key=$applied" "$file"; } || ok=0; fi
  if [ "$ok" = 1 ]; then pass=$((pass+1)); echo "  ok   $name (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]; applied=$([ -f "$file" ] && cat "$file" || echo none) want [$applied]"; printf '%s\n' "$out" | tail -4 | sed 's/^/       | /'; fi
}
run "no credential, existing key kept byte for byte"  0 "the existing value is kept (present)" "$GOOD" FAKE_EXISTING_KEY="$GOOD"
run "no credential, Secret without the key"           0 "the existing value is kept (empty)" "" FAKE_SECRET=nokey
run "no credential, no Secret yet (NotFound)"         0 "no Secret yet — written empty" "" FAKE_SECRET=absent
run "no credential, existing key PRESENT BUT EMPTY: no apply" 1 "is present but EMPTY" none FAKE_SECRET=emptykey
run "no credential, Secret without .data"             0 "the existing value is kept (empty)" "" FAKE_SECRET=nodata
run "no credential, Secret UNREADABLE: no apply"      1 "the existing Secret could not be read" none FAKE_SECRET=error
run "no credential, existing key undecodable: no apply" 1 "could not be decoded" none FAKE_SECRET=garbage
run "credential bound, not hex: no apply"             1 "is not hex; refusing to sync" none ZERO_DTE_LEDGER_KEY="zz$GOOD"
run "credential bound, too short: no apply"           1 "63 characters, 64+ hex required" none ZERO_DTE_LEDGER_KEY="${GOOD:1}"
run "credential bound, well-formed: applied"          0 "credential bound and well-formed (64 hex characters)" "$GOOD" ZERO_DTE_LEDGER_KEY="$GOOD"
run "credential bound, existing key differs: the credential wins" 0 "credential bound and well-formed" "$GOOD" ZERO_DTE_LEDGER_KEY="$GOOD" FAKE_EXISTING_KEY="$(printf 'b%.0s' $(seq 64))"
run "dry-run flag unresolved: nothing"                1 "dry-run flag did not resolve" none OE_SECRETS_DRY_RUN=
# the key's VALUE never reaches stdout: no echo line expands it (its LENGTH and its PRESENCE may be printed)
if printf '%s\n' "$BLOCK" | sed -e 's/\${#ZERO_DTE_LEDGER_KEY}//g' -e 's/\[ -n "\${ZERO_DTE_LEDGER_KEY:-}" \]//g' | grep -E '^\s*echo ' | grep -qE '\$\{?ZERO_DTE_LEDGER_KEY'; then
  fail=$((fail+1)); echo "  FAIL an echo line expands the key's value"
else
  pass=$((pass+1)); echo "  ok   no echo line expands the key's value (length and presence only)"
fi
echo "secrets-sync ledger key: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== secrets-sync-ledger-key-test: OK ==="; exit 0; }
echo "=== secrets-sync-ledger-key-test: FAILED ==="; exit 1
