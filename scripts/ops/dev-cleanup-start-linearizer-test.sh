#!/usr/bin/env bash
# `dev-cleanup start` must not leave es-trade-linearizer-service stuck in OUTPUT_TAPE_RECREATED (found 2026-11-12).
#
# THE DEFECT. After a wipe the linearizer is "not ready" for minutes BY DESIGN: a fresh tape orders the last 24 h of
# underlying.es.trades (~430k records) before it is live. start's boot-order straggler pass restarted it at 90 s, before it had
# written a record; its own guard then refused every restart ("OUTPUT_TAPE_RECREATED: the output tape is empty but the consumer
# group holds committed offsets"), and the operator deleted the consumer group by hand after the clean.
#
# Sections (no broker, no cluster; functions are lifted out BY NAME from the file under test and run against stubs):
#   1  linearizer_straggler_exempt names the linearizer and nothing else
#   2  the straggler loop of the REAL do_start skips the linearizer and still restarts any other straggler
#   3  heal_linearizer_recreated_tape: group deleted ONLY when the log says OUTPUT_TAPE_RECREATED AND the output tape is
#      measured empty; every other case leaves the group alone
#   4  mutants: take each fix out of a copy of the script and run THIS WHOLE FILE against it: it must fail
# DEV_CLEANUP_UNDER_TEST points the file at a mutant; LINEARIZER_NO_MUTANTS=1 skips section 4 (child runs set it).
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
SELF="$HERE/dev-cleanup-start-linearizer-test.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -f "$SRC" ] || { echo "FAIL: $SRC missing" >&2; exit 1; }

fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p {print} p && /^\}/ {exit}' "$2"; }
var() { grep -E "^$1=" "$2" | head -1; }

# ---- 1 ----
echo "1  linearizer_straggler_exempt"
( eval "$(var LINEARIZER_DEPLOY "$SRC")"; eval "$(fn linearizer_straggler_exempt "$SRC")"
  linearizer_straggler_exempt es-trade-linearizer-service ) && ok "the linearizer is exempt" || bad "the linearizer is not exempt from the straggler restart"
( eval "$(var LINEARIZER_DEPLOY "$SRC")"; eval "$(fn linearizer_straggler_exempt "$SRC")"
  ! linearizer_straggler_exempt level-absorption-service && ! linearizer_straggler_exempt es-trade-linearizer-service-x && ! linearizer_straggler_exempt "" ) \
  && ok "nothing else is exempt (no prefix match, no empty match)" || bad "the exemption matches more than the linearizer"

# ---- 2: the straggler loop of the REAL do_start ----
echo "2  the straggler loop in do_start"
# Run only the loop: cut do_start from "Settling" up to the self-heal summary line, with $KK/$K as recording stubs.
LOOP="$WORK/loop.sh"
awk '/^do_start\(\) \{/ {p=1} p && /Settling \(90s\)/ {q=1} q {print} q && /self-heal restarted/ {exit}' "$SRC" | grep -v '^[[:space:]]*sleep 90' > "$LOOP"
[ -s "$LOOP" ] || bad "could not cut the straggler loop out of do_start"
cat > "$WORK/kk" <<'EOF'
#!/usr/bin/env bash
# kubectl stub: two deployments, both desired 1 / ready 0
case "$*" in
  "get deploy -o name") printf 'deployment.apps/es-trade-linearizer-service\ndeployment.apps/some-other-service\n' ;;
  *"jsonpath={.spec.replicas}"*) echo 1 ;;
  *"jsonpath={.status.readyReplicas}"*) echo 0 ;;
esac
EOF
cat > "$WORK/k" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$WORK/k.calls"
EOF
chmod +x "$WORK/kk" "$WORK/k"
: > "$WORK/k.calls"
(
  KK="$WORK/kk"; K="$WORK/k"; DISABLED_DEV='^$'
  eval "$(var LINEARIZER_DEPLOY "$SRC")"; eval "$(fn linearizer_straggler_exempt "$SRC")"
  healed=0
  eval "$(cat "$LOOP")" > "$WORK/loop.out" 2>&1
)
grep -q 'rollout restart deploy/some-other-service' "$WORK/k.calls" && ok "an ordinary straggler is still restarted" || bad "an ordinary straggler was not restarted"
grep -q 'rollout restart deploy/es-trade-linearizer-service' "$WORK/k.calls" && bad "the linearizer was restarted as a straggler" || ok "the linearizer is not restarted as a straggler"

fn do_start "$SRC" | grep -q '^  heal_linearizer_recreated_tape' && ok "do_start calls heal_linearizer_recreated_tape after the straggler pass" || bad "do_start never calls heal_linearizer_recreated_tape"

# ---- 3: heal_linearizer_recreated_tape ----
echo "3  heal_linearizer_recreated_tape"
mkdir -p "$WORK/kbin"
for t in kafka-consumer-groups.sh kafka-get-offsets.sh; do :; done
cat > "$WORK/kbin/kafka-topics.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat > "$WORK/kbin/kafka-consumer-groups.sh" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$WORK/groups.calls"
[ "\${STUB_GROUP_FAIL:-0}" = 1 ] && exit 1
exit 0
EOF
cat > "$WORK/kbin/kafka-get-offsets.sh" <<'EOF'
#!/usr/bin/env bash
case "${STUB_OFFSETS:-empty}" in
  empty) printf 'underlying.es.trades.linearized:0:0\n' ;;
  full)  printf 'underlying.es.trades.linearized:0:433858\n' ;;
  none)  exit 1 ;;
  blank) exit 0 ;;
esac
EOF
cat > "$WORK/kk3" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *"jsonpath={.spec.replicas}"*) echo "${STUB_REPLICAS:-1}" ;;
  logs*) printf '%s\n' "${STUB_LOG:-}" ;;
esac
exit 0
EOF
cat > "$WORK/k3" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$WORK/k3.calls"
EOF
chmod +x "$WORK"/kbin/* "$WORK/kk3" "$WORK/k3"

heal() {  # heal LOG OFFSETS [REPLICAS] [GROUP_FAIL] -> prints the groups/k3 calls, returns the function's status
  : > "$WORK/groups.calls"; : > "$WORK/k3.calls"
  ( KK="$WORK/kk3"; K="$WORK/k3"; KT="$WORK/kbin/kafka-topics.sh"; BS=stub:1
    export STUB_LOG="$1" STUB_OFFSETS="$2" STUB_REPLICAS="${3:-1}" STUB_GROUP_FAIL="${4:-0}"
    eval "$(var LINEARIZER_DEPLOY "$SRC")"; eval "$(var LINEARIZER_GROUP "$SRC")"; eval "$(var LINEARIZER_OUTPUT_TOPIC "$SRC")"
    eval "$(fn linearizer_output_end "$SRC")"; eval "$(fn heal_linearizer_recreated_tape "$SRC")"
    heal_linearizer_recreated_tape ) > "$WORK/heal.out" 2>&1
}
deleted() { grep -q -- '--delete --group options-edge-es-trade-linearizer' "$WORK/groups.calls"; }

heal "ES_LINEARIZER_FAILED OUTPUT_TAPE_RECREATED" empty
rc=$?
{ deleted && [ $rc -eq 0 ]; } && ok "marker + empty output tape: the group is deleted" || bad "marker + empty tape did not delete the group (rc=$rc)"
grep -q 'scale deploy/es-trade-linearizer-service --replicas=0' "$WORK/k3.calls" && grep -q 'scale deploy/es-trade-linearizer-service --replicas=1' "$WORK/k3.calls" \
  && ok "the service is scaled to 0 before the delete and back to its replicas after" || bad "the scale down/up around the delete is missing"

heal "everything is fine" empty
deleted && bad "no marker in the log, group deleted anyway" || ok "no marker: the group is left alone"

heal "ES_LINEARIZER_FAILED OUTPUT_TAPE_RECREATED" full
rc=$?
{ ! deleted && [ $rc -ne 0 ]; } && ok "marker but the output tape holds records: group NOT deleted, reported" || bad "group deleted while the output tape is not empty (rc=$rc)"

heal "ES_LINEARIZER_FAILED OUTPUT_TAPE_RECREATED" none
rc=$?
{ ! deleted && [ $rc -ne 0 ]; } && ok "output tape unmeasurable (CLI fails): group NOT deleted" || bad "group deleted although the tape could not be measured (rc=$rc)"

heal "ES_LINEARIZER_FAILED OUTPUT_TAPE_RECREATED" blank
rc=$?
{ ! deleted && [ $rc -ne 0 ]; } && ok "output tape unmeasurable (empty answer): group NOT deleted" || bad "an empty answer was read as an empty tape (rc=$rc)"

heal "ES_LINEARIZER_FAILED OUTPUT_TAPE_RECREATED" empty 0
{ ! deleted; } && ok "service scaled to 0: nothing touched" || bad "touched the group of a service that is switched off"

heal "ES_LINEARIZER_FAILED OUTPUT_TAPE_RECREATED" empty 1 1
rc=$?
{ [ $rc -ne 0 ] && grep -q 'scale deploy/es-trade-linearizer-service --replicas=1' "$WORK/k3.calls"; } \
  && ok "a failed group delete returns non-zero and puts the service back" || bad "a failed group delete was swallowed or left the service at 0 (rc=$rc)"

# ---- 4: mutants ----
if [ "${LINEARIZER_NO_MUTANTS:-0}" != 1 ]; then
  echo "4  mutants (each must make this whole file fail)"
  mut() {  # mut LABEL PYTHON-EXPR-on-s
    local label="$1" expr="$2" f="$WORK/mut-$$-$RANDOM.sh"
    python3 - "$SRC" "$f" "$expr" <<'PY'
import sys
s=open(sys.argv[1]).read(); o=s
s=eval(sys.argv[3])
if s==o: sys.exit(3)
open(sys.argv[2],'w').write(s)
PY
    local prc=$?
    if [ $prc -ne 0 ]; then bad "mutant '$label' could not be built (the text it edits has moved)"; return; fi
    if DEV_CLEANUP_UNDER_TEST="$f" LINEARIZER_NO_MUTANTS=1 bash "$SELF" >/dev/null 2>&1; then bad "mutant '$label' SURVIVED"; else ok "mutant '$label' is caught"; fi
  }
  mut "straggler exemption removed" "s.replace('if linearizer_straggler_exempt \"\$d\"; then','if false; then')"
  mut "exempt matches every deployment" "s.replace('linearizer_straggler_exempt() { [ \"\$1\" = \"\$LINEARIZER_DEPLOY\" ]; }','linearizer_straggler_exempt() { true; }')"
  mut "heal does not require the marker" "s.replace(\"grep -q 'OUTPUT_TAPE_RECREATED' || return 0\",'true')"
  mut "heal does not require an empty tape" "s.replace('if [ \"\$end\" != \"0\" ]; then','if false; then')"
  mut "an unmeasured tape counts as empty" "s.replace('end=\$(linearizer_output_end) ||','end=\$(linearizer_output_end || echo 0) ||')"
  mut "heal never runs after the straggler pass" "s.replace('  heal_linearizer_recreated_tape || true\\n','')"
fi

echo
if [ $fails -eq 0 ]; then echo "ALL OK"; else echo "$fails FAILED"; exit 1; fi
