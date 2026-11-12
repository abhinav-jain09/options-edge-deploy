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

fn do_start "$SRC" | grep -q '^  local heal_rc=0; heal_linearizer_recreated_tape' && ok "do_start calls heal_linearizer_recreated_tape after the straggler pass" || bad "do_start never calls heal_linearizer_recreated_tape"

# ---- 3: heal_linearizer_recreated_tape ----
echo "3  heal_linearizer_recreated_tape"
mkdir -p "$WORK/kbin"
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
  empty)  printf 'underlying.es.trades.linearized:0:0\n' ;;
  full)   printf 'underlying.es.trades.linearized:0:433858\n' ;;
  mixed)  printf 'underlying.es.trades.linearized:0:0\nWARN something unrelated\n' ;;
  wrongtopic) printf 'some.other.topic:0:0\n' ;;
  twoparts) printf 'underlying.es.trades.linearized:0:0\nunderlying.es.trades.linearized:1:7\n' ;;
  none)   exit 1 ;;
  blank)  exit 0 ;;
esac
EOF
# kubectl (read) stub: replicas, readyReplicas, logs, and pods ONLY for the Deployment's real selector
cat > "$WORK/kk3" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *"jsonpath={.spec.replicas}"*) echo "${STUB_REPLICAS:-1}" ;;
  *"jsonpath={.status.readyReplicas}"*) [ "${STUB_READY_FAIL:-0}" = 1 ] && exit 1; echo "${STUB_READY:-0}" ;;
  logs*) printf '%s\n' "${STUB_LOG:-}" ;;
  "get pods -l app.kubernetes.io/name=es-trade-linearizer-service -o name") [ "${STUB_PODS_FAIL:-0}" = 1 ] && exit 1; [ "${STUB_PODS_REMAIN:-0}" = 1 ] && echo pod/es-trade-linearizer-service-x ;;
esac
exit 0
EOF
# kubectl (write) stub
cat > "$WORK/k3" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$WORK/k3.calls"
case "\$*" in *"--replicas=0"*) [ "\${STUB_SCALE_FAIL:-0}" = 1 ] && exit 1 ;; *"--replicas=1"*) [ "\${STUB_SCALE_UP_FAIL:-0}" = 1 ] && exit 1 ;; esac
exit 0
EOF
chmod +x "$WORK"/kbin/* "$WORK/kk3" "$WORK/k3"

heal() {  # heal LOG OFFSETS ; the other knobs (STUB_REPLICAS STUB_READY STUB_GROUP_FAIL STUB_SCALE_FAIL STUB_PODS_REMAIN) come from the caller's environment
  : > "$WORK/groups.calls"; : > "$WORK/k3.calls"
  ( KK="$WORK/kk3"; K="$WORK/k3"; KT="$WORK/kbin/kafka-topics.sh"; BS=stub:1
    export STUB_LOG="$1" STUB_OFFSETS="$2" LINEARIZER_WAIT_TRIES=2 LINEARIZER_POLL_SECS=0
    eval "$(var LINEARIZER_DEPLOY "$SRC")"; eval "$(var LINEARIZER_CONTAINER "$SRC")"; eval "$(var LINEARIZER_GROUP "$SRC")"; eval "$(var LINEARIZER_OUTPUT_TOPIC "$SRC")"
    eval "$(fn linearizer_output_end "$SRC")"; eval "$(fn linearizer_pods "$SRC")"; eval "$(fn heal_linearizer_recreated_tape "$SRC")"
    heal_linearizer_recreated_tape ) > "$WORK/heal.out" 2>&1
}
deleted() { grep -q -- '--delete --group options-edge-es-trade-linearizer' "$WORK/groups.calls"; }
restored() { grep -q 'scale deploy/es-trade-linearizer-service --replicas=1' "$WORK/k3.calls"; }
MARK="ES_LINEARIZER_FAILED OUTPUT_TAPE_RECREATED"

heal "$MARK" empty; rc=$?
{ deleted && [ $rc -eq 0 ]; } && ok "marker + not ready + empty output tape: the group is deleted" || bad "marker + empty tape did not delete the group (rc=$rc)"
grep -q 'scale deploy/es-trade-linearizer-service --replicas=0' "$WORK/k3.calls" && restored \
  && ok "the service is scaled to 0 before the delete and back to its replicas after" || bad "the scale down/up around the delete is missing"

heal "everything is fine" empty
deleted && bad "no marker in the log, group deleted anyway" || ok "no marker: the group is left alone"

STUB_READY=1 heal "$MARK" empty
{ ! deleted && [ ! -s "$WORK/k3.calls" ]; } && ok "a READY deployment with a stale marker in its log is left alone" || bad "touched a ready deployment because of a stale log marker"

heal "$MARK" full; rc=$?
{ ! deleted && [ $rc -ne 0 ]; } && ok "output tape holds records: group NOT deleted, reported" || bad "group deleted while the output tape is not empty (rc=$rc)"
for m in none blank mixed wrongtopic; do
  heal "$MARK" $m; rc=$?
  { ! deleted && [ $rc -ne 0 ]; } && ok "output tape answer '$m' is not a measurement: group NOT deleted" || bad "answer '$m' was read as an empty tape (rc=$rc)"
done
heal "$MARK" twoparts; rc=$?
{ ! deleted && [ $rc -ne 0 ]; } && ok "a second partition with records: group NOT deleted" || bad "a non-empty second partition was ignored (rc=$rc)"

STUB_REPLICAS=0 heal "$MARK" empty
{ ! deleted; } && ok "service scaled to 0: nothing touched" || bad "touched the group of a service that is switched off"

STUB_SCALE_FAIL=1 heal "$MARK" empty; rc=$?
{ ! deleted && [ $rc -ne 0 ]; } && ok "scale-down fails: group NOT deleted" || bad "group deleted although the scale-down failed (rc=$rc)"

STUB_PODS_REMAIN=1 heal "$MARK" empty; rc=$?
{ ! deleted && [ $rc -ne 0 ] && restored; } && ok "pods still present after scale-down: group NOT deleted, replicas restored" || bad "group deleted while pods were still running, or replicas not restored (rc=$rc)"

STUB_READY_FAIL=1 heal "$MARK" empty; rc=$?
{ ! deleted && [ $rc -ne 0 ] && [ ! -s "$WORK/k3.calls" ]; } && ok "readiness query fails: unknown is not 'not ready' — nothing touched" || bad "group deleted although readiness could not be read (rc=$rc)"

STUB_PODS_FAIL=1 heal "$MARK" empty; rc=$?
{ ! deleted && [ $rc -ne 0 ] && restored; } && ok "pod listing fails: unknown is not 'no pods' — group NOT deleted, replicas restored" || bad "group deleted although the pod listing failed (rc=$rc)"

STUB_SCALE_UP_FAIL=1 heal "$MARK" empty; rc=$?
{ deleted && [ $rc -ne 0 ] && grep -q 'scale-up to 1 FAILED' "$WORK/heal.out"; } && ok "a failed scale-up after the delete is returned and named" || bad "a failed scale-up after the group delete was swallowed (rc=$rc)"

STUB_GROUP_FAIL=1 heal "$MARK" empty; rc=$?
{ [ $rc -ne 0 ] && restored; } && ok "a failed group delete returns non-zero and puts the service back" || bad "a failed group delete was swallowed or left the service at 0 (rc=$rc)"

# report_linearizer_status names a cause only when the log confirms it
rep() {
  ( KK="$WORK/kk3"; export STUB_LOG="$1"; eval "$(var LINEARIZER_DEPLOY "$SRC")"; eval "$(var LINEARIZER_CONTAINER "$SRC")"; eval "$(fn report_linearizer_status "$SRC")"; report_linearizer_status ) 2>&1
}
STUB_READY_FAIL=1 rep "es-trade-linearizer fresh tape starts 24h back: 1 source records to order before the live tail" | grep -q 'could not read its readiness' && ok "report: an unreadable readiness is not reported as a backlog" || bad "report: an unreadable readiness was reported as something else"
rep "es-trade-linearizer fresh tape starts 24h back: 433504 source records to order before the live tail" | grep -q 'ordering its backlog' && ok "report: backlog named when the log says so" || bad "report: backlog not recognised"
rep "something else entirely" | grep -q 'does NOT show the backlog' && ok "report: no cause named when the log does not confirm" || bad "report: named a cause the log does not show"
rep "$MARK" | grep -q 'heal did not clear it' && ok "report: an unhealed OUTPUT_TAPE_RECREATED is called out" || bad "report: unhealed OUTPUT_TAPE_RECREATED not called out"
# the REAL tail of do_start (from the heal call to the end), with everything it calls stubbed: the heal's status must reach the exit
TAILF="$WORK/dostart-tail.sh"
{ echo 'do_start_tail() {'; awk '/^do_start\(\) \{/ {p=1} p && /^  local heal_rc=0/ {q=1} q {print} q && /^\}/ {exit}' "$SRC"; } > "$TAILF"
run_tail_rc() { local h="$1" v="$2"; ( sleep() { :; }; apply_internal_topic_configs() { :; }; run_partition_doctor() { :; }; report_session_bound_services() { :; }; report_linearizer_status() { :; }
    heal_linearizer_recreated_tape() { return "$h"; }; verify_start_readiness() { return "$v"; }
    KK="$WORK/kk3"; K="$WORK/k3"; KC=true; BS=stub:1
    eval "$(cat "$TAILF")"; do_start_tail >/dev/null 2>&1; echo $? ); }
[ "$(run_tail_rc 0 0)" = 0 ] && ok "start exits 0 when the heal and the readiness check are fine" || bad "start did not exit 0 on a clean tail"
[ "$(run_tail_rc 1 0)" != 0 ] && ok "a heal that failed makes start exit non-zero" || bad "start exited 0 although the linearizer heal failed"
[ "$(run_tail_rc 0 3)" = 3 ] && ok "a failed readiness check still sets the exit status" || bad "the readiness status was lost"
fn do_start "$SRC" | grep -q '^  report_linearizer_status' && ok "do_start reports the linearizer's status" || bad "do_start never reports the linearizer's status"

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
  mut "heal never runs after the straggler pass" "s.replace('  local heal_rc=0; heal_linearizer_recreated_tape || heal_rc=\$?','  local heal_rc=0; true || heal_rc=\$?')"
  mut "heal ignores whether the deployment is ready" "s.replace('[ \"\${rr:-0}\" -lt \"\$des\" ] 2>/dev/null || return 0\\n  log=','true\\n  log=')"
  mut "a failed scale-down is ignored" "s.replace('if ! \$K scale deploy/\"\$LINEARIZER_DEPLOY\" --replicas=0 >/dev/null 2>&1; then','if false; then')"
  mut "pods are looked up by the wrong label" "s.replace('-l \"app.kubernetes.io/name=\$LINEARIZER_DEPLOY\"','-l \"app=\$LINEARIZER_DEPLOY\"')"
  mut "start swallows a failed heal" "s.replace('if [ \"\$verify_rc\" -eq 0 ] && [ \"\$heal_rc\" -ne 0 ]; then verify_rc=\$heal_rc; fi','true')"
  mut "report reads a failed readiness query as zero" "s.replace('2>/dev/null) || { echo \"???  \$LINEARIZER_DEPLOY: could not read its readiness (kubectl failed)\"; return 0; }','2>/dev/null) || true')"
  mut "an unreadable readiness is taken as zero" "s.replace('-o jsonpath=\'{.status.readyReplicas}\' 2>/dev/null) || { echo \"  linearizer: could not read its readiness','-o jsonpath=\'{.status.readyReplicas}\' 2>/dev/null) || { true; echo \"  linearizer: could not read its readiness').replace('NOT touching its consumer group\" >&2; return 1; }\n  [ \"\${rr:-0}\"','NOT touching its consumer group\" >&2; }\n  [ \"\${rr:-0}\"')"
  mut "a failed pod listing is taken as no pods" "s.replace('if ! pods=\$(linearizer_pods); then','if false; then')"
  mut "a failed scale-up is swallowed" "s.replace('if ! \$K scale deploy/\"\$LINEARIZER_DEPLOY\" --replicas=\"\$des\" >/dev/null 2>&1; then\n    echo \"  linearizer: group deleted but','if false; then\n    echo \"  linearizer: group deleted but')"
  mut "malformed offset lines are skipped" "s.replace('    { bad = 1 }\\n','    { next }\\n')"
  mut "start never reports the linearizer" "s.replace('  report_linearizer_status\\n  echo \"Done','  echo \"Done')"
fi

echo
if [ $fails -eq 0 ]; then echo "ALL OK"; else echo "$fails FAILED"; exit 1; fi
