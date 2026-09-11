#!/usr/bin/env bash
# es.futures.footprint.strike is exact-partition AND reset-preserved in BOTH topic sets (the SPX dev/prod
# set and TOPIC_SET=es4). apply-topics.sh's exact-partition repair is a delete+recreate, gated only by the
# global KAFKA_RECREATE_MISMATCHED_TOPICS flag and by OPTIONS_EDGE_NEVER_RECREATE_TOPICS. Before the deploy
# Codex final review (finding 4) the strike log was not NEVER_RECREATE, and a partition drift with the flag
# set printed "Repairing EXACT-partition topic es.futures.footprint.strike: partitions=4 -> 1", deleted the
# topic and recreated it empty — while consumer-group offsets survived.
#
# This drives the REAL apply-topics.sh against mocked kafka CLIs (the pattern of
# apply-topics-ledger-safety-test.sh) and asserts, for every environment and both topic sets, that a drifted
# strike topic stops the run BEFORE ANY delete or create call, and says why. It also proves the test can
# fail: with strike removed from NEVER_RECREATE (a mutated copy of topics.env) the same run deletes it.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STRIKE="es.futures.footprint.strike"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

# shellcheck source=/dev/null
source "$HERE/topics.env"

# run <strike-partitions> <recreate-flag> <ENVIRONMENT> <TOPIC_SET> [<dir holding apply-topics.sh + topics.env>]
# The mocked broker answers every OTHER topic at the count declared in the set being applied (the two
# sets legitimately disagree, e.g. es.options.databento.gex.spxbridge is 4 in one and 32 in the other),
# so the only drift on the broker is the one each case injects.
run() {
  local parts="$1" recreate="$2" env_name="$3" topic_set="$4" src="${5:-$HERE}" tmp DECLARED
  if [ "$topic_set" = es4 ]; then DECLARED="$OPTIONS_EDGE_ES4_TOPICS"
  else DECLARED="$OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"; fi
  tmp="$(mktemp -d "$WORK/run.XXXX")"; LOG="$tmp/calls"; : > "$LOG"
  cat > "$tmp/kafka-topics" <<EOF
#!/usr/bin/env bash
echo "kafka-topics \$*" >> "$LOG"
name=""; prev=""
for a in "\$@"; do [ "\$prev" = "--topic" ] && name="\$a"; prev="\$a"; done
if [[ "\$*" == *--describe* ]]; then
  if [ "\$name" = "$STRIKE" ]; then
    echo "Topic: \$name TopicId: ID PartitionCount: $parts ReplicationFactor: 1"
  else
    p=\$(printf '%s' "$DECLARED" | tr ' ' '\\n' | awk -F: -v n="\$name" '\$1 == n {print \$2; exit}')
    echo "Topic: \$name TopicId: ID PartitionCount: \${p:-1} ReplicationFactor: 1"
  fi
fi
exit 0
EOF
  cat > "$tmp/kafka-configs" <<EOF
#!/usr/bin/env bash
echo "kafka-configs \$*" >> "$LOG"
exit 0
EOF
  cat > "$tmp/kafka-broker-api-versions" <<'EOF'
#!/usr/bin/env bash
echo "localhost:9092 (id: 1 rack: null) -> ("
exit 0
EOF
  cat > "$tmp/kafka-reassign-partitions" <<EOF
#!/usr/bin/env bash
echo "kafka-reassign-partitions \$*" >> "$LOG"
exit 0
EOF
  chmod +x "$tmp"/kafka-*
  PATH="$tmp:$PATH" KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_TOPIC_REPLICATION_FACTOR=1 ENVIRONMENT="$env_name" TOPIC_SET="$topic_set" \
    KAFKA_RECREATE_MISMATCHED_TOPICS="$recreate" KAFKA_TOPIC_DELETE_WAIT_SECONDS=1 \
    KAFKA_TOPIC_REPAIR_WAIT_SECONDS=1 \
    bash "$src/apply-topics.sh" > "$tmp/out" 2>&1
  RC=$?; OUT="$tmp/out"
}
n_calls() { grep -c -- "$1" "$LOG" 2>/dev/null || true; }

echo "1. strike is declared where this test assumes (otherwise every case below passes vacuously)"
for set_name in OPTIONS_EDGE_TOPICS OPTIONS_EDGE_ES4_TOPICS; do
  printf '%s\n' ${!set_name} | grep -qx "$STRIKE:1" && ok "$STRIKE:1 is in $set_name" || bad "$STRIKE:1 missing from $set_name"
done
for set_name in OPTIONS_EDGE_EXACT_PARTITION_TOPICS OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS OPTIONS_EDGE_NEVER_RECREATE_TOPICS; do
  printf '%s\n' ${!set_name:-} | grep -qx "$STRIKE" && ok "$STRIKE is in $set_name" || bad "$STRIKE missing from $set_name"
done
[ "$(grep -c '^OPTIONS_EDGE_NEVER_RECREATE_TOPICS=' "$HERE/topics.env")" = 1 ] \
  && ok "there is exactly ONE NEVER_RECREATE assignment (so it cannot be re-pointed per set)" \
  || bad "NEVER_RECREATE is assigned more than once in topics.env"

echo "2. at the declared shape nothing destructive happens, and the strike contract is still reconciled"
for combo in "production:" "dev:" "production:es4"; do
  env_name="${combo%%:*}"; topic_set="${combo#*:}"
  run 1 true "$env_name" "$topic_set"
  label="ENVIRONMENT=$env_name TOPIC_SET=${topic_set:-<default>}"
  [ "$RC" -eq 0 ] && ok "$label: apply-topics exits 0" || { bad "$label: apply-topics exited $RC"; sed 's/^/      | /' "$OUT" | tail -5; }
  [ "$(n_calls '--delete')" = 0 ] && ok "$label: no delete issued" || bad "$label: a delete was issued"
  grep -q -- "--entity-name $STRIKE --alter --add-config retention.ms=-1,cleanup.policy=delete,.*retention.bytes=-1" "$LOG" \
    && ok "$label: strike retention.ms=-1 / delete / retention.bytes=-1 reconciled" \
    || bad "$label: the strike retention contract was not reconciled"
done

echo "3. partition DRIFT with KAFKA_RECREATE_MISMATCHED_TOPICS=true is a HARD ERROR before any delete/create"
for combo in "production:" "dev:" "production:es4"; do
  env_name="${combo%%:*}"; topic_set="${combo#*:}"
  run 4 true "$env_name" "$topic_set"
  label="ENVIRONMENT=$env_name TOPIC_SET=${topic_set:-<default>}"
  [ "$RC" -ne 0 ] && ok "$label: refused (exit $RC)" || bad "$label: exited 0 with the strike log at 4 partitions"
  [ "$(n_calls '--delete')" = 0 ] && ok "$label: NO delete call of any topic" || bad "$label: $(n_calls '--delete') delete call(s)"
  [ "$(n_calls '--create')" = 0 ] && ok "$label: NO create call of any topic" || bad "$label: $(n_calls '--create') create call(s)"
  grep -q "HARD ERROR: topic $STRIKE" "$OUT" && grep -q "OPTIONS_EDGE_NEVER_RECREATE_TOPICS" "$OUT" \
    && ok "$label: and it names the strike topic and the never-recreate declaration" \
    || bad "$label: the refusal did not say why: $(head -3 "$OUT")"
done

echo "4. the test can fail: WITHOUT the declaration the same drift deletes and recreates the log"
MUT="$WORK/mutant"; mkdir -p "$MUT"
cp "$HERE/apply-topics.sh" "$MUT/"
# Position-independent: the strike token is removed wherever it sits in the list. The first form was anchored on
# the closing quote, so it only matched while strike was the LAST entry — it stopped matching the day the
# vol-premium ledgers were appended after it, and the guard below is what caught that.
sed "s/^\(OPTIONS_EDGE_NEVER_RECREATE_TOPICS=\".*\) $STRIKE\(.*\"\)$/\1\2/" "$HERE/topics.env" > "$MUT/topics.env"
if grep -q "^OPTIONS_EDGE_NEVER_RECREATE_TOPICS=.*$STRIKE" "$MUT/topics.env"; then
  bad "the mutation did not remove $STRIKE from NEVER_RECREATE — case 4 would prove nothing"
else
  for topic_set in "" es4; do
    run 4 true production "$topic_set" "$MUT"
    grep -q -- "--delete --topic $STRIKE" "$LOG" \
      && ok "TOPIC_SET=${topic_set:-<default>}: the mutant DELETES the strike log (the defect this file pins)" \
      || bad "TOPIC_SET=${topic_set:-<default>}: the mutant did not delete — the test is not sensitive to the declaration"
  done
fi

echo
if [ "$fails" -eq 0 ]; then echo "=== apply-topics-strike-safety: OK ==="; exit 0; fi
echo "=== apply-topics-strike-safety: $fails problem(s) ===" >&2; exit 1
