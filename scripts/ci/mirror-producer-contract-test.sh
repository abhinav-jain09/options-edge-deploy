#!/usr/bin/env bash
# Every MM1 mirror pipeline must generate the SAME producer contract, and must say what it does and
# does not buy. A silent edit back to acks=1, or dropping the pinned idempotence, is what this test
# exists to catch: the settings live inside a heredoc that no other check reads (the shape tests
# extract only the block after BEGIN SHAPE ASSERTIONS), so nothing else would notice.
#
# It asserts the BYTES of the generated producer.properties, not a comment: for each pipeline, the
# producer heredoc must contain exactly one acks= line reading acks=all, and exactly one
# enable.idempotence= line reading true.
set -euo pipefail
cd "$(dirname "$0")/../.."

MIRRORS=(
  Jenkinsfile.es-cvd-mirror
  Jenkinsfile.es-indicator-mirror
  Jenkinsfile.es-auction-mirror
  Jenkinsfile.es-futures-flow-mirror
  Jenkinsfile.es-strike-intel-mirror
  Jenkinsfile.opra-definition-enumeration-mirror
  Jenkinsfile.es-tape-zones-mirror
)

fail=0
for f in "${MIRRORS[@]}"; do
  [ -r "$f" ] || { echo "FAIL $f: not readable — the mirror set in this test is stale"; fail=1; continue; }
  # the generated file: everything between the producer heredoc opener and its terminator
  # FIRST stanza only, and it really is: capture starts at the opener and the script EXITS at that
  # stanza's terminator, so a second one cannot be concatenated into the block silently (the
  # one-heredoc assertion below is what refuses that file outright).
  block=$(awk '/cat > "\$MDIR\/producer\.properties" <<P$/{inb=1; next} inb && /^P$/{exit} inb' "$f")
  [ -n "$block" ] || { echo "FAIL $f: no producer.properties heredoc found"; fail=1; continue; }

  # A LAST-WINS property file makes "contains the right line" the wrong question: a second
  # enable.idempotence=false below the pinned one would pass that and silently win. Every setting
  # here is therefore counted as a TOTAL for its key, then read.
  opens=$(grep -c 'cat > "\$MDIR/producer\.properties" <<P$' "$f" || true)
  acks=$(printf '%s\n' "$block" | grep -c '^acks=' || true)
  acks_all=$(printf '%s\n' "$block" | grep -c '^acks=all$' || true)
  idem_any=$(printf '%s\n' "$block" | grep -c '^enable\.idempotence=' || true)
  idem_true=$(printf '%s\n' "$block" | grep -c '^enable\.idempotence=true$' || true)
  bootstrap=$(printf '%s\n' "$block" | grep -c '^bootstrap\.servers=' || true)

  [ "$opens" = "1" ] || { echo "FAIL $f: expected exactly one producer.properties heredoc, found $opens — only the first is checked, so a second could carry anything"; fail=1; }
  [ "$acks" = "1" ] && [ "$acks_all" = "1" ] || { echo "FAIL $f: producer must set exactly one acks= line, reading acks=all (found $acks acks lines, $acks_all of them acks=all)"; fail=1; }
  [ "$idem_any" = "1" ] && [ "$idem_true" = "1" ] || { echo "FAIL $f: producer must set exactly one enable.idempotence= line, reading true (found $idem_any lines, $idem_true of them true)"; fail=1; }
  [ "$bootstrap" = "1" ] || { echo "FAIL $f: producer must set exactly one bootstrap.servers (found $bootstrap)"; fail=1; }
done

[ "$fail" = "0" ] || { echo "mirror producer contract: FAILED"; exit 1; }
echo "mirror producer contract: ${#MIRRORS[@]} pipelines publish with acks=all and pinned idempotence"
