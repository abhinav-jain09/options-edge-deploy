#!/usr/bin/env bash
# assert-no-legacy-writer.sh <target bootstrap> <target topic> <confluent bin>
# Run BEFORE the new mirror is installed. A byte copier that is still alive writes source-registry ids into the target, and an edited script does not
# stop a process that is already running. Two independent proofs:
#   1. PROCESS: no running process (ps, this host is the MirrorMaker/bridge host) carries a MirrorMaker --whitelist naming underlying.spx.price, runs the
#      spx-es4-bridge, or runs a console producer that writes THIS target topic on THIS target broker (both targets);
#   2. BEHAVIOUR: the target topic's end offset does not advance over WAIT_S (default 30) with no mirror of ours installed. A topic fed by a
#      legitimate low-rate writer is not the case here: the only writers of this topic are the feed (source side) and the mirror.
# PS_CMD overrides `ps -axo command` for the tests.
set -euo pipefail
BS="${1:?}"; TOPIC="${2:?}"; KBIN="${3:?}"
WAIT="${WAIT_S:-30}"
procs=$(${PS_CMD:-ps -axo command}) || { echo "FAIL: cannot list processes"; exit 1; }
legacy=$(printf '%s\n' "$procs" | grep -E "MirrorMaker.*underlying(\\\\[.]|\[[.]\]|[.])spx(\\\\[.]|\[[.]\]|[.])price(\$|[^-.A-Za-z\\\\[])|spx-es4-bridge" || true)
# a console producer writing THIS target topic on THIS target broker (es4: es.underlying.spx.price on .4; mac74: underlying.spx.price on .74)
writers=$(printf '%s\n' "$procs" | grep -E "kafka-console-producer|kafka[.]tools[.]ConsoleProducer" | grep -F -- "$TOPIC" | grep -F -- "${BS%:*}" || true)
hit=$(printf '%s\n%s\n' "$legacy" "$writers" | grep -v '^$' | grep -v "assert-no-legacy-writer" || true)
if [ -n "$hit" ]; then
  echo "FAIL: a legacy byte copier of underlying.spx.price is RUNNING:"; printf '%s\n' "$hit" | cut -c1-200 | sed 's/^/      /'
  echo "      Stop it (edit its whitelist AND restart the unit; an edited script does not stop a running process), then re-run."
  exit 1
fi
end() { "${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$BS" --topic "$TOPIC" 2>/dev/null | awk -F: -v t="$TOPIC" '$1 == t && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ { s += $3; n++ } END { if (n == 0) exit 3; print s }'; }
e0=$(end) || { echo "FAIL: cannot read end offsets of $TOPIC on $BS"; exit 1; }
sleep "$WAIT"
e1=$(end) || { echo "FAIL: cannot read end offsets of $TOPIC on $BS"; exit 1; }
[ "$e1" -le "$e0" ] || { echo "FAIL: $TOPIC on $BS advanced $e0 -> $e1 in ${WAIT}s with no mirror of ours installed: a legacy writer is live"; exit 1; }
echo "  no legacy writer: no such process, and $TOPIC end offset steady at $e1 over ${WAIT}s"
