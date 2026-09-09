#!/usr/bin/env bash
# The opra-definition-enumeration-mirror job's SOURCE/TARGET shape assertions, tested against mocked
# kafka CLIs. Sibling of es-auction-mirror-shape-test.sh: the two jobs share the assertion block by
# descent, so each keeps its own extraction rather than one testing the other's copy.
#
# WHY THIS EXISTS. `kafka-topics --create --if-not-exists` binds NOTHING on a topic that already
# exists, and the job's reconcile step (`kafka-configs --alter`) can silently not take, so the
# assertions after them are the only thing standing between a drifted target and a mirror that
# happily runs against it. The same class of reader has drifted before (es-cvd-mirror installs
# #1/#2: delete.retention.ms substring-matched before retention.ms; a comma in the terminator
# class truncated "compact,delete" to "compact"), so the reader is exercised here against the
# real CLI's line layout, not trusted.
#
# THE ASSERTIONS ARE EXTRACTED FROM THE JOB, NOT COPIED. A copy would pass forever after the job
# changed. The extraction is delimited by the BEGIN/END SHAPE ASSERTIONS markers in the
# Jenkinsfile, and a missing marker is a hard failure here. Groovy's triple-single-quoted strings
# process backslash escapes, so `\\n` in the file is `\n` by the time bash sees it -- the
# extraction reproduces that.
set -euo pipefail
cd "$(dirname "$0")/../.."

JOB="Jenkinsfile.opra-definition-enumeration-mirror"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
fail=0

body="$(awk '/BEGIN SHAPE ASSERTIONS/{f=1;next} /END SHAPE ASSERTIONS/{f=0} f' "$JOB")"
[ -n "$body" ] || { echo "FAIL: shape-assertion markers not found in $JOB -- the extraction and the job have diverged" >&2; exit 1; }
# Sanity: the extracted text must actually contain the assertions this file claims to test.
for needle in 'eff()' 'source $TOPIC cleanup.policy' 'source $TOPIC retention.ms' \
              'target $TOPIC cleanup.policy' 'target $TOPIC retention.ms' 'PartitionCount' \
              '--alter' 'wrong partition count'; do
  printf '%s' "$body" | grep -qF -- "$needle" \
    || { echo "FAIL: extracted block is missing '$needle' -- parser drift, refusing to test vacuously" >&2; exit 1; }
done
# Groovy unescaping (see header), then run under bash with the job's variables provided.
printf '%s\n' "$body" | sed 's/\\\\/\\/g' > "$WORK/assertions.sh"
bash -n "$WORK/assertions.sh" || { echo "FAIL: extracted block does not parse as bash" >&2; exit 1; }

# --- mocked kafka CLIs. Each scenario declares the SOURCE and TARGET shapes as env knobs. --------
# The mocks also RECORD every invocation, so a scenario can assert what was (not) touched: a
# refused target must be refused UNTOUCHED, i.e. no --alter may reach it.
mkdir -p "$WORK/bin"
cat > "$WORK/bin/kafka-topics" <<'MOCK'
#!/usr/bin/env bash
echo "kafka-topics $*" >> "$CALLS"
broker=""; prev=""
for a in "$@"; do [ "$prev" = "--bootstrap-server" ] && broker="$a"; prev="$a"; done
if [[ "$*" == *--describe* ]]; then
  if [ "$broker" = "${SRC_BROKER:-SRC}" ]; then parts="$SRC_PARTS"; else parts="$TGT_PARTS"; fi
  echo "Topic: $MOCK_TOPIC	TopicId: ID	PartitionCount: $parts	ReplicationFactor: 1	Configs: cleanup.policy=compact,delete"
fi
exit 0
MOCK
cat > "$WORK/bin/kafka-configs" <<'MOCK'
#!/usr/bin/env bash
echo "kafka-configs $*" >> "$CALLS"
broker=""; prev=""
for a in "$@"; do [ "$prev" = "--bootstrap-server" ] && broker="$a"; prev="$a"; done
[[ "$*" == *--alter* ]] && exit "${ALTER_RC:-0}"
if [ "$broker" = "${SRC_BROKER:-SRC}" ]; then pol="$SRC_POLICY"; ret="$SRC_RET"; else pol="$TGT_POLICY"; ret="$TGT_RET"; fi
# Real kafka-configs emits delete.retention.ms BEFORE retention.ms and one config per line with a
# synonyms blob -- the layout that broke the reader once already, reproduced so the boundary
# anchoring stays under test.
echo "Dynamic configs for topic $MOCK_TOPIC are:"
echo "  delete.retention.ms=86400000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:delete.retention.ms=86400000}"
echo "  cleanup.policy=$pol sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=$pol, DEFAULT_CONFIG:log.cleanup.policy=delete}"
echo "  retention.ms=$ret sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=$ret}"
exit 0
MOCK
chmod +x "$WORK/bin/kafka-topics" "$WORK/bin/kafka-configs"

run() { # -> exit status; output in $OUT, CLI call log in $CALLS
  OUT="$WORK/out.txt"; CALLS="$WORK/calls.txt"; : > "$CALLS"
  set +e
  env PATH="$WORK/bin:$PATH" KBIN="$WORK/bin" SRC="${SRC_BROKER:-SRC}" TGT="${TGT_BROKER:-127.0.0.1:19092}" \
      TOPIC="$MOCK_TOPIC" PARTS="$PARTS" POLICY="$POLICY" RET="$RET" CALLS="$CALLS" ALTER_RC="${ALTER_RC:-0}" \
      MOCK_TOPIC="$MOCK_TOPIC" SRC_BROKER="${SRC_BROKER:-SRC}" \
      SRC_PARTS="$SRC_PARTS" SRC_POLICY="$SRC_POLICY" SRC_RET="$SRC_RET" \
      TGT_PARTS="$TGT_PARTS" TGT_POLICY="$TGT_POLICY" TGT_RET="$TGT_RET" \
      bash "$WORK/assertions.sh" >"$OUT" 2>&1
  local st=$?
  set -e
  return $st
}

check() { # description | want-status | expected-substring-when-failing
  local desc="$1" want="$2" why="${3:-}" got=0
  run || got=$?
  if [ "$got" != "$want" ]; then
    printf '  FAIL %-56s exit %s, want %s\n' "$desc" "$got" "$want"; sed 's/^/         /' "$OUT"; fail=1; return
  fi
  if [ -n "$why" ] && ! grep -qF "$why" "$OUT"; then
    printf '  FAIL %-56s exit %s but never said %q\n' "$desc" "$got" "$why"; sed 's/^/         /' "$OUT"; fail=1; return
  fi
  printf '  ok   %-56s exit %s\n' "$desc" "$got"
}
touched_target() { grep -q -- "--alter" "$CALLS"; }

# The one frozen arm of the job (keep in sync with the Jenkinsfile case arm and topics.env).
good() {
  MOCK_TOPIC=options.databento.opra.definition.enumeration PARTS=1 POLICY=delete RET=604800000
  SRC_PARTS=1 SRC_POLICY=delete SRC_RET=604800000
  TGT_PARTS=1 TGT_POLICY=delete TGT_RET=604800000
  ALTER_RC=0
}

# -- both targets are legitimate for this topic: dev is the default, prod is the other one -------
good; TGT_BROKER=127.0.0.1:19092
check "matching source+target on the DEV target passes" 0 "target options.databento.opra.definition.enumeration verified: 1 partition(s), delete, retention.ms=604800000"
good; TGT_BROKER=192.168.100.252:9092
check "matching source+target on the PROD target passes" 0 "target options.databento.opra.definition.enumeration verified"
TGT_BROKER=127.0.0.1:19092

# -- SOURCE drift fails BEFORE the target is touched ---------------------------------------------
good; SRC_POLICY=compact,delete
check "SOURCE at compact,delete FAILS" 1 "source options.databento.opra.definition.enumeration cleanup.policy=compact,delete, frozen contract says delete"
touched_target && { echo "  FAIL source-policy drift still reached the target --alter"; fail=1; } \
  || printf '  ok   %-56s\n' "source-policy drift leaves the target untouched"
good; SRC_POLICY=compact
check "SOURCE at pure compact FAILS" 1 "source options.databento.opra.definition.enumeration cleanup.policy=compact, frozen contract says delete"
good; SRC_RET=-1
check "SOURCE retention drift (unlimited) FAILS" 1 "source options.databento.opra.definition.enumeration retention.ms=-1, frozen contract says 604800000"
good; SRC_RET=43200000
check "SOURCE retention drift (12h es4 default) FAILS" 1 "source options.databento.opra.definition.enumeration retention.ms=43200000"
good; SRC_PARTS=4
check "SOURCE partition drift FAILS" 1 "source options.databento.opra.definition.enumeration partition count does not match frozen contract PARTS=1"
touched_target && { echo "  FAIL source-partition drift still reached the target --alter"; fail=1; } \
  || printf '  ok   %-56s\n' "source-partition drift leaves the target untouched"

# -- TARGET: partitions are immutable and asserted BEFORE the config reconcile -------------------
good; TGT_PARTS=32
check "PRE-EXISTING target at the dev 32-partition default FAILS" 1 "target options.databento.opra.definition.enumeration exists with wrong partition count (want 1)"
touched_target && { echo "  FAIL wrong-partition target was still reconciled (--alter reached it)"; fail=1; } \
  || printf '  ok   %-56s\n' "wrong-partition target is refused UNTOUCHED"

# -- TARGET: the reconcile must be attempted, and must be verified to have taken ------------------
good; TGT_POLICY=compact,delete
check "target still compact,delete AFTER reconcile FAILS" 1 "target options.databento.opra.definition.enumeration cleanup.policy=compact,delete, frozen contract says delete"
touched_target || { echo "  FAIL a policy-drifted target was never reconciled (no --alter)"; fail=1; }
good; TGT_POLICY=compact
check "target at pure compact FAILS" 1 "target options.databento.opra.definition.enumeration cleanup.policy=compact, frozen contract says delete"
good; TGT_RET=-1
check "target retention drift (unlimited) FAILS" 1 "target options.databento.opra.definition.enumeration retention.ms=-1, frozen contract says 604800000"
good; TGT_RET=86400000
check "target retention drift (24h env default) FAILS" 1 "target options.databento.opra.definition.enumeration retention.ms=86400000"
good; ALTER_RC=1
check "reconcile itself failing FAILS the install" 1 "cannot reconcile options.databento.opra.definition.enumeration config"

# -- the alter must bracket-wrap the comma-carrying policy or kafka-configs rejects it ------------
good; run || true
grep -qF -- '--add-config cleanup.policy=delete,retention.ms=604800000' "$CALLS" \
  && printf '  ok   %-56s\n' "reconcile passes cleanup.policy bracket-wrapped" \
  || { echo "  FAIL reconcile did not bracket-wrap the comma policy:"; grep -- '--alter' "$CALLS" | sed 's/^/         /'; fail=1; }

[ $fail -eq 0 ] && echo "=== opra-definition-enumeration-mirror-shape-test: OK ===" || { echo "=== opra-definition-enumeration-mirror-shape-test: FAILED ==="; exit 1; }
