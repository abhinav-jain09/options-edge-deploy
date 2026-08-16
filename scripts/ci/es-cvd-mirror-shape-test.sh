#!/usr/bin/env bash
# The es-cvd-mirror job's SOURCE/TARGET shape assertions, tested against mocked kafka CLIs.
#
# WHY THIS EXISTS. `kafka-topics --create --if-not-exists` binds NOTHING on a topic that already
# exists, so these assertions are the only thing standing between a drifted target and a mirror
# that happily runs against it. The assertions themselves have drifted twice: the effective-config
# reader once substring-matched delete.retention.ms before retention.ms, and the target's policy
# check was a per-policy `case` of grep SUBSTRINGS — which could not express U16's pure-compact
# contract at all. A pre-existing es.futures.cvd.levels at cleanup.policy=compact,delete satisfied
# the "grep compact" arm while its delete half ages out the latest attestation, the one record the
# producer reads back at startup (AA1).
#
# THE ASSERTIONS ARE EXTRACTED FROM THE JOB, NOT COPIED. A copy would pass forever after the job
# changed. The extraction is delimited by the BEGIN/END markers in the Jenkinsfile, and a missing
# marker is a hard failure here. Groovy's triple-single-quoted strings process backslash escapes,
# so `\\n` in the file is `\n` by the time bash sees it — the extraction reproduces that.
set -euo pipefail
cd "$(dirname "$0")/../.."

JOB="Jenkinsfile.es-cvd-mirror"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
fail=0

body="$(awk '/BEGIN SHAPE ASSERTIONS/{f=1;next} /END SHAPE ASSERTIONS/{f=0} f' "$JOB")"
[ -n "$body" ] || { echo "FAIL: shape-assertion markers not found in $JOB — the extraction and the job have diverged" >&2; exit 1; }
# Sanity: the extracted text must actually contain the assertions this file claims to test.
for needle in 'eff()' 'source $TOPIC cleanup.policy' 'target $TOPIC cleanup.policy' 'PartitionCount'; do
  printf '%s' "$body" | grep -qF "$needle" \
    || { echo "FAIL: extracted block is missing '$needle' — parser drift, refusing to test vacuously" >&2; exit 1; }
done
# Groovy unescaping (see header), then run under bash with the job's variables provided.
printf '%s\n' "$body" | sed 's/\\\\/\\/g' > "$WORK/assertions.sh"

# --- mocked kafka CLIs. Each scenario declares the SOURCE and TARGET shapes as env knobs. --------
mkdir -p "$WORK/bin"
cat > "$WORK/bin/kafka-topics" <<'EOF'
#!/usr/bin/env bash
broker=""; prev=""
for a in "$@"; do [ "$prev" = "--bootstrap-server" ] && broker="$a"; prev="$a"; done
if [[ "$*" == *--describe* ]]; then
  if [ "$broker" = "SRC" ]; then parts="$SRC_PARTS"; else parts="$TGT_PARTS"; fi
  echo "Topic: $MOCK_TOPIC TopicId: ID PartitionCount: $parts ReplicationFactor: 1"
fi
exit 0
EOF
cat > "$WORK/bin/kafka-configs" <<'EOF'
#!/usr/bin/env bash
broker=""; prev=""
for a in "$@"; do [ "$prev" = "--bootstrap-server" ] && broker="$a"; prev="$a"; done
if [ "$broker" = "SRC" ]; then pol="$SRC_POLICY"; ret="$SRC_RET"; else pol="$TGT_POLICY"; ret="$TGT_RET"; fi
# Real kafka-configs emits delete.retention.ms BEFORE retention.ms — the ordering that broke the
# reader once already, reproduced here so the boundary anchoring stays under test.
echo "Dynamic configs for topic $MOCK_TOPIC are: delete.retention.ms=86400000 sensitive=false, cleanup.policy=$pol sensitive=false, retention.ms=$ret sensitive=false"
exit 0
EOF
chmod +x "$WORK/bin/kafka-topics" "$WORK/bin/kafka-configs"

run() { # -> exit status; output in $OUT
  OUT="$WORK/out.txt"
  set +e
  env PATH="$WORK/bin:$PATH" KBIN="$WORK/bin" SRC=SRC TGT=TGT \
      TOPIC="$MOCK_TOPIC" PARTS="$PARTS" POLICY="$POLICY" RET="$RET" \
      MOCK_TOPIC="$MOCK_TOPIC" \
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
    printf '  FAIL %-52s exit %s, want %s\n' "$desc" "$got" "$want"; sed 's/^/         /' "$OUT"; fail=1; return
  fi
  if [ -n "$why" ] && ! grep -qF "$why" "$OUT"; then
    printf '  FAIL %-52s exit %s but never said %q\n' "$desc" "$got" "$why"; sed 's/^/         /' "$OUT"; fail=1; return
  fi
  printf '  ok   %-52s exit %s\n' "$desc" "$got"
}

# ── the U16 pure-compact target: the case the old substring `case` could not express ────────────
MOCK_TOPIC=es.futures.cvd.levels PARTS=1 POLICY=compact RET=-1
SRC_PARTS=1 SRC_POLICY=compact SRC_RET=-1 TGT_PARTS=1 TGT_POLICY=compact TGT_RET=-1
check "levels: matching pure-compact target passes" 0

TGT_POLICY=compact,delete
check "levels: PRE-EXISTING target at compact,delete FAILS" 1 "target es.futures.cvd.levels cleanup.policy=compact,delete"

TGT_POLICY=delete
check "levels: pre-existing target at delete FAILS" 1 "cleanup.policy=delete, frozen contract says compact"

TGT_POLICY=compact TGT_RET=43200000
check "levels: target retention drift FAILS (AA1)" 1 "retention.ms=43200000"

TGT_RET=-1 TGT_PARTS=2
check "levels: target partition drift FAILS" 1 "wrong partition count"

TGT_PARTS=1 SRC_POLICY=compact,delete
check "levels: SOURCE at compact,delete FAILS" 1 "source es.futures.cvd.levels cleanup.policy=compact,delete"

# ── the compact,delete siblings must still pass, and must not accept pure compact ───────────────
MOCK_TOPIC=es.futures.cvd.bars PARTS=1 POLICY=compact,delete RET=-1
SRC_PARTS=1 SRC_POLICY=compact,delete SRC_RET=-1 TGT_PARTS=1 TGT_POLICY=compact,delete TGT_RET=-1
check "bars: matching compact,delete target passes" 0

TGT_POLICY=compact
check "bars: target downgraded to pure compact FAILS" 1 "target es.futures.cvd.bars cleanup.policy=compact,"

TGT_POLICY=compact,delete
MOCK_TOPIC=es.futures.cvd PARTS=1 POLICY=compact,delete RET=43200000
SRC_PARTS=1 SRC_POLICY=compact,delete SRC_RET=43200000 TGT_PARTS=1 TGT_POLICY=compact,delete TGT_RET=43200000
check "cvd snapshot: matching 12h target passes" 0

SRC_RET=-1
check "cvd snapshot: SOURCE retention drift FAILS" 1 "source es.futures.cvd retention.ms=-1"

[ $fail -eq 0 ] && echo "=== es-cvd-mirror-shape-test: OK ===" || { echo "=== es-cvd-mirror-shape-test: FAILED ==="; exit 1; }
