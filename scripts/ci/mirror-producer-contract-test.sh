#!/usr/bin/env bash
# Every MM1 mirror pipeline must generate the SAME producer contract, and must say what it does and
# does not buy. A silent edit back to acks=1, or dropping the pinned idempotence, is what this test
# exists to catch: the settings live inside a heredoc that no other check reads (the shape tests
# extract only the block after BEGIN SHAPE ASSERTIONS), so nothing else would notice.
#
# It asserts the BYTES of the generated producer.properties, not a comment: the seven stanzas must be
# byte-identical to one another, that one stanza must contain exactly one acks= line reading acks=all
# and exactly one enable.idempotence= line reading true, and nothing else in the pipeline may write
# the file after the heredoc.
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

# WRITE-CLOSURE, by inversion, and by ANCHORED patterns. Enumerating writers (>, >>, sed -i, tee, cp,
# mv, install, dd of=, a python/perl one-liner, ...) is a losing game — review found four forms one
# regex missed, then a fifth that rode along on a substring match. So: every line that NAMES
# producer.properties must match one of the three WHOLE-LINE forms below, and anything else fails
# whatever command it uses, because a writer has to name its target. Each pattern is anchored ^...$
# and the ship-out form admits no shell metacharacter, so a second command cannot ride on an allowed
# line (`printf acks=1 > "$MDIR/producer.properties"; : --producer.config` is rejected).
# The limit, stated rather than papered over: a write that never names the file (a glob, a
# `for f in "$MDIR"/*`) is invisible to a text check. This test does not claim to catch that.
OPENER='^[[:space:]]*cat > "\$MDIR/producer\.properties" <<P$'
# MM1 reading the config it was handed. The path is a LITERAL grammar, not a character class: a class
# permissive enough to hold "$MDIR/producer.properties" also held `x>/mirror/producer.properties`,
# which the shell reads as a redirection that WRITES the file. Only the two paths these pipelines
# actually pass are accepted; a new read form has to be added here deliberately.
READ='^[[:space:]]*--producer\.config[[:space:]]+("\$MDIR/producer\.properties"|/mirror/producer\.properties)[[:space:]]*(\\)*$'
# the opra runner shipping the unit's files to es4: one scp/rsync, no metacharacters, and the path
# must be a SOURCE — the destination is the last argument, so the path must not be there
SHIP='^[[:space:]]*(scp|rsync)([[:space:]]+[^;|&<>`$(){}]*(\$[A-Za-z_][A-Za-z0-9_]*|\$\{[A-Za-z_][A-Za-z0-9_]*\}|[^;|&<>`$(){}])*)+[[:space:]]*(\\)*$'

fail=0
ref_hash=''
ref_file=''
for f in "${MIRRORS[@]}"; do
  [ -r "$f" ] || { echo "FAIL $f: not readable — the mirror set in this test is stale"; fail=1; continue; }
  # the generated file: everything between the producer heredoc opener and its terminator
  # FIRST stanza only, and it really is: capture starts at the opener and the script EXITS at that
  # stanza's terminator, so a second one cannot be concatenated into the block silently (the
  # one-heredoc assertion below is what refuses that file outright).
  block=$(awk '/cat > "\$MDIR\/producer\.properties" <<P$/{inb=1; next} inb && /^P$/{exit} inb' "$f")
  [ -n "$block" ] || { echo "FAIL $f: no producer.properties heredoc found"; fail=1; continue; }

  opens=$(grep -cE "$OPENER" "$f" || true)
  [ "$opens" = "1" ] || { echo "FAIL $f: expected exactly one producer.properties heredoc, found $opens — only the first is checked, so a second could carry anything"; fail=1; }

  while IFS= read -r line; do
    if printf '%s\n' "$line" | grep -qE "$OPENER"; then continue; fi
    if printf '%s\n' "$line" | grep -qE "$READ"; then continue; fi
    if printf '%s\n' "$line" | grep -qE "$SHIP"; then
      # The last argument, with the line-continuation backslash and trailing blanks removed. Done
      # with sed anchored at the END: a ${line%%...} glob matched from the leading indentation and
      # left `last` EMPTY, so an scp whose destination was producer.properties passed.
      last=$(printf '%s' "$line" | sed -E 's/[[:space:]]*\\*[[:space:]]*$//; s/.*[[:space:]]//')
      case "$last" in
        *producer.properties*) ;;   # the path is the DESTINATION: that is a write
        *) continue ;;
      esac
    fi
    echo "FAIL $f: only the heredoc may write producer.properties — this line names it another way: ${line#"${line%%[![:space:]]*}"}"
    fail=1
  done < <(grep 'producer\.properties' "$f")

  # POSITIVE BINDING: the stanza's bytes are worth nothing if nothing RUNS them. Everything above is
  # closure over writes; this requires the use. Each generated run-mirror*.sh must invoke
  # kafka-mirror-maker and pass this file to it EXACTLY once, and the file must carry exactly as many
  # such scripts as the pipeline really has (two for opra: the es4-docker runner reads the container
  # path /mirror/producer.properties, the host runner reads "$MDIR/producer.properties"). Deleting the
  # --producer.config line, or pointing it at another file, left every other assertion here green.
  case "$f" in
    Jenkinsfile.opra-definition-enumeration-mirror) want_runners=2 ;;
    *) want_runners=1 ;;
  esac
  runners=0
  while read -r s_line; do
    [ -n "$s_line" ] || continue
    e_line=$(awk -v s="$s_line" 'NR>s && /^S$/{print NR; exit}' "$f")
    if [ -z "$e_line" ]; then echo "FAIL $f: run-mirror script heredoc opened at line $s_line is never terminated"; fail=1; continue; fi
    runners=$((runners+1))
    blk=$(sed -n "$((s_line+1)),$((e_line-1))p" "$f")
    printf '%s\n' "$blk" | grep -q 'kafka-mirror-maker' || { echo "FAIL $f: the run-mirror script at line $s_line does not invoke kafka-mirror-maker — nothing would read producer.properties"; fail=1; }
    pc=$(printf '%s\n' "$blk" | grep -cE "$READ" || true)
    [ "$pc" = "1" ] || { echo "FAIL $f: the run-mirror script at line $s_line must pass --producer.config exactly once (found $pc)"; fail=1; }
  done < <(grep -nE '^[[:space:]]*cat > "\$MDIR/run-mirror[^"]*\.sh" <<S$' "$f" | cut -d: -f1)
  [ "$runners" = "$want_runners" ] || { echo "FAIL $f: expected $want_runners generated run-mirror script(s), found $runners"; fail=1; }
  reads=$(grep -cE "$READ" "$f" || true)
  [ "$reads" = "$want_runners" ] || { echo "FAIL $f: expected $want_runners --producer.config read(s), found $reads — a read outside a generated runner proves nothing"; fail=1; }

  # Byte-identical across the seven: the pipelines are copies of one another, and a setting that
  # drifts in ONE of them (a client.id here, a linger.ms there) is how they stop being one contract.
  h=$(printf '%s\n' "$block" | shasum -a 256 | cut -d' ' -f1)
  if [ -z "$ref_hash" ]; then ref_hash=$h; ref_file=$f
  elif [ "$h" != "$ref_hash" ]; then
    echo "FAIL $f: producer stanza differs from $ref_file — the seven must be byte-identical"
    diff <(awk '/cat > "\$MDIR\/producer\.properties" <<P$/{inb=1; next} inb && /^P$/{exit} inb' "$ref_file") <(printf '%s\n' "$block") || true
    fail=1
  fi

  # A LAST-WINS property file makes "contains the right line" the wrong question: a second
  # enable.idempotence=false below the pinned one would pass that and silently win. Every setting
  # here is therefore counted as a TOTAL for its key, then read.
  acks=$(printf '%s\n' "$block" | grep -c '^acks=' || true)
  acks_all=$(printf '%s\n' "$block" | grep -c '^acks=all$' || true)
  idem_any=$(printf '%s\n' "$block" | grep -c '^enable\.idempotence=' || true)
  idem_true=$(printf '%s\n' "$block" | grep -c '^enable\.idempotence=true$' || true)
  bootstrap=$(printf '%s\n' "$block" | grep -c '^bootstrap\.servers=' || true)

  [ "$acks" = "1" ] && [ "$acks_all" = "1" ] || { echo "FAIL $f: producer must set exactly one acks= line, reading acks=all (found $acks acks lines, $acks_all of them acks=all)"; fail=1; }
  [ "$idem_any" = "1" ] && [ "$idem_true" = "1" ] || { echo "FAIL $f: producer must set exactly one enable.idempotence= line, reading true (found $idem_any lines, $idem_true of them true)"; fail=1; }
  [ "$bootstrap" = "1" ] || { echo "FAIL $f: producer must set exactly one bootstrap.servers (found $bootstrap)"; fail=1; }
done

[ "$fail" = "0" ] || { echo "mirror producer contract: FAILED"; exit 1; }
echo "mirror producer contract: ${#MIRRORS[@]} pipelines publish one byte-identical stanza with acks=all and pinned idempotence"
