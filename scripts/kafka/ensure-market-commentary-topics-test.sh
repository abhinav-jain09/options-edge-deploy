#!/usr/bin/env bash
# Drives the REAL ensure-market-commentary-topics.sh against mocked kafka-topics / kafka-configs that print the
# broker's `--describe --all` format. Cases: absent -> created and verified; right shape -> ok; wrong partition count,
# wrong cleanup policy, retention one millisecond short or -1, a CLI failure -> fail closed.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
MIN=options.spx.market-commentary.minute
CUR=options.spx.market-commentary.current
EVT=options.spx.market-commentary.events
REJ=options.spx.market-commentary.rejections
ALL="$MIN $CUR $EVT $REJ"

# cfg <policy> <minute-style retention> : the first key=value per line is the effective value.
cfg() {
  printf 'All configs for topic x are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s}\n  max.message.bytes=1048588 sensitive=false synonyms={}\n  min.insync.replicas=1 sensitive=false synonyms={}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s}\n' "$1" "$1" "$2" "$2"
}

# mock <topics listed> <partitions> <policy> <retention-for-minute-events-rejections> [fail: list|create|describe|configs]
mock() {
  rm -rf "$T/bin"; mkdir -p "$T/bin"; : > "$T/created"
  cfg "$3" "$4" > "$T/cfg_long"; cfg "$3" 172800000 > "$T/cfg_current"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
case " \$* " in
  *" --list "*) [ "${5:-}" = list ] && { echo "Timed out" >&2; exit 1; }; printf '%s\n' $1 ;;
  *" --create "*) [ "${5:-}" = create ] && { echo "Not authorized" >&2; exit 1; }; echo "\$*" >> "$T/created"; echo "Created topic." ;;
  *" --describe "*) [ "${5:-}" = describe ] && { echo "broker unreachable" >&2; exit 1; }
     echo "Topic: x	TopicId: AbCd	PartitionCount: $2	ReplicationFactor: 1	Configs: " ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
[ "${5:-}" = configs ] && { echo "TimeoutException" >&2; exit 1; }
case " \$* " in
  *"$CUR"*) cat "$T/cfg_current" ;;
  *) cat "$T/cfg_long" ;;
esac
M
  chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"
}

run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=x:1 bash "$HERE/ensure-market-commentary-topics.sh" >"$T/out" 2>&1; }

mock "$ALL" 1 delete 10368000000; if run; then ok "right shape passes"; else bad "right shape rejected: $(cat "$T/out")"; fi
mock "" 1 delete 10368000000; if run && [ "$(wc -l < "$T/created" | tr -d " ")" = 4 ]; then ok "absent topics are created and verified"; else bad "absent topics: $(cat "$T/out")"; fi
mock "$ALL" 32 delete 10368000000; if run; then bad "32 partitions passed"; else ok "wrong partition count fails closed"; fi
mock "$ALL" 1 compact 10368000000; if run; then bad "compact passed"; else ok "wrong cleanup policy fails closed"; fi
mock "$ALL" 1 delete 10367999999; if run; then bad "short retention passed"; else ok "retention one millisecond short fails closed"; fi
mock "$ALL" 1 delete -1; if run; then bad "retention -1 passed (the service rejects it)"; else ok "retention -1 fails closed (the service compares numerically)"; fi
for f in list create describe configs; do
  mock "" 1 delete 10368000000 "$f"; if run; then bad "$f failure passed"; else ok "$f failure fails closed"; fi
done
echo "passed=$pass failed=$fail"; [ "$fail" = 0 ]
