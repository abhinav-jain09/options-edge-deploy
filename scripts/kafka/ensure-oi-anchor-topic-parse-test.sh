#!/usr/bin/env bash
# Adversarial cases for the policy/retention parsing in ensure-oi-anchor-topic.sh.
#
# The first version of that parser excluded commas from the value's character class, so
# "cleanup.policy=compact,delete" parsed as "compact" and PASSED -- the barrier admitted the one
# configuration it was built to refuse. These cases exist so that cannot come back silently.
set -euo pipefail
# The SHIPPED parser, sourced -- not a copy. A copied parser lets the test stay green while the
# real one is wrong, which is how the compact,delete hole survived its first review.
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"
parse_policy()    { extract 'cleanup\.policy' "$1"; }
parse_retention() { extract 'retention\.ms' "$1"; }

fail=0
check() { # description | config | expected-policy | expected-retention
  local desc="$1" cfg="$2" wp="$3" wr="$4"
  local gp gr; gp=$(parse_policy "$cfg"); gr=$(parse_retention "$cfg")
  if [ "$gp" = "$wp" ] && [ "$gr" = "$wr" ]; then
    printf '  ok   %-34s policy=%-14s retention=%s\n' "$desc" "${gp:-<none>}" "${gr:-<none>}"
  else
    printf '  FAIL %-34s policy=%-14s (want %s)  retention=%s (want %s)\n' "$desc" "${gp:-<none>}" "$wp" "${gr:-<none>}" "$wr"
    fail=1
  fi
}
S='sensitive=false synonyms={}'
check "pure compact"        "cleanup.policy=compact $S, retention.ms=-1 $S"       "compact"       "-1"
check "compact,delete"      "cleanup.policy=compact,delete $S, retention.ms=-1 $S" "compact,delete" "-1"
check "delete,compact"      "cleanup.policy=delete,compact $S, retention.ms=-1 $S" "delete,compact" "-1"
check "finite retention"    "cleanup.policy=compact $S, retention.ms=86400000 $S"  "compact"       "86400000"
check "no retention stated" "cleanup.policy=compact $S"                            "compact"       ""
check "empty output"        ""                                                     ""              ""
# delete.retention.ms must NOT be mistaken for retention.ms -- they are different knobs and the
# tombstone one is routinely set on compacted topics.
check "delete.retention only" "cleanup.policy=compact $S, delete.retention.ms=604800000 $S" "compact" ""
# Boundary cases: a bare substring match would accept all four of these as retention.ms.
check "retention at start of output" "retention.ms=-1 cleanup.policy=compact"                "compact" "-1"
check "_retention.ms is NOT it"      "cleanup.policy=compact $S, _retention.ms=-1 $S"         "compact" ""
check "Aretention.ms is NOT it"      "cleanup.policy=compact $S, Aretention.ms=-1 $S"         "compact" ""
check "malformed run-on value"       "cleanup.policy=compact,retention.ms=-1 $S"              "compact,retention.ms=-1" "-1"

# And the values the SCRIPT accepts: only pure compact with -1 may proceed.
echo "  --- acceptance ---"
for c in "compact:-1:accept" "compact,delete:-1:reject" "delete,compact:-1:reject" "compact:86400000:reject" "compact::reject" "compact,retention.ms=-1:-1:reject"; do
  IFS=: read -r pol ret want <<< "$c"
  got=$([ "$pol" = "compact" ] && [ "$ret" = "-1" ] && echo accept || echo reject)
  if [ "$got" = "$want" ]; then printf '  ok   policy=%-15s retention=%-9s -> %s\n' "$pol" "${ret:-<unset>}" "$got"
  else printf '  FAIL policy=%-15s retention=%-9s -> %s (want %s)\n' "$pol" "${ret:-<unset>}" "$got" "$want"; fail=1; fi
done
[ $fail -eq 0 ] && echo "=== ensure-oi-anchor-topic-parse-test: OK ===" || { echo "=== ensure-oi-anchor-topic-parse-test: FAILED ==="; exit 1; }
