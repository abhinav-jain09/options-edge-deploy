#!/usr/bin/env bash
# A NIGHTLY-ARCHIVED topic must keep its data long enough to survive a MISSED archive run.
#
# WHY THIS EXISTS. scripts/ops/archive/oe-topics.env already records the loss this class causes:
#
#   "delta-flow-by-strike and option-price-behavior-by-strike ... were archived NOWHERE, in any
#    environment, against a 1-day Kafka retention — every session of both was being destroyed
#    nightly"
#
# The archiver is checkpoint-based: it resumes from where it stopped. If retention expires the
# records before the next run reaches them, they are gone and the archive simply has a hole. The
# archiver notices afterwards — which is too late to be a control.
#
# THE BOUND IS THREE DAYS, NOT TWO. The daily capture is a WEEKDAY cron
# (scripts/ops/archive/oe-archive.crontab):
#
#   CRON_TZ=America/New_York
#   10 17 * * 1-5   oe-archive-daily.sh
#
# So the worst real gap is not 24h. Miss FRIDAY's run and the next one is MONDAY 17:10 — 72 hours.
# A "survives one missed run" rule that only asked for 48h would still lose every Friday session
# whose run failed, which is exactly the case an operator is least likely to be watching. Hence
# MIN_ARCHIVED_RETENTION_MS = 3 days, matching what the archived topics that got this right already
# declare (spx.basis.a1.evidence, options.databento.gex.strike.preopen = 259200000).
#
# retention.ms=-1 (infinite) always passes: nothing expires, so no run can be too late.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

MIN_ARCHIVED_RETENTION_MS=259200000   # 3 days — one missed Friday run (see header)

# ── EXEMPTIONS ────────────────────────────────────────────────────────────────────────────────
# A topic may sit below the bound ONLY with a reason recorded here, and the reason must name where
# the durable copy actually lives. "It is fine" is not a reason; an exemption without a destination
# is just the bug with a note attached.
#
# Format: <topic>|<reason>
exempt_reason() {
  case "$1" in
    es.futures.cvd)
      echo "1 Hz live snapshot stream; the DURABLE history is es.futures.cvd.bars (retention -1, one" \
           "record per closed bar per timeframe). Losing snapshot frames loses resolution, not the" \
           "session record. ES-CVD-DESIGN.md. NOTE: on prod this topic IS captured by the daily run" \
           "(2026-09-09: +82,358 records) — the starvation described in oe-topics.env is the es4" \
           "job reading the COMPACTED copy, not this one. The frames themselves are not" \
           "reconstructable from bars, so this exemption is a deliberate resolution trade, not a" \
           "claim that nothing is lost." ;;
    *) return 1 ;;
  esac
}

# ── the declared archive set and the declared retentions ──────────────────────────────────────
ARCHIVE_ENV="scripts/ops/archive/oe-topics.env"
TOPICS_ENV="scripts/kafka/topics.env"
[ -f "$ARCHIVE_ENV" ] || { echo "missing $ARCHIVE_ENV" >&2; exit 1; }
[ -f "$TOPICS_ENV" ]  || { echo "missing $TOPICS_ENV"  >&2; exit 1; }

# Every topic the prod daily capture is responsible for.
archived=$(
  # shellcheck disable=SC1090
  set +u; . "$ARCHIVE_ENV" >/dev/null 2>&1 || true; set -u
  printf '%s\n' ${OE_HEAVY_TOPICS_prod:-} | tr ' ' '\n' | sed '/^$/d' | sort -u
)
[ -n "$archived" ] || { echo "OE_HEAVY_TOPICS_prod resolved to nothing — refusing to pass vacuously" >&2; exit 1; }

# Declared retention overrides (both the shared and the prod-only declaration).
overrides=$(
  grep -hoE '(^|")OPTIONS_EDGE(_PROD_ONLY)?_TOPIC_RETENTION_OVERRIDES="[^"]*"' "$TOPICS_ENV" \
    | sed 's/^[^"]*"//; s/"$//' | tr ' ' '\n' | sed '/^$/d'
)

fail=0; checked=0; exempted=0
while read -r topic; do
  [ -n "$topic" ] || continue
  ret=$(printf '%s\n' "$overrides" | awk -F= -v t="$topic" '$1==t {print $2}' | tail -1)
  [ -n "$ret" ] || continue                 # no override => platform default, not this guard's subject
  checked=$((checked + 1))
  [ "$ret" = "-1" ] && continue             # infinite: nothing can expire
  if [ "$ret" -ge "$MIN_ARCHIVED_RETENTION_MS" ] 2>/dev/null; then continue; fi
  if reason=$(exempt_reason "$topic"); then
    exempted=$((exempted + 1))
    printf '  EXEMPT %-46s %s ms\n' "$topic" "$ret"
    printf '         reason: %s\n' "$reason" | fold -s -w 100 | sed '2,$s/^/                 /'
    continue
  fi
  fail=1
  printf '  FAIL   %-46s retention=%s ms (< %s = 3 days)\n' "$topic" "$ret" "$MIN_ARCHIVED_RETENTION_MS"
  printf '         nightly-archived (OE_HEAVY_TOPICS_prod) but expires before a missed Friday run\n'
  printf '         is retried on Monday. Widen it, set -1, or add an exemption WITH the location of\n'
  printf '         the durable copy in exempt_reason().\n'
done <<< "$archived"

echo "[archived-retention] checked $checked declared-retention archived topic(s), $exempted exempt"
if [ "$fail" -ne 0 ]; then echo "[archived-retention] FAILED"; exit 1; fi
echo "[archived-retention] OK"
