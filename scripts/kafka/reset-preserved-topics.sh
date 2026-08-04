#!/usr/bin/env bash
# The RESET-PRESERVED topic list: ONE implementation, shared by the destructive reset job, by
# cleanup-topics.sh, and by CI.
#
# Source it to get $RESET_PRESERVED_TOPICS and is_reset_preserved(); run it directly to print the
# resolved list. Both callers therefore execute the SAME parser, so a test of this file is a test of
# what production does -- which greping the caller for a function name never was.
#
# Each declaration is validated INDEPENDENTLY. Requiring only a non-empty COMBINED result meant that
# losing just the prod-only assignment left a partial keep-list that still looked valid, and on
# production that silently made underlying.vix.price purgeable.
#
# ENVIRONMENT=production adds the prod-only list. Anywhere else it is deliberately absent: vix is
# durable on production only.

_rpt_one() { # variable name -> its value; fails unless EXACTLY ONE assignment exists
  local var="$1" file="$2" n
  n=$(grep -cE "^${var}=\"" "$file" 2>/dev/null || true)
  if [ "${n:-0}" -ne 1 ]; then
    echo "FATAL: expected exactly ONE ${var}= assignment in $file, found ${n:-0}" >&2
    return 1
  fi
  sed -nE "s/^${var}=\"(.*)\"$/\\1/p" "$file"
}

rpt_resolve() { # topics.env path -> the resolved list on stdout
  local file="$1" base prod
  if [ ! -r "$file" ]; then
    echo "FATAL: cannot read $file — refusing to decide what may be purged" >&2
    return 1
  fi
  base="$(_rpt_one OPTIONS_EDGE_RESET_PRESERVED_TOPICS "$file")" || return 1
  if [ -z "${base// /}" ]; then
    echo "FATAL: OPTIONS_EDGE_RESET_PRESERVED_TOPICS is empty — refusing to purge with an empty keep-list" >&2
    return 1
  fi
  # Validated even when it will not be used, so a malformed prod declaration is caught on dev too
  # rather than waiting to be discovered on the one environment where it destroys data.
  prod="$(_rpt_one OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS "$file")" || return 1
  if [ -z "${prod// /}" ]; then
    echo "FATAL: OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS is empty" >&2
    return 1
  fi
  if [ "${ENVIRONMENT:-}" = "production" ]; then
    printf '%s %s\n' "$base" "$prod"
  else
    printf '%s\n' "$base"
  fi
}

_RPT_SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# When SOURCED -- which is how both destructive scripts use this -- the declaration path is PINNED.
# RPT_TOPICS_ENV is honoured only when this file is EXECUTED, i.e. by the tests.
#
# It was an unconditional override in the first draft, which made it an ambient production input:
# launching the off-hours purge with RPT_TOPICS_ENV=/tmp/valid-but-incomplete.env would have been
# accepted as authoritative and quietly omitted real preserved topics, with every shape check
# still passing because the crafted file is well-formed.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  _RPT_ENV="${RPT_TOPICS_ENV:-$_RPT_SELF/topics.env}"
else
  _RPT_ENV="$_RPT_SELF/topics.env"
fi
RESET_PRESERVED_TOPICS="$(rpt_resolve "$_RPT_ENV")" || exit 1
is_reset_preserved() {
  case " $RESET_PRESERVED_TOPICS " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

# Executed rather than sourced: print the list so CI can assert on it.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  printf '%s\n' $RESET_PRESERVED_TOPICS
fi
