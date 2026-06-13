#!/usr/bin/env bash
set -euo pipefail

ROOTS_RAW="${HPSF_REPO_ROOTS:-$PWD}"
IFS=':' read -r -a ROOTS <<<"$ROOTS_RAW"
failures=0

scan_files() {
  local root="$1"
  find "$root" -type f \
    \( -name '*.java' -o -name '*.py' -o -name '*.sh' -o -name '*.yml' -o -name '*.yaml' -o -name '*.json' -o -name '*.properties' -o -name 'Jenkinsfile*' \) \
    ! -path '*/.git/*' ! -path '*/target/*' ! -path '*/build/*' ! -path '*/__pycache__/*' ! -path '*/node_modules/*' \
    ! -path '*/tests/*' ! -path '*/src/test/*' ! -path '*/scripts/hpsf/*' ! -path '*/scripts/smoke/*'
}

check_forbidden() {
  local label="$1"
  local pattern="$2"
  local root="$3"
  local matches
  matches="$(scan_files "$root" \
    | xargs grep -nE "$pattern" 2>/dev/null \
    | grep -Ev 'must not|forbidden|found|ever appeared|require_not_contains|grep -F|grep -q|failures\\.append|not be persisted|contains orderInstruction\\.enabled=true|enabled true found|Dry-run/fixture' \
    || true)"
  if [[ -n "$matches" ]]; then
    echo "HPSF spec compliance failure: $label" >&2
    echo "$matches" >&2
    failures=$((failures + 1))
  fi
}

for root in "${ROOTS[@]}"; do
  [[ -d "$root" ]] || continue
  check_forbidden "orderInstruction.enabled=true literal or equivalent" 'orderInstruction[^\n]*enabled[^\n]*(true|TRUE)|"enabled"[[:space:]]*:[[:space:]]*true' "$root"
  check_forbidden "broker order placement path in HPSF code/config" 'placeOrder|IBKR_ORDER_PLACEMENT_ENABLED[^
]*(true|1|on)|ORDER_PLACEMENT_ENABLED[^
]*(true|1|on)' "$root"
  check_forbidden "old HPSF final JSON field names" 'setupType|targetZoneLower|targetZoneUpper|executionOptionType|flowAnchorOptionType|executableSignal|signalId|asOf' "$root"
done

for root in "${ROOTS[@]}"; do
  stage_a_file="$root/hpsf-processing-service/src/main/java/com/optionsedge/processing/hpsf/HpsfStageATopology.java"
  if [[ -f "$stage_a_file" ]] && grep -nE 'topics\(\)\.signal\(|HPSF_TOPIC_SIGNAL|options\.hpsf\.signal' "$stage_a_file"; then
    echo "HPSF spec compliance failure: Stage A topology references final signal output" >&2
    failures=$((failures + 1))
  fi
done

if [[ "$failures" -ne 0 ]]; then
  echo "HPSF spec compliance check failed with $failures issue(s)." >&2
  exit 1
fi

echo "HPSF spec compliance check passed."
