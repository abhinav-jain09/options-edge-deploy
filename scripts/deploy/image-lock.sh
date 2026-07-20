#!/usr/bin/env bash

all_image_vars() {
  cat <<'EOF'
RAW_TO_DISPLAY_IMAGE
WEB_IMAGE
DATABENTO_VOLUME_AGGREGATOR_IMAGE
DATABENTO_FEED_IMAGE
DATABENTO_SR3_FEED_IMAGE
DATABENTO_GEX_IMAGE
OPTION_PRICE_BEHAVIOR_IMAGE
DATABENTO_MISSION_SANDWICH_IMAGE
VOLUME_PACE_IMAGE
DIRECTIONAL_PRESSURE_IMAGE
DATABENTO_GEX_HISTORY_IMAGE
RAW_POSTGRES_WRITER_IMAGE
PIN_POSTGRES_WRITER_IMAGE
PRESSURE_POSTGRES_WRITER_IMAGE
FEED_GATEWAY_IMAGE
HPSF_PROCESSING_IMAGE
HPSF_POSTGRES_WRITER_IMAGE
SPX_MISSION_CONTROL_IMAGE
STRIKE_FLOW_CLASSIFIER_IMAGE
DELTA_FLOW_IMAGE
DEALER_LEDGER_IMAGE
DEALER_LEDGER_CALIBRATION_IMAGE
STRIKE_LIQUIDITY_HEATMAP_IMAGE
UNIFIED_SR_IMAGE
STRIKE_INTELLIGENCE_IMAGE
OPTION_TRUTH_ENGINE_IMAGE
MARKET_CARRY_IMAGE
ES_GEX_SPX_ALIGN_IMAGE
VIX_OPTION_INTELIGENCE_IMAGE
STRIKE_FLOW_AVRO_ADAPTER_IMAGE
GEX_DELTA_REDIS_WRITER_IMAGE
IBKR_FEED_IMAGE
DATABENTO_MAXPAIN_IMAGE
STRIKE_INVASION_IMAGE
INVASION_POSTGRES_WRITER_IMAGE
SPREAD_SKEW_IMAGE
REVERSAL_CONFIRMATION_IMAGE
SPREAD_SKEW_POSTGRES_WRITER_IMAGE
ES_OPEN_DIRECTION_IMAGE
ES_OPEN_DIRECTION_POSTGRES_WRITER_IMAGE
REVERSAL_POSTGRES_WRITER_IMAGE
EOF
  # short-premium-agent renders in dev+production (a standalone service that runs on prod too), NOT
  # experiment. Include it in the image-var set for dev+production so their DEPLOY_TARGET=all pin loop
  # finds it (env rewrite + lock-key allow), while experiment never requires or references it (no
  # empty-var lock failure under REQUIRE_IMAGE_LOCK).
  if [ "${ENVIRONMENT:-dev}" = "dev" ] || [ "${ENVIRONMENT:-dev}" = "production" ]; then
    cat <<'EOF'
SHORT_PREMIUM_AGENT_IMAGE
SIGNAL_FOLLOWER_IMAGE
EOF
  fi
}

target_image_vars() {
  case "${DEPLOY_TARGET:-all}" in
    all)
      all_image_vars
      ;;
    delta-flow-service)
      printf '%s\n' DELTA_FLOW_IMAGE
      ;;
    strike-liquidity-heatmap-service)
      printf '%s\n' STRIKE_LIQUIDITY_HEATMAP_IMAGE
      ;;
    *)
      echo "Unsupported DEPLOY_TARGET: ${DEPLOY_TARGET}" >&2
      return 1
      ;;
  esac
}

image_lock_key_allowed() {
  local key="$1" base
  case "$key" in
    OPTIONS_EDGE_IMAGE_LOCK_FORMAT|OPTIONS_EDGE_IMAGE_LOCK_SOURCE_REPO|OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT|OPTIONS_EDGE_IMAGE_LOCK_BUILD_URL|OPTIONS_EDGE_IMAGE_LOCK_PLATFORM|OPTIONS_EDGE_IMAGE_LOCK_TAG)
      return 0
      ;;
    *_GIT_COMMIT)
      base="${key%_GIT_COMMIT}"
      ;;
    *_JENKINS_BUILD_URL)
      base="${key%_JENKINS_BUILD_URL}"
      ;;
    *)
      base="$key"
      ;;
  esac
  while IFS= read -r var_name; do
    [ "$base" = "$var_name" ] && return 0
  done < <(all_image_vars)
  return 1
}

parse_image_lock_file() {
  local lock_file="$1" parsed_file="$2" keys_file line line_no key
  keys_file="$(mktemp)"
  : >"$parsed_file"
  line_no=0
  while IFS= read -r line || [ -n "$line" ]; do
    line_no=$((line_no + 1))
    [ -n "$line" ] || continue
    if [[ "$line" != *=* ]]; then
      echo "FATAL: image lock line $line_no is not strict KEY=value syntax: $line" >&2
      rm -f "$keys_file"
      return 1
    fi
    key="${line%%=*}"
    if [[ ! "$key" =~ ^[A-Z][A-Z0-9_]*$ ]]; then
      echo "FATAL: image lock line $line_no has invalid key: $key" >&2
      rm -f "$keys_file"
      return 1
    fi
    if ! image_lock_key_allowed "$key"; then
      echo "FATAL: image lock line $line_no has unknown key: $key" >&2
      rm -f "$keys_file"
      return 1
    fi
    if grep -Fxq "$key" "$keys_file"; then
      echo "FATAL: image lock contains duplicate key: $key" >&2
      rm -f "$keys_file"
      return 1
    fi
    printf '%s\n' "$key" >>"$keys_file"
    printf '%s\n' "$line" >>"$parsed_file"
  done <"$lock_file"
  rm -f "$keys_file"
}

image_lock_value() {
  local parsed_file="$1" wanted="$2" line key
  while IFS= read -r line || [ -n "$line" ]; do
    key="${line%%=*}"
    if [ "$key" = "$wanted" ]; then
      printf '%s\n' "${line#*=}"
      return 0
    fi
  done <"$parsed_file"
  return 1
}

rewrite_images_env() {
  local env_file="$1" tmp_file
  tmp_file="${env_file}.tmp"
  : >"$tmp_file"
  while IFS= read -r var_name; do
    [ -n "$var_name" ] || continue
    printf '%s=%s\n' "$var_name" "${!var_name:-}" >>"$tmp_file"
  done < <(all_image_vars)
  mv "$tmp_file" "$env_file"
}

apply_image_lock() {
  local lock_file="${1:-}" required="${2:-false}" env_file="$3" parsed_lock locked missing var_name
  if [ -z "$lock_file" ]; then
    if [ "$required" = "true" ]; then
      echo "FATAL: REQUIRE_IMAGE_LOCK=true but IMAGE_LOCK_FILE is empty." >&2
      return 1
    fi
    echo "IMAGE_LOCK_FILE is empty; using resolved image tags and digest-pinning them during apply."
    return 0
  fi
  if [ ! -f "$lock_file" ]; then
    if [ "$required" = "true" ]; then
      echo "FATAL: image lock file does not exist: $lock_file" >&2
      return 1
    fi
    echo "WARN: image lock file not found ($lock_file); using resolved image tags."
    return 0
  fi

  parsed_lock="$(mktemp)"
  if ! parse_image_lock_file "$lock_file" "$parsed_lock"; then
    rm -f "$parsed_lock"
    return 1
  fi

  if [ "$(image_lock_value "$parsed_lock" OPTIONS_EDGE_IMAGE_LOCK_FORMAT || true)" != "1" ]; then
    echo "FATAL: image lock file is missing OPTIONS_EDGE_IMAGE_LOCK_FORMAT=1: $lock_file" >&2
    rm -f "$parsed_lock"
    return 1
  fi

  missing=0
  while IFS= read -r var_name; do
    [ -n "$var_name" ] || continue
    locked="$(image_lock_value "$parsed_lock" "$var_name" || true)"
    if [ -z "$locked" ]; then
      if [ "$required" = "true" ]; then
        echo "FATAL: image lock missing required image variable: $var_name" >&2
        missing=1
      fi
      continue
    fi
    if [[ "$locked" != *@sha256:* ]]; then
      echo "FATAL: image lock value for $var_name is not digest-pinned: $locked" >&2
      missing=1
      continue
    fi
    printf -v "$var_name" '%s' "$locked"
    export "$var_name"
  done < <(target_image_vars)

  if [ "$missing" != "0" ]; then
    rm -f "$parsed_lock"
    return 1
  fi

  rm -f "$parsed_lock"
  rewrite_images_env "$env_file"
  echo "Applied image lock: $lock_file"
  sed 's/^/  /' "$env_file"
}
