#!/usr/bin/env bash
# Dev environment guard for OptionsEdge.
#
# This is intentionally read-only. It catches the regressions that froze dev:
#   - Kafka not reachable on the native single-broker endpoint
#   - broker 2 references in Kubernetes config
#   - non-internal Kafka topics with more than 3 partitions
#   - topics assigned to broker 2 / leaderless partitions
#   - Postgres down, which breaks Databento GEX OI baseline startup
#   - core dev services not Ready
set -euo pipefail

NS="${K8S_NAMESPACE:-options-edge}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-docker-desktop}"
KAFKA_HOME="${KAFKA_HOME:-/Users/abhinav/development/kafka-options-edge/current}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
MAX_PARTITIONS="${MAX_PARTITIONS:-3}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-options_flow_dev}"
POSTGRES_USER="${POSTGRES_USER:-options_flow}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-Options#100}"

KAFKA_TOPICS="$KAFKA_HOME/bin/kafka-topics.sh"

FAILS=()
WARNS=()
OKS=()

ok() { OKS+=("$1"); }
warn() { WARNS+=("$1"); }
fail() { FAILS+=("$1"); }
have() { command -v "$1" >/dev/null 2>&1; }

print_section() {
  printf '\n%s\n' "$1"
}

check_tools() {
  for tool in kubectl jq nc; do
    have "$tool" || fail "missing required tool: $tool"
  done
  [ -x "$KAFKA_TOPICS" ] || fail "Kafka topics CLI not executable: $KAFKA_TOPICS"
}

check_kafka() {
  [ -x "$KAFKA_TOPICS" ] || return 0
  local topics_file
  topics_file="$(mktemp)"
  if "$KAFKA_TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >"$topics_file" 2>/tmp/dev-env-guard-kafka.err; then
    ok "Kafka reachable at $KAFKA_BOOTSTRAP ($(wc -l <"$topics_file" | tr -d ' ') topics)"
  else
    fail "Kafka unreachable at $KAFKA_BOOTSTRAP: $(cat /tmp/dev-env-guard-kafka.err)"
    rm -f "$topics_file"
    return 0
  fi

  local bad_partitions
  bad_partitions="$("$KAFKA_TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe \
    | awk -v max="$MAX_PARTITIONS" '
      $1 == "Topic:" && $2 !~ /^(__|_schemas$)/ {
        topic=$2
        partitions=""
        for (i=1; i<=NF; i++) {
          if ($i == "PartitionCount:") partitions=$(i+1)
        }
        if (partitions != "" && partitions > max) print topic ":" partitions
      }')"
  if [ -n "$bad_partitions" ]; then
    fail "topics exceed ${MAX_PARTITIONS} partitions: ${bad_partitions//$'\n'/, }"
  else
    ok "All non-internal topics have <= ${MAX_PARTITIONS} partitions"
  fi

  local broker2_refs
  broker2_refs="$("$KAFKA_TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe \
    | rg 'Replicas: .*(^|,)2(,|\s|$)|Isr: .*(^|,)2(,|\s|$)|Leader: 2|Leader: -1' || true)"
  if [ -n "$broker2_refs" ]; then
    fail "Kafka topic metadata still references broker 2 or leaderless partitions"
  else
    ok "No topic metadata references broker 2 or leaderless partitions"
  fi

  rm -f "$topics_file"
}

check_kubernetes_config() {
  have kubectl || return 0
  local cm_yaml
  if ! cm_yaml="$(kubectl --context "$KUBECTL_CONTEXT" -n "$NS" get cm options-edge-config options-edge-databento-feed-config -o yaml 2>/tmp/dev-env-guard-kubectl.err)"; then
    fail "could not read dev configmaps: $(cat /tmp/dev-env-guard-kubectl.err)"
    return 0
  fi

  if printf '%s\n' "$cm_yaml" | rg 'broker[-_]?2|:29092|localhost:9093|host\.docker\.internal:29092' >/dev/null; then
    fail "Kubernetes ConfigMaps still contain broker 2 / old broker endpoints"
  else
    ok "ConfigMaps contain no broker 2 endpoints"
  fi

  local live_bootstrap
  live_bootstrap="$(printf '%s\n' "$cm_yaml" | awk '/KAFKA_BOOTSTRAP_SERVERS:/ {print $2}' | sort -u | tr '\n' ' ')"
  if printf '%s' "$live_bootstrap" | rg 'host\.docker\.internal:19092|localhost:19092' >/dev/null; then
    ok "Kubernetes bootstrap points at native Kafka ($live_bootstrap)"
  else
    fail "Kubernetes bootstrap does not point at native Kafka: $live_bootstrap"
  fi
}

check_postgres() {
  if nc -z "$POSTGRES_HOST" "$POSTGRES_PORT" >/dev/null 2>&1; then
    ok "Postgres TCP reachable at ${POSTGRES_HOST}:${POSTGRES_PORT}"
  else
    fail "Postgres is down at ${POSTGRES_HOST}:${POSTGRES_PORT}; GEX OI baseline will fail"
    return 0
  fi

  if have psql; then
    if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'select 1' >/dev/null 2>&1; then
      ok "Postgres login works for ${POSTGRES_USER}@${POSTGRES_DB}"
    else
      fail "Postgres is reachable but ${POSTGRES_USER}@${POSTGRES_DB} login/select failed"
    fi
  else
    warn "psql missing; skipped Postgres login check"
  fi
}

check_services() {
  have kubectl || return 0
  local services=(
    options-edge-web
    feed-gateway-service
    options-edge-databento-feed
    raw-to-display-databento-service
    strike-flow-classifier-databento
    databento-gex-service
    databento-gex-history-service
    databento-mission-pace-service
    spx-mission-control-service
  )

  local svc
  for svc in "${services[@]}"; do
    local ready desired counts
    counts="$(kubectl --context "$KUBECTL_CONTEXT" -n "$NS" get deploy "$svc" \
      -o jsonpath='{.status.readyReplicas} {.spec.replicas}' 2>/dev/null || printf '0 0')"
    read -r ready desired <<<"$counts"
    ready="${ready:-0}"
    desired="${desired:-0}"
    if [ "$desired" != "0" ] && [ "$ready" = "$desired" ]; then
      ok "$svc ready ($ready/$desired)"
    else
      fail "$svc not ready ($ready/$desired)"
    fi
  done
}

check_gateway_health() {
  if ! have curl || ! have jq; then
    warn "curl/jq missing; skipped gateway health JSON check"
    return 0
  fi
  local health
  if ! health="$(curl -fsS --max-time 5 http://localhost:8091/health 2>/tmp/dev-env-guard-health.err)"; then
    fail "feed gateway health unreachable: $(cat /tmp/dev-env-guard-health.err)"
    return 0
  fi

  local running state avro gex mission
  running="$(printf '%s' "$health" | jq -r '.running')"
  state="$(printf '%s' "$health" | jq -r '.stateCaughtUp')"
  avro="$(printf '%s' "$health" | jq -r '.avroCaughtUp')"
  gex="$(printf '%s' "$health" | jq -r '.gexByStrike')"
  mission="$(printf '%s' "$health" | jq -r '.missionPaces')"

  [ "$running" = "true" ] && ok "feed gateway running" || fail "feed gateway running=false"
  [ "$state" = "true" ] && ok "feed gateway state cache caught up" || fail "feed gateway state cache not caught up"
  [ "$avro" = "true" ] && ok "feed gateway avro cache caught up" || fail "feed gateway avro cache not caught up"
  [ "${gex:-0}" -gt 0 ] && ok "gateway has GEX cache ($gex)" || warn "gateway GEX cache empty"
  [ "${mission:-0}" -gt 0 ] && ok "gateway has mission pace cache ($mission)" || warn "gateway mission pace cache empty"
}

check_gex_logs() {
  have kubectl || return 0
  local logs
  logs="$(kubectl --context "$KUBECTL_CONTEXT" -n "$NS" logs deployment/databento-gex-service --tail=200 2>/dev/null || true)"
  if printf '%s\n' "$logs" | rg 'OI baseline unavailable|Connection refused|Could not initialize PostgreSQL' >/dev/null; then
    fail "databento-gex-service logs show Postgres/OI baseline failure"
  else
    ok "databento-gex-service logs show no Postgres/OI baseline failure"
  fi
}

check_tools
check_kafka
check_kubernetes_config
check_postgres
check_services
check_gateway_health
check_gex_logs

print_section "OK"
printf '  - %s\n' "${OKS[@]:-none}"

if [ "${#WARNS[@]}" -gt 0 ]; then
  print_section "WARN"
  printf '  - %s\n' "${WARNS[@]}"
fi

if [ "${#FAILS[@]}" -gt 0 ]; then
  print_section "FAIL"
  printf '  - %s\n' "${FAILS[@]}"
  exit 1
fi

print_section "PASS"
printf 'dev environment guard passed\n'
