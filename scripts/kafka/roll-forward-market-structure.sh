#!/usr/bin/env bash
# roll-forward-market-structure.sh — W0 step 4 and its verification: the DESTRUCTIVE half of the market-structure-service JSON -> Avro cutover.
# Runbook: docs/ta-books/MARKET-STRUCTURE-AVRO-CUTOVER.md (processing repository) sections 6 and 7; ordering and wiring: docs/ta-books/DEPLOY-ROLLOUT.md.
#
# WHAT IT DOES. options.spx.market-structure.{ui,sequence,dlq,patterns,recovery} carried JSON written by the previous image. The ms-runtime-5 image
# writes Avro to the SAME names (owner 2026-10-17/18: no versioned names, always rolled forward). After that image is live (step 2) and the gateway that
# decodes Avro is live (step 3), this deletes and recreates those five topics EMPTY under their own names at the topics.env shape, one at a time, so the
# retained JSON is gone. The input topic options.indicators.bars, the geometry topic and the tools topic are never named here and never touched.
#
# WHY THE ORDER IS "AVRO WRITER FIRST, WIPE AFTER". A recreate while any JSON writer is alive puts JSON in the fresh topic within seconds (the 2026-10-18
# ibkr.gex.status incident: 31 JSON heartbeats written into a topic recreated minutes earlier). A new topic id proves the OLD topic is gone, not that nothing
# wrote JSON since; so this script gates on WHO can write, then proves quiet, then wipes, then proves empty.
#
# THE GATES (all fail closed, ALL before the first delete; each is skipped by nothing):
#   G1 the bootstrap server IS the named cluster's broker (a destructive command declared "dev" must not wipe production).
#   G2 New York time is not inside Mon-Fri 00:00-15:59 (the regular session and the hours before it): only after the 16:00 ET close or on a weekend.
#   G3 the service reports SESSION_ENDED (read through the API server's pod proxy, no tool needed in the image).
#   G4 the W0 image digest is the ONLY market-structure image on the cluster: every pod whose image is options-edge-market-structure, in any namespace,
#      carries --expect-image-digest, and exactly one of them is Running in options-edge.
#   G5 --producers-attested: the owner's verified maintenance stop of every host that can run a producer (runbook 4(i)); Kafka cannot prove a process silent.
#   G6 broker-side evidence (4(b2)): kafka-transactions describes the service's transactional id; describe-producers on each of the five topics lists
#      every producer id with its last timestamp; any OTHER id with a recent timestamp is a stop.
#   G7 quiescence (4(b)): over QUIESCE seconds (default 180) the end offsets of options.indicators.bars (8 partitions) and of the five topics, and the five
#      producer counters of /metrics, do not move.
#   G8 each topic carries the id the operator read off the live broker (--pre-cutover-ids), twice (immediately before its delete).
#   G9 (step 3, the gateway that decodes Avro): every pod on the cluster whose image is options-edge-feed-gateway carries --expect-gateway-digest and at least
#      one of them is Running in options-edge: the wipe never starts while a gateway that cannot read the Avro the new image writes is the one serving.
# AFTER the recreate: new id differs, shape equals the declaration, every end offset is 0, the bars did not move, ensure-market-structure-topics.sh passes,
# and committed offsets of any group beyond the new log end are REPORTED (step 5 restarts the service and the gateway).
#
# --verify (step 6, read-only): ids differ from the pre-cutover ones, shape, and the first and last delivered record of every non-empty partition is
# Confluent-framed Avro (byte 0 = 0x00; a JSON value begins 0x7b). That is a SAMPLE. The proof of record for "no JSON retained anywhere" is the runbook
# section 7 full-decode verifier (every record, schema id resolved from the registry), which lives with the processing repository, not here.
#
# Usage:
#   roll-forward-market-structure.sh --recreate|--verify --cluster dev|production --pre-cutover-ids ui=ID,sequence=ID,dlq=ID,patterns=ID,recovery=ID \
#       [--expect-image-digest sha256:<64 hex> --expect-gateway-digest sha256:<64 hex> --producers-attested]   (all three REQUIRED for --recreate) \
#       [--gates-only]   (a rehearsal for --recreate: evaluates every gate G1-G9, the quiescence proof and the topic ids, prints the result, deletes NOTHING) \
#       [--topic-absent-after-failed-create]   (ONLY to finish a --recreate whose delete succeeded and whose create failed) \
#       [--already-recreated name=NEWID,...]   (ONLY to resume a --recreate that stopped part-way: the topics an earlier attempt already recreated, with the
#                                               NEW TOPIC ID it printed; each must still carry exactly that id, the declared shape and end offset 0) \
#       <bootstrap-servers> [kafka-bin-dir]
# Environment (tests): MS_W0_NOW="<dow> <HHMM> ..." replaces the New York clock; MS_W0_NOW_MS the epoch ms; MS_W0_QUIESCE_SECONDS; KUBECTL.
set -euo pipefail

usage() {
  sed -n '/^# Usage:/,/^# Environment/p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}
PHASE="${1:-}"
case "$PHASE" in --recreate|--verify) shift ;; *) usage ;; esac
CLUSTER=""; PRE_IDS=""; DIGEST=""; GW_DIGEST=""; ATTESTED=""; GATES_ONLY=""; ABSENT_OK=""; RECREATED=""; POS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="${2:-}"; shift 2 ;;
    --pre-cutover-ids) PRE_IDS="${2:-}"; shift 2 ;;
    --expect-image-digest) DIGEST="${2:-}"; shift 2 ;;
    --expect-gateway-digest) GW_DIGEST="${2:-}"; shift 2 ;;
    --producers-attested) ATTESTED=yes; shift ;;
    --gates-only) GATES_ONLY=yes; shift ;;
    --topic-absent-after-failed-create) ABSENT_OK=yes; shift ;;
    --already-recreated) RECREATED="${2:-}"; shift 2 ;;
    *) POS+=("$1"); shift ;;
  esac
done
case "$CLUSTER" in dev|production) ;; *) usage ;; esac
[ "$GATES_ONLY" != yes ] || [ "$PHASE" = "--recreate" ] || { echo "FATAL: --gates-only is a rehearsal of --recreate; --verify is already read-only" >&2; exit 2; }
[ -n "$PRE_IDS" ] || usage
BS="${POS[0]:-}"; [ -n "$BS" ] || usage
BIN="${POS[1]:-${KAFKA_BIN_DIR:-}}"
if [ "$PHASE" = "--recreate" ]; then
  [[ "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "FATAL: --recreate needs --expect-image-digest sha256:<64 hex> (G4); nothing was changed" >&2; exit 2; }
  [[ "$GW_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "FATAL: --recreate needs --expect-gateway-digest sha256:<64 hex> (G9); nothing was changed" >&2; exit 2; }
  [ "$ATTESTED" = yes ] || { echo "FATAL: --recreate needs --producers-attested (G5): the verified maintenance stop of every host that can run a producer; nothing was changed" >&2; exit 2; }
fi

# G1 — the bootstrap IS the cluster's broker.
case "$CLUSTER:$BS" in
  dev:localhost:19092|dev:host.docker.internal:19092|dev:192.168.100.102:19092) TXN_ID=market-structure-service-dev ;;
  production:192.168.100.252:9092) TXN_ID=market-structure-service-production ;;
  *) echo "FATAL: bootstrap '$BS' is not the $CLUSTER cluster's broker; refusing (nothing was changed)" >&2; exit 2 ;;
esac

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$HERE/avro-rollforward.lib.sh"
KT="$(rf_resolve_cli "$BIN")"
GO="$(rf_resolve_cli_named kafka-get-offsets "$BIN")"
KG="$(rf_resolve_cli_named kafka-consumer-groups "$BIN")"
kt() { "$KT" --bootstrap-server "$BS" "$@"; }
KUBECTL="${KUBECTL:-kubectl}"
NS=options-edge
SERVICE_POD_LABEL=app.kubernetes.io/name=market-structure-service
INPUT_TOPIC=options.indicators.bars
INPUT_PARTITIONS=8

# name|topic|configs (the topics.env / runbook declaration; ONE partition each, RF 1, min.insync.replicas 1)
SPECS="ui|options.spx.market-structure.ui|cleanup.policy=delete,retention.ms=604800000,max.message.bytes=1048576,min.insync.replicas=1
sequence|options.spx.market-structure.sequence|cleanup.policy=delete,retention.ms=172800000,max.message.bytes=1048576,min.insync.replicas=1
dlq|options.spx.market-structure.dlq|cleanup.policy=delete,retention.ms=604800000,max.message.bytes=1048576,min.insync.replicas=1
patterns|options.spx.market-structure.patterns|cleanup.policy=delete,retention.ms=172800000,max.message.bytes=1048576,min.insync.replicas=1
recovery|options.spx.market-structure.recovery|cleanup.policy=compact,retention.ms=-1,max.message.bytes=1048576,min.insync.replicas=1,segment.bytes=16777216,max.compaction.lag.ms=300000"
TOPICS="$(printf '%s\n' "$SPECS" | cut -d'|' -f2)"

# --pre-cutover-ids: exactly the five names, each a plausible topic id, nothing else.
declare_pre() { # name -> id
  printf '%s' "$PRE_IDS" | tr ',' '\n' | awk -F= -v n="$1" '$1 == n { print $2 }'
}
for n in ui sequence dlq patterns recovery; do
  id="$(declare_pre "$n")"
  [[ "$id" =~ ^[A-Za-z0-9_-]{22}$ ]] || { echo "FATAL: --pre-cutover-ids has no valid 22-character topic id for '$n' (got '$id'); nothing was changed" >&2; exit 2; }
done
[ "$(printf '%s' "$PRE_IDS" | tr ',' '\n' | grep -c .)" = 5 ] || { echo "FATAL: --pre-cutover-ids must name exactly ui,sequence,dlq,patterns,recovery; nothing was changed" >&2; exit 2; }

recreated_id() { # name -> the NEW id the operator says an earlier attempt gave it, or empty
  printf '%s' "$RECREATED" | tr ',' '\n' | awk -F= -v n="$1" '$1 == n { print $2 }'
}
for n in $(printf '%s' "$RECREATED" | tr ',' '\n' | cut -d= -f1); do
  case "$n" in ui|sequence|dlq|patterns|recovery) ;; *) echo "FATAL: --already-recreated names '$n', which is not one of the five topics; nothing was changed" >&2; exit 2 ;; esac
  [[ "$(recreated_id "$n")" =~ ^[A-Za-z0-9_-]{22}$ ]] || { echo "FATAL: --already-recreated has no valid 22-character id for '$n'; nothing was changed" >&2; exit 2; }
done

end_offset() { # topic partitions -> sum string "p:end,..." or fails; refuses a PARTIAL answer
  local out
  out="$("$GO" --bootstrap-server "$BS" --topic "$1" --time -1 2>&1)" || { echo "FATAL: cannot read end offsets of $1: $out" >&2; return 1; }
  printf '%s\n' "$out" | awk -F: -v t="$1" -v want="$2" '$1 == t { n++; s = s $2 ":" $3 "," } END { if (n != want) exit 1; print s }' \
    || { echo "FATAL: end offsets of $1 are partial or missing (wanted $2 partition(s)): $out" >&2; return 1; }
}
topic_present() { # a topic the operator has declared absent (a failed create being finished) is "absent", never an error; anything else must exist
  [ "$ABSENT_OK" = yes ] || return 0
  printf '%s\n' "$(rf_list "$KT" "$BS")" | grep -qx "$1"
}
offsets_snapshot() { # all six topics -> text
  local t
  for t in $INPUT_TOPIC $TOPICS; do
    printf '%s=' "$t"
    if ! topic_present "$t"; then echo absent; continue; fi
    if [ "$t" = "$INPUT_TOPIC" ]; then end_offset "$t" "$INPUT_PARTITIONS"; else end_offset "$t" 1; fi || return 1
  done
}
service_pod() {
  local pods
  pods="$("$KUBECTL" -n "$NS" get pods -l "$SERVICE_POD_LABEL" --field-selector=status.phase=Running -o name 2>&1)" || { echo "FATAL: cannot list the market-structure pod: $pods" >&2; return 1; }
  [ "$(printf '%s\n' "$pods" | grep -c .)" = 1 ] || { echo "FATAL: expected exactly one Running market-structure pod, found: $pods" >&2; return 1; }
  printf '%s\n' "${pods#pod/}"
}
pod_get() { # path -> body (a 503 body is data, not an error)
  local pod; pod="$(service_pod)" || return 1
  "$KUBECTL" get --raw "/api/v1/namespaces/$NS/pods/$pod:8080/proxy$1" 2>&1 || true
}
# rbac_denied: true only when the WHOLE body is kubectl's own authorization failure for the pod proxy (it starts with kubectl's "Error from server (Forbidden)" line and
# names pods/proxy). A body produced by the workload (JSON from /health/ready, /metrics text) never begins that way, so a service response cannot select the fallback.
rbac_denied() {
  case "$1" in
    "Error from server (Forbidden): "*pods/proxy*) return 0 ;;
    *) return 1 ;;
  esac
}
# past_close_margin: the fallback proof is only used from 16:05 ET on a weekday, so "now > rthCloseMs" (strict) holds with a 5 minute margin (G2 itself accepts 16:00:00).
past_close_margin() {
  local now dow hhmm
  now="${MS_W0_NOW:-$(TZ=America/New_York date '+%u %H%M %Y-%m-%d %H:%M:%S %Z')}"
  dow="${now%% *}"; hhmm="${now#* }"; hhmm="${hhmm%% *}"
  (( 10#$dow >= 1 && 10#$dow <= 5 && 10#$hhmm >= 1605 ))
}
# pod_identity: "uid=<pod uid> restarts=<n> ready=<true|false>" of the market-structure container, read with `kubectl get pod` (a verb every deployer can use).
# Used where the pod proxy (get pods/proxy) is not granted to the deployer service account (production): the production RBAC is NOT widened for this script.
pod_identity() {
  local pod json
  pod="$(service_pod)" || return 1
  json="$("$KUBECTL" -n "$NS" get pod "$pod" -o json 2>&1)" || { echo "FATAL: cannot read pod $pod: $json" >&2; return 1; }
  printf '%s' "$json" | python3 -I -c '
import json, sys
p = json.load(sys.stdin)
s = [c for c in (p.get("status", {}).get("containerStatuses") or []) if c.get("name") == "market-structure"]
if len(s) != 1:
    sys.exit(1)
uid, rc = p["metadata"].get("uid", ""), s[0].get("restartCount")
if not uid or not isinstance(rc, int) or isinstance(rc, bool) or rc < 0:
    sys.exit(1)
print("uid=%s restarts=%d ready=%s" % (uid, rc, "true" if s[0].get("ready") else "false"))
' || { echo "FATAL: pod $pod has no single market-structure container status" >&2; return 1; }
}
counters() { # the five producer counters of /metrics, sorted, or fails if any is missing
  local m c out="" ident
  m="$(pod_get /metrics)" || return 1
  if rbac_denied "$m"; then
      # No pod proxy for this service account: the quiescence proof rests on the END OFFSETS of the input and the five topics (offsets_snapshot, compared by the
      # caller; a producer that publishes moves them) plus the pod's identity: the same UID, the same restartCount, and ready=true at both reads.
      ident="$(pod_identity)" || return 1
      case "$ident" in
        *ready=true*) printf '%s\n' "$ident"; return 0 ;;
        *) echo "FATAL (G7): the pod proxy is not permitted and the market-structure container is not Ready ($ident); the quiescence proof requires ready=true at every read. Nothing was changed." >&2; return 1 ;;
      esac
  fi
  for c in records_consumed_total projections_published_total sequence_published_total rejections_published_total pattern_sets_published_total; do
    v="$(printf '%s\n' "$m" | awk -v k="market_structure_$c" '$1 == k { print $2 }')"
    [[ "$v" =~ ^[0-9]+$ ]] || { echo "FATAL: /metrics has no numeric market_structure_$c (got '$v'); the quiescence proof is unreadable" >&2; return 1; }
    out="$out$c=$v "
  done
  printf '%s\n' "$out"
}

gate_clock() { # G2
  local now dow hhmm
  now="${MS_W0_NOW:-$(TZ=America/New_York date '+%u %H%M %Y-%m-%d %H:%M:%S %Z')}"
  dow="${now%% *}"; hhmm="${now#* }"; hhmm="${hhmm%% *}"
  if (( 10#$dow >= 1 && 10#$dow <= 5 && 10#$hhmm < 1600 )); then
    echo "FATAL (G2): New York time is '$now' — a trading-day weekday before the 16:00 close. The wipe runs only after the close or on a weekend. Nothing was changed." >&2
    return 1
  fi
  echo "G2 ok: New York time '$now' is after the close or a weekend"
}
gate_session_ended() { # G3
  local body ident; body="$(pod_get /health/ready)" || return 1
  case "$body" in
    *'"state":"SESSION_ENDED"'*) echo "G3 ok: the service reports SESSION_ENDED" ;;
    *)
      if rbac_denied "$body"; then
        # Production deployer SA cannot get pods/proxy. Equivalent proof from the pod status alone: MarketStructureRunner.readyAt() returns true only when the state is LIVE or
        # SESSION_ENDED, and state becomes SESSION_ENDED at the moment the sequencer ends (MarketStructureRunner: the poll loop calls promoteSuccessor() as soon as sessionEnded(), which sets state=SESSION_ENDED when there is no successor, and recovery sets state from sessionEnded()); a successor session would have another trade date and so would not be Ready. The pod's trade date is today, and - once now is STRICTLY past the schedule's RTH close - only when the sequencer has ended the session. The fallback is used only
        # from 16:05 ET on a weekday (a 5 minute margin over the 16:00 close; the readiness probe is periodSeconds=10 x failureThreshold=3, so a stale Ready is at most ~30 s old, and G7 then re-reads ready=true after the full 180 s observation, so the Ready it relies on was evaluated well after the close; weekends and other trade dates are not Ready, so they fail closed here).
        past_close_margin || { echo "FATAL (G3): the pod proxy is not permitted and it is not yet 16:05 ET on a weekday, so SESSION_ENDED cannot be proven from the pod status. Nothing was changed." >&2; return 1; }
        ident="$(pod_identity)" || return 1
        case "$ident" in
          *ready=true*) echo "G3 ok (pod proxy not permitted; proven from the pod status: Ready after the 16:00 ET close implies SESSION_ENDED): $ident"; return 0 ;;
          *) echo "FATAL (G3): the pod proxy is not permitted and the market-structure container is not Ready ($ident), so SESSION_ENDED is not proven. Nothing was changed." >&2; return 1 ;;
        esac
      fi
      echo "FATAL (G3): the service does not report SESSION_ENDED: $body. Nothing was changed." >&2; return 1 ;;
  esac
}
# all_pods_json: every pod of every namespace as one JSON List. Normally `get pods -A`. The production deployer service account has no cluster-scope pod list, only
# namespaced pod reads; then the same answer is assembled namespace by namespace from `get ns` (which it may list). A namespace it cannot read is tolerated ONLY if it is a
# Kubernetes system namespace (kube-system, kube-public, kube-node-lease, default): tenant workloads there are impossible for this estate's images, and the broker-side
# producer evidence of G6 (no other producer id wrote the five topics) is the cross-namespace guard that does not depend on pod visibility. Any other unreadable namespace fails closed.
all_pods_json() {
  local out nss ns o parts=""
  if out="$("$KUBECTL" get pods -A -o json 2>&1)"; then printf '%s' "$out"; return 0; fi
  case "$out" in
    *'Error from server (Forbidden): pods is forbidden: '*'at the cluster scope'*) ;;
    *) printf '%s' "$out"; return 1 ;;
  esac
  nss="$("$KUBECTL" get ns -o name 2>&1)" || { printf '%s' "$nss"; return 1; }
  [ -n "$nss" ] || { printf 'no namespaces listed'; return 1; }
  parts="$(mktemp)"
  for ns in $nss; do
    ns="${ns#namespace/}"
    if o="$("$KUBECTL" -n "$ns" get pods -o json 2>&1)"; then
      printf '%s\n' "$o" >>"$parts"
    else
      case "$ns" in
        kube-system|kube-public|kube-node-lease|default) echo "note: namespace $ns is not readable by this service account (system namespace, tolerated)" >&2 ;;
        *) rm -f "$parts"; printf 'namespace %s is not readable: %s' "$ns" "$o"; return 1 ;;
      esac
    fi
  done
  python3 -I -c '
import json, sys
items = []
dec = json.JSONDecoder()
text = open(sys.argv[1]).read()
i = 0
while i < len(text):
    while i < len(text) and text[i].isspace():
        i += 1
    if i >= len(text):
        break
    obj, i = dec.raw_decode(text, i)
    items += obj.get("items", [])
print(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}))
' "$parts"
  local rc=$?
  rm -f "$parts"
  return $rc
}
gate_images() { # G4 (market-structure: exactly one Running) and G9 (feed-gateway: at least one Running); $1 = gate, $2 = image name, $3 = digest, $4 = min Running, $5 = max Running (-1 = no maximum), $6 = "ready" to also require that many pods whose matching REGULAR container is ready
  local pods
  pods="$(all_pods_json)" || { echo "FATAL (G4): cannot list pods cluster-wide: $pods" >&2; return 1; }
  printf '%s' "$pods" | python3 -I -c '
import json, sys
gate, image, want, lo, hi, need_ready = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), sys.argv[6] == "ready"
items = json.load(sys.stdin).get("items", [])
seen, running_pods, ready_pods = [], set(), set()
for p in items:
    ns, name, phase = p["metadata"].get("namespace"), p["metadata"]["name"], p.get("status", {}).get("phase")
    for c in (p["spec"].get("containers") or []) + (p["spec"].get("initContainers") or []):
        if image not in (c.get("image") or ""):
            continue
        ids = [s.get("imageID", "") for s in (p.get("status", {}).get("containerStatuses") or []) + (p.get("status", {}).get("initContainerStatuses") or []) if s.get("name") == c["name"]]
        seen.append((ns, name, phase, c["image"], ids))
        if phase == "Running" and ns == "options-edge":
            regular = [s for s in (p.get("status", {}).get("containerStatuses") or []) if s.get("name") == c["name"]]
            running_pods.add(name)   # a pod, however many matching containers it has
            if c in (p["spec"].get("containers") or []) and regular and all(s.get("ready") for s in regular):
                ready_pods.add(name)   # its matching REGULAR container (not an init container) is ready
running = len(running_pods)
bad = [s for s in seen if not s[4] or any(not i.endswith(want) for i in s[4])]
if not seen or bad or running < lo or (hi >= 0 and running > hi) or (need_ready and len(ready_pods) < lo):
    print("FATAL (%s): the pods of image %s are not all at %s with %d..%d Running (%d ready) in options-edge: seen=%r bad=%r running=%d" % (gate, image, want, lo, hi, len(ready_pods), seen, bad, running), file=sys.stderr)
    sys.exit(1)
print("%s ok: %d %s pod(s) on the cluster, %d Running in options-edge, all at %s" % (gate, len(seen), image, running, want))
' "$1" "$2" "$3" "$4" "$5" "${6:-any}"
}
gate_producers() { # G6
  local KX now_ms recent own t out
  KX="$(rf_resolve_cli_named kafka-transactions "$BIN")" || return 1
  now_ms="${MS_W0_NOW_MS:-$(( $(date +%s) * 1000 ))}"
  recent="${MS_W0_PRODUCER_RECENT_MS:-600000}"
  out="$("$KX" --bootstrap-server "$BS" describe --transactional-id "$TXN_ID" 2>&1)" || { echo "FATAL (G6): cannot describe transactional id $TXN_ID: $out" >&2; return 1; }
  own="$(printf '%s\n' "$out" | awk -v id="$TXN_ID" '$2 == id { print $3 }')"
  [[ "$own" =~ ^[0-9]+$ ]] || { echo "FATAL (G6): no ProducerId for $TXN_ID in: $out" >&2; return 1; }
  echo "G6: $TXN_ID is producer id $own"
  for t in $TOPICS; do
    topic_present "$t" || continue
    out="$("$KX" --bootstrap-server "$BS" describe-producers --topic "$t" --partition 0 2>&1)" || { echo "FATAL (G6): cannot describe producers of $t: $out" >&2; return 1; }
    other="$(printf '%s\n' "$out" | awk -v own="$own" -v now="$now_ms" -v rec="$recent" '$1 ~ /^[0-9]+$/ && $1 != own && $5 ~ /^[0-9]+$/ && now - $5 <= rec { printf "%s(last %s) ", $1, $5 }')"
    if [ -n "$other" ]; then
      echo "FATAL (G6): $t has a producer id other than $TXN_ID ($own) with a write in the last ${recent} ms: $other. A foreign writer is live; stop it. Nothing was changed." >&2
      return 1
    fi
  done
  echo "G6 ok: no producer id other than $own wrote the five topics in the last ${recent} ms"
  "$KX" --bootstrap-server "$BS" list 2>&1 | sed 's/^/G6 evidence (transactional ids): /' || true
}
quiescence() { # G7 -> compares two snapshots QUIESCE seconds apart; $1 = label
  local a b ca cb q="${MS_W0_QUIESCE_SECONDS:-180}"
  a="$(offsets_snapshot)" || return 1; ca="$(counters)" || return 1
  echo "G7 ($1): observing $q s"
  sleep "$q"
  b="$(offsets_snapshot)" || return 1; cb="$(counters)" || return 1
  if [ "$a" != "$b" ] || [ "$ca" != "$cb" ]; then
    echo "FATAL (G7 $1): something moved during the ${q}s observation. Offsets before: $(printf '%s' "$a" | tr '\n' ' ') after: $(printf '%s' "$b" | tr '\n' ' ') counters before: $ca after: $cb. Nothing was changed." >&2
    return 1
  fi
  echo "G7 ok ($1): no end offset and no producer counter moved"
  QUIET_SNAPSHOT="$b"
}

verify_shape() { # topic configs -> 0/1 (prints FAIL lines); uses the describe line
  local topic="$1" cfgs="$2" desc got_parts k want have ok=0
  desc="$(rf_describe "$KT" "$BS" "$topic")" || return 1
  got_parts="$(printf '%s' "$desc" | sed -nE 's/.*PartitionCount: ([0-9]+).*/\1/p')"
  [ "$got_parts" = 1 ] || { echo "FAIL: $topic has $got_parts partition(s), declared 1: $desc" >&2; return 1; }
  printf '%s\n' "$desc" | tr '\t' '\n' | grep -qx "ReplicationFactor: ${KAFKA_TOPIC_REPLICATION_FACTOR:-1}" || { echo "FAIL: $topic replication factor is not ${KAFKA_TOPIC_REPLICATION_FACTOR:-1}: $desc" >&2; return 1; }
  while IFS='=' read -r k want; do
    [ -n "$k" ] || continue
    have="$(rf_config_value "$desc" "$k")"
    [ "$have" = "$want" ] || { echo "FAIL: $topic $k is '$have', declared '$want'" >&2; ok=1; }
  done <<<"$(rf_split_declared "$cfgs")"
  return $ok
}
topic_id() { rf_topic_id "$(rf_describe "$KT" "$BS" "$1")" "$1"; }

if [ "$PHASE" = "--recreate" ]; then
  echo "=== W0 step 4: roll forward the market-structure topics on $BS (cluster $CLUSTER) ==="
  gate_clock
  gate_session_ended
  gate_images G4 options-edge-market-structure "$DIGEST" 1 1
  gate_images G9 options-edge-feed-gateway "$GW_DIGEST" 1 -1 ready
  echo "G5 ok: --producers-attested (the owner's verified maintenance stop, runbook 4(i))"
  gate_producers
  QUIET_SNAPSHOT=""
  quiescence "before the wipe"
  BEFORE_BARS="$(printf '%s\n' "$QUIET_SNAPSHOT" | grep "^$INPUT_TOPIC=")"
  # G8 for ALL five topics BEFORE the first delete: one mismatch must leave the whole set untouched.
  while IFS='|' read -r name topic cfgs; do
    LIST="$(rf_list "$KT" "$BS")" || exit 1
    pre="$(declare_pre "$name")"; done_id="$(recreated_id "$name")"
    if printf '%s\n' "$LIST" | grep -qx "$topic"; then
      cur="$(topic_id "$topic")" || exit 1
      if [ -n "$done_id" ]; then
        [ "$cur" = "$done_id" ] || { echo "FAIL (G8): $topic carries id $cur, not the id $done_id the earlier attempt gave it. Nothing was changed." >&2; exit 1; }
      else
        [ "$cur" = "$pre" ] || { echo "FAIL (G8): $topic carries id $cur, not the pre-cutover id $pre — it was already recreated and may hold live Avro (if an earlier attempt of THIS run did it, resume with --already-recreated). Nothing was changed." >&2; exit 1; }
      fi
    else
      [ "$ABSENT_OK" = yes ] && [ -z "$done_id" ] || { echo "FAIL (G8): $topic does not exist. If an earlier --recreate deleted it and failed to create it, re-run with --topic-absent-after-failed-create; otherwise find out who deleted it. Nothing was changed." >&2; exit 1; }
    fi
  done <<<"$SPECS"
  echo "G8 ok: every topic carries the id the operator read off the live broker (or the id of this run's earlier attempt)"
  # the image gates are time-of-check: the quiescence wait above took minutes, so they are judged AGAIN immediately before the first delete
  gate_images G4 options-edge-market-structure "$DIGEST" 1 1
  gate_images G9 options-edge-feed-gateway "$GW_DIGEST" 1 -1 ready
  if [ "$GATES_ONLY" = yes ]; then
    echo "--gates-only: every gate G1-G9 passed; nothing was deleted or created"
    exit 0
  fi
  # the wipe, ONE TOPIC AT A TIME: a failure part-way leaves one topic absent, and the barrier names it. Re-runnable.
  while IFS='|' read -r name topic cfgs; do
    LIST="$(rf_list "$KT" "$BS")" || exit 1
    pre="$(declare_pre "$name")"
    if [ -n "$(recreated_id "$name")" ]; then
      verify_shape "$topic" "$cfgs" || exit 1
      [ "$(end_offset "$topic" 1)" = "0:0," ] || { echo "FAIL: $topic (recreated by an earlier attempt) is not empty: a writer is live. Stop it; this resume cannot continue." >&2; exit 1; }
      echo "-- $topic already recreated by an earlier attempt (id $(recreated_id "$name")), empty, shape ok — skipped"
      continue
    fi
    if printf '%s\n' "$LIST" | grep -qx "$topic"; then
      cur="$(topic_id "$topic")" || exit 1
      [ "$cur" = "$pre" ] || { echo "FAIL (G8): $topic carries id $cur, not the pre-cutover id $pre — it was already recreated and may hold live Avro. Nothing further was changed." >&2; exit 1; }
      again="$(topic_id "$topic")" || exit 1
      [ "$again" = "$pre" ] || { echo "FAIL (G8): $topic changed id to $again while checking; nothing deleted." >&2; exit 1; }
      echo "-- delete $topic (id $cur)"
      kt --delete --topic "$topic"
      settled=0
      for _ in $(seq 1 60); do
        cur_list="$(rf_list "$KT" "$BS")" || exit 1
        printf '%s\n' "$cur_list" | grep -qx "$topic" || { settled=1; break; }
        sleep "${MS_W0_SETTLE_SLEEP:-2}"
      done
      [ "$settled" -eq 1 ] || { echo "FAIL: $topic is still present 120s after the delete; re-run once the broker settles." >&2; exit 1; }
    else
      [ "$ABSENT_OK" = yes ] || { echo "FAIL: $topic does not exist. If a previous --recreate deleted it and failed to create it, re-run with --topic-absent-after-failed-create; otherwise find out who deleted it. Nothing was changed." >&2; exit 1; }
      echo "-- $topic absent (finishing a failed create), creating fresh"
    fi
    args=(--create --topic "$topic" --partitions 1 --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR:-1}")
    while IFS='=' read -r k v; do [ -n "$k" ] && args+=(--config "$k=$v"); done <<<"$(rf_split_declared "$cfgs")"
    echo "-- create $topic (1 partition, $cfgs)"
    kt "${args[@]}" || { echo "FAIL: $topic was deleted but could NOT be created; it is absent now. Re-run with --topic-absent-after-failed-create added." >&2; exit 1; }
    new="$(topic_id "$topic")" || exit 1
    echo "NEW TOPIC ID $name: $new (was $pre)"
    [ "$new" != "$pre" ] || { echo "FAIL: $topic still has the pre-cutover id $pre" >&2; exit 1; }
    verify_shape "$topic" "$cfgs" || exit 1
    [ "$(end_offset "$topic" 1)" = "0:0," ] || { echo "FAIL: $topic is not empty right after the recreate: a writer is live. Stop it and run --recreate again." >&2; exit 1; }
  done <<<"$SPECS"
  # the runbook: then ensure-market-structure-topics.sh (the same check the service's boot makes, so the rollout cannot leave the pod unready)
  ensure_bin="$(mktemp -d)"; trap 'rm -rf "$ensure_bin"' EXIT
  for tool in kafka-topics kafka-configs; do
    if [ -n "$BIN" ]; then ln -s "$(rf_resolve_cli_named "$tool" "$BIN")" "$ensure_bin/$tool"; fi
  done
  PATH="$ensure_bin:$PATH" KAFKA_BOOTSTRAP_SERVERS="$BS" bash "$HERE/ensure-market-structure-topics.sh" || { echo "FAIL: ensure-market-structure-topics.sh refused the recreated topics" >&2; exit 1; }
  # re-read immediately after the recreate: all five at end offset 0, the input topic unmoved (a stray write now makes step 5 mandatory AND widens the section 7 wire check)
  AFTER="$(offsets_snapshot)" || exit 1
  for t in $TOPICS; do
    printf '%s\n' "$AFTER" | grep -qx "$t=0:0," || { echo "FAIL: $t is not at end offset 0 after the recreate: $AFTER" >&2; exit 1; }
  done
  [ "$(printf '%s\n' "$AFTER" | grep "^$INPUT_TOPIC=")" = "$BEFORE_BARS" ] || { echo "FAIL: $INPUT_TOPIC moved during the wipe — the input is live; the service must be restarted (step 5) and the wire check widened" >&2; exit 1; }
  echo "post-recreate: all five topics at end offset 0, $INPUT_TOPIC unmoved"
  GROUPS_OUT="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || { echo "FAIL: cannot read consumer groups on $BS: $GROUPS_OUT" >&2; exit 1; }
  STALE="$(printf '%s\n' "$GROUPS_OUT" | awk -v ts="$(printf '%s' "$TOPICS" | tr '\n' ' ')" 'BEGIN { n = split(ts, a, " "); for (i = 1; i <= n; i++) w[a[i]] = 1 } ($2 in w) && $4 ~ /^[0-9]+$/ && $4 + 0 > $5 + 0 { print $1 ":" $2 "@" $4 " (log end " $5 ")" }')"
  if [ -n "$STALE" ]; then
    echo "WARN: consumer groups hold a committed offset BEYOND the new log end (stale positions survive a topic delete). Restart the service and the gateway (step 5):" >&2
    printf '%s\n' "$STALE" >&2
  fi
  echo "=== DONE: five topics recreated empty. Next: step 5 (restart the service once more: FORCE_RESTART=true, no rollover parameter), then step 6 (--verify, then the section 7 full-decode verifier) ==="
  exit 0
fi

# --verify: read-only
echo "=== W0 step 6: verify the market-structure topics on $BS (cluster $CLUSTER) ==="
CC="$(rf_resolve_cli_named kafka-console-consumer "$BIN")"
fail=0
while IFS='|' read -r name topic cfgs; do
  pre="$(declare_pre "$name")"
  LIST="$(rf_list "$KT" "$BS")" || exit 1
  printf '%s\n' "$LIST" | grep -qx "$topic" || { echo "FAIL: $topic does not exist" >&2; fail=1; continue; }
  id="$(topic_id "$topic")" || { fail=1; continue; }
  [ "$id" != "$pre" ] || { echo "FAIL: $topic still carries its PRE-CUTOVER id $id — it is the topic that held the JSON" >&2; fail=1; continue; }
  verify_shape "$topic" "$cfgs" || { fail=1; continue; }
  if ! nonempty="$(rf_nonempty_partitions "$GO" "$BS" "$topic" 1)"; then echo "FAIL: $topic — could not determine whether it holds records" >&2; fail=1; continue; fi
  wire_ok=1; sampled=0
  while IFS=: read -r pnum p_early p_late; do
    [ -n "$pnum" ] || continue
    for probe in "earliest:$p_early" "latest:$((p_late - 1))"; do
      label="${probe%%:*}"; off="${probe#*:}"
      [ "$off" -ge 0 ] 2>/dev/null || continue
      wrc=0
      if [ "$label" = latest ]; then rf_wire_is_avro_at_log_end "$CC" "$BS" "$topic" "$pnum" "$off" "$p_early" || wrc=$?
      else rf_wire_is_avro "$CC" "$BS" "$topic" "$pnum" "$off" || wrc=$?; fi
      case $wrc in
        0) sampled=$((sampled + 1)) ;;
        1) echo "FAIL: $topic partition $pnum holds a NON-AVRO value at its $label offset ($off): a JSON writer wrote after the wipe. Stop it and run --recreate again." >&2; wire_ok=0 ;;
        2) [ "$label" = earliest ] && continue
           echo "FAIL: $topic partition $pnum delivered no record near its log end: the wire format there is unverified" >&2; wire_ok=0 ;;
      esac
    done
  done <<<"$nonempty"
  [ "$wire_ok" -eq 1 ] || { fail=1; continue; }
  echo "ok  $topic (id $id, 1 partition, $cfgs; $sampled log end(s) sampled, all Avro or tombstone)"
done <<<"$SPECS"
[ "$fail" -eq 0 ] || { echo "=== verify FAILED ===" >&2; exit 1; }
echo "=== verify OK — a SAMPLE. The proof of record is the runbook section 7 full-decode verifier ==="
